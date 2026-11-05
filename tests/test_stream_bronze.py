import json
import uuid

import pytest

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from vantage import config
from vantage.ingest.stream_bronze import (bronze_offset_stats, contiguity_report,
                                          kafka_source, to_bronze, write_bronze)

pytestmark = pytest.mark.kafka


@pytest.fixture
def admin():
    return AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})


@pytest.fixture
def topic(admin):
    # isolated 3-partition topic per test; never touches the real recentchange topic
    name = f"vantage-stream-test-{uuid.uuid4().hex[:8]}"
    admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)])[name].result(timeout=30)
    yield name
    admin.delete_topics([name])


def produce(topic, n, start=0):
    p = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "enable.idempotence": True})
    for i in range(start, start + n):
        key = f"Page_{i % 5}".encode()  # a few keys so events spread over the 3 partitions
        p.produce(topic, key=key, value=json.dumps({"seq": i}).encode())
    p.flush(30)


def run_once(spark, topic, bronze, checkpoint):
    """Drain everything available into Bronze, then stop (Trigger.AvailableNow)."""
    df = to_bronze(kafka_source(spark, config.KAFKA_BOOTSTRAP, topic))
    q = write_bronze(df, bronze, checkpoint, {"availableNow": True})
    q.awaitTermination()


def count(spark, bronze):
    return spark.read.format("delta").load(str(bronze)).count()


def test_bronze_stream_contiguous_and_idempotent(spark, tmp_path, topic):
    bronze = tmp_path / "bronze_edits"
    checkpoint = tmp_path / "ckpt"

    # N messages in -> N rows, every partition contiguous with no duplicate offsets
    produce(topic, 30)
    run_once(spark, topic, bronze, checkpoint)
    assert count(spark, bronze) == 30

    df = spark.read.format("delta").load(str(bronze))
    assert df.columns == ["key", "value", "topic", "partition", "offset",
                          "kafka_timestamp", "ingested_at", "kafka_date"]
    report = contiguity_report(bronze_offset_stats(df))
    assert report, "expected at least one partition"
    for p, r in report.items():
        assert r["contiguous"], f"partition {p} has a gap: {r}"
        assert r["no_duplicates"], f"partition {p} has duplicate offsets: {r}"
        assert r["min_offset"] == 0  # fresh topic: Bronze starts at the earliest offset

    # a second run with nothing new adds 0 rows (checkpoint resumes where it left off)
    run_once(spark, topic, bronze, checkpoint)
    assert count(spark, bronze) == 30

    # produce more -> only the new ones land
    produce(topic, 12, start=30)
    run_once(spark, topic, bronze, checkpoint)
    assert count(spark, bronze) == 42

    report = contiguity_report(bronze_offset_stats(spark.read.format("delta").load(str(bronze))))
    for p, r in report.items():
        assert r["contiguous"], f"partition {p} not contiguous after incremental run: {r}"
        assert r["no_duplicates"], f"partition {p} gained duplicate offsets: {r}"

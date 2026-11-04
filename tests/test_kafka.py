import json
import uuid

import pytest

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from vantage import config

pytestmark = pytest.mark.kafka


@pytest.fixture
def admin():
    return AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})


@pytest.fixture
def topic(admin):
    # isolated 3-partition topic per test so we never touch the real recentchange topic
    name = f"vantage-test-{uuid.uuid4().hex[:8]}"
    admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)])[name].result(timeout=30)
    yield name
    admin.delete_topics([name])


def _produce(name, key, value):
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    out = {}

    def ack(err, msg):
        assert err is None, err
        out["partition"], out["offset"] = msg.partition(), msg.offset()

    producer.produce(name, key=key.encode(), value=json.dumps(value).encode(), callback=ack)
    producer.flush(15)
    return out["partition"], out["offset"]


def test_roundtrip(topic):
    part, off = _produce(topic, "Albert_Einstein", {"title": "Albert_Einstein", "n": 1})
    assert part in (0, 1, 2) and off == 0

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": f"t-{uuid.uuid4()}",
        "enable.auto.commit": False,
    })
    try:
        consumer.assign([TopicPartition(topic, part, off)])
        msg = consumer.poll(15)
    finally:
        consumer.close()

    assert msg is not None and msg.error() is None
    assert msg.key().decode() == "Albert_Einstein"
    assert json.loads(msg.value().decode())["n"] == 1


def test_same_key_same_partition(topic):
    seen = [_produce(topic, "Berlin", {"seq": i}) for i in range(3)]
    partitions = {p for p, _ in seen}
    offsets = [o for _, o in seen]
    assert len(partitions) == 1  # one key -> one partition
    assert offsets == [0, 1, 2]  # delivered in order, consecutive

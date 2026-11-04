"""Create the recentchange Kafka topic if it isn't there yet.

Idempotent: safe to run repeatedly. Partition count, replication factor and
retention come from the [kafka] block in config/vantage.toml. Run from the
project root with the broker up (docker compose up -d):

    python scripts/create_topic.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from vantage import config


def main():
    kafka = config.KAFKA
    topic = kafka["topic"]
    retention_ms = kafka["retention_days"] * 24 * 60 * 60 * 1000

    admin = AdminClient({"bootstrap.servers": kafka["bootstrap_servers"]})

    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        parts = len(existing[topic].partitions)
        print(f"topic '{topic}' already exists: {parts} partitions")
        _print_retention(admin, topic)
        return

    new = NewTopic(
        topic,
        num_partitions=kafka["partitions"],
        replication_factor=kafka["replication_factor"],
        config={
            "retention.ms": str(retention_ms),
            "retention.bytes": str(kafka["retention_bytes"]),
        },
    )
    # create_topics returns a future per topic; .result() raises on failure
    admin.create_topics([new])[topic].result(timeout=30)
    print(
        f"created '{topic}': {kafka['partitions']} partitions, "
        f"replication {kafka['replication_factor']}, "
        f"retention {kafka['retention_days']}d, "
        f"max {kafka['retention_bytes'] / 1024**3:.0f} GiB/partition"
    )
    _print_retention(admin, topic)


def _print_retention(admin, topic):
    res = ConfigResource(ConfigResource.Type.TOPIC, topic)
    cfg = admin.describe_configs([res])[res].result(timeout=10)
    for key in ("retention.ms", "retention.bytes"):
        print(f"  {key} = {cfg[key].value}")


if __name__ == "__main__":
    main()

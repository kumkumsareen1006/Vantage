"""Day 12 smoke test: prove the broker round-trips messages and that keying by
title keeps a page's edits on one partition, in order.

Produces one keyed message and reads it back, then produces three messages with
the same key and shows they share a partition with consecutive offsets. Run from
the project root with the broker up:

    python scripts/kafka_smoke.py
"""
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Consumer, Producer, TopicPartition

from vantage import config

TOPIC = config.KAFKA_TOPIC
BOOTSTRAP = config.KAFKA_BOOTSTRAP


def produce(producer, key, value):
    """Produce one message, block for its ack, return (partition, offset)."""
    out = {}

    def ack(err, msg):
        if err is not None:
            raise RuntimeError(f"delivery failed: {err}")
        out["partition"] = msg.partition()
        out["offset"] = msg.offset()

    producer.produce(TOPIC, key=key.encode(), value=json.dumps(value).encode(), callback=ack)
    producer.flush(15)
    return out["partition"], out["offset"]


def read_one(partition, offset):
    """Read exactly the record at (partition, offset) and return (key, value)."""
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"smoke-{uuid.uuid4()}",
        "enable.auto.commit": False,
    })
    try:
        consumer.assign([TopicPartition(TOPIC, partition, offset)])
        msg = consumer.poll(15)
        if msg is None or msg.error():
            raise RuntimeError(f"nothing read back at p{partition}@{offset}: {msg and msg.error()}")
        return msg.key().decode(), json.loads(msg.value().decode())
    finally:
        consumer.close()


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    print(f"broker {BOOTSTRAP}, topic {TOPIC}\n")

    print("1) round-trip one message keyed by title")
    key = "Albert_Einstein"
    part, off = produce(producer, key, {"title": key, "note": "smoke one"})
    print(f"   produced key={key!r} -> partition {part}, offset {off}")
    back_key, back_val = read_one(part, off)
    print(f"   consumed back: key={back_key!r} value={back_val}")
    assert back_key == key and back_val["note"] == "smoke one"
    print("   round-trip OK\n")

    print("2) three messages, same key -> same partition, in order")
    key = "Berlin"
    seen = [produce(producer, key, {"title": key, "seq": i}) for i in range(3)]
    for i, (p, o) in enumerate(seen):
        print(f"   msg {i}: partition {p}, offset {o}")
    partitions = {p for p, _ in seen}
    offsets = [o for _, o in seen]
    assert len(partitions) == 1, f"key spread across partitions {partitions}"
    assert offsets == sorted(offsets) and offsets[-1] - offsets[0] == 2, f"offsets not consecutive: {offsets}"
    print(f"   all on partition {partitions.pop()}, consecutive offsets {offsets} OK")


if __name__ == "__main__":
    main()

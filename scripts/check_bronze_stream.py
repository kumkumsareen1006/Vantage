"""Audit the edits Bronze stream against Kafka and the producer's delivery log.

For each Kafka partition it compares three things:

  Bronze    min/max offset, row count, distinct-offset count
  Kafka     earliest / latest watermark offsets (what the broker holds)
  producer  data/stream/published.jsonl (what the producer confirmed it sent)

It PASSES only if, for every partition:
  - Bronze is contiguous: rows == max - min + 1 (no gaps)
  - no duplicate (partition, offset) landed: rows == distinct offsets
  - Bronze starts at Kafka's earliest retained offset: nothing missing off the front
  - every confirmed publish inside Bronze's offset range is present in Bronze

`lag` (unread tail still in the topic) is reported, not failed: the producer runs
continuously, so Bronze trailing the latest offset by a few is normal, not a gap.

    python scripts/check_bronze_stream.py

Exit code 0 on pass, 1 on fail. Run from the project root with the broker up.
"""
import json
import sys
import uuid
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Consumer, TopicPartition

from vantage import config
from vantage.ingest.stream_bronze import bronze_offset_stats, contiguity_report
from vantage.spark import get_spark


def kafka_watermarks(bootstrap, topic):
    """(earliest, latest) offset per partition. latest is the next offset to be
    written, so the last written offset is latest - 1."""
    c = Consumer({"bootstrap.servers": bootstrap,
                  "group.id": f"vantage-check-{uuid.uuid4().hex[:8]}",
                  "enable.auto.commit": False})
    try:
        md = c.list_topics(topic, timeout=10)
        out = {}
        for p in sorted(md.topics[topic].partitions):
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10, cached=False)
            out[p] = (low, high)
        return out
    finally:
        c.close()


def published_by_partition(path):
    """offsets confirmed by the producer, grouped by partition (one line per delivery)."""
    by_p = defaultdict(list)
    if not Path(path).exists():
        return by_p
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                r = json.loads(line)
                by_p[r["partition"]].append(r["offset"])
    return by_p


def main():
    bronze = config.LAKE["bronze_edits"]
    spark = get_spark(app_name="vantage-check-bronze-stream")
    try:
        df = spark.read.format("delta").load(str(bronze))
        report = contiguity_report(bronze_offset_stats(df))
    finally:
        spark.stop()

    watermarks = kafka_watermarks(config.KAFKA_BOOTSTRAP, config.KAFKA_TOPIC)
    published = published_by_partition(config.ROOT / "data" / "stream" / "published.jsonl")

    partitions = sorted(set(report) | set(watermarks))
    print(f"\nedits Bronze stream check  (topic {config.KAFKA_TOPIC})\n")
    header = (f"{'part':>4}  {'bronze_rows':>11}  {'min':>7}  {'max':>7}  "
              f"{'kafka_lo':>8}  {'kafka_hi':>8}  {'lag':>5}  {'pub_in_rng':>10}  checks")
    print(header)
    print("-" * len(header))

    ok = True
    totals = {"bronze": 0, "published": 0, "pub_in_range": 0}
    for p in partitions:
        r = report.get(p)
        low, high = watermarks.get(p, (None, None))
        pub = published.get(p, [])
        totals["published"] += len(pub)

        if r is None:
            # a partition Kafka has but Bronze never landed anything from
            print(f"{p:>4}  {'(none)':>11}  {'-':>7}  {'-':>7}  {low:>8}  {high:>8}  "
                  f"{'-':>5}  {'-':>10}  MISSING from Bronze")
            ok = False
            continue

        pub_in_range = sum(1 for o in pub if r["min_offset"] <= o <= r["max_offset"])
        lag = (high - 1) - r["max_offset"] if high is not None else None
        starts_at_earliest = (low is not None and r["min_offset"] == low)
        complete = (pub_in_range == r["rows"])
        totals["bronze"] += r["rows"]
        totals["pub_in_range"] += pub_in_range

        checks = []
        if not r["contiguous"]:
            checks.append("GAP")
        if not r["no_duplicates"]:
            checks.append("DUP_OFFSET")
        if not starts_at_earliest:
            checks.append(f"MISSING_FRONT(min={r['min_offset']} lo={low})")
        if not complete:
            checks.append(f"PUB_MISMATCH(bronze={r['rows']} pub_in_rng={pub_in_range})")
        verdict = "ok" if not checks else " ".join(checks)
        if checks:
            ok = False

        print(f"{p:>4}  {r['rows']:>11,}  {r['min_offset']:>7}  {r['max_offset']:>7}  "
              f"{low:>8}  {high:>8}  {lag:>5}  {pub_in_range:>10,}  {verdict}")

    print("-" * len(header))
    print(f"totals: bronze {totals['bronze']:,} rows  |  published.jsonl {totals['published']:,} lines  "
          f"|  published within Bronze range {totals['pub_in_range']:,}")
    print(f"\n{'PASS' if ok else 'FAIL'}\n")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

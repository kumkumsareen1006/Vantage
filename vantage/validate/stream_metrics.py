"""Per-run streaming metrics for the run_metrics table (Day 18).

Three things, each sourced differently and documented at its function:
  - throughput (rows in, input/processed rows per second, batch duration) from the
    StreamingQuery's own progress reports;
  - Kafka consumer lag per partition (latest topic offset minus what Bronze committed)
    from the broker's watermark offsets and Bronze's landed offsets;
  - Silver freshness (now minus the newest event time it holds) from the Silver table.

The throughput helpers are pure (plain numbers in, dict out) so they unit-test without
Spark or a broker.
"""
import math
from statistics import mean


def _finite(x):
    """A finite float, or None. Spark reports the per-batch rate as NaN on the first
    batch (no prior trigger to measure against); NaN/inf/None all collapse to None."""
    if x is None:
        return None
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def batch_record(num_input_rows, input_rps, processed_rps, batch_duration_ms):
    """One micro-batch's numbers, as read off a StreamingQueryProgress."""
    return {"rows": int(num_input_rows or 0),
            "input_rps": _finite(input_rps),
            "processed_rps": _finite(processed_rps),
            "duration_ms": _finite(batch_duration_ms)}


def summarize(records):
    """Aggregate per-batch records into one per-run row. rows_in sums the non-empty
    batches; the rates and batch duration are the mean over the batches that reported a
    finite value. Everything is None/0 when no real batch ran (an idle drain)."""
    nonempty = [r for r in records if r["rows"] > 0]

    def avg(key):
        vals = [r[key] for r in nonempty if r[key] is not None]
        return round(mean(vals), 3) if vals else None

    return {"rows_in": sum(r["rows"] for r in nonempty),
            "batches": len(nonempty),
            "input_rows_per_sec": avg("input_rps"),
            "processed_rows_per_sec": avg("processed_rps"),
            "batch_duration_ms": avg("duration_ms")}


def _field(p, key):
    # recentProgress is a list of dicts in this PySpark build, while the listener hands
    # out StreamingQueryProgress objects -- read either shape.
    return p.get(key) if isinstance(p, dict) else getattr(p, key, None)


def _batch_duration_ms(p):
    # total wall time of the micro-batch. In the dict form it's durationMs.triggerExecution;
    # the object form exposes the same value as .batchDuration.
    d = _field(p, "durationMs")
    if isinstance(d, dict) and d.get("triggerExecution") is not None:
        return d["triggerExecution"]
    return _field(p, "batchDuration")


def summarize_query(query):
    """summarize() over a StreamingQuery's recentProgress. recentProgress is read
    synchronously from the query object, so it holds the final batch that an async
    StreamingQueryListener can still be dispatching when the run ends."""
    return summarize([batch_record(_field(p, "numInputRows"), _field(p, "inputRowsPerSecond"),
                                   _field(p, "processedRowsPerSecond"), _batch_duration_ms(p))
                      for p in query.recentProgress])


# --- Kafka lag (needs the broker) and Silver freshness (needs Spark) ----------

def kafka_watermarks(bootstrap, topic, timeout=10):
    """(low, high) offset per partition from the broker. high is the next offset to be
    written, so lag against a committed offset c is high - c."""
    import uuid

    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": bootstrap,
                  "group.id": f"vantage-metrics-{uuid.uuid4().hex[:8]}",
                  "enable.auto.commit": False})
    try:
        md = c.list_topics(topic, timeout=timeout)
        out = {}
        for p in sorted(md.topics[topic].partitions):
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout, cached=False)
            out[p] = (low, high)
        return out
    finally:
        c.close()


def bronze_committed_offsets(bronze_df):
    """Per partition, the next Kafka offset Bronze needs = max landed offset + 1. Bronze
    lands every consumed offset contiguously, so this is what the checkpoint committed."""
    from pyspark.sql import functions as F
    rows = bronze_df.groupBy("partition").agg(F.max("offset").alias("mx")).collect()
    return {r["partition"]: r["mx"] + 1 for r in rows if r["mx"] is not None}


def kafka_lag(bootstrap, topic, bronze_committed):
    """Per-partition Kafka lag for the Bronze consumer: latest topic offset minus what
    Bronze committed. bronze_committed maps partition -> next needed offset. Returns
    {"per_partition": {p: lag}, "total": sum}. The producer keeps publishing, so a small
    positive lag is the normal unread tail, not loss."""
    hw = kafka_watermarks(bootstrap, topic)
    per = {}
    for p, (low, high) in hw.items():
        committed = bronze_committed.get(p, low)
        per[p] = max(high - committed, 0)
    return {"per_partition": per, "total": sum(per.values())}


def silver_freshness_seconds(silver_df, source="kafka"):
    """now - the newest event time in Silver, in seconds: how stale the freshest row is.
    Restricted to streamed rows (source='kafka'); the Day 17 capture backfill is historical
    Oct data and would swamp this. Computed with unix_timestamp so it's timezone-independent.
    None when there are no such rows yet."""
    from pyspark.sql import functions as F
    df = silver_df
    if source is not None and "source" in df.columns:
        df = df.where(F.col("source") == source)
    r = df.select((F.unix_timestamp(F.current_timestamp())
                   - F.unix_timestamp(F.max("event_time"))).alias("fresh")).first()
    return None if r is None or r["fresh"] is None else float(r["fresh"])

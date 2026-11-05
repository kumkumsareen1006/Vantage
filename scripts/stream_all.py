"""Run both edit streams in one Spark process: Kafka -> Bronze and Bronze -> Silver.

Same checkpoints and tables as scripts/stream_bronze.py and scripts/stream_silver.py,
just driven from one driver -- the shape used for the Day 16 restart test. 2g driver
(the batches are small).

    python scripts/stream_all.py            # continuous, both queries, until Ctrl-C
    python scripts/stream_all.py --once     # drain Kafka -> Bronze, then Bronze -> Silver

Continuous runs both queries at once (Bronze feeds Silver live). --once runs them in
sequence -- Bronze to completion, then Silver -- so Silver sees all of Bronze.
"""
import argparse
import json
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql.streaming import StreamingQueryListener

from vantage import config
from vantage.ingest.stream_bronze import kafka_source, to_bronze
from vantage.observability import RunLogger, setup_logging
from vantage.pipeline import write_stream_metrics
from vantage.spark import get_spark
from vantage.transform.silver_stream import (bronze_stream, load_salt,
                                             process_batch, silver_input)
from vantage.validate.stream_metrics import (bronze_committed_offsets, kafka_lag,
                                             silver_freshness_seconds, summarize_query)

BRONZE_QUERY = "edits_bronze"
SILVER_QUERY = "edits_silver"


class BronzeBatchLogger(StreamingQueryListener):
    """One log line per non-empty Bronze micro-batch (rows + offset ranges). The Silver
    query logs itself from its foreachBatch, so this ignores everything but Bronze."""

    def __init__(self, rlog):
        self.rlog, self.batches, self.rows = rlog, 0, 0

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        if p.name != BRONZE_QUERY or p.numInputRows == 0:
            return
        self.batches += 1
        self.rows += p.numInputRows
        src = p.sources[0]
        self.rlog.event("bronze_batch", batch_id=p.batchId, rows=p.numInputRows,
                        start_offset=src.startOffset, end_offset=src.endOffset)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


def finalize_metrics(spark, rlog, run_id, started_at, mode, qb, qs, totals, drift_fields,
                     bronze_path, silver_path, metrics_path):
    """One run_metrics row per stream stage (not per micro-batch), while the session is
    still alive. Throughput comes from each query's recentProgress; Kafka lag from the
    broker vs Bronze's landed offsets; freshness from Silver. Returns the two summaries."""
    now = datetime.now(timezone.utc)
    seconds = round((now - started_at).total_seconds(), 3)
    bronze_sum = summarize_query(qb)
    silver_sum = summarize_query(qs)

    # Kafka lag is the Bronze consumer's; a broker hiccup must not sink the whole write.
    lag_json = None
    try:
        bronze_df = spark.read.format("delta").load(str(bronze_path))
        lag = kafka_lag(config.KAFKA_BOOTSTRAP, config.KAFKA_TOPIC,
                        bronze_committed_offsets(bronze_df))
        lag_json = json.dumps(lag)
    except Exception as e:
        rlog.event("kafka_lag_unavailable", error=f"{type(e).__name__}: {e}".splitlines()[0][:200])

    freshness = silver_freshness_seconds(spark.read.format("delta").load(str(silver_path)))

    base = dict(run_id=run_id, started_at=started_at, finished_at=now, seconds=seconds,
                status="success", mode=mode)
    bronze_row = {**base, "stage": "stream:bronze", **bronze_sum,
                  "rows_out": bronze_sum["rows_in"], "rows_quarantined": 0,
                  "kafka_lag": lag_json}
    silver_row = {**base, "stage": "stream:silver", **silver_sum,
                  "rows_out": totals["inserted"], "rows_quarantined": totals["quar_inserted"],
                  "freshness_seconds": freshness, "drift_events": totals["drift_events"]}
    write_stream_metrics(spark, [bronze_row, silver_row], metrics_path)

    rlog.event("stream_metrics_written",
               bronze_rows_in=bronze_sum["rows_in"], silver_inserted=totals["inserted"],
               kafka_lag=lag_json, freshness_seconds=freshness,
               drift_events=totals["drift_events"],
               drift_fields=(sorted(drift_fields) or None))
    return bronze_sum, silver_sum


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true",
                    help="drain Bronze from Kafka, then Silver from Bronze, then stop")
    ap.add_argument("--interval", default="30 seconds")
    args = ap.parse_args(argv)

    salt = load_salt()  # fail before Spark starts if the salt is missing

    run_id = uuid.uuid4().hex
    logger = setup_logging(config.ROOT / "logs" / "stream_all.jsonl", name="vantage.stream_all")
    rlog = RunLogger(logger, run_id)

    bronze = config.LAKE["bronze_edits"]
    silver = config.LAKE["silver_edits"]
    quar = config.LAKE["quarantine_edits"]
    ckpt_b = config.CHECKPOINTS["edits_bronze"]
    ckpt_s = config.CHECKPOINTS["edits_silver"]

    spark = get_spark(app_name="vantage-stream-all", driver_memory="2g")
    # keep enough per-query progress for the end-of-run metrics to aggregate every batch
    spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "1000")
    listener = BronzeBatchLogger(rlog)
    spark.streams.addListener(listener)

    totals = {"batches": 0, "clean_in": 0, "inserted": 0, "quar_in": 0,
              "quar_inserted": 0, "drift_events": 0}
    drift_fields = set()

    def on_batch(batch_df, batch_id):
        stats = process_batch(batch_df, silver, quar)
        totals["batches"] += 1
        for k in ("clean_in", "inserted", "quar_in", "quar_inserted", "drift_events"):
            totals[k] += stats[k]
        drift_fields.update(stats["drift_fields"])
        if stats["drift_events"]:
            rlog.warning("schema_drift", batch_id=batch_id,
                         drift_events=stats["drift_events"], new_fields=stats["drift_fields"])
        rlog.event("silver_batch", batch_id=batch_id, **stats)

    def start_bronze(trigger):
        return (to_bronze(kafka_source(spark, config.KAFKA_BOOTSTRAP, config.KAFKA_TOPIC))
                .writeStream.format("delta").outputMode("append").queryName(BRONZE_QUERY)
                .option("checkpointLocation", str(ckpt_b)).partitionBy("kafka_date")
                .trigger(**trigger).start(str(bronze)))

    def start_silver(trigger):
        return (silver_input(bronze_stream(spark, bronze), salt)
                .writeStream.foreachBatch(on_batch).outputMode("append").queryName(SILVER_QUERY)
                .option("checkpointLocation", str(ckpt_s)).trigger(**trigger).start())

    metrics = config.LAKE["run_metrics"]
    started_at = datetime.now(timezone.utc)
    rlog.event("run_start", mode=("once" if args.once else "continuous"),
               bronze=str(bronze), silver=str(silver))

    if args.once:
        qb = start_bronze({"availableNow": True})
        qb.awaitTermination()
        rlog.event("bronze_drained", rows=listener.rows)
        qs = start_silver({"availableNow": True})
        qs.awaitTermination()
        try:
            bronze_sum, _ = finalize_metrics(spark, rlog, run_id, started_at, "once",
                                             qb, qs, totals, drift_fields, bronze, silver, metrics)
        finally:
            spark.stop()
        rlog.event("run_summary", mode="once", bronze_rows=bronze_sum["rows_in"], **totals)
    else:
        trig = {"processingTime": args.interval}
        qb, qs = start_bronze(trig), start_silver(trig)
        stop = {"requested": False}
        previous = signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("requested", True))
        bronze_sum = {"rows_in": listener.rows}
        try:
            while not stop["requested"] and qb.isActive and qs.isActive:
                time.sleep(1)  # flag-setting SIGINT handler -> loop notices within ~1s
            if stop["requested"]:
                rlog.event("stopping", reason="sigint")
            else:
                rlog.event("stopping", reason="query_ended")  # a query died (e.g. failOnDataLoss)
            for q in (qb, qs):
                if q.isActive:
                    q.stop()
            bronze_sum, _ = finalize_metrics(spark, rlog, run_id, started_at, "continuous",
                                             qb, qs, totals, drift_fields, bronze, silver, metrics)
        finally:
            signal.signal(signal.SIGINT, previous)
            spark.stop()
            rlog.event("run_summary", mode="continuous", bronze_rows=bronze_sum["rows_in"], **totals)

    print(f"stream_all {'once' if args.once else 'continuous'}: "
          f"bronze +{bronze_sum['rows_in']} rows, silver +{totals['inserted']} rows, "
          f"{totals['quar_inserted']} quarantined, {totals['drift_events']} schema-drift events")


if __name__ == "__main__":
    main(sys.argv[1:])

"""Run the Silver edit stream: edits Bronze Delta -> Silver (+ quarantine).

Two modes, same as the Bronze stream:

    python scripts/stream_silver.py --once     # drain Bronze, then stop
    python scripts/stream_silver.py            # continuous, a batch every 30s, until Ctrl-C

Parses each Bronze value, quarantines malformed events, hashes usernames with an
off-repo salt ($VANTAGE_USER_SALT or ~/.config/vantage/user_salt -- fails loudly if
missing), and MERGEs unique meta_ids into Silver. JSON logs to logs/stream_silver.jsonl,
one line per micro-batch. Ctrl-C stops cleanly and logs a summary. Run from the root.
"""
import argparse
import signal
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vantage import config
from vantage.observability import RunLogger, setup_logging
from vantage.spark import get_spark
from vantage.transform.silver_stream import (bronze_stream, load_salt,
                                             process_batch, silver_input)


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true",
                    help="process all available Bronze, then stop (Trigger.AvailableNow)")
    ap.add_argument("--interval", default="30 seconds",
                    help="continuous-mode processing-time trigger (default: 30 seconds)")
    args = ap.parse_args(argv)

    salt = load_salt()  # fail before Spark starts if the salt is missing

    run_id = uuid.uuid4().hex
    logger = setup_logging(config.ROOT / "logs" / "stream_silver.jsonl", name="vantage.stream_silver")
    rlog = RunLogger(logger, run_id)

    bronze = config.LAKE["bronze_edits"]
    silver = config.LAKE["silver_edits"]
    quar = config.LAKE["quarantine_edits"]
    checkpoint = config.CHECKPOINTS["edits_silver"]
    trigger = {"availableNow": True} if args.once else {"processingTime": args.interval}

    rlog.event("run_start", mode=("once" if args.once else "continuous"),
               bronze=str(bronze), silver=str(silver), quarantine=str(quar),
               checkpoint=str(checkpoint), interval=(None if args.once else args.interval))

    totals = {"batches": 0, "clean_in": 0, "inserted": 0, "quar_in": 0,
              "quar_inserted": 0, "drift_events": 0}

    def on_batch(batch_df, batch_id):
        stats = process_batch(batch_df, silver, quar)
        totals["batches"] += 1
        for k in ("clean_in", "inserted", "quar_in", "quar_inserted", "drift_events"):
            totals[k] += stats[k]
        if stats["drift_events"]:
            rlog.warning("schema_drift", batch_id=batch_id,
                         drift_events=stats["drift_events"], new_fields=stats["drift_fields"])
        rlog.event("silver_batch", batch_id=batch_id, **stats,
                   layer_b_removed=stats["clean_in"] - stats["inserted"])

    spark = get_spark(app_name="vantage-stream-silver")
    query = (silver_input(bronze_stream(spark, bronze), salt)
             .writeStream
             .foreachBatch(on_batch)
             .outputMode("append")
             .option("checkpointLocation", str(checkpoint))
             .trigger(**trigger)
             .start())

    stop = {"requested": False}
    previous = signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("requested", True))
    try:
        if args.once:
            query.awaitTermination()
        else:
            while query.isActive and not stop["requested"]:
                query.awaitTermination(1)
            if stop["requested"]:
                rlog.event("stopping", reason="sigint")
                query.stop()
    finally:
        signal.signal(signal.SIGINT, previous)
        spark.stop()
        rlog.event("run_summary", mode=("once" if args.once else "continuous"), **totals,
                   layer_b_removed=totals["clean_in"] - totals["inserted"])

    print(f"{'--once' if args.once else 'continuous'}: {totals['batches']} batches, "
          f"{totals['inserted']} new Silver rows, {totals['quar_inserted']} quarantined")


if __name__ == "__main__":
    main(sys.argv[1:])

"""Run the Bronze edit stream: Kafka recentchange topic -> edits Bronze Delta.

Two modes:

    python scripts/stream_bronze.py --once     # Trigger.AvailableNow: drain all
                                                # available offsets, then stop
    python scripts/stream_bronze.py            # continuous: a micro-batch every
                                                # 30s until Ctrl-C

Needs the broker up (docker compose up -d). Structured JSON logs go to
logs/stream_bronze.jsonl, one line per micro-batch with its row count and Kafka
offset ranges. Ctrl-C in continuous mode stops the query cleanly and logs a
summary. Run from the project root.
"""
import argparse
import signal
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql.streaming import StreamingQueryListener

from vantage import config
from vantage.ingest.stream_bronze import (MAX_OFFSETS_PER_TRIGGER, kafka_source,
                                          to_bronze, write_bronze)
from vantage.observability import RunLogger, setup_logging
from vantage.spark import get_spark


class MicroBatchLogger(StreamingQueryListener):
    """Logs one line per micro-batch (rows + Kafka offset ranges) and keeps running
    totals for the end-of-run summary. Offsets are the source's raw progress JSON,
    e.g. {"recentchange":{"0":2401,...}} -- the committed end of each partition."""

    def __init__(self, rlog):
        self.rlog = rlog
        self.batches = 0
        self.total_rows = 0
        self.last_end_offset = None

    def onQueryStarted(self, event):
        self.rlog.event("stream_started", query_id=str(event.id), name=event.name)

    def onQueryProgress(self, event):
        p = event.progress
        if p.numInputRows == 0:
            return  # skip empty no-data batches so the log only shows real work
        src = p.sources[0]
        self.batches += 1
        self.total_rows += p.numInputRows
        self.last_end_offset = src.endOffset
        self.rlog.event("micro_batch", batch_id=p.batchId, rows=p.numInputRows,
                        start_offset=src.startOffset, end_offset=src.endOffset,
                        rows_per_second=(round(p.inputRowsPerSecond, 1)
                                         if p.inputRowsPerSecond and p.inputRowsPerSecond == p.inputRowsPerSecond
                                         else None))

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        self.rlog.event("stream_terminated", query_id=str(event.id), exception=event.exception)


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true",
                    help="process everything available, then stop (Trigger.AvailableNow)")
    ap.add_argument("--interval", default="30 seconds",
                    help="continuous-mode processing-time trigger (default: 30 seconds)")
    ap.add_argument("--max-offsets", type=int, default=MAX_OFFSETS_PER_TRIGGER,
                    help=f"maxOffsetsPerTrigger cap (default: {MAX_OFFSETS_PER_TRIGGER})")
    args = ap.parse_args(argv)

    run_id = uuid.uuid4().hex
    logger = setup_logging(config.ROOT / "logs" / "stream_bronze.jsonl", name="vantage.stream_bronze")
    rlog = RunLogger(logger, run_id)

    bronze = config.LAKE["bronze_edits"]
    checkpoint = config.CHECKPOINTS["edits_bronze"]
    trigger = {"availableNow": True} if args.once else {"processingTime": args.interval}

    rlog.event("run_start", mode=("once" if args.once else "continuous"),
               topic=config.KAFKA_TOPIC, bronze=str(bronze), checkpoint=str(checkpoint),
               max_offsets_per_trigger=args.max_offsets, interval=(None if args.once else args.interval))

    spark = get_spark(app_name="vantage-stream-bronze")
    listener = MicroBatchLogger(rlog)
    spark.streams.addListener(listener)

    df = to_bronze(kafka_source(spark, config.KAFKA_BOOTSTRAP, config.KAFKA_TOPIC,
                                max_offsets_per_trigger=args.max_offsets))
    query = write_bronze(df, bronze, checkpoint, trigger)

    # A SIGINT (Ctrl-C) sets a flag rather than raising: a flag-setting handler lets
    # the interrupted JVM awaitTermination() call resume and return (PEP 475) instead
    # of surfacing as a Py4JError, so the loop exits cleanly and we stop() on purpose.
    stop = {"requested": False}
    previous = signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("requested", True))

    try:
        if args.once:
            query.awaitTermination()  # Trigger.AvailableNow self-terminates when drained
        else:
            # return to Python each second so the stop flag is checked promptly
            while query.isActive and not stop["requested"]:
                query.awaitTermination(1)
            if stop["requested"]:
                rlog.event("stopping", reason="sigint")
                query.stop()
    finally:
        signal.signal(signal.SIGINT, previous)
        # stop first: it drains the listener bus, so the final micro-batch is counted
        # before the summary reads the listener's totals.
        spark.stop()
        rlog.event("run_summary", mode=("once" if args.once else "continuous"),
                   batches=listener.batches, rows=listener.total_rows,
                   last_end_offset=listener.last_end_offset)

    print(f"{'--once' if args.once else 'continuous'}: "
          f"{listener.batches} micro-batches, {listener.total_rows} rows landed")


if __name__ == "__main__":
    main(sys.argv[1:])

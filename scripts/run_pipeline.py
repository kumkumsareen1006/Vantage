"""One entry point for the batch pipeline: bronze -> silver -> gold under one
run_id, with structured JSON logging and a run_metrics Delta row per stage. A
failed stage stops the run (later stages are marked skipped) and exits non-zero.

    python scripts/run_pipeline.py                 # incremental
    python scripts/run_pipeline.py --full           # reprocess every file through Silver
    python scripts/run_pipeline.py --start 2026-10-15T00 --end 2026-10-18T00

The per-stage scripts (build_bronze.py, build_silver.py, build_gold.py) still run
standalone; this shares their transforms via vantage.pipeline.
"""
import argparse
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vantage import config
from vantage.observability import RunLogger, setup_logging
from vantage.pipeline import (execute_pipeline, run_bronze, run_gold, run_silver,
                              write_run_metrics)
from vantage.spark import get_spark


def parse_hour(s):
    return datetime.strptime(s, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true",
                    help="reprocess every file through Silver (passed through to the stages)")
    ap.add_argument("--start", default=config.WINDOW_START, help="bronze window start, e.g. 2026-10-15T00")
    ap.add_argument("--end", default=config.WINDOW_END, help="bronze window end (exclusive)")
    args = ap.parse_args(argv)
    start, end = parse_hour(args.start), parse_hour(args.end)

    run_id = uuid.uuid4().hex
    logger = setup_logging(config.ROOT / "logs" / "pipeline.jsonl")
    rlog = RunLogger(logger, run_id)

    def metrics_writer(rows):
        s = get_spark(app_name="vantage-metrics")   # own short-lived session
        try:
            write_run_metrics(s, rows, config.LAKE["run_metrics"])
        finally:
            s.stop()

    rlog.event("run_start", full=args.full, start=args.start, end=args.end)
    t0 = time.perf_counter()
    stages = [
        ("bronze", lambda: run_bronze(start, end, args.full)),
        ("silver", lambda: run_silver(args.full)),
        ("gold", lambda: run_gold(args.full)),
    ]
    code = execute_pipeline(stages, run_id, rlog, metrics_writer)
    rlog.event("run_complete", seconds=round(time.perf_counter() - t0, 1),
               status="failed" if code else "success")
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

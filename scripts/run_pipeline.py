"""One entry point for the batch pipeline: bronze -> silver -> gold under one
run_id, with structured JSON logging and a run_metrics Delta row per stage. A
failed stage stops the run (later stages are marked skipped) and exits non-zero.

    python scripts/run_pipeline.py                 # incremental
    python scripts/run_pipeline.py --full           # reprocess every file through Silver
    python scripts/run_pipeline.py --start 2026-10-15T00 --end 2026-10-18T00
    python scripts/run_pipeline.py --target azure --stages silver,gold --full   # the Databricks job

The per-stage scripts (build_bronze.py, build_silver.py, build_gold.py) still run
standalone; this shares their transforms via vantage.pipeline.
"""
import argparse
import os
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


def _this_file():
    # A Databricks python task exec()s the script in a kernel, so __file__ isn't set;
    # argv[0] is the script path there, and the cwd is its folder.
    try:
        return Path(__file__)
    except NameError:
        if sys.argv and sys.argv[0].endswith(".py"):
            return Path(sys.argv[0])
        return Path.cwd() / "run_pipeline.py"


sys.path.insert(0, str(_this_file().resolve().parents[1]))

from vantage import config
from vantage.observability import RunLogger, setup_logging
from vantage.pipeline import (execute_pipeline, run_bronze, run_gold, run_silver,
                              write_run_metrics)
from vantage.spark import get_spark, on_databricks, release


STAGES = ("bronze", "silver", "gold")


def parse_hour(s):
    return datetime.strptime(s, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)


def parse_stages(s):
    """'silver,gold' -> ('silver', 'gold'), always in pipeline order."""
    names = {x.strip() for x in s.split(",") if x.strip()}
    unknown = names - set(STAGES)
    if unknown or not names:
        raise argparse.ArgumentTypeError(f"stages must be from {','.join(STAGES)}, got {s!r}")
    return tuple(n for n in STAGES if n in names)


def parse_days(s):
    """'2026-10-15,2026-10-16' -> ('2026-10-15', '2026-10-16'), validated as dates."""
    days = tuple(sorted({x.strip() for x in s.split(",") if x.strip()}))
    try:
        for d in days:
            datetime.strptime(d, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"days must be YYYY-MM-DD, got {s!r}")
    if not days:
        raise argparse.ArgumentTypeError("--days needs at least one date")
    return days


def runtime_check(spark):
    # title normalization's first-letter upper() follows the JVM's Unicode tables
    # (Java 8 leaves Georgian ს alone, Java 17 maps it to Ს), so record both
    java = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")
    upper_ka = spark.sql("select upper('\u10e1') u").first()["u"]
    return {"java_version": java, "upper_ka": f"U+{ord(upper_ka):04X}"}


def log_dir():
    # on Databricks the repo is synced workspace files; driver logs keep stderr anyway
    if on_databricks():
        return Path(tempfile.gettempdir()) / "vantage-logs"
    return config.ROOT / "logs"


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true",
                    help="reprocess every file through Silver (passed through to the stages)")
    ap.add_argument("--start", default=config.WINDOW_START, help="bronze window start, e.g. 2026-10-15T00")
    ap.add_argument("--end", default=config.WINDOW_END, help="bronze window end (exclusive)")
    ap.add_argument("--target", choices=["local", "azure"], default=config.TARGET,
                    help="where the Delta tables live (default: local, or $VANTAGE_TARGET)")
    ap.add_argument("--stages", type=parse_stages, default=STAGES,
                    help="comma-separated subset of bronze,silver,gold (default: all)")
    ap.add_argument("--days", type=parse_days, default=None,
                    help="Silver only: comma-separated file dates to process, e.g. 2026-10-15")
    args = ap.parse_args(argv)
    config.set_target(args.target)    # before any stage reads config.LAKE
    start, end = parse_hour(args.start), parse_hour(args.end)

    run_id = uuid.uuid4().hex
    logger = setup_logging(log_dir() / "pipeline.jsonl")
    rlog = RunLogger(logger, run_id)

    def metrics_writer(rows):
        s = get_spark(app_name="vantage-metrics")   # own short-lived session
        try:
            write_run_metrics(s, rows, config.LAKE["run_metrics"])
        finally:
            release(s)

    rlog.event("run_start", full=args.full, start=args.start, end=args.end, target=args.target,
               stages=",".join(args.stages),
               days=",".join(args.days) if args.days else None,
               databricks_runtime=os.environ.get("DATABRICKS_RUNTIME_VERSION"))
    if on_databricks():
        check = runtime_check(get_spark())
        print(f"runtime_check {check}", flush=True)
        rlog.event("runtime_check", **check)
    t0 = time.perf_counter()
    stages = [
        ("bronze", lambda: run_bronze(start, end, args.full)),
        ("silver", lambda: run_silver(args.full, args.days)),
        ("gold", lambda: run_gold(args.full)),
    ]
    stages = [st for st in stages if st[0] in args.stages]
    code = execute_pipeline(stages, run_id, rlog, metrics_writer)
    rlog.event("run_complete", seconds=round(time.perf_counter() - t0, 1),
               status="failed" if code else "success")
    return code


if __name__ == "__main__":
    # exit only on failure: inside a Databricks task even SystemExit(0) can be reported as a failed run
    code = main(sys.argv[1:])
    if code:
        sys.exit(code)

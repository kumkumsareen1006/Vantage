"""Day 11: VACUUM the Delta tables to reclaim disk held by stale versions.

OPTIMIZE compacts but deletes nothing, so every MERGE/overwrite commit leaves its old
files on disk (Silver alone held ~9 GB of them). This runs VACUUM RETAIN 24 HOURS over
Bronze, Silver, quarantine and the three Gold tables, each in its own short-lived
SparkSession with the short-retention safety check disabled for that session only. It
writes one run_metrics row per table (stage="vacuum:<table>") and prints on-disk bytes
before and after.

Standalone maintenance, like optimize_silver.py: run it on its own, never chained onto a
pipeline run. Nothing in the current snapshot of any table is touched; only version files
older than the 24-hour window go, so time travel beyond 24 hours is given up by design.

    python scripts/vacuum_tables.py
"""
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vantage import config
from vantage.observability import RunLogger, setup_logging
from vantage.pipeline import execute_pipeline, run_vacuum, write_run_metrics
from vantage.spark import get_spark

RETAIN_HOURS = 24


def tables():
    gold = Path(config.LAKE["gold"])
    return [
        ("bronze", config.LAKE["bronze"]),
        ("silver", config.LAKE["silver"]),
        ("quarantine", config.LAKE["quarantine"]),
        ("gold/top_pages_hourly", gold / "top_pages_hourly"),
        ("gold/top_pages_daily", gold / "top_pages_daily"),
        ("gold/spikes", gold / "spikes"),
    ]


def main():
    run_id = uuid.uuid4().hex
    logger = setup_logging(config.ROOT / "logs" / "pipeline.jsonl")
    rlog = RunLogger(logger, run_id)

    def metrics_writer(rows):
        s = get_spark(app_name="vantage-metrics")   # own short-lived session
        try:
            write_run_metrics(s, rows, config.LAKE["run_metrics"])
        finally:
            s.stop()

    rlog.event("vacuum_start", retain_hours=RETAIN_HOURS)
    stages = [(f"vacuum:{name}", (lambda n=name, p=path: run_vacuum(n, path=p, retain_hours=RETAIN_HOURS)))
              for name, path in tables()]
    code = execute_pipeline(stages, run_id, rlog, metrics_writer)
    rlog.event("vacuum_complete", status="failed" if code else "success")
    return code


if __name__ == "__main__":
    sys.exit(main())

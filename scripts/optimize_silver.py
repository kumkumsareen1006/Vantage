"""Day 11: compact the Silver Delta table.

A standalone maintenance command with its own SparkSession. Run it on its own,
never chained onto a --full pipeline run: that run already rewrites all of Silver,
and compaction is separate housekeeping. It records file count and size before and
after OPTIMIZE as a run_metrics row (stage="optimize") and reports the disk the old,
now-unreferenced versions still hold. It does not VACUUM, so nothing is deleted.

    python scripts/optimize_silver.py
"""
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vantage import config
from vantage.observability import RunLogger, setup_logging
from vantage.pipeline import execute_pipeline, run_optimize, write_run_metrics
from vantage.spark import get_spark


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

    rlog.event("optimize_start", table="silver")
    code = execute_pipeline([("optimize", lambda: run_optimize("silver"))],
                            run_id, rlog, metrics_writer)
    rlog.event("optimize_complete", status="failed" if code else "success")
    return code


if __name__ == "__main__":
    sys.exit(main())

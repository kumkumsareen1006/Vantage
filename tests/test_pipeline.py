"""Orchestration contract: one run_metrics row per stage, stop-on-failure with
skipped rows and a non-zero exit, and JSON log lines that all carry run_id.

Uses fake stage functions so the contract is tested without the heavy Spark jobs;
the real stages are exercised by the two end-to-end runs in the Day 10 report.
"""
import json

from vantage.observability import RunLogger, setup_logging
from vantage.pipeline import execute_pipeline, write_run_metrics


def _ok(rows_in=None, rows_out=None, rows_quarantined=None, files_processed=None):
    return lambda: {"rows_in": rows_in, "rows_out": rows_out,
                    "rows_quarantined": rows_quarantined, "files_processed": files_processed}


def _writer(spark, path):
    return lambda rows: write_run_metrics(spark, rows, str(path))


def _metrics(spark, path):
    return {r["stage"]: r for r in spark.read.format("delta").load(str(path)).collect()}


def test_one_metrics_row_per_stage_on_success(spark, tmp_path):
    rlog = RunLogger(setup_logging(tmp_path / "pipeline.jsonl"), "RUNA")
    stages = [("bronze", _ok(10, 10, 0, 2)),
              ("silver", _ok(10, 8, 1, 2)),
              ("gold", _ok(None, 5, 0, None))]
    code = execute_pipeline(stages, "RUNA", rlog, _writer(spark, tmp_path / "rm"))

    assert code == 0
    m = _metrics(spark, tmp_path / "rm")
    assert set(m) == {"bronze", "silver", "gold"}
    assert all(m[s]["status"] == "success" and m[s]["run_id"] == "RUNA" for s in m)
    assert m["silver"]["rows_quarantined"] == 1
    assert m["gold"]["rows_in"] is None and m["gold"]["files_processed"] is None
    assert all(m[s]["started_at"] is not None and m[s]["finished_at"] is not None for s in m)


def test_silver_failure_writes_failed_and_skipped_and_exits_nonzero(spark, tmp_path):
    rlog = RunLogger(setup_logging(tmp_path / "pipeline.jsonl"), "RUNB")
    gold_ran = {"v": False}

    def boom():
        raise RuntimeError("forced silver failure")

    def gold():
        gold_ran["v"] = True
        return {"rows_out": 1}

    stages = [("bronze", _ok(1, 1, 0, 1)), ("silver", boom), ("gold", gold)]
    code = execute_pipeline(stages, "RUNB", rlog, _writer(spark, tmp_path / "rm"))

    assert code == 1                   # non-zero exit on failure
    assert gold_ran["v"] is False      # gold never ran after silver failed
    m = _metrics(spark, tmp_path / "rm")
    assert m["bronze"]["status"] == "success"
    assert m["silver"]["status"] == "failed"
    assert "forced silver failure" in m["silver"]["error"]
    assert m["gold"]["status"] == "skipped"


def test_every_log_line_is_valid_json_with_run_id(spark, tmp_path):
    logfile = tmp_path / "pipeline.jsonl"
    logger = setup_logging(logfile)
    rlog = RunLogger(logger, "RUNC")
    rlog.event("run_start", full=False)
    execute_pipeline([("bronze", _ok(1, 1, 0, 1))], "RUNC", rlog, _writer(spark, tmp_path / "rm"))
    rlog.event("run_complete", status="success")
    for h in logger.handlers:
        h.flush()

    lines = [ln for ln in logfile.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert lines
    for ln in lines:
        obj = json.loads(ln)           # raises if any line isn't valid JSON
        assert obj["run_id"] == "RUNC"
        assert "event" in obj

"""Day 11: the Gold skip (rebuild only when Silver changed) and OPTIMIZE.

The skip decision and run_metrics recording are tested without the heavy Gold
build or a session stop: gold_skip_reason is pure, _latest_gold_silver_version and
optimize_table take the shared session, and execute_pipeline is driven with fake
stages. The real Gold transforms are covered by tests/test_gold.py.
"""
from datetime import datetime, timedelta, timezone

from vantage.observability import RunLogger, setup_logging
from vantage.pipeline import (_latest_gold_silver_version, _metrics_row, execute_pipeline,
                              gold_skip_reason, optimize_table, table_detail, write_run_metrics)

NOW = datetime(2026, 10, 24, 12, 0, tzinfo=timezone.utc)


def _metrics(spark, path):
    return {r["stage"]: r for r in spark.read.format("delta").load(str(path)).collect()}


# ---- gold_skip_reason (pure) ---------------------------------------------------
def test_gold_skip_reason():
    assert gold_skip_reason(full=False, silver_version=3, last_gold_version=3).endswith("(version 3)")
    assert gold_skip_reason(False, 4, 3) is None       # silver changed -> build
    assert gold_skip_reason(False, 3, None) is None    # no prior gold build -> build
    assert gold_skip_reason(True, 3, 3) is None        # --full always builds


# ---- _latest_gold_silver_version ----------------------------------------------
def test_latest_gold_silver_version_picks_newest_non_null_gold(spark, tmp_path):
    p = tmp_path / "rm"
    rows = [
        _metrics_row("r1", "gold", NOW, NOW, 1.0, "success", {"silver_version": 5}),
        _metrics_row("r2", "silver", NOW, NOW + timedelta(minutes=3), 1.0, "success", {"rows_out": 9}),
        _metrics_row("r3", "gold", NOW, NOW + timedelta(minutes=1), 1.0, "skipped", {"silver_version": 7}),
        _metrics_row("r4", "gold", NOW, NOW + timedelta(minutes=2), 1.0, "failed"),  # silver_version null
    ]
    write_run_metrics(spark, rows, str(p))
    # r4 is newest but has no version; r3 (skipped, v7) is the newest gold with one
    assert _latest_gold_silver_version(spark, p) == 7


def test_latest_gold_silver_version_none_when_absent(spark, tmp_path):
    assert _latest_gold_silver_version(spark, tmp_path / "missing") is None
    write_run_metrics(spark, [_metrics_row("r", "silver", NOW, NOW, 1.0, "success", {"rows_out": 1})],
                      str(tmp_path / "rm2"))
    assert _latest_gold_silver_version(spark, tmp_path / "rm2") is None   # no gold row yet


# ---- execute_pipeline honours a self-skip -------------------------------------
def test_self_skip_is_recorded_and_not_a_failure(spark, tmp_path):
    rlog = RunLogger(setup_logging(tmp_path / "p.jsonl"), "RUNS")
    gold_ran = {"v": False}

    def silver():
        return {"status": "skipped", "reason": "silver unchanged since last gold build (version 3)",
                "silver_version": 3}

    def gold():
        gold_ran["v"] = True
        return {"rows_out": 2}

    stages = [("bronze", lambda: {"rows_out": 1}), ("silver", silver), ("gold", gold)]
    code = execute_pipeline(stages, "RUNS", rlog, lambda rows: write_run_metrics(spark, rows, str(tmp_path / "rm")))

    assert code == 0                   # a self-skip is not a failure
    assert gold_ran["v"] is True       # a later stage still runs
    m = _metrics(spark, tmp_path / "rm")
    assert m["silver"]["status"] == "skipped"
    assert m["silver"]["reason"].endswith("(version 3)")
    assert m["silver"]["silver_version"] == 3
    assert m["gold"]["status"] == "success"


# ---- OPTIMIZE compaction ------------------------------------------------------
def test_optimize_table_reduces_file_count(spark, tmp_path):
    p = tmp_path / "t"
    for i in range(5):                 # 5 appends -> >=5 small files
        spark.range(i * 10, i * 10 + 10).write.format("delta").mode("append").save(str(p))
    before, _ = table_detail(spark, p)
    assert before >= 5

    m = optimize_table(spark, p)
    assert m["files_before"] == before
    assert 1 <= m["files_after"] < m["files_before"]     # compacted
    assert m["bytes_after"] > 0
    # old (pre-OPTIMIZE) files are still on disk until VACUUM
    assert m["old_versions_bytes"] > 0
    assert spark.read.format("delta").load(str(p)).count() == 50   # data unchanged

"""Pipeline orchestration: bronze -> silver -> gold under one run_id, with a
run_metrics Delta table and stop-on-failure.

The stage functions reuse the same transforms as the standalone build scripts
(build_silver.py shares silver_day_loop here, so there is one implementation).
Row counts come from Delta commit operationMetrics where possible instead of
extra count() scans -- see rows_written_since and each stage's return comment.
"""
import time
from collections import defaultdict
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

from vantage import config
from vantage.ingest.bronze import land_files
from vantage.ingest.pageviews import filename_for_start_hour, iter_start_hours
from vantage.spark import get_spark
from vantage.transform.gold import spike_scores, top_pages_daily, top_pages_hourly
from vantage.transform.silver import (aggregate_collisions, files_to_process,
                                      merge_silver, silver_source_files)
from vantage.validate.pageviews import (annotate_bronze, clean_rows,
                                        quarantine_rows, write_quarantine_for_files)


# ---------------------------------------------------------------- Delta metrics
def table_version(spark, path):
    """Current Delta version of a table, or -1 if it doesn't exist yet."""
    path = str(path)
    if not DeltaTable.isDeltaTable(spark, path):
        return -1
    return DeltaTable.forPath(spark, path).history(1).select("version").first()["version"]


def rows_written_since(spark, path, since_version):
    """Rows this run wrote to a Delta table, from commit operationMetrics rather
    than a scan: WRITE -> numOutputRows; MERGE -> inserted + updated. Reading the
    history is a metadata op, not a data read."""
    path = str(path)
    if not DeltaTable.isDeltaTable(spark, path):
        return 0
    total = 0
    for r in DeltaTable.forPath(spark, path).history().where(F.col("version") > since_version).collect():
        m = r["operationMetrics"] or {}
        if r["operation"] == "MERGE":
            total += int(m.get("numTargetRowsInserted") or 0) + int(m.get("numTargetRowsUpdated") or 0)
        else:
            total += int(m.get("numOutputRows") or 0)
    return total


# Each stage runs in its own SparkSession: one driver process, but the session is
# stopped between stages so a heavy stage (Silver's 142M MERGE) can't leave the JVM
# heap-pressured for the next one (Gold OOM'd the shared 4g driver this way). Each
# stage then behaves like the standalone build script it mirrors.

# ---------------------------------------------------------------- Bronze
def run_bronze(start, end, full=False):
    spark = get_spark(app_name="vantage-bronze")
    try:
        bronze = config.LAKE["bronze"]
        paths = []
        for h in iter_start_hours(start, end):
            name, _ = filename_for_start_hour(h)
            p = config.RAW_PAGEVIEWS / name
            if p.exists():
                paths.append(p)
        v0 = table_version(spark, bronze)
        landed, _ = land_files(spark, paths, bronze)  # already-landed files are skipped
        rows = rows_written_since(spark, bronze, v0)
        # append pass-through after the scope filter: every scoped line read is landed,
        # so rows_in == rows_out (Delta WRITE numOutputRows). Bronze never quarantines.
        return {"rows_in": rows, "rows_out": rows, "rows_quarantined": 0,
                "files_processed": len(landed)}
    finally:
        spark.stop()


# ---------------------------------------------------------------- Silver
def silver_todo(spark, bronze, silver_path, full):
    # (file_date, source_file) from Bronze so days line up with the real partitions
    pairs = bronze.select("file_date", "source_file").distinct().collect()
    day_of = {r["source_file"]: r["file_date"] for r in pairs}
    all_day_files = defaultdict(set)
    for r in pairs:
        all_day_files[r["file_date"]].add(r["source_file"])
    all_files = sorted(day_of)
    done = silver_source_files(spark, silver_path)
    todo = files_to_process(all_files, done, full)
    return todo, day_of, all_day_files, all_files, done


def silver_day_loop(spark, bronze, silver_path, quar_path, todo, day_of, all_day_files):
    """Parse/aggregate/MERGE/quarantine the todo files, grouped by Bronze file_date
    (one hour per file, so per-day aggregation is byte-identical to per-file).
    Shared by build_silver.py and run_silver. Returns per-day stats and the clean
    total; the caller reads Delta operationMetrics for rows written."""
    todo_by_day = defaultdict(list)
    for f in todo:
        todo_by_day[day_of[f]].append(f)
    days = sorted(todo_by_day)
    per_day, total_clean = [], 0
    for day in days:
        day_files = sorted(todo_by_day[day])
        sub = bronze.where(F.col("file_date") == F.lit(day))       # partition pruning
        if set(day_files) != all_day_files[day]:                    # incremental subset
            sub = sub.where(F.col("source_file").isin(day_files))
        ann = annotate_bronze(sub).persist()
        clean = clean_rows(ann)
        batch = aggregate_collisions(clean)
        clean_n = clean.count()        # first action: the single parse, now cached
        agg_n = batch.count()
        merge_silver(spark, batch, silver_path)
        quar = quarantine_rows(ann)
        write_quarantine_for_files(quar, quar_path, day_files)
        ann.unpersist()
        total_clean += clean_n
        per_day.append({"day": str(day), "files": len(day_files),
                        "clean": clean_n, "collisions": clean_n - agg_n})
    return {"total_clean": total_clean, "per_day": per_day,
            "days": [str(d) for d in days], "files_processed": len(todo)}


def run_silver(full=False):
    spark = get_spark(app_name="vantage-silver")
    try:
        # a day's batch is ~24x a file; keep the file-sized shape that fits 4g (see build_silver.py)
        spark.conf.set("spark.sql.files.maxPartitionBytes", str(32 * 1024 * 1024))
        spark.conf.set("spark.sql.shuffle.partitions", "200")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        bronze = spark.read.format("delta").load(str(config.LAKE["bronze"]))
        silver_path, quar_path = config.LAKE["silver"], config.LAKE["quarantine"]
        todo, day_of, all_day_files, _, _ = silver_todo(spark, bronze, silver_path, full)
        sv0 = table_version(spark, silver_path)
        qv0 = table_version(spark, quar_path)
        info = silver_day_loop(spark, bronze, silver_path, quar_path, todo, day_of, all_day_files)
        # rows_in is the cleaned-row count the loop already computed (not an extra scan);
        # rows_out is MERGE inserted+updated and rows_quarantined the quarantine WRITE, both
        # from operationMetrics.
        return {"rows_in": info["total_clean"],
                "rows_out": rows_written_since(spark, silver_path, sv0),
                "rows_quarantined": rows_written_since(spark, quar_path, qv0),
                "files_processed": info["files_processed"]}
    finally:
        spark.stop()


# ---------------------------------------------------------------- Gold
def _overwrite(df, path):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(path))


def run_gold(full=False):
    spark = get_spark(app_name="vantage-gold")
    try:
        spark.conf.set("spark.sql.files.maxPartitionBytes", str(32 * 1024 * 1024))
        spark.conf.set("spark.sql.shuffle.partitions", "400")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        gold = config.LAKE["gold"]
        silver = spark.read.format("delta").load(str(config.LAKE["silver"]))
        outs = {"top_pages_hourly": top_pages_hourly(silver),
                "top_pages_daily": top_pages_daily(silver),
                "spikes": spike_scores(silver)}
        rows_out = 0
        for name, df in outs.items():
            p = gold / name
            v0 = table_version(spark, p)
            _overwrite(df, p)
            rows_out += rows_written_since(spark, p, v0)
        # gold reads the whole Silver table (no single rows_in); rows_out is the sum of
        # the three Gold tables' WRITE numOutputRows from operationMetrics.
        return {"rows_in": None, "rows_out": rows_out, "rows_quarantined": 0,
                "files_processed": None}
    finally:
        spark.stop()


# ---------------------------------------------------------------- run_metrics
_METRIC_FIELDS = ["run_id", "stage", "started_at", "finished_at", "seconds",
                  "rows_in", "rows_out", "rows_quarantined", "files_processed",
                  "status", "error"]

RUN_METRICS_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("stage", StringType()),
    StructField("started_at", TimestampType()),
    StructField("finished_at", TimestampType()),
    StructField("seconds", DoubleType()),
    StructField("rows_in", LongType()),
    StructField("rows_out", LongType()),
    StructField("rows_quarantined", LongType()),
    StructField("files_processed", IntegerType()),
    StructField("status", StringType()),
    StructField("error", StringType()),
])


def _as_long(v):
    return None if v is None else int(v)


def _metrics_row(run_id, stage, started_at, finished_at, seconds, status, m=None, error=None):
    m = m or {}
    return {"run_id": run_id, "stage": stage, "started_at": started_at,
            "finished_at": finished_at, "seconds": seconds,
            "rows_in": _as_long(m.get("rows_in")), "rows_out": _as_long(m.get("rows_out")),
            "rows_quarantined": _as_long(m.get("rows_quarantined")),
            "files_processed": (None if m.get("files_processed") is None else int(m["files_processed"])),
            "status": status, "error": error}


def write_run_metrics(spark, rows, metrics_path):
    data = [tuple(r[f] for f in _METRIC_FIELDS) for r in rows]
    (spark.createDataFrame(data, schema=RUN_METRICS_SCHEMA)
     .write.format("delta").mode("append").save(str(metrics_path)))


def execute_pipeline(stages, run_id, rlog, metrics_writer):
    """Run stages in order under one run_id. On the first failure the remaining
    stages are marked skipped and never invoked; one run_metrics row is collected
    per stage either way. Returns the process exit code (0 ok, 1 if any stage failed).

    stages: list of (name, fn) where fn() -> metrics dict (each manages its own
    SparkSession). metrics_writer(rows) persists the rows; it runs last, with its
    own session, so the metrics survive even a stage that killed its context -- and
    if the write itself can't run, the JSON log still holds the full run.
    """
    rows, failed = [], False
    for name, fn in stages:
        if failed:
            now = datetime.now(timezone.utc)
            rows.append(_metrics_row(run_id, name, now, now, 0.0, "skipped"))
            rlog.stage(name, "skipped", seconds=0.0)
            continue
        started = datetime.now(timezone.utc)
        rlog.stage(name, "start")
        t0 = time.perf_counter()
        try:
            m = fn()
            secs = round(time.perf_counter() - t0, 3)
            rows.append(_metrics_row(run_id, name, started, datetime.now(timezone.utc), secs, "success", m))
            rlog.stage(name, "success", seconds=secs, rows_in=m.get("rows_in"),
                       rows_out=m.get("rows_out"), rows_quarantined=m.get("rows_quarantined"),
                       files_processed=m.get("files_processed"))
        except Exception as e:
            secs = round(time.perf_counter() - t0, 3)
            err = f"{type(e).__name__}: {e}".splitlines()[0][:500]
            rows.append(_metrics_row(run_id, name, started, datetime.now(timezone.utc), secs, "failed", error=err))
            rlog.stage(name, "failed", seconds=secs, error=err)
            failed = True
    try:
        metrics_writer(rows)
    except Exception as e:
        rlog.event("run_metrics_write_failed", error=f"{type(e).__name__}: {e}".splitlines()[0][:500])
    return 1 if failed else 0

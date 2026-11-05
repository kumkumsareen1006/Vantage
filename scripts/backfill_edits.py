"""Day 17 backfill: captured recent-change JSONL (Oct 15-18) -> the same edits Silver.

The streamed edits start 2026-11-05; the pageview Silver is Oct 15-18. To give the Gold
join overlapping hours, this reads the captured JSONL for that window through the SAME
Silver code (schema, quarantine, normalization, user hashing, insert-only MERGE on
meta_id) and tags the rows source='capture'. Existing streamed rows are migrated to
source='kafka'. Files are split by receive date, so it reads 10-15 through 10-18 and
filters on meta.dt.

    python scripts/backfill_edits.py
"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from vantage import config
from vantage.pipeline import rows_written_since, table_version
from vantage.spark import get_spark
from vantage.transform.silver_stream import (load_salt, merge_silver, parse_and_annotate,
                                             read_capture, write_quarantine_batch)

# receive-date files covering the meta.dt window (read 10-18 too for late-received events)
CAPTURE_FILES = ["2026-10-15.jsonl", "2026-10-16.jsonl", "2026-10-17.jsonl", "2026-10-18.jsonl"]


def window_bounds():
    s = datetime.strptime(config.EDITS_BACKFILL_START, "%Y-%m-%dT%H")
    e = datetime.strptime(config.EDITS_BACKFILL_END, "%Y-%m-%dT%H")
    return s.strftime("%Y-%m-%d %H:%M:%S"), e.strftime("%Y-%m-%d %H:%M:%S")


def migrate_source(spark, path):
    """Add the source column to an existing Silver and tag its rows 'kafka' (one-time)."""
    if not DeltaTable.isDeltaTable(spark, str(path)):
        return "no silver_edits yet; it will be created with a source column"
    if "source" in spark.read.format("delta").load(str(path)).columns:
        return "source column already present"
    spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS (source STRING)")
    spark.sql(f"UPDATE delta.`{path}` SET source = 'kafka' WHERE source IS NULL")
    return "added source column; existing streamed rows tagged 'kafka'"


def main():
    salt = load_salt()
    ws, we = window_bounds()
    silver, quar = config.LAKE["silver_edits"], config.LAKE["quarantine_edits"]

    spark = get_spark(app_name="vantage-backfill-edits")
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(32 * 1024 * 1024))
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    try:
        print("migrate:", migrate_source(spark, silver))

        paths = [p for f in CAPTURE_FILES if (p := config.RAW_EDITS / f).exists()]
        print(f"reading {len(paths)} capture files for meta.dt in [{ws}, {we})")
        ann = parse_and_annotate(read_capture(spark, paths), salt, source="capture")
        clean = ann.where(F.col("reason").isNull()
                          & (F.col("event_time") >= F.lit(ws)) & (F.col("event_time") < F.lit(we)))

        v0 = table_version(spark, silver)
        t0 = time.perf_counter()
        merge_silver(clean, silver)
        inserted = rows_written_since(spark, silver, v0)

        quar_rows = ann.where(F.col("reason").isNotNull()).persist()
        qn = quar_rows.count()
        q_by_reason = {r["reason"]: r["n"] for r in
                       quar_rows.groupBy("reason").agg(F.count(F.lit(1)).alias("n")).collect()}
        if qn:
            write_quarantine_batch(quar_rows, quar, "capture")
        quar_rows.unpersist()
        secs = time.perf_counter() - t0

        s = spark.read.format("delta").load(str(silver))
        by_source = {r["source"]: r["n"] for r in
                     s.groupBy("source").agg(F.count(F.lit(1)).alias("n")).collect()}
        print(f"\ncapture rows inserted into Silver: {inserted:,}  ({secs:.1f}s)")
        print(f"Silver total now: {s.count():,}  by source: {by_source}")
        print(f"capture quarantine: {qn:,}  {q_by_reason}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""Land raw pageview files into the Bronze Delta table.

Bronze keeps the raw line untouched plus provenance (source file, the hour the
file covers, ingest time). Scope-filtering to en / en.m here is an ingest
scoping decision to keep the lake laptop-sized, NOT cleaning -- the lines are
otherwise unchanged and Silver (Day 6) does all parsing and fixing.
"""
import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from vantage.ingest.pageviews import hour_from_filename

# Kept only if the line starts with one of these (scoping, not cleaning).
SCOPE_PREFIXES = ("en ", "en.m ")


def landed_sources(spark, bronze_path) -> set:
    """source_file values already in Bronze; empty if the table doesn't exist yet."""
    if not DeltaTable.isDeltaTable(spark, str(bronze_path)):
        return set()
    rows = spark.read.format("delta").load(str(bronze_path)).select("source_file").distinct().collect()
    return {r["source_file"] for r in rows}


def _scope_filter(df):
    col = F.col("value")
    cond = col.startswith(SCOPE_PREFIXES[0])
    for p in SCOPE_PREFIXES[1:]:
        cond = cond | col.startswith(p)
    return df.filter(cond)


def land_file(spark, gz_path, bronze_path):
    name = Path(gz_path).name
    file_hour = hour_from_filename(name)  # start-of-hour UTC
    df = (
        _scope_filter(spark.read.text(str(gz_path)))
        .withColumn("source_file", F.lit(name))
        .withColumn("file_hour", F.to_timestamp(F.lit(file_hour.strftime("%Y-%m-%d %H:%M:%S"))))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("file_date", F.to_date("file_hour"))
    )
    # one append per file: a crash leaves a file either fully landed or not at all
    df.write.format("delta").mode("append").partitionBy("file_date").save(str(bronze_path))


def land_files(spark, gz_paths, bronze_path):
    """Land each file not already in Bronze. Returns (list of (name, seconds), skipped)."""
    done = landed_sources(spark, bronze_path)
    landed, skipped = [], 0
    for p in sorted(gz_paths, key=lambda q: hour_from_filename(Path(q).name)):
        name = Path(p).name
        if name in done:
            skipped += 1
            continue
        t = time.perf_counter()
        land_file(spark, p, bronze_path)
        landed.append((name, time.perf_counter() - t))
        done.add(name)
    return landed, skipped

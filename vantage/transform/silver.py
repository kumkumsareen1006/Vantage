"""Silver transform: aggregate normalization collisions and MERGE into Silver.

Silver key is (project, title, hour). Each pageview file covers exactly one
hour, so a key's rows all come from one file -- collisions are within-file and a
single file's aggregate is the final value for its keys. That makes the MERGE
safe to run per file and idempotent: matched rows are REPLACED (not added).
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F

SILVER_KEY = ["project", "title", "hour"]
_SILVER_COLS = ["project", "title", "hour", "views", "bytes", "source_file", "date"]


def aggregate_collisions(clean):
    """Sum views and bytes per key; keep source_file (constant per key). `rows_in`
    lets the caller count collisions merged away (rows_in - 1 per key)."""
    agg = clean.groupBy(*SILVER_KEY).agg(
        F.sum("views").alias("views"),
        F.sum("bytes").alias("bytes"),
        F.first("source_file").alias("source_file"),
        F.count(F.lit(1)).alias("rows_in"),
    )
    return agg.withColumn("date", F.to_date("hour"))


def merge_silver(spark, batch, silver_path):
    """MERGE one batch into Silver. Matched -> replace views/bytes/source_file
    (so a rerun can't double counts); not matched -> insert. The partition date
    is in the ON clause so Delta prunes to that day instead of scanning Silver."""
    silver_path = str(silver_path)
    batch = batch.select(*_SILVER_COLS)
    if not DeltaTable.isDeltaTable(spark, silver_path):
        batch.write.format("delta").partitionBy("date").save(silver_path)
        return
    (DeltaTable.forPath(spark, silver_path).alias("t")
     .merge(batch.alias("s"),
            "t.date = s.date AND t.project = s.project AND t.title = s.title AND t.hour = s.hour")
     .whenMatchedUpdate(set={"views": "s.views", "bytes": "s.bytes", "source_file": "s.source_file"})
     .whenNotMatchedInsertAll()
     .execute())


def silver_source_files(spark, silver_path):
    """source_file values already in Silver; empty if the table doesn't exist."""
    silver_path = str(silver_path)
    if not DeltaTable.isDeltaTable(spark, silver_path):
        return set()
    rows = spark.read.format("delta").load(silver_path).select("source_file").distinct().collect()
    return {r["source_file"] for r in rows}


def files_to_process(all_files, done, full=False):
    """Pure selection: every file on --full, else only those not already in Silver."""
    return list(all_files) if full else [f for f in all_files if f not in done]

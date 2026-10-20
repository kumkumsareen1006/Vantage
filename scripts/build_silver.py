"""Day 7: build the Silver Delta table with an idempotent per-day MERGE.

Bronze is partitioned by file_date, so we process one day at a time: filtering on
file_date prunes the read to that partition (filtering on source_file does not --
source_file isn't the partition column, so it scans the whole table). For each day
with new files we parse once, aggregate normalization collisions on
(project, title, hour), MERGE into Silver (matched -> replace views/bytes so reruns
don't double counts), and replace those files' quarantine rows. Keys never cross
files within a day (one hour = one file), so per-day aggregation is byte-identical
to per-file. Skip-already-processed stays at the file level. Run from project root:

    python scripts/build_silver.py            # incremental: only new files
    python scripts/build_silver.py --full     # reprocess every file through MERGE
"""
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from vantage import config
from vantage.spark import get_spark
from vantage.transform.silver import (aggregate_collisions, files_to_process,
                                      merge_silver, silver_source_files)
from vantage.validate.pageviews import (annotate_bronze, clean_rows,
                                        quarantine_rows, write_quarantine_for_files)


def main(full):
    spark = get_spark(app_name="vantage-silver")
    # A day's batch is ~24x a single file, so keep the aggregation/MERGE tasks at the
    # file-sized shape that works: one ~30MB parquet per map task (else the partial
    # group-by's hash map OOMs on too many title keys), ~200 reduce partitions pinned
    # on so AQE can't coalesce them back into partitions too big to aggregate at 4g.
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(32 * 1024 * 1024))
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    bronze = spark.read.format("delta").load(str(config.LAKE["bronze"]))
    silver_path, quar_path = config.LAKE["silver"], config.LAKE["quarantine"]

    # (file_date, source_file) straight from Bronze, so days line up with the actual
    # partitions rather than re-deriving a date from the filename (they differ at
    # the midnight boundary: a file's start hour, not its label, sets file_date).
    pairs = bronze.select("file_date", "source_file").distinct().collect()
    day_of = {r["source_file"]: r["file_date"] for r in pairs}
    all_day_files = defaultdict(set)
    for r in pairs:
        all_day_files[r["file_date"]].add(r["source_file"])

    all_files = sorted(day_of)
    done = silver_source_files(spark, silver_path)
    todo = files_to_process(all_files, done, full)
    todo_by_day = defaultdict(list)
    for f in todo:
        todo_by_day[day_of[f]].append(f)
    days = sorted(todo_by_day)
    print(f"bronze files: {len(all_files)}  in silver: {len(done)}  "
          f"to process: {len(todo)} files over {len(days)} day(s)  (full={full})")

    total_clean = 0
    collisions = []  # one entry per day
    t0 = time.perf_counter()
    for day in days:
        day_files = sorted(todo_by_day[day])
        sub = bronze.where(F.col("file_date") == F.lit(day))  # partition pruning
        if set(day_files) != all_day_files[day]:               # incremental subset
            sub = sub.where(F.col("source_file").isin(day_files))
        ann = annotate_bronze(sub).persist()
        clean = clean_rows(ann)
        batch = aggregate_collisions(clean)
        clean_n = clean.count()          # first action: the single parse, now cached
        agg_n = batch.count()
        merge_silver(spark, batch, silver_path)
        quar = quarantine_rows(ann)
        if quar.count() > 0:             # replace only this day's files; skip if none
            write_quarantine_for_files(quar, quar_path, day_files)
        ann.unpersist()
        total_clean += clean_n
        collisions.append(clean_n - agg_n)
        print(f"  {day}: {len(day_files)} files, clean {clean_n:,}, collisions {clean_n - agg_n}")
    secs = time.perf_counter() - t0

    silver = spark.read.format("delta").load(str(silver_path))
    silver_rows = silver.count()
    dup_keys = silver.groupBy("project", "title", "hour").count().where(F.col("count") > 1).count()
    total_views = silver.agg(F.sum("views")).first()[0]
    quar_n = (spark.read.format("delta").load(str(quar_path)).count()
              if DeltaTable.isDeltaTable(spark, str(quar_path)) else 0)

    print(f"processed {len(todo)} files over {len(days)} day(s) in {secs:.1f}s")
    print(f"clean rows in (this run): {total_clean:,}")
    if collisions:
        print(f"collisions merged: total {sum(collisions):,}  per-day min {min(collisions)}  max {max(collisions)}")
    print(f"SILVER rows: {silver_rows:,}")
    print(f"duplicate keys (count>1): {dup_keys}")
    print(f"total views: {total_views:,}")
    print(f"quarantine rows: {quar_n}")
    spark.stop()


if __name__ == "__main__":
    main(full="--full" in sys.argv[1:])

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

The per-day loop is shared with the full pipeline (vantage.pipeline.silver_day_loop).
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from vantage import config
from vantage.pipeline import silver_day_loop, silver_todo
from vantage.spark import get_spark


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

    todo, day_of, all_day_files, all_files, done = silver_todo(spark, bronze, silver_path, full)
    print(f"bronze files: {len(all_files)}  in silver: {len(done)}  "
          f"to process: {len(todo)} files  (full={full})")

    t0 = time.perf_counter()
    info = silver_day_loop(spark, bronze, silver_path, quar_path, todo, day_of, all_day_files)
    secs = time.perf_counter() - t0
    for d in info["per_day"]:
        print(f"  {d['day']}: {d['files']} files, clean {d['clean']:,}, collisions {d['collisions']}")

    silver = spark.read.format("delta").load(str(silver_path))
    silver_rows = silver.count()
    dup_keys = silver.groupBy("project", "title", "hour").count().where(F.col("count") > 1).count()
    total_views = silver.agg(F.sum("views")).first()[0]
    quar_n = (spark.read.format("delta").load(str(quar_path)).count()
              if DeltaTable.isDeltaTable(spark, str(quar_path)) else 0)

    collisions = [d["collisions"] for d in info["per_day"]]
    print(f"processed {len(todo)} files over {len(info['days'])} day(s) in {secs:.1f}s")
    print(f"clean rows in (this run): {info['total_clean']:,}")
    if collisions:
        print(f"collisions merged: total {sum(collisions):,}  per-day min {min(collisions)}  max {max(collisions)}")
    print(f"SILVER rows: {silver_rows:,}")
    print(f"duplicate keys (count>1): {dup_keys}")
    print(f"total views: {total_views:,}")
    print(f"quarantine rows: {quar_n}")
    spark.stop()


if __name__ == "__main__":
    main(full="--full" in sys.argv[1:])

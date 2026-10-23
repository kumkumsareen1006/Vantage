"""Day 9: build the Gold tables from Silver.

Three tables, each a pure function of Silver, written with mode("overwrite"):
top_pages_hourly, top_pages_daily and spikes. Overwrite is the idempotency: the
output depends only on the current Silver, so re-running replaces the table with
the identical recompute -- no appends, no MERGE, no way to double or duplicate.

    python scripts/build_gold.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from vantage import config
from vantage.spark import get_spark
from vantage.transform.gold import spike_scores, top_pages_daily, top_pages_hourly


def overwrite(df, path):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(path))


def main():
    spark = get_spark(app_name="vantage-gold")
    # the (title, hour) group-by runs over all 142M Silver rows, so use the Silver
    # build's proven shape: one ~30MB parquet per map task, reduce partitions sized
    # so each holds a tractable number of title keys at 4g
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(32 * 1024 * 1024))
    spark.conf.set("spark.sql.shuffle.partitions", "400")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

    gold = config.LAKE["gold"]
    silver = spark.read.format("delta").load(str(config.LAKE["silver"]))

    t0 = time.perf_counter()
    hourly = top_pages_hourly(silver).cache()
    h_n = hourly.count()
    overwrite(hourly, gold / "top_pages_hourly")

    daily = top_pages_daily(silver).cache()
    d_n = daily.count()
    overwrite(daily, gold / "top_pages_daily")

    scored = spike_scores(silver).cache()
    s_n = scored.count()
    grid_titles = scored.select("title").distinct().count()
    flagged = scored.where(F.col("is_spike")).cache()
    f_n = flagged.count()
    auto = flagged.where(F.col("likely_automated")).cache()
    a_n = auto.count()
    overwrite(scored, gold / "spikes")
    secs = time.perf_counter() - t0

    print(f"gold top_pages_hourly rows: {h_n:,}")
    print(f"gold top_pages_daily rows:  {d_n:,}")
    print(f"gold spikes rows (scored):  {s_n:,}   titles in grid: {grid_titles:,}   "
          f"is_spike=true: {f_n:,}   likely_automated=true: {a_n:,}")
    print(f"built gold in {secs:.1f}s")

    print(f"\nlikely_automated spikes ({a_n}):")
    for r in (auto.orderBy(F.col("views").desc(), F.col("title").asc())
              .select(F.date_format("hour", "yyyy-MM-dd HH:mm").alias("hour"), "views",
                      F.round(F.col("en_views") / F.col("views"), 3).alias("desktop_share"),
                      "en_views", "mobile_views", "prev_views", "next_views", "title")
              .collect()):
        print(f"  {r['hour']} UTC  views={r['views']:>8,}  desktop_share={r['desktop_share']}  "
              f"(en={r['en_views']:,} / mobile={r['mobile_views']:,})  "
              f"prev={r['prev_views']}  next={r['next_views']}  {r['title']}")

    print("\ntop 10 spikes by z:")
    top10 = (flagged.orderBy(F.col("z").desc(), F.col("title").asc())
             .select(F.date_format("hour", "yyyy-MM-dd HH:mm").alias("hour"),
                     F.round("z", 2).alias("z"), "views",
                     F.round("median", 1).alias("median"), F.round("mad", 1).alias("mad"),
                     F.col("jump").cast("long").alias("jump"), "title")
             .limit(10).collect())
    for r in top10:
        print(f"  {r['hour']} UTC  z={r['z']:>9}  views={r['views']:>8,}  "
              f"median={r['median']:>9}  mad={r['mad']:>7}  jump={r['jump']:>8,}  {r['title']}")
    spark.stop()


if __name__ == "__main__":
    main()

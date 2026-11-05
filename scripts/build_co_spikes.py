"""Day 17: join edits to the pageview spikes and flag co-occurring spikes.

Reads the Gold pageview spikes table and the edits Silver, computes edit counts and an
edit spike (robust z on human_edits) per (title, hour), joins on (title, hour), and writes
Gold co_spikes: title, hour, views, en/mobile views, human_edits, bot_edits, is_spike,
edit_spike, co_spike, likely_automated. Overwrite = idempotent.

    python scripts/build_co_spikes.py
"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from vantage import config
from vantage.spark import get_spark
from vantage.transform.gold import co_spikes, edit_counts, edit_spike_scores


def window_bounds():
    s = datetime.strptime(config.EDITS_BACKFILL_START, "%Y-%m-%dT%H")
    e = datetime.strptime(config.EDITS_BACKFILL_END, "%Y-%m-%dT%H")
    return s.strftime("%Y-%m-%d %H:%M:%S"), e.strftime("%Y-%m-%d %H:%M:%S")


def sanity(df, title, hour):
    r = df.where((F.col("title") == title) & (F.col("hour") == F.lit(hour))).first()
    if r is None:
        return f"{title} @ {hour}: (no row)"
    return (f"{title} @ {hour}: views={r['views']:,} human_edits={r['human_edits']} "
            f"is_spike={r['is_spike']} edit_spike={r['edit_spike']} co_spike={r['co_spike']}")


def main():
    spark = get_spark(app_name="vantage-co-spikes")
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(32 * 1024 * 1024))
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    try:
        ws, we = window_bounds()
        gold = config.LAKE["gold"]
        spikes = spark.read.format("delta").load(str(gold / "spikes"))
        silver_edits = spark.read.format("delta").load(str(config.LAKE["silver_edits"]))

        counts = edit_counts(silver_edits, ws, we)
        edits_sp = edit_spike_scores(counts)
        final = co_spikes(spikes, counts, edits_sp).cache()

        t0 = time.perf_counter()
        n = final.count()
        n_spike = final.where(F.col("is_spike")).count()
        n_edit = final.where(F.col("edit_spike")).count()
        n_co = final.where(F.col("co_spike")).count()
        final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(gold / "co_spikes"))
        secs = time.perf_counter() - t0

        print(f"co_spikes rows: {n:,}   is_spike={n_spike:,}  edit_spike={n_edit:,}  co_spike={n_co:,}  ({secs:.1f}s)")
        print(f"edit thresholds: z_min={config.EDIT_Z_MIN} edits_floor={config.EDITS_FLOOR} "
              f"min_jump={config.EDIT_MIN_JUMP} baseline={config.EDIT_BASELINE_HOURS}")

        print(f"\ntop 10 co_spikes by views:")
        for r in (final.where(F.col("co_spike"))
                  .orderBy(F.col("views").desc(), F.col("title").asc())
                  .select(F.date_format("hour", "yyyy-MM-dd HH:mm").alias("h"),
                          "views", "human_edits", "bot_edits", "title").limit(10).collect()):
            print(f"  {r['h']} UTC  views={r['views']:>8,}  human_edits={r['human_edits']:>4}  "
                  f"bot={r['bot_edits']:>3}  {r['title']}")

        print("\nsanity checks (Day 9 investigation):")
        print(" ", sanity(final, "2026_Silver_Rail_Elementary_School_shooting", "2026-10-16 00:00:00"))
        print(" ", sanity(final, "Ying", "2026-10-17 17:00:00"))
        print(" ", sanity(final, "Hôtel_de_Ville,_Limoges", "2026-10-17 20:00:00"))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""Day 6: validate all of Bronze into the typed Silver schema + quarantine.

Writes the quarantine Delta table (idempotent overwrite). Does NOT write the
Silver clean table or aggregate collisions -- that's Day 7's MERGE. Run from the
project root:

    python scripts/build_silver.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vantage import config
from vantage.spark import get_spark
from vantage.validate.pageviews import SILVER_SCHEMA, clean_bronze, write_quarantine


def main():
    spark = get_spark(app_name="vantage-silver")
    bronze = spark.read.format("delta").load(str(config.LAKE["bronze"]))
    rows_in = bronze.count()  # Delta answers this from the log

    t = time.perf_counter()
    clean, quarantine = clean_bronze(bronze)
    write_quarantine(quarantine, config.LAKE["quarantine"])          # pass 1: parse + write
    clean_n = clean.count()                                          # pass 2: parse + count
    secs = time.perf_counter() - t

    qtab = spark.read.format("delta").load(str(config.LAKE["quarantine"]))
    q_total = qtab.count()
    q_by = {r["reason"]: r["c"]
            for r in qtab.groupBy("reason").count().withColumnRenamed("count", "c").collect()}
    dropped = rows_in - q_total - clean_n  # every Bronze row is clean, quarantined, or a non-article drop

    print(f"rows in (Bronze):      {rows_in:,}")
    print(f"clean rows:            {clean_n:,}")
    print(f"dropped non-articles:  {dropped:,}")
    print(f"quarantined:           {q_total:,}  {q_by}")
    print(f"clean schema == SILVER_SCHEMA: {clean.schema == SILVER_SCHEMA}")
    print(f"runtime (clean+write):  {secs:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()

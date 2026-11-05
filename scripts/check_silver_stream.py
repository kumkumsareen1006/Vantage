"""Audit the edits Silver stream against Bronze.

Reparses Bronze the same way the stream does and checks conservation:
  - Silver row count == distinct clean meta.ids in Bronze (every clean event lands once)
  - 0 duplicate meta_ids in Silver (the dedup guarantee)
  - quarantine row count == malformed Bronze rows (nothing dropped silently), by reason
  - no raw-username columns in Silver, and user_hash is null or a 64-hex SHA-256

    python scripts/check_silver_stream.py

Exit 0 on pass, 1 on fail. Run from the project root.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from vantage import config
from vantage.spark import get_spark
from vantage.transform.silver_stream import parse_and_annotate

FORBIDDEN_COLUMNS = {"user", "comment", "parsedcomment"}
HEX64 = r"^[0-9a-f]{64}$"


def main():
    spark = get_spark(app_name="vantage-check-silver-stream")
    try:
        bronze = spark.read.format("delta").load(str(config.LAKE["bronze_edits"]))
        ann = parse_and_annotate(bronze, salt="unused-by-check").persist()

        bronze_rows = ann.count()
        clean = ann.where(F.col("reason").isNull())
        clean_rows = clean.count()
        distinct_clean = clean.select("meta_id").distinct().count()
        distinct_bronze_ids = ann.where(F.col("meta_id").isNotNull()).select("meta_id").distinct().count()
        q_by_reason = {r["reason"]: r["n"] for r in
                       ann.where(F.col("reason").isNotNull()).groupBy("reason")
                          .agg(F.count(F.lit(1)).alias("n")).collect()}
        q_candidates = sum(q_by_reason.values())
        ann.unpersist()

        silver = spark.read.format("delta").load(str(config.LAKE["silver_edits"]))
        silver_rows = silver.count()
        silver_distinct = silver.select("meta_id").distinct().count()
        silver_cols = set(silver.columns)
        bad_hashes = silver.where(F.col("user_hash").isNotNull() & ~F.col("user_hash").rlike(HEX64)).count()

        quar_path = str(config.LAKE["quarantine_edits"])
        if DeltaTable.isDeltaTable(spark, quar_path):
            quar = spark.read.format("delta").load(quar_path)
            quar_rows = quar.count()
            quar_by_reason = {r["reason"]: r["n"] for r in
                              quar.groupBy("reason").agg(F.count(F.lit(1)).alias("n")).collect()}
        else:
            quar_rows, quar_by_reason = 0, {}
    finally:
        spark.stop()

    forbidden_present = sorted(FORBIDDEN_COLUMNS & silver_cols) + sorted(c for c in silver_cols if c.startswith("log_"))

    print("\nedits Silver stream check\n")
    print(f"  Bronze rows                 {bronze_rows:,}")
    print(f"  Bronze clean rows           {clean_rows:,}")
    print(f"  Bronze distinct clean ids   {distinct_clean:,}   (expected Silver count)")
    print(f"  Bronze distinct meta.ids    {distinct_bronze_ids:,}")
    print(f"  Bronze malformed rows       {q_candidates:,}   {q_by_reason or ''}")
    print(f"  Silver rows                 {silver_rows:,}")
    print(f"  Silver distinct meta_ids    {silver_distinct:,}")
    print(f"  quarantine rows             {quar_rows:,}   {quar_by_reason or ''}")
    print(f"  conservation: Silver {silver_rows:,} + quarantine {quar_rows:,} = {silver_rows + quar_rows:,}"
          f"  vs  distinct Bronze meta.ids {distinct_bronze_ids:,}")

    checks = []
    checks.append(("Silver == distinct clean Bronze ids", silver_rows == distinct_clean))
    checks.append(("0 duplicate meta_ids in Silver", silver_rows == silver_distinct))
    checks.append(("quarantine == malformed Bronze rows", quar_rows == q_candidates))
    checks.append(("quarantine reasons match Bronze", quar_by_reason == q_by_reason))
    checks.append(("no raw-username columns in Silver", not forbidden_present))
    checks.append(("user_hash all null-or-sha256", bad_hashes == 0))

    print()
    ok = True
    for name, passed in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}")
        ok = ok and passed
    if forbidden_present:
        print(f"         forbidden columns present: {forbidden_present}")
    print(f"\n{'PASS' if ok else 'FAIL'}\n")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

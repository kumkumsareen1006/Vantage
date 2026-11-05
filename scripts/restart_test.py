"""Day 16 restart test: prove nothing was lost across a stream crash and a producer
restart. Gathers facts from the edits Bronze and Silver Delta tables and the producer's
delivery log, then runs the three checks (vantage.validate.restart.evaluate).

    python scripts/restart_test.py

Exit 0 if all three checks pass, 1 otherwise. Run from the project root.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from vantage import config
from vantage.ingest.stream_bronze import bronze_offset_stats
from vantage.spark import get_spark
from vantage.transform.silver_stream import parse_and_annotate
from vantage.validate.restart import evaluate


def load_delivery_log(path):
    rows = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def gather(spark):
    bronze = spark.read.format("delta").load(str(config.LAKE["bronze_edits"]))
    stats = bronze_offset_stats(bronze)
    bronze_offsets = {(r["partition"], r["offset"])
                      for r in bronze.select("partition", "offset").collect()}
    ann = parse_and_annotate(bronze, salt="unused-by-restart-check").persist()
    bronze_total = ann.count()
    clean = ann.where(F.col("reason").isNull())
    bronze_malformed = bronze_total - clean.count()
    bronze_ids = {r["meta_id"] for r in clean.select("meta_id").distinct().collect()}
    ann.unpersist()

    # only the streamed rows have Kafka lineage; the Day 17 capture backfill (source='capture')
    # has no offsets and is excluded from the offset-based checks.
    silver = spark.read.format("delta").load(str(config.LAKE["silver_edits"]))
    if "source" in silver.columns:
        silver = silver.where(F.col("source") == "kafka")
    silver_rows = silver.count()
    silver_ids = {r["meta_id"] for r in silver.select("meta_id").distinct().collect()}

    return dict(bronze_stats=stats, bronze_offsets=bronze_offsets,
                bronze_total_rows=bronze_total, bronze_malformed=bronze_malformed,
                bronze_ids=bronze_ids, silver_ids=silver_ids, silver_rows=silver_rows)


def main():
    delivery = load_delivery_log(config.ROOT / "data" / "stream" / "published.jsonl")
    spark = get_spark(app_name="vantage-restart-test")
    try:
        facts = gather(spark)
    finally:
        spark.stop()

    r = evaluate(delivery_rows=delivery, **facts)

    print("\nrestart test\n")
    print(f"  delivery-log rows total     {len(delivery):,}")
    print(f"  delivery-log rows in range  {r['log_rows_in_range']:,}  "
          f"(distinct meta_ids {r['log_distinct_ids']:,})")
    print(f"  Bronze rows                 {r['bronze_total_rows']:,}  "
          f"(clean {r['bronze_clean_rows']:,}, malformed {r['bronze_malformed']:,}, "
          f"distinct ids {r['bronze_distinct_ids']:,})")
    print(f"  Silver rows                 {r['silver_rows']:,}  "
          f"(distinct meta_ids {r['silver_distinct_ids']:,})")
    print()
    print(f"  upstream duplicates in delivery log : {r['log_upstream_dups']:,}")
    print(f"  upstream duplicates in Bronze       : {r['bronze_upstream_dups']:,}")
    print(f"  duplicates Silver removed           : {r['silver_removed']:,}")
    print()

    print("  Check 1 - Bronze contiguous from 0, each (partition, offset) once:")
    for p, c in r["check1"].items():
        flags = []
        if not c["from_zero"]:
            flags.append(f"min={c['min']}!=0")
        if not c["contiguous"]:
            flags.append("GAP")
        if not c["exactly_once"]:
            flags.append("DUP_OFFSET")
        print(f"      partition {p}: rows {c['rows']:,}  offsets {c['min']}..{c['max']}  "
              f"{'ok' if not flags else ' '.join(flags)}")
    print(f"    => {'PASS' if r['check1_pass'] else 'FAIL'}")

    print(f"  Check 2 - every logged meta_id is in Silver (nothing lost):")
    print(f"      lost (logged, not in Silver): {len(r['lost_ids'])}")
    print(f"    => {'PASS' if r['check2_pass'] else 'FAIL'}")

    print(f"  Check 3 - Silver meta_ids unique: "
          f"{r['silver_rows']:,} rows, {r['silver_distinct_ids']:,} distinct")
    print(f"    => {'PASS' if r['check3_pass'] else 'FAIL'}")

    # the hard-kill race: in Bronze/Silver but never logged -- not loss
    print()
    print(f"  Bronze offsets not in delivery log (producer killed pre-callback): "
          f"{len(r['bronze_extra_offsets'])}")
    if r["bronze_extra_offsets"]:
        print(f"      {r['bronze_extra_offsets'][:20]}"
              f"{' ...' if len(r['bronze_extra_offsets']) > 20 else ''}")
    print(f"      (these are in Bronze, matched by {len(r['extra_ids'])} extra Silver meta_ids; "
          f"delivered to Kafka but unlogged -- not loss)")
    if r["missing_from_bronze"]:
        print(f"  LOSS - logged offsets missing from Bronze: {r['missing_from_bronze'][:20]}")

    print(f"\n{'PASS' if r['all_pass'] else 'FAIL'}\n")
    return 0 if r["all_pass"] else 1


if __name__ == "__main__":
    sys.exit(main())

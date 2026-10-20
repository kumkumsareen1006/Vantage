"""Land downloaded pageview files into the Bronze Delta table for a UTC window.

Idempotent at both ends: the download wrapper skips files already on disk, and
this skips files already in Bronze. Run from the project root:

    python scripts/build_bronze.py 2026-10-15T00 2026-10-18T00
"""
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vantage import config
from vantage.ingest.bronze import land_files
from vantage.ingest.pageviews import filename_for_start_hour, iter_start_hours
from vantage.spark import get_spark


def parse_hour(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)


def main(argv):
    start, end = parse_hour(argv[0]), parse_hour(argv[1])
    bronze = config.LAKE["bronze"]

    paths = []
    for h in iter_start_hours(start, end):
        name, _ = filename_for_start_hour(h)
        p = config.RAW_PAGEVIEWS / name
        if p.exists():
            paths.append(p)
        else:
            print(f"missing raw (skip): {name}")

    spark = get_spark(app_name="vantage-bronze")
    t = time.perf_counter()
    landed, skipped = land_files(spark, paths, bronze)
    secs = time.perf_counter() - t

    df = spark.read.format("delta").load(str(bronze))
    rows = df.count()
    files = df.select("source_file").distinct().count()

    print(f"\nlanded {len(landed)} files, skipped {skipped} already in Bronze, in {secs:.1f}s")
    if landed:
        per = [s for _, s in landed]
        print(f"seconds per file: min {min(per):.2f}  max {max(per):.2f}  avg {sum(per) / len(per):.2f}")
    print(f"Bronze now: {rows:,} rows across {files} source files -> {bronze}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])

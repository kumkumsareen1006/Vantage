"""Clean each raw hourly pageview file (Day 2 rules) and cache the result as
one parquet per hour under data/interim/pageviews/.

One hour at a time, so the full 48-hour set of ~1.9M-row frames never sits in
memory together. Skips hours already cached, so it is safe to re-run as more
raw files arrive.

Run from the project root:
    python scripts/cache_pageviews.py                      # config window
    python scripts/cache_pageviews.py 2026-10-15T00 2026-10-17T00
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from prototype import config
from prototype.pageviews import clean_pageview_file, hour_from_filename

RAW_DIR = Path("data/raw/pageviews")
INTERIM_DIR = Path("data/interim/pageviews")


def parse_hour(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)


def parquet_path(hour) -> Path:
    return INTERIM_DIR / f"{hour:%Y-%m-%dT%H}.parquet"


def main(argv):
    start = parse_hour(argv[0]) if len(argv) > 0 else parse_hour(config.WINDOW_START)
    end = parse_hour(argv[1]) if len(argv) > 1 else parse_hour(config.WINDOW_END)
    INTERIM_DIR.mkdir(parents=True, exist_ok=True)

    cached = skipped = 0
    rows = []
    for raw in sorted(RAW_DIR.glob("pageviews-*.gz")):
        try:
            hour = hour_from_filename(raw)  # label by hour START (tz-aware UTC Timestamp)
        except ValueError:
            continue
        if not (start <= hour < end):
            continue
        out = parquet_path(hour)
        if out.exists():
            skipped += 1
            continue
        result = clean_pageview_file(raw)
        result.clean.to_parquet(out, engine="pyarrow", index=False)
        n = len(result.clean)
        rows.append(n)
        cached += 1
        print(f"cached {out.name}  rows={n:,}  quarantined={result.stats['quarantined']}")

    print(f"\ncached {cached}, skipped {skipped} already cached")
    if rows:
        print(f"clean rows per hour: min {min(rows):,}  max {max(rows):,}  total {sum(rows):,}")


if __name__ == "__main__":
    main(sys.argv[1:])

"""Thin CLI wrapper around vantage.ingest.pageviews.Ingestor.

Run from the project root:
    python scripts/download_pageviews.py                      # config window
    python scripts/download_pageviews.py 2026-10-15T00 2026-10-18T00
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from vantage import config
from vantage.ingest.pageviews import Ingestor


def parse_hour(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)


def main(argv):
    start = parse_hour(argv[0]) if len(argv) > 0 else parse_hour(config.WINDOW_START)
    end = parse_hour(argv[1]) if len(argv) > 1 else parse_hour(config.WINDOW_END)
    r = Ingestor().download_window(start, end)
    print(f"\ndownloaded {r['downloaded']}, skipped {r['skipped']} present, missing {r['missing']}")
    print(f"window files on disk: {r['downloaded'] + r['skipped']}/{r['requested']}, "
          f"total {r['total_bytes'] / 1e9:.2f} GB")
    for n in r["missing_files"]:
        print("  missing (not on mirror):", n)


if __name__ == "__main__":
    main(sys.argv[1:])

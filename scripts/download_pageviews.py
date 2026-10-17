"""Download hourly English-Wikipedia pageview files for a UTC hour window.

Files are named by the hour they END; we take the window by hour START
(end-exclusive) and map each start hour H to the file ending at H+1. Downloads
are sequential, skip files already on disk, and go through a .part temp file so
an interrupted download is never mistaken for a complete one. Checks free disk
before starting.

Run from the project root:
    python scripts/download_pageviews.py                      # config window
    python scripts/download_pageviews.py 2026-10-15T00 2026-10-17T00
"""
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# allow running as a plain script from the project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests

from prototype import config

RAW_DIR = Path("data/raw/pageviews")
BASE = "https://dumps.wikimedia.org/other/pageviews"
# contact UA required by Wikimedia; kept in sync with CLAUDE.md
USER_AGENT = "Vantage/0.1 (https://github.com/kumkumsareen1006/Vantage; kumkumsareen04@gmail.com)"
EST_BYTES_PER_FILE = 60_000_000  # Day 2 file was ~56 MB; only used for the disk check


def parse_hour(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)


def file_for_start_hour(start: datetime):
    end = start + timedelta(hours=1)
    name = f"pageviews-{end:%Y%m%d}-{end:%H}0000.gz"
    url = f"{BASE}/{end:%Y}/{end:%Y-%m}/{name}"
    return name, url


def iter_start_hours(start: datetime, end: datetime):
    h = start
    while h < end:
        yield h
        h += timedelta(hours=1)


def check_disk(n_files: int):
    need = n_files * EST_BYTES_PER_FILE
    free = shutil.disk_usage(".").free
    print(f"disk: ~{need/1e9:.1f} GB estimated for {n_files} files, {free/1e9:.1f} GB free")
    if free < need * 2:
        raise SystemExit("Not enough free disk (want 2x the estimate). Aborting.")


def download_one(url: str, dest: Path, session: requests.Session) -> str:
    tmp = dest.with_suffix(dest.suffix + ".part")
    with session.get(url, stream=True, timeout=60) as r:
        if r.status_code == 404:
            return "missing"
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    tmp.rename(dest)
    return "downloaded"


def main(argv):
    start = parse_hour(argv[0]) if len(argv) > 0 else parse_hour(config.WINDOW_START)
    end = parse_hour(argv[1]) if len(argv) > 1 else parse_hour(config.WINDOW_END)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    hours = list(iter_start_hours(start, end))
    check_disk(len(hours))

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    downloaded = skipped = missing = 0
    total_bytes = 0
    missing_files = []
    for h in hours:
        name, url = file_for_start_hour(h)
        dest = RAW_DIR / name
        if dest.exists():
            skipped += 1
            total_bytes += dest.stat().st_size
            continue
        status = download_one(url, dest, session)
        if status == "downloaded":
            downloaded += 1
            total_bytes += dest.stat().st_size
            print(f"ok   {name}")
        else:
            missing += 1
            missing_files.append(name)
            print(f"404  {name}  (not published yet)")
        time.sleep(0.5)  # be polite to the mirror

    print(f"\ndownloaded {downloaded}, skipped {skipped} already present, missing {missing}")
    print(f"window files on disk: {downloaded + skipped}/{len(hours)}, total {total_bytes/1e9:.2f} GB")
    if missing_files:
        print("not yet on the dumps mirror (re-run later to backfill):")
        for n in missing_files:
            print("  " + n)


if __name__ == "__main__":
    main(sys.argv[1:])

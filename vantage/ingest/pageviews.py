"""Download hourly English-Wikipedia pageview files.

Files are named by the hour they END; the window is taken by hour START
(end-exclusive) and each start hour H maps to the file ending at H+1.
"""
import re
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from vantage import config

BASE = "https://dumps.wikimedia.org/other/pageviews"
# contact UA required by Wikimedia; kept in sync with CLAUDE.md
USER_AGENT = "Vantage/0.1 (https://github.com/kumkumsareen1006/Vantage; kumkumsareen04@gmail.com)"
EST_BYTES_PER_FILE = 60_000_000  # Day 2 file was ~56 MB; only used for the disk check

FILENAME_RE = re.compile(r"pageviews-(\d{8})-(\d{2})0000\.gz$")


def hour_from_filename(name) -> datetime:
    """Start-of-hour UTC for a pageview file (the name marks the hour END)."""
    m = FILENAME_RE.search(Path(name).name)
    if not m:
        raise ValueError(f"not a pageview file name: {name}")
    end = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H").replace(tzinfo=timezone.utc)
    return end - timedelta(hours=1)


def filename_for_start_hour(start: datetime, base=BASE):
    end = start + timedelta(hours=1)
    name = f"pageviews-{end:%Y%m%d}-{end:%H}0000.gz"
    url = f"{base}/{end:%Y}/{end:%Y-%m}/{name}"
    return name, url


def iter_start_hours(start: datetime, end: datetime):
    h = start
    while h < end:
        yield h
        h += timedelta(hours=1)


class Ingestor:
    """Download pageview .gz files into the raw dir, one at a time, skipping any
    already present and reporting 404s rather than failing."""

    def __init__(self, raw_dir=None, user_agent=USER_AGENT, base_url=BASE):
        self.raw_dir = Path(raw_dir) if raw_dir else config.RAW_PAGEVIEWS
        self.user_agent = user_agent
        self.base_url = base_url

    def _check_disk(self, n_files):
        need = n_files * EST_BYTES_PER_FILE
        free = shutil.disk_usage(self.raw_dir if self.raw_dir.exists() else ".").free
        print(f"disk: ~{need/1e9:.1f} GB estimated for {n_files} files, {free/1e9:.1f} GB free")
        if free < need * 2:
            raise SystemExit("Not enough free disk (want 2x the estimate). Aborting.")

    def _download_one(self, url, dest, session):
        # write to .part then rename so an interrupted download is never seen as complete
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

    def download_window(self, start: datetime, end: datetime):
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        hours = list(iter_start_hours(start, end))
        self._check_disk(len(hours))

        session = requests.Session()
        session.headers["User-Agent"] = self.user_agent

        downloaded = skipped = 0
        total_bytes = 0
        missing_files = []
        for h in hours:
            name, url = filename_for_start_hour(h, self.base_url)
            dest = self.raw_dir / name
            if dest.exists():
                skipped += 1
                total_bytes += dest.stat().st_size
                continue
            if self._download_one(url, dest, session) == "downloaded":
                downloaded += 1
                total_bytes += dest.stat().st_size
                print(f"ok   {name}")
            else:
                missing_files.append(name)
                print(f"404  {name}  (not published yet)")
            time.sleep(0.5)  # be polite to the mirror

        return {
            "requested": len(hours),
            "downloaded": downloaded,
            "skipped": skipped,
            "missing": len(missing_files),
            "missing_files": missing_files,
            "total_bytes": total_bytes,
        }

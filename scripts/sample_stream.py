"""
Grab a small sample of the Wikimedia recent-changes stream and save it as JSON.

This saves events from ALL wikis, unfiltered, on purpose: Day 1 is about
seeing what the raw data looks like before deciding what to keep.

Run from the repo root:
    python scripts/sample_stream.py
"""
import json
from pathlib import Path

import requests

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# Wikimedia requires a User-Agent with contact information
USER_AGENT = "vantage/0.1 (https://github.com/kumkumsareen1006/vantage; kumkumsareen04@gmail.com)"

N_EVENTS = 500
OUT_FILE = Path("data/raw/edits/sample_500.json")


def main():
    if "YOUR_EMAIL" in USER_AGENT:
        raise SystemExit("Edit USER_AGENT at the top of this file first.")

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}

    events = []
    bad_lines = 0

    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as r:
        r.raise_for_status()

        # Use UTF-8 so titles in other languages are saved correctly
        r.encoding = "utf-8"

        for line in r.iter_lines(decode_unicode=True):
            # Only process lines containing event data
            if not line or not line.startswith("data:"):
                continue
            payload = line[len("data:"):].strip()
            try:
                events.append(json.loads(payload))
            except json.JSONDecodeError:
                bad_lines += 1
                continue
            if len(events) >= N_EVENTS:
                break

    OUT_FILE.write_text(json.dumps(events, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved {len(events)} events to {OUT_FILE} ({bad_lines} lines failed to parse)")


if __name__ == "__main__":
    main()

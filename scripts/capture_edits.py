
"""
Collects recent edits from English Wikipedia.

Only saves events from enwiki. Events are stored in
data/raw/edits/YYYY-MM-DD.jsonl, with a separate file for each UTC day.

If the connection drops, the script tries to reconnect automatically.
It saves the last event ID so it can continue after a restart, as long
as the events are still available in the stream.

Connection activity and a heartbeat every 5 minutes are logged in
logs/capture_edits.log. I'll use this on Day 5 to check for gaps.

Some events might be duplicated after a restart. That's okay for now
since duplicates will be removed in the Silver layer using meta.id.

Run from the project folder:
    caffeinate -i python scripts/capture_edits.py

Press Ctrl-C to stop.
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
 
import requests
 
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "vantage/0.1 (https://github.com/kumkumsareen1006/vantage; kumkumsareen04@gmail.com)"
WIKI = "enwiki"
 
OUT_DIR = Path("data/raw/edits")
STATE_FILE = OUT_DIR / "_last_event_id.txt"
BAD_FILE = OUT_DIR / "_unparseable.jsonl"  # raw text of events that fail to parse
LOG_FILE = Path("logs/capture_edits.log")
 
HEARTBEAT_SECONDS = 300
SAVE_ID_EVERY_N_EVENTS = 100
MAX_BACKOFF_SECONDS = 60
 
 
def setup_logging():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.Formatter.converter = time.gmtime  # log timestamps in UTC
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ %(levelname)s %(message)s",
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
    )
 
 
def load_last_id():
    if STATE_FILE.exists():
        return STATE_FILE.read_text().strip() or None
    return None
 
 
def save_last_id(event_id):
    if event_id:
        STATE_FILE.write_text(event_id)
 
 
class DailyWriter:
    """Appends lines to a file named after the current UTC date."""
 
    def __init__(self, out_dir):
        self.out_dir = out_dir
        self.day = None
        self.fh = None
 
    def write(self, line):
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if day != self.day:
            self.close()
            self.fh = open(self.out_dir / f"{day}.jsonl", "a", encoding="utf-8")
            self.day = day
            logging.info("writing to %s.jsonl", day)
        self.fh.write(line + "\n")
        self.fh.flush()
 
    def close(self):
        if self.fh:
            self.fh.close()
            self.fh = None
 
 
def handle_event(data, event_id, writer, state):
    try:
        event = json.loads(data)
    except json.JSONDecodeError:
        state["bad"] += 1
        logging.warning("unparseable event (saved to %s): %.200s", BAD_FILE.name, data)
        with open(BAD_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"raw": data, "event_id": event_id}, ensure_ascii=False) + "\n")
    else:
        if event.get("wiki") == WIKI:
            writer.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")))
            state["written"] += 1
        else:
            state["skipped"] += 1
 
    state["last_id"] = event_id
    state["since_save"] += 1
    if state["since_save"] >= SAVE_ID_EVERY_N_EVENTS:
        save_last_id(event_id)
        state["since_save"] = 0
 
 
def stream_once(writer, state):
    """Connect and read until the connection drops. Raises on network errors."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if state["last_id"]:
        headers["Last-Event-ID"] = state["last_id"]
 
    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as r:
        r.raise_for_status()
        logging.info("connected (resuming=%s)", bool(state["last_id"]))
 
        data_lines = []
        event_id = state["last_id"]
        last_heartbeat = time.monotonic()
 
        # Server-sent events format: "field: value" lines, a blank line ends an event.
        # Read raw bytes and decode each line ourselves: decoding first makes
        # iter_lines split on Unicode line separators (U+2028 etc.) that can
        # appear inside JSON strings, which cuts events in half.
        for raw in r.iter_lines():
            line = raw.decode("utf-8")
            if line.startswith(":"):
                continue  # comment / keep-alive
            if line == "":
                if data_lines:
                    handle_event("\n".join(data_lines), event_id, writer, state)
                data_lines = []
            else:
                field, _, value = line.partition(":")
                if value.startswith(" "):
                    value = value[1:]
                if field == "data":
                    data_lines.append(value)
                elif field == "id":
                    event_id = value
 
            if time.monotonic() - last_heartbeat >= HEARTBEAT_SECONDS:
                logging.info(
                    "heartbeat written=%d skipped_other_wikis=%d bad=%d reconnects=%d",
                    state["written"], state["skipped"], state["bad"], state["reconnects"],
                )
                save_last_id(state["last_id"])
                last_heartbeat = time.monotonic()
 
 
def main():
    if "YOUR_EMAIL" in USER_AGENT:
        raise SystemExit("Edit USER_AGENT at the top of this file first.")
 
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    setup_logging()
 
    state = {
        "last_id": load_last_id(),
        "written": 0, "skipped": 0, "bad": 0, "reconnects": 0, "since_save": 0,
    }
    writer = DailyWriter(OUT_DIR)
    backoff = 1
    logging.info("capture started")
 
    try:
        while True:
            started = time.monotonic()
            try:
                stream_once(writer, state)
                logging.warning("stream closed by server")
            except requests.HTTPError as e:
                logging.warning("HTTP error: %s", e)
                # If the saved id is too old for the server to resume from, start fresh.
                if state["last_id"] and e.response is not None and 400 <= e.response.status_code < 500:
                    logging.warning("dropping saved Last-Event-ID and starting from now")
                    state["last_id"] = None
            except Exception as e:  # network drops, timeouts, etc.
                logging.warning("disconnected: %s: %s", type(e).__name__, e)
 
            save_last_id(state["last_id"])
            state["reconnects"] += 1
 
            # If we stayed connected for a while, the problem was a blip: reset backoff.
            if time.monotonic() - started > 60:
                backoff = 1
            logging.info("reconnecting in %ds", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
    except KeyboardInterrupt:
        logging.info("stopped by user")
    finally:
        save_last_id(state["last_id"])
        writer.close()
        logging.info(
            "capture ended written=%d skipped_other_wikis=%d bad=%d reconnects=%d",
            state["written"], state["skipped"], state["bad"], state["reconnects"],
        )
 
 
if __name__ == "__main__":
    main()
 
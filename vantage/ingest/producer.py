"""Publish the live Wikipedia recent-changes stream to Kafka.

Replaces the Day 1 capture script: instead of writing enwiki events to daily
JSONL files, EditProducer publishes each one to the `recentchange` topic, keyed
by page title, with the raw event JSON as the value. Idempotent producing plus
Last-Event-ID resume give at-least-once delivery; Silver dedups on meta.id.
"""
import json
import os
import time
from pathlib import Path

import requests
from confluent_kafka import Producer

from vantage import config

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "Vantage/0.1 (https://github.com/kumkumsareen1006/Vantage; kumkumsareen04@gmail.com)"
WIKI = "enwiki"

HEARTBEAT_SECONDS = 300
FLUSH_INTERVAL_SECONDS = 5
MAX_BACKOFF_SECONDS = 60


def iter_sse_events(raw_lines):
    """Yield (event_id, data) per server-sent event from an iterator of raw byte
    lines. Byte-level like the Day 1 capture: decode each line ourselves so the
    stream isn't split on Unicode line separators (U+2028 etc.) that appear inside
    JSON strings and would cut events in half."""
    data_lines = []
    event_id = None
    for raw in raw_lines:
        line = raw.decode("utf-8")
        if line.startswith(":"):
            continue  # comment / keep-alive
        if line == "":
            if data_lines:
                yield event_id, "\n".join(data_lines)
            data_lines = []
        else:
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if field == "data":
                data_lines.append(value)
            elif field == "id":
                event_id = value


class EditProducer:
    def __init__(self, *, logger, state_file, delivery_log,
                 bootstrap=None, topic=None, producer=None,
                 stream_url=STREAM_URL, flush_interval=FLUSH_INTERVAL_SECONDS,
                 max_backoff=MAX_BACKOFF_SECONDS):
        self.log = logger
        self.topic = topic or config.KAFKA_TOPIC
        self.stream_url = stream_url
        self.flush_interval = flush_interval
        self.max_backoff = max_backoff

        self.state_file = Path(state_file)
        self.delivery_log_path = Path(delivery_log)
        self.unparseable_path = self.delivery_log_path.parent / "_unparseable.jsonl"
        for p in (self.state_file, self.delivery_log_path, self.unparseable_path):
            p.parent.mkdir(parents=True, exist_ok=True)

        # enable.idempotence stops a producer retry from writing the same record
        # twice; acks=all waits for the broker to persist before the callback fires.
        self.producer = producer if producer is not None else Producer({
            "bootstrap.servers": bootstrap or config.KAFKA_BOOTSTRAP,
            "enable.idempotence": True,
            "acks": "all",
            "client.id": "vantage-edit-producer",
        })

        self.counts = {"read": 0, "published": 0, "confirmed": 0,
                       "failed": 0, "bad": 0, "reconnects": 0}
        self.last_read_id = self._load_state()
        self._delivery_fh = None
        self._running = True

    # --- state (the stream resume point) -----------------------------------

    def _load_state(self):
        if self.state_file.exists():
            return self.state_file.read_text().strip() or None
        return None

    def _save_state(self, event_id):
        # atomic replace so a crash mid-write can't leave a torn id
        tmp = self.state_file.with_suffix(self.state_file.suffix + ".tmp")
        tmp.write_text(event_id)
        os.replace(tmp, self.state_file)

    def flush_and_save(self, timeout=15):
        """Block until every outstanding message is confirmed, then save the
        resume id. We save ONLY after flush reports nothing left in the queue, so
        the saved id never runs ahead of what Kafka has actually persisted: on
        restart we resume from the last fully-confirmed point. Losing nothing
        beats resuming past an unconfirmed event (duplicates are removed in Silver)."""
        remaining = self.producer.flush(timeout)
        if remaining == 0 and self.last_read_id is not None:
            self._save_state(self.last_read_id)
            return True
        if remaining:
            self.log.event("flush_incomplete", remaining=remaining)
        return False

    # --- per-message record for the Day 16 restart test --------------------

    def _write_delivery(self, meta_id, partition, offset):
        if self._delivery_fh is None:
            self._delivery_fh = open(self.delivery_log_path, "a", encoding="utf-8")
        self._delivery_fh.write(
            json.dumps({"meta_id": meta_id, "partition": partition, "offset": offset}) + "\n"
        )
        self._delivery_fh.flush()

    def _on_delivery(self, meta_id):
        def cb(err, msg):
            if err is not None:
                self.counts["failed"] += 1
                self.log.event("delivery_failed", meta_id=meta_id, error=str(err))
            else:
                self.counts["confirmed"] += 1
                self._write_delivery(meta_id, msg.partition(), msg.offset())
        return cb

    # --- reading and publishing --------------------------------------------

    def handle_event(self, event_id, data):
        self.counts["read"] += 1
        try:
            event = json.loads(data)
        except json.JSONDecodeError:
            self.counts["bad"] += 1
            with open(self.unparseable_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"raw": data, "event_id": event_id}, ensure_ascii=False) + "\n")
            self.log.event("unparseable", event_id=event_id)
            return

        if event.get("wiki") != WIKI:
            return

        title = event.get("title")
        meta_id = (event.get("meta") or {}).get("id")
        key = title.encode("utf-8") if title is not None else None
        value = data.encode("utf-8")  # publish the raw JSON unchanged

        while True:
            try:
                self.producer.produce(self.topic, key=key, value=value,
                                      on_delivery=self._on_delivery(meta_id))
                break
            except BufferError:
                # local queue full: let callbacks drain, then retry this event
                self.producer.poll(0.5)
        self.counts["published"] += 1
        self.producer.poll(0)  # serve any ready delivery callbacks

    def _stream_once(self):
        headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
        if self.last_read_id:
            headers["Last-Event-ID"] = self.last_read_id

        with requests.get(self.stream_url, headers=headers, stream=True, timeout=(10, 60)) as r:
            r.raise_for_status()
            self.log.event("connected", resuming=bool(self.last_read_id))
            last_flush = last_heartbeat = time.monotonic()

            for event_id, data in iter_sse_events(r.iter_lines()):
                if not self._running:
                    break
                self.handle_event(event_id, data)
                self.last_read_id = event_id

                now = time.monotonic()
                if now - last_flush >= self.flush_interval:
                    self.flush_and_save()
                    last_flush = now
                if now - last_heartbeat >= HEARTBEAT_SECONDS:
                    self._heartbeat()
                    last_heartbeat = now

    def _heartbeat(self):
        self.log.event("heartbeat", **self.counts)

    def run(self):
        backoff = 1
        self.log.event("producer_started", topic=self.topic, resuming=bool(self.last_read_id))
        try:
            while self._running:
                started = time.monotonic()
                try:
                    self._stream_once()
                    self.log.event("stream_closed_by_server")
                except requests.HTTPError as e:
                    status = e.response.status_code if e.response is not None else None
                    self.log.event("http_error", status=status, error=str(e))
                    # a saved id too old to resume from gets a 4xx; start fresh
                    if self.last_read_id and status is not None and 400 <= status < 500:
                        self.log.event("dropping_last_event_id")
                        self.last_read_id = None
                except Exception as e:
                    self.log.event("disconnected", error=f"{type(e).__name__}: {e}")

                if not self._running:
                    break
                self.flush_and_save()  # persist the resume point before backing off
                self.counts["reconnects"] += 1
                if time.monotonic() - started > 60:
                    backoff = 1  # a long-lived connection: the drop was a blip
                self.log.event("reconnecting", seconds=backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, self.max_backoff)
        except KeyboardInterrupt:
            self.log.event("stopped_by_user")
        finally:
            self.close()

    def close(self):
        self._running = False
        self.flush_and_save()
        self.producer.flush(15)
        if self._delivery_fh is not None:
            self._delivery_fh.close()
            self._delivery_fh = None
        self.log.event("producer_stopped", **self.counts)

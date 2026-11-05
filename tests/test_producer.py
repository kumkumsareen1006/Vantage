import json

import pytest

from vantage.ingest.producer import EditProducer, iter_sse_events

ENWIKI = '{"wiki":"enwiki","title":"Berlin","type":"edit","meta":{"id":"id-1","dt":"2026-11-04T20:00:00Z"}}'
DEWIKI = '{"wiki":"dewiki","title":"Berlin","type":"edit","meta":{"id":"id-2","dt":"2026-11-04T20:00:00Z"}}'


class FakeMsg:
    def __init__(self, partition, offset):
        self._p, self._o = partition, offset

    def partition(self):
        return self._p

    def offset(self):
        return self._o


class FakeProducer:
    """Records produce() calls; delivers callbacks on flush(). flush_remaining>0
    simulates messages still in flight (flush timed out)."""

    def __init__(self, fail=False):
        self.produced = []
        self._cbs = []
        self.fail = fail
        self.flush_remaining = 0
        self._next_offset = 0

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.produced.append({"topic": topic, "key": key, "value": value})
        self._cbs.append(on_delivery)

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        if self.flush_remaining:
            return self.flush_remaining
        for cb in self._cbs:
            if cb is None:
                continue
            if self.fail:
                cb("delivery boom", None)
            else:
                cb(None, FakeMsg(0, self._next_offset))
                self._next_offset += 1
        self._cbs = []
        return 0


class FakeLogger:
    def __init__(self):
        self.events = []

    def event(self, event, **fields):
        self.events.append((event, fields))


def make(tmp_path, producer, logger=None):
    return EditProducer(
        logger=logger or FakeLogger(),
        state_file=tmp_path / "_last_event_id.txt",
        delivery_log=tmp_path / "published.jsonl",
        producer=producer,
    )


def test_only_enwiki_is_published(tmp_path):
    ep = make(tmp_path, FakeProducer())
    ep.handle_event("e1", ENWIKI)
    ep.handle_event("e2", DEWIKI)
    assert ep.counts["read"] == 2
    assert ep.counts["published"] == 1
    assert len(ep.producer.produced) == 1


def test_key_is_title_value_is_raw_json(tmp_path):
    ep = make(tmp_path, FakeProducer())
    ep.handle_event("e1", ENWIKI)
    msg = ep.producer.produced[0]
    assert msg["key"] == b"Berlin"
    assert msg["value"] == ENWIKI.encode("utf-8")  # raw JSON, unchanged


def test_state_not_saved_until_flush_confirms(tmp_path):
    fake = FakeProducer()
    ep = make(tmp_path, fake)
    ep.last_read_id = "evt-9"

    fake.flush_remaining = 3  # messages still in flight
    assert ep.flush_and_save() is False
    assert not ep.state_file.exists()

    fake.flush_remaining = 0  # everything confirmed
    assert ep.flush_and_save() is True
    assert ep.state_file.read_text() == "evt-9"


def test_failed_delivery_logged_not_confirmed(tmp_path):
    log = FakeLogger()
    ep = make(tmp_path, FakeProducer(fail=True), logger=log)
    ep.handle_event("e1", ENWIKI)
    ep.flush_and_save()  # drives the (failing) delivery callback

    assert ep.counts["failed"] == 1
    assert ep.counts["confirmed"] == 0
    assert not ep.delivery_log_path.exists()  # nothing recorded as delivered
    assert any(e == "delivery_failed" for e, _ in log.events)


def test_confirmed_delivery_is_recorded(tmp_path):
    ep = make(tmp_path, FakeProducer())
    ep.handle_event("e1", ENWIKI)
    ep.flush_and_save()
    assert ep.counts["confirmed"] == 1
    line = json.loads(ep.delivery_log_path.read_text().strip())
    assert line == {"meta_id": "id-1", "partition": 0, "offset": 0}


def test_unparseable_event_is_saved(tmp_path):
    log = FakeLogger()
    ep = make(tmp_path, FakeProducer(), logger=log)
    ep.handle_event("bad-1", "{not valid json")
    assert ep.counts["bad"] == 1
    assert len(ep.producer.produced) == 0
    saved = json.loads(ep.unparseable_path.read_text().strip())
    assert saved == {"raw": "{not valid json", "event_id": "bad-1"}
    assert any(e == "unparseable" for e, _ in log.events)


def test_iter_sse_events_byte_level():
    lines = [b"id: 42", b'data: {"a":1}', b"", b": keepalive", b"id: 43", b'data: {"b":2}', b""]
    assert list(iter_sse_events(iter(lines))) == [("42", '{"a":1}'), ("43", '{"b":2}')]

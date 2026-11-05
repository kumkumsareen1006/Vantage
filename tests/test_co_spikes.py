import json
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F, types as T

from vantage.transform.gold import co_spikes, edit_counts, edit_spike_scores
from vantage.transform.silver_stream import parse_and_annotate

SALT = "test-salt"
UTC = timezone.utc

BRONZE_SCHEMA = T.StructType([
    T.StructField("value", T.StringType()),
    T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()),
    T.StructField("kafka_timestamp", T.TimestampType()),
])

SPIKES_SCHEMA = T.StructType([
    T.StructField("title", T.StringType()),
    T.StructField("hour", T.TimestampType()),
    T.StructField("views", T.LongType()),
    T.StructField("en_views", T.LongType()),
    T.StructField("mobile_views", T.LongType()),
    T.StructField("is_spike", T.BooleanType()),
    T.StructField("likely_automated", T.BooleanType()),
])

COUNTS_SCHEMA = T.StructType([
    T.StructField("title", T.StringType()),
    T.StructField("hour", T.TimestampType()),
    T.StructField("human_edits", T.LongType()),
    T.StructField("bot_edits", T.LongType()),
])


def event(meta_id, dt, title, type="edit", namespace=0, bot=False):
    return json.dumps({"meta": {"id": meta_id, "dt": dt}, "type": type, "namespace": namespace,
                       "title": title, "wiki": "enwiki", "bot": bot, "user": "U",
                       "revision": {"old": 1, "new": 2}, "length": {"old": 1, "new": 2}})


def raw(spark, rows):
    data = [(v, None, None, None) for v in rows]
    return spark.createDataFrame(data, BRONZE_SCHEMA)


def silver_from(spark, events):
    return parse_and_annotate(raw(spark, events), SALT, source="capture")


def test_edit_counts_filters_type_namespace_and_separates_bots(spark):
    ev = [
        event("1", "2026-10-15T10:05:00Z", "Berlin"),                       # edit ns0 human
        event("2", "2026-10-15T10:30:00Z", "Berlin", type="new"),           # new  ns0 human
        event("3", "2026-10-15T10:40:00Z", "Berlin", bot=True),             # edit ns0 bot
        event("4", "2026-10-15T10:50:00Z", "Berlin", type="log"),           # log -> excluded
        event("5", "2026-10-15T10:55:00Z", "Berlin", type="categorize"),    # categorize -> excluded
        event("6", "2026-10-15T10:58:00Z", "Berlin", namespace=2),          # ns2 -> excluded
    ]
    c = edit_counts(silver_from(spark, ev)).collect()
    assert len(c) == 1
    r = c[0]
    assert r["title"] == "Berlin" and r["human_edits"] == 2 and r["bot_edits"] == 1


def test_space_vs_underscore_title_joins(spark):
    # an edit titled "Hong Kong" normalizes to "Hong_Kong" and joins a spike on "Hong_Kong"
    counts = edit_counts(silver_from(spark, [event("1", "2026-10-15T14:10:00Z", "Hong Kong")]))
    assert counts.first()["title"] == "Hong_Kong"
    h = datetime(2026, 10, 15, 14, tzinfo=UTC)
    spikes = spark.createDataFrame([("Hong_Kong", h, 9000, 5000, 4000, True, False)], SPIKES_SCHEMA)
    edit_sp = spark.createDataFrame([("Hong_Kong", h, True)], ["title", "hour", "edit_spike"])
    out = co_spikes(spikes, counts, edit_sp).first()
    assert out["title"] == "Hong_Kong" and out["human_edits"] == 1 and out["co_spike"] is True


def test_hour_boundary_1459_joins_the_1400_views(spark):
    # 14:59 truncates to hour 14:00 (the file ending 15:00); 15:00 truncates to 15:00
    sv = silver_from(spark, [event("a", "2026-10-15T14:59:30Z", "Mars"),
                             event("b", "2026-10-15T15:00:10Z", "Mars")])
    hours = {r["title_norm"]: None for r in sv.collect()}  # touch
    counts = edit_counts(sv)
    by_hour = {r["hour"].astimezone(UTC).strftime("%H:%M"): r["human_edits"] for r in counts.collect()}
    assert by_hour == {"14:00": 1, "15:00": 1}

    h14 = datetime(2026, 10, 15, 14, tzinfo=UTC)
    spikes = spark.createDataFrame([("Mars", h14, 7000, 4000, 3000, True, False)], SPIKES_SCHEMA)
    edit_sp = spark.createDataFrame([("Mars", h14, True)], ["title", "hour", "edit_spike"])
    rows = co_spikes(spikes, counts, edit_sp).collect()
    assert len(rows) == 1  # only the 14:00 spike hour; the 15:00 edit has no spike row
    assert rows[0]["human_edits"] == 1 and rows[0]["co_spike"] is True


def test_co_spike_needs_both(spark):
    h = datetime(2026, 10, 15, 12, tzinfo=UTC)
    spikes = spark.createDataFrame([
        ("A", h, 5000, 3000, 2000, True, False),    # view spike
        ("B", h, 5000, 3000, 2000, True, False),    # view spike
        ("C", h, 5000, 3000, 2000, False, False),   # no view spike
        ("D", h, 5000, 3000, 2000, False, False),   # no view spike
    ], SPIKES_SCHEMA)
    counts = spark.createDataFrame([("A", h, 40, 0), ("C", h, 40, 0)], COUNTS_SCHEMA)
    edit_sp = spark.createDataFrame([("A", h, True), ("C", h, True), ("B", h, False)],
                                    ["title", "hour", "edit_spike"])
    out = {r["title"]: r for r in co_spikes(spikes, counts, edit_sp).collect()}
    assert out["A"]["co_spike"] is True                       # both
    assert out["B"]["co_spike"] is False                      # view only
    assert out["C"]["co_spike"] is False                      # edit only
    assert out["D"]["co_spike"] is False and out["D"]["human_edits"] == 0  # neither, no edit row


def test_edit_spike_ignores_bots(spark):
    # 24 flat baseline hours, then a burst hour. Title P: 20 human + 100 bot -> spike.
    # Title B: 0 human + 100 bot -> never clears the human floor -> no edit spike.
    base = datetime(2026, 10, 15, 0, tzinfo=UTC)
    rows = []
    for i in range(24):
        rows.append(("P", base + timedelta(hours=i), 0, 0))
    rows.append(("P", base + timedelta(hours=24), 20, 100))     # burst: human 20, bot 100
    rows.append(("B", base + timedelta(hours=24), 0, 100))      # bot-only burst
    counts = spark.createDataFrame(rows, COUNTS_SCHEMA)

    burst = base + timedelta(hours=24)
    scored = edit_spike_scores(counts, baseline=24, z_min=6.0, edits_floor=10, min_jump=10)
    got = {(r["title"], r["hour"].astimezone(UTC)): r["edit_spike"] for r in scored.collect()}
    assert got == {("P", burst): True}   # only P's human burst is scored; B never enters the grid

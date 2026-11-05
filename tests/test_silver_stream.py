import json
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import functions as F, types as T

from prototype.normalize import normalize_title_series
from vantage.transform.silver_stream import (QUARANTINE_COLUMNS, SILVER_COLUMNS,
                                             hash_user, parse_and_annotate, process_batch)

SALT = "test-salt-0123456789"

BRONZE_SCHEMA = T.StructType([
    T.StructField("value", T.StringType()),
    T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()),
    T.StructField("kafka_timestamp", T.TimestampType()),
])


def event(meta_id="id1", dt="2026-11-05T03:12:24Z", title="Berlin", user="Alice",
          type="edit", namespace=0, bot=False, drop_meta_id=False):
    meta = {"dt": dt} if drop_meta_id else {"id": meta_id, "dt": dt}
    return json.dumps({"meta": meta, "type": type, "namespace": namespace, "title": title,
                       "wiki": "enwiki", "bot": bot, "user": user,
                       "revision": {"old": 1, "new": 2}, "length": {"old": 10, "new": 20}})


def batch(spark, rows):
    """rows: list of (value, partition, offset)."""
    ts = datetime(2026, 11, 5, 3, 12, 24, tzinfo=timezone.utc)
    return spark.createDataFrame([(v, p, o, ts) for v, p, o in rows], BRONZE_SCHEMA)


def run(spark, rows, tmp_path):
    ann = parse_and_annotate(batch(spark, rows), SALT)
    stats = process_batch(ann, tmp_path / "silver", tmp_path / "quar")
    return stats


def silver(spark, tmp_path):
    return spark.read.format("delta").load(str(tmp_path / "silver"))


def quarantine(spark, tmp_path):
    return spark.read.format("delta").load(str(tmp_path / "quar"))


def test_duplicate_meta_id_in_one_batch_yields_one_row(spark, tmp_path):
    stats = run(spark, [(event("id1"), 0, 0), (event("id1"), 0, 1)], tmp_path)
    assert stats["clean_in"] == 2 and stats["inserted"] == 1
    assert silver(spark, tmp_path).count() == 1


def test_same_id_across_two_batches_yields_one_row(spark, tmp_path):
    run(spark, [(event("id1"), 0, 0)], tmp_path)
    stats2 = run(spark, [(event("id1"), 1, 5)], tmp_path)  # same id, different offset/partition
    assert stats2["inserted"] == 0
    assert silver(spark, tmp_path).count() == 1


def test_replayed_batch_adds_nothing(spark, tmp_path):
    rows = [(event("id1"), 0, 0), (event("id2"), 0, 1)]
    first = run(spark, rows, tmp_path)
    replay = run(spark, rows, tmp_path)  # identical batch again
    assert first["inserted"] == 2 and replay["inserted"] == 0
    assert silver(spark, tmp_path).count() == 2


def test_malformed_goes_to_quarantine_with_reason(spark, tmp_path):
    rows = [
        ("not valid json {", 0, 0),                              # bad_json
        (event(drop_meta_id=True), 0, 1),                        # missing_meta_id
        (event(meta_id="id3", dt="not-a-date"), 0, 2),           # bad_meta_dt
        (event(meta_id="id4", title="   "), 0, 3),               # empty_title
        (event(meta_id="good"), 0, 4),                           # clean
    ]
    run(spark, rows, tmp_path)
    assert silver(spark, tmp_path).count() == 1  # only the clean one
    q = quarantine(spark, tmp_path)
    by_offset = {r["offset"]: r["reason"] for r in q.collect()}
    assert by_offset == {0: "bad_json", 1: "missing_meta_id", 2: "bad_meta_dt", 3: "empty_title"}
    assert set(q.columns) == set(QUARANTINE_COLUMNS) | {"quarantined_at"}


def test_username_hashing(spark, tmp_path):
    # same user -> same hash; the Spark path matches the python helper
    ann = parse_and_annotate(batch(spark, [(event(user="Alice"), 0, 0)]), SALT)
    got = ann.select("user_hash").first()["user_hash"]
    assert got == hash_user(SALT, "Alice")
    # a different salt -> a different hash for the same user
    assert hash_user("another-salt", "Alice") != hash_user(SALT, "Alice")


def test_no_raw_username_columns_in_silver(spark, tmp_path):
    run(spark, [(event(user="Alice"), 0, 0)], tmp_path)
    cols = set(silver(spark, tmp_path).columns)
    assert cols == set(SILVER_COLUMNS)
    assert not ({"user", "comment", "parsedcomment"} & cols)
    assert not any(c.startswith("log_") for c in cols)
    assert "user_hash" in cols


def test_normalization_matches_prototype(spark):
    titles = ["albert einstein", "  spaces  ", "C++", "Foo_Bar", "éclair", "x", ""]
    df = spark.createDataFrame([(t,) for t in titles], ["title"])
    from vantage.normalize import normalize_title
    got = [r["n"] for r in df.withColumn("n", normalize_title(F.col("title"))).collect()]
    expected = list(normalize_title_series(pd.Series(titles, dtype="string")))
    assert got == expected

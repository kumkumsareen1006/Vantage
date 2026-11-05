"""Day 18 schema-drift handling, three cases:
  1. an unknown field -> parsing ignores it, the row is still clean and reaches Silver,
     and it is counted as drift with the new field name listed;
  2. a required field with the wrong type (namespace as a string) -> quarantine;
  3. a required field missing (meta.id) -> quarantine.
"""
import json
from datetime import datetime, timezone

from pyspark.sql import types as T

from vantage.transform.silver_stream import parse_and_annotate, process_batch

SALT = "test-salt"

BRONZE_SCHEMA = T.StructType([
    T.StructField("value", T.StringType()),
    T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()),
    T.StructField("kafka_timestamp", T.TimestampType()),
])


def event(extra=None, namespace=0, drop_meta_id=False, title="Berlin"):
    meta = {"dt": "2026-11-05T03:12:24Z"}
    if not drop_meta_id:
        meta["id"] = "id1"
    e = {"meta": meta, "type": "edit", "namespace": namespace, "title": title,
         "wiki": "enwiki", "bot": False, "user": "Alice",
         "revision": {"old": 1, "new": 2}, "length": {"old": 10, "new": 20}}
    if extra:
        e.update(extra)
    return json.dumps(e)


def ann(spark, rows):
    ts = datetime(2026, 11, 5, 3, 12, 24, tzinfo=timezone.utc)
    df = spark.createDataFrame([(v, p, o, ts) for v, p, o in rows], BRONZE_SCHEMA)
    return parse_and_annotate(df, SALT)


def test_unknown_field_is_ignored_kept_clean_and_counted(spark, tmp_path):
    raw = event(extra={"brand_new_field": 123, "another_new": "x"})
    a = ann(spark, [(raw, 0, 0)])
    r = a.first()
    assert r["reason"] is None                         # parsing ignored the extra fields
    assert r["value"] == raw                           # Bronze still holds the raw JSON, untouched
    assert sorted(r["drift_fields"]) == ["another_new", "brand_new_field"]

    stats = process_batch(a, tmp_path / "silver", tmp_path / "quar")
    assert stats["inserted"] == 1                      # the clean row reached Silver
    assert stats["drift_events"] == 1
    assert stats["drift_fields"] == ["another_new", "brand_new_field"]


def test_wrong_type_namespace_is_quarantined(spark, tmp_path):
    # namespace as a string doesn't fit the Integer schema; from_json flags the whole
    # record, and we tell a schema mismatch apart from unparseable JSON.
    a = ann(spark, [(event(namespace="0"), 0, 0), (event(namespace=0), 0, 1)])
    by_offset = {r["offset"]: r["reason"] for r in a.collect()}
    assert by_offset == {0: "schema_mismatch", 1: None}

    stats = process_batch(a, tmp_path / "silver", tmp_path / "quar")
    assert stats["inserted"] == 1 and stats["quar_in"] == 1
    q = spark.read.format("delta").load(str(tmp_path / "quar"))
    assert [r["reason"] for r in q.collect()] == ["schema_mismatch"]


def test_missing_required_field_is_quarantined(spark, tmp_path):
    a = ann(spark, [(event(drop_meta_id=True), 0, 0)])
    assert a.first()["reason"] == "missing_meta_id"
    stats = process_batch(a, tmp_path / "silver", tmp_path / "quar")
    assert stats["inserted"] == 0 and stats["quar_in"] == 1


def test_bad_json_is_not_drift(spark, tmp_path):
    # a non-object line can't report keys, so it is bad_json, never a drift miscount
    a = ann(spark, [("not valid json {", 0, 0)])
    r = a.first()
    assert r["reason"] == "bad_json"
    assert r["drift_fields"] is None  # json_object_keys null for a non-object -> never drift
    stats = process_batch(a, tmp_path / "silver", tmp_path / "quar")
    assert stats["drift_events"] == 0 and stats["quar_in"] == 1

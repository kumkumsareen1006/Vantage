from datetime import datetime, timezone

from pyspark.sql import Row, functions as F

from vantage.transform.silver import (aggregate_collisions, files_to_process,
                                      merge_silver, silver_source_files)
from vantage.validate.pageviews import (annotate_bronze, clean_bronze,
                                        quarantine_rows, write_quarantine_for_files)

H = datetime(2026, 10, 15, 0, 0, tzinfo=timezone.utc)


def bronze_df(spark, lines, name="pageviews-20261015-010000.gz"):
    return spark.createDataFrame([Row(value=l, source_file=name, file_hour=H) for l in lines])


def one_batch(spark, views, title="A", name="f1.gz"):
    df = spark.createDataFrame([Row(project="en", title=title, hour=H, views=views, bytes=1, source_file=name)])
    return df.withColumn("date", F.to_date("hour"))


def test_collisions_summed_to_one_row(spark):
    # Foo%20Bar -> "Foo Bar" -> "Foo_Bar", colliding with Foo_Bar; mobile stays separate
    clean, _ = clean_bronze(bronze_df(spark, ["en Foo_Bar 10 0", "en Foo%20Bar 5 0", "en.m Foo_Bar 7 0"]))
    agg = aggregate_collisions(clean)
    got = {(r["project"], r["title"]): r["views"] for r in agg.collect()}
    assert got[("en", "Foo_Bar")] == 15
    assert got[("en.m", "Foo_Bar")] == 7
    assert agg.count() == 2


def test_merge_twice_identical(spark, tmp_path):
    p = str(tmp_path / "silver")
    merge_silver(spark, one_batch(spark, 10), p)
    merge_silver(spark, one_batch(spark, 10), p)
    s = spark.read.format("delta").load(p)
    assert s.count() == 1 and s.first()["views"] == 10
    assert s.groupBy("project", "title", "hour").count().where(F.col("count") > 1).count() == 0


def test_changed_views_replaces_not_adds(spark, tmp_path):
    p = str(tmp_path / "silver")
    merge_silver(spark, one_batch(spark, 10), p)
    merge_silver(spark, one_batch(spark, 20), p)  # rerun with a new value
    s = spark.read.format("delta").load(p)
    assert s.count() == 1 and s.first()["views"] == 20  # replaced, not 30


def test_reprocess_clears_stale_quarantine(spark, tmp_path):
    p = str(tmp_path / "quarantine")
    # two files land, each with one bad line
    bad = spark.createDataFrame([
        Row(value="en Bad_Views 9x 0", source_file="f1.gz", file_hour=H),
        Row(value="en Good 5 0",       source_file="f1.gz", file_hour=H),
        Row(value="en Also_Bad 7y 0",  source_file="f2.gz", file_hour=H),
    ])
    write_quarantine_for_files(quarantine_rows(annotate_bronze(bad)), p, ["f1.gz", "f2.gz"])
    counts = {r["source_file"]: r["count"]
              for r in spark.read.format("delta").load(p).groupBy("source_file").count().collect()}
    assert counts == {"f1.gz": 1, "f2.gz": 1}

    # f1 reprocessed with its bad line fixed -> f1's quarantine clears to 0 (even
    # though the new set is empty), f2 is left alone
    fixed = spark.createDataFrame([
        Row(value="en Bad_Views 12 0", source_file="f1.gz", file_hour=H),
        Row(value="en Good 5 0",       source_file="f1.gz", file_hour=H),
    ])
    write_quarantine_for_files(quarantine_rows(annotate_bronze(fixed)), p, ["f1.gz"])
    after = spark.read.format("delta").load(p)
    assert after.where(F.col("source_file") == "f1.gz").count() == 0
    assert after.where(F.col("source_file") == "f2.gz").count() == 1
    assert after.count() == 1


def test_already_processed_file_skipped(spark, tmp_path):
    p = str(tmp_path / "silver")
    merge_silver(spark, one_batch(spark, 10, name="f1.gz"), p)
    done = silver_source_files(spark, p)
    assert done == {"f1.gz"}
    assert files_to_process(["f1.gz", "f2.gz"], done) == ["f2.gz"]
    assert files_to_process(["f1.gz", "f2.gz"], done, full=True) == ["f1.gz", "f2.gz"]

"""Phase 0 prototype vs the Spark pipeline must agree on the same input.

Runs prototype/pageviews.py and Bronze -> validate -> aggregate on the same
fixture and asserts identical clean rows (after collision aggregation), identical
quarantine counts by reason, and the same number of collisions merged.
"""
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from prototype.pageviews import clean_pageview_file
from vantage.ingest.bronze import land_file
from vantage.transform.silver import aggregate_collisions
from vantage.validate.pageviews import annotate_bronze, clean_rows, quarantine_rows

FIXTURE = Path(__file__).parent / "fixtures" / "pageviews-20261015-120000.gz"
HFMT = "%Y-%m-%d %H:%M:%S"           # Python strftime (prototype side)
JFMT = "yyyy-MM-dd HH:mm:ss"         # Spark date_format (same instant, session tz UTC)


@pytest.fixture(scope="module")
def pipelines(spark):
    tmp = tempfile.mkdtemp(prefix="vantage-parity-")
    land_file(spark, str(FIXTURE), f"{tmp}/bronze")
    bronze = spark.read.format("delta").load(f"{tmp}/bronze")
    ann = annotate_bronze(bronze)
    agg = aggregate_collisions(clean_rows(ann)).cache()
    agg.count()  # materialize before the temp dir is cleaned
    quar = quarantine_rows(ann)
    q_by_reason = {r["reason"]: r["count"] for r in quar.groupBy("reason").count().collect()}
    collisions = agg.agg(F.sum("rows_in")).first()[0] - agg.count()
    # format the hour in Spark (session tz UTC); collect() would convert the
    # timestamp to the driver's local tz and spuriously differ from the prototype.
    spark_rows = {(r["project"], r["title"], r["hour_str"], r["views"], r["bytes"])
                  for r in agg.withColumn("hour_str", F.date_format("hour", JFMT)).collect()}

    proto = clean_pageview_file(str(FIXTURE))
    proto_rows = {(r.project, r.title, r.hour.strftime(HFMT), int(r.views), int(r.bytes))
                  for r in proto.clean.itertuples(index=False)}

    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
    return {
        "spark_rows": spark_rows, "proto_rows": proto_rows,
        "spark_q": q_by_reason, "proto_q": proto.stats["quarantine_by_reason"],
        "spark_collisions": collisions, "proto_collisions": proto.stats["collisions_merged"],
    }


def test_same_clean_row_set(pipelines):
    s, p = pipelines["spark_rows"], pipelines["proto_rows"]
    assert len(s) == len(p)
    assert s == p  # same (project, title, hour, views, bytes)


def test_same_quarantine_counts_by_reason(pipelines):
    assert pipelines["spark_q"] == pipelines["proto_q"]


def test_same_collisions_merged(pipelines):
    assert pipelines["spark_collisions"] == pipelines["proto_collisions"]
    assert pipelines["spark_collisions"] > 0  # the fixture has real collision pairs

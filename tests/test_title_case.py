import pandas as pd
from pyspark.sql import functions as F

from prototype.normalize import normalize_title_series
from vantage.normalize import normalize_title

# Georgian Mkhedruli ს (U+10E1) uppercases to Mtavruli Ს (U+1CA1) on Java 11+ and
# Python 3.7+. Java 8 leaves it unchanged, which split Silver keys on Databricks (Day 21).
KA = "საქართველოს_რკინიგზა"
KA_UPPER = "Ს" + KA[1:]


def test_georgian_first_letter_python():
    assert normalize_title_series(pd.Series([KA, "ს"])).tolist() == [KA_UPPER, "Ს"]


def test_georgian_first_letter_spark(spark):
    df = spark.createDataFrame([(KA,), ("ს",)], ["t"])
    out = [r["n"] for r in df.select(normalize_title(F.col("t")).alias("n")).collect()]
    assert out == [KA_UPPER, "Ს"]


def test_spark_and_python_agree_on_georgian(spark):
    df = spark.createDataFrame([(KA,)], ["t"])
    spark_out = df.select(normalize_title(F.col("t")).alias("n")).first()["n"]
    assert spark_out == normalize_title_series(pd.Series([KA]))[0]


def test_runtime_check_reports_java_17_mapping(spark):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import run_pipeline
    check = run_pipeline.runtime_check(spark)
    assert check["java_version"].startswith("17")
    assert check["upper_ka"] == "U+1CA1"

from datetime import datetime, timezone

from pyspark.sql import Row

from vantage.validate.pageviews import SILVER_SCHEMA, clean_bronze

# tz-aware UTC so createDataFrame stores the instant we mean (Bronze builds file_hour in UTC)
FILE_HOUR = datetime(2026, 10, 15, 12, 0, 0, tzinfo=timezone.utc)


def bronze_df(spark, lines, name="pageviews-20261015-130000.gz"):
    return spark.createDataFrame([Row(value=l, source_file=name, file_hour=FILE_HOUR) for l in lines])


def test_each_quarantine_reason(spark):
    lines = {
        "en Bad_Views 12x 0": "bad_views",
        "en OnlyThree 1": "wrong_field_count",
        "en Bad_Bytes 5 9x": "bad_bytes",
        'en "Unclosed 1 0': "unbalanced_quote",
        "en %E9%A1 1 0": "bad_url_encoding",     # bad %-escape -> invalid UTF-8
        "en  5 0": "empty_title",                # double space -> empty title
        "en Caf�e 1 0": "bad_utf8",         # U+FFFD from Bronze's decode
        "en Good 10 0": None,                    # survives, not quarantined
    }
    clean, quarantine = clean_bronze(bronze_df(spark, list(lines)))
    got = {r["raw_line"]: r["reason"] for r in quarantine.collect()}
    assert got == {k: v for k, v in lines.items() if v is not None}
    assert "Good" in {r["title"] for r in clean.collect()}


def test_plus_kept_namespace_and_placeholder_dropped(spark):
    lines = [
        "en C++ 3 0",           # keep; '+' is literal, not a space
        "en Talk:Foo 7 0",      # drop: namespace
        "en file:cat 2 0",      # drop: namespace, lowercased prefix
        "en - 5 0",             # drop: placeholder
        "en Real_Article 9 0",  # keep
    ]
    clean, quarantine = clean_bronze(bronze_df(spark, lines))
    rows = clean.collect()
    assert {r["title"] for r in rows} == {"C++", "Real_Article"}
    cpp = next(r for r in rows if r["title"] == "C++")
    assert cpp["views"] == 3 and cpp["project"] == "en"
    # dropped non-articles are not quarantined
    assert quarantine.count() == 0
    # hour comes straight from file_hour
    assert clean.selectExpr("date_format(hour,'yyyy-MM-dd HH:mm') h").first()["h"] == "2026-10-15 12:00"


def test_percent_decoded_like_prototype(spark):
    # %20 -> space, collapses to empty after trim -> empty_title; %C3%A9 -> é (valid)
    clean, quarantine = clean_bronze(bronze_df(spark, ["en caf%C3%A9 4 0", "en %20 1 0"]))
    assert {r["title"] for r in clean.collect()} == {"Café"}
    assert {r["reason"] for r in quarantine.collect()} == {"empty_title"}


def test_clean_matches_silver_schema(spark):
    clean, _ = clean_bronze(bronze_df(spark, ["en Foo 1 0"]))
    assert clean.schema == SILVER_SCHEMA

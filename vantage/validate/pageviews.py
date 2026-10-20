"""Silver validation for pageviews.

Parse Bronze raw lines into the typed Silver schema and route bad rows to
quarantine, reusing the exact Day 2 rules and reason strings from
prototype/pageviews.py and docs/cleaning-rules.md. Spark-native throughout
except URL-decoding, which goes through a pandas UDF calling the prototype
decoder so the "+ is literal / bad escape -> None" behaviour matches exactly.

Lives in validate/ (not transform/): the job here is schema enforcement and
quarantining. Collision aggregation and the Silver MERGE are transforms (Day 7),
so this does neither -- it returns the pre-aggregation (clean, quarantine).
"""
import pandas as pd
from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import pandas_udf

from prototype.pageviews import _url_decode
from vantage import config

SILVER_SCHEMA = T.StructType([
    T.StructField("project", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("hour", T.TimestampType()),
    T.StructField("views", T.LongType()),
    T.StructField("bytes", T.LongType()),
    T.StructField("source_file", T.StringType()),
])

QUARANTINE_SCHEMA = T.StructType([
    T.StructField("source_file", T.StringType()),
    T.StructField("raw_line", T.StringType()),
    T.StructField("reason", T.StringType()),
    T.StructField("file_hour", T.TimestampType()),
    T.StructField("quarantined_at", T.TimestampType()),
])

REPLACEMENT_CHAR = "�"  # spark.read.text put this where the gz had invalid UTF-8 (Bronze gap)
_DIGITS = r"^\d+$"
# project SP title SP views SP bytes; title greedy, views/bytes allowed empty so a
# double space yields an empty views field (-> bad_views), matching the prototype's
# rsplit-from-the-right rather than a strict 4-field match (-> wrong_field_count).
_LINE_RE = r"^(\S+) (.*) (\S*) (\S*)$"

_NAMES_LOWER = [n.lower() for n in config.NON_ARTICLE_NAMESPACES]
_PLACEHOLDERS = list(config.PLACEHOLDER_TITLES)


@pandas_udf(T.StringType())
def _url_decode_udf(s: pd.Series) -> pd.Series:
    return s.map(lambda t: _url_decode(t) if t is not None else None)


def _normalize_title(col):
    # trim both ends, spaces -> underscores, uppercase the first character only
    t = F.regexp_replace(F.regexp_replace(col, r"^\s+|\s+$", ""), " ", "_")
    return F.concat(F.upper(t.substr(F.lit(1), F.lit(1))), t.substr(F.lit(2), F.length(t)))


def clean_bronze(bronze):
    """Return (clean, quarantine) from a Bronze DataFrame (value, source_file, file_hour).

    clean matches SILVER_SCHEMA; quarantine matches QUARANTINE_SCHEMA. No collision
    aggregation and no Silver write -- those are Day 7.
    """
    df = bronze.select(F.col("value").alias("raw_line"), "source_file", "file_hour")

    title_raw = F.col("_title_raw")
    quoted = title_raw.startswith('"') & title_raw.endswith('"') & (F.length(title_raw) >= 2)
    core = F.expr("substring(_title_raw, 2, length(_title_raw) - 2)")
    df = (
        df
        .withColumn("_matched", F.col("raw_line").rlike(_LINE_RE))
        .withColumn("project", F.regexp_extract("raw_line", _LINE_RE, 1))
        .withColumn("_title_raw", F.regexp_extract("raw_line", _LINE_RE, 2))
        .withColumn("_views", F.regexp_extract("raw_line", _LINE_RE, 3))
        .withColumn("_bytes", F.regexp_extract("raw_line", _LINE_RE, 4))
        .withColumn("_quoted", quoted)
        .withColumn("_unquoted", F.when(quoted, F.regexp_replace(core, r"\\(.)", "$1")).otherwise(title_raw))
    )

    # parse-stage reason, in the prototype's precedence order (first match wins)
    reason = F.coalesce(
        F.when(F.col("raw_line").contains(REPLACEMENT_CHAR), F.lit("bad_utf8")),
        F.when(~F.col("_matched"), F.lit("wrong_field_count")),
        F.when(~F.col("_views").rlike(_DIGITS), F.lit("bad_views")),
        F.when(~F.col("_bytes").rlike(_DIGITS), F.lit("bad_bytes")),
        F.when(F.col("_title_raw").startswith('"') & ~F.col("_quoted"), F.lit("unbalanced_quote")),
        F.when(F.col("_unquoted") == "", F.lit("empty_title")),
    )
    df = df.withColumn("reason", reason)

    q1 = df.where(F.col("reason").isNotNull()).select("source_file", "raw_line", "reason", "file_hour")
    surv = df.where(F.col("reason").isNull())

    # URL-decode only %-titles, and only those rows, so the Python decoder isn't
    # called on the (vast) majority with no escapes.
    has_esc = F.col("_unquoted").contains("%")
    esc = (surv.where(has_esc)
           .withColumn("_decoded", _url_decode_udf(F.col("_unquoted")))
           .withColumn("_failed", F.col("_decoded").isNull())
           .withColumn("_title1", F.col("_decoded")))
    noesc = (surv.where(~has_esc)
             .withColumn("_decoded", F.lit(None).cast("string"))
             .withColumn("_failed", F.lit(False))
             .withColumn("_title1", F.col("_unquoted")))
    surv = esc.unionByName(noesc).withColumn("_norm", _normalize_title(F.col("_title1")))

    reason2 = F.coalesce(
        F.when(F.col("_failed"), F.lit("bad_url_encoding")),
        F.when(F.col("_norm").isNull() | (F.col("_norm") == ""), F.lit("empty_title")),
    )
    surv = surv.withColumn("reason2", reason2)

    q2 = surv.where(F.col("reason2").isNotNull()).select(
        "source_file", "raw_line", F.col("reason2").alias("reason"), "file_hour")
    surv2 = surv.where(F.col("reason2").isNull())

    # drop non-article namespaces and the "-" placeholder (counted, not quarantined)
    t = F.col("_norm")
    prefix = F.lower(F.split(t, ":").getItem(0))
    is_non_article = (t.contains(":") & prefix.isin(_NAMES_LOWER)) | t.isin(_PLACEHOLDERS)

    clean = surv2.where(~is_non_article).select(
        F.col("project").cast("string").alias("project"),
        F.col("_norm").cast("string").alias("title"),
        F.col("file_hour").cast("timestamp").alias("hour"),
        F.col("_views").cast("long").alias("views"),
        F.col("_bytes").cast("long").alias("bytes"),
        F.col("source_file").cast("string").alias("source_file"),
    )

    quarantine = q1.unionByName(q2).withColumn("quarantined_at", F.current_timestamp())
    return clean, quarantine


def write_quarantine(quarantine, path):
    """Overwrite the quarantine table with the full recompute.

    Quarantine is a deterministic function of Bronze, so overwriting with the
    complete recomputed set is idempotent -- re-running yields identical rows and
    zero duplicates. It also avoids a synthetic key, which a content-based MERGE
    would need and which would wrongly collapse two identical bad lines into one.
    """
    quarantine.write.format("delta").mode("overwrite").save(str(path))

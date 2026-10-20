"""Silver validation for pageviews.

Parse Bronze raw lines into the typed Silver schema and route bad rows to
quarantine, reusing the exact Day 2 rules and reason strings from
prototype/pageviews.py and docs/cleaning-rules.md. Spark-native throughout
except URL-decoding, which goes through a pandas UDF calling the prototype
decoder so the "+ is literal / bad escape -> None" behaviour matches exactly.

Lives in validate/ (not transform/): the job here is schema enforcement and
quarantining. Collision aggregation and the Silver MERGE are transforms (Day 7).

`annotate_bronze` does the parse once and tags every row with a (nullable)
reason and an is_non_article flag; clean_rows/quarantine_rows derive the two
outputs from it, so a caller can persist one parse and get both branches.
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


def annotate_bronze(bronze):
    """One row per Bronze line, parsed once and tagged with:
    project, title, views, bytes, source_file, file_hour, reason (null = survives),
    is_non_article (only meaningful when reason is null)."""
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
        .withColumn("parse_reason", F.coalesce(
            # spark.read.text keeps raw bytes, so invalid UTF-8 stays as those bytes
            # (not U+FFFD) in the SQL string; a UTF-8 round-trip only alters a string
            # that wasn't valid UTF-8 -- the prototype's decode-error -> bad_utf8 rule.
            F.when(F.col("raw_line") != F.decode(F.encode("raw_line", "UTF-8"), "UTF-8"),
                   F.lit("bad_utf8")),
            F.when(~F.col("_matched"), F.lit("wrong_field_count")),
            F.when(~F.col("_views").rlike(_DIGITS), F.lit("bad_views")),
            F.when(~F.col("_bytes").rlike(_DIGITS), F.lit("bad_bytes")),
            F.when(F.col("_title_raw").startswith('"') & ~F.col("_quoted"), F.lit("unbalanced_quote")),
            F.when(F.col("_unquoted") == "", F.lit("empty_title")),
        ))
    )

    surv = df.where(F.col("parse_reason").isNull())
    failed = df.where(F.col("parse_reason").isNotNull())

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
    surv = surv.withColumn("reason", F.coalesce(
        F.when(F.col("_failed"), F.lit("bad_url_encoding")),
        F.when(F.col("_norm").isNull() | (F.col("_norm") == ""), F.lit("empty_title")),
    ))

    t = F.col("_norm")
    is_non_article = (t.contains(":") & F.lower(F.split(t, ":").getItem(0)).isin(_NAMES_LOWER)) | t.isin(_PLACEHOLDERS)

    surv_ann = surv.select(
        "source_file", "raw_line", "file_hour",
        F.col("project").cast("string").alias("project"),
        F.col("_views").cast("long").alias("views"),
        F.col("_bytes").cast("long").alias("bytes"),
        F.col("_norm").cast("string").alias("title"),
        F.col("reason"),
        is_non_article.alias("is_non_article"),
    )
    failed_ann = failed.select(
        "source_file", "raw_line", "file_hour",
        F.col("project").cast("string").alias("project"),
        F.col("_views").cast("long").alias("views"),
        F.col("_bytes").cast("long").alias("bytes"),
        F.lit(None).cast("string").alias("title"),
        F.col("parse_reason").alias("reason"),
        F.lit(False).alias("is_non_article"),
    )
    return surv_ann.unionByName(failed_ann)


def clean_rows(ann):
    return ann.where(F.col("reason").isNull() & ~F.col("is_non_article")).select(
        "project", "title", F.col("file_hour").alias("hour"), "views", "bytes", "source_file")


def quarantine_rows(ann):
    return (ann.where(F.col("reason").isNotNull())
            .select("source_file", "raw_line", "reason", "file_hour")
            .withColumn("quarantined_at", F.current_timestamp()))


def clean_bronze(bronze):
    """(clean, quarantine) from a Bronze DataFrame. clean matches SILVER_SCHEMA,
    quarantine matches QUARANTINE_SCHEMA. No aggregation, no write."""
    ann = annotate_bronze(bronze)
    return clean_rows(ann), quarantine_rows(ann)


def write_quarantine(quarantine, path):
    """Full-overwrite write (Day 6). Idempotent: quarantine is a deterministic
    function of Bronze, so overwriting with the full recompute yields identical
    rows and zero duplicates."""
    quarantine.write.format("delta").mode("overwrite").save(str(path))


def write_quarantine_for_file(quarantine, path, source_file):
    """Replace only this file's quarantine rows (replaceWhere on source_file), so
    an incremental run never rewrites the whole table and re-running one file
    can't duplicate its rows. All rows written satisfy the predicate."""
    (quarantine.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"source_file = '{source_file}'")
     .save(str(path)))


def write_quarantine_for_files(quarantine, path, source_files):
    """Same as write_quarantine_for_file for a whole day's batch: replace exactly
    the quarantine rows of the files reprocessed this run, leaving every other
    file's rows alone. source_files are dump filenames, so the IN-list is safe."""
    in_list = ", ".join(f"'{f}'" for f in source_files)
    (quarantine.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"source_file IN ({in_list})")
     .save(str(path)))

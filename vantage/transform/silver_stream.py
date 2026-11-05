"""Stream the edits Bronze Delta table into Silver: parse, quarantine, dedup, hash.

Reads Bronze (not Kafka), parses each raw value with an explicit recentchange
schema, routes malformed events to a quarantine table, and MERGEs the clean ones
into Silver keyed by meta_id. Usernames are hashed with an off-repo salt and the
plaintext user / comment / log fields are never carried into Silver.

Silver keeps ALL event types and namespaces. The Day 16 restart test compares
Silver's meta.id set against every id the producer logged, so dropping anything
here would make that fail; the edit/new + namespace-0 filter belongs in the Day 17
aggregation, not in Silver. See docs/cleaning-rules.md (streaming path).

Dedup is two layers:
  a. dropDuplicatesWithinWatermark on meta_id, watermark on kafka_timestamp (when we
     ingested), NOT meta.dt (the event time). After an outage the producer replays
     old events; their meta.dt is old, so a meta.dt watermark would mark the replay
     "late" and drop it -- data loss. kafka_timestamp is current for a replay, so the
     replay stays inside the watermark and is deduped, never dropped. Bounds in-stream
     state; the delay must cover a producer reconnect+replay window.
  b. a foreachBatch MERGE that inserts a meta_id only when not already present. This
     is the real uniqueness guarantee -- it holds across the whole Silver history, not
     just the watermark -- and it makes a retried micro-batch a no-op.
"""
import hashlib
import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T

from vantage.normalize import normalize_title
from vantage.pipeline import rows_written_since, table_version

# --- username salt (outside the repo; never logged) ---------------------------
SALT_ENV = "VANTAGE_USER_SALT"
SALT_FILE = Path.home() / ".config" / "vantage" / "user_salt"


def load_salt():
    """The hashing salt, from $VANTAGE_USER_SALT or ~/.config/vantage/user_salt.
    Fails loudly if absent -- Silver must not run and silently leave usernames
    unhashed or hash with an empty salt. The value is never logged or returned
    into any log line."""
    val = os.environ.get(SALT_ENV)
    if val and val.strip():
        return val.strip()
    if SALT_FILE.exists():
        s = SALT_FILE.read_text().strip()
        if s:
            return s
    raise RuntimeError(
        f"user salt missing: set ${SALT_ENV} or put a random value in {SALT_FILE} "
        f"(keep it out of the repo). Silver refuses to run without it."
    )


def hash_user(salt: str, user: str) -> str:
    """SHA-256 of salt + username. Used by the tests; the stream does the same in
    Spark via sha2(concat(salt, user), 256)."""
    return hashlib.sha256((salt + user).encode("utf-8")).hexdigest()


# --- recentchange parse schema -------------------------------------------------
# Only the fields Silver keeps, plus _corrupt: in PERMISSIVE from_json a malformed
# value lands its raw text in _corrupt (and nulls the rest), which is how bad_json
# is told apart from valid JSON that merely lacks meta.id. user is parsed solely to
# hash it; it is dropped before Silver. comment / parsedcomment / log_* are never read.
_CORRUPT = "_corrupt"
RECENTCHANGE_SCHEMA = T.StructType([
    T.StructField("meta", T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("dt", T.StringType()),
    ])),
    T.StructField("type", T.StringType()),
    T.StructField("namespace", T.IntegerType()),
    T.StructField("title", T.StringType()),
    T.StructField("wiki", T.StringType()),
    T.StructField("bot", T.BooleanType()),
    T.StructField("user", T.StringType()),
    T.StructField("revision", T.StructType([
        T.StructField("old", T.LongType()),
        T.StructField("new", T.LongType()),
    ])),
    T.StructField("length", T.StructType([
        T.StructField("old", T.LongType()),
        T.StructField("new", T.LongType()),
    ])),
    T.StructField(_CORRUPT, T.StringType()),
])

SILVER_COLUMNS = ["meta_id", "event_time", "hour", "type", "namespace", "title",
                  "title_norm", "wiki", "bot", "user_hash", "revision_old",
                  "revision_new", "length_old", "length_new", "partition", "offset", "source"]

QUARANTINE_COLUMNS = ["value", "partition", "offset", "reason", "kafka_timestamp", "source"]


def parse_and_annotate(df, salt, source="kafka"):
    """Bronze rows (value, partition, offset, kafka_timestamp) -> every Silver column
    plus `reason` (null = clean) and `dedup_key`. Nothing is filtered here; the caller
    splits on `reason`. `source` tags where the row came from: 'kafka' for the live
    stream, 'capture' for the Day 17 JSONL backfill."""
    e = F.from_json(F.col("value"), RECENTCHANGE_SCHEMA,
                    {"columnNameOfCorruptRecord": _CORRUPT, "mode": "PERMISSIVE"})
    df = df.withColumn("_e", e)
    event_time = F.to_timestamp(F.col("_e.meta.dt"))
    title = F.col("_e.title")
    df = (df
          .withColumn("meta_id", F.col("_e.meta.id"))
          .withColumn("event_time", event_time)
          .withColumn("hour", F.date_trunc("hour", event_time))
          .withColumn("type", F.col("_e.type"))
          .withColumn("namespace", F.col("_e.namespace"))
          .withColumn("title", title)
          .withColumn("title_norm", normalize_title(title))
          .withColumn("wiki", F.col("_e.wiki"))
          .withColumn("bot", F.col("_e.bot"))
          .withColumn("user_hash", F.when(F.col("_e.user").isNotNull(),
                                          F.sha2(F.concat(F.lit(salt), F.col("_e.user")), 256)))
          .withColumn("revision_old", F.col("_e.revision.old"))
          .withColumn("revision_new", F.col("_e.revision.new"))
          .withColumn("length_old", F.col("_e.length.old"))
          .withColumn("length_new", F.col("_e.length.new"))
          .withColumn("source", F.lit(source))
          .withColumn("reason", F.coalesce(
              F.when(F.col(f"_e.{_CORRUPT}").isNotNull(), F.lit("bad_json")),
              F.when(F.col("_e.meta.id").isNull(), F.lit("missing_meta_id")),
              F.when(event_time.isNull(), F.lit("bad_meta_dt")),
              F.when(title.isNull() | (F.trim(title) == ""), F.lit("empty_title")),
          ))
          # clean rows dedup on meta_id; the rare meta_id-less malformed row (bad_json,
          # missing_meta_id) gets a unique partition:offset key so it is never collapsed.
          .withColumn("dedup_key", F.coalesce(
              F.col("meta_id"), F.concat_ws(":", F.col("partition"), F.col("offset")))))
    return df.drop("_e")


WATERMARK_DELAY = "2 hours"
# Cap files per micro-batch so catching up a Bronze backlog can't load it all at once.
MAX_FILES_PER_TRIGGER = 200


def dedup_within_watermark(df, delay=WATERMARK_DELAY):
    return df.withWatermark("kafka_timestamp", delay).dropDuplicatesWithinWatermark(["dedup_key"])


def bronze_stream(spark, bronze_path, max_files_per_trigger=MAX_FILES_PER_TRIGGER):
    return (spark.readStream.format("delta")
            .option("maxFilesPerTrigger", max_files_per_trigger)
            .load(str(bronze_path)))


def silver_input(bronze_df, salt, delay=WATERMARK_DELAY):
    """Full Bronze -> Silver transform up to (not including) the sinks: parse, annotate,
    then layer-a watermark dedup. The foreachBatch MERGE (layer b) is the sink."""
    return dedup_within_watermark(parse_and_annotate(bronze_df, salt), delay)


def _merge_insert(src, path, condition, dedup_keys):
    """Insert-only MERGE: insert a source row only when `condition` finds no match.
    The source is deduped within the batch first so MERGE never sees two rows for one
    key. Creates the table on first write."""
    spark = src.sparkSession
    src = src.dropDuplicates(dedup_keys)
    if not DeltaTable.isDeltaTable(spark, str(path)):
        src.write.format("delta").save(str(path))
        return
    (DeltaTable.forPath(spark, str(path)).alias("t")
     .merge(src.alias("s"), condition)
     .whenNotMatchedInsertAll()
     .execute())


def merge_silver(batch_df, path):
    _merge_insert(batch_df.select(*SILVER_COLUMNS), path, "t.meta_id = s.meta_id", ["meta_id"])


def merge_quarantine(batch_df, path):
    q = (batch_df.select(*QUARANTINE_COLUMNS)
         .withColumn("quarantined_at", F.current_timestamp()))
    _merge_insert(q, path, "t.partition = s.partition AND t.offset = s.offset",
                  ["partition", "offset"])


# --- batch backfill from the captured JSONL (Day 17) ---------------------------

def read_capture(spark, paths):
    """Captured recent-change JSONL -> the Bronze row shape parse_and_annotate expects.
    Capture rows have no Kafka lineage, so partition/offset/kafka_timestamp are null."""
    return (spark.read.text([str(p) for p in paths])
            .withColumn("partition", F.lit(None).cast("int"))
            .withColumn("offset", F.lit(None).cast("long"))
            .withColumn("kafka_timestamp", F.lit(None).cast("timestamp")))


def write_quarantine_batch(df, path, source):
    """Idempotent per-source quarantine write for the batch backfill: overwrite only
    this source's rows (replaceWhere), so a rerun replaces rather than appends. Capture
    rows have null partition/offset, so the streaming (partition, offset) MERGE can't key
    them -- scoping by source does."""
    q = (df.select(*QUARANTINE_COLUMNS)
         .withColumn("quarantined_at", F.current_timestamp()))
    writer = q.write.format("delta").mode("overwrite").option("mergeSchema", "true")
    if DeltaTable.isDeltaTable(df.sparkSession, str(path)):
        writer = writer.option("replaceWhere", f"source = '{source}'")
    writer.save(str(path))


def process_batch(batch_df, silver_path, quar_path):
    """Split one deduped micro-batch into clean -> Silver and malformed -> quarantine,
    MERGEing each. Returns per-batch counts for the layer-dedup accounting.
    clean_in is post-layer-a (what reached this batch); inserted is what MERGE actually
    added, so clean_in - inserted is what layer b removed."""
    spark = batch_df.sparkSession
    batch_df = batch_df.persist()
    try:
        clean = batch_df.where(F.col("reason").isNull())
        quar = batch_df.where(F.col("reason").isNotNull())
        clean_in, quar_in = clean.count(), quar.count()

        sv0 = table_version(spark, silver_path)
        merge_silver(clean, silver_path)
        inserted = rows_written_since(spark, silver_path, sv0)

        quar_inserted = 0
        if quar_in:
            qv0 = table_version(spark, quar_path)
            merge_quarantine(quar, quar_path)
            quar_inserted = rows_written_since(spark, quar_path, qv0)
        return {"clean_in": clean_in, "inserted": inserted,
                "quar_in": quar_in, "quar_inserted": quar_inserted}
    finally:
        batch_df.unpersist()

"""Stream the recentchange topic into the edits Bronze Delta table.

Spark Structured Streaming reads Kafka and appends the raw message plus Kafka
metadata to Bronze, append-only. Exactly-once against the broker comes from the
checkpoint, not from us: Delta's streaming sink records in the checkpoint which
Kafka offsets each batch committed, so a restart resumes from there with no gaps
and no rewrites.

failOnDataLoss is left ON. If offsets we still need have aged out of Kafka before
we read them, the query fails loudly instead of silently skipping past the gap --
Bronze's one job is a complete, contiguous log, so a gap must stop the stream, not
be swallowed.
"""
from pyspark.sql import functions as F

# Cap each micro-batch so catching up a big backlog can't pull the whole topic
# into one batch and blow the driver heap. Across all partitions, per trigger.
MAX_OFFSETS_PER_TRIGGER = 10_000

# Stored columns (plus kafka_date, the partition column derived from kafka_timestamp).
BRONZE_COLUMNS = ["key", "value", "topic", "partition", "offset",
                  "kafka_timestamp", "ingested_at"]


def kafka_source(spark, bootstrap, topic, starting_offsets="earliest",
                 max_offsets_per_trigger=MAX_OFFSETS_PER_TRIGGER):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", starting_offsets)
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
            .load())


def to_bronze(kafka_df):
    """Kafka source columns -> Bronze schema. value stays the raw JSON string,
    byte-for-byte what the producer published; nothing is parsed here."""
    return (kafka_df
            .select(
                F.col("key").cast("string").alias("key"),
                F.col("value").cast("string").alias("value"),
                F.col("topic"),
                F.col("partition"),
                F.col("offset"),
                F.col("timestamp").alias("kafka_timestamp"),
            )
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("kafka_date", F.to_date("kafka_timestamp")))


def write_bronze(df, bronze_path, checkpoint_path, trigger):
    """Start the append-only Delta stream. trigger is kwargs for DataStreamWriter.trigger,
    e.g. {"availableNow": True} or {"processingTime": "30 seconds"}."""
    return (df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", str(checkpoint_path))
            .partitionBy("kafka_date")
            .trigger(**trigger)
            .start(str(bronze_path)))


# --- offset auditing (used by scripts/check_bronze_stream.py and the tests) ----

def bronze_offset_stats(df):
    """Per Kafka partition: min/max offset, row count, and distinct-offset count.
    distinct < rows means Bronze landed the same (partition, offset) more than once."""
    rows = (df.groupBy("partition")
              .agg(F.min("offset").alias("min_offset"),
                   F.max("offset").alias("max_offset"),
                   F.count(F.lit(1)).alias("rows"),
                   F.countDistinct("offset").alias("distinct_offsets"))
              .orderBy("partition")
              .collect())
    return {r["partition"]: {"min_offset": r["min_offset"], "max_offset": r["max_offset"],
                             "rows": r["rows"], "distinct_offsets": r["distinct_offsets"]}
            for r in rows}


def contiguity_report(stats):
    """Per partition, decide contiguous (rows == max-min+1) and no_duplicates
    (rows == distinct offsets). stats is the bronze_offset_stats mapping."""
    out = {}
    for p, s in stats.items():
        span = s["max_offset"] - s["min_offset"] + 1
        out[p] = {**s, "span": span,
                  "contiguous": s["rows"] == span,
                  "no_duplicates": s["rows"] == s["distinct_offsets"]}
    return out

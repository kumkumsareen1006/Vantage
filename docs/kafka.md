# Vantage Kafka setup

Phase 2 streams the live Wikipedia edit feed through Kafka before it reaches the
Delta tables. This is the broker and topic that stream runs on. One broker, one
topic, running on a laptop — small on purpose, but set up the way a real cluster
would be so the code doesn't change when it grows.

## The broker

A single Kafka broker in [KRaft mode](https://kafka.apache.org/documentation/#kraft)
(no ZooKeeper), run from `docker-compose.yml`:

```
docker compose up -d      # start
docker compose down       # stop (keeps the data volume)
docker compose down -v    # stop and wipe the data
```

- **Image:** `apache/kafka:3.9.2`, pinned to an exact version so the setup is
  reproducible and can't drift to a new `latest`.
- **Heap capped** at `-Xmx512m`. The laptop shares memory with the edit capture,
  Spark and an editor, so the broker is kept small.
- **Reachable at `localhost:9092`.** The broker advertises `localhost:9092`, and
  Docker publishes that port, so clients on the Mac connect straight to it.
- **Data lives in a named Docker volume** (`vantage-kafka-data`), so it survives
  `docker compose down` and container restarts.

### Works with Spark 3.5

Spark 3.5.9's Kafka connector (`spark-sql-kafka-0-10_2.12:3.5.9`) bundles
`kafka-clients` **3.4.1**. The broker is pinned to the 3.9 line so broker and
client stay on the same major version — a combination with no compatibility
surprises. Checked directly: a Spark batch read of the topic
(`spark.read.format("kafka")`) connected to the 3.9.2 broker and returned every
record with the right partition and offset.

## The topic

`recentchange`, created idempotently by `scripts/create_topic.py`. Its settings
live in the `[kafka]` block of `config/vantage.toml`:

| Setting | Value | Why |
| --- | --- | --- |
| partitions | 3 | parallelism and room to grow (see below) |
| replication factor | 1 | one broker, so one copy |
| retention | 14 days | a page's recent edit history stays replayable |
| max size | 1 GiB per partition | disk backstop, 3 GiB total |

### Why 3 partitions

A topic's partitions are the unit of parallelism: a consumer group can run at
most one reader per partition. Three means Spark Structured Streaming can read
with up to three parallel tasks, and if this ever moves to a 3-broker cluster
the partitions spread one-per-broker without re-partitioning. One would cap us
at a single reader forever; a large number would just add overhead on a laptop.

### Why the key is the page title

Kafka keeps order only *within* a partition, and routes a message to a partition
by hashing its key. Keying every event on its page title means all edits to one
page always land on the same partition, in the order they happened. That makes
"the edits for this page, in order" a property the broker guarantees, which the
downstream per-page aggregation relies on. Events with no title fall back to
Kafka's round-robin spread across partitions.

### Auto-create is off

`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`. A producer or consumer that names a
topic that doesn't exist fails loudly instead of silently creating a new
single-partition topic. A typo can't quietly become a real topic with the wrong
settings. Topics are created only, and explicitly, by `create_topic.py`.

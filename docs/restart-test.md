# Vantage streaming restart test

The streaming path has to survive a crash without losing or duplicating an edit.
This test proves it by crashing the stream and the producer on purpose and then
checking three invariants against ground truth.

Every number below is from one real run on **2026-11-05** (UTC throughout).

## What each layer guarantees

- **Bronze** is an append-only log of the Kafka topic. Its guarantee is *no gaps*:
  every Kafka `(partition, offset)` lands exactly once, contiguous from 0. That comes
  from the Structured Streaming checkpoint, which records the offsets each batch
  committed, so a restart resumes exactly where it stopped.
- **Silver** parses Bronze and its guarantee is *unique by `meta.id`*. Upstream
  duplicates (the producer replays events after a reconnect, re-delivering them at new
  Kafka offsets) are collapsed by a watermark dedup plus an insert-only MERGE.
- The **producer** publishes with Kafka idempotence on and resumes from a saved
  `Last-Event-ID`, and logs every confirmed delivery to `data/stream/published.jsonl`
  as `{meta_id, partition, offset}`. That log is the ground truth this test checks against.

## What was done

`scripts/stream_all.py` runs both streaming queries (Kafka → Bronze, Bronze → Silver)
in one Spark driver. The sequence:

| Time (UTC) | Step |
| --- | --- |
| 04:04:13 | Start `stream_all` (continuous, 30 s trigger). It catches up the Kafka backlog and streams live. |
| 04:09:13 | **`kill -9`** the stream process mid-batch (it was on Bronze batch 36 / Silver batch 11). No clean shutdown. |
| 04:09:36 – 04:11:36 | Stream down ~2 min; the producer keeps publishing (+717 events into Kafka). |
| 04:12:10 | **Restart** `stream_all`. Its first Bronze batch resumed at the exact offsets the killed run last committed — no gap, no re-read. Runs ~3 min. |
| 04:19:28 | **`kill -9` the producer, then restart it** (done by hand with the guarded command). On restart it resumes from `Last-Event-ID` and **replays** recent events, re-delivering some at new Kafka offsets. |
| 04:21:25 – 04:24:25 | ~3 more min of live streaming, consuming the replayed (duplicate) events. |
| 04:25:02 | Stop `stream_all` cleanly (Ctrl-C): both queries stopped, summary logged. |
| 04:25:12 – 04:25:27 | Drain with `--once` so Silver fully reflects Bronze (+94 Bronze, +255 Silver). |
| 04:26:05 | Run `scripts/restart_test.py`. |

The two crashes test two different things: the `kill -9` of the **stream** tests
checkpoint recovery (Bronze stays gapless); the `kill -9` of the **producer** creates
the upstream duplicates (and a small delivery race) that Silver must absorb.

## The three checks

Counts come from the edits Bronze and Silver Delta tables and from the delivery log,
compared only up to the highest offset Bronze holds per partition (Bronze can trail
the still-running producer):

```
delivery-log rows in range  53,486   (distinct meta_ids 53,480)
Bronze rows                 53,488   (clean 53,488, malformed 0, distinct ids 53,480)
Silver rows                 53,480   (distinct meta_ids 53,480)
```

**Check 1 — Bronze has every `(partition, offset)` once, contiguous from 0.** PASS.

| partition | rows | offsets |
| --- | --- | --- |
| 0 | 16,548 | 0 … 16,547 |
| 1 | 18,958 | 0 … 18,957 |
| 2 | 17,982 | 0 … 17,981 |

No gaps and no duplicate offsets on any partition, across the `kill -9` and restart.

**Check 2 — every `meta.id` the producer logged is in Silver (nothing lost).** PASS.
Logged ids missing from Silver: **0**.

**Check 3 — Silver `meta.id`s are unique.** PASS. 53,480 rows, 53,480 distinct.

## Upstream duplicates removed

The producer's post-restart replay re-delivered events at new offsets:

- duplicates in the delivery log: **6**
- duplicates in Bronze: **8**
- duplicates Silver removed: **8** → Silver is exactly the 53,480 distinct events.

Bronze carries 2 more duplicate rows than the log (8 vs 6) because of a delivery race,
below.

## The hard-kill delivery race (not loss)

The `kill -9` of the producer caught 2 messages that had reached Kafka but whose delivery
callback had not yet written their log line:

```
Bronze offsets not in the delivery log: (0, 15884), (2, 17393)
```

These events **are** in Bronze (and Silver) — only their log lines are missing. Each is
also the duplicate copy of an event the producer re-published and logged after restart,
so every Silver `meta.id` still appears in the log (0 Silver ids are unlogged) and nothing
is lost. The arithmetic closes: `53,488 Bronze − 53,486 logged-in-range = 2`.

## How to rerun it

The producer must be running and the broker up (`docker compose up -d`). From the
project root with the venv active and `JAVA_HOME` on Java 17:

1. `python scripts/stream_all.py` — let it run a few minutes.
2. `kill -9` the stream process; wait while the producer keeps publishing.
3. `python scripts/stream_all.py` again — let it catch up.
4. `kill -9` the producer and restart it (`caffeinate -i python scripts/produce_edits.py`).
5. Let the stream run a few more minutes, stop it with Ctrl-C, then drain:
   `python scripts/stream_all.py --once`.
6. `python scripts/restart_test.py` — exits 0 only if all three checks pass.

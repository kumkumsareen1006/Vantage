# Vantage

[![tests](https://github.com/kumkumsareen1006/Vantage/actions/workflows/tests.yml/badge.svg)](https://github.com/kumkumsareen1006/Vantage/actions/workflows/tests.yml)

A Spark + Delta Lake pipeline over English Wikipedia pageviews and edits. Together,
Phase 1 and Phase 2 now answer the full question this project was built toward:
which articles suddenly drew unusual attention in a given hour — a spike of views
larger than their own recent history explains — which of those spikes look automated
rather than driven by real readers, and whether people were editing those same
articles at the same time. Phase 1 reads the raw hourly pageview dumps, cleans and
aggregates them through medallion layers, and scores each (title, hour) against a
24-hour baseline. The automated-traffic flag is a heuristic: a desktop-only,
single-hour burst is a strong signal of scraper or redirect traffic, not a verified
label.

Phase 1 is a local batch lakehouse over the pageview dumps. **Phase 2 adds the live
edit stream** — the Wikimedia recent-changes feed through Kafka and Spark Structured
Streaming into the same medallion layers, then joined to the pageview spikes to show
which surges of attention were matched by a surge of human editing. **Phase 3 runs the
batch pipeline on Azure** (ADLS Gen2, Databricks, Data Factory) with the same counts as the
laptop; see [Phase 3: Azure](#phase-3-azure).

## Architecture

```mermaid
flowchart LR
    R["Hourly pageview dumps<br/>pageviews-YYYYMMDD-HH0000.gz"] --> B
    B["<b>Bronze</b> (Delta)<br/>append-only, scope-filtered"] --> S
    S["<b>Silver</b> (Delta)<br/>cleaned, unique by<br/>(project, title, hour)"] --> G
    S -. bad rows .-> Q["<b>Quarantine</b> (Delta)<br/>raw line + reason"]

    W["Wikimedia recent-changes<br/>(server-sent events)"] --> P["<b>Producer</b><br/>idempotent, resumes<br/>from Last-Event-ID"]
    P --> K["<b>Kafka</b><br/>recentchange, 3 partitions"]
    K --> BE["<b>Bronze edits</b> (Delta)<br/>append-only, gapless offsets"]
    BE --> SE["<b>Silver edits</b> (Delta)<br/>unique by meta.id,<br/>usernames hashed"]
    SE -. bad rows .-> QE["<b>Quarantine edits</b> (Delta)<br/>raw value + reason"]
    SE --> G
    G["<b>Gold</b> (Delta)<br/>view spikes +<br/>edit co-spikes"]
```

The top lane is Phase 1 (batch). The bottom lane is Phase 2 (streaming): the live edit
feed is produced to Kafka and streamed through its own Bronze and Silver, then joined
into Gold where a view spike and a human-edit spike in the same hour become a co-spike.

- **Bronze** — every in-scope raw line landed exactly as read, one file per hour,
  appended and never rewritten. Re-landing a file is a no-op, so the ingest is
  idempotent. Every in-scope line of every landed file is there; nothing is
  filtered beyond the en/en.m scope.
- **Silver** — parsed, title-normalized, collision-aggregated, and unique by
  `(project, title, hour)`. Written with a MERGE that replaces a key's row
  instead of adding one, so a rerun can't double-count.
- **Quarantine** — any row that fails to parse or validate is kept here with its
  raw text and a reason, never dropped silently and never allowed to crash the
  run. Reprocessing a file clears its old quarantine rows first.
- **Gold** — `top_pages_hourly`, `top_pages_daily`, `spikes` (the z-score table),
  and `co_spikes` (a view spike and a human-edit spike in the same hour). Each is a
  pure function of Silver, overwritten every build, so it can't accumulate or drift;
  a build is skipped entirely when Silver hasn't changed.

The full set of filters, drops, and thresholds each layer applies is written up
as an exact spec in [docs/cleaning-rules.md](docs/cleaning-rules.md).

## Scope: `en` and `en.m` only

Vantage keeps only the English desktop (`en`) and English mobile (`en.m`)
projects and drops everything else — at ingest, in Bronze, before anything is
stored. That is a **scoping** decision, not a cleaning one: the full hourly dump
is every project and language and would be far larger than this needs to be to
run on a laptop. Narrowing to two projects keeps the data small enough to
iterate on while staying real, uncontrolled public data. The two are kept
separate through Silver (the desktop/mobile split is what the automated-traffic
flag later depends on) and summed per title in Gold.

## Why Delta

The layers lean on three Delta features that plain Parquet doesn't give you:

- **MERGE** makes Silver idempotent. A key that already exists is updated in
  place rather than appended, so reprocessing a file — or rerunning the whole
  window — converges to the same table instead of piling up duplicates.
- **Schema enforcement** means a stage can't quietly write the wrong shape; a
  column type mismatch fails the write instead of corrupting the table. New
  optional columns are added deliberately with `mergeSchema`.
- **Time travel** keeps each write as a version, so an earlier state of a table
  can be read back by version — exercised in `tests/test_spark_smoke.py`, which
  writes, MERGEs, then reads the pre-MERGE version with `versionAsOf`. VACUUM
  `RETAIN 24 HOURS` then caps how far back time travel reaches at the last 24
  hours of versions.

A separate **quarantine table** exists because the alternative — dropping a bad
line, or letting it throw — either loses data silently or stops a 150M-row run
on one malformed row. Quarantine keeps every rejected line with a reason, so the
reject rate is a number you can watch and the bad rows are there to inspect. A
pageview line that is invalid UTF-8, splits into the wrong number of fields, or
carries a non-integer view count goes to quarantine, not to Silver.

## Idempotency

Silver is written with a MERGE keyed on `(date, project, title, hour)`. A matched
key has its `views`, `bytes`, and `source_file` **replaced**; an unmatched key is
inserted. Nothing is appended to an existing key. So running the pipeline twice,
or rerunning it with `--full`, lands the same table — not a doubled one.

The proof is in both the data and the tests:

- A `--full` rebuild of all 72 files produces exactly `142,728,341` Silver rows,
  and a `GROUP BY (project, title, hour) HAVING count > 1` over that table
  returns **0** duplicate keys.
- `tests/test_silver_merge.py` merges the same batch twice and asserts the row
  count stays `1` with no duplicate key, then merges a changed value and asserts
  the row is replaced (10 → 20, not 30).

## Results

Window: **2026-10-15 00:00 – 2026-10-18 00:00 UTC**, three UTC days, 72 hourly
files. Every number below comes from the `run_metrics` Delta table, a direct
query on the Delta tables, or a test/command run — sources are footnoted; none
are from notes or memory.

| Metric | Value | Source |
|---|---|---|
| Pageview files ingested | 72 | [1] |
| Rows ingested (Bronze) | 152,935,529 | [2] |
| Silver rows | 142,728,341 | [3] |
| Collisions merged (same key, different raw spelling) | 8,678 | [4] |
| Rows quarantined | 0 (0.0000%) | [5] |
| Duplicate Silver keys after a full rerun | 0 | [6] |
| Gold rows | 84,348 = 7,200 hourly + 300 daily + 76,848 spikes | [7] |
| Titles scored for spikes / flagged `is_spike` / `likely_automated` | 1,601 / 2,027 / 7 | [8] |
| Silver files before → after OPTIMIZE | 3 → 3 (already compact) | [9] |
| Silver on-disk before → after VACUUM (RETAIN 24h) | 11,757.0 MB → 2,395.8 MB (reclaimed 9,361.2 MB) | [10] |
| Whole lake on disk before → after VACUUM | ~13 GB → 4.3 GB | [11] |
| Full run: Silver `--full` rebuild (72 files) + Gold | 1,964 s total (Silver MERGE 1,521 s, Gold 434 s) | [12] |
| No-op run (nothing to ingest): Gold rebuilt → Gold skipped | 385 s → 20 s | [13] |
| Tests | 58 passed | [14] |
| CI | green on GitHub Actions (ubuntu-24.04, Temurin 17) | [15] |

<sub>

**Sources.** [1] Bronze `COUNT(DISTINCT source_file)`; also `run_metrics` Silver
`files_processed`. [2] Bronze `count()`. [3] Silver `count()`; also `run_metrics`
Silver `rows_out`. [4] `run_metrics` Silver `rows_in − rows_out`
(142,737,019 − 142,728,341) on the full run of 2026-10-23. [5] `run_metrics`
Silver `rows_quarantined`, over the 152,935,529 Bronze rows. [6] Silver
`GROUP BY (project, title, hour) HAVING count > 1` → 0; `tests/test_silver_merge.py`.
[7] `count()` on each Gold table; also `run_metrics` Gold `rows_out` = 84,348.
[8] Queries on `gold/spikes`: distinct `title`, rows with `is_spike`, rows with
`likely_automated`. [9] `run_metrics` stage `optimize` (`scripts/optimize_silver.py`).
[10] `run_metrics` stage `vacuum:silver` (`scripts/vacuum_tables.py`). [11] `du -sh
data/lake` before and after that VACUUM. [12] `run_metrics`, run 2026-10-23
16:17 UTC (Bronze already landed, so 0 new files that run). [13] `run_metrics`,
runs 2026-10-25 13:36 UTC (Gold rebuilt, 369 s) and 13:42 UTC (Gold skipped,
~1 s). [14] `pytest -q`, 58 passed in 59.4 s locally. [15] GitHub Actions
`tests` workflow, 58 passed in 112.6 s. [16] `scripts/build_silver.py` run output
on Day 7 (4,356.8 s per-file, 1,354.5 s per-day), before `run_metrics` existed.

</sub>

## Findings

**The largest spike in the window was automated traffic, and the flag caught it.**
The single biggest spike was `Ying` — **886,853 views in the one hour
2026-10-17 17:00**, against a baseline near zero. It is 90% desktop
(en 798,356 / en.m 88,497), both neighbouring hours have **0** views, and there
were no edits to the article all window. That shape — one isolated hour, almost
all desktop — is what `likely_automated` is built to catch, and `Ying` is
flagged. Compare `Hôtel_de_Ville,_Limoges`, flagged `is_spike` across 19:00–23:00
the same day: it ramps and decays over several hours and skews mobile
(21:00 was en 2,425 / en.m 4,894), so it is **not** labelled automated — it reads
as real attention. The flag is a label on top of `is_spike`, never a drop, and
the spikes table keeps the view split and both neighbour counts so every label is
auditable.

**Grouping the Silver MERGE by day, not by file, cut the full rebuild ~3×.** The
first version ran one MERGE per pageview file, and each per-file run filtered
Bronze on `source_file` — which isn't the partition column (Bronze is partitioned
by `file_date`). The physical plan showed `PartitionFilters: []`, so nothing was
pruned and the full parse → aggregate → MERGE cycle ran once per file, 72 times
over the window. Switching the batch key to the `file_date` partition prunes the
read to a single day, and because every file covers exactly one hour a day's 24
files aggregate to a byte-identical result, so the window runs in 3 per-day cycles
instead of 72. That dropped the full Silver rebuild from **4,357 s to 1,355 s** [16];
the current `--full` rebuild runs in 1,521 s [12]. The first per-day run then
OOM'd the 4 GB driver — a day's batch is ~24× a single file — until the
aggregation was pinned back to the file-sized task shape (32 MB input partitions,
200 shuffle partitions, AQE coalescing off) so the group-by's hash map stays
inside the heap.

**A parity test caught a UTF-8 quarantine bug.**
`tests/test_parity.py` runs the Phase 0 pandas prototype and the Spark pipeline on
the same fixture — which includes a line with genuinely invalid UTF-8 bytes — and
asserts they produce the identical clean-row set, the same collision count, and
the same quarantine counts *by reason*. The Spark rule flagged `bad_utf8` by
testing whether a line contained the replacement character U+FFFD, assuming
`spark.read.text` substitutes it for invalid bytes. It doesn't: Spark keeps the
raw bytes, so the rule never matched genuinely invalid UTF-8 (the fixture's bad
line slipped past it) and instead wrongly flagged valid lines that happened to
carry a literal U+FFFD. The parity test failed on the per-reason quarantine
counts. The fix flags a line only when it fails a UTF-8 round trip
(`raw_line != decode(encode(raw_line))`). Rebuilding Silver brought the **4** real
`en` article lines whose titles contained a literal U+FFFD back as clean rows, and
the invalid-bytes line now quarantines as `bad_utf8` on both the prototype and
Spark.

## Honest limits

- **It's a 72-hour window.** Three UTC days is enough to prove the pipeline and
  surface real spikes, not enough to characterize weekly or seasonal patterns.
  The spike baseline is 24 hours, so only the last ~48 hours are even scorable.
- **Zero rows were actually quarantined.** The real dump in this window was clean,
  so the 0.0000% reject rate reflects the input, not an untested path. The
  quarantine rules are exercised instead by `tests/test_parity.py` and the unit
  tests against a hand-built fixture with deliberately malformed lines — that is
  where `bad_utf8`, `wrong_field_count`, `bad_url_encoding` and the rest are
  proven, not on production data.
- **`likely_automated` is a heuristic, not ground truth.** "Desktop-dominated and
  isolated" is a strong signal for scraper/redirect bursts, but it is a rule of
  thumb with two tuned thresholds (`DESKTOP_SHARE_MIN = 0.8`,
  `ISOLATION_RATIO = 0.05`), not a verified classification. It's a label to
  filter or inspect by, deliberately kept separate from `is_spike`.

## Phase 2 — streaming the edit feed

Phase 2 brings in the second feed: Wikimedia's live recent-changes stream. Every edit
to English Wikipedia is produced to Kafka, streamed through its own Bronze and Silver
Delta tables, and joined into Gold so a spike of **views** and a spike of human **edits**
in the same hour can be read off together. The hard requirement is that it survives a
crash — the streaming pipeline must not lose or double-count an edit when a process dies
— which the [restart test](docs/restart-test.md) proves on purpose.

### The producer

`vantage/ingest/producer.py` reads the recent-changes server-sent-event stream (English
Wikipedia only) and publishes each event's raw JSON, keyed by title, to the Kafka
`recentchange` topic. Two settings make it safe to crash and restart:

- **Kafka idempotence on** (`enable.idempotence=true`, `acks=all`), so a producer
  retry can't write the same event to the topic twice.
- **Resume from `Last-Event-ID`.** On reconnect the producer sends the last event id it
  durably recorded, so the feed resumes instead of starting over. The resume id is saved
  **only after `flush()` confirms** the events ahead of it reached Kafka, so a crash
  re-reads a few events (at-least-once from the source) rather than skipping any. Every
  confirmed delivery is logged to `data/stream/published.jsonl` as
  `{meta_id, partition, offset}` — the ground truth the restart test checks against.

A 30-minute validation run published **9,970 events = 9,970 confirmed deliveries =
9,970 messages in the topic** (0 failed), surviving 3 reconnects with 0 loss and 0
duplicates.<sup>[a]</sup>

### Bronze and Silver streams, and what each guarantees

- **Bronze edits** is Spark Structured Streaming from Kafka into an append-only Delta
  table, raw message value plus Kafka metadata (topic, partition, offset, timestamp). Its
  guarantee is *no gaps*: every Kafka `(partition, offset)` lands exactly once, contiguous
  from 0. That comes from the streaming **checkpoint**, which records the offsets each
  micro-batch committed, so a restart resumes at the exact offset it stopped at — no
  re-read, no skip. `failOnDataLoss` is left **on**: if offsets Bronze still needs have
  aged out of Kafka, the stream fails loudly rather than silently skipping a gap.
- **Silver edits** parses each Bronze value against an explicit schema and its guarantee
  is *unique by `meta.id`*. It keeps **all** event types and namespaces (the
  `edit`/`new` + namespace-0 filter is applied later, in the Gold join), so the restart
  test can compare Silver's id set against every id the producer logged. Malformed events
  go to a quarantine table with a reason, never dropped silently.

### Two dedup layers, and why the watermark uses `kafka_timestamp`

Upstream duplicates are real: after a reconnect the producer replays recent events, so
the *same* `meta.id` arrives again at a *new* Kafka offset. Silver removes them in two
layers:

1. **`dropDuplicatesWithinWatermark` on `meta.id`**, with a **2-hour** watermark on
   **`kafka_timestamp` (when we ingested the event), not `meta.dt` (when the edit
   happened)**. This matters: after an outage the replayed events carry an *old*
   `meta.dt`, so a `meta.dt` watermark would judge the replay "late" and silently drop
   it — data loss. `kafka_timestamp` is current for a replay, so the duplicate stays
   inside the watermark and is deduped, never dropped. The 2-hour delay bounds the
   in-stream state and covers a producer reconnect-and-replay window; a duplicate that
   arrives more than 2 hours after the original falls outside the watermark and is
   caught instead by layer 2's MERGE.
2. **An insert-only MERGE** (`foreachBatch`) that inserts a `meta.id` only when it isn't
   already present. This is the actual uniqueness guarantee — it holds across all Silver
   history, not just the watermark window — and makes a retried micro-batch a no-op.

### Username hashing

Silver never stores a plaintext username. The `user` field is replaced by
`user_hash = SHA-256(salt + username)`; the salt lives **outside the repo**
(`$VANTAGE_USER_SALT` or `~/.config/vantage/user_salt`) and is never logged. Silver
**refuses to run** if the salt is missing, so it can't quietly fall back to unsalted or
empty-salt hashes. The raw `user`, `comment`, `parsedcomment` and `log_*` fields are
never carried past the parse.

### The restart test

[docs/restart-test.md](docs/restart-test.md) crashes the stream and the producer on
purpose and checks three invariants against the producer's delivery log. On the
2026-11-05 run the stream was **`kill -9`'d mid-batch** (04:09:13 UTC, on Bronze
batch 36 / Silver batch 11) to test checkpoint recovery, and later the producer was
**`kill -9`'d and restarted** (04:19:28 UTC) to force an upstream replay. The results:
Bronze held **53,488** rows, every `(partition, offset)` once and contiguous from 0 on
all three partitions; Silver held **53,480** rows, exactly the 53,480 distinct logged
ids (**0 lost, 0 duplicates**); and the producer's post-restart replay created upstream
duplicates that Silver absorbed — **8 duplicate Bronze rows collapsed to the 53,480
distinct events**. All three checks pass.<sup>[b]</sup>

### Monitoring and schema drift

`scripts/stream_all.py` writes one `run_metrics` row per stream stage per run (not per
micro-batch) — `stream:bronze` and `stream:silver` — with throughput from each query's
progress, Kafka consumer lag, and Silver freshness. A representative ~5-minute live
run:<sup>[c]</sup>

| | rows in | input rows/s | processed rows/s | batch ms | Kafka lag (p0/p1/p2) | freshness |
|---|---|---|---|---|---|---|
| `stream:bronze` | 1,474 (11 batches) | 4.2 | 65.9 | 2,236 | 25 / 29 / 34 = **88** | — |
| `stream:silver` | 1,350 (10 batches) | 7.7 | 43.3 | 3,227 | — | **53 s** |

Where each number comes from: *rows in, input/processed rows per second* and *batch ms*
are aggregated from the StreamingQuery's own progress reports (mean over non-empty
batches); *Kafka lag* is the broker's latest offset minus what Bronze committed, per
partition (a small positive lag is the normal unread tail while the producer keeps
publishing, not loss); *freshness* is `now −` the newest edit time in Silver. Kafka lag
is recorded on the Bronze stage (the Kafka consumer); freshness on the Silver stage
(where edit time exists).

Schema drift is handled two ways, on purpose:

- **An unknown field is ignored but counted.** The parse keeps only the fields Silver
  uses, so a new feed field is skipped — and Bronze still holds the whole raw JSON. On
  top of that the stream compares each event's keys against the known field set, counts
  any event carrying extras, logs a `schema_drift` warning naming the new fields, and
  records a `drift_events` count in `run_metrics`.
- **A wrong-type required field is quarantined.** A value that is valid JSON but doesn't
  fit the schema (e.g. `namespace` arriving as the string `"0"`) is quarantined as
  `schema_mismatch`, told apart from a genuinely unparseable line (`bad_json`). All three
  cases are covered by `tests/test_schema_drift.py`.<sup>[d]</sup>

### Backfill and the `source` column

The producer went live on 2026-11-05, but the pageview Silver covers **2026-10-15 to
2026-10-18**. To give the Gold join overlapping hours, the recent-changes events captured
for that October window are backfilled through the *same* Silver code
(`scripts/backfill_edits.py`) — same schema, quarantine, title normalization, username
hashing and insert-only MERGE on `meta.id`. A `source` column tags where each row came
from: `capture` for the backfilled October edits, `kafka` for the live stream. The
backfill inserted **945,776** capture rows with **0 quarantined**.<sup>[e]</sup> The
`source` column is what lets the restart test and the live freshness metric look at only
the streamed rows, and keeps the historical backfill from being mistaken for live data.

### Co-spike results

Gold's `co_spikes` table joins the pageview spikes to per-hour human-edit counts on
normalized title + UTC hour. Over the October window it scored **76,848** `(title, hour)`
rows: **2,027** view spikes, **40** human-edit spikes, and **27 co-spikes** (both in the
same hour) across **16 titles**.<sup>[f]</sup> The edit-spike z-score uses **human edits
only** — bot edits never enter it — so a page edited only by bots can't produce an
edit spike and therefore **can't co-spike**.

- The co-spikes are dominated by **`Katie_Zacharia`**, which co-spiked five hours running
  on 2026-10-16 (peaking at **125,379 views with 42 human edits** at 18:00).
- **`2026_Silver_Rail_Elementary_School_shooting`** — **8,382 views, 32 human edits** at
  2026-10-16 00:00 → `co_spike = true`.
- **`Ying`** — the window's largest view spike, **886,853 views** at 2026-10-17 17:00, but
  **0 edits** → `edit_spike = false`, so `co_spike = false`. Attention without editing is
  exactly what a co-spike should *not* flag.

### Phase 2 results

| Metric | Value | Source |
|---|---|---|
| Producer 30-min validation: published / confirmed / in topic | 9,970 / 9,970 / 9,970, 0 failed, 3 reconnects, 0 loss, 0 dup | [a] |
| Restart test — Bronze rows, all `(partition, offset)` once & contiguous | 53,488 (p0 16,548 / p1 18,958 / p2 17,982) | [b] |
| Restart test — Silver rows = distinct logged ids (0 lost) | 53,480 = 53,480 | [b] |
| Restart test — upstream duplicates Silver removed | 8 | [b] |
| Backfilled capture rows into Silver edits / quarantined | 945,776 / 0 | [e] |
| Silver edits by source at backfill (capture + kafka) | 999,256 = 945,776 + 53,480 | [e] |
| Gold co-spikes: scored rows / view spikes / edit spikes / co-spikes / titles | 76,848 / 2,027 / 40 / 27 / 16 | [f] |
| Live run (~5 min): Bronze rows in / Kafka lag / Silver freshness | 1,474 / 88 / 53 s | [c] |
| Schema-drift events in that run | 0 | [c] |
| Tests | 93 passed | [g] |

<sub>

**Sources.** [a] Day 13 producer validation run: the confirmed-delivery log
`data/stream/published.jsonl` (one `{meta_id, partition, offset}` line per confirmed
delivery; its first 9,970 lines split p0 3,165 / p1 3,796 / p2 3,009) and the producer's
run summary in `logs/producer.jsonl` (published 9,970 / confirmed 9,970 / failed 0 /
3 reconnects). [b] [docs/restart-test.md](docs/restart-test.md),
run 2026-11-05 (`scripts/restart_test.py`, all three checks PASS). [c] `run_metrics` rows
`stage = stream:bronze` / `stream:silver`, `mode = continuous`, from a ~5-minute
`scripts/stream_all.py` run on 2026-11-05 (297.5 s). [d] `tests/test_schema_drift.py`
(unknown field kept + counted; wrong-type `namespace` → `schema_mismatch`; missing
required field → quarantine). [e] `scripts/backfill_edits.py` run output (rows inserted,
0 quarantined) and a `GROUP BY source` on `silver_edits` at that point. [f] `count()`
and filters on `gold/co_spikes` (`scripts/build_co_spikes.py`); thresholds
`z_min = 6`, `edits_floor = 10`, `min_jump = 10`. [g] `pytest -q`, 93 passed locally.

</sub>

### Honest limits (Phase 2)

- **One laptop, one broker.** Kafka runs as a single broker with **replication factor 1**,
  so there is no failover; a lost broker volume would lose unconsumed messages. That is a
  deliberate local-scope choice, documented, not a production topology.
- **The edit history has a 21-hour gap on 2026-10-26 to 10-27**, when the capture was
  down. It falls outside the Gold join window and so doesn't affect the current results,
  but it's a real gap in the recorded feed.
- **Edits are joined only on the 2026-10-15 to 10-18 window so far.** That's the window
  the pageview Silver covers and the October edits were backfilled for; the live stream
  (November) accumulates for a future, wider join rather than being joined to views yet.

## Phase 3: Azure

Phase 3 runs the Phase 1 batch pipeline against Azure and checks that it gives the same
numbers as the laptop. `--target azure` (or `VANTAGE_TARGET=azure`) points the batch tables at
ADLS Gen2. The rest is the same code plus a small Databricks mode:
- **Session:** reuse the cluster's Spark session, read the service principal from a secret
  scope.
- **Entry point:** a fallback for the missing `__file__`, since Databricks `exec()`s the script.
- **Cluster:** a Java 17 setting on the job cluster.

### What ran in Azure and what stayed local

| Where | What |
|---|---|
| **Azure, ADLS Gen2** (`vantagelakee7ad27`, container `lake`, hierarchical namespace on) | Bronze, Silver, Gold, quarantine and `run_metrics` Delta tables for the batch pipeline. Day 23's `landing/` copy of one day of raw files. |
| **Azure Databricks** (Premium workspace, job cluster only) | Silver and Gold, as a [Databricks Asset Bundle](databricks.yml) job running `scripts/run_pipeline.py`. See [docs/databricks.md](docs/databricks.md). |
| **Azure Data Factory** (`adf-vantage`) | One pipeline copying a day of hourly pageview files from the Wikimedia dumps into `landing/`. See [docs/data-factory.md](docs/data-factory.md). |
| **Laptop** | The raw `.gz` files, the Bronze load to ADLS (Day 20), and all of Phase 2: Kafka, the producer, the streaming tables and their checkpoints. |

Storage auth:
- Spark uses a service principal (`vantage-sp`) with one role, Storage Blob Data Contributor,
  scoped to the storage account. On the laptop its secret lives outside the repo; on
  Databricks it comes from a secret scope.
- Data Factory uses its own managed identity with the same single role. Its linked services
  hold no keys or secrets.

### ADLS parity with local

A full run from the laptop with `--target azure` wrote all three layers to ADLS. Every count
matched the local lake:

| | Local | Laptop → ADLS | Source |
|---|---|---|---|
| Bronze rows | 152,935,529 | 152,935,529 | [P1] |
| Silver rows / duplicate keys | 142,728,341 / 0 | 142,728,341 / 0 | [P2] |
| Silver total views | 562,648,784 | 562,648,784 | [P2] |
| Gold rows (hourly + daily + spikes) | 84,348 (7,200 + 300 + 76,848) | 84,348 (7,200 + 300 + 76,848) | [P2] |
| `is_spike` / `likely_automated` | 2,027 / 7 | 2,027 / 7 | [P2] |
| Stage seconds: Bronze / Silver `--full` / Gold | – / 1,521 / 434 | 632 / 1,852 / 620 | [P1] [12] |

### Databricks: timings, and why it was slower

The job runs Silver and Gold on one `Standard_E4ds_v4` node (4 vCPU, 32 GB) with DBR 15.4 LTS
and Photon off. The cluster exists only for the run.

| | Local | Laptop → ADLS | Databricks, 1 node | Source |
|---|---|---|---|---|
| Cluster setup | – | – | 352 s | [P3] |
| Silver `--full`, 72 files | 1,521 s | 1,852 s | 5,044 s | [P1] [P3] [12] |
| Gold | 434 s | 620 s | 732 s | [P1] [P3] [12] |
| Job execution | – | – | 5,820 s | [P3] |

The cluster was slower than the laptop. The subscription is an Azure Free Trial, and its eastus
quota is **4 vCPUs in total** [P4]. So the job cluster is a single 4-vCPU node, with no
workers and nothing to scale out onto.

### The Java 8 vs Java 17 Georgian bug

The first full Databricks run matched Gold exactly, but **Silver had 195 extra rows**:
142,728,536 against 142,728,341, with 562,648,986 views against 562,648,784. Collisions merged
were 8,675 against 8,678 [P5]. The cause was in title normalization, which uppercases the
first letter:

- On Java 17 (the laptop and CI), Spark's `upper('ს')` maps Georgian Mkhedruli U+10E1 to
  Mtavruli `Ს` U+1CA1.
- DBR 15.4 runs Java 8 by default [P12], whose older Unicode tables leave the letter
  unchanged.
- So titles starting with a lowercase Georgian letter got a different Silver key. MERGE inserted
  them next to the existing rows, since it never deletes. 190 of the 195 new keys start with a
  Mkhedruli letter [P5].

The fix had four parts:

1. **Java 17 on the cluster:** `JNAME: zulu17-ca-amd64` on the job cluster, and the job now logs
   `java_version` and `upper('ს')` at start. The test is `tests/test_title_case.py`.
2. **RESTORE:** Silver on ADLS was rolled back with Delta `RESTORE` to version 2, the Day 20
   state: 142,728,341 rows and 562,648,784 views [P6].
3. **One-day parity rerun:** instead of a full rerun, a Silver-only run of one day
   (`--days 2026-10-15 --full`). It logged `java_version 17.0.20` and `upper_ka U+1CA1`.
   Version 7's 2026-10-15 partition matched version 2 exactly: 50,417,645 rows and
   189,432,040 views on both sides, and an `exceptAll` on `(project, title, hour, views)` of 0
   in both directions [P6].
4. **Known limitation:** MediaWiki may not capitalize Georgian titles, so the uppercase-first
   rule can produce titles that differ from Wikipedia's. Both engines now apply the same rule,
   so joins are unaffected ([docs/cleaning-rules.md](docs/cleaning-rules.md)).

### The scheduled run

The job then ran once on its own, from a schedule:
- **Schedule:** Quartz cron `0 21 17 10 11 ? 2026`, America/New_York. The year makes it fire
  once.
- **Run:** trigger `PERIODIC`, result SUCCESS [P7].
- **Job:** the normal run, `--target azure --stages silver,gold` without `--full`. The job
  cluster was tagged `vantage_run=day22-scheduled` for cost tracking.
- **Afterwards:** the schedule was removed and redeployed straight after the run. The workspace
  was deleted after that, and its managed resource group went with it [P8].

| | Value | Source |
|---|---|---|
| Setup / execution / run | 323 s / 869 s / 1,193 s | [P7] |
| Silver (no new Bronze files) / Gold | 63.0 s (0 rows) / 757.4 s | [P1] |
| Gold built from Silver version | 7 (current Silver version 7) | [P1] [P2] |
| Silver rows / duplicate keys / views | 142,728,341 / 0 / 562,648,784 | [P2] |
| Gold rows / `is_spike` / `likely_automated` | 84,348 / 2,027 / 7 | [P2] |
| Gold spikes `exceptAll` vs local Gold spikes | 0 rows both ways | [P2] |

### Data Factory: a day of files with matching MD5s

`pl_copy_pageviews_day(day)` takes a day and copies its 24 hourly files from
`dumps.wikimedia.org` into `lake/landing/pageviews/<yyyy>/<yyyy-MM>/`. A file is named by the
end of its hour, so day D runs from `…-D-010000` to `…-(D+1)-000000`.
- **Defined in the repo:** the definitions are JSON in [`infra/adf/`](infra/adf/), deployed
  with `az datafactory`.
- **Copy settings:** a sequential ForEach copies one file at a time, using 2 DIU per copy.

| Run for 2026-10-15 (manual trigger) | Value | Source |
|---|---|---|
| Pipeline status / duration | Succeeded / 916 s | [P9] |
| Copy activities | 24 of 24 Succeeded, 26–75 s each, 2 DIU each | [P9] |
| Bytes read = written | 1,419,840,372 | [P9] |
| MD5 of each landed file = MD5 of the laptop's copy | 24 / 24 | [P10] |

### Costs

All costs are in USD from the Azure Cost Management view. They were read on
2026-11-12 04:31 UTC [P11].

| | Cost |
|---|---|
| Day 22 scheduled run only (tag `vantage_run=day22-scheduled`) | VM $0.1104, Databricks DBU $0.0807, public IP $0.0031, total **$0.1943** |
| Data Factory run, 2026-11-11 UTC | data movement $0.1000, orchestration activity runs $0.0250, read/write ops $0.0002, monitoring $0.0002, total **$0.1253** |
| **Phase 3 so far, by service** | Virtual Machines $0.8256, Azure Databricks $0.7248, Storage $0.1325, Data Factory $0.1253, Virtual Network (public IPs) $0.0190, Bandwidth $0.0005, total **$1.8277** |

Costs post with a delay of up to a day, so 2026-11-11 may still grow a little. Storage keeps
billing while the ADLS account exists.

<sub>

**Sources.**
- [P1] `run_metrics` on ADLS: run `c48da087…` (2026-11-06, laptop → ADLS full run), run
  `231c210a…` (Databricks full run), run `1d509f52…` (scheduled run; Gold `silver_version` 7).
  Local Silver and Gold seconds are from [12].
- [P2] Direct queries from the laptop on the ADLS tables: Silver `count()`, `GROUP BY
  (project, title, hour) HAVING count > 1`, `sum(views)`; `count()` on each Gold table, with
  `is_spike` / `likely_automated` filters. Also the Delta history version, and `exceptAll`
  against `data/lake/gold/spikes` (76,848 rows each side).
- [P3] Databricks Jobs API (`jobs get-run`, run 974171125724364): `setup_duration`,
  `execution_duration`. Stage seconds are from [P1].
- [P4] `az vm list-usage -l eastus` (Total Regional vCPUs limit 4). The subscription's
  `subscriptionPolicies` (ARM `GET /subscriptions/{id}`) report `quotaId FreeTrial_2014-09-01`
  and `spendingLimit On`.
- [P5] The same queries as [P2] after Databricks run 974171125724364, plus a key diff of Silver
  version 5 against version 2.
- [P6] Delta `RESTORE TABLE … TO VERSION AS OF 2` and counts. Databricks run 145895131916361's
  `runtime_check` log. Time-travel comparison of the 2026-10-15 partition at versions 7 and 2.
- [P7] Databricks Jobs API, run 636280560090751: `trigger = PERIODIC`, `result_state =
  SUCCESS`, `setup_duration`, `execution_duration`, `run_duration`.
- [P8] `jobs get` shows no schedule. `az databricks workspace delete`, then
  `az group exists -n rg-vantage-dbx-managed` returned `false`.
- [P9] Data Factory run `8cc7f424-dd6c-11f1-a9fd-dfddbfd54fa0`: `pipeline-run show` and
  `activity-run query-by-pipeline-run` (`dataRead`, `dataWritten`, `filesWritten`,
  `usedDataIntegrationUnits`, `durationInMs`).
- [P10] Spark `binaryFile` read of `landing/pageviews` (`md5(content)`) compared with
  `hashlib.md5` of each file in `data/raw/pageviews`.
- [P11] Cost Management `query` API (ActualCost): filtered by the tag; grouped by resource and
  meter for 2026-11-11; grouped by service from 2026-10-01. There were no Azure resources
  before Day 19.
- [P12] Azure Databricks docs, Databricks SDK for Java, "Create a cluster with a specific JDK
  version": "For Databricks Runtime versions 13.1 to 15.4, JDK 8 is the default, and JDK 17
  is in Public Preview." It gives `JNAME=zulu17-ca-amd64` for JDK 17.
  https://learn.microsoft.com/en-us/azure/databricks/dev-tools/sdk-java

</sub>

### Honest limits (Phase 3)

- **Free Trial subscription.** It has a spending limit and a 4-vCPU regional quota. The
  Databricks trial tier is refused on Free Trial subscriptions, so the workspace was Premium,
  with DBUs billed.
- **Single node.** The cluster has no workers, so these timings say nothing about how the job
  scales. They are slower than the laptop.
- **Development-mode bundle.** The job was deployed in Databricks Asset Bundle development
  mode, so it got a `[dev <user>]`-prefixed name and lived under the user's workspace
  folder. The schedule needed an explicit `pause_status: UNPAUSED`. It was never
  deployed in production mode, with a service principal as the run identity.
- **Preview runtime setting.** On DBR 15.4, JDK 17 is Public Preview [P12], so the Silver
  parity fix relies on a preview runtime setting.
- **Bronze was not built in the cloud.** The raw files live on the laptop, which loaded Bronze
  to ADLS. Data Factory's landing copy proves ingestion into Azure works, but the pipeline
  doesn't read from `landing/` yet.
- **Phase 2 stayed local.** Kafka and the streaming tables were not moved to Azure.
- **Workspace deleted.** The Databricks workspace is gone, so the job can't be rerun without
  recreating it ([docs/databricks.md](docs/databricks.md) has the setup).

## How to run

Requires **Python 3.11** and **Java 17**. Spark 3.5 supports Java 8, 11 and 17.
On this machine the default JDK 23 crashed Spark with a `getSubject` / security
manager error, so point `JAVA_HOME` at 17:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17    # macOS/Homebrew; adjust for your box

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

pytest -q                                        # 119 tests

# batch pipeline: bronze -> silver -> gold, one run_id, run_metrics row per stage
python scripts/run_pipeline.py --start 2026-10-15T00 --end 2026-10-18T00
python scripts/run_pipeline.py --full            # reprocess every file through Silver

# maintenance (each in its own Spark session; run on their own, not chained)
python scripts/optimize_silver.py                # compact Silver
python scripts/vacuum_tables.py                  # VACUUM RETAIN 24h, reclaim stale versions
```

A failed stage stops the run, marks the rest skipped, and exits non-zero. Each
run writes one JSON line per stage to `logs/pipeline.jsonl` and one row per stage
to the `run_metrics` Delta table.

The streaming side needs the Kafka broker up and the username salt set:

```bash
docker compose up -d                             # single-broker Kafka (KRaft)
export VANTAGE_USER_SALT=...                      # or ~/.config/vantage/user_salt

python scripts/produce_edits.py                  # live recent-changes -> Kafka (its own tab)

# Kafka -> Bronze edits -> Silver edits, both queries in one driver
python scripts/stream_all.py                     # continuous, until Ctrl-C (clean stop)
python scripts/stream_all.py --once              # drain what's available, then stop
# each run writes stream:bronze / stream:silver rows to run_metrics

python scripts/restart_test.py                   # the three restart invariants (exit 0 = pass)
python scripts/build_co_spikes.py                # rebuild Gold co_spikes
```

## Roadmap

- **Phase 1 — batch lakehouse (this repo).** Pageviews → Bronze → Silver → Gold
  on local Delta, with CI. Done.
- **Phase 2 — streaming. Done.** The live recent-changes edit stream fed through Kafka
  and Spark Structured Streaming into its own Bronze and Silver, deduped and privacy-hashed,
  surviving a [restart test](docs/restart-test.md), and joined to the pageview spikes on
  normalized title + UTC hour to add the edit co-spike signal — with stream metrics and
  schema-drift handling in `run_metrics`. See [Phase 2 — streaming the edit feed](#phase-2--streaming-the-edit-feed).
- **Phase 3 — cloud. Done.** The batch tables on ADLS Gen2 match the local run. Silver and
  Gold ran as a Databricks job, including one scheduled run, after a Java 8 vs 17
  title-casing bug was fixed. Data Factory copied a day of raw files with matching MD5s.
  See [Phase 3: Azure](#phase-3-azure).

---

A data engineering portfolio project by Kumkum Sareen.

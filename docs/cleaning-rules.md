# Vantage cleaning rules

Every filter, drop, rename, threshold and quarantine reason applied to the two
Wikipedia feeds before they reach the analysis tables, each with the reason for
it. These rules are the spec that the Phase 1 PySpark pipeline reimplements, so
they are meant to be exact. Scope is English Wikipedia only: `en` (desktop) and
`en.m` (mobile).

## Hour convention

- A pageview file `pageviews-YYYYMMDD-HH0000.gz` covers the hour **ending** at
  `HH:00`, so it is labelled by its **start** hour `HH−1:00` UTC. ([T205457])
- Edits are bucketed by truncating `meta.dt` to the hour (UTC), so an edit joins
  the pageview hour it happened in.
- The analysis window is the set of hours that have a pageview file. Hours with
  no file are left out, never invented; an internal gap (a missing hour between
  two present ones) stops the run rather than being filled.

## Pageviews (batch)

**Scope**
- Keep only lines whose project is `en` or `en.m`; drop all others at ingest —
  a scoping decision that keeps the data laptop-sized, not a quality rule.

**Parsing**
- Split each line once from the left (project) and twice from the right (views,
  bytes); the title is everything in between — titles contain spaces and colons,
  so a plain space-split would corrupt them.
- A surrounding pair of double quotes on a title is unwrapped and `\` escapes
  inside it are removed — the dump wraps titles that contain a double quote in quotes.

**Title normalization** (shared with edits)
- URL-decode percent escapes; `+` is kept literal (e.g. `C++`), not turned into
  a space — only `%XX` is an escape in these titles.
- Trim whitespace, replace spaces with underscores, and uppercase the **first
  character only**. Never lowercase: Wikipedia titles are case-sensitive after
  the first letter.

**Drops**
- Drop non-article namespaces by title prefix before the first colon
  (`Talk:`, `User:`, `Wikipedia:`, `File:`, `Category:`, `Template:`, `Portal:`,
  `Draft:`, `Module:`, `Special:`, … and their `_talk` forms), matched
  case-insensitively — Vantage measures article attention, not meta pages.
  Article titles that merely contain a colon (e.g. `Star_Wars:_Episode_IV`) are
  kept; `Main_Page` is kept.
- Drop the title `-`. It's a placeholder, not an article (15,640 views in one sample hour).

**Collisions**
- Rows that normalize to the same `(project, title, hour)` are the same page
  under different raw spellings (encoding, whitespace, `%20` vs `_`); their views
  are **summed**, not deduplicated to one — dropping would undercount.

## Edits (streaming capture)

Source is the recent-changes stream, captured to dated JSONL (split by receive
date, so a window may span more than one file). Flattened with `json_normalize`.

**Scope / drops**
- Drop events with `meta.domain == "canary"` — monitoring heartbeats, not edits.
- Keep `wiki == "enwiki"`.
- Keep `type` in (`edit`, `new`); drop `log` and `categorize` — only actual
  article revisions count.
- Keep `namespace == 0` (articles) — matches the pageview scope so the two join.

**Time bucketing — `meta.dt`, not `timestamp`**
- The hour is `meta.dt` truncated to the hour in UTC, not the top-level epoch `timestamp`.
- `meta.dt` is an ISO 8601 UTC string with milliseconds, so truncating it to the hour
  is unambiguous and needs no conversion step.
- In the captured data the two fields agree to the second; using one field everywhere
  keeps the rule simple and identical in pandas and Spark.

**Title normalization**
- Same trim / spaces-to-underscores / uppercase-first-character rule as pageviews,
  so the same page lands on the same string in both feeds.
- Edit titles are **not** URL-decoded — stream titles arrive already decoded, and
  decoding again would corrupt a title containing a literal `%`.

**Dedup and counts**
- Deduplicate on `meta.id` (the stream's unique event id); capture restarts can
  re-deliver an event, and Silver must count it once.
- Output one row per `(title, hour)` with `human_edits` and `bot_edits` counted
  separately on the `bot` flag — the edit-spike signal uses human edits only.

**Null handling**
- `bot` missing → counted as human.
- `meta.id` missing → quarantine (cannot deduplicate).
- `meta.dt` missing or unparseable → quarantine (cannot place in an hour).
- `title` missing or blank → quarantine.
- `type` / `namespace` / `wiki` / `meta.domain` missing → fails its keep-filter and
  is dropped as out-of-scope, not quarantined.
- Fields we do not use (user, comment, revision, …) are dropped; usernames are
  not carried past cleaning.

### Streaming path (Phase 2 Silver)

The rules above are the Phase 0 pandas cleaner over the captured JSONL. The Phase 2
streaming Silver (Kafka → Bronze → Silver, Day 15) parses the same events but keeps
a different, wider set of rows:

- **Keeps all event types and all namespaces.** The streaming Silver does **not**
  apply the `edit`/`new` + `namespace == 0` scope filter. The Day 16 restart test
  checks Silver's `meta.id` set against every id the producer logged, so Silver must
  hold one row per distinct event; the `edit`/`new` + namespace-0 filter is applied
  later, in the Day 17 edits-to-views aggregation. Each row carries `type` and
  `namespace` so that filter can run downstream.
- **Dedup is two layers.** (a) `dropDuplicatesWithinWatermark` on `meta.id` with the
  watermark on the **Kafka ingest time**, not `meta.dt`: after an outage the producer
  replays old events, whose `meta.dt` is old, so a `meta.dt` watermark would mark the
  replay "late" and drop it. (b) A `foreachBatch` MERGE that inserts a `meta.id` only
  when not already present — the actual uniqueness guarantee (across all Silver
  history, not just the watermark), and a no-op for a retried micro-batch.
- **Title** is kept raw and normalized into `title_norm` with the same
  trim / spaces-to-underscores / uppercase-first rule (no URL-decode).
- **Privacy.** The username is replaced by `user_hash = SHA-256(salt + username)`.
  The salt lives outside the repo (`$VANTAGE_USER_SALT` or `~/.config/vantage/user_salt`)
  and is never logged; Silver refuses to run if it is missing. The raw `user`,
  `comment`, `parsedcomment` and `log_*` fields are never written to Silver.
- **Quarantine** uses the four shared reasons (`bad_json`, `missing_meta_id`,
  `bad_meta_dt`, `empty_title`) plus `schema_mismatch` (see schema drift below) and
  keeps the raw message value with its Kafka partition and offset.

### Schema drift (Phase 2, Day 18)

The recentchange feed can change shape. Two kinds of change are handled on purpose:

- **An unknown field is ignored, not dropped silently.** `RECENTCHANGE_SCHEMA` keeps
  only the subset Silver uses, so `from_json` simply skips any field outside it — and
  Bronze still holds the whole raw JSON, so nothing is lost. On top of that, the parse
  compares each event's top-level keys against the known field set (`KNOWN_FIELDS`, the
  exact union of keys seen across every captured and streamed event) and records the
  extras in a `drift_fields` column. The Silver stream counts the events carrying extras,
  logs a `schema_drift` warning listing the new field names, and writes the per-run count
  to `run_metrics` (`drift_events`). Drift is a signal to look, not a failure: the row
  is still clean and still reaches Silver.
- **A required field with the wrong type is quarantined.** A value that is valid JSON but
  doesn't fit the schema — e.g. `namespace` arriving as the string `"0"` instead of an
  integer — makes `from_json` reject the whole record. That is told apart from a genuinely
  unparseable line (which can't report any keys) and quarantined as `schema_mismatch`,
  distinct from `bad_json`. A required field that is simply missing still quarantines under
  its existing reason (`missing_meta_id`, `bad_meta_dt`, `empty_title`).

## Spike score

Per title, views are summed across `en` + `en.m` per hour; a title is kept only
if it reaches the views floor in at least one window hour, and its missing hours
inside the window are zero-filled.

- Robust z-score over the previous `BASELINE_HOURS` hours, **excluding the
  current hour**:
  `z = (views − median) / (1.4826 × max(MAD, 1))`.
  Median/MAD (not mean/std) resist the very spikes we are hunting; `max(MAD, 1)`
  stops a flat page from dividing by zero; `1.4826` scales MAD to a standard-
  deviation equivalent.
- A spike needs **all three**: `z ≥ Z_MIN`, `views ≥ VIEWS_FLOOR`, and
  `views − median ≥ MIN_JUMP`. Most pages sit near zero most hours, so `z` alone
  flags every small blip; the views floor and the absolute jump are what keep the
  result to genuine movers.
- The first `BASELINE_HOURS` hours of the window are warm-up only (no full
  baseline) and are never flagged.

### `likely_automated` (a label, not a drop)

A flagged spike is additionally labelled `likely_automated` when **both** hold:

- **Desktop-dominated:** `en_views / views ≥ DESKTOP_SHARE_MIN` (0.8). Organic
  attention on English Wikipedia skews mobile; a burst that is almost all desktop
  is a signature of scrapers, redirects, or a single automated client.
- **Isolated:** the hour immediately before and the hour immediately after both
  have `views < ISOLATION_RATIO × views` (0.05) — near-zero on both sides. A real
  story ramps up and decays over neighbouring hours; an automated burst is one
  hour with nothing around it. A neighbour hour **outside the window is unknown**,
  so the label is `false` there (isolation cannot be confirmed).

The label never changes `is_spike`; it is an extra column so an automated-looking
spike can be filtered out or inspected separately. The spikes table keeps
`en_views`, `mobile_views` and the two neighbour view counts so the label is
auditable.

Evidence, from the 2026-10-15 00:00 – 2026-10-18 00:00 UTC window:

- `Ying` — the window's single largest spike: **886,853 views in the one hour
  2026-10-17 17:00**, 90% desktop (en 798,356 / en.m 88,497), both neighbour hours
  ≤ 1 view, and **zero edits** in the entire window. Labelled `likely_automated`.
- `Hôtel_de_Ville,_Limoges` — flagged 19:00–23:00 on 2026-10-17, but a clean
  **mobile-heavy ramp** (20:00 was en 2,464 / en.m 4,368 — 36% desktop) spread over
  several neighbouring hours with 60 co-occurring human edits. **Not** labelled.

Current thresholds (in `config/vantage.toml`, re-exported by `prototype/config.py`):
`BASELINE_HOURS = 24`, `Z_MIN = 6`, `VIEWS_FLOOR = 1000`, `MIN_JUMP = 1000`,
`DESKTOP_SHARE_MIN = 0.8`, `ISOLATION_RATIO = 0.05`. Tuned on 43 hours of data,
2026-10-15 00:00 to 2026-10-16 18:00 UTC (hour start) to surface the obvious top
movers without flagging the long tail of small blips.

### Edit spike and co-spike (Day 17)

Edits are joined to the view spikes to find pages where attention and editing surge
together. The edit side comes from the streaming edits Silver (Oct 15-18 is backfilled
from the captured JSONL through the same Silver code, tagged `source = 'capture'`; the
live-streamed rows are `source = 'kafka'`).

- **Edit counts per page per hour.** From edits Silver, only `type` in (`edit`, `new`)
  and `namespace == 0` — the actual-article-revision filter the streaming Silver defers
  to here. `hour` is `meta.dt` truncated to the UTC hour, so an edit at 14:59 and the
  views in `pageviews-...-150000.gz` (hour 14:00) share hour 14:00. `title` is
  `title_norm`, so a space-vs-underscore spelling joins the views. `human_edits` and
  `bot_edits` are counted separately on the `bot` flag (missing `bot` → human).
- **Edit spike.** The same robust z-score as views, on `human_edits`: previous
  `baseline_hours` (current hour excluded), `max(MAD, 1)`, with its own `edits_floor`
  and `min_jump`. A title is scored only if it reaches `edits_floor` human edits in at
  least one hour. **Bot edits never enter the score.**
- **co_spike** = `is_spike AND edit_spike` in the same hour — a view spike and a human-edit
  spike together. It never changes `is_spike` or `edit_spike`; it is an extra column.

Edit-spike thresholds live in `[edit_spikes]` of `config/vantage.toml`
(`baseline_hours`, `z_min`, `edits_floor`, `min_jump`).

## Quarantine reasons

A quarantined row is never dropped silently and never crashes the run; it is kept
with its raw line and a reason.

| Feed | Reason | Meaning |
|---|---|---|
| Pageviews | `bad_utf8` | line is not valid UTF-8 |
| Pageviews | `wrong_field_count` | line did not split into project/title/views/bytes |
| Pageviews | `bad_views` | views field is not a non-negative integer |
| Pageviews | `bad_bytes` | bytes field is not a non-negative integer |
| Pageviews | `unbalanced_quote` | title opens a quote it never closes |
| Pageviews | `bad_url_encoding` | percent escape decodes to invalid UTF-8 |
| Pageviews | `empty_title` | title is empty after normalization |
| Edits | `bad_json` | line is not a JSON object at all |
| Edits | `schema_mismatch` | valid JSON, but a field has the wrong type (e.g. `namespace` as a string) |
| Edits | `missing_meta_id` | no `meta.id` to deduplicate on |
| Edits | `bad_meta_dt` | `meta.dt` missing or unparseable |
| Edits | `empty_title` | title is empty after normalization |

[T205457]: https://phabricator.wikimedia.org/T205457

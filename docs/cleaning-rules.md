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

Current thresholds (in `prototype/config.py`): `BASELINE_HOURS = 24`,
`Z_MIN = 6`, `VIEWS_FLOOR = 1000`, `MIN_JUMP = 1000`. Tuned on 43 hours of data, 2026-10-15 00:00 to 2026-10-16 18:00 UTC (hour start)
to surface the obvious top movers without flagging the long tail of small blips.

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
| Edits | `bad_json` | line is not valid JSON |
| Edits | `missing_meta_id` | no `meta.id` to deduplicate on |
| Edits | `bad_meta_dt` | `meta.dt` missing or unparseable |
| Edits | `empty_title` | title is empty after normalization |

[T205457]: https://phabricator.wikimedia.org/T205457

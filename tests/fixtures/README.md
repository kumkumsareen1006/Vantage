# Test fixtures

## pageviews-20261015-120000.gz

A ~4,900-line sample of the real English Wikipedia pageview dump for the hour
ending 2026-10-15 12:00 UTC (file `pageviews-20261015-120000.gz`), taken from
the Wikimedia `pageviews` dumps at https://dumps.wikimedia.org/other/pageviews/.
The real name is kept so the filename -> hour logic is exercised by the tests.

The sample includes %-encoded titles, quoted titles, non-article namespaces, the
`-` placeholder, and real first-char-case collision pairs (two raw titles that
normalize to one page). Seven synthetic lines are appended at the end, one per
quarantine reason (`bad_views`, `wrong_field_count`, `bad_bytes`,
`unbalanced_quote`, `bad_url_encoding`, `empty_title`, and a genuinely invalid
UTF-8 line for `bad_utf8`).

Wikimedia pageview data is released under CC0 (public domain).

# Data Factory (Phase 3, Day 23)

An Azure Data Factory pipeline copies one UTC day of hourly pageview files from the
Wikimedia dumps site into the lake's landing folder. The definitions are JSON in
[`infra/adf/`](../infra/adf/), deployed with `az datafactory` by `infra/adf/deploy.sh`.

| | |
|---|---|
| Factory | `adf-vantage` (V2), `rg-vantage`, eastus, tag `project=vantage` |
| Identity | system-assigned managed identity, one role: Storage Blob Data Contributor on `vantagelakee7ad27` only |
| Source | `ls_wikimedia_dumps`: HTTP, anonymous, `https://dumps.wikimedia.org/other/pageviews/`. The Vantage User-Agent is sent from the copy source's `additionalHeaders`. |
| Sink | `ls_vantage_lake`: ADLS Gen2, URL only, so the managed identity is used. No keys or secrets. |
| Datasets | `ds_wikimedia_pageview_file`, `ds_landing_pageview_file` (Binary, parameterized path) |
| Pipeline | `pl_copy_pageviews_day(day)`: sequential ForEach over hours 1-24, one Copy per file, 2 DIU, no retry |

A file is named by the end of its hour, so day D is `pageviews-D-010000.gz` through
`pageviews-(D+1)-000000.gz`. Files land in `lake/landing/pageviews/<yyyy>/<yyyy-MM>/`, with
the folder taken from the file name, as on the dumps site.

```bash
bash infra/adf/deploy.sh
az datafactory pipeline create-run -g rg-vantage --factory-name adf-vantage \
  -n pl_copy_pageviews_day --parameters '{"day":"2026-10-15"}'
```

## Run for 2026-10-15 (2026-11-11 01:09-01:24 UTC)

- Run `8cc7f424-dd6c-11f1-a9fd-dfddbfd54fa0`, manual trigger, **Succeeded** in 916 s.
  ForEach took 908 s.
- 24 Copy activities, all Succeeded: 24 files, 1,419,840,372 bytes read and written. Each
  copy took 26-75 s and used 2 DIU, billed 0.0167 DIU-hours each.
- Verification: 24 files in landing. The MD5 of each one equals the MD5 of the same file in
  `data/raw/pageviews` on the laptop (24/24).

The landing copy is not read by the pipeline; Bronze still loads from the laptop's raw files.

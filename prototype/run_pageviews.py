"""
Run the Day 2 pageview cleaning on one or more hourly files and print a summary.

Run from the project root:
    python -m prototype.run_pageviews data/raw/pageviews/pageviews-20261006-120000.gz
"""
import sys
import time

import pandas as pd

from prototype.pageviews import clean_pageview_files


def mb(df_or_series) -> float:
    return df_or_series.memory_usage(deep=True).sum() / 1e6 if isinstance(df_or_series, pd.DataFrame) \
        else df_or_series.memory_usage(deep=True) / 1e6


def main(paths):
    if not paths:
        raise SystemExit("Usage: python -m prototype.run_pageviews <pageview .gz files>")

    start = time.perf_counter()
    result = clean_pageview_files(paths)
    seconds = time.perf_counter() - start

    for s in result.stats["per_file"]:
        print(f"\n== {s['file']}  (hour starting {s['hour']:%Y-%m-%d %H:%M} UTC)")
        print(f"lines in file           {s['lines_total']:>12,}")
        print(f"lines for en / en.m     {s['lines_in_scope']:>12,}")
        print(f"quarantined             {s['quarantined']:>12,}  {s['quarantine_by_reason']}")
        print(f"dropped (not articles)  {s['dropped_non_article']:>12,}")
        print(f"collisions merged       {s['collisions_merged']:>12,}")
        print(f"clean rows out          {s['rows_out']:>12,}")

    clean, quarantine = result.clean, result.quarantine
    print(f"\nTotal: {len(clean):,} clean rows, {len(quarantine):,} quarantined, {seconds:.1f}s")

    if len(quarantine):
        print("\nQuarantine sample (up to 3 per reason):")
        sample = quarantine.groupby("reason", group_keys=False).head(3)
        print(sample[["reason", "raw_line"]].to_string(index=False, max_colwidth=100))

    print("\nTop 10 pages (en + en.m summed):")
    top = (clean.groupby("title", as_index=False)["views"].sum()
                .nlargest(10, "views"))
    print(top.to_string(index=False))

    print("\nMemory, string vs categorical:")
    for col in ["project", "title"]:
        print(f"  {col:<8} string {mb(clean[col]):8.1f} MB   "
              f"category {mb(clean[col].astype('category')):8.1f} MB")


if __name__ == "__main__":
    main(sys.argv[1:])

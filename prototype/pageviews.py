import gzip
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote_to_bytes

import pandas as pd

from prototype import config
from prototype.normalize import normalize_title_series

FILENAME_RE = re.compile(r"pageviews-(\d{8})-(\d{2})0000\.gz$")
QUARANTINE_COLUMNS = ["source_file", "raw_line", "reason"]


@dataclass
class CleanResult:
    clean: pd.DataFrame
    quarantine: pd.DataFrame
    stats: dict = field(default_factory=dict)


def hour_from_filename(path) -> pd.Timestamp:
    m = FILENAME_RE.search(Path(path).name)
    if not m:
        raise ValueError(f"Not a pageview file name: {path}")
    end = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H").replace(tzinfo=timezone.utc)
    return pd.Timestamp(end - timedelta(hours=1))


def _quarantine(source_file, raw_lines, reason) -> pd.DataFrame:
    raw_lines = pd.Series(raw_lines, dtype="string")
    return pd.DataFrame({
        "source_file": pd.Series([source_file] * len(raw_lines), dtype="string"),
        "raw_line": raw_lines.reset_index(drop=True),
        "reason": pd.Series([reason] * len(raw_lines), dtype="string"),
    })


def read_lines(path, projects=config.PROJECTS):
    """Read the gzip file, keeping only lines for the projects in scope.
    Returns (lines in scope, quarantine frame, total line count)."""

    prefixes = tuple(p.encode() + b" " for p in projects)
    kept, bad = [], []
    total = 0
    with gzip.open(path, "rb") as fh:
        for raw in fh:
            total += 1
            if not raw.startswith(prefixes):
                continue
            raw = raw.rstrip(b"\r\n")
            try:
                kept.append(raw.decode("utf-8"))
            except UnicodeDecodeError:
                bad.append(raw.decode("utf-8", errors="replace"))
    return (
        pd.Series(kept, dtype="string", name="raw_line"),
        _quarantine(Path(path).name, bad, "bad_utf8"),
        total,
    )


def parse_lines(lines: pd.Series, source_file: str):
    """Split raw lines into project, title, views and bytes.
    Returns (parsed frame with a raw_line column, quarantine frame).
    """
    df = pd.DataFrame({"raw_line": lines})

    first = df["raw_line"].str.split(" ", n=1, expand=True)
    df["project"] = first[0]
    rest = first[1] if 1 in first.columns else pd.Series(pd.NA, index=df.index, dtype="string")

    last = rest.str.rsplit(" ", n=2, expand=True)
    for i, col in enumerate(["title", "views", "bytes"]):
        df[col] = last[i] if i in last.columns else pd.Series(pd.NA, index=df.index, dtype="string")

    reasons = pd.Series(pd.NA, index=df.index, dtype="string")


    reasons = reasons.mask(reasons.isna() & df["bytes"].isna(), "wrong_field_count")

    # Views and bytes must be plain non-negative whole numbers.
    is_number = r"\d+"
    reasons = reasons.mask(reasons.isna() & ~df["views"].fillna("").str.fullmatch(is_number), "bad_views")
    reasons = reasons.mask(reasons.isna() & ~df["bytes"].fillna("").str.fullmatch(is_number), "bad_bytes")

    title = df["title"].fillna("")
    quoted = title.str.len().ge(2) & title.str.startswith('"') & title.str.endswith('"')
    unquoted = title.where(~quoted, title.str[1:-1].str.replace(r"\\(.)", r"\1", regex=True))
    reasons = reasons.mask(reasons.isna() & title.str.startswith('"') & ~quoted, "unbalanced_quote")
    df["title"] = unquoted

    reasons = reasons.mask(reasons.isna() & df["title"].eq(""), "empty_title")

    bad = reasons.notna()
    quarantine = pd.DataFrame({
        "source_file": pd.Series(source_file, index=df.index[bad], dtype="string"),
        "raw_line": df.loc[bad, "raw_line"],
        "reason": reasons[bad],
    }).reset_index(drop=True)

    good = df.loc[~bad].copy()
    good["views"] = good["views"].astype("int64")
    good["bytes"] = good["bytes"].astype("int64")
    return good.reset_index(drop=True), quarantine


def _url_decode(title: str):
    try:
        return unquote_to_bytes(title).decode("utf-8")
    except UnicodeDecodeError:
        return None


def normalize_titles(df: pd.DataFrame, source_file: str):

    df = df.copy()
    title = df["title"]
    has_escape = title.str.contains("%", regex=False)
    decoded = pd.Series([_url_decode(t) for t in title[has_escape]],
                        index=title.index[has_escape], dtype="string")
    failed = decoded.isna()
    title = title.where(~has_escape, decoded)

    title = normalize_title_series(title)
    df["title"] = title

    reasons = pd.Series(pd.NA, index=df.index, dtype="string")
    reasons.loc[failed[failed].index] = "bad_url_encoding"
    reasons = reasons.mask(reasons.isna() & df["title"].fillna("").eq(""), "empty_title")

    bad = reasons.notna()
    quarantine = pd.DataFrame({
        "source_file": pd.Series(source_file, index=df.index[bad], dtype="string"),
        "raw_line": df.loc[bad, "raw_line"],
        "reason": reasons[bad],
    }).reset_index(drop=True)
    return df.loc[~bad].reset_index(drop=True), quarantine


def drop_non_articles(df: pd.DataFrame, namespaces=config.NON_ARTICLE_NAMESPACES,
                      placeholders=config.PLACEHOLDER_TITLES):
    prefix = df["title"].str.split(":", n=1).str[0].str.lower()
    has_colon = df["title"].str.contains(":", regex=False)
    names = {n.lower() for n in namespaces}
    is_non_article = (has_colon & prefix.isin(names)) | df["title"].isin(placeholders)
    return df.loc[~is_non_article].reset_index(drop=True), int(is_non_article.sum())


def aggregate(df: pd.DataFrame, hour: pd.Timestamp):
    out = (
        df.groupby(["project", "title"], as_index=False, sort=False)[["views", "bytes"]]
        .sum()
    )
    out.insert(0, "hour", hour)
    out["project"] = out["project"].astype("string")
    out["title"] = out["title"].astype("string")
    return out, len(df) - len(out)


def clean_pageview_file(path) -> CleanResult:
    path = Path(path)
    name = path.name
    hour = hour_from_filename(path)

    lines, q_read, total = read_lines(path)
    parsed, q_parse = parse_lines(lines, name)
    normalized, q_norm = normalize_titles(parsed, name)
    articles, non_articles = drop_non_articles(normalized)
    clean, collisions = aggregate(articles, hour)

    quarantine = pd.concat([q_read, q_parse, q_norm], ignore_index=True)
    stats = {
        "file": name,
        "hour": hour,
        "lines_total": total,
        "lines_in_scope": len(lines) + len(q_read),
        "quarantined": len(quarantine),
        "quarantine_by_reason": quarantine["reason"].value_counts().to_dict(),
        "dropped_non_article": non_articles,
        "collisions_merged": collisions,
        "rows_out": len(clean),
    }
    return CleanResult(clean, quarantine, stats)


def clean_pageview_files(paths) -> CleanResult:
    """Run clean_pageview_file on several hourly files and stack the results."""
    results = [clean_pageview_file(p) for p in sorted(paths)]
    return CleanResult(
        clean=pd.concat([r.clean for r in results], ignore_index=True),
        quarantine=pd.concat([r.quarantine for r in results], ignore_index=True),
        stats={"per_file": [r.stats for r in results]},
    )
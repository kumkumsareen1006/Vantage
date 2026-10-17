"""Clean captured recent-change events into hourly human/bot edit counts.

Mirrors the pageview cleaner: scope filters, a quarantine frame for anything
unparseable, and the same title normalization so edits and views join later.

Input is the dated JSONL files written by scripts/capture_edits.py. Those files
are split by receive date, not event time, so a window is selected on meta.dt
and may span more than one file.
"""
import json
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from prototype import config
from prototype.normalize import normalize_title_series

QUARANTINE_COLUMNS = ["source_file", "raw_line", "reason"]
KEEP_TYPES = ("edit", "new")


@dataclass
class EditResult:
    clean: pd.DataFrame          # one row per (title, hour): human_edits, bot_edits
    quarantine: pd.DataFrame
    stats: dict = field(default_factory=dict)


def _to_utc(ts) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def _empty_quarantine() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="string") for c in QUARANTINE_COLUMNS})


def _scope_filter_file(path):
    """Stream one JSONL file and keep only en-wiki article edit/new events.

    Pre-filtering while streaming keeps the out-of-scope majority (other
    namespaces, logs, categorize) out of memory. Returns the surviving event
    dicts (each tagged with its raw line and source file), a bad-JSON
    quarantine frame, and per-stage counts.
    """
    name = Path(path).name
    kept, bad_lines = [], []
    counts = dict(lines=0, bad_json=0, dropped_canary=0, dropped_non_enwiki=0,
                  dropped_other_type=0, dropped_non_article_ns=0)
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            counts["lines"] += 1
            try:
                d = json.loads(raw)
            except ValueError:
                counts["bad_json"] += 1
                bad_lines.append(raw)
                continue
            meta = d.get("meta") or {}
            if meta.get("domain") == "canary":
                counts["dropped_canary"] += 1
                continue
            if d.get("wiki") != "enwiki":
                counts["dropped_non_enwiki"] += 1
                continue
            if d.get("type") not in KEEP_TYPES:
                counts["dropped_other_type"] += 1
                continue
            if d.get("namespace") != 0:
                counts["dropped_non_article_ns"] += 1
                continue
            d["_raw"] = raw
            d["_source_file"] = name
            kept.append(d)
    quarantine = pd.DataFrame({
        "source_file": pd.Series([name] * len(bad_lines), dtype="string"),
        "raw_line": pd.Series(bad_lines, dtype="string"),
        "reason": pd.Series(["bad_json"] * len(bad_lines), dtype="string"),
    })
    return kept, quarantine, counts


def clean_edit_files(paths, window_start=None, window_end=None) -> EditResult:
    start = _to_utc(window_start or config.WINDOW_START)
    end = _to_utc(window_end or config.WINDOW_END)

    frames, quarantines, per_file = [], [], []
    cols = ["title", "bot", "meta.id", "meta.dt", "_source_file", "_raw"]
    for p in sorted(paths):
        events, q_json, counts = _scope_filter_file(p)
        quarantines.append(q_json)
        per_file.append({"file": Path(p).name, **counts, "in_scope": len(events)})
        if not events:
            continue
        f = pd.json_normalize(events)
        for c in cols:
            if c not in f.columns:
                f[c] = pd.NA
        frames.append(f[cols])

    quarantine_json = pd.concat(quarantines, ignore_index=True) if quarantines else _empty_quarantine()
    totals = {k: sum(pf[k] for pf in per_file)
              for k in ["lines", "bad_json", "dropped_canary", "dropped_non_enwiki",
                        "dropped_other_type", "dropped_non_article_ns", "in_scope"]}

    if not frames:
        clean = pd.DataFrame({"title": pd.Series(dtype="string"),
                              "hour": pd.Series(dtype="datetime64[ns, UTC]"),
                              "human_edits": pd.Series(dtype="int64"),
                              "bot_edits": pd.Series(dtype="int64")})
        stats = {"per_file": per_file, "totals": totals, "window": [str(start), str(end)],
                 "validation_quarantine_by_reason": {}, "dropped_out_of_window": 0,
                 "duplicates_removed": 0, "rows_out": 0,
                 "human_edits_total": 0, "bot_edits_total": 0}
        return EditResult(clean, quarantine_json, stats)

    df = pd.concat(frames, ignore_index=True).rename(
        columns={"meta.id": "meta_id", "meta.dt": "meta_dt", "_source_file": "source_file", "_raw": "raw_line"})

    # Validate the fields we rely on; anything missing or unparseable is quarantined, not dropped silently.
    dt = pd.to_datetime(df["meta_dt"], utc=True, errors="coerce", format="ISO8601")
    title = df["title"].astype("string")
    reasons = pd.Series(pd.NA, index=df.index, dtype="string")
    reasons = reasons.mask(reasons.isna() & df["meta_id"].isna(), "missing_meta_id")
    reasons = reasons.mask(reasons.isna() & dt.isna(), "bad_meta_dt")
    reasons = reasons.mask(reasons.isna() & (title.isna() | title.str.strip().eq("")), "empty_title")
    bad = reasons.notna()

    q_validate = pd.DataFrame({
        "source_file": df.loc[bad, "source_file"].astype("string"),
        "raw_line": df.loc[bad, "raw_line"].astype("string"),
        "reason": reasons[bad],
    }).reset_index(drop=True)

    good = df.loc[~bad, ["title", "bot", "meta_id"]].copy()
    good["hour"] = dt.loc[good.index].dt.floor("h")

    in_window = (good["hour"] >= start) & (good["hour"] < end)
    dropped_out_of_window = int((~in_window).sum())
    good = good.loc[in_window]

    before = len(good)
    good = good.drop_duplicates(subset="meta_id", keep="first")
    duplicates_removed = before - len(good)

    good["title"] = normalize_title_series(good["title"].astype("string"))
    is_bot = good["bot"].fillna(False).astype(bool)   # missing bot flag treated as human
    good["human_edits"] = (~is_bot).astype("int64")
    good["bot_edits"] = is_bot.astype("int64")

    clean = (good.groupby(["title", "hour"], as_index=False, sort=True)[["human_edits", "bot_edits"]]
                 .sum())
    clean["title"] = clean["title"].astype("string")

    quarantine = pd.concat([quarantine_json, q_validate], ignore_index=True)
    stats = {
        "per_file": per_file,
        "totals": totals,
        "window": [str(start), str(end)],
        "validation_quarantine_by_reason": q_validate["reason"].value_counts().to_dict(),
        "dropped_out_of_window": dropped_out_of_window,
        "duplicates_removed": duplicates_removed,
        "rows_out": len(clean),
        "human_edits_total": int(clean["human_edits"].sum()),
        "bot_edits_total": int(clean["bot_edits"].sum()),
    }
    return EditResult(clean.reset_index(drop=True), quarantine, stats)

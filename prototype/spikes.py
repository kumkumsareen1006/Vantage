"""Robust-z spike detection on hourly pageviews.

Works off the cached per-hour parquet (en + en.m summed per title). The spike
window is exactly the set of hours that have a pageview file; a title's missing
hours are zero-filled only inside that window. An internal gap (a missing hour
between two present ones) is an error, not something to fill across.

Score for hour t uses the previous 24 hours as the baseline, excluding t:
    z = (views - median) / (1.4826 * max(MAD, 1))
A spike needs z >= Z_MIN and views >= VIEWS_FLOOR and (views - median) >= MIN_JUMP.
The first 24 hours are warm-up: no baseline, so never flagged.
"""
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

from prototype import config

SPIKE_COLUMNS = ["title", "hour", "views", "median", "mad", "z", "jump"]


def hour_from_parquet(path) -> pd.Timestamp:
    # cache files are named by the hour START, e.g. 2026-10-15T00.parquet
    return pd.Timestamp(datetime.strptime(Path(path).stem, "%Y-%m-%dT%H"), tz="UTC")


def _hour_views(path) -> pd.Series:
    df = pd.read_parquet(path, columns=["title", "views"])
    return df.groupby("title", sort=False)["views"].sum()  # sums en + en.m


def _check_contiguous(hours):
    if not hours:
        raise ValueError("no pageview hours found")
    full = pd.date_range(hours[0], hours[-1], freq="h")
    present = set(hours)
    missing = [h for h in full if h not in present]
    if missing:
        raise ValueError(
            f"internal gap in pageview hours: {len(missing)} hour(s) missing between "
            f"{hours[0]} and {hours[-1]}, e.g. {[str(m) for m in missing[:5]]}. "
            "Stopping instead of zero-filling across the gap."
        )


def load_view_matrix(paths, views_floor=None):
    """Build the titles x hours view matrix for titles that clear the floor at
    least once. Returns (titles, hours, arr). Raises on an internal hour gap."""
    views_floor = config.VIEWS_FLOOR if views_floor is None else views_floor
    paths = sorted(paths, key=hour_from_parquet)
    hours = [hour_from_parquet(p) for p in paths]
    _check_contiguous(hours)

    keep = set()
    for p in paths:
        s = _hour_views(p)
        keep.update(s.index[s.to_numpy() >= views_floor].tolist())

    cols = []
    for p, h in zip(paths, hours):
        s = _hour_views(p)
        cols.append(s[s.index.isin(keep)].rename(h))
    mat = pd.concat(cols, axis=1).reindex(index=sorted(keep)).fillna(0.0)
    mat = mat[hours]  # columns in time order

    titles = np.asarray(mat.index, dtype=object)
    hours_arr = np.asarray(hours, dtype=object)
    return titles, hours_arr, mat.to_numpy(dtype="float64")


def rolling_median_mad(arr, baseline=None):
    """Median and MAD over the previous `baseline` hours, excluding the current
    hour. Warm-up hours (no full baseline) come back as NaN."""
    baseline = config.BASELINE_HOURS if baseline is None else baseline
    n = arr.shape[1]
    median = np.full(arr.shape, np.nan)
    mad = np.full(arr.shape, np.nan)
    if n <= baseline:
        return median, mad  # all warm-up
    win = sliding_window_view(arr, baseline, axis=1)        # (T, n-baseline+1, baseline)
    med_w = np.median(win, axis=2)
    mad_w = np.median(np.abs(win - med_w[:, :, None]), axis=2)
    # hour t (t >= baseline) is scored from window starting at t-baseline (covers t-baseline..t-1)
    median[:, baseline:] = med_w[:, : n - baseline]
    mad[:, baseline:] = mad_w[:, : n - baseline]
    return median, mad


def robust_z(arr, median, mad):
    denom = 1.4826 * np.maximum(mad, 1.0)
    with np.errstate(invalid="ignore", divide="ignore"):
        return (arr - median) / denom


def spike_table(titles, hours, arr, median, mad,
                z_min=None, views_floor=None, min_jump=None):
    z_min = config.Z_MIN if z_min is None else z_min
    views_floor = config.VIEWS_FLOOR if views_floor is None else views_floor
    min_jump = config.MIN_JUMP if min_jump is None else min_jump

    z = robust_z(arr, median, mad)
    jump = arr - median
    # NaN (warm-up) fails every comparison, so warm-up hours are never flagged
    mask = (z >= z_min) & (arr >= views_floor) & (jump >= min_jump)
    ti, hj = np.where(mask)
    out = pd.DataFrame({
        "title": pd.Series(titles[ti], dtype="string"),
        "hour": hours[hj],
        "views": arr[ti, hj].astype("int64"),
        "median": median[ti, hj],
        "mad": mad[ti, hj],
        "z": z[ti, hj],
        "jump": jump[ti, hj].astype("int64"),
    })
    return out.sort_values("z", ascending=False).reset_index(drop=True)


def detect_spikes(parquet_paths, z_min=None, views_floor=None, min_jump=None, baseline=None):
    titles, hours, arr = load_view_matrix(parquet_paths, views_floor)
    median, mad = rolling_median_mad(arr, baseline)
    return spike_table(titles, hours, arr, median, mad, z_min, views_floor, min_jump)

import numpy as np
import pandas as pd
import pytest

from prototype.spikes import (load_view_matrix, rolling_median_mad, robust_z,
                              spike_table)


def _hours(n, start="2026-10-15"):
    return np.asarray(pd.date_range(start, periods=n, freq="h", tz="UTC"), dtype=object)


def _write_parquet(d, hour_ts, rows):
    df = pd.DataFrame(rows, columns=["project", "title", "views"])
    df["hour"] = hour_ts
    df["bytes"] = 0
    df.to_parquet(d / f"{hour_ts:%Y-%m-%dT%H}.parquet", index=False)


def test_flat_page_mad_zero_no_divzero():
    titles, hours = np.array(["Flat"], dtype=object), _hours(26)
    arr = np.full((1, 26), 100.0)  # constant traffic -> MAD 0
    median, mad = rolling_median_mad(arr, 24)
    z = robust_z(arr, median, mad)
    assert mad[0, 24] == 0
    assert np.isfinite(z[0, 24]) and z[0, 24] == 0  # max(MAD,1) keeps it finite
    tbl = spike_table(titles, hours, arr, median, mad, z_min=1, views_floor=1, min_jump=1)
    assert len(tbl) == 0


def test_current_hour_excluded_from_baseline():
    titles, hours = np.array(["X"], dtype=object), _hours(25)
    arr = np.concatenate([np.full(24, 10.0), [1000.0]]).reshape(1, 25)
    median, mad = rolling_median_mad(arr, 24)
    # baseline for hour 24 is hours 0..23 (all 10); the 1000 at hour 24 must not be in it
    assert median[0, 24] == 10 and mad[0, 24] == 0
    tbl = spike_table(titles, hours, arr, median, mad, z_min=6, views_floor=100, min_jump=100)
    assert list(tbl.title) == ["X"] and int(tbl.views.iloc[0]) == 1000


def test_all_three_conditions_required():
    titles, hours = np.array(["X"], dtype=object), _hours(25)

    def flagged(base, last, **kw):
        arr = np.concatenate([base, [last]]).reshape(1, 25)
        med, mad = rolling_median_mad(arr, 24)
        kw = {"z_min": 6, "views_floor": 1000, "min_jump": 500, **kw}
        return len(spike_table(titles, hours, arr, med, mad, **kw)) == 1

    flat = np.full(24, 10.0)
    assert flagged(flat, 2000)              # all three pass
    assert not flagged(flat, 900)           # fails views floor (900 < 1000)
    high = np.full(24, 5000.0)
    assert not flagged(high, 5400)          # fails min_jump (jump 400 < 500), z/floor ok
    noisy = np.tile([0.0, 2000.0], 12)      # median 1000, MAD 1000 -> big denom
    assert not flagged(noisy, 2500)         # fails z (~1.0 < 6), floor/jump ok


def test_floor_excludes_low_traffic(tmp_path):
    hours = pd.date_range("2026-10-15", periods=25, freq="h", tz="UTC")
    for h in hours:
        _write_parquet(tmp_path, h, [("en", "Low", 50),
                                     ("en", "High", 2000 if h == hours[24] else 5)])
    titles, _, _ = load_view_matrix(sorted(tmp_path.glob("*.parquet")), views_floor=1000)
    assert "High" in set(titles) and "Low" not in set(titles)


def test_en_and_mobile_summed(tmp_path):
    h = pd.Timestamp("2026-10-15", tz="UTC")
    _write_parquet(tmp_path, h, [("en", "A", 100), ("en.m", "A", 50)])
    _, _, arr = load_view_matrix(sorted(tmp_path.glob("*.parquet")), views_floor=1)
    assert arr[0, 0] == 150


def test_internal_gap_raises(tmp_path):
    hours = pd.date_range("2026-10-15", periods=5, freq="h", tz="UTC")
    for h in [hours[0], hours[1], hours[3], hours[4]]:  # hours[2] missing: a middle gap
        _write_parquet(tmp_path, h, [("en", "A", 2000)])
    with pytest.raises(ValueError, match="internal gap"):
        load_view_matrix(sorted(tmp_path.glob("*.parquet")), views_floor=1)


def test_trailing_gap_ok(tmp_path):
    # only the first 3 contiguous hours present; no hole between them, so this is fine
    for h in pd.date_range("2026-10-15", periods=3, freq="h", tz="UTC"):
        _write_parquet(tmp_path, h, [("en", "A", 2000)])
    _, _, arr = load_view_matrix(sorted(tmp_path.glob("*.parquet")), views_floor=1)
    assert arr.shape == (1, 3)

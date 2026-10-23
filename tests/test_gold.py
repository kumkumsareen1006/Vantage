"""Gold spike-score and top-pages rules, on small fixed inputs.

The spike rules mirror prototype/spikes.py; see tests/test_spikes.py for the
NumPy side and tests/test_parity (Day 9 report) for the real-data cross-check.
"""
from datetime import datetime, timedelta, timezone

import numpy as np
from pyspark.sql import Row, functions as F

from vantage.transform.gold import spike_scores, top_pages_daily, top_pages_hourly

H0 = datetime(2026, 10, 15, 0, 0, tzinfo=timezone.utc)


def hr(i):
    return H0 + timedelta(hours=i)


def hs(i):
    return hr(i).strftime("%Y-%m-%d %H:%M:%S")


def silver_df(spark, rows):
    # rows: (project, title, hour_index, views)
    return spark.createDataFrame(
        [Row(project=p, title=t, hour=hr(i), views=v, bytes=0) for (p, t, i, v) in rows])


def scored_list(df):
    # hour as a UTC string so the assertions don't depend on the driver's local tz
    rows = df.withColumn("hs", F.date_format("hour", "yyyy-MM-dd HH:mm:ss")).collect()
    return rows


def test_even_length_median_matches_numpy(spark):
    # baseline hours 0..23 = 0..23 (even length); scored at hour 24
    rows = [("en", "T", i, i) for i in range(24)] + [("en", "T", 24, 5000)]
    scored = scored_list(spike_scores(silver_df(spark, rows),
                                      baseline=24, z_min=6, views_floor=1000, min_jump=1))
    assert len(scored) == 1 and scored[0]["hs"] == hs(24)
    assert scored[0]["median"] == 11.5 == float(np.median(np.arange(24)))  # mean of 12th and 13th


def test_mad_floor_of_one(spark):
    rows = [("en", "Flat", i, 100) for i in range(24)] + [("en", "Flat", 24, 2000)]
    scored = scored_list(spike_scores(silver_df(spark, rows),
                                      baseline=24, z_min=6, views_floor=1000, min_jump=1))
    assert len(scored) == 1
    assert scored[0]["mad"] == 0.0 and scored[0]["median"] == 100.0
    assert abs(scored[0]["z"] - (2000 - 100) / (1.4826 * 1.0)) < 1e-9  # max(MAD,1), stays finite
    assert scored[0]["is_spike"]


def test_current_hour_excluded_from_baseline(spark):
    rows = [("en", "X", i, 10) for i in range(24)] + [("en", "X", 24, 1000)]
    scored = scored_list(spike_scores(silver_df(spark, rows),
                                      baseline=24, z_min=6, views_floor=100, min_jump=100))
    assert len(scored) == 1
    # baseline is hours 0..23 (all 10); the 1000 at hour 24 must not be in it
    assert scored[0]["median"] == 10.0 and scored[0]["mad"] == 0.0
    assert scored[0]["views"] == 1000 and scored[0]["is_spike"]


def test_warmup_hours_never_scored(spark):
    # a huge value at hour 3 is inside the warm-up window, so it is never scored
    rows = [("en", "W", i, (100000 if i == 3 else 10)) for i in range(25)]
    scored = scored_list(spike_scores(silver_df(spark, rows),
                                      baseline=24, z_min=6, views_floor=1000, min_jump=1))
    assert {r["hs"] for r in scored} == {hs(24)}          # only hour 24 has a full baseline
    assert all(not r["is_spike"] for r in scored)          # and hour 24's own views are just 10


def test_zero_filled_hours_count_in_baseline(spark):
    # Filler is present every hour, so the axis is contiguous 0..25
    rows = [("en", "Filler", i, 2000) for i in range(26)]
    # A clears the floor at hour 0; present at 13..25=200; hours 1..12 MISSING -> zero-filled
    rows += [("en", "A", 0, 5000)] + [("en", "A", i, 200) for i in range(13, 26)]
    scored = scored_list(spike_scores(silver_df(spark, rows),
                                      baseline=24, z_min=6, views_floor=1000, min_jump=1))
    a25 = [r for r in scored if r["title"] == "A" and r["hs"] == hs(25)]
    assert len(a25) == 1  # exists only because the 12 missing hours were zero-filled to make 24
    # baseline hours 1..24 = twelve 0s + twelve 200s -> median = mean(0, 200) = 100
    assert a25[0]["median"] == 100.0


def test_en_and_mobile_summed(spark):
    rows = ([("en", "S", i, 5) for i in range(24)] + [("en.m", "S", i, 5) for i in range(24)]
            + [("en", "S", 24, 600), ("en.m", "S", 24, 600)])
    scored = scored_list(spike_scores(silver_df(spark, rows),
                                      baseline=24, z_min=6, views_floor=1000, min_jump=1))
    assert len(scored) == 1
    assert scored[0]["views"] == 1200          # en + en.m summed (600 + 600)
    assert scored[0]["median"] == 10.0          # baseline 5 + 5 per hour
    assert scored[0]["is_spike"]


def test_top_pages_hourly_sums_projects_and_ranks(spark):
    rows = [("en", "A", 0, 100), ("en.m", "A", 0, 50), ("en", "B", 0, 120), ("en", "C", 0, 10)]
    got = {r["title"]: r for r in top_pages_hourly(silver_df(spark, rows), n=2).collect()}
    assert set(got) == {"A", "B"}               # C (rank 3) dropped by top-2
    assert got["A"]["views"] == 150 and got["A"]["rank"] == 1   # 100 + 50 summed
    assert got["B"]["rank"] == 2


def test_top_pages_daily_sums_hours_and_projects(spark):
    rows = [("en", "A", 0, 100), ("en", "A", 1, 100), ("en.m", "A", 0, 50), ("en", "B", 0, 120)]
    got = {r["title"]: r for r in top_pages_daily(silver_df(spark, rows), n=10).collect()}
    assert got["A"]["views"] == 250            # across both hours and both projects
    assert got["B"]["views"] == 120


# ---- likely_automated label (en vs en.m, neighbour isolation, window edge) ----

def _auto_kwargs():
    return dict(baseline=24, z_min=6, views_floor=1000, min_jump=1000,
                desktop_share_min=0.8, isolation_ratio=0.05)


def test_isolated_desktop_spike_is_flagged(spark):
    # Filler holds a contiguous axis 0..26. Auto appears only at hour 25: desktop-
    # dominated, neighbours (24, 26) are zero-filled -> isolated and labelled.
    rows = [("en", "Filler", i, 2000) for i in range(27)]
    rows += [("en", "Auto", 25, 5000), ("en.m", "Auto", 25, 200)]
    scored = scored_list(spike_scores(silver_df(spark, rows), **_auto_kwargs()))
    a = [r for r in scored if r["title"] == "Auto" and r["hs"] == hs(25)]
    assert len(a) == 1
    assert a[0]["views"] == 5200 and a[0]["en_views"] == 5000 and a[0]["mobile_views"] == 200
    assert a[0]["is_spike"] and a[0]["likely_automated"]


def test_ramping_mobile_spike_not_flagged(spark):
    # Organic ramps 22..26 and skews mobile; its spike hour (25) has a busy neighbour
    # (24 = 3000, not near-zero) and desktop_share 0.3 -> a real spike, not automated.
    rows = [("en", "Filler", i, 2000) for i in range(27)]
    ramp = {22: 300, 23: 1500, 24: 3000, 25: 6000, 26: 4000}
    for h, v in ramp.items():
        desk = int(v * 0.3)
        rows += [("en", "Organic", h, desk), ("en.m", "Organic", h, v - desk)]
    scored = scored_list(spike_scores(silver_df(spark, rows), **_auto_kwargs()))
    o = [r for r in scored if r["title"] == "Organic" and r["hs"] == hs(25)]
    assert len(o) == 1 and o[0]["views"] == 6000
    assert o[0]["is_spike"] and not o[0]["likely_automated"]


def test_edge_hour_spike_not_flagged(spark):
    # Edge spikes at the last axis hour (25): desktop-heavy and left-isolated, but the
    # right neighbour is outside the window (null) so isolation can't be confirmed.
    rows = [("en", "Filler", i, 2000) for i in range(26)]   # axis 0..25
    rows += [("en", "Edge", 25, 5000), ("en.m", "Edge", 25, 100)]
    scored = scored_list(spike_scores(silver_df(spark, rows), **_auto_kwargs()))
    e = [r for r in scored if r["title"] == "Edge" and r["hs"] == hs(25)]
    assert len(e) == 1 and e[0]["next_views"] is None
    assert e[0]["is_spike"] and not e[0]["likely_automated"]

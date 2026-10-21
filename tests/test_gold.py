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

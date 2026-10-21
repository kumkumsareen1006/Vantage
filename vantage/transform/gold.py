"""Gold transforms: top pages and the robust-z spike score, from Silver.

Each is a pure function of Silver (same input -> same output), so the build
overwrites the Gold tables every run; re-running can't accumulate or duplicate.

Spike score mirrors prototype/spikes.py exactly. en and en.m are summed per
(title, hour) first. A title is scored only if it clears the views floor at least
once; its missing hours are zero-filled across the window so the baseline counts
them (Silver has no row for an hour with 0 views). The baseline is the previous
`baseline` hours, current hour excluded (rowsBetween(-baseline, -1)); the first
`baseline` hours are warm-up and never scored. Median of the 24-value window is
the mean of the two middle order statistics (same as numpy's even-length median);
MAD applies the same median to the absolute deviations.
"""
from pyspark.sql import Window, functions as F

from vantage import config


def _views_by_title_hour(silver):
    # drop project -> sums en + en.m for the same title/hour
    return silver.groupBy("title", "hour").agg(F.sum("views").alias("views"))


def top_pages_hourly(silver, n=100):
    vth = _views_by_title_hour(silver)
    w = Window.partitionBy("hour").orderBy(F.col("views").desc(), F.col("title").asc())
    return (vth.withColumn("rank", F.row_number().over(w))
            .where(F.col("rank") <= n)
            .select("hour", "rank", "title", "views"))


def top_pages_daily(silver, n=100):
    # sum across both projects and all 24 hours of the UTC day
    vtd = (silver.groupBy(F.to_date("hour").alias("date"), "title")
           .agg(F.sum("views").alias("views")))
    w = Window.partitionBy("date").orderBy(F.col("views").desc(), F.col("title").asc())
    return (vtd.withColumn("rank", F.row_number().over(w))
            .where(F.col("rank") <= n)
            .select("date", "rank", "title", "views"))


def view_grid(silver, views_floor=None):
    """Zero-filled (title x hour) views for titles that clear the floor at least
    once. Raises on an internal hour gap, like the prototype -- don't fill across."""
    views_floor = config.VIEWS_FLOOR if views_floor is None else views_floor
    vth = _views_by_title_hour(silver)

    # build the hour axis inside Spark (min..max by 1h); collecting the bounds would
    # round-trip the timestamps through the driver's local tz
    hours = vth.select("hour").distinct()
    axis = (hours.agg(F.min("hour").alias("lo"), F.max("hour").alias("hi"))
            .select(F.explode(F.sequence("lo", "hi", F.expr("INTERVAL 1 HOUR"))).alias("hour")))
    if axis.count() != hours.count():
        raise ValueError("internal gap in Silver hours; refusing to zero-fill across it")

    kept = (vth.groupBy("title").agg(F.max("views").alias("mx"))
            .where(F.col("mx") >= F.lit(views_floor)).select("title"))
    return (kept.crossJoin(axis)
            .join(vth, ["title", "hour"], "left")
            .withColumn("views", F.coalesce(F.col("views"), F.lit(0)).cast("long")))


def spike_scores(silver, baseline=None, z_min=None, views_floor=None, min_jump=None):
    baseline = config.BASELINE_HOURS if baseline is None else baseline
    z_min = config.Z_MIN if z_min is None else z_min
    views_floor = config.VIEWS_FLOOR if views_floor is None else views_floor
    min_jump = config.MIN_JUMP if min_jump is None else min_jump
    lo_i, hi_i = baseline // 2 - 1, baseline // 2  # two middle indices of the sorted window

    grid = view_grid(silver, views_floor)
    w = Window.partitionBy("title").orderBy("hour").rowsBetween(-baseline, -1)
    scored = grid.withColumn("base", F.collect_list("views").over(w)).where(F.size("base") == baseline)

    sarr = F.sort_array(F.col("base"))
    scored = scored.withColumn("median", (sarr.getItem(lo_i) + sarr.getItem(hi_i)) / 2.0)
    dev = F.sort_array(F.transform(F.col("base"), lambda x: F.abs(x - F.col("median"))))
    scored = (scored
              .withColumn("mad", (dev.getItem(lo_i) + dev.getItem(hi_i)) / 2.0)
              .withColumn("jump", F.col("views") - F.col("median"))
              .withColumn("z", (F.col("views") - F.col("median"))
                          / (F.lit(1.4826) * F.greatest(F.col("mad"), F.lit(1.0))))
              .withColumn("is_spike", (F.col("z") >= F.lit(z_min))
                          & (F.col("views") >= F.lit(views_floor))
                          & (F.col("jump") >= F.lit(min_jump))))
    return scored.select("title", "hour", "views", "median", "mad", "z", "jump", "is_spike")

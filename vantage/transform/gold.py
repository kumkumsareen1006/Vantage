"""Gold transforms: top pages and the robust-z spike score, from Silver.

Each is a pure function of Silver (same input -> same output), so the build
overwrites the Gold tables every run; re-running can't accumulate or duplicate.

Spike score mirrors prototype/spikes.py exactly. en and en.m are summed per
(title, hour) first (the desktop/mobile split is kept alongside). A title is
scored only if it clears the views floor at least once; its missing hours are
zero-filled across the window so the baseline counts them (Silver has no row for
an hour with 0 views). The baseline is the previous `baseline` hours, current
hour excluded (rowsBetween(-baseline, -1)); the first `baseline` hours are
warm-up and never scored. Median of the 24-value window is the mean of the two
middle order statistics (same as numpy's even-length median); MAD applies the
same median to the absolute deviations.

likely_automated is an extra label on top of is_spike (it never changes
is_spike): a spike that is desktop-dominated and isolated -- a single hour with
near-zero views on both sides. Organic attention ramps and decays over
neighbouring hours and skews mobile; a scraper/redirect burst spikes one hour on
desktop with nothing around it. A neighbour outside the window is null, so the
flag is false there (can't confirm isolation). See docs/cleaning-rules.md.
"""
from pyspark.sql import Window, functions as F

from vantage import config


def _views_by_title_hour(silver):
    # drop project -> sums en + en.m; keep the split so Gold can see desktop vs mobile
    return silver.groupBy("title", "hour").agg(
        F.sum("views").alias("views"),
        F.sum(F.when(F.col("project") == "en", F.col("views")).otherwise(0)).alias("en_views"),
        F.sum(F.when(F.col("project") == "en.m", F.col("views")).otherwise(0)).alias("mobile_views"))


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
    """Zero-filled (title x hour) views, with the en/mobile split, for titles that
    clear the floor at least once. Raises on an internal hour gap, like the
    prototype -- don't fill across."""
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
            .withColumn("views", F.coalesce(F.col("views"), F.lit(0)).cast("long"))
            .withColumn("en_views", F.coalesce(F.col("en_views"), F.lit(0)).cast("long"))
            .withColumn("mobile_views", F.coalesce(F.col("mobile_views"), F.lit(0)).cast("long")))


def spike_scores(silver, baseline=None, z_min=None, views_floor=None, min_jump=None,
                 desktop_share_min=None, isolation_ratio=None):
    baseline = config.BASELINE_HOURS if baseline is None else baseline
    z_min = config.Z_MIN if z_min is None else z_min
    views_floor = config.VIEWS_FLOOR if views_floor is None else views_floor
    min_jump = config.MIN_JUMP if min_jump is None else min_jump
    desktop_share_min = config.DESKTOP_SHARE_MIN if desktop_share_min is None else desktop_share_min
    isolation_ratio = config.ISOLATION_RATIO if isolation_ratio is None else isolation_ratio
    lo_i, hi_i = baseline // 2 - 1, baseline // 2  # two middle indices of the sorted window

    grid = view_grid(silver, views_floor)
    # neighbour hours on the full contiguous grid, before warm-up is dropped, so a
    # neighbour is the real adjacent clock hour; null at the window edge -> can't confirm
    nb = Window.partitionBy("title").orderBy("hour")
    grid = (grid.withColumn("prev_views", F.lag("views").over(nb))
                .withColumn("next_views", F.lead("views").over(nb)))

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

    desktop_share = F.when(F.col("views") > 0, F.col("en_views") / F.col("views")).otherwise(F.lit(0.0))
    # both neighbours present (inside the window) and near-zero relative to this hour
    isolated = (F.col("prev_views").isNotNull() & F.col("next_views").isNotNull()
                & (F.col("prev_views") < F.col("views") * F.lit(isolation_ratio))
                & (F.col("next_views") < F.col("views") * F.lit(isolation_ratio)))
    scored = scored.withColumn(
        "likely_automated",
        F.col("is_spike") & (desktop_share >= F.lit(desktop_share_min)) & isolated)

    return scored.select("title", "hour", "views", "en_views", "mobile_views",
                         "median", "mad", "z", "jump", "prev_views", "next_views",
                         "is_spike", "likely_automated")


# --------------------------------------------------------------- edits -> Gold (Day 17)
EDIT_TYPES = ("edit", "new")


def edit_counts(silver_edits, window_start=None, window_end=None):
    """Per (title, hour): human and bot edit counts. Only type in (edit, new) and
    namespace 0 -- the actual-article-revision filter the streaming Silver deferred.
    `title` is title_norm, so it joins the pageview spikes. bot missing -> counted human.
    hour is already meta.dt truncated to the UTC hour in Silver."""
    df = silver_edits.where(F.col("type").isin(*EDIT_TYPES) & (F.col("namespace") == 0))
    if window_start is not None:
        df = df.where((F.col("hour") >= F.lit(window_start)) & (F.col("hour") < F.lit(window_end)))
    is_bot = F.coalesce(F.col("bot"), F.lit(False))
    return (df.groupBy(F.col("title_norm").alias("title"), "hour")
            .agg(F.sum(F.when(~is_bot, 1).otherwise(0)).cast("long").alias("human_edits"),
                 F.sum(F.when(is_bot, 1).otherwise(0)).cast("long").alias("bot_edits")))


def _edit_grid(counts, edits_floor):
    """Zero-filled (title x hour) human/bot edits for titles that reach edits_floor human
    edits at least once. Raises on an internal hour gap (don't fill across), like view_grid."""
    hours = counts.select("hour").distinct()
    axis = (hours.agg(F.min("hour").alias("lo"), F.max("hour").alias("hi"))
            .select(F.explode(F.sequence("lo", "hi", F.expr("INTERVAL 1 HOUR"))).alias("hour")))
    if axis.count() != hours.count():
        raise ValueError("internal gap in edit hours; refusing to zero-fill across it")
    kept = (counts.groupBy("title").agg(F.max("human_edits").alias("mx"))
            .where(F.col("mx") >= F.lit(edits_floor)).select("title"))
    return (kept.crossJoin(axis).join(counts, ["title", "hour"], "left")
            .withColumn("human_edits", F.coalesce(F.col("human_edits"), F.lit(0)).cast("long"))
            .withColumn("bot_edits", F.coalesce(F.col("bot_edits"), F.lit(0)).cast("long")))


def edit_spike_scores(counts, baseline=None, z_min=None, edits_floor=None, min_jump=None):
    """Robust z-score on human_edits, same shape as the view spike: previous `baseline`
    hours (current excluded), max(MAD, 1), plus an edits floor and a minimum jump. Bot
    edits never enter the score."""
    baseline = config.EDIT_BASELINE_HOURS if baseline is None else baseline
    z_min = config.EDIT_Z_MIN if z_min is None else z_min
    edits_floor = config.EDITS_FLOOR if edits_floor is None else edits_floor
    min_jump = config.EDIT_MIN_JUMP if min_jump is None else min_jump
    lo_i, hi_i = baseline // 2 - 1, baseline // 2

    grid = _edit_grid(counts, edits_floor)
    w = Window.partitionBy("title").orderBy("hour").rowsBetween(-baseline, -1)
    scored = grid.withColumn("base", F.collect_list("human_edits").over(w)).where(F.size("base") == baseline)

    sarr = F.sort_array(F.col("base"))
    scored = scored.withColumn("median", (sarr.getItem(lo_i) + sarr.getItem(hi_i)) / 2.0)
    dev = F.sort_array(F.transform(F.col("base"), lambda x: F.abs(x - F.col("median"))))
    scored = (scored
              .withColumn("mad", (dev.getItem(lo_i) + dev.getItem(hi_i)) / 2.0)
              .withColumn("jump", F.col("human_edits") - F.col("median"))
              .withColumn("z_edit", (F.col("human_edits") - F.col("median"))
                          / (F.lit(1.4826) * F.greatest(F.col("mad"), F.lit(1.0))))
              .withColumn("edit_spike", (F.col("z_edit") >= F.lit(z_min))
                          & (F.col("human_edits") >= F.lit(edits_floor))
                          & (F.col("jump") >= F.lit(min_jump))))
    return scored.select("title", "hour", "human_edits", "bot_edits", "edit_spike",
                         "z_edit", "median", "mad", "jump")


def co_spikes(spikes, counts, edit_spikes):
    """Join the pageview spikes to the edit counts and edit spikes on (title, hour).
    Driven by the spikes table (one row per scored pageview hour); edits left-join in,
    missing -> 0 / false. co_spike = a view spike and an edit spike in the same hour."""
    es = edit_spikes.select("title", "hour", "edit_spike")
    base = spikes.select("title", "hour", "views", "en_views", "mobile_views",
                         "is_spike", "likely_automated")
    return (base.join(counts, ["title", "hour"], "left")
            .join(es, ["title", "hour"], "left")
            .withColumn("human_edits", F.coalesce(F.col("human_edits"), F.lit(0)).cast("long"))
            .withColumn("bot_edits", F.coalesce(F.col("bot_edits"), F.lit(0)).cast("long"))
            .withColumn("edit_spike", F.coalesce(F.col("edit_spike"), F.lit(False)))
            .withColumn("co_spike", F.col("is_spike") & F.col("edit_spike"))
            .select("title", "hour", "views", "en_views", "mobile_views",
                    "human_edits", "bot_edits", "is_spike", "edit_spike", "co_spike",
                    "likely_automated"))

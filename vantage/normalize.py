"""Spark title normalization, shared by the pageview and edit Silver paths.

Spark mirror of prototype/normalize.py: trim, spaces -> underscores, uppercase
the first character only (never lowercase; Wikipedia titles are case-sensitive
after the first letter). Pageviews URL-decode before calling this; edit-stream
titles arrive already decoded, so they call it directly.
"""
from pyspark.sql import functions as F


def normalize_title(col):
    t = F.regexp_replace(F.regexp_replace(col, r"^\s+|\s+$", ""), " ", "_")
    return F.concat(F.upper(t.substr(F.lit(1), F.lit(1))), t.substr(F.lit(2), F.length(t)))

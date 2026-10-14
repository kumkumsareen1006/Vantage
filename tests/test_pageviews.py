import gzip

import pandas as pd
import pytest

from prototype.pageviews import clean_pageview_file, hour_from_filename

LINES = [
    'en Foo_Bar 10 0',
    'en Foo%20Bar 5 0',                       # collides with Foo_Bar after decoding
    'en  Foo_Bar 2 0',                        # extra space before title: still Foo_Bar
    'en.m Foo_Bar 7 0',                       # mobile stays a separate project
    'en "\\"A\\"_Is_for_Alibi" 3 0',          # quoted title with escaped quotes
    'en NaN 4 0',                             # real article called "NaN"
    'en iPhone 6 0',                          # first letter gets uppercased only
    'en C++ 1 0',                             # "+" is not a space
    'en Star_Wars:_Episode_IV 8 0',           # colon in an article title: kept
    'en Talk:Foo_Bar 9 0',                    # non-article namespace: dropped
    'en file:Cat.jpg 9 0',                    # namespace in lowercase: dropped
    'en - 5 0',                               # placeholder title: dropped
    'en Bad_Views 12x 0',                     # quarantine: bad_views
    'en Missing_Views  0',                    # quarantine: empty field
    'en %E9%A1 1 0',                          # quarantine: broken UTF-8 escape
    'en "Unclosed_quote 1 0',                 # quarantine: unbalanced_quote
    'en OnlyThree 1',                         # quarantine: wrong_field_count
    'de Foo 99 0',                            # out of scope: never read
    '"" Wikifunctions:About 1 0',             # out of scope: never read
]


@pytest.fixture
def result(tmp_path):
    path = tmp_path / "pageviews-20261006-120000.gz"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("\n".join(LINES) + "\n")
    return clean_pageview_file(path)


def views(result, project, title):
    rows = result.clean[(result.clean.project == project) & (result.clean.title == title)]
    assert len(rows) <= 1
    return int(rows.views.iloc[0]) if len(rows) else None


def test_hour_is_start_of_hour():
    assert hour_from_filename("pageviews-20261006-120000.gz") == pd.Timestamp("2026-10-06 11:00", tz="UTC")
    assert hour_from_filename("pageviews-20261007-000000.gz") == pd.Timestamp("2026-10-06 23:00", tz="UTC")


def test_collisions_are_summed(result):
    assert views(result, "en", "Foo_Bar") == 17
    assert views(result, "en.m", "Foo_Bar") == 7
    assert result.stats["collisions_merged"] == 2


def test_titles(result):
    assert views(result, "en", '"A"_Is_for_Alibi') == 3
    assert views(result, "en", "NaN") == 4
    assert views(result, "en", "IPhone") == 6
    assert views(result, "en", "C++") == 1
    assert views(result, "en", "Star_Wars:_Episode_IV") == 8


def test_non_articles_dropped(result):
    assert views(result, "en", "Talk:Foo_Bar") is None
    assert views(result, "en", "File:Cat.jpg") is None
    assert views(result, "en", "-") is None
    assert result.stats["dropped_non_article"] == 3


def test_quarantine(result):
    reasons = result.quarantine.set_index("raw_line")["reason"].to_dict()
    assert reasons == {
        "en Bad_Views 12x 0": "bad_views",
        "en Missing_Views  0": "bad_views",
        "en %E9%A1 1 0": "bad_url_encoding",
        'en "Unclosed_quote 1 0': "unbalanced_quote",
        "en OnlyThree 1": "wrong_field_count",
    }


def test_scope(result):
    assert set(result.clean.project) == {"en", "en.m"}
    assert result.stats["lines_total"] == len(LINES)
    assert result.stats["lines_in_scope"] == len(LINES) - 2


def test_every_in_scope_line_is_accounted_for(result):
    s = result.stats
    assert s["lines_in_scope"] == (
        s["quarantined"] + s["dropped_non_article"] + s["collisions_merged"] + s["rows_out"]
    )
import gzip
import json

import pytest

from prototype.edits import clean_edit_files
from prototype.pageviews import clean_pageview_file

HOUR = "2026-10-15T00:30:00.000Z"
WIN = ("2026-10-15T00", "2026-10-15T02")


def ev(title="Some_Page", type="edit", namespace=0, bot=False,
       id="id-1", dt=HOUR, domain="en.wikipedia.org", wiki="enwiki"):
    return {"wiki": wiki, "type": type, "namespace": namespace, "bot": bot,
            "title": title,
            "meta": {"domain": domain, "id": id, "dt": dt}}


def write(tmp_path, events, name="2026-10-15.jsonl"):
    p = tmp_path / name
    with open(p, "w", encoding="utf-8") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")
    return p


def test_canary_dropped(tmp_path):
    p = write(tmp_path, [ev(title="Keep_Page", id="a"),
                         ev(title="Canary_Page", id="b", domain="canary")])
    res = clean_edit_files([p], *WIN)
    assert "Keep_Page" in set(res.clean.title)
    assert "Canary_Page" not in set(res.clean.title)
    assert res.stats["totals"]["dropped_canary"] == 1


def test_duplicate_meta_id_counted_once(tmp_path):
    p = write(tmp_path, [ev(title="Dup_Page", id="dup"),
                         ev(title="Dup_Page", id="dup")])
    res = clean_edit_files([p], *WIN)
    row = res.clean[res.clean.title == "Dup_Page"]
    assert int(row.human_edits.iloc[0]) == 1
    assert res.stats["duplicates_removed"] == 1


def test_bot_vs_human_split(tmp_path):
    p = write(tmp_path, [ev(title="Split_Page", id="h", bot=False),
                         ev(title="Split_Page", id="b", bot=True)])
    res = clean_edit_files([p], *WIN)
    row = res.clean[res.clean.title == "Split_Page"].iloc[0]
    assert int(row.human_edits) == 1
    assert int(row.bot_edits) == 1


def test_log_and_categorize_dropped(tmp_path):
    p = write(tmp_path, [ev(title="A", id="1", type="log"),
                         ev(title="B", id="2", type="categorize"),
                         ev(title="C", id="3", type="edit")])
    res = clean_edit_files([p], *WIN)
    assert set(res.clean.title) == {"C"}
    assert res.stats["totals"]["dropped_other_type"] == 2


def test_nonzero_namespace_dropped(tmp_path):
    p = write(tmp_path, [ev(title="Talk_Thing", id="1", namespace=1),
                         ev(title="Article", id="2", namespace=0)])
    res = clean_edit_files([p], *WIN)
    assert set(res.clean.title) == {"Article"}
    assert res.stats["totals"]["dropped_non_article_ns"] == 1


def test_title_normalization_matches_pageviews(tmp_path):
    # edit-stream titles carry spaces; pageview titles carry underscores; both must land the same
    p = write(tmp_path, [ev(title="data engineering", id="1")])
    edit_title = clean_edit_files([p], *WIN).clean.title.iloc[0]

    gz = tmp_path / "pageviews-20261015-010000.gz"
    with gzip.open(gz, "wt", encoding="utf-8") as fh:
        fh.write("en data_engineering 5 0\n")
    pv_title = clean_pageview_file(gz).clean.title.iloc[0]

    assert edit_title == pv_title == "Data_engineering"


def test_bad_json_quarantined(tmp_path):
    p = tmp_path / "2026-10-15.jsonl"
    with open(p, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(ev(title="Good", id="1")) + "\n")
        fh.write("{not valid json\n")
    res = clean_edit_files([p], *WIN)
    assert (res.quarantine.reason == "bad_json").sum() == 1
    assert "Good" in set(res.clean.title)


def test_out_of_window_dropped(tmp_path):
    p = write(tmp_path, [ev(title="In", id="1", dt="2026-10-15T00:30:00Z"),
                         ev(title="Out", id="2", dt="2026-10-14T23:30:00Z")])
    res = clean_edit_files([p], *WIN)
    assert set(res.clean.title) == {"In"}
    assert res.stats["dropped_out_of_window"] == 1

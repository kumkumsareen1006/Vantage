import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from vantage.ingest.pageviews import USER_AGENT

ADF = Path(__file__).resolve().parents[1] / "infra" / "adf"


def load(rel):
    return json.loads((ADF / rel).read_text())


def day_files(day):
    # mirrors the pipeline's addHours(day, 1..24): file names mark the end of the hour
    start = datetime.combine(day, datetime.min.time())
    return [f"pageviews-{(start + timedelta(hours=h)):%Y%m%d-%H}0000.gz" for h in range(1, 25)]


def test_day_files_cover_the_day():
    names = day_files(date(2026, 10, 15))
    assert len(names) == 24
    assert names[0] == "pageviews-20261015-010000.gz"
    assert names[-1] == "pageviews-20261016-000000.gz"


def test_foreach_is_sequential_over_24_hours():
    fe = load("pipeline/pl_copy_pageviews_day.json")["activities"][0]
    assert fe["type"] == "ForEach"
    assert fe["typeProperties"]["isSequential"] is True
    assert fe["typeProperties"]["items"]["value"] == "@range(1, 24)"


def test_copy_sends_vantage_user_agent():
    copy = load("pipeline/pl_copy_pageviews_day.json")["activities"][0]["typeProperties"]["activities"][0]
    headers = copy["typeProperties"]["source"]["storeSettings"]["additionalHeaders"]
    assert headers == f"User-Agent: {USER_AGENT}"


def test_linked_services_hold_no_credentials():
    http = load("linkedService/ls_wikimedia_dumps.json")
    assert http["typeProperties"]["authenticationType"] == "Anonymous"
    lake = load("linkedService/ls_vantage_lake.json")
    assert set(lake["typeProperties"]) == {"url"}     # no key/SP, so the managed identity is used
    for f in ADF.rglob("*.json"):
        assert not re.search(r"accountKey|servicePrincipal|password|sasUri|secret|connectionString",
                             f.read_text(), re.I), f

import argparse
import re
import sys
from pathlib import Path

import pytest

from vantage import config, spark as vspark

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import run_pipeline  # noqa: E402


SP = {"client_id": "cid", "client_secret": "shh", "tenant_id": "tid"}


def test_oauth_conf_keys():
    conf = vspark.azure_oauth_conf("acct", SP)
    host = "acct.dfs.core.windows.net"
    assert conf[f"fs.azure.account.auth.type.{host}"] == "OAuth"
    assert conf[f"fs.azure.account.oauth2.client.id.{host}"] == "cid"
    assert conf[f"fs.azure.account.oauth2.client.secret.{host}"] == "shh"
    assert conf[f"fs.azure.account.oauth2.client.endpoint.{host}"] == \
        "https://login.microsoftonline.com/tid/oauth2/token"
    # every secret-bearing key must contain "secret" so Spark redacts it
    assert all("secret" in k for k, v in conf.items() if v == "shh")


def test_on_databricks(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert not vspark.on_databricks()
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")
    assert vspark.on_databricks()


class _FakeSession:
    stopped = False

    def stop(self):
        self.stopped = True


def test_release_stops_only_locally(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    s = _FakeSession()
    vspark.release(s)
    assert s.stopped

    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")
    s = _FakeSession()
    vspark.release(s)
    assert not s.stopped


def test_parse_stages_keeps_pipeline_order():
    assert run_pipeline.parse_stages("gold,silver") == ("silver", "gold")
    assert run_pipeline.parse_stages(" bronze ") == ("bronze",)
    assert run_pipeline.parse_stages("bronze,silver,gold") == run_pipeline.STAGES


@pytest.mark.parametrize("bad", ["", "silver,platinum", ","])
def test_parse_stages_rejects(bad):
    with pytest.raises(argparse.ArgumentTypeError):
        run_pipeline.parse_stages(bad)


def test_log_dir(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert run_pipeline.log_dir() == config.ROOT / "logs"
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")
    assert run_pipeline.log_dir() != config.ROOT / "logs"


def test_only_selected_stages_run(monkeypatch):
    ran = []
    monkeypatch.setattr(run_pipeline, "run_bronze", lambda *a: ran.append("bronze") or {})
    monkeypatch.setattr(run_pipeline, "run_silver", lambda *a: ran.append("silver") or {})
    monkeypatch.setattr(run_pipeline, "run_gold", lambda *a: ran.append("gold") or {})
    monkeypatch.setattr(run_pipeline, "setup_logging", lambda *a, **k: __import__("logging").getLogger("t"))
    written = []
    monkeypatch.setattr(run_pipeline, "get_spark", lambda **k: _FakeSession())
    monkeypatch.setattr(run_pipeline, "write_run_metrics", lambda s, rows, p: written.extend(rows))
    try:
        assert run_pipeline.main(["--stages", "silver,gold"]) == 0
    finally:
        config.set_target("local")
    assert ran == ["silver", "gold"]
    assert [r["stage"] for r in written] == ["silver", "gold"]


# The job spec is what Azure bills against, so pin the cost guards.
def test_job_spec_cost_guards():
    spec = (ROOT / "databricks.yml").read_text()
    assert re.search(r"^\s+timeout_seconds: \d+$", spec, re.M)
    assert re.search(r"^\s+num_workers: 0$", spec, re.M)
    assert re.search(r"^\s+max_retries: 0$", spec, re.M)
    assert re.search(r"^\s+max_concurrent_runs: 1$", spec, re.M)
    assert "existing_cluster_id" not in spec        # job cluster only, gone when the run ends
    assert "runtime_engine: STANDARD" in spec       # no Photon
    assert '"--stages", "silver,gold"' in spec
    assert "VANTAGE_TARGET: azure" in spec

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.azure  # conftest skips the module unless SP creds exist


def test_azure_delta_roundtrip():
    # separate process: the shared spark fixture is local-only and a JVM's classpath
    # (the abfss jars) is fixed once launched, so azure gets its own interpreter.
    helper = Path(__file__).parent / "_azure_roundtrip.py"
    r = subprocess.run([sys.executable, str(helper)], capture_output=True, text=True,
                       timeout=600, env={**os.environ, "VANTAGE_TARGET": "azure"})
    assert r.returncode == 0, r.stderr[-3000:]
    assert "ROUNDTRIP_OK" in r.stdout, r.stdout[-2000:]

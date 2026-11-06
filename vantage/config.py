"""Load config/vantage.toml once and expose typed values.

Single source of truth for paths, scope and thresholds, shared by the vantage
package and the Phase 0 prototype (prototype/config.py re-exports from here).
"""
import os
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "vantage.toml"

with open(CONFIG_PATH, "rb") as fh:
    CONFIG = tomllib.load(fh)

PROJECTS = tuple(CONFIG["projects"]["in_scope"])
NON_ARTICLE_NAMESPACES = tuple(CONFIG["titles"]["non_article_namespaces"])
PLACEHOLDER_TITLES = tuple(CONFIG["titles"]["placeholders"])

WINDOW_START = CONFIG["window"]["start"]
WINDOW_END = CONFIG["window"]["end"]

BASELINE_HOURS = CONFIG["spikes"]["baseline_hours"]
Z_MIN = CONFIG["spikes"]["z_min"]
VIEWS_FLOOR = CONFIG["spikes"]["views_floor"]
MIN_JUMP = CONFIG["spikes"]["min_jump"]
DESKTOP_SHARE_MIN = CONFIG["spikes"]["desktop_share_min"]
ISOLATION_RATIO = CONFIG["spikes"]["isolation_ratio"]

EDIT_BASELINE_HOURS = CONFIG["edit_spikes"]["baseline_hours"]
EDIT_Z_MIN = CONFIG["edit_spikes"]["z_min"]
EDITS_FLOOR = CONFIG["edit_spikes"]["edits_floor"]
EDIT_MIN_JUMP = CONFIG["edit_spikes"]["min_jump"]

EDITS_BACKFILL_START = CONFIG["edits_backfill"]["start"]
EDITS_BACKFILL_END = CONFIG["edits_backfill"]["end"]


KAFKA = CONFIG["kafka"]
KAFKA_BOOTSTRAP = KAFKA["bootstrap_servers"]
KAFKA_TOPIC = KAFKA["topic"]


def _abs(rel: str) -> Path:
    return (ROOT / rel).resolve()


RAW_PAGEVIEWS = _abs(CONFIG["paths"]["raw_pageviews"])
RAW_EDITS = _abs(CONFIG["paths"]["raw_edits"])
INTERIM_PAGEVIEWS = _abs(CONFIG["paths"]["interim_pageviews"])

AZURE = CONFIG.get("azure", {})

# Phase 3: the batch tables can live on ADLS Gen2 instead of local disk. The switch
# is --target local|azure (or VANTAGE_TARGET); only these tables move -- the streaming
# tables and checkpoints stay local, and raw .gz files are always read from local disk.
_AZURE_LAKE_KEYS = ("bronze", "silver", "gold", "quarantine", "run_metrics")

_LOCAL_LAKE = {name: _abs(rel) for name, rel in CONFIG["lake"].items()}

# Structured Streaming checkpoint locations -> absolute paths
CHECKPOINTS = {name: _abs(rel) for name, rel in CONFIG.get("checkpoints", {}).items()}


def _azure_uri(name):
    return f"abfss://{AZURE['container']}@{AZURE['storage_account']}.dfs.core.windows.net/{name}"


def lake_join(base, *parts):
    """Join a lake path with child segments. Path join locally; string join for an
    abfss:// URI (Path would collapse the // after the scheme)."""
    base = str(base)
    if "://" in base:
        return "/".join([base.rstrip("/"), *parts])
    return Path(base).joinpath(*parts)


# bronze / silver / gold / quarantine / run_metrics / *_edits -> local paths or (under
# --target azure) abfss:// URIs for the batch tables. Mutated in place by set_target so
# every `config.LAKE[...]` lookup sees the active target.
LAKE = dict(_LOCAL_LAKE)
TARGET = "local"


def set_target(target):
    global TARGET
    if target not in ("local", "azure"):
        raise ValueError(f"unknown target {target!r} (use local or azure)")
    if target == "azure" and not AZURE:
        raise RuntimeError("--target azure needs an [azure] section in config/vantage.toml")
    TARGET = target
    LAKE.clear()
    LAKE.update(_LOCAL_LAKE)
    if target == "azure":
        LAKE.update({name: _azure_uri(name) for name in _AZURE_LAKE_KEYS})


set_target(os.environ.get("VANTAGE_TARGET", "local"))

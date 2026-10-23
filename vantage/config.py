"""Load config/vantage.toml once and expose typed values.

Single source of truth for paths, scope and thresholds, shared by the vantage
package and the Phase 0 prototype (prototype/config.py re-exports from here).
"""
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


def _abs(rel: str) -> Path:
    return (ROOT / rel).resolve()


RAW_PAGEVIEWS = _abs(CONFIG["paths"]["raw_pageviews"])
RAW_EDITS = _abs(CONFIG["paths"]["raw_edits"])
INTERIM_PAGEVIEWS = _abs(CONFIG["paths"]["interim_pageviews"])

# bronze / silver / gold / quarantine -> absolute paths
LAKE = {name: _abs(rel) for name, rel in CONFIG["lake"].items()}

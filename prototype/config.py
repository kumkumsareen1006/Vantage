"""Phase 0 config, sourced from config/vantage.toml via vantage.config so every
tunable value lives in one place. Kept as a thin re-export for the prototype's
existing imports (prototype.pageviews, edits, spikes)."""
from vantage.config import (  # noqa: F401
    PROJECTS,
    NON_ARTICLE_NAMESPACES,
    PLACEHOLDER_TITLES,
    WINDOW_START,
    WINDOW_END,
    BASELINE_HOURS,
    Z_MIN,
    VIEWS_FLOOR,
    MIN_JUMP,
)

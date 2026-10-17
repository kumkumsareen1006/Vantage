PROJECTS = ("en", "en.m")

NON_ARTICLE_NAMESPACES = (
    "Talk",
    "User", "User_talk",
    "Wikipedia", "Wikipedia_talk", "WP", "WT", "Project", "Project_talk",
    "File", "File_talk", "Image", "Image_talk", "Media",
    "MediaWiki", "MediaWiki_talk",
    "Template", "Template_talk",
    "Help", "Help_talk",
    "Category", "Category_talk",
    "Portal", "Portal_talk",
    "Draft", "Draft_talk",
    "TimedText", "TimedText_talk",
    "Module", "Module_talk",
    "Special",
)
PLACEHOLDER_TITLES = ("-",)

# Day 3 analysis window (UTC), by hour START; end is exclusive.
WINDOW_START = "2026-10-15T00"
WINDOW_END = "2026-10-17T00"

# Spike detection (robust z-score over the previous BASELINE_HOURS, current hour excluded).
# Tuned on the Oct 15-16 2026 window; see docs/cleaning-rules.md.
BASELINE_HOURS = 24
Z_MIN = 6.0
VIEWS_FLOOR = 1000
MIN_JUMP = 1000
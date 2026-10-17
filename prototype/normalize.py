"""Title normalization shared by the pageview and edit cleaners.

Both sources must land on the same string for the same page so they join later.
The shared part is: trim, spaces to underscores, uppercase only the first
character. Pageviews additionally URL-decode before this; edit-stream titles
are already decoded, so they call this directly.
"""
import pandas as pd


def normalize_title_series(title: pd.Series) -> pd.Series:
    title = title.str.strip().str.replace(" ", "_", regex=False)
    # uppercase the first character only; Wikipedia titles are case-sensitive after it
    return title.str[:1].str.upper() + title.str[1:]

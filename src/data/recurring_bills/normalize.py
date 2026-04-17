"""Hebrew name normalization for the K16-K18 name-match fallback."""

from __future__ import annotations

import re

_YEAR_SUFFIX_RE = re.compile(
    r",\s*הת\S+[\-\u2013]\d{4}\s*$",
)


def strip_year_suffix(name: str) -> str:
    """Strip the trailing Hebrew year suffix and trailing whitespace.

    Matches suffixes shaped like ``, התשס"ג-2003`` or ``, התשע״ג\u20132013``.
    Returns the input unchanged if no suffix is present.
    """
    if not name:
        return name
    return _YEAR_SUFFIX_RE.sub("", name).rstrip()

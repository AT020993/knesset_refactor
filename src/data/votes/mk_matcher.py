"""Resolve a Knesset WebSiteApi ``MkName`` to an internal ``PersonID``.

The votes API gives a display name (``"LastName FirstName"``) but no id, so we
bridge to ``KNS_Person.PersonID`` using a name dictionary scoped to a single
Knesset term. Scoping to the term's members (a) keeps the candidate set small
(~150) and (b) auto-resolves homonyms like "ישראל כץ" who appear across
different Knessets.

Matching, validated at 118/118 on a sample of Knesset-25 votes:
  * normalise away nikud, geresh/quote marks, and repeated whitespace;
  * the API name must start with the person's (multi-word) LastName;
  * the remaining first-name part must be token-prefix compatible with the
    person's FirstName, with the final token allowed to match as a character
    prefix (handles "בני"→"בנימין", "אורית"→"אורית מלכה", "יצחק"→"יצחק זאב").
"""

from __future__ import annotations

import re

_STRIP = re.compile(r"[֑-ׇ'\"׳״]")  # nikud + geresh/gershayim + quotes
_WS = re.compile(r"\s+")


def normalize(name: str | None) -> str:
    """Collapse whitespace and drop nikud/geresh/quote marks."""
    if not name:
        return ""
    return _WS.sub(" ", _STRIP.sub("", name)).strip()


def _first_name_compatible(rest: str, first_name: str) -> bool:
    """True if the API name's leftover (post-lastname) matches the FirstName.

    All leading tokens must match exactly; the last comparable token may match
    as a character-level prefix in either direction.
    """
    if not rest or not first_name:
        return False
    a, b = rest.split(" "), first_name.split(" ")
    n = min(len(a), len(b))
    if a[: n - 1] != b[: n - 1]:
        return False
    last_a, last_b = a[n - 1], b[n - 1]
    return last_a.startswith(last_b) or last_b.startswith(last_a)


class MkNameMatcher:
    """Name→PersonID resolver for one Knesset term.

    Build with ``people`` = iterable of ``(person_id, first_name, last_name)``
    for the term's members (e.g. from ``KNS_PersonToPosition`` filtered by
    ``KnessetNum``).
    """

    def __init__(self, people: list[tuple[int, str, str]]):
        # Store normalized (person_id, first, last); sort by descending lastname
        # length so longer (more specific) surnames win the startswith test.
        self._people = sorted(
            ((pid, normalize(fn), normalize(ln)) for pid, fn, ln in people),
            key=lambda p: len(p[2]),
            reverse=True,
        )

    def resolve(self, mk_name: str) -> int | None:
        """Return the unique PersonID for ``mk_name``, or None if no unique match."""
        m = normalize(mk_name)
        if not m:
            return None
        hits: set[int] = set()
        for pid, fn, ln in self._people:
            if m != ln and not m.startswith(ln + " "):
                continue
            rest = m[len(ln) :].strip()
            if _first_name_compatible(rest, fn):
                hits.add(pid)
        return next(iter(hits)) if len(hits) == 1 else None

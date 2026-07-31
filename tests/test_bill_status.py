"""Tests for the hand-authored bill reading-stage ladder."""

from __future__ import annotations

from data.snapshots.bill_status import (
    BILL_STATUS_RUNGS,
    RUNG_ORDER,
    rung_for,
)


def test_every_rung_in_order_is_defined():
    assert set(RUNG_ORDER) == set(BILL_STATUS_RUNGS)


def test_no_status_id_appears_in_two_rungs():
    seen: dict[int, str] = {}
    for rung, ids in BILL_STATUS_RUNGS.items():
        for sid in ids:
            assert sid not in seen, f"{sid} in both {seen.get(sid)} and {rung}"
            seen[sid] = rung


def test_rung_for_resolves_known_statuses():
    assert rung_for(118) == "התקבלה — הפכה לחוק"
    assert rung_for(104) == "הונחה — טרם נדונה"
    assert rung_for(113) == "עברה קריאה ראשונה"


def test_rung_for_returns_none_for_unknown():
    assert rung_for(999) is None


def test_law_rung_is_the_highest_reading_milestone():
    """Ordering is the point: 'the last reading passed' means the highest
    rung reached, so a bill that became law must outrank one still in
    first-reading prep."""
    assert RUNG_ORDER.index("התקבלה — הפכה לחוק") > RUNG_ORDER.index(
        "עברה קריאה ראשונה"
    )
    assert RUNG_ORDER.index("עברה קריאה ראשונה") > RUNG_ORDER.index(
        "עברה קריאה טרומית"
    )

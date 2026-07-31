"""Reading-stage ladder for private-member bills.

``KNS_Status`` carries 35 statuses of TypeDesc 'הצעת חוק' but its
``OrderTransition`` column is entirely NULL, so the progression a bill
makes through readings cannot be derived — it is authored here.

This lives upstream, not in the consumer, so snapshots carry a resolved
label and rung rather than a raw id every consumer would have to decode
identically. Validated against the live warehouse: 29 distinct statuses
occur across 59,015 bills and every one maps to a rung.
"""

from __future__ import annotations

LAID = "הונחה — טרם נדונה"
PRELIMINARY = "עברה קריאה טרומית"
FIRST = "עברה קריאה ראשונה"
LAW = "התקבלה — הפכה לחוק"
STOPPED = "נעצרה / הוסרה"
MERGED = "מוזגה / פוצלה / הוסבה"
CONTINUITY = "דין רציפות"

#: Ordered lowest → highest. "The last reading a bill passed" means the
#: highest rung it reached, so order is semantic, not cosmetic. The three
#: terminal-but-not-progress rungs sit after the reading ladder.
RUNG_ORDER: tuple[str, ...] = (
    LAID,
    PRELIMINARY,
    FIRST,
    LAW,
    STOPPED,
    MERGED,
    CONTINUITY,
)

BILL_STATUS_RUNGS: dict[str, tuple[int, ...]] = {
    LAID: (104, 150),
    PRELIMINARY: (101, 106, 108, 109, 111, 141, 142, 167),
    FIRST: (113, 114, 115, 117, 130, 131, 178, 179),
    LAW: (118,),
    STOPPED: (110, 140, 143, 176, 177),
    MERGED: (122, 124, 126, 158, 161, 162, 165, 169),
    CONTINUITY: (120, 175, 181),
}

_BY_STATUS: dict[int, str] = {
    sid: rung for rung, ids in BILL_STATUS_RUNGS.items() for sid in ids
}


def rung_for(status_id: int) -> str | None:
    """Rung for a bill status id, or None if unmapped.

    None is a signal, not a default — Task 2's export test fails the build
    on any unmapped status so a new Knesset introducing one cannot silently
    fall out of every rung.
    """
    return _BY_STATUS.get(status_id)

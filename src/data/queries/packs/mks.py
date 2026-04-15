"""MK (Member of Knesset) query definitions.

MVP shape for Phase 0 — one query per domain pack, proving the export shape.
Fan-out (bills-by-mk, questions-by-mk, motions-by-mk, roles history, etc.)
lands in Phase 4 alongside the real FastAPI endpoints.
"""

from __future__ import annotations

from typing import Any

MK_QUERIES: dict[str, dict[str, Any]] = {
    "mk_summary": {
        "sql": """
WITH LatestPosition AS (
    -- One row per MK: their globally latest faction assignment.
    -- StartDate is VARCHAR in the warehouse; cast so sort is chronological,
    -- not lexicographic (e.g., '2020-12' would sort after '2020-2' otherwise).
    SELECT
        PersonID,
        KnessetNum,
        FactionID,
        FactionName,
        DutyDesc,
        StartDate,
        ROW_NUMBER() OVER (
            PARTITION BY PersonID
            ORDER BY
                TRY_CAST(StartDate AS TIMESTAMP) DESC NULLS LAST,
                KnessetNum DESC NULLS LAST,
                PersonToPositionID DESC
        ) AS rn
    FROM KNS_PersonToPosition
    WHERE FactionID IS NOT NULL
)
SELECT
    p.PersonID                                  AS mk_id,
    TRIM(p.FirstName || ' ' || p.LastName)      AS name_he,
    p.GenderDesc                                AS gender,
    CAST(lp.KnessetNum AS INTEGER)              AS knesset_num,
    CAST(lp.FactionID AS BIGINT)                AS faction_id,
    lp.FactionName                              AS faction_name,
    lp.DutyDesc                                 AS current_role,
    p.IsCurrent                                 AS is_current
FROM KNS_Person p
LEFT JOIN LatestPosition lp
    ON p.PersonID = lp.PersonID AND lp.rn = 1
ORDER BY p.PersonID
""".strip(),
        "knesset_filter_column": "lp.KnessetNum",
        "faction_filter_column": "lp.FactionID",
        "description": "Per-MK bio row with most recent faction and duty.",
    },
}

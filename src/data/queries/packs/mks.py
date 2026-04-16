"""MK (Member of Knesset) query definitions.

One row per (MK, Knesset-term) — the downstream API returns all term rows
for a given MK, ordered newest first, so the site can show full career history.
"""

from __future__ import annotations

from typing import Any

MK_QUERIES: dict[str, dict[str, Any]] = {
    "mk_summary": {
        "sql": """
WITH LatestPerTerm AS (
    -- One row per (MK, Knesset): their latest faction within that term.
    -- An MK can switch factions mid-term; we keep the most recent.
    SELECT
        PersonID,
        KnessetNum,
        FactionID,
        FactionName,
        DutyDesc,
        StartDate,
        ROW_NUMBER() OVER (
            PARTITION BY PersonID, KnessetNum
            ORDER BY
                TRY_CAST(StartDate AS TIMESTAMP) DESC NULLS LAST,
                PersonToPositionID DESC
        ) AS rn
    FROM KNS_PersonToPosition
    WHERE FactionID IS NOT NULL
)
SELECT
    p.PersonID                                  AS mk_id,
    TRIM(p.FirstName || ' ' || p.LastName)      AS name_he,
    p.GenderDesc                                AS gender,
    CAST(lpt.KnessetNum AS INTEGER)             AS knesset_num,
    CAST(lpt.FactionID AS BIGINT)               AS faction_id,
    lpt.FactionName                             AS faction_name,
    lpt.DutyDesc                                AS current_role,
    p.IsCurrent                                 AS is_current
FROM KNS_Person p
JOIN LatestPerTerm lpt
    ON p.PersonID = lpt.PersonID AND lpt.rn = 1
ORDER BY p.PersonID, lpt.KnessetNum DESC NULLS LAST
""".strip(),
        "knesset_filter_column": "lpt.KnessetNum",
        "faction_filter_column": "lpt.FactionID",
        "description": "One row per (MK, Knesset-term) with latest faction assignment.",
    },
}

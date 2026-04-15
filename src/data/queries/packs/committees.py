"""Committee query definitions.

MVP shape for Phase 0. Chair resolution is a placeholder — KNS_PersonToPosition
DutyDesc does not cleanly tag chairs in the current warehouse dump; the real
lookup will be sorted in Phase 4 once a chairmanship table (or a better
heuristic) is in place.
"""

from __future__ import annotations

from typing import Any

COMMITTEES_QUERIES: dict[str, dict[str, Any]] = {
    "committee_list": {
        "sql": """
WITH CommitteeSessionCounts AS (
    SELECT
        CommitteeID,
        KnessetNum,
        COUNT(DISTINCT CommitteeSessionID) AS session_count
    FROM KNS_CommitteeSession
    WHERE CommitteeID IS NOT NULL
    GROUP BY CommitteeID, KnessetNum
)
SELECT
    CAST(c.CommitteeID AS BIGINT)               AS committee_id,
    c.Name                                      AS name_he,
    CAST(c.KnessetNum AS INTEGER)               AS knesset_num,
    c.CommitteeTypeDesc                         AS type_he,
    c.CategoryDesc                              AS category_he,
    COALESCE(csc.session_count, 0)              AS session_count,
    c.StartDate                                 AS start_date,
    c.FinishDate                                AS finish_date
FROM KNS_Committee c
LEFT JOIN CommitteeSessionCounts csc
    ON c.CommitteeID = csc.CommitteeID AND c.KnessetNum = csc.KnessetNum
""".strip(),
        "knesset_filter_column": "c.KnessetNum",
        "faction_filter_column": None,
        "description": "Committees per Knesset with type, category, and session count.",
    },
}

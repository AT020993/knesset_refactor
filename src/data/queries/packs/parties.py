"""Party (faction) query definitions.

MVP shape for Phase 0. Richer views (historical faction splits/unions,
cross-Knesset continuity via ``UserFactionCoalitionStatus.NewFactionName``)
come in Phase 4.
"""

from __future__ import annotations

from typing import Any

PARTIES_QUERIES: dict[str, dict[str, Any]] = {
    "party_list": {
        "sql": """
WITH FactionMembers AS (
    SELECT
        KnessetNum,
        FactionID,
        COUNT(DISTINCT PersonID) AS member_count
    FROM KNS_PersonToPosition
    WHERE FactionID IS NOT NULL
    GROUP BY KnessetNum, FactionID
)
SELECT
    CAST(f.FactionID AS BIGINT)             AS party_id,
    f.Name                                  AS name_he,
    CAST(f.KnessetNum AS INTEGER)           AS knesset_num,
    COALESCE(fm.member_count, 0)            AS member_count,
    ufcs.CoalitionStatus                    AS coalition_status,
    ufcs.NewFactionName                     AS standardised_name,
    f.IsCurrent                             AS is_current,
    f.StartDate                             AS start_date,
    f.FinishDate                            AS finish_date
FROM KNS_Faction f
LEFT JOIN FactionMembers fm
    ON f.FactionID = fm.FactionID AND f.KnessetNum = fm.KnessetNum
LEFT JOIN UserFactionCoalitionStatus ufcs
    ON f.FactionID = ufcs.FactionID AND f.KnessetNum = ufcs.KnessetNum
""".strip(),
        "knesset_filter_column": "f.KnessetNum",
        "faction_filter_column": "f.FactionID",
        "description": "Factions per Knesset with member count and coalition flag.",
    },
}

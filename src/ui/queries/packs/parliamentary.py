"""Parliamentary query definitions."""

from typing import Any

from ui.queries.sql_templates import SQLTemplates

PARLIAMENTARY_QUERIES: dict[str, dict[str, Any]] = {
    "Parliamentary Queries (Full Details)": {
        "sql": f"""
WITH {SQLTemplates.STANDARD_FACTION_LOOKUP},
{SQLTemplates.MINISTER_LOOKUP}
SELECT
    Q.QueryID,
    Q.Number,
    Q.KnessetNum,
    Q.Name AS QueryName,
    Q.TypeID AS QueryTypeID,
    Q.TypeDesc AS QueryTypeDesc,
    S."Desc" AS QueryStatusDesc,
    P.FirstName AS MKFirstName,
    P.LastName AS MKLastName,
    P.GenderDesc AS MKGender,
    P.IsCurrent AS MKIsCurrent,

    -- Use simplified faction lookup with our improved faction data
    COALESCE(f.Name, 'Unknown') AS MKFactionName,
    COALESCE(ufs.CoalitionStatus, 'Unknown') AS MKFactionCoalitionStatus,

    M.Name AS MinistryName,
    M.IsActive AS MinistryIsActive,
    strftime(CAST(Q.SubmitDate AS TIMESTAMP), '%Y-%m-%d') AS SubmitDateFormatted,
    strftime(CAST(Q.ReplyMinisterDate AS TIMESTAMP), '%Y-%m-%d') AS AnswerDate,

    -- Simplified minister lookup
    COALESCE(min_p.FirstName || ' ' || min_p.LastName, 'Unknown') AS ResponsibleMinisterName,
    COALESCE(ml.DutyDesc, 'Unknown') AS ResponsibleMinisterPosition,

    -- Research coding columns (policy classification)
    uqcoding.MajorIL AS CodingMajorIL,
    uqcoding.MinorIL AS CodingMinorIL,
    uqcoding.MajorCAP AS CodingMajorCAP,
    uqcoding.MinorCAP AS CodingMinorCAP,
    uqcoding.Religion AS CodingReligion,
    uqcoding.Territories AS CodingTerritories

FROM KNS_Query Q
LEFT JOIN KNS_Person P ON Q.PersonID = P.PersonID
LEFT JOIN KNS_GovMinistry M ON Q.GovMinistryID = M.GovMinistryID
LEFT JOIN KNS_Status S ON Q.StatusID = S.StatusID
LEFT JOIN StandardFactionLookup sfl ON Q.PersonID = sfl.PersonID
    AND Q.KnessetNum = sfl.KnessetNum
    AND sfl.rn = 1
LEFT JOIN KNS_Faction f ON sfl.FactionID = f.FactionID
LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID
    AND Q.KnessetNum = ufs.KnessetNum
LEFT JOIN MinisterLookup ml ON Q.GovMinistryID = ml.GovMinistryID AND ml.rn = 1
LEFT JOIN KNS_Person min_p ON ml.PersonID = min_p.PersonID
LEFT JOIN UserQueryCoding uqcoding ON Q.QueryID = uqcoding.QueryID

ORDER BY Q.KnessetNum DESC, Q.QueryID DESC
LIMIT 1000;
        """,
        "knesset_filter_column": "Q.KnessetNum",
        "faction_filter_column": "f.FactionID",
        "description": (
            "Comprehensive query data with faction details, "
            "ministry information, and responsible ministers"
        ),
    },
}

"""
Predefined SQL queries for data analysis and export - FIXED VERSION.

This module contains complex SQL queries that were previously hardcoded
in the UI layer. Each query is defined with its SQL and metadata for
filtering and display purposes.

Fixed to avoid LATERAL join issues with window functions in DuckDB.
"""

from typing import Any, Dict

# Query definitions with their SQL and metadata
PREDEFINED_QUERIES: Dict[str, Dict[str, Any]] = {
    "Queries + Full Details": {
        "sql": """
WITH FactionLookup AS (
    SELECT 
        ptp.PersonID,
        ptp.KnessetNum,
        ptp.FactionID,
        ROW_NUMBER() OVER (PARTITION BY ptp.PersonID, ptp.KnessetNum ORDER BY 
            CASE WHEN ptp.FactionID IS NOT NULL THEN 0 ELSE 1 END,
            ptp.StartDate DESC NULLS LAST
        ) as rn
    FROM KNS_PersonToPosition ptp
    WHERE ptp.FactionID IS NOT NULL
),
MinisterLookup AS (
    SELECT 
        p2p.GovMinistryID,
        p2p.PersonID,
        p2p.DutyDesc,
        ROW_NUMBER() OVER (PARTITION BY p2p.GovMinistryID ORDER BY p2p.StartDate DESC) as rn
    FROM KNS_PersonToPosition p2p
    WHERE (
        p2p.DutyDesc LIKE 'שר %' OR p2p.DutyDesc LIKE 'השר %' OR p2p.DutyDesc = 'שר' OR
        p2p.DutyDesc LIKE 'שרה %' OR p2p.DutyDesc LIKE 'השרה %' OR p2p.DutyDesc = 'שרה' OR
        p2p.DutyDesc = 'ראש הממשלה'
    )
    AND p2p.DutyDesc NOT LIKE 'סגן %' AND p2p.DutyDesc NOT LIKE 'סגנית %'
    AND p2p.DutyDesc NOT LIKE '%יושב ראש%' AND p2p.DutyDesc NOT LIKE '%יו""ר%'
)
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
    COALESCE(ml.DutyDesc, 'Unknown') AS ResponsibleMinisterPosition

FROM KNS_Query Q
LEFT JOIN KNS_Person P ON Q.PersonID = P.PersonID
LEFT JOIN KNS_GovMinistry M ON Q.GovMinistryID = M.GovMinistryID
LEFT JOIN KNS_Status S ON Q.StatusID = S.StatusID
LEFT JOIN FactionLookup fl ON Q.PersonID = fl.PersonID 
    AND Q.KnessetNum = fl.KnessetNum 
    AND fl.rn = 1
LEFT JOIN KNS_Faction f ON fl.FactionID = f.FactionID
LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID 
    AND Q.KnessetNum = ufs.KnessetNum
LEFT JOIN MinisterLookup ml ON Q.GovMinistryID = ml.GovMinistryID AND ml.rn = 1
LEFT JOIN KNS_Person min_p ON ml.PersonID = min_p.PersonID

ORDER BY Q.KnessetNum DESC, Q.QueryID DESC;
        """,
        "knesset_filter_column": "Q.KnessetNum",
        "faction_filter_column": "f.FactionID",
        "description": (
            "Comprehensive query data with faction details, "
            "ministry information, and responsible ministers"
        ),
    },
    "Agenda Items + Full Details": {
        "sql": """
WITH AgendaFactionLookup AS (
    SELECT 
        ptp.PersonID,
        ptp.KnessetNum,
        ptp.FactionID,
        ROW_NUMBER() OVER (PARTITION BY ptp.PersonID, ptp.KnessetNum ORDER BY 
            CASE WHEN ptp.FactionID IS NOT NULL THEN 0 ELSE 1 END,
            ptp.StartDate DESC NULLS LAST
        ) as rn
    FROM KNS_PersonToPosition ptp
    WHERE ptp.FactionID IS NOT NULL
)
SELECT
    A.AgendaID,
    A.Number,
    A.KnessetNum,
    A.Name AS AgendaName,
    A.SubTypeID AS AgendaSubTypeID,
    A.SubTypeDesc AS AgendaSubTypeDesc,
    A.StatusID AS AgendaStatusID,
    S."Desc" AS AgendaStatusDesc,
    A.ClassificationID AS AgendaClassificationID,
    A.ClassificationDesc AS AgendaClassificationDesc,
    P.FirstName AS InitiatorMKFirstName,
    P.LastName AS InitiatorMKLastName,
    P.GenderDesc AS InitiatorMKGender,
    P.IsCurrent AS InitiatorMKIsCurrent,

    -- Simplified faction lookup
    COALESCE(f.Name, 'Unknown') AS InitiatorMKFactionName,
    COALESCE(ufs.CoalitionStatus, 'Unknown') AS InitiatorMKFactionCoalitionStatus,

    strftime(CAST(A.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d') AS LastUpdatedDateFormatted,
    strftime(CAST(A.PresidentDecisionDate AS TIMESTAMP), '%Y-%m-%d') AS PresidentDecisionDateFormatted

FROM KNS_Agenda A
LEFT JOIN KNS_Person P ON A.InitiatorPersonID = P.PersonID
LEFT JOIN KNS_Status S ON A.StatusID = S.StatusID
LEFT JOIN AgendaFactionLookup afl ON A.InitiatorPersonID = afl.PersonID 
    AND A.KnessetNum = afl.KnessetNum 
    AND afl.rn = 1
LEFT JOIN KNS_Faction f ON afl.FactionID = f.FactionID
LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID 
    AND A.KnessetNum = ufs.KnessetNum

ORDER BY A.KnessetNum DESC, A.AgendaID DESC;
        """,
        "knesset_filter_column": "A.KnessetNum",
        "faction_filter_column": "f.FactionID",
        "description": (
            "Comprehensive agenda items data with faction details "
            "and status information"
        ),
    },
    "Bills + Full Details": {
        "sql": """
SELECT
    B.BillID,
    B.KnessetNum,
    B.Name AS BillName,
    B.SubTypeID AS BillSubTypeID,
    B.SubTypeDesc AS BillSubTypeDesc,
    B.PrivateNumber,
    B.CommitteeID AS BillCommitteeID,
    C.Name AS CommitteeName,
    B.StatusID AS BillStatusID,
    S."Desc" AS BillStatusDesc,
    B.Number AS BillNumber,
    B.PostponementReasonID,
    B.PostponementReasonDesc,
    B.SummaryLaw AS BillSummaryLaw,
    strftime(CAST(B.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d') AS LastUpdatedDateFormatted,

    -- Bill initiator information using our new table
    GROUP_CONCAT(DISTINCT (Pi.FirstName || ' ' || Pi.LastName), ', ') AS BillInitiatorNames,
    GROUP_CONCAT(DISTINCT Pi.FirstName, ', ') AS BillInitiatorFirstNames,
    GROUP_CONCAT(DISTINCT Pi.LastName, ', ') AS BillInitiatorLastNames,
    COUNT(DISTINCT BI.PersonID) AS BillInitiatorCount

FROM KNS_Bill B
LEFT JOIN KNS_Committee C ON B.CommitteeID = C.CommitteeID
LEFT JOIN KNS_Status S ON B.StatusID = S.StatusID
LEFT JOIN KNS_BillInitiator BI ON B.BillID = BI.BillID
LEFT JOIN KNS_Person Pi ON BI.PersonID = Pi.PersonID

GROUP BY 
    B.BillID, B.KnessetNum, B.Name, B.SubTypeID, B.SubTypeDesc, B.PrivateNumber,
    B.CommitteeID, C.Name, B.StatusID, S."Desc", B.Number, B.PostponementReasonID,
    B.PostponementReasonDesc, B.SummaryLaw, B.LastUpdatedDate

ORDER BY B.KnessetNum DESC, B.BillID DESC;
        """,
        "knesset_filter_column": "B.KnessetNum",
        "faction_filter_column": "NULL", # Bills don't have direct faction association
        "description": (
            "Comprehensive bill data with initiator information, "
            "committee assignments, and status details"
        ),
    },
}


def get_query_sql(query_name: str) -> str:
    """Get the SQL for a specific query."""
    return PREDEFINED_QUERIES.get(query_name, {}).get("sql", "")


def get_query_info(query_name: str) -> Dict[str, Any]:
    """Get all information for a specific query."""
    return PREDEFINED_QUERIES.get(query_name, {})


def get_all_query_names() -> list:
    """Get list of all available query names."""
    return list(PREDEFINED_QUERIES.keys())


def get_filter_columns(query_name: str) -> tuple:
    """Get the filter column names for a query."""
    query_info = PREDEFINED_QUERIES.get(query_name, {})
    return (
        query_info.get("knesset_filter_column"),
        query_info.get("faction_filter_column")
    )
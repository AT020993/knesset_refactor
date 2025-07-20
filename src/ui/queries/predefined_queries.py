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
    COALESCE(P.FirstName, 'Institutional') AS InitiatorMKFirstName,
    COALESCE(P.LastName, 'Initiative') AS InitiatorMKLastName,
    COALESCE(P.GenderDesc, 'N/A') AS InitiatorMKGender,
    COALESCE(P.IsCurrent, false) AS InitiatorMKIsCurrent,

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
WITH BillMainInitiatorFaction AS (
    SELECT 
        BI.BillID,
        B.KnessetNum,
        BI.PersonID as MainInitiatorPersonID,
        ptp.FactionID,
        ROW_NUMBER() OVER (PARTITION BY BI.BillID ORDER BY BI.Ordinal) as rn
    FROM KNS_BillInitiator BI
    JOIN KNS_Bill B ON BI.BillID = B.BillID
    LEFT JOIN KNS_PersonToPosition ptp ON BI.PersonID = ptp.PersonID 
        AND B.KnessetNum = ptp.KnessetNum
        AND ptp.FactionID IS NOT NULL
    WHERE BI.Ordinal = 1
),
BillMergeInfo AS (
    SELECT 
        bu.UnionBillID as MergedBillID,
        bu.MainBillID as LeadingBillID,
        lb.Name as LeadingBillName,
        lb.Number as LeadingBillNumber
    FROM KNS_BillUnion bu
    LEFT JOIN KNS_Bill lb ON bu.MainBillID = lb.BillID
),
CommitteeSessionActivity AS (
    SELECT 
        CAST(CommitteeID AS BIGINT) as CommitteeID,
        COUNT(*) as TotalSessions,
        MIN(CAST(StartDate AS TIMESTAMP)) as FirstSessionDate,
        MAX(CAST(StartDate AS TIMESTAMP)) as LastSessionDate,
        COUNT(DISTINCT KnessetNum) as KnessetSpan,
        COUNT(CASE WHEN StartDate IS NOT NULL AND FinishDate IS NOT NULL THEN 1 END) as SessionsWithDuration,
        -- Calculate realistic session frequency per year (capped and filtered)
        CASE 
            WHEN MIN(CAST(StartDate AS TIMESTAMP)) IS NOT NULL AND MAX(CAST(StartDate AS TIMESTAMP)) IS NOT NULL THEN
                CASE 
                    WHEN DATE_DIFF('day', MIN(CAST(StartDate AS TIMESTAMP)), MAX(CAST(StartDate AS TIMESTAMP))) >= 30 THEN
                        -- Only calculate for committees active at least 30 days, cap at 260 sessions/year (5 per week)
                        LEAST(
                            ROUND(COUNT(*) / (DATE_DIFF('day', MIN(CAST(StartDate AS TIMESTAMP)), MAX(CAST(StartDate AS TIMESTAMP))) / 365.25), 1),
                            260.0
                        )
                    ELSE 
                        -- For short-term committees, show sessions per month instead
                        ROUND(COUNT(*) / GREATEST(DATE_DIFF('day', MIN(CAST(StartDate AS TIMESTAMP)), MAX(CAST(StartDate AS TIMESTAMP))) / 30.0, 1), 1)
                END
            ELSE 0
        END as AvgSessionsPerYear
    FROM KNS_CommitteeSession 
    WHERE StartDate IS NOT NULL AND CommitteeID IS NOT NULL
    GROUP BY CAST(CommitteeID AS BIGINT)
),
BillPlenumMapping AS (
    SELECT BillID, PlenumSessionID, SessionNumber, SessionDate::DATE as SessionDate, MatchScore
    FROM (VALUES
        (75200, 11571, 22.0, '2003-05-19T16:00:00', 1.425), (75196, 11179, 21.0, '2003-05-14T11:00:00', 0.917), (75214, 11179, 21.0, '2003-05-14T11:00:00', 0.912), (85983, 11179, 21.0, '2003-05-14T11:00:00', 0.924), (74948, 11179, 21.0, '2003-05-14T11:00:00', 0.906), (74948, 11179, 21.0, '2003-05-14T11:00:00', 0.940), (74940, 11179, 21.0, '2003-05-14T11:00:00', 0.921), (85989, 11179, 21.0, '2003-05-14T11:00:00', 0.902), (89821, 11179, 21.0, '2003-05-14T11:00:00', 0.927), (85979, 11179, 21.0, '2003-05-14T11:00:00', 0.902), (173104, 11179, 21.0, '2003-05-14T11:00:00', 0.885), (85976, 11434, 24.0, '2003-05-21T11:00:00', 0.901), (86098, 11434, 24.0, '2003-05-21T11:00:00', 0.891), (75196, 11434, 24.0, '2003-05-21T11:00:00', 0.906), (75214, 11434, 24.0, '2003-05-21T11:00:00', 0.920), (31711, 11434, 24.0, '2003-05-21T11:00:00', 0.921), (84766, 11434, 24.0, '2003-05-21T11:00:00', 0.901)
    ) AS mapping(BillID, PlenumSessionID, SessionNumber, SessionDate, MatchScore)
),
BillPlenumSessions AS (
    SELECT 
        bpm.BillID,
        COUNT(DISTINCT bpm.PlenumSessionID) as PlenumSessionCount,
        MIN(bpm.SessionDate) as FirstPlenumSession,
        MAX(bpm.SessionDate) as LastPlenumSession,
        AVG(CASE 
            WHEN ps.StartDate IS NOT NULL AND ps.FinishDate IS NOT NULL 
            THEN DATE_DIFF('minute', CAST(ps.StartDate AS TIMESTAMP), CAST(ps.FinishDate AS TIMESTAMP))
        END) as AvgPlenumSessionDurationMinutes,
        GROUP_CONCAT(DISTINCT CAST(bpm.SessionNumber AS VARCHAR) || ': ' || ps.Name, ' | ') as PlenumSessionNames,
        AVG(bpm.MatchScore) as AvgMatchScore
    FROM BillPlenumMapping bpm
    JOIN KNS_PlenumSession ps ON bpm.PlenumSessionID = ps.PlenumSessionID
    GROUP BY bpm.BillID
)
SELECT
    B.BillID,
    B.KnessetNum,
    B.Name AS BillName,
    B.SubTypeID AS BillSubTypeID,
    B.SubTypeDesc AS BillSubTypeDesc,
    B.PrivateNumber,
    B.CommitteeID AS BillCommitteeID,
    COALESCE(C.Name, CASE WHEN B.CommitteeID IS NOT NULL THEN 'Committee ID ' || CAST(B.CommitteeID AS VARCHAR) ELSE NULL END) AS CommitteeName,
    B.StatusID AS BillStatusID,
    S."Desc" AS BillStatusDesc,
    B.Number AS BillNumber,
    B.PostponementReasonID,
    B.PostponementReasonDesc,
    B.SummaryLaw AS BillSummaryLaw,
    strftime(CAST(B.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d') AS LastUpdatedDateFormatted,

    -- Bill initiator information with proper distinction using Ordinal field
    CASE 
        WHEN COUNT(DISTINCT BI.PersonID) > 0 THEN 
            CASE 
                WHEN COUNT(DISTINCT CASE WHEN BI.Ordinal = 1 AND BI.IsInitiator = true THEN BI.PersonID END) > 0 THEN
                    GROUP_CONCAT(DISTINCT CASE WHEN BI.Ordinal = 1 AND BI.IsInitiator = true THEN (Pi.FirstName || ' ' || Pi.LastName) END, ', ')
                ELSE GROUP_CONCAT(DISTINCT CASE WHEN BI.Ordinal = 1 THEN (Pi.FirstName || ' ' || Pi.LastName) END, ', ')
            END
        ELSE 'Government Initiative'
    END AS BillMainInitiatorNames,
    
    -- Main initiator coalition status
    CASE 
        WHEN COUNT(DISTINCT BI.PersonID) > 0 THEN 
            COALESCE(
                MAX(CASE WHEN BI.Ordinal = 1 THEN ufs.CoalitionStatus END),
                'Unknown'
            )
        ELSE 'Government'
    END AS BillMainInitiatorCoalitionStatus,
    
    -- Leading bill information for merged bills (Status ID 122)
    CASE 
        WHEN B.StatusID = 122 THEN 
            CASE 
                WHEN MAX(bmi.LeadingBillName) IS NOT NULL THEN 
                    CONCAT('Bill #', COALESCE(CAST(MAX(bmi.LeadingBillNumber) AS VARCHAR), 'Unknown'), ': ', MAX(bmi.LeadingBillName))
                ELSE 'Merged (relationship data not available in source)'
            END
        ELSE NULL
    END AS MergedWithLeadingBill,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN BI.Ordinal > 1 OR BI.IsInitiator IS NULL THEN BI.PersonID END) > 0 THEN
            GROUP_CONCAT(DISTINCT CASE WHEN BI.Ordinal > 1 OR BI.IsInitiator IS NULL THEN (Pi.FirstName || ' ' || Pi.LastName) END, ', ')
        ELSE 'None'
    END AS BillSupportingMemberNames,
    COUNT(DISTINCT BI.PersonID) AS BillTotalMemberCount,
    COUNT(DISTINCT CASE WHEN BI.Ordinal = 1 THEN BI.PersonID END) AS BillMainInitiatorCount,
    COUNT(DISTINCT CASE WHEN BI.Ordinal > 1 OR BI.IsInitiator IS NULL THEN BI.PersonID END) AS BillSupportingMemberCount,
    
    -- Committee session activity data
    COALESCE(csa.TotalSessions, 0) AS CommitteeTotalSessions,
    strftime(csa.FirstSessionDate, '%Y-%m-%d') as CommitteeFirstSession,
    strftime(csa.LastSessionDate, '%Y-%m-%d') as CommitteeLastSession,
    COALESCE(csa.KnessetSpan, 0) AS CommitteeKnessetSpan,
    COALESCE(csa.SessionsWithDuration, 0) AS CommitteeSessionsWithDuration,
    COALESCE(csa.AvgSessionsPerYear, 0) AS CommitteeAvgSessionsPerYear,
    
    -- Calculate potential processing timeline (days from publication to last committee session)
    CASE 
        WHEN B.PublicationDate IS NOT NULL AND csa.LastSessionDate IS NOT NULL THEN
            DATE_DIFF('day', CAST(B.PublicationDate AS TIMESTAMP), csa.LastSessionDate)
        ELSE NULL
    END as DaysFromPublicationToLastCommitteeSession,
    
    -- Committee activity level classification
    CASE 
        WHEN csa.TotalSessions IS NOT NULL THEN
            CASE 
                WHEN csa.TotalSessions > 100 THEN 'Very Active'
                WHEN csa.TotalSessions > 50 THEN 'Active'  
                WHEN csa.TotalSessions > 20 THEN 'Moderate'
                ELSE 'Limited'
            END
        ELSE 'No Committee'
    END as CommitteeActivityLevel,
    
    -- Plenum session data (from KNS_PlmSessionItem text analysis)
    COALESCE(bps.PlenumSessionCount, 0) AS BillPlenumSessionCount,
    strftime(bps.FirstPlenumSession, '%Y-%m-%d') as BillFirstPlenumSession,
    strftime(bps.LastPlenumSession, '%Y-%m-%d') as BillLastPlenumSession,
    ROUND(bps.AvgPlenumSessionDurationMinutes, 1) AS BillAvgPlenumSessionDurationMinutes,
    CASE 
        WHEN LENGTH(bps.PlenumSessionNames) > 200 THEN 
            SUBSTRING(bps.PlenumSessionNames, 1, 197) || '...'
        ELSE bps.PlenumSessionNames
    END AS BillPlenumSessionNames,
    ROUND(bps.AvgMatchScore, 3) AS BillPlenumMatchScore

FROM KNS_Bill B
LEFT JOIN KNS_Committee C ON CAST(B.CommitteeID AS BIGINT) = C.CommitteeID
LEFT JOIN KNS_Status S ON B.StatusID = S.StatusID
LEFT JOIN KNS_BillInitiator BI ON B.BillID = BI.BillID
LEFT JOIN KNS_Person Pi ON BI.PersonID = Pi.PersonID
LEFT JOIN BillMainInitiatorFaction bmif ON B.BillID = bmif.BillID AND bmif.rn = 1
LEFT JOIN KNS_Faction f ON bmif.FactionID = f.FactionID
LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID 
    AND B.KnessetNum = ufs.KnessetNum
LEFT JOIN BillMergeInfo bmi ON B.BillID = bmi.MergedBillID
LEFT JOIN CommitteeSessionActivity csa ON CAST(B.CommitteeID AS BIGINT) = csa.CommitteeID
LEFT JOIN BillPlenumSessions bps ON B.BillID = bps.BillID

GROUP BY 
    B.BillID, B.KnessetNum, B.Name, B.SubTypeID, B.SubTypeDesc, B.PrivateNumber,
    B.CommitteeID, C.Name, B.StatusID, S."Desc", B.Number, B.PostponementReasonID,
    B.PostponementReasonDesc, B.SummaryLaw, B.LastUpdatedDate, B.PublicationDate,
    csa.TotalSessions, csa.FirstSessionDate, csa.LastSessionDate, csa.KnessetSpan,
    csa.SessionsWithDuration, csa.AvgSessionsPerYear,
    bps.PlenumSessionCount, bps.FirstPlenumSession, bps.LastPlenumSession, 
    bps.AvgPlenumSessionDurationMinutes, bps.PlenumSessionNames, bps.AvgMatchScore

ORDER BY B.KnessetNum DESC, B.BillID DESC;
        """,
        "knesset_filter_column": "B.KnessetNum",
        "faction_filter_column": "NULL", # Bills don't have direct faction association
        "description": (
            "Comprehensive bill data with initiator information, committee assignments, "
            "status details, committee session activity analysis, and plenum session information "
            "extracted from document file paths"
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
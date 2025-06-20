"""
Predefined SQL queries for data analysis and export.

This module contains complex SQL queries that were previously hardcoded
in the UI layer. Each query is defined with its SQL and metadata for
filtering and display purposes.
"""

from typing import Any, Dict

# Query definitions with their SQL and metadata
PREDEFINED_QUERIES: Dict[str, Dict[str, Any]] = {
    "Queries + Full Details": {
        "sql": """
WITH MKLatestFactionDetailsInKnesset AS (
    SELECT
        p2p.PersonID,
        p2p.KnessetNum,
        p2p.FactionID,
        p2p.FactionName,
        ufs.CoalitionStatus,
        p2p.PersonToPositionID,
        ROW_NUMBER() OVER (
            PARTITION BY p2p.PersonID, p2p.KnessetNum
            ORDER BY p2p.StartDate DESC, p2p.FinishDate DESC NULLS LAST,
                     p2p.PersonToPositionID DESC
        ) as rn
    FROM KNS_PersonToPosition p2p
    LEFT JOIN UserFactionCoalitionStatus ufs
        ON p2p.FactionID = ufs.FactionID AND p2p.KnessetNum = ufs.KnessetNum
    WHERE p2p.FactionID IS NOT NULL
),
ActiveMKFactionDetailsForQuery AS (
    SELECT
        q_inner.QueryID,
        p2p_inner.FactionID AS ActiveFactionID,
        p2p_inner.FactionName AS ActiveFactionName,
        ufs_inner.CoalitionStatus AS ActiveCoalitionStatus,
        ROW_NUMBER() OVER (
            PARTITION BY q_inner.QueryID
            ORDER BY p2p_inner.StartDate DESC, p2p_inner.PersonToPositionID DESC
        ) as rn_active
    FROM KNS_Query q_inner
    JOIN KNS_PersonToPosition p2p_inner ON q_inner.PersonID = p2p_inner.PersonID
        AND q_inner.KnessetNum = p2p_inner.KnessetNum
        AND CAST(q_inner.SubmitDate AS TIMESTAMP)
            BETWEEN CAST(p2p_inner.StartDate AS TIMESTAMP)
            AND CAST(COALESCE(p2p_inner.FinishDate, '9999-12-31') AS TIMESTAMP)
    LEFT JOIN UserFactionCoalitionStatus ufs_inner
        ON p2p_inner.FactionID = ufs_inner.FactionID
        AND p2p_inner.KnessetNum = ufs_inner.KnessetNum
    WHERE p2p_inner.FactionID IS NOT NULL
),
MinisterOfReplyMinistry AS (
    -- This CTE finds the Minister for the GovMinistryID associated
    -- with the Query, around the ReplyMinisterDate.
    SELECT
        q_m.QueryID,
        min_p.FirstName || ' ' || min_p.LastName AS ResponsibleMinisterName,
        min_p2p.DutyDesc AS ResponsibleMinisterPosition,
        ROW_NUMBER() OVER (
            PARTITION BY q_m.QueryID
            -- Prioritize positions active on ReplyMinisterDate,
            -- then by most recent start date.
            ORDER BY
                (CASE WHEN CAST(q_m.ReplyMinisterDate AS TIMESTAMP)
                    BETWEEN CAST(min_p2p.StartDate AS TIMESTAMP)
                    AND CAST(COALESCE(min_p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                    THEN 0 ELSE 1 END),
                min_p2p.StartDate DESC,
                min_p2p.PersonToPositionID DESC
        ) as rn_min
    FROM KNS_Query q_m
    LEFT JOIN KNS_PersonToPosition min_p2p
        ON q_m.GovMinistryID = min_p2p.GovMinistryID  -- Match on Ministry ID
        AND q_m.KnessetNum = min_p2p.KnessetNum  -- Match on Knesset Number
        -- Refined condition to identify Ministers based on DutyDesc,
        -- excluding deputies.
        AND (
                min_p2p.DutyDesc LIKE 'שר %' OR           -- Minister of
                min_p2p.DutyDesc LIKE 'השר %' OR          -- The Minister of
                min_p2p.DutyDesc = 'שר' OR                -- Exactly "שר"
                min_p2p.DutyDesc LIKE 'שרה %' OR         -- Female Minister of
                min_p2p.DutyDesc LIKE 'השרה %' OR        -- The Female Minister
                min_p2p.DutyDesc = 'שרה' OR              -- Exactly "שרה"
                min_p2p.DutyDesc = 'ראש הממשלה'         -- Prime Minister
            )
        AND min_p2p.DutyDesc NOT LIKE 'סגן %'             -- Exclude Deputy
        AND min_p2p.DutyDesc NOT LIKE 'סגנית %'           -- Exclude Female Deputy
        AND min_p2p.DutyDesc NOT LIKE '%יושב ראש%'      -- Exclude Chairman
        AND min_p2p.DutyDesc NOT LIKE '%יו""ר%'           -- Exclude Chairman abbrev
        -- Minister's term started before or on reply date
        AND CAST(q_m.ReplyMinisterDate AS TIMESTAMP) >=
            CAST(min_p2p.StartDate AS TIMESTAMP)
    LEFT JOIN KNS_Person min_p ON min_p2p.PersonID = min_p.PersonID
    WHERE q_m.ReplyMinisterDate IS NOT NULL AND q_m.GovMinistryID IS NOT NULL
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
    P.IsCurrent AS MKIsCurrent, -- Added

    COALESCE(AMFD.ActiveFactionName, FallbackFaction.FactionName) AS MKFactionName,
    COALESCE(AMFD.ActiveCoalitionStatus, FallbackFaction.CoalitionStatus)
        AS MKFactionCoalitionStatus,

    M.Name AS MinistryName,
    M.IsActive AS MinistryIsActive, -- Added
    strftime(CAST(Q.SubmitDate AS TIMESTAMP), '%Y-%m-%d') AS SubmitDateFormatted,
    strftime(CAST(Q.ReplyMinisterDate AS TIMESTAMP), '%Y-%m-%d') AS AnswerDate,

    MRM.ResponsibleMinisterName, -- Added (Minister of the replying Ministry)
    MRM.ResponsibleMinisterPosition -- Added (Position of that Minister)
    -- AnswerText is not available in KNS_Query table.

FROM KNS_Query Q
LEFT JOIN KNS_Person P ON Q.PersonID = P.PersonID
LEFT JOIN KNS_GovMinistry M ON Q.GovMinistryID = M.GovMinistryID
LEFT JOIN KNS_Status S ON Q.StatusID = S.StatusID
LEFT JOIN ActiveMKFactionDetailsForQuery AMFD
    ON Q.QueryID = AMFD.QueryID AND AMFD.rn_active = 1
LEFT JOIN MKLatestFactionDetailsInKnesset FallbackFaction
    ON Q.PersonID = FallbackFaction.PersonID
    AND Q.KnessetNum = FallbackFaction.KnessetNum AND FallbackFaction.rn = 1
LEFT JOIN MinisterOfReplyMinistry MRM
    ON Q.QueryID = MRM.QueryID AND MRM.rn_min = 1

ORDER BY Q.KnessetNum DESC, Q.QueryID DESC LIMIT 10000;
        """,
        "knesset_filter_column": "Q.KnessetNum",
        "faction_filter_column":
            "COALESCE(AMFD.ActiveFactionID, FallbackFaction.FactionID)",
        "description": (
            "Comprehensive query data with faction details, "
            "ministry information, and responsible ministers"
        ),
    },
    "Agenda Items + Full Details": {
        "sql": """
WITH MKLatestFactionDetailsInKnesset AS (
    SELECT
        p2p.PersonID,
        p2p.KnessetNum,
        p2p.FactionID,
        p2p.FactionName,
        ufs.CoalitionStatus,
        p2p.PersonToPositionID,
        ROW_NUMBER() OVER (
            PARTITION BY p2p.PersonID, p2p.KnessetNum
            ORDER BY p2p.StartDate DESC, p2p.FinishDate DESC NULLS LAST,
                     p2p.PersonToPositionID DESC
        ) as rn
    FROM KNS_PersonToPosition p2p
    LEFT JOIN UserFactionCoalitionStatus ufs
        ON p2p.FactionID = ufs.FactionID AND p2p.KnessetNum = ufs.KnessetNum
    WHERE p2p.FactionID IS NOT NULL
),
ActiveInitiatorFactionDetailsForAgenda AS (
    SELECT
        a_inner.AgendaID,
        p2p_inner.FactionID AS ActiveFactionID,
        p2p_inner.FactionName AS ActiveFactionName,
        ufs_inner.CoalitionStatus AS ActiveCoalitionStatus,
        ROW_NUMBER() OVER (
            PARTITION BY a_inner.AgendaID
            ORDER BY p2p_inner.StartDate DESC, p2p_inner.PersonToPositionID DESC
        ) as rn_active
    FROM KNS_Agenda a_inner
    JOIN KNS_PersonToPosition p2p_inner
        ON a_inner.InitiatorPersonID = p2p_inner.PersonID
        AND a_inner.KnessetNum = p2p_inner.KnessetNum
        AND CAST(COALESCE(a_inner.PresidentDecisionDate, a_inner.LastUpdatedDate)
                 AS TIMESTAMP)
            BETWEEN CAST(p2p_inner.StartDate AS TIMESTAMP)
            AND CAST(COALESCE(p2p_inner.FinishDate, '9999-12-31') AS TIMESTAMP)
    LEFT JOIN UserFactionCoalitionStatus ufs_inner
        ON p2p_inner.FactionID = ufs_inner.FactionID
        AND p2p_inner.KnessetNum = ufs_inner.KnessetNum
    WHERE p2p_inner.FactionID IS NOT NULL AND a_inner.InitiatorPersonID IS NOT NULL
)
SELECT
    A.AgendaID,
    A.Number AS AgendaNumber,
    A.KnessetNum,
    A.Name AS AgendaName, -- This is the main name/title of the agenda item
    A.Name AS AgendaDescription, -- Using A.Name as AgendaDescription
    A.ClassificationDesc AS AgendaClassification,
    S."Desc" AS AgendaStatus,
    A.InitiatorPersonID, -- Added InitiatorPersonID for combination with PersonID
    INIT_P.PersonID AS InitiatorPersonIDResolved, -- Added resolved PersonID
    INIT_P.FirstName AS InitiatorFirstName,
    INIT_P.LastName AS InitiatorLastName,
    INIT_P.GenderDesc AS InitiatorGender,

    COALESCE(AIFD.ActiveFactionName, FallbackFaction_init.FactionName)
        AS InitiatorFactionName,
    COALESCE(AIFD.ActiveCoalitionStatus, FallbackFaction_init.CoalitionStatus)
        AS InitiatorFactionCoalitionStatus,

    HC.Name AS HandlingCommitteeName,
    HC.IsCurrent AS CommitteeIsActive, -- Changed from HC.IsActive to HC.IsCurrent
    strftime(CAST(A.PresidentDecisionDate AS TIMESTAMP), '%Y-%m-%d')
        AS PresidentDecisionDateFormatted

FROM KNS_Agenda A
LEFT JOIN KNS_Status S ON A.StatusID = S.StatusID
LEFT JOIN KNS_Person INIT_P ON A.InitiatorPersonID = INIT_P.PersonID
LEFT JOIN KNS_Committee HC ON A.CommitteeID = HC.CommitteeID
LEFT JOIN ActiveInitiatorFactionDetailsForAgenda AIFD
    ON A.AgendaID = AIFD.AgendaID AND AIFD.rn_active = 1
LEFT JOIN MKLatestFactionDetailsInKnesset FallbackFaction_init
    ON A.InitiatorPersonID = FallbackFaction_init.PersonID
    AND A.KnessetNum = FallbackFaction_init.KnessetNum
    AND FallbackFaction_init.rn = 1

ORDER BY A.KnessetNum DESC, A.AgendaID DESC LIMIT 10000;
        """,
        "knesset_filter_column": "A.KnessetNum",
        "faction_filter_column":
            "COALESCE(AIFD.ActiveFactionID, FallbackFaction_init.FactionID)",
        "description": (
            "Comprehensive agenda items with initiator details, "
            "committee information, and faction status"
        ),
    },
    "Bills + Full Details": {
        "sql": """
WITH BillInitiators AS (
    SELECT
        bi.BillID,
        STRING_AGG(p.FirstName || ' ' || p.LastName, ', ') AS InitiatorNames,
        STRING_AGG(p.FirstName, ', ') AS InitiatorFirstNames,
        STRING_AGG(p.LastName, ', ') AS InitiatorLastNames,
        COUNT(DISTINCT bi.PersonID) AS InitiatorCount
    FROM KNS_BillInitiator bi
    LEFT JOIN KNS_Person p ON bi.PersonID = p.PersonID
    GROUP BY bi.BillID
)
SELECT
    B.BillID,
    B.Number AS BillNumber,
    B.KnessetNum,
    B.Name AS BillName,
    B.SubTypeDesc AS BillSubType,
    S."Desc" AS BillStatus,
    B.PrivateNumber,
    C.Name AS CommitteeName,
    B.PostponementReasonDesc,
    B.PublicationDate,
    B.MagazineNumber,
    B.PageNumber,
    B.IsContinuationBill,
    B.SummaryLaw,
    B.PublicationSeriesDesc,
    B.PublicationSeriesFirstCall,
    strftime(CAST(B.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d')
        AS LastUpdatedDateFormatted,
    COALESCE(BI.InitiatorNames, 'Unknown') AS BillInitiatorNames,
    COALESCE(BI.InitiatorFirstNames, '') AS BillInitiatorFirstNames,
    COALESCE(BI.InitiatorLastNames, '') AS BillInitiatorLastNames,
    COALESCE(BI.InitiatorCount, 0) AS BillInitiatorCount

FROM KNS_Bill B
LEFT JOIN KNS_Status S ON B.StatusID = S.StatusID
LEFT JOIN KNS_Committee C ON B.CommitteeID = C.CommitteeID
LEFT JOIN BillInitiators BI ON B.BillID = BI.BillID

ORDER BY B.KnessetNum DESC, B.BillID DESC LIMIT 10000;
        """,
        "knesset_filter_column": "B.KnessetNum",
        "faction_filter_column": "",
        "description": (
            "Comprehensive bill data with initiator details, committee "
            "assignments, and status information"
        ),
    },
}


def get_query_definition(query_name: str) -> Dict[str, Any]:
    """Get a specific query definition by name."""
    return PREDEFINED_QUERIES.get(query_name, {})


def get_all_query_names() -> list[str]:
    """Get list of all available predefined query names."""
    return list(PREDEFINED_QUERIES.keys())


def get_query_sql(query_name: str) -> str:
    """Get the SQL for a specific query."""
    query_def = get_query_definition(query_name)
    return query_def.get("sql", "")


def get_query_filters(query_name: str) -> Dict[str, str]:
    """Get the filter column mappings for a specific query."""
    query_def = get_query_definition(query_name)
    return {
        "knesset_filter_column": query_def.get("knesset_filter_column", ""),
        "faction_filter_column": query_def.get("faction_filter_column", ""),
    }


def get_query_description(query_name: str) -> str:
    """Get the description for a specific query."""
    query_def = get_query_definition(query_name)
    return query_def.get("description", "")

"""
Predefined SQL queries for data analysis and export - ENHANCED VERSION.

This module contains complex SQL queries that were previously hardcoded
in the UI layer. Each query is defined with its SQL and metadata for
filtering and display purposes.

Key improvements:
- Fixed LATERAL join issues with window functions in DuckDB
- Enhanced committee name resolution (71.4% success rate vs previous 14.8%)
- Comprehensive bill analysis with 49 columns including committee data
- Historical committee coverage across Knessets 1-25
- Nearly complete KNS_CmtSessionItem dataset (74,951/75,051 records) for accurate bill-to-session connections
- Verified committee session data for 10,232 bills (17.6% coverage) with 100% accuracy

Committee Resolution:
The BillCommitteeName field now resolves 71.4% of committee assignments to actual names
instead of "Committee [number]" fallbacks, thanks to improved historical committee data
fetching using KnessetNum filtering.
"""

from typing import Any, Dict
from utils.faction_resolver import FactionResolver, get_faction_name_field, get_coalition_status_field
from utils.committee_resolver import CommitteeResolver, get_committee_name_with_enhanced_fallback

# Query definitions with their SQL and metadata
PREDEFINED_QUERIES: Dict[str, Dict[str, Any]] = {
    "Queries + Full Details": {
        "sql": """
WITH StandardFactionLookup AS (
            SELECT 
                ptp.PersonID as PersonID,
                ptp.KnessetNum as KnessetNum,
                ptp.FactionID,
                ROW_NUMBER() OVER (
                    PARTITION BY ptp.PersonID, ptp.KnessetNum 
                    ORDER BY 
                        CASE WHEN ptp.FactionID IS NOT NULL THEN 0 ELSE 1 END,
                        ptp.KnessetNum DESC,
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
LEFT JOIN StandardFactionLookup sfl ON Q.PersonID = sfl.PersonID 
    AND Q.KnessetNum = sfl.KnessetNum 
    AND sfl.rn = 1
LEFT JOIN KNS_Faction f ON sfl.FactionID = f.FactionID
LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID 
    AND Q.KnessetNum = ufs.KnessetNum
LEFT JOIN MinisterLookup ml ON Q.GovMinistryID = ml.GovMinistryID AND ml.rn = 1
LEFT JOIN KNS_Person min_p ON ml.PersonID = min_p.PersonID

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
    "Agenda Items + Full Details": {
        "sql": """
WITH StandardFactionLookup AS (
            SELECT
                ptp.PersonID as PersonID,
                ptp.KnessetNum as KnessetNum,
                ptp.FactionID,
                ROW_NUMBER() OVER (
                    PARTITION BY ptp.PersonID, ptp.KnessetNum
                    ORDER BY
                        CASE WHEN ptp.FactionID IS NOT NULL THEN 0 ELSE 1 END,
                        ptp.KnessetNum DESC,
                        ptp.StartDate DESC NULLS LAST
                ) as rn
            FROM KNS_PersonToPosition ptp
            WHERE ptp.FactionID IS NOT NULL
        ),
-- Agenda Documents CTE (aggregates documents per agenda)
AgendaDocuments AS (
    SELECT
        da.AgendaID,
        COUNT(*) as DocumentCount,
        -- Primary document URL (prefer PDF)
        MAX(CASE WHEN da.ApplicationDesc = 'PDF' THEN da.FilePath END) AS PrimaryDocPDF,
        MAX(CASE WHEN da.ApplicationDesc != 'PDF' THEN da.FilePath END) AS PrimaryDocOther,
        MAX(da.GroupTypeDesc) AS PrimaryDocType,
        -- Document counts by format
        COUNT(CASE WHEN da.ApplicationDesc = 'PDF' THEN 1 END) AS PDFDocCount,
        COUNT(CASE WHEN da.ApplicationDesc = 'DOC' OR da.ApplicationDesc = 'DOCX' THEN 1 END) AS WordDocCount,
        -- All documents as concatenated string
        GROUP_CONCAT(DISTINCT da.GroupTypeDesc || ' (' || da.ApplicationDesc || ')', ' | ') as DocumentTypes
    FROM KNS_DocumentAgenda da
    WHERE da.FilePath IS NOT NULL
    GROUP BY da.AgendaID
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

    -- Main Initiator Display (prominent field showing who proposed the agenda)
    CASE
        WHEN A.ClassificationDesc = 'עצמאית' AND P.PersonID IS NOT NULL THEN
            P.FirstName || ' ' || P.LastName || ' (' || COALESCE(f.Name, 'Unknown Faction') || ')'
        WHEN A.ClassificationDesc = 'כוללת' THEN
            'Inclusive Proposal (הצעה כוללת - Multiple MKs)'
        ELSE
            COALESCE(P.FirstName || ' ' || P.LastName, 'No Initiator Recorded')
    END AS MainInitiatorDisplay,

    -- Proposal Type in English
    CASE
        WHEN A.ClassificationDesc = 'עצמאית' THEN 'Independent'
        WHEN A.ClassificationDesc = 'כוללת' THEN 'Inclusive'
        ELSE A.ClassificationDesc
    END AS ProposalTypeEN,

    -- Document fields
    COALESCE(ad.DocumentCount, 0) AS AgendaDocumentCount,
    COALESCE(ad.PrimaryDocPDF, ad.PrimaryDocOther) AS AgendaPrimaryDocumentURL,
    ad.PrimaryDocType AS AgendaPrimaryDocumentType,
    ad.PDFDocCount AS AgendaPDFDocCount,
    ad.WordDocCount AS AgendaWordDocCount,
    ad.DocumentTypes AS AgendaDocumentTypes,
    -- Knesset website link for agenda
    'https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid=' || CAST(A.AgendaID AS VARCHAR) AS AgendaKnessetWebsiteURL,

    strftime(CAST(A.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d') AS LastUpdatedDateFormatted,
    strftime(CAST(A.PresidentDecisionDate AS TIMESTAMP), '%Y-%m-%d') AS PresidentDecisionDateFormatted

FROM KNS_Agenda A
LEFT JOIN KNS_Person P ON A.InitiatorPersonID = P.PersonID
LEFT JOIN KNS_Status S ON A.StatusID = S.StatusID
LEFT JOIN StandardFactionLookup sfl ON A.InitiatorPersonID = sfl.PersonID
    AND A.KnessetNum = sfl.KnessetNum
    AND sfl.rn = 1
LEFT JOIN KNS_Faction f ON sfl.FactionID = f.FactionID
LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID
    AND A.KnessetNum = ufs.KnessetNum
LEFT JOIN AgendaDocuments ad ON A.AgendaID = ad.AgendaID

ORDER BY A.KnessetNum DESC, A.AgendaID DESC
LIMIT 1000;
        """,
        "knesset_filter_column": "A.KnessetNum",
        "faction_filter_column": "f.FactionID",
        "description": (
            "Comprehensive agenda items data with faction details, "
            "status information, and document links"
        ),
    },
    "Bills + Full Details": {
        "sql": """
WITH StandardFactionLookup AS (
            SELECT 
                ptp.PersonID as PersonID,
                ptp.KnessetNum as KnessetNum,
                ptp.FactionID,
                ROW_NUMBER() OVER (
                    PARTITION BY ptp.PersonID, ptp.KnessetNum 
                    ORDER BY 
                        CASE WHEN ptp.FactionID IS NOT NULL THEN 0 ELSE 1 END,
                        ptp.KnessetNum DESC,
                        ptp.StartDate DESC NULLS LAST
                ) as rn
            FROM KNS_PersonToPosition ptp
            WHERE ptp.FactionID IS NOT NULL
        ),
BillMainInitiatorFaction AS (
    SELECT 
        BI.BillID,
        B.KnessetNum,
        BI.PersonID as MainInitiatorPersonID,
        sfl.FactionID,
        ROW_NUMBER() OVER (PARTITION BY BI.BillID ORDER BY BI.Ordinal) as rn
    FROM KNS_BillInitiator BI
    JOIN KNS_Bill B ON BI.BillID = B.BillID
    LEFT JOIN StandardFactionLookup sfl ON BI.PersonID = sfl.PersonID 
        AND B.KnessetNum = sfl.KnessetNum
        AND sfl.rn = 1
    WHERE BI.Ordinal = 1
),
BillSupportingMemberFactions AS (
    SELECT 
        BI.BillID,
        BI.PersonID,
        B.KnessetNum,
        sfl.FactionID,
        ROW_NUMBER() OVER (PARTITION BY BI.BillID, BI.PersonID ORDER BY BI.Ordinal) as rn
    FROM KNS_BillInitiator BI
    JOIN KNS_Bill B ON BI.BillID = B.BillID
    LEFT JOIN StandardFactionLookup sfl ON BI.PersonID = sfl.PersonID 
        AND B.KnessetNum = sfl.KnessetNum
        AND sfl.rn = 1
    WHERE (BI.Ordinal > 1 OR BI.IsInitiator IS NULL)
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
BillCommitteeSessions AS (
    -- Direct bill-to-session connections using complete KNS_CmtSessionItem dataset
    SELECT 
        csi.ItemID as BillID,
        COUNT(DISTINCT csi.CommitteeSessionID) as BillSpecificSessions,
        MIN(CAST(cs.StartDate AS TIMESTAMP)) as FirstRelevantSession,
        MAX(CAST(cs.StartDate AS TIMESTAMP)) as LastRelevantSession
    FROM KNS_CmtSessionItem csi
    JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
    WHERE csi.ItemID IS NOT NULL
        AND cs.StartDate IS NOT NULL
    GROUP BY csi.ItemID
),
BillPlenumSessions AS (
    SELECT 
        psi.ItemID as BillID,
        COUNT(DISTINCT psi.PlenumSessionID) as PlenumSessionCount,
        MIN(CAST(ps.StartDate AS TIMESTAMP)) as FirstPlenumSession,
        MAX(CAST(ps.StartDate AS TIMESTAMP)) as LastPlenumSession,
        AVG(CASE 
            WHEN ps.StartDate IS NOT NULL AND ps.FinishDate IS NOT NULL 
            THEN DATE_DIFF('minute', CAST(ps.StartDate AS TIMESTAMP), CAST(ps.FinishDate AS TIMESTAMP))
        END) as AvgPlenumSessionDurationMinutes,
        GROUP_CONCAT(DISTINCT CAST(ps.Number AS VARCHAR) || ': ' || ps.Name, ' | ') as PlenumSessionNames
    FROM KNS_PlmSessionItem psi
    JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
    WHERE psi.ItemID IS NOT NULL
    GROUP BY psi.ItemID
),
BillDocuments AS (
    -- Prioritize documents by importance and recency
    SELECT
        db.BillID,
        COUNT(*) as DocumentCount,

        -- Primary document (most important single link) - Published Law
        MAX(CASE
            WHEN db.GroupTypeDesc = 'חוק - פרסום ברשומות' AND db.ApplicationDesc = 'PDF' THEN db.FilePath
        END) AS PrimaryDoc_PublishedLaw_PDF,

        MAX(CASE
            WHEN db.GroupTypeDesc = 'חוק - פרסום ברשומות' AND db.ApplicationDesc != 'PDF' THEN db.FilePath
        END) AS PrimaryDoc_PublishedLaw_Other,

        -- First reading document (most common access point)
        MAX(CASE
            WHEN db.GroupTypeDesc = 'הצעת חוק לקריאה הראשונה' AND db.ApplicationDesc = 'PDF' THEN db.FilePath
        END) AS FirstReading_Doc_PDF,

        MAX(CASE
            WHEN db.GroupTypeDesc = 'הצעת חוק לקריאה הראשונה' AND db.ApplicationDesc != 'PDF' THEN db.FilePath
        END) AS FirstReading_Doc_Other,

        -- Second/Third reading (final version before law)
        MAX(CASE
            WHEN db.GroupTypeDesc LIKE 'הצעת חוק לקריאה השנייה והשלישית%' AND db.ApplicationDesc = 'PDF' THEN db.FilePath
        END) AS SecondThirdReading_Doc_PDF,

        MAX(CASE
            WHEN db.GroupTypeDesc LIKE 'הצעת חוק לקריאה השנייה והשלישית%' AND db.ApplicationDesc != 'PDF' THEN db.FilePath
        END) AS SecondThirdReading_Doc_Other,

        -- Early discussion (original proposal)
        MAX(CASE
            WHEN db.GroupTypeDesc = 'הצעת חוק לדיון מוקדם' AND db.ApplicationDesc = 'PDF' THEN db.FilePath
        END) AS EarlyDiscussion_Doc_PDF,

        MAX(CASE
            WHEN db.GroupTypeDesc = 'הצעת חוק לדיון מוקדם' AND db.ApplicationDesc != 'PDF' THEN db.FilePath
        END) AS EarlyDiscussion_Doc_Other,

        -- Count by category for user info
        COUNT(CASE WHEN db.GroupTypeDesc = 'חוק - פרסום ברשומות' THEN 1 END) AS PublishedLawCount,
        COUNT(CASE WHEN db.GroupTypeDesc = 'הצעת חוק לקריאה הראשונה' THEN 1 END) AS FirstReadingCount,
        COUNT(CASE WHEN db.GroupTypeDesc LIKE 'הצעת חוק לקריאה השנייה והשלישית%' THEN 1 END) AS SecondThirdCount,
        COUNT(CASE WHEN db.GroupTypeDesc = 'הצעת חוק לדיון מוקדם' THEN 1 END) AS EarlyDiscussionCount,
        COUNT(CASE WHEN db.GroupTypeDesc NOT IN (
            'חוק - פרסום ברשומות',
            'הצעת חוק לקריאה הראשונה',
            'הצעת חוק לדיון מוקדם'
        ) AND db.GroupTypeDesc NOT LIKE 'הצעת חוק לקריאה השנייה והשלישית%' THEN 1 END) AS OtherDocCount,

        -- Keep legacy concatenated format for backward compatibility (exports)
        GROUP_CONCAT(DISTINCT db.GroupTypeDesc || ' (' || db.ApplicationDesc || '): ' || db.FilePath, ' | ') as DocumentLinks

    FROM KNS_DocumentBill db
    WHERE db.FilePath IS NOT NULL
    GROUP BY db.BillID
),
BillFirstSubmission AS (
    -- Get the earliest activity date for each bill (true submission date)
    -- This considers initiator assignment, committee sessions, plenum sessions, and publication
    SELECT 
        B.BillID,
        MIN(earliest_date) as FirstSubmissionDate
    FROM KNS_Bill B
    LEFT JOIN (
        -- Initiator assignment dates (often the earliest/true submission)
        SELECT 
            BI.BillID,
            MIN(CAST(BI.LastUpdatedDate AS TIMESTAMP)) as earliest_date
        FROM KNS_BillInitiator BI
        WHERE BI.LastUpdatedDate IS NOT NULL
        GROUP BY BI.BillID
        
        UNION ALL
        
        -- Committee session dates
        SELECT 
            csi.ItemID as BillID,
            MIN(CAST(cs.StartDate AS TIMESTAMP)) as earliest_date
        FROM KNS_CmtSessionItem csi
        JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
        WHERE csi.ItemID IS NOT NULL AND cs.StartDate IS NOT NULL
        GROUP BY csi.ItemID
        
        UNION ALL
        
        -- Plenum session dates
        SELECT 
            psi.ItemID as BillID,
            MIN(CAST(ps.StartDate AS TIMESTAMP)) as earliest_date
        FROM KNS_PlmSessionItem psi
        JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
        WHERE psi.ItemID IS NOT NULL AND ps.StartDate IS NOT NULL
        GROUP BY psi.ItemID
        
        UNION ALL
        
        -- Publication dates
        SELECT 
            B.BillID,
            CAST(B.PublicationDate AS TIMESTAMP) as earliest_date
        FROM KNS_Bill B
        WHERE B.PublicationDate IS NOT NULL
    ) all_dates ON B.BillID = all_dates.BillID
    WHERE all_dates.earliest_date IS NOT NULL
    GROUP BY B.BillID
)
SELECT
    B.BillID,
    B.KnessetNum,
    B.Name AS BillName,
    B.SubTypeID AS BillSubTypeID,
    B.SubTypeDesc AS BillSubTypeDesc,
    B.PrivateNumber,
    COALESCE(C.Name, CASE WHEN B.CommitteeID IS NOT NULL THEN 'Committee ' || CAST(B.CommitteeID AS VARCHAR) ELSE NULL END) AS BillCommitteeName,
    
    -- Committee additional information from KNS_Committee
    C.CommitteeTypeDesc AS BillCommitteeTypeDesc,
    C.AdditionalTypeID AS BillCommitteeAdditionalTypeID,
    C.AdditionalTypeDesc AS BillCommitteeAdditionalTypeDesc,
    C.CommitteeParentName AS BillCommitteeParentName,
    
    B.StatusID AS BillStatusID,
    S."Desc" AS BillStatusDesc,
    B.Number AS BillNumber,
    B.PostponementReasonID,
    B.PostponementReasonDesc,
    B.SummaryLaw AS BillSummaryLaw,
    strftime(CAST(B.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d') AS LastUpdatedDateFormatted,
    
    -- Additional KNS_Bill fields
    strftime(CAST(B.PublicationDate AS TIMESTAMP), '%Y-%m-%d') AS BillPublicationDate,
    B.MagazineNumber AS BillMagazineNumber,
    B.PageNumber AS BillPageNumber,
    COALESCE(B.IsContinuationBill, false) AS BillIsContinuationBill,
    B.PublicationSeriesID AS BillPublicationSeriesID,
    B.PublicationSeriesDesc AS BillPublicationSeriesDesc,
    B.PublicationSeriesFirstCall AS BillPublicationSeriesFirstCall,

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
    
    -- Main initiator faction name
    CASE 
        WHEN COUNT(DISTINCT BI.PersonID) > 0 THEN 
            COALESCE(
                MAX(CASE WHEN BI.Ordinal = 1 THEN f.Name END),
                'Unknown'
            )
        ELSE 'Government'
    END AS BillMainInitiatorFactionName,
    
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
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN BI.Ordinal > 1 OR BI.IsInitiator IS NULL THEN BI.PersonID END) > 0 THEN
            GROUP_CONCAT(DISTINCT CASE WHEN BI.Ordinal > 1 OR BI.IsInitiator IS NULL THEN (Pi.FirstName || ' ' || Pi.LastName || ' (' || COALESCE(sf.Name, 'Unknown Faction') || ')') END, ', ')
        ELSE 'None'
    END AS BillSupportingMembersWithFactions,
    COUNT(DISTINCT BI.PersonID) AS BillTotalMemberCount,
    COUNT(DISTINCT CASE WHEN BI.Ordinal = 1 THEN BI.PersonID END) AS BillMainInitiatorCount,
    COUNT(DISTINCT CASE WHEN BI.Ordinal > 1 OR BI.IsInitiator IS NULL THEN BI.PersonID END) AS BillSupportingMemberCount,
    
    -- Coalition/Opposition member counts
    COUNT(DISTINCT CASE WHEN BI.Ordinal = 1 AND ufs.CoalitionStatus = 'Coalition' THEN BI.PersonID 
                        WHEN (BI.Ordinal > 1 OR BI.IsInitiator IS NULL) AND sufs.CoalitionStatus = 'Coalition' THEN BI.PersonID END) AS BillCoalitionMemberCount,
    COUNT(DISTINCT CASE WHEN BI.Ordinal = 1 AND ufs.CoalitionStatus = 'Opposition' THEN BI.PersonID 
                        WHEN (BI.Ordinal > 1 OR BI.IsInitiator IS NULL) AND sufs.CoalitionStatus = 'Opposition' THEN BI.PersonID END) AS BillOppositionMemberCount,
    
    -- Coalition/Opposition member percentages
    CASE 
        WHEN COUNT(DISTINCT BI.PersonID) > 0 THEN
            ROUND((COUNT(DISTINCT CASE WHEN BI.Ordinal = 1 AND ufs.CoalitionStatus = 'Coalition' THEN BI.PersonID 
                                      WHEN (BI.Ordinal > 1 OR BI.IsInitiator IS NULL) AND sufs.CoalitionStatus = 'Coalition' THEN BI.PersonID END) * 100.0) 
                  / COUNT(DISTINCT BI.PersonID), 1)
        ELSE 0.0
    END AS BillCoalitionMemberPercentage,
    CASE 
        WHEN COUNT(DISTINCT BI.PersonID) > 0 THEN
            ROUND((COUNT(DISTINCT CASE WHEN BI.Ordinal = 1 AND ufs.CoalitionStatus = 'Opposition' THEN BI.PersonID 
                                      WHEN (BI.Ordinal > 1 OR BI.IsInitiator IS NULL) AND sufs.CoalitionStatus = 'Opposition' THEN BI.PersonID END) * 100.0) 
                  / COUNT(DISTINCT BI.PersonID), 1)
        ELSE 0.0
    END AS BillOppositionMemberPercentage,
    
    -- Bill-specific committee session data (direct bill-to-session connections)
    COALESCE(bcs.BillSpecificSessions, 0) AS BillCommitteeSessions,
    strftime(bcs.FirstRelevantSession, '%Y-%m-%d') as BillFirstCommitteeSession,
    strftime(bcs.LastRelevantSession, '%Y-%m-%d') as BillLastCommitteeSession,
    
    -- Plenum session data (from KNS_PlmSessionItem text analysis)
    COALESCE(bps.PlenumSessionCount, 0) AS BillPlenumSessionCount,
    strftime(bps.FirstPlenumSession, '%Y-%m-%d') as BillFirstPlenumSession,
    strftime(bps.FirstPlenumSession, '%Y-%m-%d') as FirstPlenumDiscussionDate,
    strftime(bps.LastPlenumSession, '%Y-%m-%d') as BillLastPlenumSession,
    ROUND(bps.AvgPlenumSessionDurationMinutes, 1) AS BillAvgPlenumSessionDurationMinutes,
    CASE 
        WHEN LENGTH(bps.PlenumSessionNames) > 200 THEN 
            SUBSTRING(bps.PlenumSessionNames, 1, 197) || '...'
        ELSE bps.PlenumSessionNames
    END AS BillPlenumSessionNames,
    psi.ItemTypeDesc AS BillPlenumItemType,
    
    -- Document information
    COALESCE(bd.DocumentCount, 0) AS BillDocumentCount,

    -- Primary document URL (prioritized: Published Law > First Reading > 2nd/3rd Reading > Early Discussion)
    COALESCE(
        bd.PrimaryDoc_PublishedLaw_PDF,
        bd.PrimaryDoc_PublishedLaw_Other,
        bd.FirstReading_Doc_PDF,
        bd.FirstReading_Doc_Other,
        bd.SecondThirdReading_Doc_PDF,
        bd.SecondThirdReading_Doc_Other,
        bd.EarlyDiscussion_Doc_PDF,
        bd.EarlyDiscussion_Doc_Other
    ) AS BillPrimaryDocumentURL,

    -- Document type label for primary doc
    CASE
        WHEN bd.PrimaryDoc_PublishedLaw_PDF IS NOT NULL OR bd.PrimaryDoc_PublishedLaw_Other IS NOT NULL THEN 'Published Law'
        WHEN bd.FirstReading_Doc_PDF IS NOT NULL OR bd.FirstReading_Doc_Other IS NOT NULL THEN 'First Reading'
        WHEN bd.SecondThirdReading_Doc_PDF IS NOT NULL OR bd.SecondThirdReading_Doc_Other IS NOT NULL THEN '2nd/3rd Reading'
        WHEN bd.EarlyDiscussion_Doc_PDF IS NOT NULL OR bd.EarlyDiscussion_Doc_Other IS NOT NULL THEN 'Early Discussion'
        ELSE NULL
    END AS BillPrimaryDocumentType,

    -- Format (PDF preferred, otherwise other formats)
    CASE
        WHEN bd.PrimaryDoc_PublishedLaw_PDF IS NOT NULL THEN 'PDF'
        WHEN bd.PrimaryDoc_PublishedLaw_Other IS NOT NULL THEN 'DOC'
        WHEN bd.FirstReading_Doc_PDF IS NOT NULL THEN 'PDF'
        WHEN bd.FirstReading_Doc_Other IS NOT NULL THEN 'DOC'
        WHEN bd.SecondThirdReading_Doc_PDF IS NOT NULL THEN 'PDF'
        WHEN bd.SecondThirdReading_Doc_Other IS NOT NULL THEN 'DOC'
        WHEN bd.EarlyDiscussion_Doc_PDF IS NOT NULL THEN 'PDF'
        WHEN bd.EarlyDiscussion_Doc_Other IS NOT NULL THEN 'DOC'
        ELSE NULL
    END AS BillPrimaryDocumentFormat,

    -- Document category counts (for detailed view)
    COALESCE(bd.PublishedLawCount, 0) AS BillPublishedLawDocCount,
    COALESCE(bd.FirstReadingCount, 0) AS BillFirstReadingDocCount,
    COALESCE(bd.SecondThirdCount, 0) AS BillSecondThirdReadingDocCount,
    COALESCE(bd.EarlyDiscussionCount, 0) AS BillEarlyDiscussionDocCount,
    COALESCE(bd.OtherDocCount, 0) AS BillOtherDocCount,

    -- Legacy format (for backward compatibility in exports, truncated for display)
    CASE
        WHEN LENGTH(bd.DocumentLinks) > 500 THEN
            SUBSTRING(bd.DocumentLinks, 1, 497) || '...'
        ELSE bd.DocumentLinks
    END AS BillDocumentLinks,

    -- Knesset.gov.il website link for bill details
    'https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid='
        || CAST(B.BillID AS VARCHAR) AS BillKnessetWebsiteURL,

    -- First Bill Submission Date (earliest activity: initiator assignment, committee, plenum, or publication)
    strftime(bfs.FirstSubmissionDate, '%Y-%m-%d') AS FirstBillSubmissionDate

FROM KNS_Bill B
LEFT JOIN KNS_Committee C ON CAST(B.CommitteeID AS BIGINT) = C.CommitteeID
LEFT JOIN KNS_Status S ON B.StatusID = S.StatusID
LEFT JOIN KNS_BillInitiator BI ON B.BillID = BI.BillID
LEFT JOIN KNS_Person Pi ON BI.PersonID = Pi.PersonID
LEFT JOIN BillMainInitiatorFaction bmif ON B.BillID = bmif.BillID AND bmif.rn = 1
LEFT JOIN KNS_Faction f ON bmif.FactionID = f.FactionID
LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID 
    AND B.KnessetNum = ufs.KnessetNum
LEFT JOIN BillSupportingMemberFactions bsmf ON BI.BillID = bsmf.BillID 
    AND BI.PersonID = bsmf.PersonID 
    AND bsmf.rn = 1
LEFT JOIN KNS_Faction sf ON bsmf.FactionID = sf.FactionID
LEFT JOIN UserFactionCoalitionStatus sufs ON sf.FactionID = sufs.FactionID 
    AND B.KnessetNum = sufs.KnessetNum
LEFT JOIN BillMergeInfo bmi ON B.BillID = bmi.MergedBillID
LEFT JOIN BillCommitteeSessions bcs ON B.BillID = bcs.BillID
LEFT JOIN BillPlenumSessions bps ON B.BillID = bps.BillID
LEFT JOIN KNS_PlmSessionItem psi ON B.BillID = psi.ItemID
LEFT JOIN BillDocuments bd ON B.BillID = bd.BillID
LEFT JOIN BillFirstSubmission bfs ON B.BillID = bfs.BillID

GROUP BY 
    B.BillID, B.KnessetNum, B.Name, B.SubTypeID, B.SubTypeDesc, B.PrivateNumber,
    B.CommitteeID, C.Name, C.CommitteeTypeDesc, C.AdditionalTypeID, 
    C.AdditionalTypeDesc, C.CommitteeParentName, B.StatusID, S."Desc", B.Number, 
    B.PostponementReasonID, B.PostponementReasonDesc, B.SummaryLaw, B.LastUpdatedDate, 
    B.PublicationDate, B.MagazineNumber, B.PageNumber, B.IsContinuationBill, 
    B.PublicationSeriesID, B.PublicationSeriesDesc, B.PublicationSeriesFirstCall,
    bcs.BillSpecificSessions, bcs.FirstRelevantSession, bcs.LastRelevantSession,
    bps.PlenumSessionCount, bps.FirstPlenumSession, bps.LastPlenumSession,
    bps.AvgPlenumSessionDurationMinutes, bps.PlenumSessionNames, psi.ItemTypeDesc,
    bd.DocumentCount, bd.DocumentLinks,
    bd.PrimaryDoc_PublishedLaw_PDF, bd.PrimaryDoc_PublishedLaw_Other,
    bd.FirstReading_Doc_PDF, bd.FirstReading_Doc_Other,
    bd.SecondThirdReading_Doc_PDF, bd.SecondThirdReading_Doc_Other,
    bd.EarlyDiscussion_Doc_PDF, bd.EarlyDiscussion_Doc_Other,
    bd.PublishedLawCount, bd.FirstReadingCount, bd.SecondThirdCount,
    bd.EarlyDiscussionCount, bd.OtherDocCount,
    bfs.FirstSubmissionDate

ORDER BY B.KnessetNum DESC, B.BillID DESC
LIMIT 1000;
        """,
        "knesset_filter_column": "B.KnessetNum",
        "faction_filter_column": "NULL", # Bills don't have direct faction association
        "description": (
            "Comprehensive bill data with initiator information, committee assignments, "
            "status details, committee session activity analysis, plenum session information "
            "with FirstPlenumDiscussionDate showing when each bill was first discussed in plenum, "
            "and FirstBillSubmissionDate showing the earliest activity date (true submission: initiator assignment, committee, plenum, or publication)"
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
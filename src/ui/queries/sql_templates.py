"""
Reusable SQL CTE templates for the Knesset parliamentary data platform.

This module consolidates commonly repeated SQL patterns into reusable templates
to reduce code duplication and ensure consistency across queries.

Usage:
    from ui.queries.sql_templates import SQLTemplates

    query = f'''
    WITH {SQLTemplates.STANDARD_FACTION_LOOKUP},
    {SQLTemplates.BILL_FIRST_SUBMISSION}
    SELECT ...
    '''
"""

from typing import Dict, List


class SQLTemplates:
    """Reusable SQL CTE fragments for query construction."""

    # Standard faction lookup with ROW_NUMBER to handle multiple positions per MK
    # Used in: Parliamentary Queries, Agenda Motions, Bills & Legislation
    STANDARD_FACTION_LOOKUP = """StandardFactionLookup AS (
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
)"""

    # Bill first submission date calculation
    # Considers: initiator assignment, committee sessions, plenum sessions, publication
    # Used in: Bills query, Bills per Faction chart, Bills by Coalition chart, Top Initiators chart
    BILL_FIRST_SUBMISSION = """BillFirstSubmission AS (
    -- Get the earliest activity date for each bill (true submission date)
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
)"""

    # Bill status categorization CASE statement
    # - Passed (118): Green
    # - First Reading (104,108,111,141,109,101,106,142,150,113,130,114): Blue
    # - Stopped/Inactive (all others): Red
    BILL_STATUS_CASE_HE = """CASE
    WHEN b.StatusID = 118 THEN 'התקבלה בקריאה שלישית'
    WHEN b.StatusID IN (104, 108, 111, 141, 109, 101, 106, 142, 150, 113, 130, 114) THEN 'קריאה ראשונה'
    ELSE 'הופסק/לא פעיל'
END"""

    # Bill status categorization in English
    BILL_STATUS_CASE_EN = """CASE
    WHEN b.StatusID = 118 THEN 'Passed'
    WHEN b.StatusID IN (104, 108, 111, 141, 109, 101, 106, 142, 150, 113, 130, 114) THEN 'First Reading'
    ELSE 'Stopped'
END"""

    # Query answer status categorization
    # Used in: Query Status by Faction chart, Ministry Response charts
    QUERY_STATUS_CASE = """CASE
    WHEN S."Desc" IN ('התקבלה תשובה', 'התקבלה תשובת ביניים', 'התקבלה תשובה חלקית') THEN 'Answered'
    WHEN S."Desc" IN ('נדחתה', 'נדחתה עקב הפסקת פעילות הכנסת') THEN 'Rejected'
    WHEN S."Desc" = 'הוסרה' THEN 'Removed'
    WHEN S."Desc" = 'נקבע תאריך תשובה' THEN 'Other/In Progress'
    ELSE 'Other/In Progress'
END"""

    # Minister lookup for queries
    # Used in: Parliamentary Queries
    MINISTER_LOOKUP = """MinisterLookup AS (
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
    AND p2p.DutyDesc NOT LIKE '%יושב ראש%' AND p2p.DutyDesc NOT LIKE '%יו"ר%'
)"""

    # Agenda documents aggregation
    # Used in: Agenda Motions query
    AGENDA_DOCUMENTS = """AgendaDocuments AS (
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
)"""

    # Stage order and colors for bill status charts
    BILL_STAGE_ORDER: List[str] = ['הופסק/לא פעיל', 'קריאה ראשונה', 'התקבלה בקריאה שלישית']

    BILL_STAGE_COLORS: Dict[str, str] = {
        'הופסק/לא פעיל': '#EF553B',        # Red - Stopped
        'קריאה ראשונה': '#636EFA',          # Blue - First Reading
        'התקבלה בקריאה שלישית': '#00CC96'   # Green - Passed
    }

    BILL_STAGE_COLORS_EN: Dict[str, str] = {
        'Stopped': '#EF553B',       # Red
        'First Reading': '#636EFA', # Blue
        'Passed': '#00CC96'         # Green
    }

    # First reading status IDs (for filtering)
    FIRST_READING_STATUS_IDS: List[int] = [104, 108, 111, 141, 109, 101, 106, 142, 150, 113, 130, 114]

    # Passed status ID
    PASSED_STATUS_ID: int = 118

    @classmethod
    def get_bill_status_case(cls, table_alias: str = "b", language: str = "he") -> str:
        """Get bill status CASE with custom table alias.

        Args:
            table_alias: The alias used for the KNS_Bill table (default: 'b')
            language: 'he' for Hebrew labels, 'en' for English (default: 'he')

        Returns:
            SQL CASE statement for bill status categorization
        """
        if language == "en":
            return cls.BILL_STATUS_CASE_EN.replace("b.StatusID", f"{table_alias}.StatusID")
        return cls.BILL_STATUS_CASE_HE.replace("b.StatusID", f"{table_alias}.StatusID")

    @classmethod
    def get_standard_faction_lookup(cls, table_alias: str = "ptp") -> str:
        """Get StandardFactionLookup CTE with custom table alias.

        Args:
            table_alias: The alias to use for KNS_PersonToPosition (default: 'ptp')

        Returns:
            SQL CTE for faction lookup
        """
        if table_alias == "ptp":
            return cls.STANDARD_FACTION_LOOKUP
        return cls.STANDARD_FACTION_LOOKUP.replace("ptp.", f"{table_alias}.")

    @classmethod
    def get_bill_first_submission(cls, bill_alias: str = "B") -> str:
        """Get BillFirstSubmission CTE with custom bill table alias.

        Args:
            bill_alias: The alias to use for KNS_Bill (default: 'B')

        Returns:
            SQL CTE for bill first submission date
        """
        if bill_alias == "B":
            return cls.BILL_FIRST_SUBMISSION
        return cls.BILL_FIRST_SUBMISSION.replace("B.BillID", f"{bill_alias}.BillID").replace(
            "FROM KNS_Bill B", f"FROM KNS_Bill {bill_alias}"
        )

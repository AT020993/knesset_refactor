"""
CAP Annotation Repository

Handles CRUD operations for bill annotations:
- Get uncoded bills
- Get coded bills
- Get annotation by bill ID
- Save annotation
- Delete annotation
- Filter API bills

Includes caching for annotation counts to avoid redundant database queries.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd
import streamlit as st

from backend.connection_manager import get_db_connection


# Module-level cached function for annotation counts
# This avoids recalculating counts on every page refresh
def _get_annotation_counts_impl(db_path_str: str) -> Dict[int, int]:
    """
    Get annotation counts for all bills.

    Returns a dictionary mapping BillID -> annotation count.

    Args:
        db_path_str: String path to database

    Returns:
        Dictionary of {bill_id: annotation_count}
    """
    try:
        from pathlib import Path
        db_path = Path(db_path_str)
        with get_db_connection(db_path, read_only=True) as conn:
            result = conn.execute("""
                SELECT BillID, COUNT(*) as total
                FROM UserBillCAP
                GROUP BY BillID
            """).fetchdf()

            if result.empty:
                return {}

            return dict(zip(result["BillID"].tolist(), result["total"].tolist()))
    except Exception:
        return {}


# Apply Streamlit caching decorator if available (not in test environment)
try:
    _get_cached_annotation_counts = st.cache_data(ttl=120, show_spinner=False)(_get_annotation_counts_impl)
except Exception:
    # Fallback for test environment where st.cache_data may not work
    _get_cached_annotation_counts = _get_annotation_counts_impl


def clear_annotation_counts_cache():
    """
    Clear the annotation counts cache.

    Call this after saving or deleting annotations to ensure
    counts are refreshed on the next query.
    """
    try:
        if hasattr(_get_cached_annotation_counts, 'clear'):
            _get_cached_annotation_counts.clear()
    except Exception:
        # Ignore errors in test environment
        pass


class CAPAnnotationRepository:
    """Repository for CAP bill annotation CRUD operations."""

    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        """Initialize the annotation repository."""
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)

    def get_annotation_counts(self) -> Dict[int, int]:
        """
        Get annotation counts for all bills (cached).

        This uses the module-level cached function to avoid
        redundant database queries across page refreshes.

        Returns:
            Dictionary mapping BillID to annotation count
        """
        return _get_cached_annotation_counts(str(self.db_path))

    def get_uncoded_bills(
        self, knesset_num: Optional[int] = None, limit: int = 100,
        search_term: Optional[str] = None,
        researcher_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get bills that haven't been coded by the specified researcher.

        In multi-annotator mode, each researcher has their own queue of bills
        to annotate. A bill appears in a researcher's queue until THEY annotate it,
        regardless of whether other researchers have annotated it.

        Args:
            knesset_num: Filter by Knesset number (optional)
            limit: Maximum number of bills to return
            search_term: Search by Bill ID or name (optional)
            researcher_id: Filter by researcher (required for accurate filtering)

        Returns:
            DataFrame with uncoded bills for this researcher
        """
        try:
            # Base query with subquery for annotation count
            query = """
                SELECT
                    B.BillID,
                    B.KnessetNum,
                    B.Name AS BillName,
                    B.SubTypeDesc AS BillType,
                    B.PrivateNumber,
                    strftime(CAST(B.PublicationDate AS TIMESTAMP), '%Y-%m-%d') AS PublicationDate,
                    strftime(CAST(B.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d') AS LastUpdated,
                    S."Desc" AS StatusDesc,
                    'https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid='
                        || CAST(B.BillID AS VARCHAR) AS BillURL,
                    COALESCE(ann_count.total, 0) AS AnnotationCount
                FROM KNS_Bill B
                LEFT JOIN KNS_Status S ON B.StatusID = S.StatusID
                LEFT JOIN (
                    SELECT BillID, COUNT(*) as total
                    FROM UserBillCAP
                    GROUP BY BillID
                ) ann_count ON B.BillID = ann_count.BillID
                WHERE NOT EXISTS (
                    SELECT 1 FROM UserBillCAP CAP
                    WHERE CAP.BillID = B.BillID
            """

            params = []

            # If researcher_id provided, filter by that researcher
            if researcher_id is not None:
                query += " AND CAP.ResearcherID = ?"
                params.append(researcher_id)

            query += ")"  # Close the NOT EXISTS subquery

            if knesset_num is not None:
                query += " AND B.KnessetNum = ?"
                params.append(knesset_num)

            # Search by Bill ID or Name
            if search_term:
                search_term = search_term.strip()
                query += " AND (CAST(B.BillID AS VARCHAR) LIKE ? OR B.Name LIKE ?)"
                params.append(f"%{search_term}%")
                params.append(f"%{search_term}%")

            query += f" ORDER BY B.KnessetNum DESC, B.BillID DESC LIMIT {limit}"

            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                if params:
                    result = conn.execute(query, params).fetchdf()
                else:
                    result = conn.execute(query).fetchdf()
                return result

        except Exception as e:
            self.logger.error(f"Error getting uncoded bills: {e}", exc_info=True)
            return pd.DataFrame()

    def get_coded_bills(
        self,
        knesset_num: Optional[int] = None,
        cap_code: Optional[int] = None,
        limit: int = 100,
        researcher_id: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Get bills that have been coded.

        Args:
            knesset_num: Filter by Knesset number (optional)
            cap_code: Filter by CAP code (optional)
            limit: Maximum number of bills to return
            researcher_id: Filter by researcher (optional, shows all if None)

        Returns:
            DataFrame with coded bills
        """
        try:
            query = """
                SELECT
                    CAP.AnnotationID,
                    CAP.BillID,
                    COALESCE(B.KnessetNum, 0) AS KnessetNum,
                    COALESCE(B.Name, 'Bill #' || CAST(CAP.BillID AS VARCHAR) || ' (from API)') AS BillName,
                    COALESCE(B.SubTypeDesc, CAP.Source) AS BillType,
                    CAP.CAPMinorCode,
                    T.MinorTopic_HE AS CAPTopic_HE,
                    T.MinorTopic_EN AS CAPTopic_EN,
                    T.MajorTopic_HE AS CAPMajorTopic_HE,
                    CAP.Direction,
                    CAP.ResearcherID,
                    R.DisplayName AS AssignedBy,
                    strftime(CAP.AssignedDate, '%Y-%m-%d %H:%M') AS AssignedDate,
                    CAP.Confidence,
                    CAP.Notes,
                    CAP.SubmissionDate,
                    CAP.Source,
                    'https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid='
                        || CAST(CAP.BillID AS VARCHAR) AS BillURL,
                    COALESCE(ann_count.total, 1) AS AnnotationCount
                FROM UserBillCAP CAP
                LEFT JOIN KNS_Bill B ON CAP.BillID = B.BillID
                JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
                LEFT JOIN UserResearchers R ON CAP.ResearcherID = R.ResearcherID
                LEFT JOIN (
                    SELECT BillID, COUNT(*) as total
                    FROM UserBillCAP
                    GROUP BY BillID
                ) ann_count ON CAP.BillID = ann_count.BillID
            """

            conditions = []
            params = []

            if knesset_num is not None:
                conditions.append("COALESCE(B.KnessetNum, 0) = ?")
                params.append(knesset_num)

            if cap_code is not None:
                conditions.append("CAP.CAPMinorCode = ?")
                params.append(cap_code)

            if researcher_id is not None:
                conditions.append("CAP.ResearcherID = ?")
                params.append(researcher_id)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += f" ORDER BY CAP.AssignedDate DESC LIMIT {limit}"

            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                if params:
                    result = conn.execute(query, params).fetchdf()
                else:
                    result = conn.execute(query).fetchdf()
                return result

        except Exception as e:
            self.logger.error(f"Error getting coded bills: {e}", exc_info=True)
            return pd.DataFrame()

    def get_recent_annotations(
        self, limit: int = 5, researcher_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get most recently annotated bills.

        Args:
            limit: Maximum number of recent annotations to return
            researcher_id: Filter by researcher (optional)

        Returns:
            DataFrame with recent annotations
        """
        try:
            query = """
                SELECT
                    CAP.BillID,
                    COALESCE(B.Name, 'Bill #' || CAST(CAP.BillID AS VARCHAR)) AS BillName,
                    T.MinorCode,
                    T.MinorTopic_HE,
                    CAP.Direction,
                    CAP.ResearcherID,
                    R.DisplayName AS AssignedBy,
                    strftime(CAP.AssignedDate, '%Y-%m-%d %H:%M') AS AssignedDate
                FROM UserBillCAP CAP
                LEFT JOIN KNS_Bill B ON CAP.BillID = B.BillID
                JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
                LEFT JOIN UserResearchers R ON CAP.ResearcherID = R.ResearcherID
            """

            params = []
            if researcher_id is not None:
                query += " WHERE CAP.ResearcherID = ?"
                params.append(researcher_id)

            query += " ORDER BY CAP.AssignedDate DESC LIMIT ?"
            params.append(limit)

            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = conn.execute(query, params).fetchdf()
                return result

        except Exception as e:
            self.logger.error(f"Error getting recent annotations: {e}", exc_info=True)
            return pd.DataFrame()

    def get_bills_with_status(
        self,
        knesset_num: Optional[int] = None,
        limit: int = 100,
        search_term: Optional[str] = None,
        include_coded: bool = False,
        researcher_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get bills with their annotation status (coded/uncoded) for a specific researcher.

        In multi-annotator mode, "coded" means coded BY THIS RESEARCHER.
        Bills coded by other researchers still appear as uncoded for this researcher.

        Args:
            knesset_num: Filter by Knesset number (optional)
            limit: Maximum number of bills to return
            search_term: Search by Bill ID or name (optional)
            include_coded: Whether to include bills already coded by this researcher
            researcher_id: The researcher's ID for determining coded status

        Returns:
            DataFrame with bills and their annotation status
        """
        try:
            # Build query with researcher-specific annotation check
            query = """
                SELECT
                    B.BillID,
                    B.KnessetNum,
                    B.Name AS BillName,
                    B.SubTypeDesc AS BillType,
                    strftime(CAST(B.PublicationDate AS TIMESTAMP), '%Y-%m-%d') AS PublicationDate,
                    S."Desc" AS StatusDesc,
                    CASE WHEN my_cap.BillID IS NOT NULL THEN 1 ELSE 0 END AS IsCoded,
                    my_cap.CAPMinorCode,
                    T.MinorCode,
                    T.MinorTopic_HE,
                    my_cap.Direction,
                    'https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid='
                        || CAST(B.BillID AS VARCHAR) AS BillURL,
                    COALESCE(ann_count.total, 0) AS AnnotationCount
                FROM KNS_Bill B
                LEFT JOIN KNS_Status S ON B.StatusID = S.StatusID
            """

            params = []

            # Join with current researcher's annotation (if researcher_id provided)
            if researcher_id is not None:
                query += """
                LEFT JOIN UserBillCAP my_cap ON B.BillID = my_cap.BillID
                    AND my_cap.ResearcherID = ?
                """
                params.append(researcher_id)
            else:
                query += " LEFT JOIN UserBillCAP my_cap ON B.BillID = my_cap.BillID"

            query += """
                LEFT JOIN UserCAPTaxonomy T ON my_cap.CAPMinorCode = T.MinorCode
                LEFT JOIN (
                    SELECT BillID, COUNT(*) as total
                    FROM UserBillCAP
                    GROUP BY BillID
                ) ann_count ON B.BillID = ann_count.BillID
                WHERE 1=1
            """

            # If not including coded, only show uncoded (for this researcher)
            if not include_coded:
                query += " AND my_cap.BillID IS NULL"

            if knesset_num is not None:
                query += " AND B.KnessetNum = ?"
                params.append(knesset_num)

            # Search by Bill ID or Name
            if search_term:
                search_term = search_term.strip()
                query += " AND (CAST(B.BillID AS VARCHAR) LIKE ? OR B.Name LIKE ?)"
                params.append(f"%{search_term}%")
                params.append(f"%{search_term}%")

            query += f" ORDER BY B.KnessetNum DESC, B.BillID DESC LIMIT {limit}"

            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                if params:
                    result = conn.execute(query, params).fetchdf()
                else:
                    result = conn.execute(query).fetchdf()
                return result

        except Exception as e:
            self.logger.error(f"Error getting bills with status: {e}", exc_info=True)
            return pd.DataFrame()

    def get_annotation_by_bill_id(
        self, bill_id: int, researcher_id: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get the full annotation details for a specific bill.

        In multi-annotator mode, if researcher_id is provided, returns only
        that researcher's annotation. Otherwise returns the first annotation found.

        Args:
            bill_id: The bill ID
            researcher_id: Filter by researcher (optional)

        Returns:
            Dictionary with annotation details or None if not found
        """
        try:
            query = """
                SELECT
                    CAP.AnnotationID,
                    CAP.BillID,
                    CAP.CAPMinorCode,
                    CAP.Direction,
                    CAP.ResearcherID,
                    R.DisplayName AS AssignedBy,
                    strftime(CAP.AssignedDate, '%Y-%m-%d %H:%M') AS AssignedDate,
                    CAP.Confidence,
                    CAP.Notes,
                    CAP.Source,
                    CAP.SubmissionDate,
                    T.MajorCode,
                    T.MajorTopic_HE,
                    T.MajorTopic_EN,
                    T.MinorTopic_HE,
                    T.MinorTopic_EN
                FROM UserBillCAP CAP
                JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
                LEFT JOIN UserResearchers R ON CAP.ResearcherID = R.ResearcherID
                WHERE CAP.BillID = ?
            """
            params = [bill_id]

            if researcher_id is not None:
                query += " AND CAP.ResearcherID = ?"
                params.append(researcher_id)

            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = conn.execute(query, params).fetchdf()
                if not result.empty:
                    return result.iloc[0].to_dict()
                return None

        except Exception as e:
            self.logger.error(
                f"Error getting annotation for bill {bill_id}: {e}", exc_info=True
            )
            return None

    def get_all_annotations_for_bill(self, bill_id: int) -> pd.DataFrame:
        """
        Get all annotations for a bill from all researchers.

        Used to display other researchers' annotations for inter-rater comparison.

        Args:
            bill_id: The bill ID

        Returns:
            DataFrame with all annotations for this bill
        """
        try:
            query = """
                SELECT
                    CAP.AnnotationID,
                    CAP.BillID,
                    CAP.ResearcherID,
                    R.DisplayName AS ResearcherName,
                    CAP.CAPMinorCode,
                    T.MinorTopic_HE,
                    T.MinorTopic_EN,
                    T.MajorTopic_HE,
                    CAP.Direction,
                    strftime(CAP.AssignedDate, '%Y-%m-%d %H:%M') AS AssignedDate,
                    CAP.Confidence,
                    CAP.Notes
                FROM UserBillCAP CAP
                JOIN UserResearchers R ON CAP.ResearcherID = R.ResearcherID
                JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
                WHERE CAP.BillID = ?
                ORDER BY CAP.AssignedDate DESC
            """

            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = conn.execute(query, [bill_id]).fetchdf()
                return result

        except Exception as e:
            self.logger.error(
                f"Error getting all annotations for bill {bill_id}: {e}", exc_info=True
            )
            return pd.DataFrame()

    def save_annotation(
        self,
        bill_id: int,
        cap_minor_code: int,
        direction: int,
        researcher_id: int,
        confidence: str = "Medium",
        notes: str = "",
        source: str = "Database",
        submission_date: str = "",
    ) -> bool:
        """
        Save a bill annotation for a specific researcher.

        In multi-annotator mode, each researcher can have their own annotation
        for the same bill. This method upserts (updates if exists, inserts if not)
        the annotation for the given researcher.

        Args:
            bill_id: The bill ID to annotate
            cap_minor_code: The CAP minor code (e.g., 101, 201, 301)
            direction: Direction code (+1, -1, or 0)
            researcher_id: The researcher's database ID
            confidence: Confidence level (High, Medium, Low)
            notes: Optional notes
            source: Source of the bill (Database or API)
            submission_date: Bill submission date

        Returns:
            True if successful, False otherwise
        """
        # Validate researcher_id is an integer (not string display name)
        # This catches the common mistake of passing cap_researcher_name instead of cap_user_id
        if not isinstance(researcher_id, int):
            self.logger.error(
                f"researcher_id must be int, got {type(researcher_id).__name__}: {researcher_id}. "
                "Did you pass cap_researcher_name instead of cap_user_id?"
            )
            return False

        if researcher_id <= 0:
            self.logger.error(f"Invalid researcher_id: {researcher_id}. Must be a positive integer.")
            return False

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                # Check if THIS researcher already annotated this bill
                existing = conn.execute(
                    "SELECT AnnotationID FROM UserBillCAP WHERE BillID = ? AND ResearcherID = ?",
                    [bill_id, researcher_id]
                ).fetchone()

                if existing:
                    # UPDATE existing annotation for this researcher
                    conn.execute(
                        """
                        UPDATE UserBillCAP SET
                            CAPMinorCode = ?,
                            Direction = ?,
                            AssignedDate = CURRENT_TIMESTAMP,
                            Confidence = ?,
                            Notes = ?,
                            Source = ?,
                            SubmissionDate = ?
                        WHERE BillID = ? AND ResearcherID = ?
                    """,
                        [
                            cap_minor_code,
                            direction,
                            confidence,
                            notes,
                            source,
                            submission_date,
                            bill_id,
                            researcher_id,
                        ],
                    )
                    self.logger.info(
                        f"Updated annotation for bill {bill_id} by researcher {researcher_id}"
                    )
                else:
                    # INSERT new annotation for this researcher
                    conn.execute(
                        """
                        INSERT INTO UserBillCAP
                        (BillID, ResearcherID, CAPMinorCode, Direction, Confidence, Notes, Source, SubmissionDate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        [
                            bill_id,
                            researcher_id,
                            cap_minor_code,
                            direction,
                            confidence,
                            notes,
                            source,
                            submission_date,
                        ],
                    )
                    self.logger.info(
                        f"Created annotation for bill {bill_id} by researcher {researcher_id}"
                    )

                # Clear cached annotation counts since data changed
                clear_annotation_counts_cache()
                return True

        except Exception as e:
            self.logger.error(
                f"Error saving annotation for bill {bill_id}: {e}", exc_info=True
            )
            return False

    def delete_annotation(
        self, bill_id: int, researcher_id: Optional[int] = None
    ) -> bool:
        """
        Delete an annotation for a bill.

        In multi-annotator mode, if researcher_id is provided, deletes only
        that researcher's annotation. Otherwise deletes all annotations for
        the bill (use with caution).

        Args:
            bill_id: The bill ID
            researcher_id: Delete only this researcher's annotation (recommended)

        Returns:
            True if successful, False otherwise
        """
        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                if researcher_id is not None:
                    conn.execute(
                        "DELETE FROM UserBillCAP WHERE BillID = ? AND ResearcherID = ?",
                        [bill_id, researcher_id]
                    )
                    self.logger.info(
                        f"Deleted annotation for bill {bill_id} by researcher {researcher_id}"
                    )
                else:
                    # Delete all annotations for this bill (admin action)
                    conn.execute("DELETE FROM UserBillCAP WHERE BillID = ?", [bill_id])
                    self.logger.info(f"Deleted all annotations for bill {bill_id}")

                # Clear cached annotation counts since data changed
                clear_annotation_counts_cache()
                return True

        except Exception as e:
            self.logger.error(
                f"Error deleting annotation for bill {bill_id}: {e}", exc_info=True
            )
            return False

    def get_bill_documents(self, bill_id: int) -> pd.DataFrame:
        """
        Fetch documents for a bill from KNS_DocumentBill, prioritized by type.

        Documents are ordered with most relevant first:
        1. Published Law (חוק - פרסום ברשומות)
        2. First Reading (הצעת חוק לקריאה הראשונה)
        3. Second/Third Reading (הצעת חוק לקריאה השנייה והשלישית)
        4. Early Discussion (הצעת חוק לדיון מוקדם)
        5. Other documents
        Within each type, PDFs are prioritized over other formats.

        Args:
            bill_id: The bill ID to fetch documents for

        Returns:
            DataFrame with columns: DocumentType, Format, URL
        """
        try:
            query = """
                SELECT
                    GroupTypeDesc as DocumentType,
                    ApplicationDesc as Format,
                    FilePath as URL
                FROM KNS_DocumentBill
                WHERE BillID = ?
                    AND FilePath IS NOT NULL
                ORDER BY
                    CASE GroupTypeDesc
                        WHEN 'חוק - פרסום ברשומות' THEN 1
                        WHEN 'הצעת חוק לקריאה הראשונה' THEN 2
                        WHEN 'הצעת חוק לקריאה השנייה והשלישית' THEN 3
                        WHEN 'הצעת חוק לדיון מוקדם' THEN 4
                        ELSE 5
                    END,
                    CASE ApplicationDesc WHEN 'PDF' THEN 1 ELSE 2 END
            """

            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = conn.execute(query, [bill_id]).fetchdf()
                return result

        except Exception as e:
            self.logger.error(
                f"Error fetching documents for bill {bill_id}: {e}", exc_info=True
            )
            return pd.DataFrame()

    def get_bills_not_in_database(
        self, api_bills: pd.DataFrame, limit: int = 100,
        researcher_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Filter API bills to only those not in the local database.

        In multi-annotator mode, excludes bills already coded by the specified
        researcher (other researchers' annotations don't count).

        Args:
            api_bills: DataFrame of bills from API
            limit: Maximum number of results
            researcher_id: Filter out bills coded by this researcher

        Returns:
            DataFrame of bills not in local database
        """
        if api_bills.empty:
            return api_bills

        try:
            bill_ids = api_bills["BillID"].tolist()
            placeholders = ",".join(["?" for _ in bill_ids])

            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                # Get bills that ARE in database
                query = f"SELECT BillID FROM KNS_Bill WHERE BillID IN ({placeholders})"
                existing = conn.execute(query, bill_ids).fetchdf()
                existing_ids = (
                    set(existing["BillID"].tolist()) if not existing.empty else set()
                )

                # Filter to bills NOT in database
                not_in_db = api_bills[~api_bills["BillID"].isin(existing_ids)]

                # Exclude bills already coded by THIS researcher
                if researcher_id is not None:
                    query2 = (
                        f"SELECT BillID FROM UserBillCAP "
                        f"WHERE BillID IN ({placeholders}) AND ResearcherID = ?"
                    )
                    coded = conn.execute(query2, bill_ids + [researcher_id]).fetchdf()
                else:
                    # Fallback: exclude any coded bills (old behavior)
                    query2 = (
                        f"SELECT DISTINCT BillID FROM UserBillCAP WHERE BillID IN ({placeholders})"
                    )
                    coded = conn.execute(query2, bill_ids).fetchdf()

                coded_ids = set(coded["BillID"].tolist()) if not coded.empty else set()
                not_in_db = not_in_db[~not_in_db["BillID"].isin(coded_ids)]

                return not_in_db.head(limit)

        except Exception as e:
            self.logger.error(f"Error filtering bills: {e}", exc_info=True)
            return api_bills.head(limit)

"""
CAP Annotation Repository

Handles CRUD operations for bill annotations:
- Get uncoded bills
- Get coded bills
- Get annotation by bill ID
- Save annotation
- Delete annotation
- Filter API bills
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd

from backend.connection_manager import get_db_connection


class CAPAnnotationRepository:
    """Repository for CAP bill annotation CRUD operations."""

    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        """Initialize the annotation repository."""
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)

    def get_uncoded_bills(
        self, knesset_num: Optional[int] = None, limit: int = 100
    ) -> pd.DataFrame:
        """
        Get bills that haven't been coded yet.

        Args:
            knesset_num: Filter by Knesset number (optional)
            limit: Maximum number of bills to return

        Returns:
            DataFrame with uncoded bills
        """
        try:
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
                        || CAST(B.BillID AS VARCHAR) AS BillURL
                FROM KNS_Bill B
                LEFT JOIN KNS_Status S ON B.StatusID = S.StatusID
                LEFT JOIN UserBillCAP CAP ON B.BillID = CAP.BillID
                WHERE CAP.BillID IS NULL
            """

            params = []
            if knesset_num is not None:
                query += " AND B.KnessetNum = ?"
                params.append(knesset_num)

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
    ) -> pd.DataFrame:
        """
        Get bills that have been coded.

        Args:
            knesset_num: Filter by Knesset number (optional)
            cap_code: Filter by CAP code (optional)
            limit: Maximum number of bills to return

        Returns:
            DataFrame with coded bills
        """
        try:
            query = """
                SELECT
                    CAP.BillID,
                    COALESCE(B.KnessetNum, 0) AS KnessetNum,
                    COALESCE(B.Name, 'Bill #' || CAST(CAP.BillID AS VARCHAR) || ' (from API)') AS BillName,
                    COALESCE(B.SubTypeDesc, CAP.Source) AS BillType,
                    CAP.CAPMinorCode,
                    T.MinorTopic_HE AS CAPTopic_HE,
                    T.MinorTopic_EN AS CAPTopic_EN,
                    T.MajorTopic_HE AS CAPMajorTopic_HE,
                    CAP.Direction,
                    CAP.AssignedBy,
                    strftime(CAP.AssignedDate, '%Y-%m-%d %H:%M') AS AssignedDate,
                    CAP.Confidence,
                    CAP.Notes,
                    CAP.SubmissionDate,
                    CAP.Source,
                    'https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid='
                        || CAST(CAP.BillID AS VARCHAR) AS BillURL
                FROM UserBillCAP CAP
                LEFT JOIN KNS_Bill B ON CAP.BillID = B.BillID
                JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
            """

            conditions = []
            params = []

            if knesset_num is not None:
                conditions.append("COALESCE(B.KnessetNum, 0) = ?")
                params.append(knesset_num)

            if cap_code is not None:
                conditions.append("CAP.CAPMinorCode = ?")
                params.append(cap_code)

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

    def get_annotation_by_bill_id(self, bill_id: int) -> Optional[Dict[str, Any]]:
        """
        Get the full annotation details for a specific bill.

        Args:
            bill_id: The bill ID

        Returns:
            Dictionary with annotation details or None if not found
        """
        try:
            query = """
                SELECT
                    CAP.BillID,
                    CAP.CAPMinorCode,
                    CAP.Direction,
                    CAP.AssignedBy,
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
                WHERE CAP.BillID = ?
            """

            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = conn.execute(query, [bill_id]).fetchdf()
                if not result.empty:
                    return result.iloc[0].to_dict()
                return None

        except Exception as e:
            self.logger.error(
                f"Error getting annotation for bill {bill_id}: {e}", exc_info=True
            )
            return None

    def save_annotation(
        self,
        bill_id: int,
        cap_minor_code: int,
        direction: int,
        assigned_by: str,
        confidence: str = "Medium",
        notes: str = "",
        source: str = "Database",
        submission_date: str = "",
    ) -> bool:
        """
        Save a bill annotation.

        Args:
            bill_id: The bill ID to annotate
            cap_minor_code: The CAP minor code (e.g., 101, 201, 301)
            direction: Direction code (+1, -1, or 0)
            assigned_by: Name of the researcher
            confidence: Confidence level (High, Medium, Low)
            notes: Optional notes
            source: Source of the bill (Database or API)
            submission_date: Bill submission date

        Returns:
            True if successful, False otherwise
        """
        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                existing = conn.execute(
                    "SELECT BillID FROM UserBillCAP WHERE BillID = ?", [bill_id]
                ).fetchone()

                if existing:
                    conn.execute(
                        """
                        UPDATE UserBillCAP SET
                            CAPMinorCode = ?,
                            Direction = ?,
                            AssignedBy = ?,
                            AssignedDate = CURRENT_TIMESTAMP,
                            Confidence = ?,
                            Notes = ?,
                            Source = ?,
                            SubmissionDate = ?
                        WHERE BillID = ?
                    """,
                        [
                            cap_minor_code,
                            direction,
                            assigned_by,
                            confidence,
                            notes,
                            source,
                            submission_date,
                            bill_id,
                        ],
                    )
                    self.logger.info(f"Updated annotation for bill {bill_id}")
                else:
                    conn.execute(
                        """
                        INSERT INTO UserBillCAP
                        (BillID, CAPMinorCode, Direction, AssignedBy, Confidence, Notes, Source, SubmissionDate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        [
                            bill_id,
                            cap_minor_code,
                            direction,
                            assigned_by,
                            confidence,
                            notes,
                            source,
                            submission_date,
                        ],
                    )
                    self.logger.info(f"Created annotation for bill {bill_id}")

                return True

        except Exception as e:
            self.logger.error(
                f"Error saving annotation for bill {bill_id}: {e}", exc_info=True
            )
            return False

    def delete_annotation(self, bill_id: int) -> bool:
        """
        Delete an annotation for a bill.

        Args:
            bill_id: The bill ID

        Returns:
            True if successful, False otherwise
        """
        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                conn.execute("DELETE FROM UserBillCAP WHERE BillID = ?", [bill_id])
                self.logger.info(f"Deleted annotation for bill {bill_id}")
                return True

        except Exception as e:
            self.logger.error(
                f"Error deleting annotation for bill {bill_id}: {e}", exc_info=True
            )
            return False

    def get_bills_not_in_database(
        self, api_bills: pd.DataFrame, limit: int = 100
    ) -> pd.DataFrame:
        """
        Filter API bills to only those not in the local database.

        Args:
            api_bills: DataFrame of bills from API
            limit: Maximum number of results

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

                # Also exclude already coded bills
                query2 = (
                    f"SELECT BillID FROM UserBillCAP WHERE BillID IN ({placeholders})"
                )
                coded = conn.execute(query2, bill_ids).fetchdf()
                coded_ids = set(coded["BillID"].tolist()) if not coded.empty else set()

                not_in_db = not_in_db[~not_in_db["BillID"].isin(coded_ids)]

                return not_in_db.head(limit)

        except Exception as e:
            self.logger.error(f"Error filtering bills: {e}", exc_info=True)
            return api_bills.head(limit)

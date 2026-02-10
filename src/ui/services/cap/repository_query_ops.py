"""Read/query operations for CAP annotation repository."""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from backend.connection_manager import get_db_connection


def get_uncoded_bills(
    repo: Any,
    knesset_num: Optional[int] = None,
    limit: int = 100,
    search_term: Optional[str] = None,
    researcher_id: Optional[int] = None,
) -> pd.DataFrame:
    """Get bills not yet coded by the specified researcher."""
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

        params: list[Any] = []
        if researcher_id is not None:
            query += " AND CAP.ResearcherID = ?"
            params.append(researcher_id)

        query += ")"

        if knesset_num is not None:
            query += " AND B.KnessetNum = ?"
            params.append(knesset_num)

        if search_term:
            term = search_term.strip()
            query += " AND (CAST(B.BillID AS VARCHAR) LIKE ? OR B.Name LIKE ?)"
            params.append(f"%{term}%")
            params.append(f"%{term}%")

        query += f" ORDER BY B.KnessetNum DESC, B.BillID DESC LIMIT {limit}"

        with get_db_connection(repo.db_path, read_only=True, logger_obj=repo.logger) as conn:
            if params:
                return conn.execute(query, params).fetchdf()
            return conn.execute(query).fetchdf()

    except Exception as exc:
        repo.logger.error(f"Error getting uncoded bills: {exc}", exc_info=True)
        return pd.DataFrame()


def get_coded_bills(
    repo: Any,
    knesset_num: Optional[int] = None,
    cap_code: Optional[int] = None,
    limit: int = 100,
    researcher_id: Optional[int] = None,
) -> pd.DataFrame:
    """Get bills that already have CAP coding."""
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

        conditions: list[str] = []
        params: list[Any] = []

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

        with get_db_connection(repo.db_path, read_only=True, logger_obj=repo.logger) as conn:
            if params:
                return conn.execute(query, params).fetchdf()
            return conn.execute(query).fetchdf()

    except Exception as exc:
        repo.logger.error(f"Error getting coded bills: {exc}", exc_info=True)
        return pd.DataFrame()


def get_recent_annotations(
    repo: Any,
    limit: int = 5,
    researcher_id: Optional[int] = None,
) -> pd.DataFrame:
    """Get most recent annotations."""
    try:
        query = """
            SELECT
                CAP.BillID,
                COALESCE(B.Name, 'Bill #' || CAST(CAP.BillID AS VARCHAR)) AS BillName,
                T.MinorCode,
                T.MinorTopic_HE,
                CAP.ResearcherID,
                R.DisplayName AS AssignedBy,
                strftime(CAP.AssignedDate, '%Y-%m-%d %H:%M') AS AssignedDate
            FROM UserBillCAP CAP
            LEFT JOIN KNS_Bill B ON CAP.BillID = B.BillID
            JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
            LEFT JOIN UserResearchers R ON CAP.ResearcherID = R.ResearcherID
        """

        params: list[Any] = []
        if researcher_id is not None:
            query += " WHERE CAP.ResearcherID = ?"
            params.append(researcher_id)

        query += " ORDER BY CAP.AssignedDate DESC LIMIT ?"
        params.append(limit)

        with get_db_connection(repo.db_path, read_only=True, logger_obj=repo.logger) as conn:
            return conn.execute(query, params).fetchdf()

    except Exception as exc:
        repo.logger.error(f"Error getting recent annotations: {exc}", exc_info=True)
        return pd.DataFrame()


def get_bills_with_status(
    repo: Any,
    knesset_num: Optional[int] = None,
    limit: int = 100,
    search_term: Optional[str] = None,
    include_coded: bool = False,
    researcher_id: Optional[int] = None,
) -> pd.DataFrame:
    """Get bills plus coding status for a researcher."""
    try:
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
                'https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid='
                    || CAST(B.BillID AS VARCHAR) AS BillURL,
                COALESCE(ann_count.total, 0) AS AnnotationCount
            FROM KNS_Bill B
            LEFT JOIN KNS_Status S ON B.StatusID = S.StatusID
        """

        params: list[Any] = []
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

        if not include_coded:
            query += " AND my_cap.BillID IS NULL"

        if knesset_num is not None:
            query += " AND B.KnessetNum = ?"
            params.append(knesset_num)

        if search_term:
            term = search_term.strip()
            query += " AND (CAST(B.BillID AS VARCHAR) LIKE ? OR B.Name LIKE ?)"
            params.append(f"%{term}%")
            params.append(f"%{term}%")

        query += f" ORDER BY B.KnessetNum DESC, B.BillID DESC LIMIT {limit}"

        with get_db_connection(repo.db_path, read_only=True, logger_obj=repo.logger) as conn:
            if params:
                return conn.execute(query, params).fetchdf()
            return conn.execute(query).fetchdf()

    except Exception as exc:
        repo.logger.error(f"Error getting bills with status: {exc}", exc_info=True)
        return pd.DataFrame()


def get_annotation_by_bill_id(
    repo: Any,
    bill_id: int,
    researcher_id: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    """Get full annotation details for one bill."""
    try:
        query = """
            SELECT
                CAP.AnnotationID,
                CAP.BillID,
                CAP.CAPMinorCode,
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
        params: list[Any] = [bill_id]

        if researcher_id is not None:
            query += " AND CAP.ResearcherID = ?"
            params.append(researcher_id)

        with get_db_connection(repo.db_path, read_only=True, logger_obj=repo.logger) as conn:
            result = conn.execute(query, params).fetchdf()
            if not result.empty:
                return result.iloc[0].to_dict()
            return None

    except Exception as exc:
        repo.logger.error(
            f"Error getting annotation for bill {bill_id}: {exc}", exc_info=True
        )
        return None


def get_all_annotations_for_bill(repo: Any, bill_id: int) -> pd.DataFrame:
    """Get all annotations for a bill across researchers."""
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
                strftime(CAP.AssignedDate, '%Y-%m-%d %H:%M') AS AssignedDate,
                CAP.Confidence,
                CAP.Notes
            FROM UserBillCAP CAP
            JOIN UserResearchers R ON CAP.ResearcherID = R.ResearcherID
            JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
            WHERE CAP.BillID = ?
            ORDER BY CAP.AssignedDate DESC
        """

        with get_db_connection(repo.db_path, read_only=True, logger_obj=repo.logger) as conn:
            return conn.execute(query, [bill_id]).fetchdf()

    except Exception as exc:
        repo.logger.error(
            f"Error getting all annotations for bill {bill_id}: {exc}", exc_info=True
        )
        return pd.DataFrame()


def get_bill_documents(repo: Any, bill_id: int) -> pd.DataFrame:
    """Fetch documents for a bill ordered by relevance."""
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

        with get_db_connection(repo.db_path, read_only=True, logger_obj=repo.logger) as conn:
            return conn.execute(query, [bill_id]).fetchdf()

    except Exception as exc:
        repo.logger.error(
            f"Error fetching documents for bill {bill_id}: {exc}", exc_info=True
        )
        return pd.DataFrame()


def get_bills_not_in_database(
    repo: Any,
    api_bills: pd.DataFrame,
    limit: int = 100,
    researcher_id: Optional[int] = None,
) -> pd.DataFrame:
    """Filter API bills to those not in DB and not already coded by researcher."""
    if api_bills.empty:
        return api_bills

    try:
        bill_ids = api_bills["BillID"].tolist()
        placeholders = ",".join(["?" for _ in bill_ids])

        with get_db_connection(repo.db_path, read_only=True, logger_obj=repo.logger) as conn:
            existing_query = f"SELECT BillID FROM KNS_Bill WHERE BillID IN ({placeholders})"
            existing = conn.execute(existing_query, bill_ids).fetchdf()
            existing_ids = set(existing["BillID"].tolist()) if not existing.empty else set()

            not_in_db = api_bills[~api_bills["BillID"].isin(existing_ids)]

            if researcher_id is not None:
                coded_query = (
                    f"SELECT BillID FROM UserBillCAP "
                    f"WHERE BillID IN ({placeholders}) AND ResearcherID = ?"
                )
                coded = conn.execute(coded_query, bill_ids + [researcher_id]).fetchdf()
            else:
                coded_query = (
                    f"SELECT DISTINCT BillID FROM UserBillCAP WHERE BillID IN ({placeholders})"
                )
                coded = conn.execute(coded_query, bill_ids).fetchdf()

            coded_ids = set(coded["BillID"].tolist()) if not coded.empty else set()
            not_in_db = not_in_db[~not_in_db["BillID"].isin(coded_ids)]
            return not_in_db.head(limit)

    except Exception as exc:
        repo.logger.error(f"Error filtering bills: {exc}", exc_info=True)
        return api_bills.head(limit)


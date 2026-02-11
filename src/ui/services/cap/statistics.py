"""
CAP Statistics and Export Service

Handles analytics and data export for bill annotations:
- Annotation statistics by category, Knesset
- CSV export functionality
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any

from backend.connection_manager import get_db_connection


class CAPStatisticsService:
    """Service for CAP annotation statistics and export."""

    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        """Initialize the statistics service."""
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)

    def get_annotation_stats(self) -> Dict[str, Any]:
        """
        Get statistics about annotations.

        In multi-annotator mode, counts unique bills (not total annotations).
        For example, if 2 researchers annotated the same bill, that counts as
        1 coded bill, not 2.

        Returns:
            Dictionary with annotation statistics including:
            - total_coded: Number of unique annotated bills
            - total_annotations: Total annotation records (may be > total_coded)
            - total_bills: Total bills in database
            - total_researchers: Number of researchers with annotations
            - by_major_category: Breakdown by major category (unique bills)
            - by_knesset: Breakdown by Knesset number (unique bills)
            - by_researcher: Breakdown by researcher
        """
        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                stats = {}

                # Combined scalar counts (4 queries → 1)
                scalar_result = conn.execute("""
                    SELECT
                        COUNT(DISTINCT BillID) as total_coded,
                        COUNT(*) as total_annotations,
                        COUNT(DISTINCT ResearcherID) as total_researchers,
                        (SELECT COUNT(*) FROM KNS_Bill) as total_bills
                    FROM UserBillCAP
                """).fetchone()

                stats["total_coded"] = scalar_result[0] if scalar_result else 0
                stats["total_annotations"] = scalar_result[1] if scalar_result else 0
                stats["total_researchers"] = scalar_result[2] if scalar_result else 0
                stats["total_bills"] = scalar_result[3] if scalar_result else 0

                # By major category (count unique bills, not annotations)
                by_major = conn.execute("""
                    SELECT T.MajorTopic_HE, COUNT(DISTINCT CAP.BillID) as count
                    FROM UserBillCAP CAP
                    JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
                    GROUP BY T.MajorCode, T.MajorTopic_HE
                    ORDER BY T.MajorCode
                """).fetchdf()
                stats["by_major_category"] = by_major.to_dict("records")

                # By direction - deprecated, return empty list for backwards compatibility
                stats["by_direction"] = []

                # By Knesset (count unique bills)
                by_knesset = conn.execute("""
                    SELECT B.KnessetNum, COUNT(DISTINCT CAP.BillID) as count
                    FROM UserBillCAP CAP
                    JOIN KNS_Bill B ON CAP.BillID = B.BillID
                    GROUP BY B.KnessetNum
                    ORDER BY B.KnessetNum DESC
                """).fetchdf()
                stats["by_knesset"] = by_knesset.to_dict("records")

                # By researcher (annotation count per researcher)
                by_researcher = conn.execute("""
                    SELECT
                        R.DisplayName as researcher_name,
                        COUNT(*) as annotation_count,
                        COUNT(DISTINCT CAP.BillID) as unique_bills
                    FROM UserBillCAP CAP
                    JOIN UserResearchers R ON CAP.ResearcherID = R.ResearcherID
                    GROUP BY CAP.ResearcherID, R.DisplayName
                    ORDER BY annotation_count DESC
                """).fetchdf()
                stats["by_researcher"] = by_researcher.to_dict("records")

                return stats

        except Exception as e:
            self.logger.error(f"Error getting annotation stats: {e}", exc_info=True)
            return {}

    def export_annotations(self, output_path: Path) -> bool:
        """
        Export all annotations to CSV.

        In multi-annotator mode, each annotation is a separate row, including
        the researcher who made it. This allows analysis of inter-rater reliability.

        Args:
            output_path: Path to save the CSV file

        Returns:
            True if successful, False otherwise
        """
        try:
            query = """
                SELECT
                    CAP.AnnotationID,
                    B.BillID,
                    B.KnessetNum,
                    B.Name AS BillName,
                    B.SubTypeDesc AS BillType,
                    B.PrivateNumber,
                    strftime(CAST(B.PublicationDate AS TIMESTAMP), '%Y-%m-%d') AS PublicationDate,
                    CAP.CAPMinorCode,
                    T.MajorCode AS CAPMajorCode,
                    T.MajorTopic_HE AS CAPMajorTopic_HE,
                    T.MajorTopic_EN AS CAPMajorTopic_EN,
                    T.MinorTopic_HE AS CAPMinorTopic_HE,
                    T.MinorTopic_EN AS CAPMinorTopic_EN,
                    CAP.ResearcherID,
                    R.DisplayName AS ResearcherName,
                    R.Username AS ResearcherUsername,
                    strftime(CAP.AssignedDate, '%Y-%m-%d %H:%M:%S') AS AssignedDate,
                    CAP.Confidence,
                    CAP.Notes,
                    CAP.Source,
                    CAP.SubmissionDate
                FROM UserBillCAP CAP
                JOIN KNS_Bill B ON CAP.BillID = B.BillID
                JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
                LEFT JOIN UserResearchers R ON CAP.ResearcherID = R.ResearcherID
                ORDER BY B.KnessetNum DESC, B.BillID DESC, CAP.ResearcherID
            """

            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = conn.execute(query).fetchdf()
                result.to_csv(output_path, index=False, encoding="utf-8-sig")
                self.logger.info(f"Exported {len(result)} annotations to {output_path}")
                return True

        except Exception as e:
            self.logger.error(f"Error exporting annotations: {e}", exc_info=True)
            return False

    def get_coverage_stats(self) -> Dict[str, Any]:
        """
        Get annotation coverage statistics.

        Returns:
            Dictionary with coverage statistics
        """
        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                stats = {}

                # Coverage by Knesset (using NULLIF to prevent division by zero)
                coverage = conn.execute("""
                    SELECT
                        B.KnessetNum,
                        COUNT(DISTINCT B.BillID) AS total_bills,
                        COUNT(DISTINCT CAP.BillID) AS coded_bills,
                        COALESCE(
                            ROUND(
                                100.0 * COUNT(DISTINCT CAP.BillID) /
                                NULLIF(COUNT(DISTINCT B.BillID), 0),
                                1
                            ),
                            0.0
                        ) AS coverage_pct
                    FROM KNS_Bill B
                    LEFT JOIN UserBillCAP CAP ON B.BillID = CAP.BillID
                    GROUP BY B.KnessetNum
                    ORDER BY B.KnessetNum DESC
                """).fetchdf()
                stats["by_knesset"] = coverage.to_dict("records")

                return stats

        except Exception as e:
            self.logger.error(f"Error getting coverage stats: {e}", exc_info=True)
            return {}

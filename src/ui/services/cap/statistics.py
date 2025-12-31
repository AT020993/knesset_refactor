"""
CAP Statistics and Export Service

Handles analytics and data export for bill annotations:
- Annotation statistics by category, direction, Knesset
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

        Returns:
            Dictionary with annotation statistics including:
            - total_coded: Number of annotated bills
            - total_bills: Total bills in database
            - by_major_category: Breakdown by major category
            - by_direction: Breakdown by direction
            - by_knesset: Breakdown by Knesset number
        """
        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                stats = {}

                # Total coded
                result = conn.execute(
                    "SELECT COUNT(*) as count FROM UserBillCAP"
                ).fetchone()
                stats["total_coded"] = result[0] if result else 0

                # Total bills
                result = conn.execute(
                    "SELECT COUNT(*) as count FROM KNS_Bill"
                ).fetchone()
                stats["total_bills"] = result[0] if result else 0

                # By major category
                by_major = conn.execute("""
                    SELECT T.MajorTopic_HE, COUNT(*) as count
                    FROM UserBillCAP CAP
                    JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
                    GROUP BY T.MajorCode, T.MajorTopic_HE
                    ORDER BY T.MajorCode
                """).fetchdf()
                stats["by_major_category"] = by_major.to_dict("records")

                # By direction
                by_direction = conn.execute("""
                    SELECT Direction, COUNT(*) as count
                    FROM UserBillCAP
                    GROUP BY Direction
                """).fetchdf()
                stats["by_direction"] = by_direction.to_dict("records")

                # By Knesset
                by_knesset = conn.execute("""
                    SELECT B.KnessetNum, COUNT(*) as count
                    FROM UserBillCAP CAP
                    JOIN KNS_Bill B ON CAP.BillID = B.BillID
                    GROUP BY B.KnessetNum
                    ORDER BY B.KnessetNum DESC
                """).fetchdf()
                stats["by_knesset"] = by_knesset.to_dict("records")

                return stats

        except Exception as e:
            self.logger.error(f"Error getting annotation stats: {e}", exc_info=True)
            return {}

    def export_annotations(self, output_path: Path) -> bool:
        """
        Export all annotations to CSV.

        Args:
            output_path: Path to save the CSV file

        Returns:
            True if successful, False otherwise
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
                    CAP.CAPMinorCode,
                    T.MajorCode AS CAPMajorCode,
                    T.MajorTopic_HE AS CAPMajorTopic_HE,
                    T.MajorTopic_EN AS CAPMajorTopic_EN,
                    T.MinorTopic_HE AS CAPMinorTopic_HE,
                    T.MinorTopic_EN AS CAPMinorTopic_EN,
                    CAP.Direction,
                    CASE CAP.Direction
                        WHEN 1 THEN 'הרחבה/חיזוק'
                        WHEN -1 THEN 'צמצום/פגיעה'
                        ELSE 'אחר'
                    END AS Direction_HE,
                    CAP.AssignedBy,
                    strftime(CAP.AssignedDate, '%Y-%m-%d %H:%M:%S') AS AssignedDate,
                    CAP.Confidence,
                    CAP.Notes,
                    CAP.Source,
                    CAP.SubmissionDate
                FROM UserBillCAP CAP
                JOIN KNS_Bill B ON CAP.BillID = B.BillID
                JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
                ORDER BY B.KnessetNum DESC, B.BillID DESC
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

                # Coverage by Knesset
                coverage = conn.execute("""
                    SELECT
                        B.KnessetNum,
                        COUNT(DISTINCT B.BillID) AS total_bills,
                        COUNT(DISTINCT CAP.BillID) AS coded_bills,
                        ROUND(
                            100.0 * COUNT(DISTINCT CAP.BillID) / COUNT(DISTINCT B.BillID),
                            1
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

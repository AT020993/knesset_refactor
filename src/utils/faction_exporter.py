"""
Faction export utility for generating CSV files with faction data per Knesset.

This module provides functionality to export all factions appearing in each Knesset
along with their coalition status where available.
"""

import io
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import duckdb

from config.settings import Settings
from backend.connection_manager import get_db_connection, safe_execute_query


class FactionExporter:
    """Utility for exporting faction data per Knesset with coalition status."""

    def __init__(self, db_path: Optional[Path] = None, logger_obj: Optional[logging.Logger] = None):
        """
        Initialize the faction exporter.

        Args:
            db_path: Path to the database (defaults to Settings.get_db_path())
            logger_obj: Logger instance for error reporting
        """
        self.db_path = db_path or Settings.get_db_path()
        self.logger = logger_obj or logging.getLogger(__name__)

    def get_all_factions_with_coalition_status(self, knesset_num: Optional[int] = None) -> pd.DataFrame:
        """
        Get all factions per Knesset with coalition status.

        Args:
            knesset_num: Optional specific Knesset number. If None, returns all Knessets.

        Returns:
            DataFrame with columns: KnessetNum, FactionID, FactionName, CoalitionStatus, MemberCount
        """
        knesset_filter = f"AND ptp.KnessetNum = {knesset_num}" if knesset_num else ""

        query = f"""
        SELECT DISTINCT
            ptp.KnessetNum,
            f.FactionID,
            COALESCE(ufs.NewFactionName, f.Name) as FactionName,
            COALESCE(ufs.CoalitionStatus, 'Unknown') as CoalitionStatus,
            COUNT(DISTINCT ptp.PersonID) as MemberCount
        FROM KNS_PersonToPosition ptp
        JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
        LEFT JOIN UserFactionCoalitionStatus ufs
            ON f.FactionID = ufs.FactionID
            AND ptp.KnessetNum = ufs.KnessetNum
        WHERE ptp.FactionID IS NOT NULL
          AND ptp.KnessetNum IS NOT NULL
          {knesset_filter}
        GROUP BY ptp.KnessetNum, f.FactionID, COALESCE(ufs.NewFactionName, f.Name), ufs.CoalitionStatus
        ORDER BY ptp.KnessetNum DESC, FactionName
        """

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                result = safe_execute_query(conn, query, self.logger)
                if isinstance(result, pd.DataFrame):
                    self.logger.info(f"Retrieved {len(result)} faction records")
                    return result
                return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error getting factions with coalition status: {e}", exc_info=True)
            return pd.DataFrame()

    def get_faction_summary_by_knesset(self) -> pd.DataFrame:
        """
        Get summary statistics of factions per Knesset.

        Returns:
            DataFrame with columns: KnessetNum, TotalFactions, CoalitionCount, OppositionCount, UnknownCount
        """
        query = """
        WITH FactionStats AS (
            SELECT DISTINCT
                ptp.KnessetNum,
                f.FactionID,
                COALESCE(ufs.CoalitionStatus, 'Unknown') as CoalitionStatus
            FROM KNS_PersonToPosition ptp
            JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
            LEFT JOIN UserFactionCoalitionStatus ufs
                ON f.FactionID = ufs.FactionID
                AND ptp.KnessetNum = ufs.KnessetNum
            WHERE ptp.FactionID IS NOT NULL
              AND ptp.KnessetNum IS NOT NULL
        )
        SELECT
            KnessetNum,
            COUNT(DISTINCT FactionID) as TotalFactions,
            COUNT(DISTINCT CASE WHEN CoalitionStatus = 'Coalition' THEN FactionID END) as CoalitionCount,
            COUNT(DISTINCT CASE WHEN CoalitionStatus = 'Opposition' THEN FactionID END) as OppositionCount,
            COUNT(DISTINCT CASE WHEN CoalitionStatus NOT IN ('Coalition', 'Opposition') THEN FactionID END) as UnknownCount
        FROM FactionStats
        GROUP BY KnessetNum
        ORDER BY KnessetNum DESC
        """

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                result = safe_execute_query(conn, query, self.logger)
                return result if result is not None else pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error getting faction summary: {e}", exc_info=True)
            return pd.DataFrame()

    def get_available_knesset_numbers(self) -> list:
        """
        Get list of available Knesset numbers that have faction data.

        Returns:
            List of Knesset numbers in descending order
        """
        query = """
        SELECT DISTINCT KnessetNum
        FROM KNS_PersonToPosition
        WHERE FactionID IS NOT NULL
          AND KnessetNum IS NOT NULL
        ORDER BY KnessetNum DESC
        """

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                result = safe_execute_query(conn, query, self.logger)
                if isinstance(result, pd.DataFrame) and not result.empty:
                    return result['KnessetNum'].tolist()
                return []
        except Exception as e:
            self.logger.error(f"Error getting available Knesset numbers: {e}", exc_info=True)
            return []

    def export_to_csv_buffer(self, knesset_num: Optional[int] = None) -> io.BytesIO:
        """
        Export factions to CSV buffer with UTF-8 BOM encoding for Excel compatibility.

        Args:
            knesset_num: Optional specific Knesset number. If None, exports all Knessets.

        Returns:
            BytesIO buffer containing the CSV data
        """
        df = self.get_all_factions_with_coalition_status(knesset_num)

        # Add Hebrew column headers for better readability
        df = df.rename(columns={
            'KnessetNum': 'Knesset Number / מספר כנסת',
            'FactionID': 'Faction ID / מזהה סיעה',
            'FactionName': 'Faction Name / שם הסיעה',
            'CoalitionStatus': 'Coalition Status / מעמד קואליציוני',
            'MemberCount': 'Member Count / מספר חברים'
        })

        # Create CSV with UTF-8 BOM encoding
        csv_data = df.to_csv(index=False).encode('utf-8-sig')
        return io.BytesIO(csv_data)

    def export_to_file(self, output_path: Path, knesset_num: Optional[int] = None) -> bool:
        """
        Export factions to CSV file.

        Args:
            output_path: Path where the CSV file will be saved
            knesset_num: Optional specific Knesset number. If None, exports all Knessets.

        Returns:
            True if export succeeded, False otherwise
        """
        try:
            buffer = self.export_to_csv_buffer(knesset_num)
            with open(output_path, 'wb') as f:
                f.write(buffer.getvalue())

            self.logger.info(f"Exported faction data to {output_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error exporting to file: {e}", exc_info=True)
            return False

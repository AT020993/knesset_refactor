"""
Parliamentary data export utility for generating CSV files with faction information.

This module exports three comprehensive datasets:
- All agendas with faction information
- All queries with faction information
- All bills with faction information

Each export includes KnessetNum, FactionID, FactionName, and CoalitionStatus.
"""

import logging
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from config.settings import Settings
from backend.connection_manager import get_db_connection, safe_execute_query


class ParliamentaryExporter:
    """Utility for exporting parliamentary data with faction information."""

    def __init__(self, db_path: Optional[Path] = None, logger_obj: Optional[logging.Logger] = None):
        """
        Initialize the parliamentary exporter.

        Args:
            db_path: Path to the database (defaults to Settings.get_db_path())
            logger_obj: Logger instance for error reporting
        """
        self.db_path = db_path or Settings.get_db_path()
        self.logger = logger_obj or logging.getLogger(__name__)

    def export_all_agendas(self, output_path: str) -> int:
        """
        Export all agenda motions with faction information.

        Args:
            output_path: Path to save the CSV file

        Returns:
            Number of rows exported
        """
        query = """
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
        )
        SELECT
            A.AgendaID,
            A.KnessetNum,
            sfl.FactionID,
            F.Name as FactionName,
            '' as CoalitionStatus
        FROM KNS_Agenda A
        LEFT JOIN StandardFactionLookup sfl
            ON A.InitiatorPersonID = sfl.PersonID
            AND A.KnessetNum = sfl.KnessetNum
            AND sfl.rn = 1
        LEFT JOIN KNS_Faction F ON sfl.FactionID = F.FactionID
        ORDER BY A.KnessetNum DESC, A.AgendaID DESC
        """

        return self._execute_and_export(query, output_path, "agendas")

    def export_all_queries(self, output_path: str) -> int:
        """
        Export all parliamentary queries with faction information.

        Args:
            output_path: Path to save the CSV file

        Returns:
            Number of rows exported
        """
        query = """
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
        )
        SELECT
            Q.QueryID,
            Q.KnessetNum,
            sfl.FactionID,
            F.Name as FactionName,
            '' as CoalitionStatus
        FROM KNS_Query Q
        LEFT JOIN StandardFactionLookup sfl
            ON Q.PersonID = sfl.PersonID
            AND Q.KnessetNum = sfl.KnessetNum
            AND sfl.rn = 1
        LEFT JOIN KNS_Faction F ON sfl.FactionID = F.FactionID
        ORDER BY Q.KnessetNum DESC, Q.QueryID DESC
        """

        return self._execute_and_export(query, output_path, "queries")

    def export_all_bills(self, output_path: str) -> int:
        """
        Export all bills with faction information.

        Args:
            output_path: Path to save the CSV file

        Returns:
            Number of rows exported
        """
        query = """
        WITH MainInitiators AS (
            -- Get main initiator (Ordinal=1) for each bill
            SELECT
                BI.BillID,
                BI.PersonID
            FROM KNS_BillInitiator BI
            WHERE BI.Ordinal = 1
        ),
        InitiatorFactionLookup AS (
            -- Get faction for main initiator
            SELECT
                mi.BillID,
                ptp.FactionID,
                ROW_NUMBER() OVER (
                    PARTITION BY mi.BillID
                    ORDER BY
                        CASE WHEN ptp.FactionID IS NOT NULL THEN 0 ELSE 1 END,
                        ptp.StartDate DESC NULLS LAST
                ) as rn
            FROM MainInitiators mi
            JOIN KNS_Bill B ON mi.BillID = B.BillID
            LEFT JOIN KNS_PersonToPosition ptp ON mi.PersonID = ptp.PersonID
                AND B.KnessetNum = ptp.KnessetNum
                AND ptp.FactionID IS NOT NULL
        )
        SELECT
            B.BillID,
            B.KnessetNum,
            ifl.FactionID,
            F.Name as FactionName,
            '' as CoalitionStatus
        FROM KNS_Bill B
        LEFT JOIN InitiatorFactionLookup ifl ON B.BillID = ifl.BillID AND ifl.rn = 1
        LEFT JOIN KNS_Faction F ON ifl.FactionID = F.FactionID
        ORDER BY B.KnessetNum DESC, B.BillID DESC
        """

        return self._execute_and_export(query, output_path, "bills")

    def export_all(self, output_dir: str = ".") -> Dict[str, int]:
        """
        Export all three CSV files to the specified directory.

        Args:
            output_dir: Directory where files will be saved (default: current directory)

        Returns:
            Dictionary mapping filename to row count
        """
        output_path = Path(output_dir)
        results = {}

        # Export agendas
        agenda_path = output_path / "all_agendas.csv"
        results["all_agendas.csv"] = self.export_all_agendas(str(agenda_path))
        self.logger.info(f"Exported {results['all_agendas.csv']:,} agendas to {agenda_path}")

        # Export queries
        query_path = output_path / "all_queries.csv"
        results["all_queries.csv"] = self.export_all_queries(str(query_path))
        self.logger.info(f"Exported {results['all_queries.csv']:,} queries to {query_path}")

        # Export bills
        bill_path = output_path / "all_bills.csv"
        results["all_bills.csv"] = self.export_all_bills(str(bill_path))
        self.logger.info(f"Exported {results['all_bills.csv']:,} bills to {bill_path}")

        return results

    def _execute_and_export(self, query: str, output_path: str, entity_type: str) -> int:
        """
        Execute a query and export results to CSV.

        Args:
            query: SQL query to execute
            output_path: Path to save the CSV file
            entity_type: Type of entity being exported (for logging)

        Returns:
            Number of rows exported
        """
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                df = safe_execute_query(conn, query, self.logger)

                if df is None or df.empty:
                    self.logger.warning(f"No {entity_type} data to export")
                    return 0

                # Export to CSV with UTF-8 BOM for Excel compatibility
                df.to_csv(output_path, index=False, encoding='utf-8-sig')

                row_count = len(df)
                self.logger.info(f"Exported {row_count:,} {entity_type} to {output_path}")
                return row_count

        except Exception as e:
            self.logger.error(f"Error exporting {entity_type}: {e}", exc_info=True)
            return 0


# Command-line interface for running exports
if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    exporter = ParliamentaryExporter()

    if len(sys.argv) > 1:
        output_dir = sys.argv[1]
    else:
        output_dir = "."

    print(f"Exporting parliamentary data to {output_dir}...")
    results = exporter.export_all(output_dir)

    print("\n=== Export Complete ===")
    for filename, count in results.items():
        print(f"  {filename}: {count:,} rows")
    print(f"\nTotal: {sum(results.values()):,} rows exported")

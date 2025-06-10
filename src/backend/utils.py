"""Backend utility functions and helpers."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd

from backend.connection_manager import get_db_connection, safe_execute_query
from config.settings import Settings


def map_mk_site_code(
    db_path: Path, logger_obj: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """Return mapping of MK internal PersonID to website SiteID."""
    if logger_obj is None:
        logger_obj = logging.getLogger(__name__)

    try:
        with get_db_connection(db_path, read_only=True, logger_obj=logger_obj) as con:
            # Check if KNS_MkSiteCode table exists
            tables_df = con.execute("SHOW TABLES").df()
            if "kns_mksitecode" in tables_df["name"].str.lower().tolist():
                return safe_execute_query(
                    con, "SELECT KnsID, SiteID FROM KNS_MkSiteCode", logger_obj
                )
            else:
                logger_obj.info(
                    "KNS_MkSiteCode table not found. Cannot map MK site codes."
                )
                return pd.DataFrame(columns=["KnsID", "SiteID"])

    except Exception as e:
        logger_obj.warning(f"Error accessing KNS_MkSiteCode: {e}", exc_info=True)
        return pd.DataFrame(columns=["KnsID", "SiteID"])


def get_faction_display_mapping(
    db_path: Path, logger_obj: Optional[logging.Logger] = None
) -> Dict[str, int]:
    """Get mapping of faction names to their display order/priority."""
    if logger_obj is None:
        logger_obj = logging.getLogger(__name__)

    try:
        with get_db_connection(db_path, read_only=True, logger_obj=logger_obj) as con:
            query = """
            SELECT DISTINCT FactionName, COUNT(*) as member_count
            FROM KNS_PersonToPosition 
            WHERE FactionName IS NOT NULL 
            GROUP BY FactionName 
            ORDER BY member_count DESC
            """

            result = safe_execute_query(con, query, logger_obj)
            if result is not None and not result.empty:
                # Create mapping based on member count (higher count = lower number = higher priority)
                return {
                    faction: idx
                    for idx, faction in enumerate(result["FactionName"].tolist())
                }

    except Exception as e:
        logger_obj.warning(f"Error getting faction display mapping: {e}", exc_info=True)

    return {}


def get_database_summary(
    db_path: Path, logger_obj: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """Get a summary of the database contents."""
    if logger_obj is None:
        logger_obj = logging.getLogger(__name__)

    summary = {
        "database_path": str(db_path),
        "exists": db_path.exists(),
        "tables": {},
        "total_tables": 0,
        "total_rows": 0,
    }

    if not db_path.exists():
        return summary

    try:
        with get_db_connection(db_path, read_only=True, logger_obj=logger_obj) as con:
            # Get list of tables
            tables_query = (
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            )
            tables_result = safe_execute_query(con, tables_query, logger_obj)

            if tables_result is not None and not tables_result.empty:
                table_names = tables_result["table_name"].tolist()
                summary["total_tables"] = len(table_names)

                # Get row count for each table
                for table_name in table_names:
                    try:
                        count_query = f'SELECT COUNT(*) as count FROM "{table_name}"'
                        count_result = safe_execute_query(con, count_query, logger_obj)

                        if count_result is not None and not count_result.empty:
                            row_count = count_result.iloc[0]["count"]
                            summary["tables"][table_name] = row_count
                            summary["total_rows"] += row_count
                        else:
                            summary["tables"][table_name] = 0

                    except Exception as e:
                        logger_obj.warning(
                            f"Error getting count for table {table_name}: {e}"
                        )
                        summary["tables"][table_name] = -1  # Error indicator

    except Exception as e:
        logger_obj.error(f"Error getting database summary: {e}", exc_info=True)

    return summary


def validate_database_integrity(
    db_path: Path, logger_obj: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """Validate database integrity and return a report."""
    if logger_obj is None:
        logger_obj = logging.getLogger(__name__)

    report = {
        "database_exists": db_path.exists(),
        "issues": [],
        "warnings": [],
        "table_checks": {},
        "overall_status": "unknown",
    }

    if not db_path.exists():
        report["issues"].append(f"Database file does not exist: {db_path}")
        report["overall_status"] = "error"
        return report

    try:
        from backend.tables import KnessetTables

        expected_tables = KnessetTables.get_table_names()

        with get_db_connection(db_path, read_only=True, logger_obj=logger_obj) as con:
            # Check which tables exist
            tables_query = (
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            )
            tables_result = safe_execute_query(con, tables_query, logger_obj)

            existing_tables = []
            if tables_result is not None and not tables_result.empty:
                existing_tables = tables_result["table_name"].tolist()

            # Check for missing tables
            missing_tables = [t for t in expected_tables if t not in existing_tables]
            if missing_tables:
                report["warnings"].extend(
                    [f"Missing table: {t}" for t in missing_tables]
                )

            # Check for unexpected tables
            unexpected_tables = [
                t
                for t in existing_tables
                if t not in expected_tables and not t.startswith("User")
            ]
            if unexpected_tables:
                report["warnings"].extend(
                    [f"Unexpected table: {t}" for t in unexpected_tables]
                )

            # Check each existing table
            for table_name in existing_tables:
                table_check = {"exists": True, "row_count": 0, "has_data": False}

                try:
                    count_query = f'SELECT COUNT(*) as count FROM "{table_name}"'
                    count_result = safe_execute_query(con, count_query, logger_obj)

                    if count_result is not None and not count_result.empty:
                        row_count = count_result.iloc[0]["count"]
                        table_check["row_count"] = row_count
                        table_check["has_data"] = row_count > 0

                        if row_count == 0:
                            report["warnings"].append(f"Table {table_name} is empty")

                except Exception as e:
                    table_check["error"] = str(e)
                    report["issues"].append(f"Error checking table {table_name}: {e}")

                report["table_checks"][table_name] = table_check

        # Determine overall status
        if report["issues"]:
            report["overall_status"] = "error"
        elif report["warnings"]:
            report["overall_status"] = "warning"
        else:
            report["overall_status"] = "healthy"

    except Exception as e:
        logger_obj.error(f"Error during integrity check: {e}", exc_info=True)
        report["issues"].append(f"Integrity check failed: {e}")
        report["overall_status"] = "error"

    return report


def backup_database(
    db_path: Path, backup_path: Path, logger_obj: Optional[logging.Logger] = None
) -> bool:
    """Create a backup of the database."""
    if logger_obj is None:
        logger_obj = logging.getLogger(__name__)

    try:
        import shutil

        if not db_path.exists():
            logger_obj.error(f"Source database does not exist: {db_path}")
            return False

        # Ensure backup directory exists
        backup_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy the database file
        shutil.copy2(db_path, backup_path)

        logger_obj.info(f"Database backed up from {db_path} to {backup_path}")
        return True

    except Exception as e:
        logger_obj.error(f"Error creating backup: {e}", exc_info=True)
        return False


def restore_database(
    backup_path: Path, db_path: Path, logger_obj: Optional[logging.Logger] = None
) -> bool:
    """Restore database from backup."""
    if logger_obj is None:
        logger_obj = logging.getLogger(__name__)

    try:
        import shutil

        if not backup_path.exists():
            logger_obj.error(f"Backup file does not exist: {backup_path}")
            return False

        # Ensure target directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy the backup to the database location
        shutil.copy2(backup_path, db_path)

        logger_obj.info(f"Database restored from {backup_path} to {db_path}")
        return True

    except Exception as e:
        logger_obj.error(f"Error restoring backup: {e}", exc_info=True)
        return False

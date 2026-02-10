"""Catalog repair and hard-delete operations for CAPUserService."""

from __future__ import annotations

import os
import shutil
import tempfile
from typing import Any

import duckdb


def hard_delete_user(service: Any, researcher_id: int) -> bool:
    """Permanently delete a user when they have no annotations."""
    service.ensure_table_exists()

    try:
        annotation_count = get_user_annotation_count(service, researcher_id)
        service.logger.info(f"User {researcher_id} has {annotation_count} annotations")
        if annotation_count > 0:
            service.logger.warning(
                f"Cannot hard delete user ID {researcher_id}: "
                f"has {annotation_count} annotations. Use soft delete instead."
            )
            return False

        service.logger.info(
            f"Attempting to delete user {researcher_id} using raw connection"
        )
        conn = duckdb.connect(str(service.db_path), read_only=False)
        try:
            user_exists = conn.execute(
                "SELECT 1 FROM UserResearchers WHERE ResearcherID = ?",
                [researcher_id],
            ).fetchone()
            if not user_exists:
                service.logger.warning(f"User {researcher_id} does not exist")
                return False

            try:
                conn.execute(
                    "DELETE FROM UserResearchers WHERE ResearcherID = ?",
                    [researcher_id],
                )

                still_exists = conn.execute(
                    "SELECT 1 FROM UserResearchers WHERE ResearcherID = ?",
                    [researcher_id],
                ).fetchone()
                if not still_exists:
                    service.logger.info(f"Successfully deleted user ID: {researcher_id}")
                    return True
            except Exception as delete_exc:
                error_str = str(delete_exc)
                service.logger.error(f"Delete failed: {error_str}")
                if "UserBillCAP_new" in error_str:
                    service.logger.warning(
                        "Corrupted catalog detected - using EXPORT/IMPORT to fix"
                    )
                    conn.close()
                    return rebuild_database_catalog(service, researcher_id)
                raise

            service.logger.error(f"Delete executed but user {researcher_id} still exists!")
            return False
        finally:
            conn.close()
    except Exception as exc:
        service.logger.error(f"Error hard deleting user: {exc}", exc_info=True)
        return False


def rebuild_database_catalog(service: Any, researcher_id_to_delete: int) -> bool:
    """Rebuild full DuckDB catalog and retry user deletion."""
    service.logger.info("Starting database catalog rebuild...")

    export_dir = tempfile.mkdtemp(prefix="duckdb_export_")
    db_path_str = str(service.db_path)
    backup_path = db_path_str + ".backup"

    try:
        service.logger.info(f"Exporting database to {export_dir}...")
        conn = duckdb.connect(db_path_str, read_only=False)
        try:
            conn.execute(f"EXPORT DATABASE '{export_dir}' (FORMAT PARQUET)")
            service.logger.info("Export completed")
        finally:
            conn.close()

        service.logger.info("Backing up original database...")
        shutil.copy2(db_path_str, backup_path)

        service.logger.info("Removing original database...")
        os.remove(db_path_str)
        wal_path = db_path_str + ".wal"
        if os.path.exists(wal_path):
            os.remove(wal_path)

        service.logger.info("Creating fresh database and importing...")
        conn = duckdb.connect(db_path_str, read_only=False)
        try:
            conn.execute(f"IMPORT DATABASE '{export_dir}'")
            service.logger.info("Import completed")

            service.logger.info(f"Deleting user {researcher_id_to_delete}...")
            conn.execute(
                "DELETE FROM UserResearchers WHERE ResearcherID = ?",
                [researcher_id_to_delete],
            )

            still_exists = conn.execute(
                "SELECT 1 FROM UserResearchers WHERE ResearcherID = ?",
                [researcher_id_to_delete],
            ).fetchone()
            if still_exists:
                raise RuntimeError("Delete succeeded but user still exists")

            conn.execute("CHECKPOINT")
            service.logger.info(
                f"Successfully deleted user {researcher_id_to_delete} after catalog rebuild"
            )

            if os.path.exists(backup_path):
                os.remove(backup_path)
            return True
        except Exception as import_exc:
            service.logger.error(f"Import or delete failed: {import_exc}")

            if os.path.exists(backup_path):
                service.logger.info("Restoring from backup...")
                if os.path.exists(db_path_str):
                    os.remove(db_path_str)
                shutil.move(backup_path, db_path_str)
            raise
        finally:
            conn.close()
    except Exception as exc:
        service.logger.error(f"Catalog rebuild failed: {exc}", exc_info=True)
        return False
    finally:
        if os.path.exists(export_dir):
            shutil.rmtree(export_dir, ignore_errors=True)


def get_user_annotation_count(service: Any, researcher_id: int) -> int:
    """Count user annotations using raw connection for resilient catalog handling."""
    import traceback

    service.logger.info(
        f"get_user_annotation_count called for researcher_id={researcher_id}"
    )
    service.ensure_table_exists()

    conn = None
    try:
        conn = duckdb.connect(str(service.db_path), read_only=False)
        service.logger.info("Checking if UserBillCAP table exists...")
        table_check = conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'UserBillCAP'"
        ).fetchone()
        if not table_check:
            service.logger.info("UserBillCAP table does not exist, returning 0")
            return 0

        service.logger.info("Querying annotation count...")
        result = conn.execute(
            """
            SELECT COUNT(*) FROM UserBillCAP
            WHERE ResearcherID = ?
            """,
            [researcher_id],
        ).fetchone()
        service.logger.info(f"Query succeeded, count={result[0] if result else 0}")
        return result[0] if result else 0
    except Exception as exc:
        error_str = str(exc)
        service.logger.error(
            f"Error in get_user_annotation_count: {error_str}\n"
            f"Full traceback:\n{traceback.format_exc()}"
        )
        if "UserBillCAP_new" in error_str:
            service.logger.error(
                "CRITICAL: UserBillCAP_new reference detected! "
                "Returning 0 to allow delete to proceed."
            )
        return 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

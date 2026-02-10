"""Metadata and freshness operations for storage sync service."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import duckdb

from data.services.sync_types import SyncMetadata


def get_sync_metadata(service: Any, settings: Any) -> SyncMetadata:
    """Collect typed metadata for sync decisions."""
    local_exists = settings.DEFAULT_DB_PATH.exists()
    cloud_exists = service.check_cloud_data_exists() if service.enabled else False
    freshness_state = service.compare_freshness() if service.enabled else "unknown"
    return SyncMetadata(
        local_exists=local_exists,
        cloud_exists=cloud_exists,
        freshness_state=freshness_state,
    )


def check_cloud_data_exists(service: Any) -> bool:
    """Check whether cloud DB file exists."""
    if not service.enabled:
        return False
    return bool(service.gcs_manager.file_exists("data/warehouse.duckdb"))


def get_cloud_file_info(service: Any) -> dict[str, Any]:
    """Get metadata summary for cloud artifacts."""
    if not service.enabled:
        return {}

    info: dict[str, Any] = {}
    try:
        db_meta = service.gcs_manager.get_file_metadata("data/warehouse.duckdb")
        if db_meta:
            info["database"] = {
                "exists": True,
                "size_mb": db_meta["size"] / (1024 * 1024),
                "updated": db_meta["updated"],
            }
        else:
            info["database"] = {"exists": False}

        parquet_files = service.gcs_manager.list_files(prefix="data/parquet/")
        info["parquet_files"] = {"count": len(parquet_files), "files": parquet_files}

        csv_meta = service.gcs_manager.get_file_metadata("data/faction_coalition_status.csv")
        info["faction_csv"] = {"exists": csv_meta is not None}

        resume_meta = service.gcs_manager.get_file_metadata("data/.resume_state.json")
        info["resume_state"] = {"exists": resume_meta is not None}
    except Exception as exc:
        service.logger.error(f"Error getting cloud file info: {exc}", exc_info=True)
        info["error"] = str(exc)

    return info


def get_local_last_modified(service: Any, settings: Any) -> datetime | None:
    """Read last local DB modification timestamp from metadata table or file mtime."""
    try:
        if not settings.DEFAULT_DB_PATH.exists():
            return None

        conn = duckdb.connect(str(settings.DEFAULT_DB_PATH), read_only=True)
        try:
            result = conn.execute(
                """
                SELECT Value FROM _SyncMetadata WHERE Key = 'last_modified'
                """
            ).fetchone()
            if result:
                return datetime.fromisoformat(result[0])
        except Exception:
            pass
        finally:
            conn.close()

        mtime = settings.DEFAULT_DB_PATH.stat().st_mtime
        return datetime.fromtimestamp(mtime)
    except Exception as exc:
        service.logger.debug(f"Error getting local last modified: {exc}")
        return None


def update_last_modified(service: Any, settings: Any) -> bool:
    """Update local last-modified marker in _SyncMetadata."""
    try:
        if not settings.DEFAULT_DB_PATH.exists():
            return False

        conn = duckdb.connect(str(settings.DEFAULT_DB_PATH), read_only=False)
        try:
            now = datetime.now().isoformat()
            conn.execute(
                """
                INSERT OR REPLACE INTO _SyncMetadata (Key, Value, UpdatedAt)
                VALUES ('last_modified', ?, CURRENT_TIMESTAMP)
                """,
                [now],
            )
            return True
        except Exception as exc:
            service.logger.debug(f"Could not update _SyncMetadata (table may not exist): {exc}")
            return False
        finally:
            conn.close()
    except Exception as exc:
        service.logger.debug(f"Error updating last modified: {exc}")
        return False


def get_cloud_last_modified(service: Any) -> datetime | None:
    """Get cloud DB last-updated timestamp if available."""
    if not service.enabled:
        return None
    try:
        meta = service.gcs_manager.get_file_metadata("data/warehouse.duckdb")
        if meta:
            updated = meta.get("updated")
            if isinstance(updated, datetime):
                return updated
            if isinstance(updated, str):
                try:
                    return datetime.fromisoformat(updated.replace("Z", "+00:00"))
                except ValueError:
                    return None
        return None
    except Exception as exc:
        service.logger.debug(f"Error getting cloud last modified: {exc}")
        return None


def compare_freshness(service: Any, settings: Any) -> str:
    """Compare local and cloud freshness with clock-skew tolerance."""
    _ = settings
    local_modified = service.get_local_last_modified()
    cloud_modified = service.get_cloud_last_modified()

    if local_modified is None and cloud_modified is None:
        return "unknown"
    if local_modified is None:
        return "cloud_newer"
    if cloud_modified is None:
        return "local_newer"

    diff = (local_modified - cloud_modified).total_seconds()
    if diff > 60:
        return "local_newer"
    if diff < -60:
        return "cloud_newer"
    return "up_to_date"

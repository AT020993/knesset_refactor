"""Startup sync and backup operations for storage sync service."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Callable


def _database_result_succeeded(results: Any) -> bool:
    """Extract database success from transfer results map."""
    if isinstance(results, dict):
        return bool(results.get("database", False))
    return False

def backup_local_database(service: Any, settings: Any) -> Path | None:
    """Create local DB backup before overwrite."""
    db_path = Path(settings.DEFAULT_DB_PATH)
    if not db_path.exists():
        return None

    try:
        backup_path = db_path.with_suffix(".duckdb.backup")
        shutil.copy2(db_path, backup_path)
        service.logger.info(f"Created database backup at {backup_path}")
        return backup_path
    except Exception as exc:
        service.logger.warning(f"Failed to create backup: {exc}")
        return None


def smart_sync_on_startup(
    service: Any,
    settings: Any,
    force_download: bool = False,
    progress_callback: Callable[[str], None] | None = None,
) -> bool:
    """Apply startup sync strategy based on local/cloud freshness."""
    db_path = Path(settings.DEFAULT_DB_PATH)
    if not service.enabled:
        service.logger.info("Cloud storage disabled, checking local data only")
        return db_path.exists()

    local_db_exists = db_path.exists()
    if local_db_exists and not force_download:
        freshness = service.compare_freshness()
        if freshness == "cloud_newer":
            service.logger.info("Cloud database is newer, downloading...")
            if progress_callback:
                progress_callback("Cloud has newer data, downloading...")
            backup_local_database(service, settings)
            results = service.download_all_data(progress_callback)
            return _database_result_succeeded(results)
        if freshness == "local_newer":
            service.logger.info("Local database is newer than cloud")
        else:
            service.logger.info("Local database exists, skipping cloud download")
        return True

    if progress_callback:
        progress_callback("Checking cloud storage...")

    cloud_exists = service.check_cloud_data_exists()
    if not cloud_exists:
        service.logger.info("No data in cloud storage, local refresh required")
        return False

    if local_db_exists:
        backup_local_database(service, settings)

    service.logger.info("Downloading data from cloud storage...")
    results = service.download_all_data(progress_callback)
    success = _database_result_succeeded(results)
    if success:
        service.logger.info("Successfully synced data from cloud storage")
    else:
        service.logger.warning("Failed to download database from cloud storage")
    return success

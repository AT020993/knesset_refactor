"""Transfer/upload/download operations for storage sync service."""

from __future__ import annotations

from typing import Any, Callable


def download_all_data(
    service: Any,
    settings: Any,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Download all cloud artifacts to local filesystem."""
    if not service.enabled:
        service.logger.info("Cloud storage sync disabled, skipping download")
        return {}

    service.logger.info("Starting download from cloud storage...")
    results: dict[str, Any] = {}

    try:
        if progress_callback:
            progress_callback("Downloading database...")

        db_success = bool(service.gcs_manager.download_file(
            gcs_path="data/warehouse.duckdb",
            local_path=settings.DEFAULT_DB_PATH,
        ))
        results["database"] = db_success

        if progress_callback:
            progress_callback("Downloading Parquet files...")

        raw_parquet_results = service.gcs_manager.download_directory(
            gcs_prefix="data/parquet",
            local_dir=settings.PARQUET_DIR,
            include_patterns=["*.parquet"],
        )
        parquet_results = (
            {str(k): bool(v) for k, v in raw_parquet_results.items()}
            if isinstance(raw_parquet_results, dict)
            else {}
        )
        results["parquet_files"] = parquet_results
        results["parquet_count"] = len([v for v in parquet_results.values() if v])

        if progress_callback:
            progress_callback("Downloading faction coalition data...")

        csv_success = bool(service.gcs_manager.download_file(
            gcs_path="data/faction_coalition_status.csv",
            local_path=settings.FACTION_COALITION_STATUS_FILE,
        ))
        results["faction_csv"] = csv_success

        if progress_callback:
            progress_callback("Downloading resume state...")

        resume_success = bool(service.gcs_manager.download_file(
            gcs_path="data/.resume_state.json",
            local_path=settings.RESUME_STATE_FILE,
        ))
        results["resume_state"] = resume_success

        total_success = sum(
            [
                db_success,
                len([v for v in parquet_results.values() if v]) > 0,
                csv_success,
                resume_success,
            ]
        )
        service.logger.info(f"Download complete: {total_success}/4 categories successful")
        if progress_callback:
            progress_callback(f"Download complete: {total_success}/4 categories synced")

        return results

    except Exception as exc:
        service.logger.error(f"Error during download: {exc}", exc_info=True)
        return {"error": str(exc)}


def upload_all_data(
    service: Any,
    settings: Any,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Upload local artifacts to cloud storage."""
    if not service.enabled:
        service.logger.info("Cloud storage sync disabled, skipping upload")
        return {}

    service.logger.info("Starting upload to cloud storage...")
    results: dict[str, Any] = {}

    try:
        if progress_callback:
            progress_callback("Uploading database...")

        db_success = bool(service.gcs_manager.upload_file(
            local_path=settings.DEFAULT_DB_PATH,
            gcs_path="data/warehouse.duckdb",
        ))
        results["database"] = db_success

        if progress_callback:
            progress_callback("Uploading Parquet files...")

        raw_parquet_results = service.gcs_manager.upload_directory(
            local_dir=settings.PARQUET_DIR,
            gcs_prefix="data/parquet",
            include_patterns=["*.parquet"],
        )
        parquet_results = (
            {str(k): bool(v) for k, v in raw_parquet_results.items()}
            if isinstance(raw_parquet_results, dict)
            else {}
        )
        results["parquet_files"] = parquet_results
        results["parquet_count"] = len([v for v in parquet_results.values() if v])

        if progress_callback:
            progress_callback("Uploading faction coalition data...")

        csv_success = False
        if settings.FACTION_COALITION_STATUS_FILE.exists():
            csv_success = bool(service.gcs_manager.upload_file(
                local_path=settings.FACTION_COALITION_STATUS_FILE,
                gcs_path="data/faction_coalition_status.csv",
            ))
        results["faction_csv"] = csv_success

        if progress_callback:
            progress_callback("Uploading resume state...")

        resume_success = False
        if settings.RESUME_STATE_FILE.exists():
            resume_success = bool(service.gcs_manager.upload_file(
                local_path=settings.RESUME_STATE_FILE,
                gcs_path="data/.resume_state.json",
            ))
        results["resume_state"] = resume_success

        total_success = sum(
            [
                db_success,
                len([v for v in parquet_results.values() if v]) > 0,
                csv_success,
                resume_success,
            ]
        )
        service.logger.info(f"Upload complete: {total_success}/4 categories successful")
        if progress_callback:
            progress_callback(f"Upload complete: {total_success}/4 categories synced")

        return results

    except Exception as exc:
        service.logger.error(f"Error during upload: {exc}", exc_info=True)
        return {"error": str(exc)}


def upload_database_only(service: Any, settings: Any) -> bool:
    """Upload only the DuckDB database file."""
    if not service.enabled:
        service.logger.debug("Cloud storage sync disabled, skipping database upload")
        return False

    try:
        success = bool(service.gcs_manager.upload_file(
            local_path=settings.DEFAULT_DB_PATH,
            gcs_path="data/warehouse.duckdb",
        ))
        if success:
            service.logger.info("Database synced to cloud storage")
        return success
    except Exception as exc:
        service.logger.warning(f"Failed to sync database to cloud: {exc}")
        return False

"""Storage synchronization facade for cloud persistence."""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from config.settings import Settings
from data.services.sync_types import SyncDirection, SyncMetadata, SyncReport
from data.storage.cloud_storage import (
    CloudStorageManager,
    create_gcs_manager_from_streamlit_secrets,
)
from data.storage.credential_resolver import GCSCredentialResolver

from . import storage_sync_metadata_ops as metadata_ops
from . import storage_sync_startup_ops as startup_ops
from . import storage_sync_transfer_ops as transfer_ops


class StorageSyncService:
    """Service for synchronizing local data with cloud storage."""

    def __init__(
        self,
        gcs_manager: Optional[CloudStorageManager] = None,
        logger_obj: Optional[logging.Logger] = None,
    ):
        self.logger = logger_obj or logging.getLogger(__name__)
        self.gcs_manager = gcs_manager or self._create_gcs_manager()
        self.enabled = self.gcs_manager is not None

        if self.enabled:
            self.logger.info("Storage sync service initialized with GCS")
        else:
            self.logger.info("Storage sync service initialized without GCS (disabled)")

    def _create_gcs_manager(self) -> Optional[CloudStorageManager]:
        """Resolve and construct GCS manager from supported credential sources."""
        manager = create_gcs_manager_from_streamlit_secrets(self.logger)
        if manager:
            return manager

        credentials, bucket_name = GCSCredentialResolver.resolve(self.logger)
        if credentials and bucket_name:
            try:
                return CloudStorageManager(
                    bucket_name=bucket_name,
                    credentials_dict=credentials,
                    logger_obj=self.logger,
                )
            except Exception as exc:
                self.logger.warning(
                    f"Failed to create GCS manager from resolved credentials: {exc}"
                )

        return None

    def is_enabled(self) -> bool:
        """Check if cloud storage sync is enabled."""
        return self.enabled

    def download_all_data(
        self,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> dict[str, Any]:
        """Download all cloud artifacts to local storage."""
        return transfer_ops.download_all_data(self, Settings, progress_callback)

    def upload_all_data(
        self,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> dict[str, Any]:
        """Upload all local artifacts to cloud storage."""
        return transfer_ops.upload_all_data(self, Settings, progress_callback)

    def upload_database_only(self) -> bool:
        """Upload only the DuckDB database file."""
        return transfer_ops.upload_database_only(self, Settings)

    def sync_after_refresh(
        self,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> bool:
        """Upload after a successful local refresh."""
        return self.sync_after_refresh_report(progress_callback).success

    def sync_after_refresh_report(
        self,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> SyncReport:
        """Return typed report for post-refresh upload."""
        if not self.enabled:
            return SyncReport(
                success=True,
                direction=SyncDirection.NONE,
                message="Cloud storage sync disabled",
            )

        results = self.upload_all_data(progress_callback)
        if "error" in results:
            return SyncReport(
                success=False,
                direction=SyncDirection.LOCAL_TO_CLOUD,
                message=str(results["error"]),
                details=results,
            )

        return SyncReport(
            success=True,
            direction=SyncDirection.LOCAL_TO_CLOUD,
            message="Upload completed",
            details=results,
        )

    def get_sync_metadata(self) -> SyncMetadata:
        """Collect typed metadata for sync decisions."""
        return metadata_ops.get_sync_metadata(self, Settings)

    def check_cloud_data_exists(self) -> bool:
        """Check if primary database file exists in cloud storage."""
        return metadata_ops.check_cloud_data_exists(self)

    def get_cloud_file_info(self) -> dict[str, Any]:
        """Get cloud artifact metadata summary."""
        return metadata_ops.get_cloud_file_info(self)

    def get_local_last_modified(self):
        """Get local DB last-modified timestamp."""
        return metadata_ops.get_local_last_modified(self, Settings)

    def update_last_modified(self) -> bool:
        """Update local DB last-modified marker."""
        return metadata_ops.update_last_modified(self, Settings)

    def get_cloud_last_modified(self):
        """Get cloud DB last-modified timestamp."""
        return metadata_ops.get_cloud_last_modified(self)

    def compare_freshness(self) -> str:
        """Compare local vs cloud data freshness."""
        return metadata_ops.compare_freshness(self, Settings)

    def _backup_local_database(self):
        """Create local DB backup before overwrite."""
        return startup_ops.backup_local_database(self, Settings)

    def smart_sync_on_startup(
        self,
        force_download: bool = False,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> bool:
        """Intelligently sync local data from cloud at startup."""
        return startup_ops.smart_sync_on_startup(
            self,
            Settings,
            force_download=force_download,
            progress_callback=progress_callback,
        )

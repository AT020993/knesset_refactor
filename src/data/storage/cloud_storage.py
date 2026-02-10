"""Google Cloud Storage manager for persistent data storage."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

from . import cloud_storage_ops

try:
    from google.cloud import storage
    from google.oauth2 import service_account

    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    storage = None
    service_account = None


class CloudStorageManager:
    """Manage upload/download operations with Google Cloud Storage."""

    REQUIRED_CREDENTIAL_FIELDS = {
        "type",
        "project_id",
        "private_key",
        "client_email",
    }

    def __init__(
        self,
        bucket_name: str,
        credentials_dict: Optional[dict[str, Any]] = None,
        credentials_path: Optional[Path] = None,
        logger_obj: Optional[logging.Logger] = None,
    ):
        self.logger = logger_obj or logging.getLogger(__name__)
        cloud_storage_ops.initialize_manager(
            self,
            bucket_name=bucket_name,
            credentials_dict=credentials_dict,
            credentials_path=credentials_path,
            gcs_available=GCS_AVAILABLE,
            storage_module=storage,
            service_account_module=service_account,
            required_fields=self.REQUIRED_CREDENTIAL_FIELDS,
        )

    def _validate_credentials_dict(self, credentials_dict: dict[str, Any]) -> None:
        """Validate credential fields for service account config."""
        cloud_storage_ops.validate_credentials_dict(
            credentials_dict,
            self.REQUIRED_CREDENTIAL_FIELDS,
        )

    def upload_file(self, local_path: Path, gcs_path: str) -> bool:
        return cloud_storage_ops.upload_file(self, local_path, gcs_path)

    def download_file(self, gcs_path: str, local_path: Path) -> bool:
        return cloud_storage_ops.download_file(self, gcs_path, local_path)

    def file_exists(self, gcs_path: str) -> bool:
        return cloud_storage_ops.file_exists(self, gcs_path)

    def upload_directory(
        self,
        local_dir: Path,
        gcs_prefix: str = "",
        include_patterns: Optional[list[str]] = None,
    ) -> dict[str, bool]:
        return cloud_storage_ops.upload_directory(
            self,
            local_dir,
            gcs_prefix,
            include_patterns,
        )

    def download_directory(
        self,
        gcs_prefix: str,
        local_dir: Path,
        include_patterns: Optional[list[str]] = None,
    ) -> dict[str, bool]:
        return cloud_storage_ops.download_directory(
            self,
            gcs_prefix,
            local_dir,
            include_patterns,
        )

    def list_files(self, prefix: str = "") -> list[str]:
        return cloud_storage_ops.list_files(self, prefix)

    def delete_file(self, gcs_path: str) -> bool:
        return cloud_storage_ops.delete_file(self, gcs_path)

    def get_file_metadata(self, gcs_path: str) -> Optional[dict[str, Any]]:
        return cloud_storage_ops.get_file_metadata(self, gcs_path)


def create_gcs_manager_from_config(
    config: dict[str, Any],
    logger_obj: Optional[logging.Logger] = None,
) -> Optional[CloudStorageManager]:
    """Create CloudStorageManager from generic configuration dictionary."""
    return cloud_storage_ops.create_manager_from_config(
        CloudStorageManager,
        config,
        logger_obj,
    )


def create_gcs_manager_from_streamlit_secrets(
    logger_obj: Optional[logging.Logger] = None,
) -> Optional[CloudStorageManager]:
    """Create CloudStorageManager from Streamlit secrets."""
    logger = logger_obj or logging.getLogger(__name__)

    try:
        import streamlit as st

        config = cloud_storage_ops.load_streamlit_config(st, logger)
        if not config:
            return None
        return create_gcs_manager_from_config(config, logger)

    except ImportError:
        logger.info("Streamlit not available, skipping GCS initialization")
        return None
    except Exception as exc:
        logger.error(
            f"Error creating GCS manager from Streamlit secrets: {exc}",
            exc_info=True,
        )
        return None

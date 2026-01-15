"""Google Cloud Storage manager for persistent data storage."""

import json
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
import tempfile

try:
    from google.cloud import storage
    from google.oauth2 import service_account
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    storage = None
    service_account = None


class CloudStorageManager:
    """
    Manages upload/download operations with Google Cloud Storage.

    Handles syncing of DuckDB database, Parquet files, CSV files, and state files
    between local storage and GCS bucket.
    """

    def __init__(
        self,
        bucket_name: str,
        credentials_dict: Optional[Dict[str, Any]] = None,
        credentials_path: Optional[Path] = None,
        logger_obj: Optional[logging.Logger] = None
    ):
        """
        Initialize GCS manager.

        Args:
            bucket_name: Name of the GCS bucket
            credentials_dict: Service account credentials as dict (from Streamlit secrets)
            credentials_path: Path to service account JSON file
            logger_obj: Logger instance
        """
        if not GCS_AVAILABLE:
            raise ImportError(
                "google-cloud-storage not installed. "
                "Install with: pip install google-cloud-storage"
            )

        self.bucket_name = bucket_name
        self.logger = logger_obj or logging.getLogger(__name__)

        # Initialize credentials
        if credentials_dict:
            self.credentials = service_account.Credentials.from_service_account_info(
                credentials_dict
            )
        elif credentials_path:
            self.credentials = service_account.Credentials.from_service_account_file(
                str(credentials_path)
            )
        else:
            # Use default credentials (from environment)
            self.credentials = None

        # Initialize client
        self.client = storage.Client(credentials=self.credentials)
        self.bucket = self.client.bucket(bucket_name)

        self.logger.info(f"Initialized CloudStorageManager for bucket: {bucket_name}")

    def upload_file(self, local_path: Path, gcs_path: str) -> bool:
        """
        Upload a single file to GCS.

        Args:
            local_path: Path to local file
            gcs_path: Destination path in GCS bucket

        Returns:
            True if successful, False otherwise
        """
        if not local_path.exists():
            self.logger.warning(f"File not found for upload: {local_path}")
            return False

        try:
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(str(local_path))
            self.logger.info(f"Uploaded {local_path.name} to gs://{self.bucket_name}/{gcs_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error uploading {local_path}: {e}", exc_info=True)
            return False

    def download_file(self, gcs_path: str, local_path: Path) -> bool:
        """
        Download a single file from GCS.

        Args:
            gcs_path: Source path in GCS bucket
            local_path: Destination path on local filesystem

        Returns:
            True if successful, False otherwise
        """
        try:
            blob = self.bucket.blob(gcs_path)

            if not blob.exists():
                self.logger.info(f"File not found in GCS: {gcs_path}")
                return False

            # Ensure parent directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)

            blob.download_to_filename(str(local_path))
            self.logger.info(f"Downloaded gs://{self.bucket_name}/{gcs_path} to {local_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error downloading {gcs_path}: {e}", exc_info=True)
            return False

    def file_exists(self, gcs_path: str) -> bool:
        """
        Check if a file exists in GCS.

        Args:
            gcs_path: Path to check in GCS bucket

        Returns:
            True if file exists, False otherwise
        """
        try:
            blob = self.bucket.blob(gcs_path)
            return blob.exists()
        except Exception as e:
            self.logger.error(f"Error checking file existence {gcs_path}: {e}", exc_info=True)
            return False

    def upload_directory(
        self,
        local_dir: Path,
        gcs_prefix: str = "",
        include_patterns: Optional[List[str]] = None
    ) -> Dict[str, bool]:
        """
        Upload all files from a directory to GCS.

        Args:
            local_dir: Local directory to upload
            gcs_prefix: Prefix for GCS paths (folder name)
            include_patterns: List of file patterns to include (e.g., ['*.parquet'])

        Returns:
            Dictionary mapping local file paths to upload success status
        """
        if not local_dir.exists():
            self.logger.warning(f"Directory not found for upload: {local_dir}")
            return {}

        results = {}

        # Get files to upload
        if include_patterns:
            files_to_upload = []
            for pattern in include_patterns:
                files_to_upload.extend(local_dir.glob(pattern))
        else:
            files_to_upload = local_dir.iterdir()

        # Upload each file
        for local_file in files_to_upload:
            if local_file.is_file():
                gcs_path = f"{gcs_prefix}/{local_file.name}" if gcs_prefix else local_file.name
                success = self.upload_file(local_file, gcs_path)
                results[str(local_file)] = success

        return results

    def download_directory(
        self,
        gcs_prefix: str,
        local_dir: Path,
        include_patterns: Optional[List[str]] = None
    ) -> Dict[str, bool]:
        """
        Download all files with a given prefix from GCS.

        Args:
            gcs_prefix: Prefix to filter GCS files (folder name)
            local_dir: Local directory to download to
            include_patterns: List of file patterns to include (e.g., ['*.parquet'])

        Returns:
            Dictionary mapping GCS paths to download success status
        """
        results = {}

        try:
            # List all blobs with the prefix
            blobs = self.client.list_blobs(self.bucket_name, prefix=gcs_prefix)

            for blob in blobs:
                # Skip if it's a directory marker
                if blob.name.endswith('/'):
                    continue

                # Apply pattern filtering if specified
                if include_patterns:
                    if not any(blob.name.endswith(pattern.replace('*', '')) for pattern in include_patterns):
                        continue

                # Extract filename from blob name
                filename = blob.name.split('/')[-1]
                local_path = local_dir / filename

                # Download the file
                try:
                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    blob.download_to_filename(str(local_path))
                    self.logger.info(f"Downloaded {blob.name} to {local_path}")
                    results[blob.name] = True
                except Exception as e:
                    self.logger.error(f"Error downloading {blob.name}: {e}", exc_info=True)
                    results[blob.name] = False

        except Exception as e:
            self.logger.error(f"Error listing blobs with prefix {gcs_prefix}: {e}", exc_info=True)

        return results

    def list_files(self, prefix: str = "") -> List[str]:
        """
        List all files in the bucket with optional prefix.

        Args:
            prefix: Optional prefix to filter files

        Returns:
            List of file paths in GCS
        """
        try:
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
            return [blob.name for blob in blobs if not blob.name.endswith('/')]
        except Exception as e:
            self.logger.error(f"Error listing files: {e}", exc_info=True)
            return []

    def delete_file(self, gcs_path: str) -> bool:
        """
        Delete a file from GCS.

        Args:
            gcs_path: Path to file in GCS bucket

        Returns:
            True if successful, False otherwise
        """
        try:
            blob = self.bucket.blob(gcs_path)
            if blob.exists():
                blob.delete()
                self.logger.info(f"Deleted gs://{self.bucket_name}/{gcs_path}")
                return True
            else:
                self.logger.warning(f"File not found for deletion: {gcs_path}")
                return False
        except Exception as e:
            self.logger.error(f"Error deleting {gcs_path}: {e}", exc_info=True)
            return False

    def get_file_metadata(self, gcs_path: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a file in GCS.

        Args:
            gcs_path: Path to file in GCS bucket

        Returns:
            Dictionary with file metadata or None if not found
        """
        try:
            blob = self.bucket.blob(gcs_path)
            if blob.exists():
                blob.reload()
                return {
                    'name': blob.name,
                    'size': blob.size,
                    'updated': blob.updated,
                    'content_type': blob.content_type,
                    'md5_hash': blob.md5_hash
                }
            else:
                return None
        except Exception as e:
            self.logger.error(f"Error getting metadata for {gcs_path}: {e}", exc_info=True)
            return None


def create_gcs_manager_from_config(
    config: Dict[str, Any],
    logger_obj: Optional[logging.Logger] = None
) -> Optional[CloudStorageManager]:
    """
    Create CloudStorageManager from a configuration dictionary.

    This is the framework-agnostic factory function. The config dict should have:
    - 'bucket_name': GCS bucket name (required)
    - 'credentials': Dict with service account credentials (optional)
    - 'credentials_path': Path to credentials JSON file (optional)

    Args:
        config: Configuration dictionary
        logger_obj: Optional logger instance

    Returns:
        CloudStorageManager instance or None if config is invalid
    """
    logger = logger_obj or logging.getLogger(__name__)

    try:
        bucket_name = config.get('bucket_name')
        if not bucket_name:
            logger.info("GCS bucket name not configured")
            return None

        credentials_dict = config.get('credentials')
        credentials_path = config.get('credentials_path')

        if credentials_path:
            credentials_path = Path(credentials_path)

        return CloudStorageManager(
            bucket_name=bucket_name,
            credentials_dict=credentials_dict,
            credentials_path=credentials_path,
            logger_obj=logger
        )

    except Exception as e:
        logger.error(f"Error creating GCS manager from config: {e}", exc_info=True)
        return None


def create_gcs_manager_from_streamlit_secrets(
    logger_obj: Optional[logging.Logger] = None
) -> Optional[CloudStorageManager]:
    """
    Create CloudStorageManager from Streamlit secrets.

    DEPRECATED: This function is being moved to the UI layer.
    Use ui.services.gcs_factory.create_gcs_manager_from_streamlit_secrets() instead.
    Or use create_gcs_manager_from_config() with a generic config dict.

    Expects secrets in this format:
    [gcp_service_account]
    type = "service_account"
    project_id = "..."
    private_key_id = "..."
    private_key = "..."
    client_email = "..."
    ...

    [storage]
    gcs_bucket_name = "bucket-name"

    Args:
        logger_obj: Optional logger instance

    Returns:
        CloudStorageManager instance or None if Streamlit not available or secrets missing
    """
    logger = logger_obj or logging.getLogger(__name__)

    try:
        import streamlit as st

        if not hasattr(st, 'secrets'):
            logger.warning("Streamlit secrets not available")
            return None

        # Check if GCS configuration exists
        if 'storage' not in st.secrets or 'gcs_bucket_name' not in st.secrets['storage']:
            logger.info("GCS bucket name not configured in Streamlit secrets")
            return None

        # Build config dict from Streamlit secrets
        config = {
            'bucket_name': st.secrets['storage']['gcs_bucket_name']
        }

        if 'gcp_service_account' in st.secrets:
            config['credentials'] = dict(st.secrets['gcp_service_account'])

        return create_gcs_manager_from_config(config, logger)

    except ImportError:
        logger.info("Streamlit not available, skipping GCS initialization")
        return None
    except Exception as e:
        logger.error(f"Error creating GCS manager from Streamlit secrets: {e}", exc_info=True)
        return None

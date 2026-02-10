"""Internal operations for CloudStorageManager and factory helpers."""

from __future__ import annotations

import base64
import json
import logging
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar


ManagerT = TypeVar("ManagerT")


def validate_credentials_dict(
    credentials_dict: dict[str, Any],
    required_fields: set[str],
) -> None:
    """Validate credentials dictionary shape."""
    if not credentials_dict:
        raise ValueError("credentials_dict cannot be empty")

    missing = required_fields - set(credentials_dict.keys())
    if missing:
        raise ValueError(
            f"GCS credentials missing required fields: {sorted(missing)}. "
            f"Expected fields: {sorted(required_fields)}"
        )


def initialize_manager(
    manager: Any,
    *,
    bucket_name: str,
    credentials_dict: Optional[dict[str, Any]],
    credentials_path: Optional[Path],
    gcs_available: bool,
    storage_module: Any,
    service_account_module: Any,
    required_fields: set[str],
) -> None:
    """Initialize manager credentials/client/bucket."""
    if not gcs_available:
        raise ImportError(
            "google-cloud-storage not installed. "
            "Install with: pip install google-cloud-storage"
        )

    manager.bucket_name = bucket_name

    if credentials_dict:
        validate_credentials_dict(credentials_dict, required_fields)
        manager.credentials = service_account_module.Credentials.from_service_account_info(
            credentials_dict
        )
    elif credentials_path:
        manager.credentials = service_account_module.Credentials.from_service_account_file(
            str(credentials_path)
        )
    else:
        manager.credentials = None

    manager.client = storage_module.Client(credentials=manager.credentials)
    manager.bucket = manager.client.bucket(bucket_name)
    manager.logger.info(f"Initialized CloudStorageManager for bucket: {bucket_name}")


def upload_file(manager: Any, local_path: Path, gcs_path: str) -> bool:
    """Upload single file."""
    if not local_path.exists():
        manager.logger.warning(f"File not found for upload: {local_path}")
        return False

    try:
        blob = manager.bucket.blob(gcs_path)
        blob.upload_from_filename(str(local_path))
        manager.logger.info(
            f"Uploaded {local_path.name} to gs://{manager.bucket_name}/{gcs_path}"
        )
        return True
    except Exception as exc:
        manager.logger.error(f"Error uploading {local_path}: {exc}", exc_info=True)
        return False


def download_file(manager: Any, gcs_path: str, local_path: Path) -> bool:
    """Download single file."""
    try:
        blob = manager.bucket.blob(gcs_path)
        if not blob.exists():
            manager.logger.info(f"File not found in GCS: {gcs_path}")
            return False

        local_path.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local_path))
        manager.logger.info(
            f"Downloaded gs://{manager.bucket_name}/{gcs_path} to {local_path}"
        )
        return True
    except Exception as exc:
        manager.logger.error(f"Error downloading {gcs_path}: {exc}", exc_info=True)
        return False


def file_exists(manager: Any, gcs_path: str) -> bool:
    """Check file existence."""
    try:
        blob = manager.bucket.blob(gcs_path)
        return bool(blob.exists())
    except Exception as exc:
        manager.logger.error(
            f"Error checking file existence {gcs_path}: {exc}",
            exc_info=True,
        )
        return False


def upload_directory(
    manager: Any,
    local_dir: Path,
    gcs_prefix: str = "",
    include_patterns: Optional[list[str]] = None,
) -> dict[str, bool]:
    """Upload directory contents."""
    if not local_dir.exists():
        manager.logger.warning(f"Directory not found for upload: {local_dir}")
        return {}

    results: dict[str, bool] = {}

    if include_patterns:
        files_to_upload: list[Path] = []
        for pattern in include_patterns:
            files_to_upload.extend(local_dir.glob(pattern))
    else:
        files_to_upload = list(local_dir.iterdir())

    for local_file in files_to_upload:
        if local_file.is_file():
            gcs_path = (
                f"{gcs_prefix}/{local_file.name}" if gcs_prefix else local_file.name
            )
            success = upload_file(manager, local_file, gcs_path)
            results[str(local_file)] = success

    return results


def download_directory(
    manager: Any,
    gcs_prefix: str,
    local_dir: Path,
    include_patterns: Optional[list[str]] = None,
) -> dict[str, bool]:
    """Download directory contents."""
    results: dict[str, bool] = {}

    try:
        blobs = manager.client.list_blobs(manager.bucket_name, prefix=gcs_prefix)
        for blob in blobs:
            if blob.name.endswith("/"):
                continue

            if include_patterns:
                if not any(
                    blob.name.endswith(pattern.replace("*", ""))
                    for pattern in include_patterns
                ):
                    continue

            filename = blob.name.split("/")[-1]
            local_path = local_dir / filename

            try:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                blob.download_to_filename(str(local_path))
                manager.logger.info(f"Downloaded {blob.name} to {local_path}")
                results[blob.name] = True
            except Exception as exc:
                manager.logger.error(
                    f"Error downloading {blob.name}: {exc}",
                    exc_info=True,
                )
                results[blob.name] = False
    except Exception as exc:
        manager.logger.error(
            f"Error listing blobs with prefix {gcs_prefix}: {exc}",
            exc_info=True,
        )

    return results


def list_files(manager: Any, prefix: str = "") -> list[str]:
    """List files in bucket."""
    try:
        blobs = manager.client.list_blobs(manager.bucket_name, prefix=prefix)
        return [blob.name for blob in blobs if not blob.name.endswith("/")]
    except Exception as exc:
        manager.logger.error(f"Error listing files: {exc}", exc_info=True)
        return []


def delete_file(manager: Any, gcs_path: str) -> bool:
    """Delete file in bucket."""
    try:
        blob = manager.bucket.blob(gcs_path)
        if blob.exists():
            blob.delete()
            manager.logger.info(f"Deleted gs://{manager.bucket_name}/{gcs_path}")
            return True
        manager.logger.warning(f"File not found for deletion: {gcs_path}")
        return False
    except Exception as exc:
        manager.logger.error(f"Error deleting {gcs_path}: {exc}", exc_info=True)
        return False


def get_file_metadata(manager: Any, gcs_path: str) -> Optional[dict[str, Any]]:
    """Get file metadata from bucket."""
    try:
        blob = manager.bucket.blob(gcs_path)
        if blob.exists():
            blob.reload()
            return {
                "name": blob.name,
                "size": blob.size,
                "updated": blob.updated,
                "content_type": blob.content_type,
                "md5_hash": blob.md5_hash,
            }
        return None
    except Exception as exc:
        manager.logger.error(
            f"Error getting metadata for {gcs_path}: {exc}",
            exc_info=True,
        )
        return None


def create_manager_from_config(
    manager_cls: Callable[..., ManagerT],
    config: dict[str, Any],
    logger_obj: Optional[logging.Logger] = None,
) -> ManagerT | None:
    """Create manager instance from generic config dict."""
    logger = logger_obj or logging.getLogger(__name__)
    try:
        bucket_name = config.get("bucket_name")
        if not bucket_name:
            logger.info("GCS bucket name not configured")
            return None

        credentials_dict = config.get("credentials")
        credentials_path = config.get("credentials_path")
        if credentials_path:
            credentials_path = Path(credentials_path)

        return manager_cls(
            bucket_name=bucket_name,
            credentials_dict=credentials_dict,
            credentials_path=credentials_path,
            logger_obj=logger,
        )
    except Exception as exc:
        logger.error(f"Error creating GCS manager from config: {exc}", exc_info=True)
        return None


def load_streamlit_config(st_obj: Any, logger: logging.Logger) -> dict[str, Any] | None:
    """Build generic cloud config from Streamlit secrets object."""
    if not hasattr(st_obj, "secrets"):
        logger.warning("Streamlit secrets not available")
        return None

    if "storage" not in st_obj.secrets or "gcs_bucket_name" not in st_obj.secrets["storage"]:
        logger.info("GCS bucket name not configured in Streamlit secrets")
        return None

    config: dict[str, Any] = {
        "bucket_name": st_obj.secrets["storage"]["gcs_bucket_name"],
    }

    if "gcp_service_account" not in st_obj.secrets:
        return config

    gcp_secrets = st_obj.secrets["gcp_service_account"]
    if "credentials_base64" in gcp_secrets:
        try:
            decoded = base64.b64decode(gcp_secrets["credentials_base64"]).decode("utf-8")
            config["credentials"] = json.loads(decoded)
            logger.info("Loaded GCP credentials from credentials_base64 field")
        except Exception as exc:
            logger.error(f"Failed to parse credentials_base64: {exc}")
            return None
    elif "credentials_json" in gcp_secrets:
        try:
            config["credentials"] = json.loads(gcp_secrets["credentials_json"])
            logger.info("Loaded GCP credentials from credentials_json field")
        except json.JSONDecodeError as exc:
            logger.error(f"Failed to parse credentials_json: {exc}")
            return None
    elif "client_email" in gcp_secrets and "private_key" in gcp_secrets:
        config["credentials"] = dict(gcp_secrets)
        logger.info("Loaded GCP credentials from direct fields")
    else:
        available_keys = list(gcp_secrets.keys()) if hasattr(gcp_secrets, "keys") else []
        logger.warning(
            "GCP credentials incomplete. Found keys: %s. Need "
            "'credentials_base64', 'credentials_json', or direct fields.",
            available_keys,
        )

    return config

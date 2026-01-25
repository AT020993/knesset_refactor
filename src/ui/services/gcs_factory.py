"""
GCS factory functions for Streamlit UI layer.

This module provides Streamlit-specific factory functions for creating
CloudStorageManager instances from Streamlit secrets.
"""

import logging
from typing import Optional

import streamlit as st

from data.storage.cloud_storage import CloudStorageManager, create_gcs_manager_from_config


def create_gcs_manager_from_streamlit_secrets(
    logger_obj: Optional[logging.Logger] = None
) -> Optional[CloudStorageManager]:
    """
    Create CloudStorageManager from Streamlit secrets.

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
        CloudStorageManager instance or None if secrets missing
    """
    logger = logger_obj or logging.getLogger(__name__)

    try:
        # Check if Streamlit secrets are available
        if not hasattr(st, 'secrets'):
            logger.info("☁️ Cloud storage: DISABLED (not running in Streamlit)")
            return None

        # Check if bucket name is configured
        storage_secrets = st.secrets.get('storage', {})
        bucket_name = storage_secrets.get('gcs_bucket_name')
        if not bucket_name:
            logger.info("☁️ Cloud storage: DISABLED (no bucket configured in secrets)")
            return None

        # Check for any credential format
        gcp_secrets = st.secrets.get('gcp_service_account', {})
        has_credentials = any([
            gcp_secrets.get('credentials_base64'),
            gcp_secrets.get('credentials_json'),
            gcp_secrets.get('client_email'),  # Direct fields
        ])

        if not has_credentials:
            logger.info("☁️ Cloud storage: DISABLED (no GCP credentials in secrets)")
            return None

        # Build config dict from Streamlit secrets
        config = {
            'bucket_name': bucket_name,
            'credentials': dict(gcp_secrets)
        }

        # Try to create the manager
        manager = create_gcs_manager_from_config(config, logger)
        if manager is not None:
            logger.info(f"☁️ Cloud storage: ENABLED (bucket: {bucket_name})")
        else:
            logger.warning("☁️ Cloud storage: DISABLED (manager creation failed)")
        return manager

    except Exception as e:
        logger.warning(f"☁️ Cloud storage: DISABLED (error: {e})")
        return None


def get_gcs_config_from_streamlit() -> dict:
    """
    Extract GCS configuration from Streamlit secrets as a dict.

    This is useful when you need the raw config without creating a manager.

    Returns:
        Configuration dict suitable for create_gcs_manager_from_config()
    """
    config = {}

    try:
        if hasattr(st, 'secrets'):
            if 'storage' in st.secrets and 'gcs_bucket_name' in st.secrets['storage']:
                config['bucket_name'] = st.secrets['storage']['gcs_bucket_name']

            if 'gcp_service_account' in st.secrets:
                config['credentials'] = dict(st.secrets['gcp_service_account'])
    except Exception:
        pass

    return config

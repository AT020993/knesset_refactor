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

    except Exception as e:
        logger.error(f"Error creating GCS manager from Streamlit secrets: {e}", exc_info=True)
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

"""GCS Credential Resolver - resolves credentials from multiple sources.

Provides a unified way to load Google Cloud Storage credentials from:
1. Streamlit secrets (for cloud deployment)
2. Environment variables (for local development)
3. .env file (for local development convenience)
"""

import base64
import json
import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any, Tuple


class GCSCredentialResolver:
    """Resolve GCS credentials from multiple sources.

    Priority order:
    1. Streamlit secrets (for cloud deployment)
    2. GOOGLE_APPLICATION_CREDENTIALS environment variable
    3. .env file with GCS_CREDENTIALS_PATH
    """

    @classmethod
    def resolve(
        cls,
        logger: Optional[logging.Logger] = None
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Try each credential source in priority order.

        Args:
            logger: Optional logger instance

        Returns:
            Tuple of (credentials_dict, bucket_name) or (None, None) if no credentials found
        """
        log = logger or logging.getLogger(__name__)

        # Priority 1: Streamlit secrets (for cloud deployment)
        credentials, bucket_name = cls._from_streamlit_secrets(log)
        if credentials and bucket_name:
            log.info("Resolved GCS credentials from Streamlit secrets")
            return credentials, bucket_name

        # Priority 2: Environment variable GOOGLE_APPLICATION_CREDENTIALS
        credentials, bucket_name = cls._from_env_var(log)
        if credentials and bucket_name:
            log.info("Resolved GCS credentials from environment variables")
            return credentials, bucket_name

        # Priority 3: .env file
        credentials, bucket_name = cls._from_dotenv(log)
        if credentials and bucket_name:
            log.info("Resolved GCS credentials from .env file")
            return credentials, bucket_name

        log.debug("No GCS credentials found from any source")
        return None, None

    @classmethod
    def _from_streamlit_secrets(
        cls,
        logger: logging.Logger
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Try to load credentials from Streamlit secrets."""
        try:
            import streamlit as st

            if not hasattr(st, 'secrets'):
                return None, None

            # Check if GCS configuration exists
            if 'storage' not in st.secrets or 'gcs_bucket_name' not in st.secrets['storage']:
                return None, None

            bucket_name = st.secrets['storage']['gcs_bucket_name']

            # Try multiple formats for GCP credentials
            if 'gcp_service_account' not in st.secrets:
                return None, None

            gcp_secrets = st.secrets['gcp_service_account']

            # Format 1: Base64 encoded JSON
            if 'credentials_base64' in gcp_secrets:
                try:
                    decoded = base64.b64decode(gcp_secrets['credentials_base64']).decode('utf-8')
                    credentials = json.loads(decoded)
                    return credentials, bucket_name
                except Exception as e:
                    logger.warning(f"Failed to decode credentials_base64: {e}")

            # Format 2: JSON string
            if 'credentials_json' in gcp_secrets:
                try:
                    credentials = json.loads(gcp_secrets['credentials_json'])
                    return credentials, bucket_name
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse credentials_json: {e}")

            # Format 3: Direct fields
            if 'client_email' in gcp_secrets and 'private_key' in gcp_secrets:
                credentials = dict(gcp_secrets)
                return credentials, bucket_name

            return None, None

        except ImportError:
            return None, None
        except Exception as e:
            logger.debug(f"Error loading from Streamlit secrets: {e}")
            return None, None

    @classmethod
    def _from_env_var(
        cls,
        logger: logging.Logger
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Try to load credentials from environment variables."""
        try:
            credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            bucket_name = os.environ.get('GCS_BUCKET_NAME')

            if not credentials_path or not bucket_name:
                return None, None

            # Load credentials from file
            creds_path = Path(credentials_path)
            if not creds_path.exists():
                logger.warning(f"Credentials file not found: {credentials_path}")
                return None, None

            with open(creds_path, 'r') as f:
                credentials = json.load(f)

            return credentials, bucket_name

        except Exception as e:
            logger.debug(f"Error loading from environment variables: {e}")
            return None, None

    @classmethod
    def _from_dotenv(
        cls,
        logger: logging.Logger
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Try to load credentials from .env file."""
        try:
            # Find .env file (check cwd and project root)
            dotenv_paths = [
                Path('.env'),
                Path(__file__).parent.parent.parent.parent / '.env',  # Project root
            ]

            dotenv_path = None
            for path in dotenv_paths:
                if path.exists():
                    dotenv_path = path
                    break

            if not dotenv_path:
                return None, None

            # Parse .env file manually (avoid adding python-dotenv dependency)
            env_vars = {}
            with open(dotenv_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' in line:
                        key, value = line.split('=', 1)
                        # Remove quotes if present
                        value = value.strip().strip('"').strip("'")
                        env_vars[key.strip()] = value

            credentials_path = env_vars.get('GCS_CREDENTIALS_PATH') or env_vars.get('GOOGLE_APPLICATION_CREDENTIALS')
            bucket_name = env_vars.get('GCS_BUCKET_NAME')

            if not credentials_path or not bucket_name:
                return None, None

            # Handle relative paths
            creds_path = Path(credentials_path)
            if not creds_path.is_absolute():
                creds_path = dotenv_path.parent / creds_path

            if not creds_path.exists():
                logger.warning(f"Credentials file from .env not found: {creds_path}")
                return None, None

            with open(creds_path, 'r') as f:
                credentials = json.load(f)

            return credentials, bucket_name

        except Exception as e:
            logger.debug(f"Error loading from .env file: {e}")
            return None, None

    @classmethod
    def get_bucket_name(cls, logger: Optional[logging.Logger] = None) -> Optional[str]:
        """Get just the bucket name without loading full credentials.

        Useful for checking if sync is configured without loading credentials.
        """
        log = logger or logging.getLogger(__name__)

        # Check Streamlit secrets
        try:
            import streamlit as st
            if hasattr(st, 'secrets') and 'storage' in st.secrets:
                bucket = st.secrets['storage'].get('gcs_bucket_name')
                if bucket:
                    return str(bucket)
        except ImportError:
            pass
        except Exception:
            pass

        # Check environment
        bucket = os.environ.get('GCS_BUCKET_NAME')
        if bucket:
            return bucket

        # Check .env
        dotenv_paths = [
            Path('.env'),
            Path(__file__).parent.parent.parent.parent / '.env',
        ]

        for path in dotenv_paths:
            if path.exists():
                try:
                    with open(path, 'r') as f:
                        for line in f:
                            line = line.strip()
                            if line.startswith('GCS_BUCKET_NAME='):
                                value = line.split('=', 1)[1].strip().strip('"').strip("'")
                                if value:
                                    return value
                except Exception:
                    pass

        return None

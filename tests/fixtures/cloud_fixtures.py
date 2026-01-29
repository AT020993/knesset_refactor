# tests/fixtures/cloud_fixtures.py
"""
Fixtures for cloud compatibility testing.

Provides mocked Streamlit secrets, GCS clients, and environment simulation.
These fixtures enable testing of code that runs differently in local
development vs Streamlit Cloud environments.
"""

import base64
import json
import os
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch
import pytest


# Sample GCS credentials for testing (not real credentials)
MOCK_GCS_CREDENTIALS = {
    "type": "service_account",
    "project_id": "test-project",
    "private_key_id": "key123",
    "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIBOQIBAAJAtest\n-----END RSA PRIVATE KEY-----\n",
    "client_email": "test@test-project.iam.gserviceaccount.com",
    "client_id": "123456789",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}

MOCK_GCS_CREDENTIALS_BASE64 = base64.b64encode(
    json.dumps(MOCK_GCS_CREDENTIALS).encode()
).decode()


@pytest.fixture
def mock_streamlit_secrets():
    """
    Mock st.secrets with configurable values.

    Returns a factory function that creates a mock secrets object.
    This allows tests to simulate different Streamlit Cloud configurations.

    Usage:
        def test_with_gcs(mock_streamlit_secrets):
            secrets = mock_streamlit_secrets(gcs_enabled=True)
            # Test code that uses secrets

        def test_without_gcs(mock_streamlit_secrets):
            secrets = mock_streamlit_secrets(gcs_enabled=False)
            # Test graceful degradation
    """
    def _create_secrets(
        gcs_enabled: bool = True,
        use_base64: bool = True,
        bucket_name: str = "test-bucket",
        cap_enabled: bool = True,
    ) -> MagicMock:
        """
        Create a mock secrets object.

        Args:
            gcs_enabled: Whether GCS storage is configured
            use_base64: Whether to use base64-encoded credentials (Streamlit Cloud pattern)
            bucket_name: GCS bucket name
            cap_enabled: Whether CAP annotation is enabled

        Returns:
            MagicMock configured to behave like st.secrets
        """
        secrets = MagicMock()

        # Build the secrets structure
        secrets_data = {
            "storage": {
                "gcs_bucket_name": bucket_name if gcs_enabled else "",
                "enable_cloud_storage": gcs_enabled,
            },
            "gcp_service_account": {
                "credentials_base64": MOCK_GCS_CREDENTIALS_BASE64 if use_base64 else None,
                "type": None if use_base64 else MOCK_GCS_CREDENTIALS["type"],
                "project_id": None if use_base64 else MOCK_GCS_CREDENTIALS["project_id"],
                "private_key": None if use_base64 else MOCK_GCS_CREDENTIALS["private_key"],
                "client_email": None if use_base64 else MOCK_GCS_CREDENTIALS["client_email"],
            },
            "cap_annotation": {
                "enabled": cap_enabled,
                "bootstrap_admin_username": "admin",
                "bootstrap_admin_display_name": "Test Admin",
                "bootstrap_admin_password": "test-password",
            },
        }

        # Configure .get() method
        def mock_get(key, default=None):
            return secrets_data.get(key, default)

        secrets.get = mock_get

        # Allow dict-style access via __getitem__
        def mock_getitem(key):
            if key in secrets_data:
                # Return a MagicMock that also supports nested access
                section = MagicMock()
                section_data = secrets_data[key]
                section.get = lambda k, d=None: section_data.get(k, d)
                section.__getitem__ = lambda k: section_data[k]
                section.__contains__ = lambda k: k in section_data
                return section
            raise KeyError(key)

        secrets.__getitem__ = mock_getitem

        # Allow 'in' checks
        secrets.__contains__ = lambda key: key in secrets_data

        return secrets

    return _create_secrets


@pytest.fixture
def mock_gcs_client():
    """
    Mock Google Cloud Storage client.

    Provides a fully mocked GCS client with bucket and blob operations.
    Useful for testing upload/download operations without actual GCS calls.

    Usage:
        def test_upload(mock_gcs_client):
            client = mock_gcs_client
            # client.bucket().blob().upload_from_filename() is mocked
    """
    client = MagicMock()
    bucket = MagicMock()
    blob = MagicMock()

    # Setup chain: client.bucket() -> bucket.blob() -> blob
    client.bucket.return_value = bucket
    bucket.blob.return_value = blob
    bucket.list_blobs.return_value = []

    # Blob operations
    blob.exists.return_value = True
    blob.download_to_filename = MagicMock()
    blob.upload_from_filename = MagicMock()
    blob.size = 1024  # 1KB mock file
    blob.updated = "2024-01-01T00:00:00Z"

    return client


@pytest.fixture
def streamlit_context():
    """
    Simulate Streamlit runtime context with running event loop.

    Use this to test code that behaves differently in Streamlit vs CLI.
    Streamlit uses Tornado with its own event loop, which means asyncio.run()
    fails with "This event loop is already running".

    Usage:
        def test_async_in_streamlit(streamlit_context):
            with streamlit_context():
                # asyncio.get_running_loop() will return a loop
                # Code should use thread isolation pattern
    """
    import asyncio

    class StreamlitContext:
        """Context manager that simulates Streamlit's Tornado event loop."""

        def __init__(self):
            self.loop = None
            self._original_get_running_loop = asyncio.get_running_loop

        def __enter__(self):
            # Create a running loop to simulate Streamlit's Tornado context
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

            # Patch get_running_loop to return our loop
            def mock_get_running_loop():
                return self.loop

            asyncio.get_running_loop = mock_get_running_loop
            return self

        def __exit__(self, *args):
            asyncio.get_running_loop = self._original_get_running_loop
            if self.loop:
                self.loop.close()
                asyncio.set_event_loop(None)

    return StreamlitContext


@pytest.fixture
def cli_context():
    """
    Simulate CLI context with no running event loop.

    Use this to test code that behaves differently in CLI vs Streamlit.
    In CLI context, asyncio.run() works normally.

    Usage:
        def test_async_in_cli(cli_context):
            with cli_context():
                # asyncio.get_running_loop() will raise RuntimeError
                # Code can use asyncio.run() directly
    """
    import asyncio

    class CLIContext:
        """Context manager that ensures no running event loop (CLI mode)."""

        def __enter__(self):
            # Ensure no running loop
            try:
                asyncio.get_running_loop()
                raise RuntimeError("Unexpected running loop in CLI context")
            except RuntimeError:
                pass  # Expected - no running loop
            return self

        def __exit__(self, *args):
            pass

    return CLIContext


@pytest.fixture
def mock_session_state():
    """
    Mock Streamlit session state with dict-like behavior.

    Simulates st.session_state for testing state persistence.
    Supports attribute access, dict access, and 'in' checks.

    Usage:
        def test_state_persistence(mock_session_state):
            state = mock_session_state
            state.user_id = 123
            assert state.user_id == 123
            assert "user_id" in state
    """
    state = {}

    class MockSessionState:
        """Mock implementation of Streamlit's session state."""

        def __getattr__(self, key):
            if key.startswith("_"):
                raise AttributeError(key)
            return state.get(key)

        def __setattr__(self, key, value):
            state[key] = value

        def __contains__(self, key):
            return key in state

        def get(self, key, default=None):
            return state.get(key, default)

        def __getitem__(self, key):
            return state[key]

        def __setitem__(self, key, value):
            state[key] = value

        def __delitem__(self, key):
            del state[key]

        def clear(self):
            state.clear()

        def keys(self):
            return state.keys()

        def values(self):
            return state.values()

        def items(self):
            return state.items()

        def _get_state(self):
            """Get a copy of the internal state for assertions."""
            return state.copy()

        def _reset(self):
            """Reset the state (useful between tests)."""
            state.clear()

    return MockSessionState()


@pytest.fixture
def resource_limited_environment(monkeypatch):
    """
    Simulate Streamlit Cloud free tier resource constraints.

    The free tier has approximately:
    - Memory limit: ~1GB (simulated via DataFrame size checks)
    - Shared CPU (simulated via artificial tracking)

    This fixture patches pandas DataFrame creation to track memory usage
    and raise MemoryError if the simulated limit is exceeded.

    Usage:
        def test_memory_limits(resource_limited_environment):
            env = resource_limited_environment
            # Creating large DataFrames will raise MemoryError
            env["reset"]()  # Reset memory counter between operations
    """
    import pandas as pd

    original_dataframe_init = pd.DataFrame.__init__

    # Track total memory usage
    total_memory_bytes = [0]
    MEMORY_LIMIT_BYTES = 500 * 1024 * 1024  # 500MB for safety margin

    def limited_dataframe_init(self, *args, **kwargs):
        original_dataframe_init(self, *args, **kwargs)
        # Estimate memory
        try:
            mem = self.memory_usage(deep=True).sum()
            total_memory_bytes[0] += mem
            if total_memory_bytes[0] > MEMORY_LIMIT_BYTES:
                raise MemoryError(
                    f"Simulated memory limit exceeded: {total_memory_bytes[0] / 1024 / 1024:.1f}MB"
                )
        except Exception as e:
            if isinstance(e, MemoryError):
                raise
            pass  # Ignore other memory estimation errors

    monkeypatch.setattr(pd.DataFrame, "__init__", limited_dataframe_init)

    return {
        "reset": lambda: total_memory_bytes.__setitem__(0, 0),
        "get_usage": lambda: total_memory_bytes[0],
        "limit_bytes": MEMORY_LIMIT_BYTES,
    }


# Additional helper fixtures


@pytest.fixture
def mock_gcs_manager():
    """
    Mock the GCSManager class used for cloud storage operations.

    Provides a complete mock of GCSManager with all common methods.
    """
    manager = MagicMock()

    # Common methods
    manager.is_available.return_value = True
    manager.download_database.return_value = True
    manager.upload_database.return_value = True
    manager.get_bucket_name.return_value = "test-bucket"
    manager.get_credentials.return_value = MOCK_GCS_CREDENTIALS

    return manager


@pytest.fixture
def mock_storage_sync_service():
    """
    Mock the StorageSyncService for testing sync operations.
    """
    service = MagicMock()

    service.is_enabled.return_value = True
    service.sync_after_refresh.return_value = True
    service.download_from_cloud.return_value = True
    service.upload_to_cloud.return_value = True

    return service


@pytest.fixture
def cloud_environment_vars(monkeypatch):
    """
    Set up environment variables to simulate cloud environment.

    Returns a function to configure different environment scenarios.
    """
    def _setup_env(
        google_credentials_path: Optional[str] = None,
        streamlit_cloud: bool = False,
    ):
        if google_credentials_path:
            monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", google_credentials_path)
        else:
            monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

        if streamlit_cloud:
            # Streamlit Cloud sets these environment variables
            monkeypatch.setenv("STREAMLIT_SHARING_MODE", "streamlit")
            monkeypatch.setenv("HOME", "/home/appuser")
        else:
            monkeypatch.delenv("STREAMLIT_SHARING_MODE", raising=False)

    return _setup_env

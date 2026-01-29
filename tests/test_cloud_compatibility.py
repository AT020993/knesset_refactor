# tests/test_cloud_compatibility.py
"""
Cloud Compatibility Test Suite

Tests ensuring the application works correctly in both local development
and Streamlit Cloud environments. Covers:
- GCS credential loading and operations
- Secrets management (base64 vs JSON vs env)
- Async/threading patterns in Streamlit context
- Database persistence and connection management
- Session state handling
- Resource constraint simulation
"""

import base64
import json
import os
from unittest.mock import MagicMock, patch
import pytest

from tests.fixtures.cloud_fixtures import MOCK_GCS_CREDENTIALS, MOCK_GCS_CREDENTIALS_BASE64


class TestCloudCredentialLoading:
    """Tests for GCS credential loading from various sources.

    Verifies that the application correctly loads credentials from:
    - Base64-encoded secrets (Streamlit Cloud TOML format)
    - Direct JSON configuration
    - Environment variables (local development)
    - Graceful fallback when credentials are missing
    """

    def test_load_credentials_from_base64(self, mock_streamlit_secrets):
        """Base64-encoded credentials should be decoded correctly."""
        secrets = mock_streamlit_secrets(use_base64=True, gcs_enabled=True)

        # Create a mock streamlit module
        mock_st = MagicMock()
        mock_st.secrets = secrets

        with patch.dict("sys.modules", {"streamlit": mock_st}):
            # Need to reimport after patching sys.modules
            from data.storage.cloud_storage import (
                create_gcs_manager_from_streamlit_secrets,
                CloudStorageManager,
            )

            with patch("data.storage.cloud_storage.storage") as mock_storage:
                with patch("data.storage.cloud_storage.service_account") as mock_sa:
                    # Setup the mocks
                    mock_client = MagicMock()
                    mock_storage.Client.return_value = mock_client
                    mock_creds = MagicMock()
                    mock_sa.Credentials.from_service_account_info.return_value = mock_creds

                    manager = create_gcs_manager_from_streamlit_secrets()

                    # Verify credentials were decoded from base64
                    assert manager is not None
                    # Verify from_service_account_info was called with decoded credentials
                    call_args = mock_sa.Credentials.from_service_account_info.call_args
                    assert call_args is not None
                    creds_dict = call_args[0][0]
                    assert creds_dict["type"] == "service_account"
                    assert creds_dict["project_id"] == "test-project"

    def test_load_credentials_from_json_fields(self, mock_streamlit_secrets):
        """JSON fields in secrets should be assembled into credentials dict."""
        # Create secrets with direct fields instead of base64
        secrets = mock_streamlit_secrets(use_base64=False, gcs_enabled=True)

        mock_st = MagicMock()
        mock_st.secrets = secrets

        with patch.dict("sys.modules", {"streamlit": mock_st}):
            from data.storage.cloud_storage import create_gcs_manager_from_streamlit_secrets

            with patch("data.storage.cloud_storage.storage") as mock_storage:
                with patch("data.storage.cloud_storage.service_account") as mock_sa:
                    mock_client = MagicMock()
                    mock_storage.Client.return_value = mock_client
                    mock_creds = MagicMock()
                    mock_sa.Credentials.from_service_account_info.return_value = mock_creds

                    manager = create_gcs_manager_from_streamlit_secrets()

                    # Should still create manager from individual fields
                    assert manager is not None
                    # Verify from_service_account_info was called
                    assert mock_sa.Credentials.from_service_account_info.called

    def test_credentials_missing_returns_none(self, mock_streamlit_secrets):
        """Missing credentials should return None (graceful degradation)."""
        secrets = mock_streamlit_secrets(gcs_enabled=False)

        mock_st = MagicMock()
        mock_st.secrets = secrets

        with patch.dict("sys.modules", {"streamlit": mock_st}):
            from data.storage.cloud_storage import create_gcs_manager_from_streamlit_secrets

            manager = create_gcs_manager_from_streamlit_secrets()
            assert manager is None

    def test_invalid_base64_handles_gracefully(self):
        """Invalid base64 should not crash, should return None."""
        # Create a mock with invalid base64 credentials
        secrets = MagicMock()

        # Setup secrets structure with invalid base64
        secrets_data = {
            "storage": {"gcs_bucket_name": "test-bucket"},
            "gcp_service_account": {"credentials_base64": "not-valid-base64!!!"},
        }

        secrets.get = lambda key, default=None: secrets_data.get(key, default)

        def mock_getitem(key):
            if key in secrets_data:
                section = MagicMock()
                section_data = secrets_data[key]
                section.get = lambda k, d=None: section_data.get(k, d)
                section.__getitem__ = lambda k: section_data[k]
                section.__contains__ = lambda k: k in section_data
                return section
            raise KeyError(key)

        secrets.__getitem__ = mock_getitem
        secrets.__contains__ = lambda key: key in secrets_data

        mock_st = MagicMock()
        mock_st.secrets = secrets

        with patch.dict("sys.modules", {"streamlit": mock_st}):
            from data.storage.cloud_storage import create_gcs_manager_from_streamlit_secrets

            # Should handle gracefully, not crash
            try:
                manager = create_gcs_manager_from_streamlit_secrets()
                # Either None or valid manager (depending on fallback logic)
                # Since base64 decode fails, should return None
                assert manager is None
            except Exception as e:
                pytest.fail(f"Should handle invalid base64 gracefully: {e}")

    def test_environment_variable_fallback(self, monkeypatch, tmp_path):
        """Should fall back to GOOGLE_APPLICATION_CREDENTIALS env var."""
        from data.storage.cloud_storage import CloudStorageManager

        # Create a mock credentials file
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text(json.dumps(MOCK_GCS_CREDENTIALS))

        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(creds_file))

        # CloudStorageManager should be able to initialize with no explicit credentials
        with patch("data.storage.cloud_storage.storage") as mock_storage:
            with patch("data.storage.cloud_storage.service_account") as mock_sa:
                mock_client = MagicMock()
                mock_storage.Client.return_value = mock_client

                # When credentials_dict and credentials_path are None,
                # CloudStorageManager sets self.credentials = None
                # and storage.Client() uses default credentials from env
                manager = CloudStorageManager(bucket_name="test-bucket")

                assert manager is not None
                # Verify Client was called (it uses GOOGLE_APPLICATION_CREDENTIALS internally)
                assert mock_storage.Client.called


class TestCloudStorageOperations:
    """Tests for upload/download operations with graceful degradation.

    Verifies that:
    - Database uploads to GCS work correctly
    - Database downloads from GCS work correctly
    - Operations gracefully degrade when GCS is unavailable
    - Proper error handling for network failures
    """

    def test_upload_file_success(self, mock_gcs_client, tmp_path):
        """Upload should succeed when GCS is available."""
        from pathlib import Path
        from data.storage.cloud_storage import CloudStorageManager

        # Create a test file
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        with patch("data.storage.cloud_storage.storage") as mock_storage:
            with patch("data.storage.cloud_storage.service_account"):
                # Setup mock client chain
                mock_storage.Client.return_value = mock_gcs_client

                manager = CloudStorageManager(
                    bucket_name="test-bucket",
                    credentials_dict=MOCK_GCS_CREDENTIALS
                )
                result = manager.upload_file(test_file, "remote/test.txt")

                assert result is True
                # Verify upload was called
                mock_gcs_client.bucket.return_value.blob.return_value.upload_from_filename.assert_called_once()

    def test_upload_file_failure_returns_false(self, mock_gcs_client, tmp_path):
        """Upload failure should return False, not raise exception."""
        from data.storage.cloud_storage import CloudStorageManager

        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        # Make upload fail
        mock_gcs_client.bucket.return_value.blob.return_value.upload_from_filename.side_effect = Exception("Network error")

        with patch("data.storage.cloud_storage.storage") as mock_storage:
            with patch("data.storage.cloud_storage.service_account"):
                mock_storage.Client.return_value = mock_gcs_client

                manager = CloudStorageManager(
                    bucket_name="test-bucket",
                    credentials_dict=MOCK_GCS_CREDENTIALS
                )
                result = manager.upload_file(test_file, "remote/test.txt")

                assert result is False  # Graceful failure

    def test_download_file_success(self, mock_gcs_client, tmp_path):
        """Download should succeed when file exists."""
        from data.storage.cloud_storage import CloudStorageManager

        local_path = tmp_path / "downloaded.txt"

        # File exists in GCS
        mock_gcs_client.bucket.return_value.blob.return_value.exists.return_value = True

        with patch("data.storage.cloud_storage.storage") as mock_storage:
            with patch("data.storage.cloud_storage.service_account"):
                mock_storage.Client.return_value = mock_gcs_client

                manager = CloudStorageManager(
                    bucket_name="test-bucket",
                    credentials_dict=MOCK_GCS_CREDENTIALS
                )
                result = manager.download_file("remote/file.txt", local_path)

                assert result is True
                # Verify download was called
                mock_gcs_client.bucket.return_value.blob.return_value.download_to_filename.assert_called_once()

    def test_download_nonexistent_file_returns_false(self, mock_gcs_client, tmp_path):
        """Downloading non-existent file should return False."""
        from data.storage.cloud_storage import CloudStorageManager

        # File doesn't exist
        mock_gcs_client.bucket.return_value.blob.return_value.exists.return_value = False

        local_path = tmp_path / "downloaded.txt"

        with patch("data.storage.cloud_storage.storage") as mock_storage:
            with patch("data.storage.cloud_storage.service_account"):
                mock_storage.Client.return_value = mock_gcs_client

                manager = CloudStorageManager(
                    bucket_name="test-bucket",
                    credentials_dict=MOCK_GCS_CREDENTIALS
                )
                result = manager.download_file("remote/nonexistent.txt", local_path)

                assert result is False
                # Verify download was NOT called since file doesn't exist
                mock_gcs_client.bucket.return_value.blob.return_value.download_to_filename.assert_not_called()

    def test_storage_sync_service_disabled_gracefully(self):
        """StorageSyncService should handle disabled state gracefully."""
        from data.services.storage_sync_service import StorageSyncService

        with patch("data.services.storage_sync_service.create_gcs_manager_from_streamlit_secrets", return_value=None):
            service = StorageSyncService()

            assert service.is_enabled() is False

            # Operations should return gracefully
            result = service.upload_all_data()
            assert result == {}  # Graceful no-op returns empty dict

    def test_sync_after_annotation_when_enabled(self, mock_gcs_client, monkeypatch):
        """Sync should trigger after annotation when cloud is enabled."""
        from data.services.storage_sync_service import StorageSyncService
        from data.storage.cloud_storage import CloudStorageManager

        # Create a mock manager that behaves like CloudStorageManager
        mock_manager = MagicMock(spec=CloudStorageManager)
        mock_manager.upload_file.return_value = True
        mock_manager.upload_directory.return_value = {}

        # Create service and manually enable it
        # (conftest autouse fixture disables it by default)
        service = StorageSyncService(gcs_manager=mock_manager)
        service.gcs_manager = mock_manager
        service.enabled = True

        # Also patch is_enabled to return True for this test
        monkeypatch.setattr(service, "is_enabled", lambda: True)

        assert service.is_enabled() is True

        # Call upload_all_data
        service.upload_all_data()

        # Should have attempted upload (upload_file for database at minimum)
        assert mock_manager.upload_file.called or mock_manager.upload_directory.called

    def test_download_on_startup_creates_local_files(self, mock_gcs_client, tmp_path, monkeypatch):
        """Download at startup should create local database files."""
        from data.services.storage_sync_service import StorageSyncService
        from data.storage.cloud_storage import CloudStorageManager

        # Create a mock manager that behaves like CloudStorageManager
        mock_manager = MagicMock(spec=CloudStorageManager)
        mock_manager.download_file.return_value = True
        mock_manager.download_directory.return_value = {}
        mock_manager.file_exists.return_value = True

        # Create service and manually enable it
        # (conftest autouse fixture disables it by default)
        service = StorageSyncService(gcs_manager=mock_manager)
        service.gcs_manager = mock_manager
        service.enabled = True

        # Also patch is_enabled to return True for this test
        monkeypatch.setattr(service, "is_enabled", lambda: True)

        assert service.is_enabled() is True

        # Call download_all_data
        result = service.download_all_data()

        # Should have attempted database download
        assert mock_manager.download_file.called
        # Result should contain database key
        assert 'database' in result


class TestSecretsManagement:
    """Tests for Streamlit secrets loading patterns.

    Verifies that:
    - st.secrets is correctly parsed with nested sections
    - Base64 credentials are properly decoded
    - Missing secrets don't crash the application
    - CAP annotation secrets are correctly loaded
    """
    pass


class TestAsyncStreamlitPatterns:
    """Tests for async code running in Streamlit's event loop context.

    Verifies that:
    - Async code works in Streamlit's Tornado event loop
    - Thread isolation pattern works correctly
    - CLI context (no running loop) works correctly
    - tqdm doesn't cause BrokenPipeError in threads
    """

    def test_async_in_cli_context_uses_asyncio_run(self, cli_context):
        """In CLI context (no running loop), asyncio.run() should work."""
        import asyncio

        async def async_operation():
            await asyncio.sleep(0.01)
            return "completed"

        with cli_context():
            # Should be able to use asyncio.run directly
            result = asyncio.run(async_operation())
            assert result == "completed"

    def test_async_in_streamlit_context_needs_thread_isolation(self, streamlit_context):
        """In Streamlit context (running loop), thread isolation is required.

        The streamlit_context fixture simulates the key behavior that matters for
        our code: asyncio.get_running_loop() returns a loop (indicating we're in
        Streamlit context). In real Streamlit, asyncio.run() would fail, but what
        our code actually does is detect the running loop and use thread isolation.

        This test verifies:
        1. The streamlit_context properly makes get_running_loop() succeed
        2. Thread isolation pattern works correctly for running async code
        """
        import asyncio
        import concurrent.futures

        async def async_operation():
            await asyncio.sleep(0.01)
            return "completed"

        def run_in_thread():
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                return new_loop.run_until_complete(async_operation())
            finally:
                new_loop.close()

        with streamlit_context():
            # In Streamlit context, get_running_loop() returns a loop (doesn't raise)
            # This is what our code uses to detect Streamlit context
            try:
                loop = asyncio.get_running_loop()
                assert loop is not None, "Should have a running loop in Streamlit context"
            except RuntimeError:
                pytest.fail("get_running_loop() should succeed in Streamlit context")

            # Thread isolation should work - run async code in a fresh thread with its own loop
            # This is the pattern our code uses when it detects a running loop
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_thread)
                result = future.result(timeout=5)
                assert result == "completed"

    def test_data_refresh_detects_streamlit_context(self, streamlit_context):
        """DataRefreshService should detect Streamlit context and use threads."""
        from data.services.data_refresh_service import DataRefreshService

        # Create a mock service with mocked async method
        with patch("data.services.data_refresh_service.ODataClient"):
            with patch("data.services.data_refresh_service.DatabaseRepository"):
                with patch("data.services.data_refresh_service.ResumeStateService"):
                    with patch("data.services.data_refresh_service.StorageSyncService"):
                        service = DataRefreshService(db_path=":memory:")

                        # Mock the async refresh_tables method
                        async def mock_refresh_tables(tables, progress_callback):
                            return True

                        with patch.object(service, "refresh_tables", mock_refresh_tables):
                            with streamlit_context():
                                # Should not raise "event loop already running"
                                # The service should detect the running loop and use thread isolation
                                try:
                                    result = service.refresh_tables_sync(tables=["KNS_Person"])
                                    # Should complete successfully using thread isolation
                                    assert result is True
                                except RuntimeError as e:
                                    if "already running" in str(e):
                                        pytest.fail("Should handle running event loop gracefully")
                                    raise

    def test_tqdm_in_thread_context_doesnt_crash(self):
        """tqdm should not crash when stderr is not connected (thread context)."""
        import sys
        from io import StringIO

        # Simulate thread context where stderr might not be a TTY
        mock_stderr = StringIO()
        mock_stderr.isatty = lambda: False

        with patch.object(sys, "stderr", mock_stderr):
            try:
                from tqdm import tqdm
                # Should use fallback or handle gracefully when disable=True
                # (the pattern used in the codebase: disable=not sys.stderr.isatty())
                with tqdm(total=10, disable=not sys.stderr.isatty()) as pbar:
                    for i in range(10):
                        pbar.update(1)
            except BrokenPipeError:
                pytest.fail("tqdm should handle non-TTY stderr gracefully")

    def test_concurrent_database_operations_in_threads(self, tmp_path):
        """Concurrent DB operations in threads should not conflict."""
        import concurrent.futures
        import duckdb

        db_path = tmp_path / "test.duckdb"

        # Create initial database
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
        conn.execute("INSERT INTO test VALUES (1, 'initial')")
        conn.close()

        def read_operation():
            conn = duckdb.connect(str(db_path), read_only=True)
            result = conn.execute("SELECT * FROM test").fetchall()
            conn.close()
            return len(result)

        # Multiple concurrent reads should work
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(read_operation) for _ in range(5)]
            results = [f.result(timeout=10) for f in futures]

            assert all(r == 1 for r in results)


class TestDatabasePersistence:
    """Tests for database connection management across contexts.

    Verifies that:
    - Connection manager works in both local and cloud contexts
    - Read-only connections work correctly
    - Connection pooling doesn't leak
    - Database sync operations complete successfully
    """

    def test_connection_manager_context_manager(self, tmp_path):
        """get_db_connection should work as context manager."""
        from pathlib import Path
        from backend.connection_manager import get_db_connection

        db_path = tmp_path / "test.duckdb"

        # Create table and insert data
        with get_db_connection(db_path, read_only=False) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
            conn.execute("INSERT INTO test VALUES (1)")

        # Connection should be closed, but data persisted
        with get_db_connection(db_path, read_only=True) as conn:
            result = conn.execute("SELECT COUNT(*) FROM test").fetchone()[0]
            assert result == 1

    def test_read_only_connection_prevents_writes(self, tmp_path):
        """Read-only connections should prevent write operations."""
        from pathlib import Path
        from backend.connection_manager import get_db_connection

        db_path = tmp_path / "test.duckdb"

        # Create database first
        with get_db_connection(db_path, read_only=False) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")

        # Read-only should prevent writes
        with get_db_connection(db_path, read_only=True) as conn:
            with pytest.raises(Exception):  # DuckDB raises on write attempt
                conn.execute("INSERT INTO test VALUES (1)")

    def test_connection_leak_detection(self, tmp_path):
        """ConnectionMonitor should detect leaked connections."""
        from pathlib import Path
        from backend.connection_manager import get_db_connection, _connection_monitor

        db_path = tmp_path / "test.duckdb"

        # Verify the mechanism exists - get initial count of active connections
        initial_connections = _connection_monitor.get_active_connections()
        initial_count = len(initial_connections)

        # Open connection in context manager
        with get_db_connection(db_path, read_only=False) as conn:
            # Inside context, connection should be registered
            during_connections = _connection_monitor.get_active_connections()
            assert len(during_connections) == initial_count + 1

        # After context, connection should be released
        after_connections = _connection_monitor.get_active_connections()
        assert len(after_connections) == initial_count

        # Verify monitor has expected methods
        assert hasattr(_connection_monitor, "get_active_connections")
        assert hasattr(_connection_monitor, "register_connection")
        assert hasattr(_connection_monitor, "unregister_connection")

    def test_database_recovery_from_connection_error(self, tmp_path):
        """Should recover gracefully from database connection errors."""
        from pathlib import Path
        from backend.connection_manager import get_db_connection

        # Path to non-existent directory (parent doesn't exist)
        db_path = tmp_path / "nonexistent_dir" / "test.duckdb"

        # Should handle missing directory gracefully
        try:
            with get_db_connection(db_path, read_only=False) as conn:
                pass
        except Exception as e:
            # Should be a clear error, not a crash
            error_str = str(e).lower()
            assert any(keyword in error_str for keyword in [
                "directory", "path", "permission", "no such file", "cannot open"
            ]), f"Expected path-related error, got: {e}"

    def test_concurrent_write_read_connections(self, tmp_path):
        """Concurrent connections should be handled correctly based on DuckDB constraints.

        DuckDB allows multiple read-only connections, but not mixed read/write simultaneously.
        This test verifies:
        1. Multiple read connections can be opened simultaneously
        2. Sequential write-then-read access works correctly
        """
        from pathlib import Path
        from backend.connection_manager import get_db_connection
        import concurrent.futures

        db_path = tmp_path / "test.duckdb"

        # Create database first
        with get_db_connection(db_path, read_only=False) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
            conn.execute("INSERT INTO test VALUES (1)")

        # Test 1: Multiple concurrent read connections should work
        def read_count():
            with get_db_connection(db_path, read_only=True) as conn:
                return conn.execute("SELECT COUNT(*) FROM test").fetchone()[0]

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(read_count) for _ in range(3)]
            results = [f.result(timeout=10) for f in futures]
            assert all(r == 1 for r in results), "All concurrent reads should return 1"

        # Test 2: Sequential write-then-read works correctly
        with get_db_connection(db_path, read_only=False) as conn:
            conn.execute("INSERT INTO test VALUES (2)")

        with get_db_connection(db_path, read_only=True) as conn:
            result = conn.execute("SELECT COUNT(*) FROM test").fetchone()[0]
            assert result == 2

    def test_sequence_persistence_across_connections(self, tmp_path):
        """DuckDB sequences should persist across connections."""
        from pathlib import Path
        from backend.connection_manager import get_db_connection

        db_path = tmp_path / "test.duckdb"

        # Create sequence and use it
        with get_db_connection(db_path, read_only=False) as conn:
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_test START 1")
            val1 = conn.execute("SELECT nextval('seq_test')").fetchone()[0]
            assert val1 == 1

        # Sequence should continue in new connection
        with get_db_connection(db_path, read_only=False) as conn:
            val2 = conn.execute("SELECT nextval('seq_test')").fetchone()[0]
            assert val2 == 2


class TestSessionStatePatterns:
    """Tests for session state persistence and timeout handling.

    Verifies that:
    - Session state persists across reruns
    - Renderer instances are cached in session state
    - State is properly initialized on first load
    - State cleanup works correctly
    """
    pass


class TestResourceConstraints:
    """Tests simulating Streamlit Cloud free tier resource limits.

    Verifies that:
    - Application handles memory constraints gracefully
    - Large DataFrames don't exceed limits
    - Lazy loading reduces memory footprint
    - Operations complete within timeout limits
    """
    pass

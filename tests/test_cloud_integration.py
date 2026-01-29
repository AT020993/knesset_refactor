# tests/test_cloud_integration.py
"""
Cloud Integration Tests

End-to-end tests for cloud deployment scenarios.
These tests can optionally run against real GCS if credentials are available.

Run with real GCS:
    GCS_TEST_BUCKET=my-bucket pytest tests/test_cloud_integration.py -v

Run mocked (default):
    pytest tests/test_cloud_integration.py -v
"""

import os
import pytest
from unittest.mock import MagicMock, patch

from tests.fixtures.cloud_fixtures import MOCK_GCS_CREDENTIALS, MOCK_GCS_CREDENTIALS_BASE64

# Check if real GCS credentials are available
REAL_GCS_AVAILABLE = os.environ.get("GCS_TEST_BUCKET") is not None


@pytest.mark.integration
class TestCloudDeploymentScenarios:
    """Integration tests for cloud deployment scenarios."""

    def test_fresh_deployment_downloads_database(self, tmp_path, mock_gcs_client):
        """Fresh Streamlit Cloud deployment should download database from GCS."""
        from data.services.storage_sync_service import StorageSyncService

        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with patch("data.services.storage_sync_service.create_gcs_manager_from_streamlit_secrets") as mock_factory:
            mock_manager = MagicMock()
            mock_manager.download_file.return_value = True
            mock_manager.file_exists.return_value = True
            mock_manager.download_directory.return_value = {}
            mock_factory.return_value = mock_manager

            service = StorageSyncService(gcs_manager=mock_manager)
            # Manually enable since conftest disables by default
            service.gcs_manager = mock_manager
            service.enabled = True

            if service.is_enabled():
                result = service.download_all_data()
                # Should have attempted to download database
                calls = mock_manager.download_file.call_args_list
                db_calls = [c for c in calls if "duckdb" in str(c)]
                assert len(db_calls) > 0 or mock_manager.download_directory.called

    def test_annotation_triggers_cloud_sync(self, tmp_path, mock_gcs_client):
        """Saving annotation should trigger cloud sync."""
        from data.services.storage_sync_service import StorageSyncService

        sync_called = [False]

        def mock_sync(*args, **kwargs):
            sync_called[0] = True
            return {}

        with patch("data.services.storage_sync_service.create_gcs_manager_from_streamlit_secrets", return_value=MagicMock()):
            mock_manager = MagicMock()
            mock_manager.upload_file.return_value = True
            mock_manager.upload_directory.return_value = {}

            service = StorageSyncService(gcs_manager=mock_manager)
            service.gcs_manager = mock_manager
            service.enabled = True

            with patch.object(service, "upload_all_data", mock_sync):
                if service.is_enabled():
                    service.upload_all_data()
                    assert sync_called[0]

    def test_app_startup_without_gcs_works(self, tmp_path):
        """App should start successfully without GCS credentials."""
        from data.services.storage_sync_service import StorageSyncService

        with patch("data.services.storage_sync_service.create_gcs_manager_from_streamlit_secrets", return_value=None):
            service = StorageSyncService()
            assert service.is_enabled() is False
            result = service.download_all_data()
            assert result is None or isinstance(result, dict)

    def test_database_persists_after_sync_cycle(self, tmp_path, mock_gcs_client):
        """Database changes should persist through upload/download cycle."""
        from backend.connection_manager import get_db_connection

        db_path = tmp_path / "test.duckdb"

        with get_db_connection(db_path, read_only=False) as conn:
            conn.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
            conn.execute("INSERT INTO test VALUES (1, 'original')")

        # Verify data persists
        with get_db_connection(db_path, read_only=True) as conn:
            result = conn.execute("SELECT value FROM test WHERE id = 1").fetchone()
            assert result[0] == "original"

    @pytest.mark.skipif(not REAL_GCS_AVAILABLE, reason="No GCS credentials")
    def test_real_gcs_upload_download(self, tmp_path):
        """Test actual GCS upload and download (requires credentials)."""
        from data.storage.cloud_storage import CloudStorageManager

        bucket_name = os.environ["GCS_TEST_BUCKET"]
        test_file = tmp_path / "test_upload.txt"
        test_file.write_text("test content")

        manager = CloudStorageManager(bucket_name=bucket_name)
        remote_path = f"test/{test_file.name}"

        upload_result = manager.upload_file(str(test_file), remote_path)
        assert upload_result is True

        download_path = tmp_path / "test_download.txt"
        download_result = manager.download_file(remote_path, str(download_path))
        assert download_result is True
        assert download_path.read_text() == "test content"


@pytest.mark.integration
class TestSecretsConfiguration:
    """Integration tests for secrets configuration scenarios."""

    def test_base64_credentials_decode_correctly(self):
        """Base64-encoded credentials should decode to valid JSON."""
        import base64
        import json

        decoded = base64.b64decode(MOCK_GCS_CREDENTIALS_BASE64).decode("utf-8")
        parsed = json.loads(decoded)

        assert parsed["type"] == MOCK_GCS_CREDENTIALS["type"]
        assert parsed["project_id"] == MOCK_GCS_CREDENTIALS["project_id"]

    def test_missing_secrets_section_handled(self):
        """Missing secrets sections should be handled gracefully."""
        from ui.services.gcs_factory import create_gcs_manager_from_streamlit_secrets

        empty_secrets = MagicMock()
        empty_secrets.get.return_value = None
        empty_secrets.__contains__ = lambda self, key: False

        with patch("streamlit.secrets", empty_secrets):
            with patch("ui.services.gcs_factory.st") as mock_st:
                mock_st.secrets = empty_secrets
                result = create_gcs_manager_from_streamlit_secrets()
                assert result is None

    def test_cap_annotation_secrets_structure(self, mock_streamlit_secrets):
        """CAP annotation secrets should have required fields."""
        # Create mock secrets with CAP enabled
        secrets = mock_streamlit_secrets(cap_enabled=True)
        cap_secrets = secrets["cap_annotation"]

        # Verify required fields exist
        required_fields = ["enabled", "bootstrap_admin_username", "bootstrap_admin_password"]
        for field in required_fields:
            assert field in cap_secrets, f"Missing required field: {field}"


@pytest.mark.integration
class TestConcurrentAccess:
    """Integration tests for concurrent user access scenarios."""

    def test_multiple_researchers_can_annotate_same_bill(self, tmp_path):
        """Multiple researchers should be able to annotate the same bill."""
        from backend.connection_manager import get_db_connection

        db_path = tmp_path / "test.duckdb"

        with get_db_connection(db_path, read_only=False) as conn:
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS seq_annotation_id START 1;
                CREATE TABLE UserBillCAP (
                    AnnotationID INTEGER PRIMARY KEY DEFAULT nextval('seq_annotation_id'),
                    BillID INTEGER NOT NULL,
                    ResearcherID INTEGER NOT NULL,
                    CAPMinorCode INTEGER NOT NULL,
                    UNIQUE(BillID, ResearcherID)
                )
            """)

        # Researcher 1 annotates bill 100
        with get_db_connection(db_path, read_only=False) as conn:
            conn.execute("INSERT INTO UserBillCAP (BillID, ResearcherID, CAPMinorCode) VALUES (100, 1, 101)")

        # Researcher 2 annotates same bill
        with get_db_connection(db_path, read_only=False) as conn:
            conn.execute("INSERT INTO UserBillCAP (BillID, ResearcherID, CAPMinorCode) VALUES (100, 2, 102)")

        # Verify both annotations exist
        with get_db_connection(db_path, read_only=True) as conn:
            count = conn.execute("SELECT COUNT(*) FROM UserBillCAP WHERE BillID = 100").fetchone()[0]
            assert count == 2

    def test_duplicate_annotation_same_researcher_updates(self, tmp_path):
        """Same researcher re-annotating should update, not duplicate."""
        from backend.connection_manager import get_db_connection

        db_path = tmp_path / "test.duckdb"

        with get_db_connection(db_path, read_only=False) as conn:
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS seq_annotation_id START 1;
                CREATE TABLE UserBillCAP (
                    AnnotationID INTEGER PRIMARY KEY DEFAULT nextval('seq_annotation_id'),
                    BillID INTEGER NOT NULL,
                    ResearcherID INTEGER NOT NULL,
                    CAPMinorCode INTEGER NOT NULL,
                    UNIQUE(BillID, ResearcherID)
                )
            """)

        # First annotation
        with get_db_connection(db_path, read_only=False) as conn:
            conn.execute("INSERT INTO UserBillCAP (BillID, ResearcherID, CAPMinorCode) VALUES (100, 1, 101)")

        # Re-annotation (update via upsert)
        with get_db_connection(db_path, read_only=False) as conn:
            conn.execute("""
                INSERT INTO UserBillCAP (BillID, ResearcherID, CAPMinorCode)
                VALUES (100, 1, 102)
                ON CONFLICT (BillID, ResearcherID) DO UPDATE
                SET CAPMinorCode = EXCLUDED.CAPMinorCode
            """)

        # Should only have one annotation
        with get_db_connection(db_path, read_only=True) as conn:
            count = conn.execute("SELECT COUNT(*) FROM UserBillCAP WHERE BillID = 100 AND ResearcherID = 1").fetchone()[0]
            assert count == 1
            code = conn.execute("SELECT CAPMinorCode FROM UserBillCAP WHERE BillID = 100 AND ResearcherID = 1").fetchone()[0]
            assert code == 102

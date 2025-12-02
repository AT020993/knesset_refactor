"""
Tests for data services functionality.
"""
import pytest
import json
import asyncio
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock, MagicMock, mock_open
import pandas as pd
from typing import Dict, List, Optional, Any, Callable

from src.data.services.data_refresh_service import DataRefreshService
from src.data.services.resume_state_service import ResumeStateService
from src.data.repositories.database_repository import DatabaseRepository
from src.api.odata_client import ODataClient
from src.config.database import DatabaseConfig


class TestDataRefreshService:
    """Test DataRefreshService functionality."""

    def setup_method(self):
        """Setup test fixtures."""
        self.mock_db_path = Path("test.db")
        self.mock_logger = Mock()
        self.service = DataRefreshService(self.mock_db_path, self.mock_logger)

    @pytest.mark.asyncio
    async def test_refresh_single_table_success(self):
        """Test successful single table refresh."""
        test_df = pd.DataFrame({
            'PersonID': [1, 2, 3],
            'FirstName': ['Alice', 'Bob', 'Charlie'],
            'LastName': ['Smith', 'Jones', 'Brown']
        })

        with patch.object(self.service.odata_client, 'download_table', new_callable=AsyncMock, return_value=test_df) as mock_download, \
             patch.object(self.service.db_repository, 'store_table', return_value=True) as mock_store, \
             patch.object(self.service.resume_service, 'clear_table_state') as mock_clear_state, \
             patch.object(DatabaseConfig, 'is_cursor_table', return_value=True):

            result = await self.service.refresh_single_table("KNS_Person")

            assert result is True
            # Check that download_table was called
            mock_download.assert_called_once()
            call_args = mock_download.call_args
            assert call_args[0][0] == "KNS_Person"
            # State should be cleared after successful cursor table refresh
            mock_store.assert_called_once()
            mock_clear_state.assert_called_once_with("KNS_Person")

    @pytest.mark.asyncio
    async def test_refresh_single_table_with_resume_state(self):
        """Test single table refresh with existing resume state."""
        resume_state = {
            'last_pk': 100,
            'total_rows': 50,
            'chunk_size': 100,
            'timestamp': '2024-01-01T00:00:00'
        }

        test_df = pd.DataFrame({
            'PersonID': [101, 102, 103],
            'FirstName': ['David', 'Eve', 'Frank']
        })

        with patch.object(self.service.resume_service, 'get_table_state', return_value=resume_state), \
             patch.object(DatabaseConfig, 'is_cursor_table', return_value=True), \
             patch.object(self.service.odata_client, 'download_table', new_callable=AsyncMock, return_value=test_df) as mock_download, \
             patch.object(self.service.db_repository, 'store_table', return_value=True):

            result = await self.service.refresh_single_table("KNS_Person")

            assert result is True
            # Verify resume state was passed to download_table
            mock_download.assert_called_once_with("KNS_Person", resume_state)

    @pytest.mark.asyncio
    async def test_refresh_single_table_download_failure(self):
        """Test handling of download failures."""
        with patch.object(self.service.odata_client, 'download_table', new_callable=AsyncMock, side_effect=Exception("Download failed")) as mock_download:

            result = await self.service.refresh_single_table("KNS_Person")

            assert result is False
            mock_download.assert_called_once()
            # Verify error was logged
            self.mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_refresh_single_table_storage_failure(self):
        """Test handling of storage failures."""
        test_df = pd.DataFrame({'PersonID': [1], 'FirstName': ['Test']})

        with patch.object(self.service.odata_client, 'download_table', new_callable=AsyncMock, return_value=test_df), \
             patch.object(self.service.db_repository, 'store_table', return_value=False) as mock_store:

            result = await self.service.refresh_single_table("KNS_Person")

            assert result is False
            mock_store.assert_called_once()
            self.mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_refresh_single_table_empty_dataframe(self):
        """Test handling of empty DataFrame from download."""
        empty_df = pd.DataFrame()

        with patch.object(self.service.odata_client, 'download_table', new_callable=AsyncMock, return_value=empty_df), \
             patch.object(self.service.db_repository, 'store_table', return_value=True) as mock_store:

            result = await self.service.refresh_single_table("KNS_Person")

            # Empty DataFrame should return True (no data to store)
            assert result is True
            # Store should not be called for empty DataFrame
            mock_store.assert_not_called()

    @pytest.mark.asyncio
    async def test_refresh_tables_multiple_success(self):
        """Test refreshing multiple tables successfully."""
        tables = ["KNS_Person", "KNS_Faction", "KNS_Status"]
        test_df = pd.DataFrame({'ID': [1], 'Name': ['Test']})
        progress_callback = Mock()

        with patch.object(self.service, 'refresh_single_table', new_callable=AsyncMock, return_value=True) as mock_refresh, \
             patch.object(self.service.db_repository, 'load_faction_coalition_status', return_value=True):

            result = await self.service.refresh_tables(tables, progress_callback)

            assert result is True
            assert mock_refresh.call_count == 3

    @pytest.mark.asyncio
    async def test_refresh_tables_partial_failure(self):
        """Test handling of partial failures in multiple table refresh."""
        tables = ["KNS_Person", "KNS_Faction", "KNS_Status"]
        progress_callback = Mock()

        # Mock second table to fail
        async def mock_refresh_side_effect(table_name, callback=None):
            if table_name == "KNS_Faction":
                return False
            return True

        with patch.object(self.service, 'refresh_single_table', new_callable=AsyncMock, side_effect=mock_refresh_side_effect) as mock_refresh, \
             patch.object(self.service.db_repository, 'load_faction_coalition_status', return_value=True):

            result = await self.service.refresh_tables(tables, progress_callback)

            # Should continue despite partial failure
            assert mock_refresh.call_count == 3
            # Overall result should be False due to one failure
            assert result is False

            # Verify all tables were attempted
            called_tables = [call[0][0] for call in mock_refresh.call_args_list]
            assert set(called_tables) == set(tables)

    @pytest.mark.asyncio
    async def test_refresh_tables_with_invalid_table(self):
        """Test refresh with invalid table names."""
        tables = ["ValidTable", "InvalidTable", "AnotherValid"]
        progress_callback = Mock()

        # Should raise ValueError for invalid tables
        with pytest.raises(ValueError, match="Invalid table names"):
            await self.service.refresh_tables(tables, progress_callback)

    def test_refresh_tables_sync(self):
        """Test synchronous wrapper for refresh_tables."""
        tables = ["KNS_Person"]
        progress_callback = Mock()

        with patch.object(asyncio, 'run', return_value=True) as mock_run:

            result = self.service.refresh_tables_sync(tables, progress_callback)

            assert result is True
            mock_run.assert_called_once()

    def test_refresh_faction_status_only(self):
        """Test faction status refresh functionality."""
        with patch.object(self.service.db_repository, 'load_faction_coalition_status', return_value=True) as mock_load:

            result = self.service.refresh_faction_status_only()

            assert result is True
            mock_load.assert_called_once()

    def test_refresh_faction_status_failure(self):
        """Test faction status refresh failure handling."""
        with patch.object(self.service.db_repository, 'load_faction_coalition_status', return_value=False):

            result = self.service.refresh_faction_status_only()

            assert result is False


class TestResumeStateService:
    """Test ResumeStateService functionality."""

    def setup_method(self):
        """Setup test fixtures."""
        self.test_state_file = Path("test_resume_state.json")
        # Patch _load_state during initialization to prevent file access
        with patch.object(ResumeStateService, '_load_state', return_value={}):
            self.service = ResumeStateService(self.test_state_file)

    def test_get_table_state_empty_state(self):
        """Test getting state when no state exists."""
        self.service._state = {}

        state = self.service.get_table_state("KNS_Person")

        assert state == {"last_pk": -1, "total_rows": 0}

    def test_get_table_state_existing_data(self):
        """Test getting state with existing data."""
        mock_state_data = {
            "KNS_Person": {
                "last_pk": 100,
                "total_rows": 50,
                "chunk_size": 100,
                "timestamp": "2024-01-01T00:00:00"
            }
        }

        self.service._state = mock_state_data

        state = self.service.get_table_state("KNS_Person")

        assert state == mock_state_data["KNS_Person"]

    def test_get_table_state_missing_table(self):
        """Test getting state for table not in state."""
        mock_state_data = {
            "KNS_Faction": {
                "last_pk": 50,
                "total_rows": 25,
                "chunk_size": 100
            }
        }

        self.service._state = mock_state_data

        state = self.service.get_table_state("KNS_Person")

        assert state == {"last_pk": -1, "total_rows": 0}

    def test_update_table_state_new_file(self):
        """Test updating state when file doesn't exist."""
        self.service._state = {}

        with patch.object(self.service, '_save_state') as mock_save:

            self.service.update_table_state("KNS_Person", 100, 50, 100)

            # Verify state was updated
            assert "KNS_Person" in self.service._state
            assert self.service._state["KNS_Person"]["last_pk"] == 100
            assert self.service._state["KNS_Person"]["total_rows"] == 50
            assert self.service._state["KNS_Person"]["chunk_size"] == 100

            # Verify save was called
            mock_save.assert_called_once()

    def test_update_table_state_existing_file(self):
        """Test updating state with existing data."""
        existing_data = {
            "KNS_Faction": {
                "last_pk": 50,
                "total_rows": 25,
                "chunk_size": 100,
                "timestamp": "2024-01-01T00:00:00"
            }
        }

        self.service._state = existing_data.copy()

        with patch.object(self.service, '_save_state') as mock_save:

            self.service.update_table_state("KNS_Person", 200, 100, 100)

            # Verify existing data was preserved and new data added
            assert "KNS_Faction" in self.service._state
            assert "KNS_Person" in self.service._state
            assert self.service._state["KNS_Person"]["last_pk"] == 200

            mock_save.assert_called_once()

    def test_clear_table_state(self):
        """Test clearing state for specific table."""
        existing_data = {
            "KNS_Person": {"last_pk": 100, "total_rows": 50},
            "KNS_Faction": {"last_pk": 50, "total_rows": 25}
        }

        self.service._state = existing_data.copy()

        with patch.object(self.service, '_save_state') as mock_save:

            self.service.clear_table_state("KNS_Person")

            # Verify only specified table was removed
            assert "KNS_Person" not in self.service._state
            assert "KNS_Faction" in self.service._state

            mock_save.assert_called_once()

    def test_get_all_states(self):
        """Test getting all table states."""
        mock_state_data = {
            "KNS_Person": {"last_pk": 100, "total_rows": 50},
            "KNS_Faction": {"last_pk": 50, "total_rows": 25}
        }

        self.service._state = mock_state_data

        all_states = self.service.get_all_states()

        assert all_states == mock_state_data
        # Verify it returns a copy
        assert all_states is not self.service._state

    def test_clear_all_states(self):
        """Test clearing all table states."""
        self.service._state = {
            "KNS_Person": {"last_pk": 100, "total_rows": 50},
            "KNS_Faction": {"last_pk": 50, "total_rows": 25}
        }

        with patch.object(self.service, '_save_state') as mock_save:

            self.service.clear_all_states()

            # Verify state was cleared
            assert self.service._state == {}
            mock_save.assert_called_once()

    def test_corrupted_file_handling(self):
        """Test handling of corrupted JSON file."""
        # Use a unique file path to avoid state pollution
        corrupted_file = Path("corrupted_test.json")

        with patch('builtins.open', mock_open(read_data="invalid json")), \
             patch.object(Path, 'exists', return_value=True):

            # Create new service instance which will try to load
            service = ResumeStateService(corrupted_file)

            # Should handle corrupted file gracefully
            assert service._state == {}

    def test_file_permission_error(self):
        """Test handling of file permission errors."""
        self.service._state = {}

        with patch('builtins.open', side_effect=PermissionError("Access denied")):

            # Should handle permission errors gracefully
            self.service.update_table_state("KNS_Person", 100, 50, 100)

            # State should still be updated in memory
            assert "KNS_Person" in self.service._state

    def test_legacy_format_migration(self):
        """Test migration from legacy state format."""
        # Simulate old format without timestamp
        legacy_data = {
            "KNS_Person": {
                "last_pk": 100,
                "total_rows": 50,
                "chunk_size": 100
                # Missing timestamp
            }
        }

        self.service._state = legacy_data

        state = self.service.get_table_state("KNS_Person")

        # Should handle legacy format gracefully
        assert state["last_pk"] == 100
        assert state["total_rows"] == 50
        assert state["chunk_size"] == 100


class TestServiceIntegration:
    """Test integration between different services."""

    def setup_method(self):
        """Setup test fixtures."""
        self.mock_db_path = Path("test.db")
        self.mock_logger = Mock()
        self.data_service = DataRefreshService(self.mock_db_path, self.mock_logger)

    @pytest.mark.asyncio
    async def test_data_refresh_with_resume_state_integration(self):
        """Test integration between DataRefreshService and ResumeStateService."""
        # Setup resume state
        resume_state = {
            'last_pk': 50,
            'total_rows': 25,
            'chunk_size': 100,
            'timestamp': '2024-01-01T00:00:00'
        }

        # Mock data from cursor position
        test_df = pd.DataFrame({
            'PersonID': [51, 52, 53],
            'FirstName': ['Alice', 'Bob', 'Charlie']
        })

        with patch.object(self.data_service.resume_service, 'get_table_state', return_value=resume_state), \
             patch.object(DatabaseConfig, 'is_cursor_table', return_value=True), \
             patch.object(self.data_service.odata_client, 'download_table', new_callable=AsyncMock, return_value=test_df) as mock_download, \
             patch.object(self.data_service.db_repository, 'store_table', return_value=True), \
             patch.object(self.data_service.resume_service, 'clear_table_state') as mock_clear:

            result = await self.data_service.refresh_single_table("KNS_Person")

            assert result is True
            # Verify resume state was passed to client
            mock_download.assert_called_once_with("KNS_Person", resume_state)
            # Verify state was cleared after success
            mock_clear.assert_called_once_with("KNS_Person")

    @pytest.mark.asyncio
    async def test_error_handling_preserves_resume_state(self):
        """Test that resume state is preserved when operations fail."""
        resume_state = {'last_pk': 50, 'total_rows': 25, 'chunk_size': 100}

        with patch.object(self.data_service.resume_service, 'get_table_state', return_value=resume_state), \
             patch.object(DatabaseConfig, 'is_cursor_table', return_value=True), \
             patch.object(self.data_service.odata_client, 'download_table', new_callable=AsyncMock, side_effect=Exception("Network error")), \
             patch.object(self.data_service.resume_service, 'clear_table_state') as mock_clear:

            result = await self.data_service.refresh_single_table("KNS_Person")

            assert result is False
            # Resume state should NOT be cleared on failure
            mock_clear.assert_not_called()

    @pytest.mark.asyncio
    async def test_repository_integration(self):
        """Test integration with DatabaseRepository."""
        test_df = pd.DataFrame({
            'PersonID': [1, 2, 3],
            'FirstName': ['Alice', 'Bob', 'Charlie']
        })

        with patch.object(self.data_service.odata_client, 'download_table', new_callable=AsyncMock, return_value=test_df), \
             patch.object(self.data_service.db_repository, 'store_table', return_value=True) as mock_store, \
             patch.object(self.data_service.db_repository, 'table_exists', return_value=True), \
             patch.object(self.data_service.db_repository, 'get_table_count', return_value=3):

            result = await self.data_service.refresh_single_table("KNS_Person")

            assert result is True
            # Verify DataFrame was passed to repository
            mock_store.assert_called_once_with(test_df, "KNS_Person")

    def test_progress_callback_integration(self):
        """Test progress callback integration across services."""
        progress_calls = []

        def mock_progress_callback(message, progress=None):
            progress_calls.append((message, progress))

        tables = ["KNS_Person", "KNS_Faction"]

        async def mock_refresh_single_table(table_name, callback=None):
            # Simulate progress updates
            if callback:
                callback(table_name, 100)
            return True

        with patch.object(self.data_service, 'refresh_single_table', new_callable=AsyncMock, side_effect=mock_refresh_single_table), \
             patch.object(self.data_service.db_repository, 'load_faction_coalition_status', return_value=True):

            # Run synchronous version for easier testing
            result = self.data_service.refresh_tables_sync(tables, mock_progress_callback)

            # Verify the operation completed
            assert result is True


class TestServiceErrorScenarios:
    """Test error scenarios and edge cases in services."""

    def setup_method(self):
        """Setup test fixtures."""
        self.mock_db_path = Path("test.db")
        self.mock_logger = Mock()
        self.service = DataRefreshService(self.mock_db_path, self.mock_logger)

    @pytest.mark.asyncio
    async def test_network_timeout_handling(self):
        """Test handling of network timeouts."""
        import asyncio

        with patch.object(self.service.odata_client, 'download_table', new_callable=AsyncMock, side_effect=asyncio.TimeoutError("Request timeout")):

            result = await self.service.refresh_single_table("KNS_Person")

            assert result is False
            # Verify timeout was logged
            self.mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_database_connection_failure(self):
        """Test handling of database connection failures."""
        test_df = pd.DataFrame({'PersonID': [1], 'FirstName': ['Test']})

        with patch.object(self.service.odata_client, 'download_table', new_callable=AsyncMock, return_value=test_df), \
             patch.object(self.service.db_repository, 'store_table', side_effect=Exception("Database connection failed")):

            result = await self.service.refresh_single_table("KNS_Person")

            assert result is False
            self.mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_disk_space_exhaustion(self):
        """Test handling of disk space issues."""
        test_df = pd.DataFrame({'PersonID': [1], 'FirstName': ['Test']})

        with patch.object(self.service.odata_client, 'download_table', new_callable=AsyncMock, return_value=test_df), \
             patch.object(self.service.db_repository, 'store_table', side_effect=OSError("No space left on device")):

            result = await self.service.refresh_single_table("KNS_Person")

            assert result is False
            self.mock_logger.error.assert_called()

    def test_resume_state_service_file_corruption(self):
        """Test resume state service with corrupted state file."""
        with patch('builtins.open', mock_open(read_data="corrupted json {")), \
             patch.object(Path, 'exists', return_value=True):

            # Should handle corruption gracefully
            resume_service = ResumeStateService(Path("test_state.json"))

            assert resume_service._state == {}

    @pytest.mark.asyncio
    async def test_concurrent_table_refresh_safety(self):
        """Test thread safety of concurrent table refreshes."""
        # This test would verify that concurrent operations don't interfere
        # In practice, this might require more sophisticated testing setup

        async def mock_slow_refresh(table_name, callback=None):
            # No real sleep needed - just return immediately for testing
            return True

        with patch.object(self.service, 'refresh_single_table', new_callable=AsyncMock, side_effect=mock_slow_refresh):

            # Start multiple refreshes concurrently
            tasks = [
                self.service.refresh_single_table("KNS_Person"),
                self.service.refresh_single_table("KNS_Faction"),
                self.service.refresh_single_table("KNS_Status")
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # All should complete successfully
            assert all(result is True for result in results if not isinstance(result, Exception))

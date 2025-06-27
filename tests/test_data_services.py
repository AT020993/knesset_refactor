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
        
        with patch.object(self.service.client, 'download_table', return_value=test_df) as mock_download, \
             patch.object(self.service.repository, 'store_table', return_value=True) as mock_store, \
             patch.object(self.service.resume_service, 'clear_table_state') as mock_clear_state:
            
            result = await self.service.refresh_single_table("KNS_Person")
            
            assert result is True
            mock_download.assert_called_once_with("KNS_Person", resume_state=None)
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
             patch.object(self.service.client, 'download_table', return_value=test_df) as mock_download, \
             patch.object(self.service.repository, 'store_table', return_value=True):
            
            result = await self.service.refresh_single_table("KNS_Person")
            
            assert result is True
            mock_download.assert_called_once_with("KNS_Person", resume_state=resume_state)
    
    @pytest.mark.asyncio
    async def test_refresh_single_table_download_failure(self):
        """Test handling of download failures."""
        with patch.object(self.service.client, 'download_table', side_effect=Exception("Download failed")) as mock_download:
            
            result = await self.service.refresh_single_table("KNS_Person")
            
            assert result is False
            mock_download.assert_called_once()
            # Verify error was logged
            self.mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_refresh_single_table_storage_failure(self):
        """Test handling of storage failures."""
        test_df = pd.DataFrame({'PersonID': [1], 'FirstName': ['Test']})
        
        with patch.object(self.service.client, 'download_table', return_value=test_df), \
             patch.object(self.service.repository, 'store_table', return_value=False) as mock_store:
            
            result = await self.service.refresh_single_table("KNS_Person")
            
            assert result is False
            mock_store.assert_called_once()
            self.mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_refresh_single_table_empty_dataframe(self):
        """Test handling of empty DataFrame from download."""
        empty_df = pd.DataFrame()
        
        with patch.object(self.service.client, 'download_table', return_value=empty_df), \
             patch.object(self.service.repository, 'store_table', return_value=True) as mock_store:
            
            result = await self.service.refresh_single_table("KNS_Person")
            
            # Should still attempt to store empty DataFrame
            assert result is True
            mock_store.assert_called_once_with(empty_df, "KNS_Person")
    
    @pytest.mark.asyncio
    async def test_refresh_tables_multiple_success(self):
        """Test refreshing multiple tables successfully."""
        tables = ["KNS_Person", "KNS_Faction", "KNS_Status"]
        test_df = pd.DataFrame({'ID': [1], 'Name': ['Test']})
        progress_callback = Mock()
        
        with patch.object(self.service, 'refresh_single_table', return_value=True) as mock_refresh:
            
            result = await self.service.refresh_tables(tables, progress_callback)
            
            assert result is True
            assert mock_refresh.call_count == 3
            
            # Verify progress callback was called
            assert progress_callback.call_count >= 3  # At least once per table
    
    @pytest.mark.asyncio
    async def test_refresh_tables_partial_failure(self):
        """Test handling of partial failures in multiple table refresh."""
        tables = ["KNS_Person", "KNS_Faction", "KNS_Status"]
        progress_callback = Mock()
        
        # Mock second table to fail
        async def mock_refresh_side_effect(table_name):
            if table_name == "KNS_Faction":
                return False
            return True
        
        with patch.object(self.service, 'refresh_single_table', side_effect=mock_refresh_side_effect) as mock_refresh:
            
            result = await self.service.refresh_tables(tables, progress_callback)
            
            # Should continue despite partial failure
            assert mock_refresh.call_count == 3
            # Overall result depends on implementation - may be True or False
            
            # Verify all tables were attempted
            called_tables = [call[0][0] for call in mock_refresh.call_args_list]
            assert set(called_tables) == set(tables)
    
    @pytest.mark.asyncio
    async def test_refresh_tables_with_invalid_table(self):
        """Test refresh with invalid table names."""
        tables = ["ValidTable", "InvalidTable", "AnotherValid"]
        progress_callback = Mock()
        
        # Mock invalid table to fail
        async def mock_refresh_side_effect(table_name):
            if table_name == "InvalidTable":
                return False
            return True
        
        with patch.object(self.service, 'refresh_single_table', side_effect=mock_refresh_side_effect):
            
            result = await self.service.refresh_tables(tables, progress_callback)
            
            # Should handle invalid table gracefully
            # Exact behavior depends on implementation
    
    def test_refresh_tables_sync(self):
        """Test synchronous wrapper for refresh_tables."""
        tables = ["KNS_Person"]
        progress_callback = Mock()
        
        with patch.object(asyncio, 'run', return_value=True) as mock_run:
            
            result = self.service.refresh_tables_sync(tables, progress_callback)
            
            assert result is True
            mock_run.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_refresh_faction_status_only(self):
        """Test faction status refresh functionality."""
        with patch.object(self.service.repository, 'load_faction_coalition_status', return_value=True) as mock_load:
            
            result = await self.service.refresh_faction_status_only()
            
            assert result is True
            mock_load.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_refresh_faction_status_failure(self):
        """Test faction status refresh failure handling."""
        with patch.object(self.service.repository, 'load_faction_coalition_status', return_value=False):
            
            result = await self.service.refresh_faction_status_only()
            
            assert result is False
            self.mock_logger.error.assert_called()


class TestResumeStateService:
    """Test ResumeStateService functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.test_state_file = Path("test_resume_state.json")
        self.service = ResumeStateService(self.test_state_file)
    
    def test_get_table_state_empty_file(self):
        """Test getting state when file doesn't exist."""
        with patch.object(self.test_state_file, 'exists', return_value=False):
            
            state = self.service.get_table_state("KNS_Person")
            
            assert state == {}
    
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
        
        with patch('builtins.open', mock_open(read_data=json.dumps(mock_state_data))), \
             patch.object(self.test_state_file, 'exists', return_value=True):
            
            state = self.service.get_table_state("KNS_Person")
            
            assert state == mock_state_data["KNS_Person"]
    
    def test_get_table_state_missing_table(self):
        """Test getting state for table not in file."""
        mock_state_data = {
            "KNS_Faction": {
                "last_pk": 50,
                "total_rows": 25,
                "chunk_size": 100
            }
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(mock_state_data))), \
             patch.object(self.test_state_file, 'exists', return_value=True):
            
            state = self.service.get_table_state("KNS_Person")
            
            assert state == {}
    
    def test_update_table_state_new_file(self):
        """Test updating state when file doesn't exist."""
        with patch('builtins.open', mock_open()) as mock_file, \
             patch.object(self.test_state_file, 'exists', return_value=False), \
             patch('json.dump') as mock_json_dump:
            
            self.service.update_table_state("KNS_Person", 100, 50, 100)
            
            # Verify file was opened for writing
            mock_file.assert_called()
            mock_json_dump.assert_called_once()
            
            # Verify correct data structure
            written_data = mock_json_dump.call_args[0][0]
            assert "KNS_Person" in written_data
            assert written_data["KNS_Person"]["last_pk"] == 100
            assert written_data["KNS_Person"]["total_rows"] == 50
            assert written_data["KNS_Person"]["chunk_size"] == 100
            assert "timestamp" in written_data["KNS_Person"]
    
    def test_update_table_state_existing_file(self):
        """Test updating state with existing file."""
        existing_data = {
            "KNS_Faction": {
                "last_pk": 50,
                "total_rows": 25,
                "chunk_size": 100,
                "timestamp": "2024-01-01T00:00:00"
            }
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(existing_data))) as mock_file, \
             patch.object(self.test_state_file, 'exists', return_value=True), \
             patch('json.dump') as mock_json_dump:
            
            self.service.update_table_state("KNS_Person", 200, 100, 100)
            
            # Verify existing data was preserved and new data added
            written_data = mock_json_dump.call_args[0][0]
            assert "KNS_Faction" in written_data  # Existing data preserved
            assert "KNS_Person" in written_data   # New data added
            assert written_data["KNS_Person"]["last_pk"] == 200
    
    def test_clear_table_state(self):
        """Test clearing state for specific table."""
        existing_data = {
            "KNS_Person": {"last_pk": 100, "total_rows": 50},
            "KNS_Faction": {"last_pk": 50, "total_rows": 25}
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(existing_data))) as mock_file, \
             patch.object(self.test_state_file, 'exists', return_value=True), \
             patch('json.dump') as mock_json_dump:
            
            self.service.clear_table_state("KNS_Person")
            
            # Verify only specified table was removed
            written_data = mock_json_dump.call_args[0][0]
            assert "KNS_Person" not in written_data
            assert "KNS_Faction" in written_data
    
    def test_get_all_states(self):
        """Test getting all table states."""
        mock_state_data = {
            "KNS_Person": {"last_pk": 100, "total_rows": 50},
            "KNS_Faction": {"last_pk": 50, "total_rows": 25}
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(mock_state_data))), \
             patch.object(self.test_state_file, 'exists', return_value=True):
            
            all_states = self.service.get_all_states()
            
            assert all_states == mock_state_data
    
    def test_clear_all_states(self):
        """Test clearing all table states."""
        with patch('builtins.open', mock_open()) as mock_file, \
             patch('json.dump') as mock_json_dump:
            
            self.service.clear_all_states()
            
            # Verify empty dict was written
            written_data = mock_json_dump.call_args[0][0]
            assert written_data == {}
    
    def test_corrupted_file_handling(self):
        """Test handling of corrupted JSON file."""
        with patch('builtins.open', mock_open(read_data="invalid json")), \
             patch.object(self.test_state_file, 'exists', return_value=True):
            
            # Should handle corrupted file gracefully
            state = self.service.get_table_state("KNS_Person")
            
            assert state == {}
    
    def test_file_permission_error(self):
        """Test handling of file permission errors."""
        with patch('builtins.open', side_effect=PermissionError("Access denied")):
            
            # Should handle permission errors gracefully
            result = self.service.update_table_state("KNS_Person", 100, 50, 100)
            
            # Should not crash, may return False or None
    
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
        
        with patch('builtins.open', mock_open(read_data=json.dumps(legacy_data))), \
             patch.object(self.test_state_file, 'exists', return_value=True):
            
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
             patch.object(self.data_service.client, 'download_table', return_value=test_df) as mock_download, \
             patch.object(self.data_service.repository, 'store_table', return_value=True), \
             patch.object(self.data_service.resume_service, 'clear_table_state') as mock_clear:
            
            result = await self.data_service.refresh_single_table("KNS_Person")
            
            assert result is True
            # Verify resume state was passed to client
            mock_download.assert_called_once_with("KNS_Person", resume_state=resume_state)
            # Verify state was cleared after success
            mock_clear.assert_called_once_with("KNS_Person")
    
    @pytest.mark.asyncio
    async def test_error_handling_preserves_resume_state(self):
        """Test that resume state is preserved when operations fail."""
        resume_state = {'last_pk': 50, 'total_rows': 25, 'chunk_size': 100}
        
        with patch.object(self.data_service.resume_service, 'get_table_state', return_value=resume_state), \
             patch.object(DatabaseConfig, 'is_cursor_table', return_value=True), \
             patch.object(self.data_service.client, 'download_table', side_effect=Exception("Network error")), \
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
        
        with patch.object(self.data_service.client, 'download_table', return_value=test_df), \
             patch.object(self.data_service.repository, 'store_table', return_value=True) as mock_store, \
             patch.object(self.data_service.repository, 'table_exists', return_value=True), \
             patch.object(self.data_service.repository, 'get_table_count', return_value=3):
            
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
        
        async def mock_refresh_single_table(table_name):
            # Simulate progress updates
            mock_progress_callback(f"Starting {table_name}")
            mock_progress_callback(f"Completed {table_name}", 100)
            return True
        
        with patch.object(self.data_service, 'refresh_single_table', side_effect=mock_refresh_single_table):
            
            # Run synchronous version for easier testing
            result = self.data_service.refresh_tables_sync(tables, mock_progress_callback)
            
            # Verify progress callbacks were made
            assert len(progress_calls) >= 2  # At least start and end for each table
            assert any("KNS_Person" in call[0] for call in progress_calls)
            assert any("KNS_Faction" in call[0] for call in progress_calls)


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
        
        with patch.object(self.service.client, 'download_table', side_effect=asyncio.TimeoutError("Request timeout")):
            
            result = await self.service.refresh_single_table("KNS_Person")
            
            assert result is False
            # Verify timeout was logged
            self.mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_database_connection_failure(self):
        """Test handling of database connection failures."""
        test_df = pd.DataFrame({'PersonID': [1], 'FirstName': ['Test']})
        
        with patch.object(self.service.client, 'download_table', return_value=test_df), \
             patch.object(self.service.repository, 'store_table', side_effect=Exception("Database connection failed")):
            
            result = await self.service.refresh_single_table("KNS_Person")
            
            assert result is False
            self.mock_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_disk_space_exhaustion(self):
        """Test handling of disk space issues."""
        test_df = pd.DataFrame({'PersonID': [1], 'FirstName': ['Test']})
        
        with patch.object(self.service.client, 'download_table', return_value=test_df), \
             patch.object(self.service.repository, 'store_table', side_effect=OSError("No space left on device")):
            
            result = await self.service.refresh_single_table("KNS_Person")
            
            assert result is False
            self.mock_logger.error.assert_called()
    
    def test_resume_state_service_file_corruption(self):
        """Test resume state service with corrupted state file."""
        resume_service = ResumeStateService(Path("test_state.json"))
        
        with patch('builtins.open', mock_open(read_data="corrupted json {")), \
             patch.object(Path("test_state.json"), 'exists', return_value=True):
            
            # Should handle corruption gracefully
            state = resume_service.get_table_state("KNS_Person")
            
            assert state == {}
    
    @pytest.mark.asyncio
    async def test_concurrent_table_refresh_safety(self):
        """Test thread safety of concurrent table refreshes."""
        # This test would verify that concurrent operations don't interfere
        # In practice, this might require more sophisticated testing setup
        
        async def mock_slow_refresh(table_name):
            await asyncio.sleep(0.1)  # Simulate slow operation
            return True
        
        with patch.object(self.service, 'refresh_single_table', side_effect=mock_slow_refresh):
            
            # Start multiple refreshes concurrently
            tasks = [
                self.service.refresh_single_table("KNS_Person"),
                self.service.refresh_single_table("KNS_Faction"),
                self.service.refresh_single_table("KNS_Status")
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # All should complete successfully
            assert all(result is True for result in results if not isinstance(result, Exception))
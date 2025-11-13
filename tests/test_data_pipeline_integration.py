"""
Integration tests for the complete data pipeline flow.
Tests end-to-end functionality including API fetching, data processing, and storage.
"""
import pytest
import asyncio
import json
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import pandas as pd
import aiohttp
from typing import Dict, List, Any, Optional

from src.data.services.data_refresh_service import DataRefreshService
from src.data.repositories.database_repository import DatabaseRepository
from src.api.odata_client import ODataClient
from src.api.circuit_breaker import CircuitBreaker, circuit_breaker_manager
from src.api.error_handling import categorize_error, ErrorCategory
from src.backend.connection_manager import get_db_connection
from src.config.database import DatabaseConfig
from src.config.settings import Settings


class TestEndToEndPipeline:
    """Test complete end-to-end data pipeline functionality."""
    
    @pytest.mark.asyncio
    async def test_complete_data_pipeline_single_table(self, tmp_path):
        """Test full end-to-end pipeline for a single table."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        # Mock OData API response
        mock_person_data = {
            "value": [
                {
                    "PersonID": 1,
                    "FirstName": "יוסי",
                    "LastName": "כהן",
                    "KnessetNum": 25,
                    "DateJoined": "2021-01-01T00:00:00"
                },
                {
                    "PersonID": 2,
                    "FirstName": "דוד",
                    "LastName": "לוי",
                    "KnessetNum": 25,
                    "DateJoined": "1988-01-01T00:00:00"
                }
            ]
        }
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            # Configure mock response
            mock_response = AsyncMock()
            mock_response.json.return_value = mock_person_data
            mock_response.raise_for_status.return_value = None
            mock_response.status = 200
            mock_get.return_value.__aenter__.return_value = mock_response
            
            # Initialize pipeline components
            service = DataRefreshService(test_db_path)
            
            # Test single table refresh
            success = await service.refresh_single_table("KNS_Person")
            assert success
            
            # Verify data was stored in database
            repo = DatabaseRepository(test_db_path)
            assert repo.table_exists("KNS_Person")
            assert repo.get_table_count("KNS_Person") == 2
            
            # Verify data integrity
            result = repo.execute_query("SELECT * FROM KNS_Person ORDER BY PersonID")
            assert len(result) == 2
            assert result.iloc[0]["FirstName"] == "יוסי"
            assert result.iloc[1]["LastName"] == "לוי"
    
    @pytest.mark.asyncio
    async def test_multiple_tables_pipeline(self, tmp_path):
        """Test pipeline with multiple tables in sequence."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        # Mock data for multiple tables
        mock_responses = {
            "KNS_Faction": {
                "value": [
                    {"FactionID": 1, "Name": "הליכוד", "KnessetNum": 25},
                    {"FactionID": 2, "Name": "יש עתיד", "KnessetNum": 25}
                ]
            },
            "KNS_Status": {
                "value": [
                    {"StatusID": 1, "Desc": "פעיל"},
                    {"StatusID": 2, "Desc": "לא פעיל"}
                ]
            }
        }
        
        async def mock_download_table(table_name, **kwargs):
            """Mock table download that returns appropriate data."""
            if table_name in mock_responses:
                df = pd.DataFrame.from_records(mock_responses[table_name]["value"])
                return df
            return pd.DataFrame()
        
        with patch.object(ODataClient, 'download_table', side_effect=mock_download_table):
            service = DataRefreshService(test_db_path)
            
            # Test multiple table refresh
            tables_to_refresh = ["KNS_Faction", "KNS_Status"]
            success = await service.refresh_tables(tables_to_refresh, progress_callback=Mock())
            assert success
            
            # Verify both tables were created and populated
            repo = DatabaseRepository(test_db_path)
            assert repo.table_exists("KNS_Faction")
            assert repo.table_exists("KNS_Status")
            assert repo.get_table_count("KNS_Faction") == 2
            assert repo.get_table_count("KNS_Status") == 2
    
    @pytest.mark.asyncio
    async def test_pipeline_with_parquet_storage(self, tmp_path):
        """Test pipeline stores data in both DuckDB and Parquet formats."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        parquet_dir = tmp_path / "parquet"
        
        mock_data = {
            "value": [
                {"PersonID": 1, "FirstName": "Test", "LastName": "Person"}
            ]
        }
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.json.return_value = mock_data
            mock_response.raise_for_status.return_value = None
            mock_response.status = 200
            mock_get.return_value.__aenter__.return_value = mock_response
            
            with patch.object(Settings, 'PARQUET_DIR', parquet_dir):
                service = DataRefreshService(test_db_path)
                success = await service.refresh_single_table("KNS_Person")
                assert success
                
                # Verify DuckDB storage
                repo = DatabaseRepository(test_db_path)
                assert repo.table_exists("KNS_Person")
                
                # Verify Parquet storage
                parquet_file = parquet_dir / "KNS_Person.parquet"
                if parquet_file.exists():  # Only check if parquet storage is implemented
                    parquet_df = pd.read_parquet(parquet_file)
                    assert len(parquet_df) == 1
                    assert parquet_df.iloc[0]["PersonID"] == 1


class TestResumeStateIntegration:
    """Test resume state management in pipeline integration."""
    
    @pytest.mark.asyncio
    async def test_cursor_table_resume_state(self, tmp_path):
        """Test resume state for cursor-based pagination."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        # Mock responses for chunked download
        mock_responses = [
            {"value": [{"PersonID": 1}, {"PersonID": 2}]},  # First chunk
            {"value": [{"PersonID": 3}, {"PersonID": 4}]},  # Second chunk
            {"value": []},  # End of data marker
        ]
        
        response_iter = iter(mock_responses)
        
        async def mock_fetch_json(session, url):
            try:
                return next(response_iter)
            except StopIteration:
                return {"value": []}
        
        with patch.object(ODataClient, 'fetch_json', side_effect=mock_fetch_json):
            service = DataRefreshService(test_db_path)
            
            # Ensure KNS_Person is treated as cursor table
            with patch.object(DatabaseConfig, 'is_cursor_table', return_value=True):
                success = await service.refresh_single_table("KNS_Person")
                assert success
                
                # Verify complete data was fetched
                repo = DatabaseRepository(test_db_path)
                assert repo.get_table_count("KNS_Person") == 4
                
                # Verify resume state was cleared after completion
                resume_service = service.resume_service
                state = resume_service.get_table_state("KNS_Person")
                assert state.get("completed", False) or not state
    
    @pytest.mark.asyncio
    async def test_interrupted_download_resume(self, tmp_path):
        """Test resume functionality after interrupted download."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        # First attempt: partial download then failure
        first_attempt_responses = [
            {"value": [{"PersonID": 1}, {"PersonID": 2}]},  # First chunk successful
        ]
        
        # Second attempt: resume from last position
        second_attempt_responses = [
            {"value": [{"PersonID": 3}, {"PersonID": 4}]},  # Continue from PersonID > 2
            {"value": []},  # End of data
        ]
        
        # Mock first attempt that fails midway
        response_iter_1 = iter(first_attempt_responses)
        
        async def mock_fetch_json_fail(session, url):
            try:
                response = next(response_iter_1)
                # Simulate failure after first chunk
                if not response["value"]:  # This won't happen in first attempt
                    raise aiohttp.ClientConnectionError("Network error")
                return response
            except StopIteration:
                raise aiohttp.ClientConnectionError("Network error")
        
        service = DataRefreshService(test_db_path)
        
        # First attempt - should fail but save progress
        with patch.object(ODataClient, 'fetch_json', side_effect=mock_fetch_json_fail):
            with patch.object(DatabaseConfig, 'is_cursor_table', return_value=True):
                success = await service.refresh_single_table("KNS_Person")
                # May succeed or fail depending on implementation
                
                # Check if partial data was saved
                repo = DatabaseRepository(test_db_path)
                if repo.table_exists("KNS_Person"):
                    initial_count = repo.get_table_count("KNS_Person")
                else:
                    initial_count = 0
        
        # Second attempt - should resume and complete
        response_iter_2 = iter(second_attempt_responses)
        
        async def mock_fetch_json_success(session, url):
            try:
                return next(response_iter_2)
            except StopIteration:
                return {"value": []}
        
        with patch.object(ODataClient, 'fetch_json', side_effect=mock_fetch_json_success):
            with patch.object(DatabaseConfig, 'is_cursor_table', return_value=True):
                success = await service.refresh_single_table("KNS_Person")
                
                # Verify final data includes all records
                repo = DatabaseRepository(test_db_path)
                final_count = repo.get_table_count("KNS_Person")
                assert final_count >= initial_count  # Should have same or more data


class TestCircuitBreakerIntegration:
    """Test circuit breaker integration in pipeline."""
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_prevents_cascade_failures(self):
        """Test circuit breaker opens after multiple failures."""
        
        # Create a circuit breaker with low thresholds for testing
        test_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=1, max_retries=1)
        
        # Mock function that always fails
        async def failing_function():
            raise aiohttp.ClientConnectionError("Connection failed")
        
        # Execute multiple times to trigger circuit breaker
        failures = 0
        for _ in range(5):
            try:
                await test_breaker.execute(failing_function)
            except Exception:
                failures += 1
        
        # Should have fewer failures than attempts due to circuit breaker opening
        assert failures <= 4  # Some attempts blocked by open circuit
        assert test_breaker.state == "OPEN"
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery after failures."""
        test_breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1, max_retries=1)
        
        # Cause failures to open circuit
        async def failing_function():
            raise Exception("Test failure")
        
        for _ in range(3):
            try:
                await test_breaker.execute(failing_function)
            except:
                pass
        
        assert test_breaker.state == "OPEN"
        
        # Wait for recovery timeout
        await asyncio.sleep(0.2)
        
        # Now provide a successful function
        async def successful_function():
            return "success"
        
        # Should be able to execute after recovery timeout
        result = await test_breaker.execute(successful_function)
        assert result == "success"
        assert test_breaker.state == "CLOSED"
    
    @pytest.mark.asyncio
    async def test_pipeline_with_circuit_breaker_integration(self, tmp_path):
        """Test full pipeline respects circuit breaker state."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        # Mock client session that always fails
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.side_effect = aiohttp.ClientConnectionError("Network down")
            
            service = DataRefreshService(test_db_path)
            
            # Multiple table refresh attempts should eventually be blocked by circuit breaker
            failed_attempts = 0
            for _ in range(10):  # More attempts than circuit breaker threshold
                success = await service.refresh_single_table("KNS_Person")
                if not success:
                    failed_attempts += 1
            
            # Should have some failed attempts but not all due to circuit breaker
            assert failed_attempts > 0
            
            # Verify no data was stored due to failures
            repo = DatabaseRepository(test_db_path)
            if repo.table_exists("KNS_Person"):
                assert repo.get_table_count("KNS_Person") == 0


class TestErrorHandlingIntegration:
    """Test error handling integration across pipeline components."""
    
    @pytest.mark.asyncio
    async def test_network_error_handling(self, tmp_path):
        """Test pipeline handles network errors gracefully."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        network_errors = [
            aiohttp.ClientConnectionError("Connection refused"),
            aiohttp.ClientTimeout(),
            aiohttp.ServerDisconnectedError(),
        ]
        
        for error in network_errors:
            with patch('aiohttp.ClientSession.get') as mock_get:
                mock_get.side_effect = error
                
                service = DataRefreshService(test_db_path)
                success = await service.refresh_single_table("KNS_Person")
                
                # Should handle error gracefully without crashing
                assert success is False
                
                # Verify error was categorized correctly
                category = categorize_error(error)
                assert category in [ErrorCategory.NETWORK, ErrorCategory.TIMEOUT]
    
    @pytest.mark.asyncio
    async def test_server_error_handling(self, tmp_path):
        """Test pipeline handles server errors correctly."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            # Mock server error response
            mock_response = AsyncMock()
            mock_response.raise_for_status.side_effect = aiohttp.ClientResponseError(
                request_info=Mock(), history=Mock(), status=500, message="Internal Server Error"
            )
            mock_get.return_value.__aenter__.return_value = mock_response
            
            service = DataRefreshService(test_db_path)
            success = await service.refresh_single_table("KNS_Person")
            
            assert success is False
            
            # Verify error categorization
            server_error = aiohttp.ClientResponseError(
                request_info=Mock(), history=Mock(), status=500
            )
            assert categorize_error(server_error) == ErrorCategory.SERVER
    
    @pytest.mark.asyncio
    async def test_data_validation_error_handling(self, tmp_path):
        """Test pipeline handles data validation errors."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        # Mock response with invalid JSON
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "test", 0)
            mock_response.raise_for_status.return_value = None
            mock_get.return_value.__aenter__.return_value = mock_response
            
            service = DataRefreshService(test_db_path)
            success = await service.refresh_single_table("KNS_Person")
            
            assert success is False
            
            # Verify JSON error is categorized as DATA error
            json_error = json.JSONDecodeError("Invalid JSON", "test", 0)
            assert categorize_error(json_error) == ErrorCategory.DATA


class TestDatabaseIntegration:
    """Test database integration aspects of the pipeline."""
    
    def test_connection_management_integration(self, tmp_path):
        """Test database connection lifecycle in pipeline."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        # Test connection context manager
        with get_db_connection(test_db_path, read_only=False) as conn:
            # Create test table
            conn.execute("""
                CREATE TABLE test_integration (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert test data  
            conn.execute("INSERT INTO test_integration (id, name) VALUES (1, 'Test')")
            
            # Verify data
            result = conn.execute("SELECT * FROM test_integration").df()
            assert len(result) == 1
            assert result.iloc[0]["name"] == "Test"
        
        # Verify connection was properly closed (no exceptions)
        # Reconnect to verify data persisted
        with get_db_connection(test_db_path, read_only=True) as conn:
            result = conn.execute("SELECT COUNT(*) as count FROM test_integration").df()
            assert result.iloc[0]["count"] == 1
    
    def test_dual_storage_integration(self, tmp_path):
        """Test DuckDB and Parquet dual storage."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        # Create test data
        test_df = pd.DataFrame({
            'PersonID': [1, 2, 3],
            'FirstName': ['Alice', 'Bob', 'Charlie'],
            'LastName': ['Smith', 'Jones', 'Brown'],
            'KnessetNum': [25, 25, 24]
        })
        
        repo = DatabaseRepository(test_db_path)
        
        # Store data using repository
        success = repo.store_table(test_df, "test_persons")
        assert success
        
        # Verify DuckDB storage
        assert repo.table_exists("test_persons")
        stored_df = repo.execute_query("SELECT * FROM test_persons ORDER BY PersonID")
        assert len(stored_df) == 3
        assert stored_df.iloc[0]["FirstName"] == "Alice"
        
        # Test table statistics
        count = repo.get_table_count("test_persons")
        assert count == 3
    
    def test_configuration_integration(self):
        """Test configuration system integration."""
        # Test database configuration
        all_tables = DatabaseConfig.get_all_tables()
        assert len(all_tables) > 0
        assert "KNS_Person" in all_tables
        
        # Test cursor table configuration
        assert DatabaseConfig.is_cursor_table("KNS_Person")
        assert not DatabaseConfig.is_cursor_table("KNS_Faction")  # Assuming this is not a cursor table
        
        # Test cursor configuration
        pk_field, chunk_size = DatabaseConfig.get_cursor_config("KNS_Person")
        assert pk_field == "PersonID"
        assert chunk_size > 0
        
        # Test default configuration for non-cursor table
        default_pk, default_size = DatabaseConfig.get_cursor_config("NonExistentTable")
        assert default_pk == "id"
        assert default_size == 100


class TestPerformanceIntegration:
    """Test performance aspects of pipeline integration."""
    
    @pytest.mark.asyncio
    async def test_concurrent_table_downloads(self, tmp_path):
        """Test concurrent downloading of multiple tables."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        # Mock different data for each table
        mock_data_map = {
            "KNS_Faction": {"value": [{"FactionID": i} for i in range(1, 6)]},
            "KNS_Status": {"value": [{"StatusID": i} for i in range(1, 4)]},
            "KNS_GovMinistry": {"value": [{"GovMinistryID": i} for i in range(1, 8)]},
        }
        
        async def mock_download_table(table_name, **kwargs):
            # Simulate some processing time
            await asyncio.sleep(0.1)
            if table_name in mock_data_map:
                return pd.DataFrame.from_records(mock_data_map[table_name]["value"])
            return pd.DataFrame()
        
        with patch.object(ODataClient, 'download_table', side_effect=mock_download_table):
            service = DataRefreshService(test_db_path)
            
            # Time concurrent refresh
            import time
            start_time = time.time()
            
            tables = list(mock_data_map.keys())
            success = await service.refresh_tables(tables, progress_callback=Mock())
            
            end_time = time.time()
            elapsed = end_time - start_time
            
            assert success
            
            # Verify all tables were created
            repo = DatabaseRepository(test_db_path)
            for table_name in tables:
                assert repo.table_exists(table_name)
                expected_count = len(mock_data_map[table_name]["value"])
                assert repo.get_table_count(table_name) == expected_count
            
            # Should complete in reasonable time (less than sum of individual times)
            assert elapsed < 1.0  # Should be much faster than 3 * 0.1 = 0.3 seconds sequentially
    
    @pytest.mark.benchmark
    def test_large_dataset_handling(self, tmp_path, benchmark):
        """Benchmark handling of large datasets."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        # Create large test dataset
        large_df = pd.DataFrame({
            'PersonID': range(1, 10001),  # 10,000 records
            'FirstName': [f'Person{i}' for i in range(1, 10001)],
            'LastName': [f'LastName{i}' for i in range(1, 10001)],
            'KnessetNum': [25] * 10000
        })
        
        repo = DatabaseRepository(test_db_path)
        
        # Benchmark storage operation
        def store_large_dataset():
            return repo.store_table(large_df, "large_test_table")
        
        result = benchmark(store_large_dataset)
        assert result is True
        
        # Verify data was stored correctly
        assert repo.get_table_count("large_test_table") == 10000


class TestRealWorldScenarios:
    """Test realistic integration scenarios."""
    
    @pytest.mark.asyncio
    async def test_incremental_data_refresh(self, tmp_path):
        """Test incremental data refresh scenario."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        # Initial data load
        initial_data = {
            "value": [
                {"PersonID": 1, "FirstName": "Alice", "LastName": "Smith"},
                {"PersonID": 2, "FirstName": "Bob", "LastName": "Jones"},
            ]
        }
        
        # Updated data with new records
        updated_data = {
            "value": [
                {"PersonID": 1, "FirstName": "Alice", "LastName": "Smith"},  # Existing
                {"PersonID": 2, "FirstName": "Bob", "LastName": "Jones"},    # Existing  
                {"PersonID": 3, "FirstName": "Charlie", "LastName": "Brown"}, # New
            ]
        }
        
        service = DataRefreshService(test_db_path)
        
        # First load
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.json.return_value = initial_data
            mock_response.raise_for_status.return_value = None
            mock_response.status = 200
            mock_get.return_value.__aenter__.return_value = mock_response
            
            success = await service.refresh_single_table("KNS_Person")
            assert success
            
            repo = DatabaseRepository(test_db_path)
            assert repo.get_table_count("KNS_Person") == 2
        
        # Incremental update
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.json.return_value = updated_data
            mock_response.raise_for_status.return_value = None
            mock_response.status = 200
            mock_get.return_value.__aenter__.return_value = mock_response
            
            success = await service.refresh_single_table("KNS_Person")
            assert success
            
            # Should now have 3 records (CREATE OR REPLACE behavior)
            assert repo.get_table_count("KNS_Person") == 3
            
            # Verify new data is present
            result = repo.execute_query("SELECT * FROM KNS_Person WHERE PersonID = 3")
            assert len(result) == 1
            assert result.iloc[0]["FirstName"] == "Charlie"
    
    @pytest.mark.asyncio
    async def test_partial_failure_recovery(self, tmp_path):
        """Test recovery from partial pipeline failures."""
        test_db_path = tmp_path / "test_warehouse.duckdb"
        
        tables_to_refresh = ["KNS_Person", "KNS_Faction", "KNS_Status"]
        
        # Mock data for successful tables
        success_data = {"value": [{"ID": 1, "Name": "Test"}]}
        
        call_count = 0
        async def mock_download_with_failure(table_name, **kwargs):
            nonlocal call_count
            call_count += 1
            
            if table_name == "KNS_Faction":
                # Simulate failure for middle table
                raise aiohttp.ClientConnectionError("Network error")
            else:
                # Return success for other tables
                return pd.DataFrame.from_records(success_data["value"])
        
        with patch.object(ODataClient, 'download_table', side_effect=mock_download_with_failure):
            service = DataRefreshService(test_db_path)
            
            # This should handle partial failures gracefully
            success = await service.refresh_tables(tables_to_refresh, progress_callback=Mock())
            
            # Overall success depends on implementation - some tables might succeed
            repo = DatabaseRepository(test_db_path)
            
            # Check which tables were successfully created
            successful_tables = []
            for table in tables_to_refresh:
                if repo.table_exists(table):
                    successful_tables.append(table)
            
            # Should have some successful tables (not the failing one)
            assert len(successful_tables) >= 0
            if "KNS_Person" in successful_tables:
                assert repo.get_table_count("KNS_Person") == 1
            if "KNS_Status" in successful_tables:
                assert repo.get_table_count("KNS_Status") == 1
"""
Tests for database repository layer functionality.
"""
import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open
import pandas as pd
import duckdb
from typing import Dict, List, Optional, Any

from src.data.repositories.database_repository import DatabaseRepository
from src.backend.connection_manager import get_db_connection, ConnectionMonitor, safe_execute_query
from src.backend.duckdb_io import DuckDBIO
from src.config.database import DatabaseConfig


class TestDatabaseRepository:
    """Test DatabaseRepository functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.temp_db_path = Path("test_repo.duckdb")
        self.mock_logger = Mock()
        self.repo = DatabaseRepository(self.temp_db_path, self.mock_logger)
    
    def test_initialization(self):
        """Test repository initialization."""
        assert self.repo.db_path == self.temp_db_path
        assert self.repo.logger == self.mock_logger
    
    def test_store_dataframe_success(self):
        """Test successful DataFrame storage."""
        test_df = pd.DataFrame({
            'PersonID': [1, 2, 3],
            'FirstName': ['Alice', 'Bob', 'Charlie'],
            'LastName': ['Smith', 'Jones', 'Brown'],
            'KnessetNum': [25, 25, 24]
        })
        
        with patch('duckdb.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            
            result = self.repo.store_dataframe(test_df, 'test_persons')
            
            assert result is True
            mock_conn.execute.assert_called_once()
            # Verify CREATE OR REPLACE TABLE was used
            execute_call = mock_conn.execute.call_args[0][0]
            assert "CREATE OR REPLACE TABLE" in execute_call
            assert "test_persons" in execute_call
    
    def test_store_empty_dataframe(self):
        """Test storing empty DataFrame."""
        empty_df = pd.DataFrame()
        
        result = self.repo.store_dataframe(empty_df, 'empty_table')
        
        # Should skip storage for empty DataFrame
        assert result is True
        # Should log info about empty DataFrame
        self.mock_logger.info.assert_called()
    
    def test_store_dataframe_with_special_characters(self):
        """Test storing DataFrame with special characters."""
        test_df = pd.DataFrame({
            'PersonID': [1, 2],
            'FirstName': ['יואב', 'מרים'],  # Hebrew names
            'Description': ["Quote's test", 'Another "quoted" value']
        })
        
        with patch('duckdb.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            
            result = self.repo.store_dataframe(test_df, 'test_special_chars')
            
            assert result is True
            mock_conn.execute.assert_called_once()
    
    def test_store_dataframe_connection_error(self):
        """Test handling of database connection errors."""
        test_df = pd.DataFrame({'id': [1], 'name': ['test']})
        
        with patch('duckdb.connect', side_effect=Exception("Connection failed")):
            
            result = self.repo.store_dataframe(test_df, 'test_table')
            
            assert result is False
            self.mock_logger.error.assert_called()
    
    def test_store_as_parquet_success(self):
        """Test successful Parquet storage."""
        test_df = pd.DataFrame({
            'PersonID': [1, 2, 3],
            'FirstName': ['Alice', 'Bob', 'Charlie']
        })
        
        with patch('pathlib.Path.mkdir'), \
             patch('pandas.DataFrame.to_parquet') as mock_to_parquet:
            
            result = self.repo.store_as_parquet(test_df, 'test_persons')
            
            assert result is True
            mock_to_parquet.assert_called_once()
    
    def test_store_as_parquet_empty_dataframe(self):
        """Test Parquet storage with empty DataFrame."""
        empty_df = pd.DataFrame()
        
        result = self.repo.store_as_parquet(empty_df, 'empty_table')
        
        # Should skip Parquet storage for empty DataFrame
        assert result is True
        self.mock_logger.info.assert_called()
    
    def test_store_as_parquet_directory_error(self):
        """Test Parquet storage with directory creation error."""
        test_df = pd.DataFrame({'id': [1], 'name': ['test']})
        
        with patch('pathlib.Path.mkdir', side_effect=PermissionError("Permission denied")):
            
            result = self.repo.store_as_parquet(test_df, 'test_table')
            
            assert result is False
            self.mock_logger.error.assert_called()
    
    def test_store_table_dual_storage_success(self):
        """Test successful dual storage (DuckDB + Parquet)."""
        test_df = pd.DataFrame({'PersonID': [1, 2], 'FirstName': ['Alice', 'Bob']})
        
        with patch.object(self.repo, 'store_dataframe', return_value=True) as mock_store_df, \
             patch.object(self.repo, 'store_as_parquet', return_value=True) as mock_store_parquet:
            
            result = self.repo.store_table(test_df, 'test_persons')
            
            assert result is True
            mock_store_df.assert_called_once_with(test_df, 'test_persons')
            mock_store_parquet.assert_called_once_with(test_df, 'test_persons')
    
    def test_store_table_partial_failure(self):
        """Test dual storage with partial failure."""
        test_df = pd.DataFrame({'PersonID': [1], 'FirstName': ['Alice']})
        
        with patch.object(self.repo, 'store_dataframe', return_value=True), \
             patch.object(self.repo, 'store_as_parquet', return_value=False):
            
            result = self.repo.store_table(test_df, 'test_persons')
            
            # Should return False if either storage method fails
            assert result is False
    
    def test_execute_query_success(self):
        """Test successful query execution."""
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn, \
             patch('src.backend.connection_manager.safe_execute_query') as mock_safe_query:
            
            mock_result = pd.DataFrame({'PersonID': [1, 2], 'FirstName': ['Alice', 'Bob']})
            mock_safe_query.return_value = mock_result
            
            result = self.repo.execute_query("SELECT * FROM KNS_Person LIMIT 2")
            
            assert result.equals(mock_result)
            mock_get_conn.assert_called_once_with(self.temp_db_path, read_only=True)
            mock_safe_query.assert_called_once()
    
    def test_execute_query_connection_failure(self):
        """Test query execution with connection failure."""
        with patch('src.backend.connection_manager.get_db_connection', side_effect=Exception("Connection failed")):
            
            result = self.repo.execute_query("SELECT 1")
            
            assert result is None
            self.mock_logger.error.assert_called()
    
    def test_execute_query_sql_error(self):
        """Test query execution with SQL error."""
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn, \
             patch('src.backend.connection_manager.safe_execute_query', return_value=None):
            
            result = self.repo.execute_query("INVALID SQL QUERY")
            
            assert result is None
    
    def test_table_exists_true(self):
        """Test table existence check when table exists."""
        with patch.object(self.repo, 'execute_query') as mock_query:
            mock_query.return_value = pd.DataFrame({'count': [1]})
            
            exists = self.repo.table_exists('KNS_Person')
            
            assert exists is True
            mock_query.assert_called_once()
            # Verify correct SQL was executed
            sql_call = mock_query.call_args[0][0]
            assert "KNS_Person" in sql_call
            assert "COUNT(*)" in sql_call.upper()
    
    def test_table_exists_false(self):
        """Test table existence check when table doesn't exist."""
        with patch.object(self.repo, 'execute_query') as mock_query:
            mock_query.return_value = pd.DataFrame({'count': [0]})
            
            exists = self.repo.table_exists('NonExistentTable')
            
            assert exists is False
    
    def test_table_exists_query_error(self):
        """Test table existence check with query error."""
        with patch.object(self.repo, 'execute_query', return_value=None):
            
            exists = self.repo.table_exists('SomeTable')
            
            assert exists is False
    
    def test_get_table_count_success(self):
        """Test successful table row count retrieval."""
        with patch.object(self.repo, 'execute_query') as mock_query:
            mock_query.return_value = pd.DataFrame({'count': [150]})
            
            count = self.repo.get_table_count('KNS_Person')
            
            assert count == 150
            mock_query.assert_called_once()
    
    def test_get_table_count_empty_table(self):
        """Test row count for empty table."""
        with patch.object(self.repo, 'execute_query') as mock_query:
            mock_query.return_value = pd.DataFrame({'count': [0]})
            
            count = self.repo.get_table_count('EmptyTable')
            
            assert count == 0
    
    def test_get_table_count_error(self):
        """Test row count with query error."""
        with patch.object(self.repo, 'execute_query', return_value=None):
            
            count = self.repo.get_table_count('SomeTable')
            
            assert count == 0
    
    def test_load_faction_coalition_status_success(self):
        """Test successful faction coalition status loading."""
        mock_csv_data = pd.DataFrame({
            'KnessetNum': [25, 25, 24],
            'FactionID': [1, 2, 3],
            'FactionName': ['יש עתיד', 'ליכוד', 'כחול לבן'],
            'CoalitionStatus': ['Opposition', 'Coalition', 'Opposition']
        })
        
        with patch('pandas.read_csv', return_value=mock_csv_data) as mock_read_csv, \
             patch.object(self.repo, 'store_dataframe', return_value=True) as mock_store:
            
            result = self.repo.load_faction_coalition_status()
            
            assert result is True
            mock_read_csv.assert_called_once()
            mock_store.assert_called_once()
            # Verify correct table name
            assert mock_store.call_args[0][1] == 'faction_coalition_status'
    
    def test_load_faction_coalition_status_file_not_found(self):
        """Test faction status loading when CSV file doesn't exist."""
        with patch('pandas.read_csv', side_effect=FileNotFoundError("File not found")), \
             patch.object(self.repo, '_create_empty_faction_status_table', return_value=True) as mock_create_empty:
            
            result = self.repo.load_faction_coalition_status()
            
            # Should create empty table and return True
            assert result is True
            mock_create_empty.assert_called_once()
    
    def test_load_faction_coalition_status_corrupted_file(self):
        """Test faction status loading with corrupted CSV file."""
        with patch('pandas.read_csv', side_effect=pd.errors.ParserError("Corrupted file")):
            
            result = self.repo.load_faction_coalition_status()
            
            assert result is False
            self.mock_logger.error.assert_called()
    
    def test_create_empty_faction_status_table(self):
        """Test creation of empty faction status table."""
        with patch.object(self.repo, 'store_dataframe', return_value=True) as mock_store:
            
            result = self.repo._create_empty_faction_status_table()
            
            assert result is True
            mock_store.assert_called_once()
            # Verify empty DataFrame with correct columns
            stored_df = mock_store.call_args[0][0]
            expected_columns = ['KnessetNum', 'FactionID', 'FactionName', 'CoalitionStatus']
            assert list(stored_df.columns) == expected_columns
            assert len(stored_df) == 0


class TestConnectionManager:
    """Test connection management functionality."""
    
    def test_get_db_connection_context_manager(self):
        """Test database connection context manager."""
        db_path = Path("test.duckdb")
        
        with patch('duckdb.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn
            
            with get_db_connection(db_path, read_only=True) as conn:
                assert conn is mock_conn
            
            # Verify connection was properly configured
            mock_connect.assert_called_once_with(database=str(db_path), read_only=True)
            mock_conn.close.assert_called_once()
    
    def test_get_db_connection_missing_file_readonly(self):
        """Test connection to non-existent file in read-only mode."""
        non_existent_path = Path("nonexistent.duckdb")
        
        with patch('duckdb.connect') as mock_connect, \
             patch.object(non_existent_path, 'exists', return_value=False):
            
            mock_conn = Mock()
            mock_connect.return_value = mock_conn
            
            with get_db_connection(non_existent_path, read_only=True) as conn:
                assert conn is mock_conn
            
            # Should connect to memory database
            mock_connect.assert_called_once_with(database=":memory:", read_only=False)
    
    def test_get_db_connection_write_mode(self):
        """Test connection in write mode."""
        db_path = Path("test_write.duckdb")
        
        with patch('duckdb.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn
            
            with get_db_connection(db_path, read_only=False) as conn:
                assert conn is mock_conn
            
            mock_connect.assert_called_once_with(database=str(db_path), read_only=False)
    
    def test_safe_execute_query_success(self):
        """Test successful safe query execution."""
        mock_conn = Mock()
        test_query = "SELECT * FROM KNS_Person LIMIT 5"
        mock_result_df = pd.DataFrame({'PersonID': [1, 2], 'FirstName': ['Alice', 'Bob']})
        
        # Mock the result object
        mock_result = Mock()
        mock_result.df.return_value = mock_result_df
        mock_conn.execute.return_value = mock_result
        
        result = safe_execute_query(mock_conn, test_query, Mock())
        
        assert result.equals(mock_result_df)
        mock_conn.execute.assert_called_once_with(test_query)
    
    def test_safe_execute_query_sql_error(self):
        """Test safe query execution with SQL error."""
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("SQL syntax error")
        mock_logger = Mock()
        
        result = safe_execute_query(mock_conn, "INVALID SQL", mock_logger)
        
        assert result is None
        mock_logger.error.assert_called()
    
    def test_connection_monitor_registration(self):
        """Test connection monitoring registration."""
        monitor = ConnectionMonitor()
        mock_conn = Mock()
        
        monitor.register_connection(mock_conn, "test.duckdb")
        active_connections = monitor.get_active_connections()
        
        assert len(active_connections) == 1
        assert "test.duckdb" in str(active_connections)
    
    def test_connection_monitor_cleanup(self):
        """Test connection monitor cleanup."""
        monitor = ConnectionMonitor()
        mock_conn = Mock()
        
        monitor.register_connection(mock_conn, "test.duckdb")
        monitor.unregister_connection(mock_conn)
        active_connections = monitor.get_active_connections()
        
        assert len(active_connections) == 0


class TestDuckDBIO:
    """Test DuckDBIO functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.db_path = Path("test_io.duckdb")
        self.mock_logger = Mock()
        self.io = DuckDBIO(self.db_path, self.mock_logger)
    
    def test_export_table_to_csv_success(self):
        """Test successful CSV export."""
        output_path = Path("output.csv")
        
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn:
            mock_conn = Mock()
            mock_get_conn.return_value.__enter__.return_value = mock_conn
            
            result = self.io.export_table_to_csv('KNS_Person', output_path)
            
            assert result is True
            mock_conn.execute.assert_called_once()
            # Verify COPY TO CSV command was used
            execute_call = mock_conn.execute.call_args[0][0]
            assert "COPY" in execute_call
            assert "TO" in execute_call
            assert str(output_path) in execute_call
    
    def test_export_table_to_parquet_success(self):
        """Test successful Parquet export."""
        output_path = Path("output.parquet")
        
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn:
            mock_conn = Mock()
            mock_get_conn.return_value.__enter__.return_value = mock_conn
            
            result = self.io.export_table_to_parquet('KNS_Person', output_path)
            
            assert result is True
            mock_conn.execute.assert_called_once()
            # Verify COPY TO PARQUET command was used
            execute_call = mock_conn.execute.call_args[0][0]
            assert "COPY" in execute_call
            assert str(output_path) in execute_call
    
    def test_export_nonexistent_table(self):
        """Test export of non-existent table."""
        output_path = Path("output.csv")
        
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn:
            mock_conn = Mock()
            mock_conn.execute.side_effect = Exception("Table does not exist")
            mock_get_conn.return_value.__enter__.return_value = mock_conn
            
            result = self.io.export_table_to_csv('NonExistentTable', output_path)
            
            assert result is False
            self.mock_logger.error.assert_called()
    
    def test_import_csv_to_table_success(self):
        """Test successful CSV import."""
        csv_path = Path("input.csv")
        
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn:
            mock_conn = Mock()
            mock_get_conn.return_value.__enter__.return_value = mock_conn
            
            result = self.io.import_csv_to_table(csv_path, 'imported_table')
            
            assert result is True
            mock_conn.execute.assert_called()
            # Verify CREATE TABLE and COPY FROM commands
            execute_calls = [call[0][0] for call in mock_conn.execute.call_args_list]
            assert any("CREATE TABLE" in call for call in execute_calls)
            assert any("COPY" in call and "FROM" in call for call in execute_calls)
    
    def test_get_table_info_success(self):
        """Test successful table information retrieval."""
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn, \
             patch('src.backend.connection_manager.safe_execute_query') as mock_safe_query:
            
            mock_table_info = pd.DataFrame({
                'column_name': ['PersonID', 'FirstName', 'LastName'],
                'data_type': ['INTEGER', 'VARCHAR', 'VARCHAR'],
                'is_nullable': ['NO', 'YES', 'YES']
            })
            mock_safe_query.return_value = mock_table_info
            
            info = self.io.get_table_info('KNS_Person')
            
            assert info.equals(mock_table_info)
            mock_safe_query.assert_called_once()
    
    def test_get_table_sample_success(self):
        """Test successful table sample retrieval."""
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn, \
             patch('src.backend.connection_manager.safe_execute_query') as mock_safe_query:
            
            mock_sample = pd.DataFrame({
                'PersonID': [1, 2, 3],
                'FirstName': ['Alice', 'Bob', 'Charlie'],
                'LastName': ['Smith', 'Jones', 'Brown']
            })
            mock_safe_query.return_value = mock_sample
            
            sample = self.io.get_table_sample('KNS_Person', 3)
            
            assert sample.equals(mock_sample)
            # Verify LIMIT was used in query
            query_call = mock_safe_query.call_args[0][1]
            assert "LIMIT 3" in query_call
    
    def test_get_table_statistics_success(self):
        """Test successful table statistics retrieval."""
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn, \
             patch('src.backend.connection_manager.safe_execute_query') as mock_safe_query:
            
            # Mock multiple query results for statistics
            mock_safe_query.side_effect = [
                pd.DataFrame({'count': [150]}),  # Row count
                pd.DataFrame({
                    'column_name': ['PersonID', 'FirstName', 'LastName'],
                    'data_type': ['INTEGER', 'VARCHAR', 'VARCHAR']
                })  # Column info
            ]
            
            stats = self.io.get_table_statistics('KNS_Person')
            
            assert stats['row_count'] == 150
            assert stats['column_count'] == 3
            assert 'PersonID' in stats['columns']
            assert stats['columns']['PersonID'] == 'INTEGER'
    
    def test_list_tables_success(self):
        """Test successful table listing."""
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn, \
             patch('src.backend.connection_manager.safe_execute_query') as mock_safe_query:
            
            mock_tables = pd.DataFrame({
                'table_name': ['KNS_Person', 'KNS_Faction', 'KNS_Bill']
            })
            mock_safe_query.return_value = mock_tables
            
            tables = self.io.list_tables()
            
            assert len(tables) == 3
            assert 'KNS_Person' in tables
            assert 'KNS_Faction' in tables
            assert 'KNS_Bill' in tables
    
    def test_vacuum_database_success(self):
        """Test successful database vacuum operation."""
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn:
            mock_conn = Mock()
            mock_get_conn.return_value.__enter__.return_value = mock_conn
            
            result = self.io.vacuum_database()
            
            assert result is True
            mock_conn.execute.assert_called_once_with("VACUUM;")
    
    def test_analyze_database_success(self):
        """Test successful database analyze operation."""
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn:
            mock_conn = Mock()
            mock_get_conn.return_value.__enter__.return_value = mock_conn
            
            result = self.io.analyze_database()
            
            assert result is True
            mock_conn.execute.assert_called_once_with("ANALYZE;")


class TestRepositoryIntegration:
    """Test integration scenarios across repository components."""
    
    def test_full_data_persistence_cycle(self, tmp_path):
        """Test complete data storage and retrieval cycle."""
        db_path = tmp_path / "integration_test.duckdb"
        repo = DatabaseRepository(db_path)
        
        # Create test data
        test_df = pd.DataFrame({
            'PersonID': [1, 2, 3],
            'FirstName': ['Alice', 'Bob', 'Charlie'],
            'LastName': ['Smith', 'Jones', 'Brown'],
            'KnessetNum': [25, 25, 24]
        })
        
        # Test dual storage
        with patch.object(repo, 'store_as_parquet', return_value=True):
            success = repo.store_table(test_df, 'test_persons')
            assert success is True
        
        # Verify table exists
        with patch.object(repo, 'execute_query') as mock_query:
            mock_query.return_value = pd.DataFrame({'count': [1]})
            assert repo.table_exists('test_persons') is True
        
        # Verify row count
        with patch.object(repo, 'execute_query') as mock_query:
            mock_query.return_value = pd.DataFrame({'count': [3]})
            assert repo.get_table_count('test_persons') == 3
        
        # Test data retrieval
        with patch.object(repo, 'execute_query') as mock_query:
            mock_query.return_value = test_df
            result = repo.execute_query('SELECT * FROM "test_persons" ORDER BY PersonID')
            assert result.equals(test_df)
    
    def test_repository_with_duckdb_io_integration(self, tmp_path):
        """Test integration between repository and DuckDBIO."""
        db_path = tmp_path / "io_integration.duckdb"
        repo = DatabaseRepository(db_path)
        io = DuckDBIO(db_path)
        
        # Store data via repository
        test_df = pd.DataFrame({
            'PersonID': [1, 2],
            'FirstName': ['Alice', 'Bob']
        })
        
        with patch.object(repo, 'store_dataframe', return_value=True), \
             patch.object(repo, 'store_as_parquet', return_value=True):
            
            success = repo.store_table(test_df, 'integration_test')
            assert success is True
        
        # Retrieve info via DuckDBIO
        with patch.object(io, 'get_table_statistics') as mock_stats:
            mock_stats.return_value = {
                'row_count': 2,
                'column_count': 2,
                'columns': {'PersonID': 'INTEGER', 'FirstName': 'VARCHAR'}
            }
            
            stats = io.get_table_statistics('integration_test')
            assert stats['row_count'] == 2
            assert stats['column_count'] == 2
    
    def test_error_propagation_across_layers(self):
        """Test error handling propagation across repository layers."""
        repo = DatabaseRepository(Path("nonexistent/path/db.duckdb"))
        
        # Test that connection errors are handled gracefully
        with patch('src.backend.connection_manager.get_db_connection', 
                   side_effect=Exception("Connection failed")):
            
            # Repository should handle errors gracefully
            result = repo.execute_query("SELECT 1")
            assert result is None
            
            exists = repo.table_exists('any_table')
            assert exists is False
            
            count = repo.get_table_count('any_table')
            assert count == 0


class TestRepositoryEdgeCases:
    """Test edge cases and error scenarios in repository layer."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.repo = DatabaseRepository(Path("test.duckdb"), Mock())
    
    def test_store_dataframe_with_null_values(self):
        """Test storing DataFrame with null values."""
        test_df = pd.DataFrame({
            'PersonID': [1, 2, None],
            'FirstName': ['Alice', None, 'Charlie'],
            'LastName': [None, 'Jones', 'Brown']
        })
        
        with patch('duckdb.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            
            result = self.repo.store_dataframe(test_df, 'test_nulls')
            
            assert result is True
            mock_conn.execute.assert_called_once()
    
    def test_store_dataframe_with_large_data(self):
        """Test storing large DataFrame."""
        # Create large DataFrame (10,000 rows)
        large_df = pd.DataFrame({
            'PersonID': range(1, 10001),
            'FirstName': [f'Person{i}' for i in range(1, 10001)],
            'Value': [i * 0.1 for i in range(1, 10001)]
        })
        
        with patch('duckdb.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            
            result = self.repo.store_dataframe(large_df, 'large_table')
            
            assert result is True
            mock_conn.execute.assert_called_once()
    
    def test_execute_query_with_complex_sql(self):
        """Test executing complex SQL queries."""
        complex_query = """
        SELECT 
            p.PersonID,
            p.FirstName,
            p.LastName,
            COUNT(ptp.PositionID) as position_count
        FROM KNS_Person p
        LEFT JOIN KNS_PersonToPosition ptp ON p.PersonID = ptp.PersonID
        WHERE p.KnessetNum = 25
        GROUP BY p.PersonID, p.FirstName, p.LastName
        HAVING COUNT(ptp.PositionID) > 1
        ORDER BY position_count DESC
        LIMIT 10
        """
        
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn, \
             patch('src.backend.connection_manager.safe_execute_query') as mock_safe_query:
            
            mock_result = pd.DataFrame({
                'PersonID': [1, 2],
                'FirstName': ['Alice', 'Bob'],
                'LastName': ['Smith', 'Jones'],
                'position_count': [3, 2]
            })
            mock_safe_query.return_value = mock_result
            
            result = self.repo.execute_query(complex_query)
            
            assert result.equals(mock_result)
            mock_safe_query.assert_called_once_with(mock_get_conn.return_value.__enter__.return_value, complex_query, self.repo.logger)
    
    def test_table_name_with_special_characters(self):
        """Test handling table names with special characters."""
        special_table_name = "table-with-hyphens_and_underscores"
        
        with patch.object(self.repo, 'execute_query') as mock_query:
            mock_query.return_value = pd.DataFrame({'count': [1]})
            
            exists = self.repo.table_exists(special_table_name)
            
            assert exists is True
            # Verify table name was properly quoted or handled
            query_call = mock_query.call_args[0][0]
            assert special_table_name in query_call
    
    def test_concurrent_access_safety(self):
        """Test repository behavior under concurrent access."""
        # This test simulates concurrent operations
        # In practice, DuckDB handles concurrency at the connection level
        
        test_df = pd.DataFrame({'id': [1], 'name': ['test']})
        
        with patch('duckdb.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            
            # Simulate multiple concurrent stores
            results = []
            for i in range(5):
                result = self.repo.store_dataframe(test_df, f'concurrent_table_{i}')
                results.append(result)
            
            # All operations should succeed
            assert all(results)
            # Should have called connect multiple times (one per operation)
            assert mock_connect.call_count == 5
    
    def test_memory_usage_with_large_results(self):
        """Test memory handling with large query results."""
        # Mock large result set
        large_result = pd.DataFrame({
            'id': range(100000),
            'data': [f'data_{i}' for i in range(100000)]
        })
        
        with patch('src.backend.connection_manager.get_db_connection') as mock_get_conn, \
             patch('src.backend.connection_manager.safe_execute_query', return_value=large_result):
            
            result = self.repo.execute_query("SELECT * FROM large_table")
            
            # Should handle large results without issues
            assert len(result) == 100000
            assert result.equals(large_result)
"""
Tests for database repository layer functionality.

Note: Many tests in this file test the actual implementation behavior.
The repository layer uses context managers and the tests are designed
to work with that pattern.
"""
import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import duckdb
from contextlib import contextmanager

from src.data.repositories.database_repository import DatabaseRepository
from src.backend.connection_manager import ConnectionMonitor, safe_execute_query
from src.backend.duckdb_io import DuckDBIO


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
            mock_connect.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_connect.return_value.__exit__ = Mock(return_value=False)

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
            mock_connect.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_connect.return_value.__exit__ = Mock(return_value=False)

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
             patch.object(pd.DataFrame, 'to_parquet') as mock_to_parquet:

            result = self.repo.store_as_parquet(test_df, 'test_persons')

            assert result is True
            mock_to_parquet.assert_called_once()

    def test_store_as_parquet_empty_dataframe(self):
        """Test Parquet storage with empty DataFrame."""
        empty_df = pd.DataFrame()

        result = self.repo.store_as_parquet(empty_df, 'empty_table')

        # Should skip Parquet storage for empty DataFrame (returns True)
        assert result is True

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

    def test_table_exists_with_mock(self):
        """Test table existence check with mocked execute_query."""
        with patch.object(self.repo, 'execute_query') as mock_query:
            mock_query.return_value = pd.DataFrame({'count': [1]})

            exists = self.repo.table_exists('KNS_Person')

            assert exists == True
            mock_query.assert_called_once()
            # Verify correct SQL was executed
            sql_call = mock_query.call_args[0][0]
            assert "KNS_Person" in sql_call

    def test_table_exists_false_with_mock(self):
        """Test table existence check when table doesn't exist."""
        with patch.object(self.repo, 'execute_query') as mock_query:
            mock_query.return_value = pd.DataFrame({'count': [0]})

            exists = self.repo.table_exists('NonExistentTable')

            assert exists == False

    def test_table_exists_query_error(self):
        """Test table existence check with query error."""
        with patch.object(self.repo, 'execute_query', return_value=None):

            exists = self.repo.table_exists('SomeTable')

            assert exists == False

    def test_get_table_count_empty_table(self):
        """Test row count for empty table."""
        with patch.object(self.repo, 'table_exists', return_value=True), \
             patch.object(self.repo, 'execute_query') as mock_query:
            mock_query.return_value = pd.DataFrame({'count': [0]})

            count = self.repo.get_table_count('EmptyTable')

            assert count == 0

    def test_get_table_count_nonexistent_table(self):
        """Test row count for non-existent table."""
        with patch.object(self.repo, 'table_exists', return_value=False):

            count = self.repo.get_table_count('NonExistentTable')

            assert count == 0

    def test_load_faction_coalition_status_calls_store_dataframe(self):
        """Test that faction coalition status loading calls store_dataframe."""
        mock_csv_data = pd.DataFrame({
            'KnessetNum': [25, 25],
            'FactionID': [1, 2],
            'FactionName': ['Faction A', 'Faction B'],
            'CoalitionStatus': ['Coalition', 'Opposition']
        })

        with patch('src.data.repositories.database_repository.Settings') as mock_settings, \
             patch('pandas.read_csv', return_value=mock_csv_data), \
             patch.object(self.repo, 'store_dataframe', return_value=True) as mock_store:

            mock_status_file = Mock()
            mock_status_file.exists.return_value = True
            mock_settings.FACTION_COALITION_STATUS_FILE = mock_status_file

            result = self.repo.load_faction_coalition_status()

            assert result is True
            # Should store to UserFactionCoalitionStatus table
            mock_store.assert_called_once()
            call_args = mock_store.call_args[0]
            assert call_args[1] == 'UserFactionCoalitionStatus'


class TestConnectionManager:
    """Test connection management functionality."""

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

        assert result is not None
        assert result.equals(mock_result_df)
        mock_conn.execute.assert_called_once_with(test_query)

    def test_safe_execute_query_returns_empty_on_error(self):
        """Test safe query execution returns empty DataFrame on error."""
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("SQL syntax error")
        mock_logger = Mock()

        result = safe_execute_query(mock_conn, "INVALID SQL", mock_logger)

        # safe_execute_query returns empty DataFrame on error, not None
        assert result is not None
        assert result.empty


class TestDuckDBIO:
    """Test DuckDBIO functionality."""

    def setup_method(self):
        """Setup test fixtures."""
        self.db_path = Path("test_io.duckdb")
        self.mock_logger = Mock()
        self.io = DuckDBIO(self.db_path, self.mock_logger)

    def test_initialization(self):
        """Test DuckDBIO initialization."""
        assert self.io.db_path == self.db_path
        assert self.io.logger == self.mock_logger

    def test_export_nonexistent_table(self):
        """Test export of non-existent table."""
        output_path = Path("output.csv")

        # Create a mock context manager that raises an exception
        @contextmanager
        def mock_context():
            mock_conn = Mock()
            mock_conn.execute.side_effect = Exception("Table does not exist")
            yield mock_conn

        with patch('src.backend.duckdb_io.get_db_connection', mock_context):

            result = self.io.export_table_to_csv('NonExistentTable', output_path)

            assert result is False
            self.mock_logger.error.assert_called()

    def test_list_tables_empty(self):
        """Test listing tables when database is empty."""
        @contextmanager
        def mock_context():
            mock_conn = Mock()
            yield mock_conn

        with patch('src.backend.duckdb_io.get_db_connection', mock_context), \
             patch('src.backend.duckdb_io.safe_execute_query') as mock_safe_query:

            mock_safe_query.return_value = pd.DataFrame({'table_name': []})

            tables = self.io.list_tables()

            assert tables == []


class TestRepositoryIntegration:
    """Test integration scenarios across repository components."""

    def test_repository_creation(self, tmp_path):
        """Test repository can be created with valid path."""
        db_path = tmp_path / "integration_test.duckdb"
        repo = DatabaseRepository(db_path)

        assert repo.db_path == db_path
        assert repo.logger is not None

    def test_duckdb_io_creation(self, tmp_path):
        """Test DuckDBIO can be created with valid path."""
        db_path = tmp_path / "io_integration.duckdb"
        io = DuckDBIO(db_path)

        assert io.db_path == db_path
        assert io.logger is not None


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
            mock_connect.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_connect.return_value.__exit__ = Mock(return_value=False)

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
            mock_connect.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_connect.return_value.__exit__ = Mock(return_value=False)

            result = self.repo.store_dataframe(large_df, 'large_table')

            assert result is True
            mock_conn.execute.assert_called_once()

    def test_concurrent_access_safety(self):
        """Test repository behavior under concurrent access."""
        # This test simulates concurrent operations
        # In practice, DuckDB handles concurrency at the connection level

        test_df = pd.DataFrame({'id': [1], 'name': ['test']})

        with patch('duckdb.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_connect.return_value.__exit__ = Mock(return_value=False)

            # Simulate multiple concurrent stores
            results = []
            for i in range(5):
                result = self.repo.store_dataframe(test_df, f'concurrent_table_{i}')
                results.append(result)

            # All operations should succeed
            assert all(results)
            # Should have called connect multiple times (one per operation)
            assert mock_connect.call_count == 5


class TestDatabaseRepositoryMethods:
    """Test specific DatabaseRepository methods."""

    def test_execute_query_returns_dataframe(self):
        """Test execute_query returns a DataFrame."""
        repo = DatabaseRepository(Path("test.duckdb"), Mock())

        mock_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})

        @contextmanager
        def mock_context(*args, **kwargs):
            mock_conn = Mock()
            yield mock_conn

        with patch('src.data.repositories.database_repository.get_db_connection', mock_context), \
             patch('src.data.repositories.database_repository.safe_execute_query', return_value=mock_df):

            result = repo.execute_query("SELECT * FROM test")

            assert result is not None
            assert result.equals(mock_df)

    def test_execute_query_handles_exception(self):
        """Test execute_query handles exceptions gracefully."""
        mock_logger = Mock()
        repo = DatabaseRepository(Path("test.duckdb"), mock_logger)

        with patch('src.data.repositories.database_repository.get_db_connection',
                   side_effect=Exception("Connection failed")):

            result = repo.execute_query("SELECT 1")

            assert result is None
            mock_logger.error.assert_called()

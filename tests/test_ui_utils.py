import pytest
import pandas as pd
from pathlib import Path
from unittest import mock
import duckdb
from datetime import datetime, timezone

# Import functions to be tested
from src.ui.ui_utils import (
    connect_db,
    safe_execute_query,
    get_db_table_list,
    get_table_columns,
    get_filter_options_from_db,
    format_exception_for_ui,
    human_readable_timestamp,
    get_last_updated_for_table,
)


@pytest.fixture(autouse=True)
def reset_mocks():
    """Reset mocks before each test."""
    yield


# NOTE: mock_logger and temp_db_path fixtures are provided by conftest.py


class TestConnectDb:
    @mock.patch('src.ui.ui_utils.duckdb.connect')
    @mock.patch('src.ui.ui_utils.st')
    def test_connect_db_existing_file(self, mock_st, mock_duckdb_connect, tmp_path):
        """Test connect_db when the database file exists."""
        db_path = tmp_path / "test.db"
        db_path.touch()  # Create the file
        mock_connection = mock.MagicMock()
        mock_duckdb_connect.return_value = mock_connection

        conn = connect_db(db_path)

        mock_duckdb_connect.assert_called_once_with(database=db_path.as_posix(), read_only=True)
        assert conn == mock_connection

    @mock.patch('src.ui.ui_utils.duckdb.connect')
    @mock.patch('src.ui.ui_utils.st')
    def test_connect_db_non_existing_file_readonly(self, mock_st, mock_duckdb_connect, tmp_path):
        """Test connect_db when the database file does not exist in readonly mode."""
        db_path = tmp_path / "nonexistent.db"

        conn = connect_db(db_path, read_only=True)

        # Should return in-memory connection as fallback
        mock_st.warning.assert_called()
        assert conn is not None  # Returns in-memory connection

    @mock.patch('src.ui.ui_utils.duckdb.connect')
    @mock.patch('src.ui.ui_utils.st')
    def test_connect_db_exception(self, mock_st, mock_duckdb_connect, tmp_path):
        """Test connect_db when duckdb.connect raises an exception."""
        db_path = tmp_path / "test.db"
        db_path.touch()
        error_message = "Connection failed"

        # Mock to raise exception on first call (main connection),
        # but return a mock connection on second call (fallback in-memory)
        mock_fallback_conn = mock.MagicMock()
        mock_duckdb_connect.side_effect = [Exception(error_message), mock_fallback_conn]

        conn = connect_db(db_path)

        mock_st.error.assert_called()
        # Returns in-memory connection as fallback
        assert conn is mock_fallback_conn


class TestSafeExecuteQuery:
    @mock.patch('src.ui.ui_utils.st')
    def test_safe_execute_query_success(self, mock_st):
        """Test safe_execute_query successfully returns a DataFrame."""
        mock_conn = mock.MagicMock(spec=duckdb.DuckDBPyConnection)
        mock_df = pd.DataFrame({'col1': [1, 2]})
        mock_conn.execute.return_value.df.return_value = mock_df
        query = "SELECT * FROM dummy_table"

        df = safe_execute_query(mock_conn, query)

        mock_conn.execute.assert_called_once_with(query)
        pd.testing.assert_frame_equal(df, mock_df)

    @mock.patch('src.ui.ui_utils.st')
    def test_safe_execute_query_exception(self, mock_st):
        """Test safe_execute_query when an exception occurs during query execution."""
        mock_conn = mock.MagicMock(spec=duckdb.DuckDBPyConnection)
        error_message = "Query execution failed"
        mock_conn.execute.side_effect = Exception(error_message)
        query = "SELECT * FROM dummy_table"

        df = safe_execute_query(mock_conn, query)

        mock_conn.execute.assert_called_once_with(query)
        assert df.empty
        mock_st.error.assert_called()


class TestGetDbTableList:
    @mock.patch('src.ui.ui_utils.get_db_connection')
    @mock.patch('src.ui.ui_utils.safe_execute_query')
    @mock.patch('src.ui.ui_utils.st')
    def test_get_db_table_list_success(self, mock_st, mock_safe_execute, mock_get_conn, tmp_path):
        """Test get_db_table_list successfully returns a list of tables."""
        db_path = tmp_path / "test.db"
        db_path.touch()

        mock_conn = mock.MagicMock()
        mock_get_conn.return_value.__enter__ = mock.MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = mock.MagicMock(return_value=False)

        mock_tables_df = pd.DataFrame({'name': ['table1', 'table2']})
        mock_safe_execute.return_value = mock_tables_df

        tables = get_db_table_list(db_path)

        assert tables == ['table1', 'table2']

    @mock.patch('src.ui.ui_utils.st')
    def test_get_db_table_list_db_not_exists(self, mock_st, tmp_path):
        """Test get_db_table_list when the database file does not exist."""
        db_path = tmp_path / "nonexistent.db"

        tables = get_db_table_list(db_path)

        assert tables == []


class TestGetTableColumns:
    @mock.patch('src.ui.ui_utils.get_db_connection')
    @mock.patch('src.ui.ui_utils.safe_execute_query')
    @mock.patch('src.ui.ui_utils.st')
    def test_get_table_columns_success(self, mock_st, mock_safe_execute, mock_get_conn, tmp_path):
        """Test get_table_columns successfully returns column lists."""
        db_path = tmp_path / "test.db"
        db_path.touch()

        mock_conn = mock.MagicMock()
        mock_get_conn.return_value.__enter__ = mock.MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = mock.MagicMock(return_value=False)

        mock_columns_df = pd.DataFrame({
            'name': ['id', 'name', 'value'],
            'type': ['INTEGER', 'VARCHAR', 'DOUBLE']
        })
        mock_safe_execute.return_value = mock_columns_df

        all_cols, numeric_cols, cat_cols = get_table_columns(db_path, "test_table")

        assert all_cols == ['id', 'name', 'value']
        assert numeric_cols == ['id', 'value']
        assert cat_cols == ['name']

    @mock.patch('src.ui.ui_utils.st')
    def test_get_table_columns_empty_table_name(self, mock_st, tmp_path):
        """Test get_table_columns with empty table name."""
        db_path = tmp_path / "test.db"
        db_path.touch()

        all_cols, numeric_cols, cat_cols = get_table_columns(db_path, "")

        assert all_cols == []
        assert numeric_cols == []
        assert cat_cols == []

    @mock.patch('src.ui.ui_utils.st')
    def test_get_table_columns_db_not_exists(self, mock_st, tmp_path):
        """Test get_table_columns when database doesn't exist."""
        db_path = tmp_path / "nonexistent.db"

        all_cols, numeric_cols, cat_cols = get_table_columns(db_path, "test_table")

        assert all_cols == []
        assert numeric_cols == []
        assert cat_cols == []


class TestGetFilterOptionsFromDb:
    @mock.patch('src.ui.ui_utils.get_db_connection')
    @mock.patch('src.ui.ui_utils.safe_execute_query')
    @mock.patch('src.ui.ui_utils.st')
    def test_get_filter_options_success(self, mock_st, mock_safe_execute, mock_get_conn, tmp_path):
        """Test get_filter_options_from_db returns correct data."""
        db_path = tmp_path / "test.db"
        db_path.touch()

        mock_conn = mock.MagicMock()
        mock_get_conn.return_value.__enter__ = mock.MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = mock.MagicMock(return_value=False)

        # First call returns Knesset nums
        knesset_df = pd.DataFrame({'KnessetNum': [25, 24, 23]})
        # Second call returns table list
        tables_df = pd.DataFrame({'table_name': ['kns_faction', 'userfactioncoalitionstatus']})
        # Third call returns factions
        factions_df = pd.DataFrame({
            'FactionName': ['Likud', 'Yesh Atid'],
            'FactionID': [1, 2],
            'KnessetNum': [25, 25]
        })
        mock_safe_execute.side_effect = [knesset_df, tables_df, factions_df]

        knesset_nums, factions = get_filter_options_from_db(db_path)

        assert knesset_nums == [25, 24, 23]
        pd.testing.assert_frame_equal(factions, factions_df)

    @mock.patch('src.ui.ui_utils.st')
    def test_get_filter_options_db_not_exists(self, mock_st, tmp_path):
        """Test get_filter_options_from_db when database doesn't exist."""
        db_path = tmp_path / "nonexistent.db"

        knesset_nums, factions = get_filter_options_from_db(db_path)

        assert knesset_nums == []
        assert factions.empty


class TestFormatExceptionForUi:
    def test_format_exception_with_exception(self):
        """Test format_exception_for_ui with an exception."""
        try:
            raise ValueError("Test error message")
        except ValueError:
            import sys
            result = format_exception_for_ui(sys.exc_info())

        assert "ValueError" in result
        assert "Test error message" in result

    def test_format_exception_no_exception(self):
        """Test format_exception_for_ui when no exception."""
        result = format_exception_for_ui((None, None, None))

        assert result == "No exception information available."


class TestHumanReadableTimestamp:
    def test_timestamp_from_float(self):
        """Test human_readable_timestamp with a float timestamp."""
        ts = datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc).timestamp()
        result = human_readable_timestamp(ts)

        assert "2023-01-15" in result
        assert "UTC" in result

    def test_timestamp_from_datetime(self):
        """Test human_readable_timestamp with a datetime object."""
        dt = datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = human_readable_timestamp(dt)

        assert "2023-01-15" in result
        assert "10:30:00" in result
        assert "UTC" in result

    def test_timestamp_none_value(self):
        """Test human_readable_timestamp with None."""
        result = human_readable_timestamp(None)

        assert result == "N/A"

    def test_timestamp_invalid_input(self):
        """Test human_readable_timestamp with invalid input returns original value as string."""
        result = human_readable_timestamp("invalid_date_string")

        # Function returns the original string on parse failure
        assert "invalid_date_string" in result or "Invalid" in result


class TestGetLastUpdatedForTable:
    def test_file_exists(self, tmp_path):
        """Test get_last_updated_for_table when parquet file exists."""
        parquet_dir = tmp_path
        table_name = "test_table"
        parquet_file = parquet_dir / f"{table_name}.parquet"
        parquet_file.touch()  # Create the file

        result = get_last_updated_for_table(parquet_dir, table_name)

        assert "UTC" in result
        # Should contain today's date or recent date
        assert result != "Never (or N/A)"

    def test_file_not_exists(self, tmp_path):
        """Test get_last_updated_for_table when parquet file doesn't exist."""
        parquet_dir = tmp_path
        table_name = "nonexistent_table"

        result = get_last_updated_for_table(parquet_dir, table_name)

        assert result == "Never (or N/A)"

    def test_with_logger(self, tmp_path, mock_logger):
        """Test get_last_updated_for_table with a logger."""
        parquet_dir = tmp_path
        table_name = "test_table"
        parquet_file = parquet_dir / f"{table_name}.parquet"
        parquet_file.touch()

        result = get_last_updated_for_table(parquet_dir, table_name, mock_logger)

        assert "UTC" in result

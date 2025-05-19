import pytest
import pandas as pd
from pathlib import Path
from unittest import mock
import duckdb
import streamlit as st
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

# Mock streamlit globally for all tests in this file
st_mock = mock.MagicMock()

# It's common to mock streamlit functions at the module level if they are widely used
st.cache_data = mock.MagicMock(wraps=lambda func: func) # Mock cache_data to just run the function
st.error = st_mock.error
st.info = st_mock.info
st.warning = st_mock.warning
st.sidebar = mock.MagicMock()
st.sidebar.error = st_mock.sidebar.error

class TestUiUtils:
    @mock.patch('src.ui.ui_utils.duckdb.connect')
    @mock.patch('src.ui.ui_utils.Path')
    def test_connect_db_existing_file(self, mock_path_class, mock_duckdb_connect):
        """Test connect_db when the database file exists."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = True
        mock_connection = mock.MagicMock()
        mock_duckdb_connect.return_value = mock_connection

        conn = connect_db(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        mock_duckdb_connect.assert_called_once_with(database=db_path_str, read_only=True)
        st_mock.info.assert_called_once_with(f"Connecting to database: {db_path_str}")
        assert conn == mock_connection

    @mock.patch('src.ui.ui_utils.duckdb.connect')
    @mock.patch('src.ui.ui_utils.Path')
    def test_connect_db_non_existing_file(self, mock_path_class, mock_duckdb_connect):
        """Test connect_db when the database file does not exist."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = False

        conn = connect_db(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        st_mock.warning.assert_called_once_with(f"Database file not found at {db_path_str}. App may not function as expected.")
        mock_duckdb_connect.assert_not_called()
        assert conn is None

    @mock.patch('src.ui.ui_utils.duckdb.connect')
    @mock.patch('src.ui.ui_utils.Path')
    def test_connect_db_exception(self, mock_path_class, mock_duckdb_connect):
        """Test connect_db when duckdb.connect raises an exception."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = True
        error_message = "Connection failed"
        mock_duckdb_connect.side_effect = Exception(error_message)

        conn = connect_db(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        mock_duckdb_connect.assert_called_once_with(database=db_path_str, read_only=True)
        st_mock.error.assert_called_once_with(f"Failed to connect to database {db_path_str}: {error_message}")
        assert conn is None

    def test_safe_execute_query_success(self):
        """Test safe_execute_query successfully returns a DataFrame."""
        mock_conn = mock.MagicMock(spec=duckdb.DuckDBPyConnection)
        mock_df = pd.DataFrame({'col1': [1, 2]})
        mock_conn.execute.return_value.fetchdf.return_value = mock_df
        query = "SELECT * FROM dummy_table"

        df = safe_execute_query(mock_conn, query)

        mock_conn.execute.assert_called_once_with(query)
        pd.testing.assert_frame_equal(df, mock_df)
        st_mock.error.assert_not_called()

    def test_safe_execute_query_no_connection(self):
        """Test safe_execute_query when connection is None."""
        query = "SELECT * FROM dummy_table"
        df = safe_execute_query(None, query)
        assert df.empty
        st_mock.error.assert_called_once_with("Database connection is not available.")

    def test_safe_execute_query_exception(self):
        """Test safe_execute_query when an exception occurs during query execution."""
        mock_conn = mock.MagicMock(spec=duckdb.DuckDBPyConnection)
        error_message = "Query execution failed"
        mock_conn.execute.side_effect = Exception(error_message)
        query = "SELECT * FROM dummy_table"

        df = safe_execute_query(mock_conn, query)

        mock_conn.execute.assert_called_once_with(query)
        assert df.empty
        st_mock.error.assert_called_once_with(f"Failed to execute query: {query}. Error: {error_message}")

    @mock.patch('src.ui.ui_utils.connect_db')
    @mock.patch('src.ui.ui_utils.safe_execute_query')
    @mock.patch('src.ui.ui_utils.Path')
    def test_get_db_table_list_success(self, mock_path_class, mock_safe_execute_query, mock_connect_db):
        """Test get_db_table_list successfully returns a list of tables."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = True
        mock_conn = mock.MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_tables_df = pd.DataFrame({'name': ['table1', 'table2']})
        mock_safe_execute_query.return_value = mock_tables_df

        tables = get_db_table_list(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        mock_connect_db.assert_called_once_with(db_path_str)
        mock_safe_execute_query.assert_called_once_with(mock_conn, "SHOW TABLES;")
        assert tables == ['table1', 'table2']
        mock_conn.close.assert_called_once()
        st_mock.sidebar.error.assert_not_called()

    @mock.patch('src.ui.ui_utils.Path')
    def test_get_db_table_list_db_not_exists(self, mock_path_class):
        """Test get_db_table_list when the database file does not exist."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = False

        tables = get_db_table_list(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        assert tables == []
        st_mock.sidebar.error.assert_called_once_with(f"Database file not found at {db_path_str}")

    @mock.patch('src.ui.ui_utils.connect_db')
    @mock.patch('src.ui.ui_utils.safe_execute_query')
    @mock.patch('src.ui.ui_utils.Path')
    def test_get_db_table_list_query_fails(self, mock_path_class, mock_safe_execute_query, mock_connect_db):
        """Test get_db_table_list when safe_execute_query returns an empty DataFrame."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = True
        mock_conn = mock.MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_safe_execute_query.return_value = pd.DataFrame() # Query fails or returns no tables

        tables = get_db_table_list(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        mock_connect_db.assert_called_once_with(db_path_str)
        mock_safe_execute_query.assert_called_once_with(mock_conn, "SHOW TABLES;")
        assert tables == []
        mock_conn.close.assert_called_once()
        st_mock.sidebar.error.assert_not_called() # No error if query is 'successful' but returns no data

    @mock.patch('src.ui.ui_utils.connect_db')
    @mock.patch('src.ui.ui_utils.safe_execute_query')
    @mock.patch('src.ui.ui_utils.Path')
    def test_get_db_table_list_connect_fails(self, mock_path_class, mock_safe_execute_query, mock_connect_db):
        """Test get_db_table_list when connect_db returns None."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = True
        mock_connect_db.return_value = None # Connection fails

        tables = get_db_table_list(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        mock_connect_db.assert_called_once_with(db_path_str)
        mock_safe_execute_query.assert_not_called()
        assert tables == []
        # Error is handled and logged by connect_db, not directly here
        st_mock.sidebar.error.assert_not_called()

    @mock.patch('src.ui.ui_utils.connect_db')
    @mock.patch('src.ui.ui_utils.safe_execute_query')
    @mock.patch('src.ui.ui_utils.Path')
    def test_get_table_columns_success(self, mock_path_class, mock_safe_execute_query, mock_connect_db):
        """Test get_table_columns successfully returns column information."""
        db_path_str = "dummy.db"
        table_name = "dummy_table"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = True
        mock_conn = mock.MagicMock()
        mock_connect_db.return_value = mock_conn
        # Simulate PRAGMA table_info output
        mock_columns_df = pd.DataFrame({
            'name': ['col_int', 'col_float', 'col_str', 'col_bool', 'col_date'],
            'type': ['INTEGER', 'DOUBLE', 'VARCHAR', 'BOOLEAN', 'DATE']
        })
        mock_safe_execute_query.return_value = mock_columns_df

        all_cols, num_cols, cat_cols = get_table_columns(db_path_str, table_name)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        mock_connect_db.assert_called_once_with(db_path_str)
        mock_safe_execute_query.assert_called_once_with(mock_conn, f"PRAGMA table_info('{table_name}');")
        assert all_cols == ['col_int', 'col_float', 'col_str', 'col_bool', 'col_date']
        assert num_cols == ['col_int', 'col_float']
        assert cat_cols == ['col_str', 'col_bool', 'col_date'] # Assuming DATE is treated as categorical here
        mock_conn.close.assert_called_once()

    @mock.patch('src.ui.ui_utils.Path')
    def test_get_table_columns_db_not_exists(self, mock_path_class):
        """Test get_table_columns when the database file does not exist."""
        db_path_str = "dummy.db"
        table_name = "dummy_table"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = False

        all_cols, num_cols, cat_cols = get_table_columns(db_path_str, table_name)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        assert all_cols == []
        assert num_cols == []
        assert cat_cols == []
        st_mock.error.assert_called_once_with(f"Database file not found at {db_path_str}")


    def test_get_table_columns_empty_table_name(self):
        """Test get_table_columns with an empty table name."""
        db_path_str = "dummy.db"
        all_cols, num_cols, cat_cols = get_table_columns(db_path_str, "")
        assert all_cols == []
        assert num_cols == []
        assert cat_cols == []
        st_mock.error.assert_called_once_with("Table name cannot be empty.")

    @mock.patch('src.ui.ui_utils.connect_db')
    @mock.patch('src.ui.ui_utils.safe_execute_query')
    @mock.patch('src.ui.ui_utils.Path')
    def test_get_table_columns_query_fails(self, mock_path_class, mock_safe_execute_query, mock_connect_db):
        """Test get_table_columns when safe_execute_query returns an empty DataFrame."""
        db_path_str = "dummy.db"
        table_name = "dummy_table"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = True
        mock_conn = mock.MagicMock()
        mock_connect_db.return_value = mock_conn
        mock_safe_execute_query.return_value = pd.DataFrame() # Query fails or returns no columns

        all_cols, num_cols, cat_cols = get_table_columns(db_path_str, table_name)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_connect_db.assert_called_once_with(db_path_str)
        mock_safe_execute_query.assert_called_once_with(mock_conn, f"PRAGMA table_info('{table_name}');")
        assert all_cols == []
        assert num_cols == []
        assert cat_cols == []
        mock_conn.close.assert_called_once()

    @mock.patch('src.ui.ui_utils.connect_db')
    @mock.patch('src.ui.ui_utils.safe_execute_query')
    @mock.patch('src.ui.ui_utils.Path')
    def test_get_filter_options_from_db_success(self, mock_path_class, mock_safe_execute_query, mock_connect_db):
        """Test get_filter_options_from_db successfully returns filter options."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = True
        mock_conn = mock.MagicMock()
        mock_connect_db.return_value = mock_conn

        mock_knesset_nums_df = pd.DataFrame({'KnessetNum': [23, 24, 25]})
        mock_factions_df = pd.DataFrame({'FactionName': ['Likud', 'Yesh Atid'], 'KnessetNum': [25, 25]})

        # Simulate the behavior of safe_execute_query for different queries
        def safe_execute_query_side_effect(conn, query):
            if "SELECT DISTINCT KnessetNum FROM factions_main ORDER BY KnessetNum DESC" in query:
                return mock_knesset_nums_df
            elif "SELECT DISTINCT FactionName, KnessetNum FROM factions_main" in query: # simplified
                return mock_factions_df
            return pd.DataFrame()

        mock_safe_execute_query.side_effect = safe_execute_query_side_effect

        knesset_nums, factions_df = get_filter_options_from_db(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        mock_connect_db.assert_called_once_with(db_path_str)
        assert mock_safe_execute_query.call_count == 2 # Called for KnessetNum and Factions
        assert knesset_nums == [25, 24, 23] # Check for reverse sorting
        pd.testing.assert_frame_equal(factions_df, mock_factions_df)
        mock_conn.close.assert_called_once()
        st_mock.error.assert_not_called()

    @mock.patch('src.ui.ui_utils.Path')
    def test_get_filter_options_from_db_not_exists(self, mock_path_class):
        """Test get_filter_options_from_db when the database file does not exist."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = False

        knesset_nums, factions_df = get_filter_options_from_db(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        assert knesset_nums == []
        assert factions_df.empty
        st_mock.error.assert_called_once_with(f"Database file not found at {db_path_str}")

    @mock.patch('src.ui.ui_utils.connect_db')
    @mock.patch('src.ui.ui_utils.safe_execute_query')
    @mock.patch('src.ui.ui_utils.Path')
    def test_get_filter_options_from_db_knesset_query_fails(self, mock_path_class, mock_safe_execute_query, mock_connect_db):
        """Test get_filter_options_from_db when the KnessetNum query fails."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = True
        mock_conn = mock.MagicMock()
        mock_connect_db.return_value = mock_conn

        # KnessetNum query returns empty
        mock_safe_execute_query.side_effect = [pd.DataFrame(), pd.DataFrame({'FactionName': ['Likud']})]

        knesset_nums, factions_df = get_filter_options_from_db(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_connect_db.assert_called_once_with(db_path_str)
        assert knesset_nums == [] # Should be empty
        # Factions df might still be populated depending on logic, here we assume it would be empty or not useful
        # For this test, we focus on knesset_nums being empty as a result of the first query failing
        assert factions_df.empty # Given the side effect, the second query will also return empty
        mock_conn.close.assert_called_once()

    @mock.patch('src.ui.ui_utils.connect_db')
    @mock.patch('src.ui.ui_utils.safe_execute_query')
    @mock.patch('src.ui.ui_utils.Path')
    def test_get_filter_options_from_db_connect_fails(self, mock_path_class, mock_safe_execute_query, mock_connect_db):
        """Test get_filter_options_from_db when connect_db returns None."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = True
        mock_connect_db.return_value = None # Connection fails

        knesset_nums, factions_df = get_filter_options_from_db(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_connect_db.assert_called_once_with(db_path_str)
        mock_safe_execute_query.assert_not_called()
        assert knesset_nums == []
        assert factions_df.empty

    def test_format_exception_for_ui_with_exception(self):
        """Test format_exception_for_ui with a real exception."""
        try:
            raise ValueError("Test error message")
        except ValueError as e:
            exc_info = (type(e), e, e.__traceback__)
            output = format_exception_for_ui(exc_info)
            assert "ValueError" in output
            assert "Test error message" in output
            assert "Traceback (most recent call last):" in output # Check for traceback presence

    def test_format_exception_for_ui_with_none(self):
        """Test format_exception_for_ui with None as input."""
        output = format_exception_for_ui(None)
        assert output == "No exception information available."

    def test_format_exception_for_ui_with_none_tuple(self):
        """Test format_exception_for_ui with (None, None, None) as input."""
        output = format_exception_for_ui((None, None, None))
        assert output == "No exception information available."

    def test_human_readable_timestamp_none_or_nat(self):
        """Test human_readable_timestamp with None or NaT inputs."""
        assert human_readable_timestamp(None) == "N/A"
        assert human_readable_timestamp(pd.NaT) == "N/A"

    def test_human_readable_timestamp_numeric(self):
        """Test human_readable_timestamp with numeric (Unix timestamp) inputs."""
        # Test with an integer timestamp
        ts_int = 1678886400  # Corresponds to 2023-03-15 12:00:00 UTC
        expected_dt_int = datetime.fromtimestamp(ts_int, timezone.utc)
        expected_str_int = expected_dt_int.strftime('%Y-%m-%d %H:%M:%S %Z')
        assert human_readable_timestamp(ts_int) == expected_str_int

        # Test with a float timestamp
        ts_float = 1678886400.123
        expected_dt_float = datetime.fromtimestamp(ts_float, timezone.utc)
        expected_str_float = expected_dt_float.strftime('%Y-%m-%d %H:%M:%S %Z')
        assert human_readable_timestamp(ts_float) == expected_str_float

    def test_human_readable_timestamp_iso_string(self):
        """Test human_readable_timestamp with ISO format string inputs."""
        iso_str_utc = "2023-03-15T12:00:00Z"
        expected_dt_utc = datetime(2023, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        expected_str_utc = expected_dt_utc.strftime('%Y-%m-%d %H:%M:%S %Z')
        assert human_readable_timestamp(iso_str_utc) == expected_str_utc

        iso_str_offset = "2023-03-15T15:00:00+03:00" # Same as 12:00:00 UTC
        expected_dt_offset = datetime(2023, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        expected_str_offset = expected_dt_offset.strftime('%Y-%m-%d %H:%M:%S %Z')
        assert human_readable_timestamp(iso_str_offset) == expected_str_offset

    def test_human_readable_timestamp_datetime_object(self):
        """Test human_readable_timestamp with datetime objects."""
        dt_naive = datetime(2023, 3, 15, 12, 0, 0) # Assumed local, should be converted to UTC
        dt_utc = dt_naive.replace(tzinfo=timezone.utc) # Make it UTC for expectation
        expected_str_naive = dt_utc.strftime('%Y-%m-%d %H:%M:%S %Z')
        assert human_readable_timestamp(dt_naive) == expected_str_naive


        dt_aware_utc = datetime(2023, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        expected_str_aware_utc = dt_aware_utc.strftime('%Y-%m-%d %H:%M:%S %Z')
        assert human_readable_timestamp(dt_aware_utc) == expected_str_aware_utc


    def test_human_readable_timestamp_pandas_timestamp(self):
        """Test human_readable_timestamp with pandas Timestamp objects."""
        pd_ts_naive = pd.Timestamp("2023-03-15 12:00:00")
        # Pandas Timestamps are often timezone-aware by default or easily converted
        pd_ts_utc = pd_ts_naive.tz_localize('UTC')
        expected_str_pd_utc = pd_ts_utc.strftime('%Y-%m-%d %H:%M:%S %Z')
        assert human_readable_timestamp(pd_ts_naive) == expected_str_pd_utc # Naive converted to UTC

        pd_ts_aware = pd.Timestamp("2023-03-15 12:00:00", tz="UTC")
        expected_str_pd_aware = pd_ts_aware.strftime('%Y-%m-%d %H:%M:%S %Z')
        assert human_readable_timestamp(pd_ts_aware) == expected_str_pd_aware


    def test_human_readable_timestamp_invalid_input(self):
        """Test human_readable_timestamp with invalid input types."""
        assert human_readable_timestamp("invalid_date_string") == "Invalid timestamp"
        assert human_readable_timestamp([1, 2, 3]) == "Invalid timestamp"
        assert human_readable_timestamp({}) == "Invalid timestamp"

    @mock.patch('src.ui.ui_utils.Path')
    @mock.patch('src.ui.ui_utils.human_readable_timestamp')
    def test_get_last_updated_for_table_file_exists(self, mock_human_readable_timestamp, mock_path_class):
        """Test get_last_updated_for_table when the file exists."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = True
        mock_timestamp = 1678886400  # Example Unix timestamp
        mock_db_path_instance.stat.return_value.st_mtime = mock_timestamp
        mock_human_readable_timestamp.return_value = "2023-03-15 12:00:00 UTC"

        last_updated = get_last_updated_for_table(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        mock_db_path_instance.stat.assert_called_once()
        mock_human_readable_timestamp.assert_called_once_with(mock_timestamp)
        assert last_updated == "2023-03-15 12:00:00 UTC"
        st_mock.error.assert_not_called()

    @mock.patch('src.ui.ui_utils.Path')
    @mock.patch('src.ui.ui_utils.human_readable_timestamp')
    def test_get_last_updated_for_table_file_not_exists(self, mock_human_readable_timestamp, mock_path_class):
        """Test get_last_updated_for_table when the file does not exist."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = False

        last_updated = get_last_updated_for_table(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        mock_db_path_instance.stat.assert_not_called()
        mock_human_readable_timestamp.assert_not_called()
        assert last_updated == "Database file not found."
        st_mock.error.assert_not_called() # No streamlit error, just returns a string

    @mock.patch('src.ui.ui_utils.Path')
    @mock.patch('src.ui.ui_utils.human_readable_timestamp')
    def test_get_last_updated_for_table_os_error(self, mock_human_readable_timestamp, mock_path_class):
        """Test get_last_updated_for_table when Path.stat() raises OSError."""
        db_path_str = "dummy.db"
        mock_db_path_instance = mock_path_class.return_value
        mock_db_path_instance.exists.return_value = True
        error_message = "File system error"
        mock_db_path_instance.stat.side_effect = OSError(error_message)

        last_updated = get_last_updated_for_table(db_path_str)

        mock_path_class.assert_called_once_with(db_path_str)
        mock_db_path_instance.exists.assert_called_once()
        mock_db_path_instance.stat.assert_called_once()
        mock_human_readable_timestamp.assert_not_called()
        assert f"Error accessing file metadata: {error_message}" in last_updated
        st_mock.error.assert_called_once_with(f"Error accessing file metadata for {db_path_str}: {error_message}")

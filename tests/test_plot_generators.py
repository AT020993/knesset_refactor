import pytest
import pandas as pd
from pathlib import Path
from unittest import mock
import plotly.graph_objects as go
import plotly.express as px # For patching specific functions
from duckdb import DuckDBPyConnection # For type hinting mock connection

# Import functions to be tested
from src.ui.plot_generators import (
    check_tables_exist,
    plot_queries_by_year,
    plot_query_types_distribution,
    plot_agendas_by_year,
    plot_bills_by_status,
    plot_bill_initiators_type,
    plot_committee_meetings_by_year,
    plot_committee_meetings_by_type,
    plot_factions_by_knesset,
    plot_members_by_gender,
    plot_members_by_age_group,
)

# Mock streamlit globally for all tests in this file
st_mock = mock.MagicMock()

# It's common to mock streamlit functions at the module level if they are widely used
# We need to import 'streamlit' to mock its members.
# We assume 'src.ui.plot_generators' imports 'streamlit as st'.
# If not, we need to patch 'streamlit' where it's imported by plot_generators.
# For now, let's assume direct import 'import streamlit as st' in plot_generators.py
# and mock these functions globally.
# If plot_generators.py uses 'from streamlit import error, info, warning',
# then we'd patch 'src.ui.plot_generators.error', etc.

# To be safe, let's patch them in 'src.ui.plot_generators' namespace
@pytest.fixture(autouse=True)
def mock_streamlit_in_module():
    with mock.patch('src.ui.plot_generators.st') as mock_st_module:
        mock_st_module.error = st_mock.error
        mock_st_module.info = st_mock.info
        mock_st_module.warning = st_mock.warning
        yield mock_st_module

# Mock logger
@pytest.fixture
def mock_logger():
    return mock.MagicMock()

# Mock database connection
@pytest.fixture
def mock_conn():
    conn = mock.MagicMock(spec=DuckDBPyConnection)
    # Default behavior for con.sql().df() to return empty DataFrame
    conn.sql.return_value.df.return_value = pd.DataFrame()
    # Default behavior for con.execute().df() to return empty DataFrame
    conn.execute.return_value.df.return_value = pd.DataFrame()
    return conn

# Mock connect_func
@pytest.fixture
def mock_connect_func(mock_conn):
    connect_func = mock.MagicMock()
    connect_func.return_value = mock_conn
    return connect_func

# Mock Path object for db_path
@pytest.fixture
def mock_db_path():
    path_mock = mock.MagicMock(spec=Path)
    path_mock.exists.return_value = True # Default to db exists
    return path_mock


class TestCheckTablesExist:
    def test_all_tables_present(self, mock_conn, mock_logger):
        """Test check_tables_exist when all required tables are present."""
        # Simulate duckdb_tables() output
        mock_conn.execute.return_value.df.return_value = pd.DataFrame({
            'name': ['queries_main', 'agendas_main', 'bills_main', 'committees_main', 'members_main', 'factions_main']
        })
        
        result = check_tables_exist(mock_conn, mock_logger)
        
        assert result is True
        mock_conn.execute.assert_called_once_with("SELECT name FROM duckdb_tables();")
        st_mock.warning.assert_not_called()
        st_mock.error.assert_not_called()
        mock_logger.info.assert_called_with("All required tables exist.")

    def test_some_tables_missing(self, mock_conn, mock_logger):
        """Test check_tables_exist when some required tables are missing."""
        mock_conn.execute.return_value.df.return_value = pd.DataFrame({
            'name': ['queries_main', 'bills_main'] # Missing agendas_main, committees_main, etc.
        })
        
        result = check_tables_exist(mock_conn, mock_logger)
        
        assert result is False
        mock_conn.execute.assert_called_once_with("SELECT name FROM duckdb_tables();")
        st_mock.error.assert_called_once() # Should call st.error with missing tables
        assert "Missing required tables:" in st_mock.error.call_args[0][0]
        assert "agendas_main" in st_mock.error.call_args[0][0]
        mock_logger.error.assert_called()

    def test_no_tables_found(self, mock_conn, mock_logger):
        """Test check_tables_exist when duckdb_tables() returns empty."""
        mock_conn.execute.return_value.df.return_value = pd.DataFrame({'name': []}) # No tables
        
        result = check_tables_exist(mock_conn, mock_logger)
        
        assert result is False
        st_mock.error.assert_called_once()
        assert "Missing required tables:" in st_mock.error.call_args[0][0]
        assert "queries_main" in st_mock.error.call_args[0][0] # All should be listed as missing
        mock_logger.error.assert_called()

    def test_db_execution_error(self, mock_conn, mock_logger):
        """Test check_tables_exist when con.execute raises an exception."""
        error_message = "DB error"
        mock_conn.execute.side_effect = Exception(error_message)
        
        result = check_tables_exist(mock_conn, mock_logger)
        
        assert result is False
        st_mock.error.assert_called_once_with(f"Error checking for tables: {error_message}")
        mock_logger.error.assert_called_with(f"Failed to check for tables: {error_message}")

# Future tests for plotting functions will go here
class TestPlotQueriesByYear:
    @patch('src.ui.plot_generators.px.bar')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_queries_by_year."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data = pd.DataFrame({
            'SubmitYear': ['2020', '2021'],
            'KnessetNum': [23, 24],
            'QueryCount': [100, 150]
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_bar.return_value = go.Figure() # Ensure px.bar returns a Figure object

        fig = plot_queries_by_year(mock_db_path, mock_connect_func, mock_logger)

        assert isinstance(fig, go.Figure)
        mock_db_path.exists.assert_called_once()
        mock_connect_func.assert_called_once_with(mock_db_path, read_only=True)
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['queries_main'])
        mock_conn.sql.assert_called_once_with(
            "SELECT strftime(SubmitDate, '%Y') AS SubmitYear, KnessetNum, COUNT(*) AS QueryCount FROM queries_main GROUP BY SubmitYear, KnessetNum ORDER BY SubmitYear, KnessetNum"
        )
        mock_px_bar.assert_called_once()
        call_args = mock_px_bar.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data)
        assert call_args['x'] == 'SubmitYear'
        assert call_args['y'] == 'QueryCount'
        assert call_args['color'] == 'KnessetNum'
        assert 'Queries Submitted Over Time' in call_args['title']
        st_mock.error.assert_not_called()
        st_mock.info.assert_not_called()
        mock_conn.close.assert_called_once()


    def test_db_not_found(self, mock_db_path, mock_connect_func, mock_logger):
        """Test plot_queries_by_year when database file does not exist."""
        mock_db_path.exists.return_value = False
        
        fig = plot_queries_by_year(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_db_path.exists.assert_called_once()
        st_mock.error.assert_called_once_with(f"Database file not found at {mock_db_path}")
        mock_connect_func.assert_not_called()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_tables_not_found(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_queries_by_year when required tables do not exist."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = False
        mock_connect_func.return_value = mock_conn
        
        fig = plot_queries_by_year(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['queries_main'])
        # st.warning or st.error is called by check_tables_exist, not directly here.
        # We can check if the logger was called appropriately by this function.
        mock_logger.warning.assert_called_with("Aborting plot generation due to missing tables or connection issues.")
        mock_conn.close.assert_called_once()


    @patch('src.ui.plot_generators.px.bar')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_no_data_found(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_queries_by_year when the query returns an empty DataFrame."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        mock_conn.sql.return_value.df.return_value = pd.DataFrame() # Empty DataFrame
        
        fig = plot_queries_by_year(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.info.assert_called_once_with("No data found for 'Queries Submitted Over Time'.")
        mock_px_bar.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_sql_execution_error(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_queries_by_year when con.sql raises an exception."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        error_message = "SQL error"
        mock_conn.sql.side_effect = Exception(error_message)
        
        fig = plot_queries_by_year(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.error.assert_called_once_with(f"Error generating plot 'Queries Submitted Over Time': {error_message}")
        mock_logger.error.assert_called_with(f"SQL error for plot 'Queries Submitted Over Time': {error_message}")
        mock_conn.close.assert_called_once()
        
    @patch('src.ui.plot_generators.px.bar')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success_with_knesset_filter(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_queries_by_year with Knesset filter."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data_filtered = pd.DataFrame({
            'SubmitYear': ['2021'],
            'KnessetNum': [24],
            'QueryCount': [150]
        })
        mock_conn.sql.return_value.df.return_value = sample_data_filtered
        mock_px_bar.return_value = go.Figure()

        knesset_filter = 24
        fig = plot_queries_by_year(mock_db_path, mock_connect_func, mock_logger, knesset_filter=knesset_filter)

        assert isinstance(fig, go.Figure)
        expected_query = f"SELECT strftime(SubmitDate, '%Y') AS SubmitYear, KnessetNum, COUNT(*) AS QueryCount FROM queries_main WHERE KnessetNum = {knesset_filter} GROUP BY SubmitYear, KnessetNum ORDER BY SubmitYear, KnessetNum"
        mock_conn.sql.assert_called_once_with(expected_query)
        mock_px_bar.assert_called_once()
        call_args = mock_px_bar.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data_filtered)
        assert call_args['x'] == 'SubmitYear'
        assert call_args['y'] == 'QueryCount'
        assert 'Queries Submitted Over Time (Knesset 24)' in call_args['title']
        mock_conn.close.assert_called_once()

class TestPlotQueryTypesDistribution:
    @patch('src.ui.plot_generators.px.pie')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success(self, mock_check_tables_exist, mock_px_pie, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_query_types_distribution."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data = pd.DataFrame({
            'QueryTypeName': ['Type A', 'Type B'],
            'QueryCount': [50, 75]
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_pie.return_value = go.Figure()

        fig = plot_query_types_distribution(mock_db_path, mock_connect_func, mock_logger)

        assert isinstance(fig, go.Figure)
        mock_db_path.exists.assert_called_once()
        mock_connect_func.assert_called_once_with(mock_db_path, read_only=True)
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['queries_main', 'queries_meta'])
        mock_conn.sql.assert_called_once_with(
            "SELECT qm.QueryTypeName, COUNT(q.QueryID) AS QueryCount FROM queries_main q JOIN queries_meta qm ON q.QueryTypeID = qm.QueryTypeID GROUP BY qm.QueryTypeName ORDER BY QueryCount DESC"
        )
        mock_px_pie.assert_called_once()
        call_args = mock_px_pie.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data)
        assert call_args['names'] == 'QueryTypeName'
        assert call_args['values'] == 'QueryCount'
        assert 'Distribution of Query Types' in call_args['title']
        st_mock.error.assert_not_called()
        st_mock.info.assert_not_called()
        mock_conn.close.assert_called_once()

    def test_db_not_found(self, mock_db_path, mock_connect_func, mock_logger):
        """Test plot_query_types_distribution when database file does not exist."""
        mock_db_path.exists.return_value = False
        
        fig = plot_query_types_distribution(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_db_path.exists.assert_called_once()
        st_mock.error.assert_called_once_with(f"Database file not found at {mock_db_path}")
        mock_connect_func.assert_not_called()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_tables_not_found(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_query_types_distribution when required tables do not exist."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = False
        mock_connect_func.return_value = mock_conn
        
        fig = plot_query_types_distribution(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['queries_main', 'queries_meta'])
        mock_logger.warning.assert_called_with("Aborting plot generation due to missing tables or connection issues.")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.pie')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_no_data_found(self, mock_check_tables_exist, mock_px_pie, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_query_types_distribution when the query returns an empty DataFrame."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        mock_conn.sql.return_value.df.return_value = pd.DataFrame() # Empty DataFrame
        
        fig = plot_query_types_distribution(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.info.assert_called_once_with("No data found for 'Distribution of Query Types'.")
        mock_px_pie.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_sql_execution_error(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_query_types_distribution when con.sql raises an exception."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        error_message = "SQL error"
        mock_conn.sql.side_effect = Exception(error_message)
        
        fig = plot_query_types_distribution(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.error.assert_called_once_with(f"Error generating plot 'Distribution of Query Types': {error_message}")
        mock_logger.error.assert_called_with(f"SQL error for plot 'Distribution of Query Types': {error_message}")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.pie')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success_with_knesset_filter(self, mock_check_tables_exist, mock_px_pie, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_query_types_distribution with Knesset filter."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data_filtered = pd.DataFrame({
            'QueryTypeName': ['Type A'],
            'QueryCount': [30]
        })
        mock_conn.sql.return_value.df.return_value = sample_data_filtered
        mock_px_pie.return_value = go.Figure()

        knesset_filter = 24
        fig = plot_query_types_distribution(mock_db_path, mock_connect_func, mock_logger, knesset_filter=knesset_filter)

        assert isinstance(fig, go.Figure)
        expected_query = f"SELECT qm.QueryTypeName, COUNT(q.QueryID) AS QueryCount FROM queries_main q JOIN queries_meta qm ON q.QueryTypeID = qm.QueryTypeID WHERE q.KnessetNum = {knesset_filter} GROUP BY qm.QueryTypeName ORDER BY QueryCount DESC"
        mock_conn.sql.assert_called_once_with(expected_query)
        mock_px_pie.assert_called_once()
        call_args = mock_px_pie.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data_filtered)
        assert call_args['names'] == 'QueryTypeName'
        assert call_args['values'] == 'QueryCount'
        assert 'Distribution of Query Types (Knesset 24)' in call_args['title']
        mock_conn.close.assert_called_once()

class TestPlotAgendasByYear:
    @patch('src.ui.plot_generators.px.line')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success(self, mock_check_tables_exist, mock_px_line, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_agendas_by_year."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data = pd.DataFrame({
            'Year': ['2020', '2021'],
            'KnessetNum': [23, 24],
            'AgendaCount': [10, 15]
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_line.return_value = go.Figure()

        fig = plot_agendas_by_year(mock_db_path, mock_connect_func, mock_logger)

        assert isinstance(fig, go.Figure)
        mock_db_path.exists.assert_called_once()
        mock_connect_func.assert_called_once_with(mock_db_path, read_only=True)
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['agendas_main'])
        mock_conn.sql.assert_called_once_with(
            "SELECT strftime(AgendaDate, '%Y') AS Year, KnessetNum, COUNT(DISTINCT AgendaID) AS AgendaCount FROM agendas_main GROUP BY Year, KnessetNum ORDER BY Year, KnessetNum"
        )
        mock_px_line.assert_called_once()
        call_args = mock_px_line.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data)
        assert call_args['x'] == 'Year'
        assert call_args['y'] == 'AgendaCount'
        assert call_args['color'] == 'KnessetNum'
        assert 'Agendas Published Over Time' in call_args['title']
        st_mock.error.assert_not_called()
        st_mock.info.assert_not_called()
        mock_conn.close.assert_called_once()

    def test_db_not_found(self, mock_db_path, mock_connect_func, mock_logger):
        """Test plot_agendas_by_year when database file does not exist."""
        mock_db_path.exists.return_value = False
        
        fig = plot_agendas_by_year(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_db_path.exists.assert_called_once()
        st_mock.error.assert_called_once_with(f"Database file not found at {mock_db_path}")
        mock_connect_func.assert_not_called()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_tables_not_found(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_agendas_by_year when required tables do not exist."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = False
        mock_connect_func.return_value = mock_conn
        
        fig = plot_agendas_by_year(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['agendas_main'])
        mock_logger.warning.assert_called_with("Aborting plot generation due to missing tables or connection issues.")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.line')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_no_data_found(self, mock_check_tables_exist, mock_px_line, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_agendas_by_year when the query returns an empty DataFrame."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        mock_conn.sql.return_value.df.return_value = pd.DataFrame() # Empty DataFrame
        
        fig = plot_agendas_by_year(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.info.assert_called_once_with("No data found for 'Agendas Published Over Time'.")
        mock_px_line.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_sql_execution_error(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_agendas_by_year when con.sql raises an exception."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        error_message = "SQL error"
        mock_conn.sql.side_effect = Exception(error_message)
        
        fig = plot_agendas_by_year(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.error.assert_called_once_with(f"Error generating plot 'Agendas Published Over Time': {error_message}")
        mock_logger.error.assert_called_with(f"SQL error for plot 'Agendas Published Over Time': {error_message}")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.line')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success_with_knesset_filter(self, mock_check_tables_exist, mock_px_line, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_agendas_by_year with Knesset filter."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data_filtered = pd.DataFrame({
            'Year': ['2021'],
            'KnessetNum': [24],
            'AgendaCount': [15]
        })
        mock_conn.sql.return_value.df.return_value = sample_data_filtered
        mock_px_line.return_value = go.Figure()

        knesset_filter = 24
        fig = plot_agendas_by_year(mock_db_path, mock_connect_func, mock_logger, knesset_filter=knesset_filter)

        assert isinstance(fig, go.Figure)
        expected_query = f"SELECT strftime(AgendaDate, '%Y') AS Year, KnessetNum, COUNT(DISTINCT AgendaID) AS AgendaCount FROM agendas_main WHERE KnessetNum = {knesset_filter} GROUP BY Year, KnessetNum ORDER BY Year, KnessetNum"
        mock_conn.sql.assert_called_once_with(expected_query)
        mock_px_line.assert_called_once()
        call_args = mock_px_line.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data_filtered)
        assert call_args['x'] == 'Year'
        assert call_args['y'] == 'AgendaCount'
        assert 'Agendas Published Over Time (Knesset 24)' in call_args['title']
        mock_conn.close.assert_called_once()

class TestPlotBillsByStatus:
    @patch('src.ui.plot_generators.px.bar')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_bills_by_status."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data = pd.DataFrame({
            'StatusDesc': ['Approved', 'Pending'],
            'BillCount': [30, 70],
            'KnessetNum': [25, 25] # Assuming Status is per Knesset
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_bar.return_value = go.Figure()

        fig = plot_bills_by_status(mock_db_path, mock_connect_func, mock_logger)

        assert isinstance(fig, go.Figure)
        mock_db_path.exists.assert_called_once()
        mock_connect_func.assert_called_once_with(mock_db_path, read_only=True)
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['bills_main', 'bills_status_meta'])
        mock_conn.sql.assert_called_once_with(
             "SELECT bsm.StatusDesc, COUNT(b.BillID) AS BillCount, b.KnessetNum FROM bills_main b JOIN bills_status_meta bsm ON b.StatusID = bsm.StatusID GROUP BY bsm.StatusDesc, b.KnessetNum ORDER BY BillCount DESC"
        )
        mock_px_bar.assert_called_once()
        call_args = mock_px_bar.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data)
        assert call_args['x'] == 'StatusDesc'
        assert call_args['y'] == 'BillCount'
        assert call_args['color'] == 'KnessetNum'
        assert 'Distribution of Bills by Status' in call_args['title']
        st_mock.error.assert_not_called()
        st_mock.info.assert_not_called()
        mock_conn.close.assert_called_once()

    def test_db_not_found(self, mock_db_path, mock_connect_func, mock_logger):
        """Test plot_bills_by_status when database file does not exist."""
        mock_db_path.exists.return_value = False
        
        fig = plot_bills_by_status(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_db_path.exists.assert_called_once()
        st_mock.error.assert_called_once_with(f"Database file not found at {mock_db_path}")
        mock_connect_func.assert_not_called()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_tables_not_found(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_bills_by_status when required tables do not exist."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = False
        mock_connect_func.return_value = mock_conn
        
        fig = plot_bills_by_status(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['bills_main', 'bills_status_meta'])
        mock_logger.warning.assert_called_with("Aborting plot generation due to missing tables or connection issues.")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.bar')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_no_data_found(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_bills_by_status when the query returns an empty DataFrame."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        mock_conn.sql.return_value.df.return_value = pd.DataFrame() # Empty DataFrame
        
        fig = plot_bills_by_status(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.info.assert_called_once_with("No data found for 'Distribution of Bills by Status'.")
        mock_px_bar.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_sql_execution_error(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_bills_by_status when con.sql raises an exception."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        error_message = "SQL error"
        mock_conn.sql.side_effect = Exception(error_message)
        
        fig = plot_bills_by_status(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.error.assert_called_once_with(f"Error generating plot 'Distribution of Bills by Status': {error_message}")
        mock_logger.error.assert_called_with(f"SQL error for plot 'Distribution of Bills by Status': {error_message}")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.bar')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success_with_knesset_filter(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_bills_by_status with Knesset filter."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data_filtered = pd.DataFrame({
            'StatusDesc': ['Approved'],
            'BillCount': [20],
            'KnessetNum': [24]
        })
        mock_conn.sql.return_value.df.return_value = sample_data_filtered
        mock_px_bar.return_value = go.Figure()

        knesset_filter = 24
        fig = plot_bills_by_status(mock_db_path, mock_connect_func, mock_logger, knesset_filter=knesset_filter)

        assert isinstance(fig, go.Figure)
        expected_query = f"SELECT bsm.StatusDesc, COUNT(b.BillID) AS BillCount, b.KnessetNum FROM bills_main b JOIN bills_status_meta bsm ON b.StatusID = bsm.StatusID WHERE b.KnessetNum = {knesset_filter} GROUP BY bsm.StatusDesc, b.KnessetNum ORDER BY BillCount DESC"
        mock_conn.sql.assert_called_once_with(expected_query)
        mock_px_bar.assert_called_once()
        call_args = mock_px_bar.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data_filtered)
        assert call_args['x'] == 'StatusDesc'
        assert call_args['y'] == 'BillCount'
        assert 'Distribution of Bills by Status (Knesset 24)' in call_args['title']
        mock_conn.close.assert_called_once()

class TestPlotBillInitiatorsType:
    @patch('src.ui.plot_generators.px.pie')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success(self, mock_check_tables_exist, mock_px_pie, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_bill_initiators_type."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data = pd.DataFrame({
            'InitiatorType': ['Government', 'MK'],
            'BillCount': [60, 40]
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_pie.return_value = go.Figure()

        fig = plot_bill_initiators_type(mock_db_path, mock_connect_func, mock_logger)

        assert isinstance(fig, go.Figure)
        mock_db_path.exists.assert_called_once()
        mock_connect_func.assert_called_once_with(mock_db_path, read_only=True)
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['bills_main', 'bills_initiators_main', 'bills_initiators_type_meta'])
        mock_conn.sql.assert_called_once_with(
            """
            SELECT bitm.InitiatorType, COUNT(DISTINCT b.BillID) AS BillCount
            FROM bills_main b
            JOIN bills_initiators_main bim ON b.BillID = bim.BillID
            JOIN bills_initiators_type_meta bitm ON bim.InitiatorTypeID = bitm.InitiatorTypeID
            GROUP BY bitm.InitiatorType
            ORDER BY BillCount DESC
            """
        )
        mock_px_pie.assert_called_once()
        call_args = mock_px_pie.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data)
        assert call_args['names'] == 'InitiatorType'
        assert call_args['values'] == 'BillCount'
        assert 'Distribution of Bills by Initiator Type' in call_args['title']
        st_mock.error.assert_not_called()
        st_mock.info.assert_not_called()
        mock_conn.close.assert_called_once()

    def test_db_not_found(self, mock_db_path, mock_connect_func, mock_logger):
        """Test plot_bill_initiators_type when database file does not exist."""
        mock_db_path.exists.return_value = False
        
        fig = plot_bill_initiators_type(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_db_path.exists.assert_called_once()
        st_mock.error.assert_called_once_with(f"Database file not found at {mock_db_path}")
        mock_connect_func.assert_not_called()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_tables_not_found(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_bill_initiators_type when required tables do not exist."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = False
        mock_connect_func.return_value = mock_conn
        
        fig = plot_bill_initiators_type(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['bills_main', 'bills_initiators_main', 'bills_initiators_type_meta'])
        mock_logger.warning.assert_called_with("Aborting plot generation due to missing tables or connection issues.")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.pie')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_no_data_found(self, mock_check_tables_exist, mock_px_pie, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_bill_initiators_type when the query returns an empty DataFrame."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        mock_conn.sql.return_value.df.return_value = pd.DataFrame() # Empty DataFrame
        
        fig = plot_bill_initiators_type(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.info.assert_called_once_with("No data found for 'Distribution of Bills by Initiator Type'.")
        mock_px_pie.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_sql_execution_error(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_bill_initiators_type when con.sql raises an exception."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        error_message = "SQL error"
        mock_conn.sql.side_effect = Exception(error_message)
        
        fig = plot_bill_initiators_type(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.error.assert_called_once_with(f"Error generating plot 'Distribution of Bills by Initiator Type': {error_message}")
        mock_logger.error.assert_called_with(f"SQL error for plot 'Distribution of Bills by Initiator Type': {error_message}")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.pie')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success_with_knesset_filter(self, mock_check_tables_exist, mock_px_pie, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_bill_initiators_type with Knesset filter."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data_filtered = pd.DataFrame({
            'InitiatorType': ['Government'],
            'BillCount': [50]
        })
        mock_conn.sql.return_value.df.return_value = sample_data_filtered
        mock_px_pie.return_value = go.Figure()

        knesset_filter = 24
        fig = plot_bill_initiators_type(mock_db_path, mock_connect_func, mock_logger, knesset_filter=knesset_filter)

        assert isinstance(fig, go.Figure)
        expected_query = f"""
            SELECT bitm.InitiatorType, COUNT(DISTINCT b.BillID) AS BillCount
            FROM bills_main b
            JOIN bills_initiators_main bim ON b.BillID = bim.BillID
            JOIN bills_initiators_type_meta bitm ON bim.InitiatorTypeID = bitm.InitiatorTypeID
            WHERE b.KnessetNum = {knesset_filter}
            GROUP BY bitm.InitiatorType
            ORDER BY BillCount DESC
            """
        mock_conn.sql.assert_called_once_with(expected_query)
        mock_px_pie.assert_called_once()
        call_args = mock_px_pie.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data_filtered)
        assert call_args['names'] == 'InitiatorType'
        assert call_args['values'] == 'BillCount'
        assert 'Distribution of Bills by Initiator Type (Knesset 24)' in call_args['title']
        mock_conn.close.assert_called_once()

class TestPlotCommitteeMeetingsByYear:
    @patch('src.ui.plot_generators.px.bar')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_committee_meetings_by_year."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data = pd.DataFrame({
            'Year': ['2020', '2021'],
            'KnessetNum': [23, 24],
            'MeetingCount': [200, 250]
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_bar.return_value = go.Figure()

        fig = plot_committee_meetings_by_year(mock_db_path, mock_connect_func, mock_logger)

        assert isinstance(fig, go.Figure)
        mock_db_path.exists.assert_called_once()
        mock_connect_func.assert_called_once_with(mock_db_path, read_only=True)
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['committees_main'])
        mock_conn.sql.assert_called_once_with(
            "SELECT strftime(StartDate, '%Y') AS Year, KnessetNum, COUNT(DISTINCT CommitteeSessionID) AS MeetingCount FROM committees_main GROUP BY Year, KnessetNum ORDER BY Year, KnessetNum"
        )
        mock_px_bar.assert_called_once()
        call_args = mock_px_bar.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data)
        assert call_args['x'] == 'Year'
        assert call_args['y'] == 'MeetingCount'
        assert call_args['color'] == 'KnessetNum'
        assert 'Committee Meetings Over Time' in call_args['title']
        st_mock.error.assert_not_called()
        st_mock.info.assert_not_called()
        mock_conn.close.assert_called_once()

    def test_db_not_found(self, mock_db_path, mock_connect_func, mock_logger):
        """Test plot_committee_meetings_by_year when database file does not exist."""
        mock_db_path.exists.return_value = False
        
        fig = plot_committee_meetings_by_year(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_db_path.exists.assert_called_once()
        st_mock.error.assert_called_once_with(f"Database file not found at {mock_db_path}")
        mock_connect_func.assert_not_called()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_tables_not_found(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_committee_meetings_by_year when required tables do not exist."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = False
        mock_connect_func.return_value = mock_conn
        
        fig = plot_committee_meetings_by_year(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['committees_main'])
        mock_logger.warning.assert_called_with("Aborting plot generation due to missing tables or connection issues.")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.bar')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_no_data_found(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_committee_meetings_by_year when the query returns an empty DataFrame."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        mock_conn.sql.return_value.df.return_value = pd.DataFrame() # Empty DataFrame
        
        fig = plot_committee_meetings_by_year(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.info.assert_called_once_with("No data found for 'Committee Meetings Over Time'.")
        mock_px_bar.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_sql_execution_error(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_committee_meetings_by_year when con.sql raises an exception."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        error_message = "SQL error"
        mock_conn.sql.side_effect = Exception(error_message)
        
        fig = plot_committee_meetings_by_year(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.error.assert_called_once_with(f"Error generating plot 'Committee Meetings Over Time': {error_message}")
        mock_logger.error.assert_called_with(f"SQL error for plot 'Committee Meetings Over Time': {error_message}")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.bar')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success_with_knesset_filter(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_committee_meetings_by_year with Knesset filter."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data_filtered = pd.DataFrame({
            'Year': ['2021'],
            'KnessetNum': [24],
            'MeetingCount': [220]
        })
        mock_conn.sql.return_value.df.return_value = sample_data_filtered
        mock_px_bar.return_value = go.Figure()

        knesset_filter = 24
        fig = plot_committee_meetings_by_year(mock_db_path, mock_connect_func, mock_logger, knesset_filter=knesset_filter)

        assert isinstance(fig, go.Figure)
        expected_query = f"SELECT strftime(StartDate, '%Y') AS Year, KnessetNum, COUNT(DISTINCT CommitteeSessionID) AS MeetingCount FROM committees_main WHERE KnessetNum = {knesset_filter} GROUP BY Year, KnessetNum ORDER BY Year, KnessetNum"
        mock_conn.sql.assert_called_once_with(expected_query)
        mock_px_bar.assert_called_once()
        call_args = mock_px_bar.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data_filtered)
        assert call_args['x'] == 'Year'
        assert call_args['y'] == 'MeetingCount'
        assert 'Committee Meetings Over Time (Knesset 24)' in call_args['title']
        mock_conn.close.assert_called_once()

class TestPlotCommitteeMeetingsByType:
    @patch('src.ui.plot_generators.px.pie')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success(self, mock_check_tables_exist, mock_px_pie, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_committee_meetings_by_type."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data = pd.DataFrame({
            'CommitteeName': ['Finance Committee', 'Education Committee'],
            'MeetingCount': [100, 150],
            'KnessetNum': [25, 25] # Assuming data is grouped by KnessetNum if not filtered
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_pie.return_value = go.Figure()

        fig = plot_committee_meetings_by_type(mock_db_path, mock_connect_func, mock_logger)

        assert isinstance(fig, go.Figure)
        mock_db_path.exists.assert_called_once()
        mock_connect_func.assert_called_once_with(mock_db_path, read_only=True)
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['committees_main', 'committees_meta'])
        mock_conn.sql.assert_called_once_with(
             "SELECT cm.CommitteeName, COUNT(DISTINCT c.CommitteeSessionID) AS MeetingCount, c.KnessetNum FROM committees_main c JOIN committees_meta cm ON c.CommitteeID = cm.CommitteeID GROUP BY cm.CommitteeName, c.KnessetNum ORDER BY MeetingCount DESC"
        )
        mock_px_pie.assert_called_once()
        call_args = mock_px_pie.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data)
        assert call_args['names'] == 'CommitteeName'
        assert call_args['values'] == 'MeetingCount'
        assert call_args['color'] == 'KnessetNum' # if KnessetNum is in the grouped data
        assert 'Distribution of Committee Meetings by Type' in call_args['title']
        st_mock.error.assert_not_called()
        st_mock.info.assert_not_called()
        mock_conn.close.assert_called_once()

    def test_db_not_found(self, mock_db_path, mock_connect_func, mock_logger):
        """Test plot_committee_meetings_by_type when database file does not exist."""
        mock_db_path.exists.return_value = False
        
        fig = plot_committee_meetings_by_type(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_db_path.exists.assert_called_once()
        st_mock.error.assert_called_once_with(f"Database file not found at {mock_db_path}")
        mock_connect_func.assert_not_called()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_tables_not_found(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_committee_meetings_by_type when required tables do not exist."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = False
        mock_connect_func.return_value = mock_conn
        
        fig = plot_committee_meetings_by_type(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['committees_main', 'committees_meta'])
        mock_logger.warning.assert_called_with("Aborting plot generation due to missing tables or connection issues.")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.pie')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_no_data_found(self, mock_check_tables_exist, mock_px_pie, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_committee_meetings_by_type when the query returns an empty DataFrame."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        mock_conn.sql.return_value.df.return_value = pd.DataFrame() # Empty DataFrame
        
        fig = plot_committee_meetings_by_type(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.info.assert_called_once_with("No data found for 'Distribution of Committee Meetings by Type'.")
        mock_px_pie.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_sql_execution_error(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_committee_meetings_by_type when con.sql raises an exception."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        error_message = "SQL error"
        mock_conn.sql.side_effect = Exception(error_message)
        
        fig = plot_committee_meetings_by_type(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.error.assert_called_once_with(f"Error generating plot 'Distribution of Committee Meetings by Type': {error_message}")
        mock_logger.error.assert_called_with(f"SQL error for plot 'Distribution of Committee Meetings by Type': {error_message}")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.pie')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success_with_knesset_filter(self, mock_check_tables_exist, mock_px_pie, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_committee_meetings_by_type with Knesset filter."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data_filtered = pd.DataFrame({
            'CommitteeName': ['Finance Committee'],
            'MeetingCount': [80],
            'KnessetNum': [24]
        })
        mock_conn.sql.return_value.df.return_value = sample_data_filtered
        mock_px_pie.return_value = go.Figure()

        knesset_filter = 24
        fig = plot_committee_meetings_by_type(mock_db_path, mock_connect_func, mock_logger, knesset_filter=knesset_filter)

        assert isinstance(fig, go.Figure)
        expected_query = f"SELECT cm.CommitteeName, COUNT(DISTINCT c.CommitteeSessionID) AS MeetingCount, c.KnessetNum FROM committees_main c JOIN committees_meta cm ON c.CommitteeID = cm.CommitteeID WHERE c.KnessetNum = {knesset_filter} GROUP BY cm.CommitteeName, c.KnessetNum ORDER BY MeetingCount DESC"
        mock_conn.sql.assert_called_once_with(expected_query)
        mock_px_pie.assert_called_once()
        call_args = mock_px_pie.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data_filtered)
        assert call_args['names'] == 'CommitteeName'
        assert call_args['values'] == 'MeetingCount'
        # Color by KnessetNum is not explicitly in the function's pie chart if filtered by one Knesset
        # but the data still contains KnessetNum. The original function might need adjustment or this test.
        # For now, asserting the title is the main check for filter application.
        assert 'Distribution of Committee Meetings by Type (Knesset 24)' in call_args['title']
        mock_conn.close.assert_called_once()

class TestPlotFactionsByKnesset:
    @patch('src.ui.plot_generators.px.bar')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_factions_by_knesset."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data = pd.DataFrame({
            'KnessetNum': [23, 24, 25],
            'FactionCount': [10, 12, 11]
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_bar.return_value = go.Figure()

        fig = plot_factions_by_knesset(mock_db_path, mock_connect_func, mock_logger)

        assert isinstance(fig, go.Figure)
        mock_db_path.exists.assert_called_once()
        mock_connect_func.assert_called_once_with(mock_db_path, read_only=True)
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['factions_main'])
        mock_conn.sql.assert_called_once_with(
            "SELECT KnessetNum, COUNT(DISTINCT FactionID) AS FactionCount FROM factions_main GROUP BY KnessetNum ORDER BY KnessetNum"
        )
        mock_px_bar.assert_called_once()
        call_args = mock_px_bar.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data)
        assert call_args['x'] == 'KnessetNum'
        assert call_args['y'] == 'FactionCount'
        assert 'Number of Factions by Knesset' in call_args['title']
        st_mock.error.assert_not_called()
        st_mock.info.assert_not_called()
        mock_conn.close.assert_called_once()

    def test_db_not_found(self, mock_db_path, mock_connect_func, mock_logger):
        """Test plot_factions_by_knesset when database file does not exist."""
        mock_db_path.exists.return_value = False
        
        fig = plot_factions_by_knesset(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_db_path.exists.assert_called_once()
        st_mock.error.assert_called_once_with(f"Database file not found at {mock_db_path}")
        mock_connect_func.assert_not_called()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_tables_not_found(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_factions_by_knesset when required tables do not exist."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = False
        mock_connect_func.return_value = mock_conn
        
        fig = plot_factions_by_knesset(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['factions_main'])
        mock_logger.warning.assert_called_with("Aborting plot generation due to missing tables or connection issues.")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.bar')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_no_data_found(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_factions_by_knesset when the query returns an empty DataFrame."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        mock_conn.sql.return_value.df.return_value = pd.DataFrame() # Empty DataFrame
        
        fig = plot_factions_by_knesset(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.info.assert_called_once_with("No data found for 'Number of Factions by Knesset'.")
        mock_px_bar.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_sql_execution_error(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_factions_by_knesset when con.sql raises an exception."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        error_message = "SQL error"
        mock_conn.sql.side_effect = Exception(error_message)
        
        fig = plot_factions_by_knesset(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.error.assert_called_once_with(f"Error generating plot 'Number of Factions by Knesset': {error_message}")
        mock_logger.error.assert_called_with(f"SQL error for plot 'Number of Factions by Knesset': {error_message}")
        mock_conn.close.assert_called_once()

    # This plot does not take Knesset or Faction filters, so no need for those specific tests here.
    # If it did, we would add them similar to other plot functions.

class TestPlotMembersByGender:
    @patch('src.ui.plot_generators.px.pie')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success(self, mock_check_tables_exist, mock_px_pie, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_members_by_gender."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data = pd.DataFrame({
            'GenderDesc': ['Male', 'Female'],
            'MemberCount': [80, 40],
            'KnessetNum': [25, 25] # Assuming data is grouped by KnessetNum if not filtered
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_pie.return_value = go.Figure()

        fig = plot_members_by_gender(mock_db_path, mock_connect_func, mock_logger)

        assert isinstance(fig, go.Figure)
        mock_db_path.exists.assert_called_once()
        mock_connect_func.assert_called_once_with(mock_db_path, read_only=True)
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['members_main', 'members_gender_meta'])
        mock_conn.sql.assert_called_once_with(
            "SELECT mgm.GenderDesc, COUNT(DISTINCT m.PersonID) AS MemberCount, m.KnessetNum FROM members_main m JOIN members_gender_meta mgm ON m.GenderID = mgm.GenderID GROUP BY mgm.GenderDesc, m.KnessetNum ORDER BY MemberCount DESC"
        )
        mock_px_pie.assert_called_once()
        call_args = mock_px_pie.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data)
        assert call_args['names'] == 'GenderDesc'
        assert call_args['values'] == 'MemberCount'
        assert call_args['color'] == 'KnessetNum'
        assert 'Distribution of Knesset Members by Gender' in call_args['title']
        st_mock.error.assert_not_called()
        st_mock.info.assert_not_called()
        mock_conn.close.assert_called_once()

    def test_db_not_found(self, mock_db_path, mock_connect_func, mock_logger):
        """Test plot_members_by_gender when database file does not exist."""
        mock_db_path.exists.return_value = False
        
        fig = plot_members_by_gender(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_db_path.exists.assert_called_once()
        st_mock.error.assert_called_once_with(f"Database file not found at {mock_db_path}")
        mock_connect_func.assert_not_called()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_tables_not_found(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_members_by_gender when required tables do not exist."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = False
        mock_connect_func.return_value = mock_conn
        
        fig = plot_members_by_gender(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['members_main', 'members_gender_meta'])
        mock_logger.warning.assert_called_with("Aborting plot generation due to missing tables or connection issues.")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.pie')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_no_data_found(self, mock_check_tables_exist, mock_px_pie, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_members_by_gender when the query returns an empty DataFrame."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        mock_conn.sql.return_value.df.return_value = pd.DataFrame() # Empty DataFrame
        
        fig = plot_members_by_gender(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.info.assert_called_once_with("No data found for 'Distribution of Knesset Members by Gender'.")
        mock_px_pie.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_sql_execution_error(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_members_by_gender when con.sql raises an exception."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        error_message = "SQL error"
        mock_conn.sql.side_effect = Exception(error_message)
        
        fig = plot_members_by_gender(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.error.assert_called_once_with(f"Error generating plot 'Distribution of Knesset Members by Gender': {error_message}")
        mock_logger.error.assert_called_with(f"SQL error for plot 'Distribution of Knesset Members by Gender': {error_message}")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.pie')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success_with_knesset_filter(self, mock_check_tables_exist, mock_px_pie, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_members_by_gender with Knesset filter."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data_filtered = pd.DataFrame({
            'GenderDesc': ['Male', 'Female'],
            'MemberCount': [70, 30],
            'KnessetNum': [24, 24]
        })
        mock_conn.sql.return_value.df.return_value = sample_data_filtered
        mock_px_pie.return_value = go.Figure()

        knesset_filter = 24
        fig = plot_members_by_gender(mock_db_path, mock_connect_func, mock_logger, knesset_filter=knesset_filter)

        assert isinstance(fig, go.Figure)
        expected_query = f"SELECT mgm.GenderDesc, COUNT(DISTINCT m.PersonID) AS MemberCount, m.KnessetNum FROM members_main m JOIN members_gender_meta mgm ON m.GenderID = mgm.GenderID WHERE m.KnessetNum = {knesset_filter} GROUP BY mgm.GenderDesc, m.KnessetNum ORDER BY MemberCount DESC"
        mock_conn.sql.assert_called_once_with(expected_query)
        mock_px_pie.assert_called_once()
        call_args = mock_px_pie.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data_filtered)
        assert call_args['names'] == 'GenderDesc'
        assert call_args['values'] == 'MemberCount'
        assert 'Distribution of Knesset Members by Gender (Knesset 24)' in call_args['title']
        mock_conn.close.assert_called_once()

class TestPlotMembersByAgeGroup:
    @patch('src.ui.plot_generators.px.histogram')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success(self, mock_check_tables_exist, mock_px_histogram, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_members_by_age_group."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data = pd.DataFrame({
            'Age': [30, 35, 40, 45, 50], # Sample ages
            'KnessetNum': [25, 25, 25, 25, 25]
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_histogram.return_value = go.Figure()

        fig = plot_members_by_age_group(mock_db_path, mock_connect_func, mock_logger)

        assert isinstance(fig, go.Figure)
        mock_db_path.exists.assert_called_once()
        mock_connect_func.assert_called_once_with(mock_db_path, read_only=True)
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['members_main'])
        mock_conn.sql.assert_called_once_with(
            "SELECT Age, KnessetNum FROM members_main WHERE Age IS NOT NULL"
        )
        mock_px_histogram.assert_called_once()
        call_args = mock_px_histogram.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data)
        assert call_args['x'] == 'Age'
        assert call_args['color'] == 'KnessetNum'
        assert call_args['nbins'] > 0 # Check nbins is set
        assert 'Distribution of Knesset Members by Age Group' in call_args['title']
        st_mock.error.assert_not_called()
        st_mock.info.assert_not_called()
        mock_conn.close.assert_called_once()

    def test_db_not_found(self, mock_db_path, mock_connect_func, mock_logger):
        """Test plot_members_by_age_group when database file does not exist."""
        mock_db_path.exists.return_value = False
        
        fig = plot_members_by_age_group(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_db_path.exists.assert_called_once()
        st_mock.error.assert_called_once_with(f"Database file not found at {mock_db_path}")
        mock_connect_func.assert_not_called()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_tables_not_found(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_members_by_age_group when required tables do not exist."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = False
        mock_connect_func.return_value = mock_conn
        
        fig = plot_members_by_age_group(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        mock_check_tables_exist.assert_called_once_with(mock_conn, mock_logger, ['members_main'])
        mock_logger.warning.assert_called_with("Aborting plot generation due to missing tables or connection issues.")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.histogram')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_no_data_found(self, mock_check_tables_exist, mock_px_histogram, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_members_by_age_group when the query returns an empty DataFrame."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        mock_conn.sql.return_value.df.return_value = pd.DataFrame() # Empty DataFrame
        
        fig = plot_members_by_age_group(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.info.assert_called_once_with("No data found for 'Distribution of Knesset Members by Age Group'.")
        mock_px_histogram.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.check_tables_exist')
    def test_sql_execution_error(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test plot_members_by_age_group when con.sql raises an exception."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        error_message = "SQL error"
        mock_conn.sql.side_effect = Exception(error_message)
        
        fig = plot_members_by_age_group(mock_db_path, mock_connect_func, mock_logger)
        
        assert fig is None
        st_mock.error.assert_called_once_with(f"Error generating plot 'Distribution of Knesset Members by Age Group': {error_message}")
        mock_logger.error.assert_called_with(f"SQL error for plot 'Distribution of Knesset Members by Age Group': {error_message}")
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.histogram')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success_with_knesset_filter(self, mock_check_tables_exist, mock_px_histogram, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_members_by_age_group with Knesset filter."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data_filtered = pd.DataFrame({
            'Age': [32, 38, 42],
            'KnessetNum': [24, 24, 24]
        })
        mock_conn.sql.return_value.df.return_value = sample_data_filtered
        mock_px_histogram.return_value = go.Figure()

        knesset_filter = 24
        fig = plot_members_by_age_group(mock_db_path, mock_connect_func, mock_logger, knesset_filter=knesset_filter)

        assert isinstance(fig, go.Figure)
        expected_query = f"SELECT Age, KnessetNum FROM members_main WHERE Age IS NOT NULL AND KnessetNum = {knesset_filter}"
        mock_conn.sql.assert_called_once_with(expected_query)
        mock_px_histogram.assert_called_once()
        call_args = mock_px_histogram.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data_filtered)
        assert call_args['x'] == 'Age'
        # Color by KnessetNum might be redundant if filtered, but the data still contains the column
        assert 'Distribution of Knesset Members by Age Group (Knesset 24)' in call_args['title']
        mock_conn.close.assert_called_once()

    @patch('src.ui.plot_generators.px.histogram')
    @patch('src.ui.plot_generators.check_tables_exist')
    def test_success_with_faction_filter(self, mock_check_tables_exist, mock_px_histogram, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        """Test successful plot generation for plot_members_by_age_group with Faction filter."""
        mock_db_path.exists.return_value = True
        mock_check_tables_exist.return_value = True
        mock_connect_func.return_value = mock_conn
        
        sample_data_filtered = pd.DataFrame({
            'Age': [32, 38, 42],
            'KnessetNum': [24, 24, 24],
            'FactionName': ['Likud', 'Likud', 'Likud']
        })
        mock_conn.sql.return_value.df.return_value = sample_data_filtered # This needs to be the result of the JOIN
        mock_px_histogram.return_value = go.Figure()

        faction_filter = 'Likud'
        fig = plot_members_by_age_group(mock_db_path, mock_connect_func, mock_logger, faction_filter=faction_filter)

        assert isinstance(fig, go.Figure)
        # The query for faction filter involves a JOIN and is more complex
        expected_query = f"""
            SELECT m.Age, m.KnessetNum, fm.FactionName
            FROM members_main m
            JOIN members_faction_main mfm ON m.PersonID = mfm.PersonID AND m.KnessetNum = mfm.KnessetNum
            JOIN factions_main fm ON mfm.FactionID = fm.FactionID AND mfm.KnessetNum = fm.KnessetNum
            WHERE m.Age IS NOT NULL AND fm.FactionName = '{faction_filter}'
            """
        # Normalize whitespace for comparison if necessary, or ensure query in function matches this format
        mock_conn.sql.assert_called_once_with(mock.ANY) # Using ANY because exact string match with newlines can be tricky
        actual_query = mock_conn.sql.call_args[0][0]
        assert ''.join(expected_query.split()) == ''.join(actual_query.split()) # Compare with whitespace removed


        mock_px_histogram.assert_called_once()
        call_args = mock_px_histogram.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], sample_data_filtered)
        assert call_args['x'] == 'Age'
        assert 'Distribution of Knesset Members by Age Group (Faction: Likud)' in call_args['title']
        mock_conn.close.assert_called_once()

import pytest
import pandas as pd
from pathlib import Path
from unittest import mock
import plotly.graph_objects as go
import plotly.express as px # For patching specific functions
from duckdb import DuckDBPyConnection # For type hinting mock connection

# Import functions to be tested - UPDATED IMPORTS
from src.ui.plot_generators import (
    check_tables_exist,
    plot_queries_by_time_period,  # UPDATED from plot_queries_by_year
    plot_query_types_distribution,
    plot_agendas_by_time_period,  # UPDATED from plot_agendas_by_year
    # Assuming these were placeholder names or older versions,
    # I'll comment them out if they are not actual functions in the latest plot_generators.py.
    # If they are actual and distinct, they should be tested.
    # plot_bills_by_status,
    # plot_bill_initiators_type,
    # plot_committee_meetings_by_year, # Likely also needs rename if pattern followed
    # plot_committee_meetings_by_type,
    # plot_factions_by_knesset,
    # plot_members_by_gender,
    # plot_members_by_age_group,
    # Keep other existing, correctly named plot functions from your actual plot_generators.py
    plot_agenda_classifications_pie,
    plot_query_status_by_faction,
    plot_agenda_status_distribution,
    plot_queries_per_faction_in_knesset,
    plot_queries_by_coalition_and_answer_status,
    plot_queries_by_ministry_and_status,
)

# Mock streamlit globally for all tests in this file
st_mock = mock.MagicMock()


@pytest.fixture(autouse=True)
def mock_streamlit_in_module():
    with mock.patch('src.ui.plot_generators.st') as mock_st_module:
        mock_st_module.error = st_mock.error
        mock_st_module.info = st_mock.info
        mock_st_module.warning = st_mock.warning
        yield mock_st_module

@pytest.fixture
def mock_logger():
    return mock.MagicMock()

@pytest.fixture
def mock_conn():
    conn = mock.MagicMock(spec=DuckDBPyConnection)
    conn.sql.return_value.df.return_value = pd.DataFrame()
    conn.execute.return_value.df.return_value = pd.DataFrame()
    # If your check_tables_exist uses a different structure for duckdb_tables()
    # you might need to adjust this mock. For example, if it expects 'table_name':
    conn.execute.return_value.df.return_value = pd.DataFrame({'table_name': []})
    return conn

@pytest.fixture
def mock_connect_func(mock_conn):
    connect_func = mock.MagicMock()
    connect_func.return_value = mock_conn
    return connect_func

@pytest.fixture
def mock_db_path():
    path_mock = mock.MagicMock(spec=Path)
    path_mock.exists.return_value = True
    return path_mock


class TestCheckTablesExist:
    def test_all_tables_present(self, mock_conn, mock_logger):
        mock_conn.execute.return_value.df.return_value = pd.DataFrame({
            'table_name': ['kns_query', 'kns_agenda'] # Example tables
        })
        result = check_tables_exist(mock_conn, ['KNS_Query', 'KNS_Agenda'], mock_logger)
        assert result is True
        st_mock.warning.assert_not_called()

    def test_some_tables_missing(self, mock_conn, mock_logger):
        mock_conn.execute.return_value.df.return_value = pd.DataFrame({
            'table_name': ['kns_query']
        })
        result = check_tables_exist(mock_conn, ['KNS_Query', 'KNS_Agenda'], mock_logger)
        assert result is False
        st_mock.warning.assert_called_once()
        assert "KNS_Agenda" in st_mock.warning.call_args[0][0]

    def test_db_execution_error(self, mock_conn, mock_logger):
        mock_conn.execute.side_effect = Exception("DB error")
        result = check_tables_exist(mock_conn, ['KNS_Query'], mock_logger)
        assert result is False
        st_mock.error.assert_called_once_with("Error checking table existence: DB error")


# UPDATED Test Class name and function calls
class TestPlotQueriesByTimePeriod:
    @mock.patch('src.ui.plot_generators.px.bar')
    @mock.patch('src.ui.plot_generators.check_tables_exist')
    def test_success_single_knesset(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        mock_check_tables_exist.return_value = True
        sample_data = pd.DataFrame({
            'TimePeriod': ['2020', '2021'],
            'QueryCount': [100, 150]
            # No KnessetNum column when filtered for a single Knesset
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_bar.return_value = go.Figure()

        # Call the UPDATED function name
        fig = plot_queries_by_time_period(mock_db_path, mock_connect_func, mock_logger, knesset_filter=[25])

        assert isinstance(fig, go.Figure)
        mock_check_tables_exist.assert_called_once_with(mock_conn, ['KNS_Query'], mock_logger)
        mock_conn.sql.assert_called_once() # Check specific SQL if necessary
        mock_px_bar.assert_called_once()
        call_args = mock_px_bar.call_args[1]
        pd.testing.assert_frame_equal(call_args['data_frame'], sample_data)
        assert call_args['x'] == 'TimePeriod'
        assert call_args['y'] == 'QueryCount'
        assert 'Queries per Year for Knesset 25' in call_args['title'] # Title reflects single Knesset
        assert call_args.get('color') is None # No color by KnessetNum for single view
        st_mock.error.assert_not_called()

    @mock.patch('src.ui.plot_generators.px.bar')
    @mock.patch('src.ui.plot_generators.check_tables_exist')
    def test_success_multiple_knessets(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        mock_check_tables_exist.return_value = True
        sample_data = pd.DataFrame({
            'TimePeriod': ['2020', '2020', '2021', '2021'],
            'KnessetNum': ['24', '25', '24', '25'],
            'QueryCount': [100, 120, 150, 170]
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_bar.return_value = go.Figure()

        fig = plot_queries_by_time_period(mock_db_path, mock_connect_func, mock_logger, knesset_filter=[24, 25])

        assert isinstance(fig, go.Figure)
        mock_px_bar.assert_called_once()
        call_args = mock_px_bar.call_args[1]
        assert call_args['color'] == 'KnessetNum'
        assert 'Knessets: 24, 25' in call_args['title']
        st_mock.error.assert_not_called()


    def test_db_not_found(self, mock_db_path, mock_connect_func, mock_logger):
        mock_db_path.exists.return_value = False
        fig = plot_queries_by_time_period(mock_db_path, mock_connect_func, mock_logger) # UPDATED
        assert fig is None
        st_mock.error.assert_called_once_with("Database not found. Cannot generate visualization.")

    @mock.patch('src.ui.plot_generators.check_tables_exist')
    def test_tables_not_found(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        mock_check_tables_exist.return_value = False
        fig = plot_queries_by_time_period(mock_db_path, mock_connect_func, mock_logger) # UPDATED
        assert fig is None
        # st.warning is called by check_tables_exist

    @mock.patch('src.ui.plot_generators.check_tables_exist')
    def test_no_data_found(self, mock_check_tables_exist, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        mock_check_tables_exist.return_value = True
        mock_conn.sql.return_value.df.return_value = pd.DataFrame() # Empty
        fig = plot_queries_by_time_period(mock_db_path, mock_connect_func, mock_logger, knesset_filter=[25]) # UPDATED
        assert fig is None
        st_mock.info.assert_called_once()


class TestPlotQueryTypesDistribution:
    @mock.patch('src.ui.plot_generators.px.bar') # Changed from pie to bar
    @mock.patch('src.ui.plot_generators.check_tables_exist')
    def test_success_single_knesset(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        mock_check_tables_exist.return_value = True
        sample_data = pd.DataFrame({
            'TypeDesc': ['Type A', 'Type B'],
            'QueryCount': [50, 75]
        })
        mock_conn.execute.return_value.df.return_value = sample_data # Assuming execute for parameterized query
        mock_px_bar.return_value = go.Figure()

        fig = plot_query_types_distribution(mock_db_path, mock_connect_func, mock_logger, knesset_filter=[25])

        assert isinstance(fig, go.Figure)
        mock_check_tables_exist.assert_called_once_with(mock_conn, ['KNS_Query'], mock_logger)
        mock_conn.execute.assert_called_once()
        mock_px_bar.assert_called_once()
        call_args = mock_px_bar.call_args[1]
        pd.testing.assert_frame_equal(call_args['data_frame'], sample_data)
        assert call_args['x'] == 'TypeDesc'
        assert call_args['y'] == 'QueryCount'
        assert 'Distribution of Query Types for Knesset 25' in call_args['title']
        st_mock.error.assert_not_called()

    def test_requires_single_knesset(self, mock_db_path, mock_connect_func, mock_logger):
        """Test that an info message is shown if not exactly one Knesset is provided."""
        fig = plot_query_types_distribution(mock_db_path, mock_connect_func, mock_logger, knesset_filter=[24, 25])
        assert fig is None
        st_mock.info.assert_called_once_with("Please select a single Knesset to view the 'Query Types Distribution' plot.")

        st_mock.reset_mock()
        fig = plot_query_types_distribution(mock_db_path, mock_connect_func, mock_logger, knesset_filter=[])
        assert fig is None
        st_mock.info.assert_called_once_with("Please select a single Knesset to view the 'Query Types Distribution' plot.")


# UPDATED Test Class name and function calls
class TestPlotAgendasByTimePeriod:
    @mock.patch('src.ui.plot_generators.px.bar')
    @mock.patch('src.ui.plot_generators.check_tables_exist')
    def test_success_single_knesset(self, mock_check_tables_exist, mock_px_bar, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        mock_check_tables_exist.return_value = True
        sample_data = pd.DataFrame({
            'TimePeriod': ['2020', '2021'],
            'AgendaCount': [10, 15]
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_px_bar.return_value = go.Figure()

        fig = plot_agendas_by_time_period(mock_db_path, mock_connect_func, mock_logger, knesset_filter=[25]) # UPDATED

        assert isinstance(fig, go.Figure)
        mock_px_bar.assert_called_once()
        call_args = mock_px_bar.call_args[1]
        assert 'Agenda Items per Year for Knesset 25' in call_args['title']
        assert call_args.get('color') is None
        st_mock.error.assert_not_called()

# Add more tests for other plot functions, ensuring to use updated names and logic for single Knesset selection
# For example, for plot_queries_by_faction_status:

class TestPlotQueryStatusByFaction:
    @mock.patch('src.ui.plot_generators.go.Figure')
    @mock.patch('src.ui.plot_generators.check_tables_exist')
    def test_success_single_knesset(self, mock_check_tables_exist, mock_go_figure, mock_db_path, mock_connect_func, mock_logger, mock_conn):
        mock_check_tables_exist.return_value = True
        sample_data = pd.DataFrame({
            'StatusDescription': ['נענתה', 'לא נענתה'],
            'FactionName': ['Likud', 'Yesh Atid'],
            'QueryCount': [100, 80]
        })
        mock_conn.sql.return_value.df.return_value = sample_data
        mock_go_figure.return_value = go.Figure()

        fig = plot_query_status_by_faction(mock_db_path, mock_connect_func, mock_logger, knesset_filter=[25])

        assert isinstance(fig, go.Figure)
        mock_go_figure.assert_called_once()
        st_mock.error.assert_not_called()

    def test_requires_single_knesset(self, mock_db_path, mock_connect_func, mock_logger):
        fig = plot_query_status_by_faction(mock_db_path, mock_connect_func, mock_logger, knesset_filter=[24, 25])
        assert fig is None
        st_mock.info.assert_called_once() # Check specific message if needed

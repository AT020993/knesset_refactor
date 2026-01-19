"""
Tests for sidebar_components module.

These tests verify the sidebar component functions work correctly.
"""
import pytest
import pandas as pd
from pathlib import Path
from unittest import mock


def _passthrough_decorator(func=None, *args, **kwargs):
    """A decorator that does nothing but return the original function."""
    if func is None:
        def wrapper(fn):
            return fn
        return wrapper
    return func


class MockSessionState(dict):
    """A dict subclass that also supports attribute access like Streamlit's session_state."""
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(f"'MockSessionState' object has no attribute '{key}'")

    def __setattr__(self, key, value):
        self[key] = value


@pytest.fixture(autouse=True)
def mock_streamlit():
    """Mock streamlit module to prevent UI initialization."""
    mock_st = mock.MagicMock()
    mock_st.session_state = MockSessionState()
    mock_st.cache_data = _passthrough_decorator
    mock_st.cache_resource = _passthrough_decorator
    mock_st.sidebar = mock.MagicMock()

    with mock.patch.dict("sys.modules", {"streamlit": mock_st}):
        yield mock_st


@pytest.fixture
def mock_logger():
    return mock.MagicMock()


@pytest.fixture
def mock_db_path(tmp_path):
    """Create a mock database path that exists."""
    db_path = tmp_path / "test.db"
    db_path.touch()
    return db_path


@pytest.fixture
def mock_connect_func():
    """Mock connection function."""
    conn_mock = mock.MagicMock()
    sql_result = mock.MagicMock()
    sql_result.df.return_value = pd.DataFrame()
    conn_mock.sql.return_value = sql_result
    conn_mock.execute.return_value = sql_result
    return mock.MagicMock(return_value=conn_mock)


@pytest.fixture
def mock_format_exc_func():
    return mock.MagicMock(return_value="Formatted Traceback")


class TestSidebarComponentsModule:
    """Test sidebar components module structure."""

    def test_module_imports_successfully(self, mock_streamlit):
        """Test that sidebar_components module can be imported."""
        from src.ui import sidebar_components
        assert sidebar_components is not None

    def test_display_sidebar_exists(self, mock_streamlit):
        """Test that display_sidebar function exists."""
        from src.ui.sidebar_components import display_sidebar
        assert callable(display_sidebar)

    def test_handle_functions_exist(self, mock_streamlit):
        """Test that handler functions exist."""
        from src.ui.sidebar_components import (
            _handle_data_refresh_button_click,
            _handle_run_query_button_click,
            _handle_explore_table_button_click,
        )
        assert callable(_handle_data_refresh_button_click)
        assert callable(_handle_run_query_button_click)
        assert callable(_handle_explore_table_button_click)


class TestHandleDataRefreshButtonClick:
    """Test the data refresh button click handler."""

    def test_refresh_already_running(self, mock_streamlit, mock_logger, mock_db_path, mock_format_exc_func):
        """Test that warning is shown when refresh is already running."""
        from src.ui.sidebar_components import _handle_data_refresh_button_click

        # Setup session state
        mock_streamlit.session_state['data_refresh_process_running'] = True

        _handle_data_refresh_button_click(mock_db_path, mock_logger, mock_format_exc_func)

        mock_streamlit.sidebar.warning.assert_called_once_with("Refresh process is already running.")

    def test_no_tables_selected(self, mock_streamlit, mock_logger, mock_db_path, mock_format_exc_func):
        """Test that warning is shown when no tables are selected."""
        from src.ui.sidebar_components import _handle_data_refresh_button_click

        mock_streamlit.session_state['data_refresh_process_running'] = False
        mock_streamlit.session_state['ms_tables_to_refresh'] = []

        _handle_data_refresh_button_click(mock_db_path, mock_logger, mock_format_exc_func)

        mock_streamlit.sidebar.warning.assert_called_once_with("⚠️ No tables selected. Please select tables from the dropdown above.")


class TestHandleRunQueryButtonClick:
    """Test the run query button click handler."""

    def test_db_not_found_no_error(self, mock_streamlit, mock_logger, mock_connect_func, mock_format_exc_func, tmp_path):
        """Test that function handles non-existent database gracefully (no-op)."""
        from src.ui.sidebar_components import _handle_run_query_button_click

        db_path = tmp_path / "nonexistent.db"  # Doesn't exist
        mock_streamlit.session_state['selected_query_name'] = "TestQuery"

        exports_dict = {"TestQuery": {"sql": "SELECT 1"}}

        # Should not raise exception - function just returns early if db doesn't exist
        _handle_run_query_button_click(
            exports_dict, db_path, mock_connect_func,
            mock_logger, mock_format_exc_func, {}
        )

        # Connection should NOT be made if db doesn't exist
        mock_connect_func.assert_not_called()

    def test_no_query_selected(self, mock_streamlit, mock_logger, mock_db_path, mock_connect_func, mock_format_exc_func):
        """Test that function handles no query selected gracefully."""
        from src.ui.sidebar_components import _handle_run_query_button_click

        mock_streamlit.session_state['selected_query_name'] = None

        exports_dict = {"TestQuery": {"sql": "SELECT * FROM test"}}

        # Should not raise exception
        _handle_run_query_button_click(
            exports_dict, mock_db_path, mock_connect_func,
            mock_logger, mock_format_exc_func, {}
        )

        # Connection should NOT be made if no query selected
        mock_connect_func.assert_not_called()


class TestHandleExploreTableButtonClick:
    """Test the explore table button click handler."""

    def test_no_table_selected(self, mock_streamlit, mock_logger, mock_db_path, mock_connect_func, mock_format_exc_func):
        """Test that function handles no table selected gracefully (no-op)."""
        from src.ui.sidebar_components import _handle_explore_table_button_click

        # Use the correct session state key
        mock_streamlit.session_state['selected_table_for_explorer'] = ""

        _handle_explore_table_button_click(
            mock_db_path, mock_connect_func,
            mock.MagicMock(return_value=[]),  # get_db_table_list
            mock.MagicMock(return_value=([], [], [])),  # get_table_columns
            mock_logger, mock_format_exc_func, {}
        )

        # Function should return early without calling connect_func when no table selected
        mock_connect_func.assert_not_called()


class TestTableDisplayNames:
    """Test the table display name mapping."""

    def test_table_display_names_exist(self, mock_streamlit):
        """Test that TABLE_DISPLAY_NAMES dictionary exists."""
        from src.ui.sidebar_components import TABLE_DISPLAY_NAMES
        assert isinstance(TABLE_DISPLAY_NAMES, dict)
        assert len(TABLE_DISPLAY_NAMES) > 0

    def test_get_display_name_function(self, mock_streamlit):
        """Test the get_table_display_name function."""
        from src.ui.sidebar_components import get_table_display_name, TABLE_DISPLAY_NAMES

        # Test known table
        known_table = list(TABLE_DISPLAY_NAMES.keys())[0]
        display_name = get_table_display_name(known_table)
        assert display_name == TABLE_DISPLAY_NAMES[known_table]

        # Test unknown table returns original name
        unknown_table = "UnknownTable123"
        display_name = get_table_display_name(unknown_table)
        assert display_name == unknown_table

    def test_get_table_name_from_display(self, mock_streamlit):
        """Test the get_table_name_from_display function."""
        from src.ui.sidebar_components import (
            get_table_name_from_display,
            get_table_display_name,
            TABLE_DISPLAY_NAMES
        )

        # Test round-trip: table -> display -> table
        for table_name in TABLE_DISPLAY_NAMES:
            display_name = get_table_display_name(table_name)
            recovered_name = get_table_name_from_display(display_name)
            assert recovered_name == table_name

        # Test unknown display name returns original
        unknown_display = "Unknown Display Name"
        result = get_table_name_from_display(unknown_display)
        assert result == unknown_display


class TestSelectAllTablesOption:
    """Test the select all tables option."""

    def test_select_all_option_exists(self, mock_streamlit):
        """Test that _SELECT_ALL_TABLES_OPTION constant exists."""
        from src.ui.sidebar_components import _SELECT_ALL_TABLES_OPTION
        assert isinstance(_SELECT_ALL_TABLES_OPTION, str)
        assert len(_SELECT_ALL_TABLES_OPTION) > 0

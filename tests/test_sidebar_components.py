import pytest
import pandas as pd
from pathlib import Path
from unittest import mock
import asyncio # For mocking asyncio.run

# Import the functions to be tested
from src.ui.sidebar_components import (
    _handle_data_refresh_button_click,
    _handle_run_query_button_click,
    _handle_explore_table_button_click,
    display_sidebar,
    # _sidebar_progress_cb, # REMOVED - Cannot import nested function
)
import src.ui.sidebar_components as sc_module # To access _SELECT_ALL_TABLES_OPTION

# Mock backend.fetch_table.TABLES at the module level where it's imported
MOCKED_TABLES_LIST = ["table1", "table2", "table3"] # Changed to list of strings

@pytest.fixture(autouse=True)
def mock_backend_tables_list(monkeypatch): # Renamed for clarity
    monkeypatch.setattr("src.ui.sidebar_components.TABLES", MOCKED_TABLES_LIST)
    monkeypatch.setattr("src.backend.fetch_table.TABLES", MOCKED_TABLES_LIST) # Also mock where it's originally defined if ft uses it


@pytest.fixture
def mock_st_session_state():
    _session_state = {}
    with mock.patch("src.ui.sidebar_components.st.session_state", _session_state, create=True):
         yield _session_state

@pytest.fixture
def mock_st_sidebar():
    with mock.patch("src.ui.sidebar_components.st.sidebar", spec=True) as mock_sidebar:
        mock_progress_bar = mock.MagicMock()
        mock_sidebar.progress.return_value = mock_progress_bar
        # mock_progress_bar.progress = mock.MagicMock() # The progress object itself is called

        mock_status_text_empty = mock.MagicMock()
        mock_status_text_empty.text = mock.MagicMock()
        mock_status_text_empty.success = mock.MagicMock()
        mock_status_text_empty.error = mock.MagicMock()
        mock_sidebar.empty.return_value = mock_status_text_empty
        yield mock_sidebar

@pytest.fixture
def mock_st_main():
    with mock.patch("src.ui.sidebar_components.st", spec=True) as mock_main_st:
        if not hasattr(mock_main_st, 'sidebar') or not isinstance(mock_main_st.sidebar, mock.MagicMock):
             mock_main_st.sidebar = mock.MagicMock(spec=True)
             mock_progress_bar = mock.MagicMock()
             # mock_main_st.sidebar.progress.return_value = mock_progress_bar # progress is called on the object
             mock_main_st.sidebar.progress.return_value.progress = mock.MagicMock()


        mock_main_st.cache_data.clear = mock.MagicMock()
        mock_main_st.cache_resource.clear = mock.MagicMock()
        mock_main_st.toast = mock.MagicMock()
        mock_main_st.rerun = mock.MagicMock()
        mock_main_st.error = mock.MagicMock()
        mock_main_st.code = mock.MagicMock()
        mock_main_st.warning = mock.MagicMock()
        mock_main_st.info = mock.MagicMock()
        mock_main_st.success = mock.MagicMock() # For messages in main area if any
        yield mock_main_st

@pytest.fixture
def mock_logger():
    return mock.MagicMock()

@pytest.fixture
def mock_db_path():
    path_mock = mock.MagicMock(spec=Path)
    path_mock.exists.return_value = True # Default to db exists
    return path_mock

@pytest.fixture
def mock_connect_func():
    conn_mock = mock.MagicMock()
    sql_execute_result_mock = mock.MagicMock()
    sql_execute_result_mock.df.return_value = pd.DataFrame()
    conn_mock.sql.return_value = sql_execute_result_mock
    execute_result_mock = mock.MagicMock()
    execute_result_mock.df.return_value = pd.DataFrame()
    conn_mock.execute.return_value = execute_result_mock
    connect_func_mock = mock.MagicMock(return_value=conn_mock)
    return connect_func_mock

@pytest.fixture
def mock_format_exc_func():
    return mock.MagicMock(return_value="Formatted Traceback")


@pytest.fixture
def mock_asyncio_run():
    with mock.patch("asyncio.run") as mock_run:
        # Make the mock execute the passed coroutine (or a mock of it)
        async def side_effect_runner(coro, *args, **kwargs):
            if hasattr(coro, 'is_mock') and coro.is_mock: # Check if it's already a mock
                 return await coro(*args, **kwargs)
            if asyncio.iscoroutine(coro):
                return await coro
            elif asyncio.iscoroutinefunction(coro):
                 return await coro(*args, **kwargs)
            return coro # Should be an awaitable if not a coroutine

        mock_run.side_effect = side_effect_runner
        yield mock_run

@pytest.fixture
def mock_refresh_tables_ft(): # Mock for backend.fetch_table.refresh_tables
    with mock.patch("src.ui.sidebar_components.ft.refresh_tables", new_callable=mock.AsyncMock) as mock_refresh:
        yield mock_refresh


class TestHandleDataRefreshButtonClick:
    def test_refresh_already_running(self, mock_st_session_state, mock_st_sidebar, mock_logger, mock_db_path, mock_format_exc_func):
        mock_st_session_state['data_refresh_process_running'] = True
        _handle_data_refresh_button_click(mock_db_path, mock_logger, mock_format_exc_func)
        mock_st_sidebar.warning.assert_called_once_with("Refresh process is already running.")

    def test_no_tables_selected(self, mock_st_session_state, mock_st_sidebar, mock_logger, mock_db_path, mock_format_exc_func):
        mock_st_session_state['data_refresh_process_running'] = False
        mock_st_session_state['ms_tables_to_refresh'] = []
        _handle_data_refresh_button_click(mock_db_path, mock_logger, mock_format_exc_func)
        mock_st_sidebar.warning.assert_called_once_with("No tables selected for refresh.")

    @mock.patch("src.ui.sidebar_components.st.rerun") # Mock st.rerun directly
    def test_successful_refresh_and_callback(
        self, mock_st_rerun, mock_st_session_state, mock_st_sidebar, mock_logger,
        mock_db_path, mock_format_exc_func, mock_asyncio_run, mock_refresh_tables_ft
    ):
        mock_st_session_state['data_refresh_process_running'] = False
        selected_tables = ["table1", "table2"]
        mock_st_session_state['ms_tables_to_refresh'] = selected_tables
        # completed_tables_count is initialized inside the handler

        # This will make asyncio.run effectively call the wrapper, which calls mock_refresh_tables_ft
        async def dummy_refresh_tables(*args, **kwargs):
            callback = kwargs.get('progress_cb')
            if callback:
                # Simulate callback for each table
                for i, table_name in enumerate(selected_tables):
                    callback(table_name, 10 * (i + 1)) # Simulate num_rows_fetched
            return None # Simulate successful completion

        mock_refresh_tables_ft.side_effect = dummy_refresh_tables

        _handle_data_refresh_button_click(mock_db_path, mock_logger, mock_format_exc_func)

        assert mock_st_session_state['data_refresh_process_running'] is True # Set at start
        mock_st_sidebar.progress.assert_called_once_with(0, text="Preparing refresh...")
        mock_st_sidebar.empty.return_value.text.assert_any_call("Initializing refresh...")

        mock_asyncio_run.assert_called_once()
        mock_refresh_tables_ft.assert_called_once()
        call_args_to_refresh = mock_refresh_tables_ft.call_args
        assert call_args_to_refresh[1]['tables'] == selected_tables
        assert call_args_to_refresh[1]['db_path'] == mock_db_path
        assert callable(call_args_to_refresh[1]['progress_cb'])

        # Verify callback effects (progress bar and status text)
        # First call by dummy_refresh_tables for "table1"
        progress_bar_mock = mock_st_sidebar.progress.return_value
        status_text_mock = mock_st_sidebar.empty.return_value

        calls_to_progress = [
            mock.call(0, text="Preparing refresh..."), # Initial call by handler
            mock.call(int((1/2)*100), text="Fetched 10 rows for table1. (1/2 tables done)"),
            mock.call(int((2/2)*100), text="Fetched 20 rows for table2. (2/2 tables done)"),
            mock.call(100, text="Refresh complete!") # Final call by handler
        ]
        # The progress_bar_mock itself is called with progress value and text
        # For st.progress(0, text=...), the first arg is the value, text is kwarg.
        # So, mock_st_sidebar.progress() returns an object, and that object is called.
        # Let's check the calls on the object returned by st.sidebar.progress
        assert progress_bar_mock.call_count == 4 # Initial + 2 from callback + final
        assert progress_bar_mock.call_args_list[0] == mock.call(0, text="Preparing refresh...")
        assert progress_bar_mock.call_args_list[1] == mock.call(50, text="Fetched 10 rows for table1. (1/2 tables done)")
        assert progress_bar_mock.call_args_list[2] == mock.call(100, text="Fetched 20 rows for table2. (2/2 tables done)")
        assert progress_bar_mock.call_args_list[3] == mock.call(100, text="Refresh complete!")


        calls_to_status_text = [
            mock.call("Initializing refresh..."),
            mock.call("Fetched 10 rows for table1. (1/2 tables done)"),
            mock.call("Fetched 20 rows for table2. (2/2 tables done)"),
        ]
        status_text_mock.text.assert_has_calls(calls_to_status_text, any_order=False)
        status_text_mock.success.assert_called_once_with("All selected tables refreshed successfully.")


        mock_st_main.cache_data.clear.assert_called_once() # From st in sidebar_components
        mock_st_main.cache_resource.clear.assert_called_once()
        mock_st_sidebar.success.assert_called_once_with("Data refresh process complete!")
        assert mock_st_session_state['data_refresh_process_running'] is False # Reset at end
        mock_st_rerun.assert_called_once()


    @mock.patch("src.ui.sidebar_components.st.rerun")
    def test_refresh_failure(
        self, mock_st_rerun, mock_st_session_state, mock_st_sidebar, mock_logger,
        mock_db_path, mock_format_exc_func, mock_asyncio_run, mock_refresh_tables_ft
    ):
        mock_st_session_state['data_refresh_process_running'] = False
        mock_st_session_state['ms_tables_to_refresh'] = ["table1"]
        error_message = "Refresh failed miserably"

        mock_refresh_tables_ft.side_effect = Exception(error_message)
        # asyncio.run will propagate this exception

        _handle_data_refresh_button_click(mock_db_path, mock_logger, mock_format_exc_func)

        # Initial UI updates happen before the error
        mock_st_sidebar.progress.assert_called_once_with(0, text="Preparing refresh...")
        mock_st_sidebar.empty.return_value.text.assert_any_call("Initializing refresh...")

        mock_asyncio_run.assert_called_once() # Attempted to run
        mock_refresh_tables_ft.assert_called_once()

        mock_st_sidebar.error.assert_called_once()
        assert error_message in mock_st_sidebar.error.call_args[0][0]
        mock_st_sidebar.code.assert_called_once()
        mock_format_exc_func.assert_called_once()

        mock_st_sidebar.empty.return_value.error.assert_called_once_with(f"Error during refresh: {error_message}")
        mock_st_sidebar.progress.return_value.assert_any_call(0, text=f"Error: {error_message}")


        assert mock_st_session_state['data_refresh_process_running'] is False
        mock_logger.error.assert_called()
        mock_st_rerun.assert_called_once()


class TestHandleRunQueryButtonClick:
    @pytest.fixture
    def mock_exports_dict(self):
        return {
            "Query1": {"sql": "SELECT * FROM table1", "knesset_filter_column": "KNum", "faction_filter_column": "FID"},
            "Query2_NoParams": {"sql": "SELECT Name FROM table2"}
        }

    def test_db_not_found(
        self, mock_st_session_state, mock_logger, mock_db_path,
        mock_connect_func, mock_exports_dict, mock_format_exc_func
    ):
        mock_db_path.exists.return_value = False
        mock_st_session_state['selected_query_name'] = "Query1"
        _handle_run_query_button_click(mock_exports_dict, mock_db_path, mock_connect_func, mock_logger, mock_format_exc_func, {})
        st.error.assert_called_once_with("Database not found. Please ensure 'data/warehouse.duckdb' exists or run data refresh.")


    @mock.patch("src.ui.sidebar_components.st.toast")
    @mock.patch("src.ui.sidebar_components.st.error")
    def test_successful_query_no_filters(
        self, mock_st_error, mock_st_toast, mock_st_session_state, mock_logger, mock_db_path,
        mock_connect_func, mock_exports_dict, mock_format_exc_func
    ):
        mock_db_path.exists.return_value = True
        query_name = "Query2_NoParams"
        mock_st_session_state['selected_query_name'] = query_name
        mock_st_session_state['ms_knesset_filter'] = []
        mock_st_session_state['ms_faction_filter'] = []

        mock_df_results = pd.DataFrame({'Name': ['Result A']})
        mock_conn = mock_connect_func.return_value
        mock_conn.sql.return_value.df.return_value = mock_df_results

        _handle_run_query_button_click(mock_exports_dict, mock_db_path, mock_connect_func, mock_logger, mock_format_exc_func, {})

        expected_sql = mock_exports_dict[query_name]["sql"].strip().rstrip(";")
        mock_conn.sql.assert_called_once_with(expected_sql)
        pd.testing.assert_frame_equal(mock_st_session_state['query_results_df'], mock_df_results)
        assert mock_st_session_state['executed_query_name'] == query_name
        mock_st_toast.assert_called_once_with(f"✅ Query '{query_name}' executed.", icon="📊")
        # st.rerun is not called by this handler

# More tests for _handle_run_query_button_click with filters and errors...
# More tests for _handle_explore_table_button_click ...

class TestDisplaySidebar:
    @pytest.fixture
    def mock_ui_utils_for_sidebar(self):
        with mock.patch("src.ui.sidebar_components.ui_utils") as mock_utils:
            mock_utils.get_filter_options_from_db.return_value = ([25, 24], pd.DataFrame({'FactionName': ['Likud', 'Yesh Atid'], 'KnessetNum': [25, 25], 'FactionID': [1,2]}))
            mock_utils.get_db_table_list.return_value = ["bills_main", "members_main"]
            yield mock_utils

    @pytest.fixture
    def mock_exports_dict_for_sidebar(self):
        return { "Query Alpha": {"sql": "SELECT alpha"}, "Query Beta": {"sql": "SELECT beta"}}

    @mock.patch("src.ui.sidebar_components._handle_data_refresh_button_click")
    @mock.patch("src.ui.sidebar_components._handle_run_query_button_click")
    @mock.patch("src.ui.sidebar_components._handle_explore_table_button_click")
    def test_widget_initialization_and_button_clicks(
        self, mock_handle_explore, mock_handle_run_query, mock_handle_refresh,
        mock_st_session_state, mock_st_sidebar, mock_logger, mock_db_path,
        mock_connect_func, mock_ui_utils_for_sidebar, mock_exports_dict_for_sidebar,
        mock_format_exc_func # Add mock_format_exc_func here
    ):
        # Faction display map needs to be created from the mock_ui_utils output
        _, f_df = mock_ui_utils_for_sidebar.get_filter_options_from_db()
        faction_display_map_arg = {f"{row['FactionName']} (K{row['KnessetNum']})": row["FactionID"] for _, row in f_df.iterrows()}


        # Simulate button clicks by controlling the return value of st.sidebar.button
        # This is a simplified way if only one button is "clicked" per call to display_sidebar
        # For more complex scenarios, a side_effect function for st.sidebar.button is better.

        # Test Data Refresh Button
        mock_st_sidebar.button.return_value = False # Default
        with mock.patch.object(mock_st_sidebar, 'button', side_effect=lambda label, key, **kwargs: key == "btn_refresh_data") as specific_button_mock:
            display_sidebar(
                db_path_arg=mock_db_path, exports_arg=mock_exports_dict_for_sidebar,
                connect_func_arg=mock_connect_func,
                get_db_table_list_func_arg=mock_ui_utils_for_sidebar.get_db_table_list,
                get_table_columns_func_arg=mock.Mock(), # Add mock for this
                get_filter_options_func_arg=mock_ui_utils_for_sidebar.get_filter_options_from_db,
                faction_display_map_arg=faction_display_map_arg,
                ui_logger_arg=mock_logger, format_exc_func_arg=mock_format_exc_func
            )
            mock_handle_refresh.assert_called_once_with(mock_db_path, mock_logger, mock_format_exc_func)
        mock_handle_refresh.reset_mock()

        # Test Run Query Button
        with mock.patch.object(mock_st_sidebar, 'button', side_effect=lambda label, key, **kwargs: key == "btn_run_query") as specific_button_mock:
            display_sidebar(
                db_path_arg=mock_db_path, exports_arg=mock_exports_dict_for_sidebar,
                connect_func_arg=mock_connect_func,
                get_db_table_list_func_arg=mock_ui_utils_for_sidebar.get_db_table_list,
                get_table_columns_func_arg=mock.Mock(),
                get_filter_options_func_arg=mock_ui_utils_for_sidebar.get_filter_options_from_db,
                faction_display_map_arg=faction_display_map_arg,
                ui_logger_arg=mock_logger, format_exc_func_arg=mock_format_exc_func
            )
            mock_handle_run_query.assert_called_once_with(
                mock_exports_dict_for_sidebar, mock_db_path, mock_connect_func, mock_logger, mock_format_exc_func, faction_display_map_arg
            )
        mock_handle_run_query.reset_mock()

        # Test Explore Table Button
        with mock.patch.object(mock_st_sidebar, 'button', side_effect=lambda label, key, **kwargs: key == "btn_explore_table") as specific_button_mock:
            display_sidebar(
                db_path_arg=mock_db_path, exports_arg=mock_exports_dict_for_sidebar,
                connect_func_arg=mock_connect_func,
                get_db_table_list_func_arg=mock_ui_utils_for_sidebar.get_db_table_list,
                get_table_columns_func_arg=mock.Mock(), # Pass the actual mock
                get_filter_options_func_arg=mock_ui_utils_for_sidebar.get_filter_options_from_db,
                faction_display_map_arg=faction_display_map_arg,
                ui_logger_arg=mock_logger, format_exc_func_arg=mock_format_exc_func
            )
            mock_handle_explore.assert_called_once_with(
                mock_db_path, mock_connect_func,
                mock_ui_utils_for_sidebar.get_db_table_list, # Pass the actual mock func
                mock.ANY, # get_table_columns_func_arg is passed
                mock_logger, mock_format_exc_func, faction_display_map_arg
            )
        mock_handle_explore.reset_mock()


        # Check widget initialization calls (these happen on every call to display_sidebar)
        mock_st_sidebar.multiselect.assert_any_call(
            label="Select tables to refresh/fetch:",
            options=[sc_module._SELECT_ALL_TABLES_OPTION] + MOCKED_TABLES_LIST, # Use constant from module
            default=mock.ANY, # Default can vary based on session state
            key="ms_tables_to_refresh_widget",
        )
        mock_st_sidebar.selectbox.assert_any_call(
            "Select a predefined query:",
            options=[""] + list(mock_exports_dict_for_sidebar.keys()),
            index=mock.ANY,
            key="sb_selected_query_name",
        )
        mock_st_sidebar.selectbox.assert_any_call(
            "Select a table to explore:",
            options=[""] + ["bills_main", "members_main"],
            index=mock.ANY,
            key="sb_selected_table_explorer",
        )
        mock_st_sidebar.multiselect.assert_any_call(
            "Knesset Number(s):",
            options=[25, 24],
            default=mock.ANY,
            key="ms_knesset_filter_widget",
        )
        mock_st_sidebar.multiselect.assert_any_call(
            "Faction(s) (by Knesset):",
            options=list(faction_display_map_arg.keys()),
            default=mock.ANY,
            help=mock.ANY,
            key="ms_faction_filter_widget",
        )

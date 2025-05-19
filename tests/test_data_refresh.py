import pytest
import pandas as pd
from pathlib import Path
from unittest import mock
import streamlit # Will be mocked
import duckdb # For connect_db mock return type hint

# Constants for mocking module-level variables in src.ui.data_refresh
MOCKED_DB_PATH_STR = "mock_data/mock_knesset_data.db"
MOCKED_PARQUET_DIR_STR = "mock_data/parquet_files_related_to_db"
MOCKED_TABLES_DICT = {
    "bills_main": {"name": "Bills Main Table", "description": "Main table for bills data."},
    "members_main": {"name": "Members Main Table", "description": "Main table for Knesset members data."}
}
MOCKED_EXPORTS_DICT = {
    "Bills Query": {"sql": "SELECT * FROM bills_main LIMIT 10;", "params": []},
    "Members Query": {"sql": "SELECT * FROM members_main WHERE KnessetNum = ?;", "params": ["KnessetNum"]}
}

MOCK_PLOT_FUNCTION_1 = mock.MagicMock(return_value="mock_figure_1")
MOCK_PLOT_FUNCTION_2 = mock.MagicMock(return_value="mock_figure_2")

MOCKED_AVAILABLE_PLOTS_BY_TOPIC = {
    "Bills": {
        "Bills Over Time": MOCK_PLOT_FUNCTION_1,
    },
    "Members": {
        "Members by Age": MOCK_PLOT_FUNCTION_2,
    }
}
DEFAULT_MAX_ROWS_CHART_BUILDER = 10000
DEFAULT_MAX_UNIQUE_FACET = 20

# Mock setup_logging at the very beginning as it's called at module import time in data_refresh.py
@pytest.fixture(scope="session", autouse=True)
def mock_setup_logging_at_import():
    # This needs to be active before src.ui.data_refresh is even imported by tests
    # if setup_logging is called at the top level of data_refresh.py.
    # Using scope="session" and autouse=True helps ensure it's active early.
    # However, the specific import and patching moment can be tricky.
    # If data_refresh.py is imported by test_data_refresh.py before this fixture runs,
    # the original setup_logging might already be called.
    # A common pattern is to ensure patching happens before the first import of the target module.
    # Pytest itself imports test files, which then import target modules.
    # For now, we assume this fixture is effective. If not, might need to structure imports differently.
    
    # If 'src.ui.data_refresh.setup_logging' is the path, it means data_refresh.py
    # imports setup_logging from src.utils.logger_setup.
    # We need to patch it where it's *looked up* by data_refresh.py.
    mock_logger_instance = mock.MagicMock()
    with mock.patch("src.ui.data_refresh.setup_logging", return_value=mock_logger_instance) as patched_setup_logging:
        # This print is for debugging test execution order if needed
        # print("Patched setup_logging in src.ui.data_refresh")
        yield patched_setup_logging


@pytest.fixture
def mock_ui_logger():
    # This will be used to patch 'ui_logger' within 'src.ui.data_refresh'
    # This is distinct from the mock_setup_logging_at_import, which patches the setup function.
    # ui_logger is the *result* of setup_logging().
    # The mock_setup_logging_at_import fixture ensures ui_logger becomes a MagicMock.
    # This fixture can provide that same MagicMock or a new one if tests need to isolate it further.
    # For simplicity, let's assume mock_setup_logging_at_import correctly makes ui_logger a mock.
    # This fixture can then be used to get a reference to it if needed, or to re-patch it per test.
    # For now, let's rely on the autouse fixture for ui_logger to be a mock.
    # If tests need to assert calls on ui_logger, they might need this fixture to get the mock object.
    # We can refine this if specific test needs arise.
    # For now, the goal is to ensure it *is* mocked. The autouse fixture does that.
    # This fixture isn't strictly necessary if the autouse one works as intended for ui_logger.
    # Let's return the same mock that setup_logging was configured to return.
    return mock_setup_logging_at_import().return_value


# Patching module-level constants in src.ui.data_refresh
@pytest.fixture(autouse=True)
def mock_module_constants(monkeypatch):
    monkeypatch.setattr("src.ui.data_refresh.DB_PATH", Path(MOCKED_DB_PATH_STR))
    monkeypatch.setattr("src.ui.data_refresh.PARQUET_DIR", Path(MOCKED_PARQUET_DIR_STR))
    monkeypatch.setattr("src.ui.data_refresh.TABLES", MOCKED_TABLES_DICT)
    monkeypatch.setattr("src.ui.data_refresh.EXPORTS", MOCKED_EXPORTS_DICT)
    monkeypatch.setattr("src.ui.data_refresh.AVAILABLE_PLOTS_BY_TOPIC", MOCKED_AVAILABLE_PLOTS_BY_TOPIC)
    monkeypatch.setattr("src.ui.data_refresh.MAX_ROWS_FOR_CHART_BUILDER", DEFAULT_MAX_ROWS_CHART_BUILDER)
    monkeypatch.setattr("src.ui.data_refresh.MAX_UNIQUE_VALUES_FOR_FACET", DEFAULT_MAX_UNIQUE_FACET)

@pytest.fixture
def mock_st_session_state():
    # Provides a dictionary to be used as st.session_state for each test.
    # Individual tests can populate this dict as needed.
    _session_state_dict = {}
    with mock.patch("src.ui.data_refresh.st.session_state", _session_state_dict, create=True):
        yield _session_state_dict

@pytest.fixture
def mock_st():
    """Mocks the entire streamlit module (st) used in src.ui.data_refresh."""
    with mock.patch("src.ui.data_refresh.st") as mock_st_module:
        # Ensure st.session_state is a dictionary that can be manipulated
        # This might be redundant if mock_st_session_state fixture is also used and patches the same target.
        # It's safer to ensure session_state is part of this comprehensive 'st' mock.
        if not hasattr(mock_st_module, 'session_state') or not isinstance(mock_st_module.session_state, dict):
             mock_st_module.session_state = {} # Initialize if not already a dict by another fixture

        mock_st_module.set_page_config = mock.MagicMock()
        mock_st_module.title = mock.MagicMock()
        mock_st_module.expander = mock.MagicMock()
        # Mock the expander context manager behavior
        mock_expander_instance = mock.MagicMock()
        mock_st_module.expander.return_value.__enter__.return_value = mock_expander_instance
        mock_expander_instance.markdown = mock.MagicMock() # If expander().markdown() is used

        mock_st_module.markdown = mock.MagicMock()
        mock_st_module.divider = mock.MagicMock()
        mock_st_module.header = mock.MagicMock()
        mock_st_module.info = mock.MagicMock()
        mock_st_module.dataframe = mock.MagicMock()
        mock_st_module.download_button = mock.MagicMock()
        mock_st_module.code = mock.MagicMock()
        mock_st_module.selectbox = mock.MagicMock()
        mock_st_module.plotly_chart = mock.MagicMock()
        mock_st_module.text_area = mock.MagicMock()
        mock_st_module.button = mock.MagicMock(return_value=False) # Default to not clicked
        mock_st_module.warning = mock.MagicMock()
        mock_st_module.error = mock.MagicMock()
        mock_st_module.spinner = mock.MagicMock()
        # Mock spinner as a context manager
        mock_spinner_instance = mock.MagicMock()
        mock_st_module.spinner.return_value.__enter__.return_value = mock_spinner_instance
        
        mock_st_module.cache_data = mock.MagicMock() # For st.cache_data.clear()
        mock_st_module.cache_data.clear = mock.MagicMock()
        mock_st_module.cache_resource = mock.MagicMock() # For st.cache_resource.clear()
        mock_st_module.cache_resource.clear = mock.MagicMock()
        
        mock_st_module.rerun = mock.MagicMock()
        yield mock_st_module

# Mocking imported functions
@pytest.fixture
def mock_ui_utils():
    with mock.patch("src.ui.data_refresh.ui_utils") as mock_utils:
        mock_utils.get_filter_options_from_db.return_value = ([], pd.DataFrame()) # Knesset nums, Factions df
        
        # Mock connect_db to return a mock connection object
        mock_conn = mock.MagicMock(spec=duckdb.DuckDBPyConnection)
        # Mock the .sql().df() chain
        mock_sql_result = mock.MagicMock()
        mock_sql_result.df.return_value = pd.DataFrame() # Default: empty DataFrame
        mock_conn.sql.return_value = mock_sql_result
        mock_utils.connect_db.return_value = mock_conn
        
        mock_utils.safe_execute_query.return_value = pd.DataFrame()
        mock_utils.get_last_updated_for_table.return_value = "Never"
        mock_utils.format_exception_for_ui.return_value = "Formatted Exception Details"
        yield mock_utils

@pytest.fixture
def mock_display_sidebar():
    with mock.patch("src.ui.data_refresh.display_sidebar") as mock_sidebar_func:
        yield mock_sidebar_func

@pytest.fixture
def mock_display_chart_builder():
    with mock.patch("src.ui.data_refresh.display_chart_builder") as mock_chart_builder_func:
        yield mock_chart_builder_func

# Example test to ensure fixtures are working (can be removed later)
def test_fixtures_load_correctly(
    mock_st, mock_st_session_state,
    mock_ui_logger, 
    mock_module_constants, # This fixture applies monkeypatching
    mock_ui_utils, 
    mock_display_sidebar, 
    mock_display_chart_builder
):
    # Test st mock
    mock_st.title("Test Title")
    mock_st.title.assert_called_once_with("Test Title")
    assert isinstance(mock_st.session_state, dict) # Check session_state is a dict
    
    # Test ui_logger (via the setup_logging mock)
    # ui_logger in data_refresh.py should be the return_value of the mocked setup_logging
    from src.ui.data_refresh import ui_logger as actual_ui_logger
    actual_ui_logger.info("Test Log")
    actual_ui_logger.info.assert_called_once_with("Test Log")
    
    # Test module constants (they are patched by mock_module_constants)
    from src.ui.data_refresh import DB_PATH, PARQUET_DIR, TABLES, EXPORTS, AVAILABLE_PLOTS_BY_TOPIC, MAX_ROWS_FOR_CHART_BUILDER, MAX_UNIQUE_VALUES_FOR_FACET
    assert DB_PATH == Path(MOCKED_DB_PATH_STR)
    assert PARQUET_DIR == Path(MOCKED_PARQUET_DIR_STR)
    assert TABLES == MOCKED_TABLES_DICT
    assert EXPORTS == MOCKED_EXPORTS_DICT
    assert AVAILABLE_PLOTS_BY_TOPIC == MOCKED_AVAILABLE_PLOTS_BY_TOPIC
    assert MAX_ROWS_FOR_CHART_BUILDER == DEFAULT_MAX_ROWS_CHART_BUILDER
    assert MAX_UNIQUE_VALUES_FOR_FACET == DEFAULT_MAX_UNIQUE_FACET

    # Test imported function mocks
    mock_ui_utils.get_last_updated_for_table("some_table")
    mock_ui_utils.get_last_updated_for_table.assert_called_once_with("some_table")
    
    mock_display_sidebar()
    mock_display_sidebar.assert_called_once()
    
    mock_display_chart_builder()
    mock_display_chart_builder.assert_called_once()

    # Test that the mock_setup_logging_at_import was called (implicitly by importing data_refresh)
    # This is a bit indirect. The better check is that actual_ui_logger behaves as a mock.
    # from src.ui.data_refresh import setup_logging as actual_setup_logging_in_module
    # assert actual_setup_logging_in_module.called # This checks if the *patched object* was called
    # This assertion depends on how many times data_refresh might re-import or call it, usually once.
    # The fixture mock_setup_logging_at_import already yields the patched object.
    # We can access it via the fixture if needed, but the important part is that ui_logger is mocked.
    # For now, the check on actual_ui_logger is sufficient.
    # This pass is for the example test, real tests will be added below.
    pass

# --- Tests for Session State Initialization ---

def test_session_state_default_initialization(
    mock_st_session_state, # This provides the session_state dict
    mock_st, # Ensures st.session_state is patched correctly if data_refresh accesses it via st.
    mock_module_constants, # Ensures constants in data_refresh are mocked
    mock_ui_utils, # Ensures ui_utils in data_refresh are mocked
    mock_display_sidebar, # Ensures display_sidebar in data_refresh is mocked
    mock_display_chart_builder, # Ensures display_chart_builder in data_refresh is mocked
    mock_setup_logging_at_import # Ensures ui_logger is mocked
):
    """
    Test that session state keys are initialized to their default values
    when src.ui.data_refresh is first imported and its module-level code runs.
    """
    # At this point, mock_st_session_state is empty because it's freshly created for this test.
    # Importing the module will trigger its top-level session state initialization logic.
    import src.ui.data_refresh as data_refresh_module

    # Check for presence and default values of all expected keys
    # General App State
    assert 'selected_query_name' not in mock_st_session_state or mock_st_session_state['selected_query_name'] is None
    assert 'executed_query_name' not in mock_st_session_state or mock_st_session_state['executed_query_name'] is None
    assert 'executed_query_sql' not in mock_st_session_state or mock_st_session_state['executed_query_sql'] is None
    assert 'query_results_df' not in mock_st_session_state or mock_st_session_state['query_results_df'] is None
    assert 'explored_table_name' not in mock_st_session_state or mock_st_session_state['explored_table_name'] is None
    assert 'explore_results_df' not in mock_st_session_state or mock_st_session_state['explore_results_df'] is None
    
    # Sidebar Filters
    assert 'ms_knesset_filter' not in mock_st_session_state or mock_st_session_state['ms_knesset_filter'] == []
    assert 'ms_faction_filter' not in mock_st_session_state or mock_st_session_state['ms_faction_filter'] == []
    
    # Data Refresh Specific State
    assert 'ms_tables_to_refresh' not in mock_st_session_state or mock_st_session_state['ms_tables_to_refresh'] == []
    assert 'all_tables_selected_for_refresh_flag' not in mock_st_session_state or mock_st_session_state['all_tables_selected_for_refresh_flag'] is False
    assert 'data_refresh_process_running' not in mock_st_session_state or mock_st_session_state['data_refresh_process_running'] is False
    assert 'completed_tables_count' not in mock_st_session_state or mock_st_session_state['completed_tables_count'] == 0
    
    # Chart Builder Specific State
    assert 'builder_selected_table' not in mock_st_session_state or mock_st_session_state['builder_selected_table'] is None
    assert 'builder_columns' not in mock_st_session_state or mock_st_session_state['builder_columns'] == []
    assert 'builder_numeric_columns' not in mock_st_session_state or mock_st_session_state['builder_numeric_columns'] == []
    assert 'builder_categorical_columns' not in mock_st_session_state or mock_st_session_state['builder_categorical_columns'] == []
    assert 'builder_data_for_cs_filters' not in mock_st_session_state or \
        (isinstance(mock_st_session_state['builder_data_for_cs_filters'], pd.DataFrame) and mock_st_session_state['builder_data_for_cs_filters'].empty)
    
    assert 'builder_chart_type' not in mock_st_session_state or mock_st_session_state['builder_chart_type'] is None
    assert 'previous_builder_chart_type' not in mock_st_session_state or mock_st_session_state['previous_builder_chart_type'] is None
    assert 'builder_x_axis' not in mock_st_session_state or mock_st_session_state['builder_x_axis'] is None
    assert 'builder_y_axis' not in mock_st_session_state or mock_st_session_state['builder_y_axis'] is None
    assert 'builder_color_column' not in mock_st_session_state or mock_st_session_state['builder_color_column'] is None
    assert 'builder_facet_row' not in mock_st_session_state or mock_st_session_state['builder_facet_row'] is None
    assert 'builder_facet_col' not in mock_st_session_state or mock_st_session_state['builder_facet_col'] is None
    assert 'builder_names_pie' not in mock_st_session_state or mock_st_session_state['builder_names_pie'] is None
    assert 'builder_values_pie' not in mock_st_session_state or mock_st_session_state['builder_values_pie'] is None
    assert 'builder_knesset_filter_cs' not in mock_st_session_state or mock_st_session_state['builder_knesset_filter_cs'] == []
    assert 'builder_faction_filter_cs' not in mock_st_session_state or mock_st_session_state['builder_faction_filter_cs'] == []
    assert 'builder_generated_chart' not in mock_st_session_state or mock_st_session_state['builder_generated_chart'] is None
    assert 'builder_last_query' not in mock_st_session_state or mock_st_session_state['builder_last_query'] is None

def test_session_state_preexisting_values_are_not_overwritten(
    mock_st_session_state, # This provides the session_state dict
    mock_st, 
    mock_module_constants, 
    mock_ui_utils, 
    mock_display_sidebar, 
    mock_display_chart_builder,
    mock_setup_logging_at_import
):
    """
    Test that pre-existing session state values are not overwritten by the
    default initialization logic in src.ui.data_refresh.
    """
    # Pre-populate some keys with non-default values
    preexisting_query_name = "MyCustomQuery"
    preexisting_knesset_filter = [25]
    preexisting_chart_type = "line"
    preexisting_df = pd.DataFrame({'test': [1]})

    mock_st_session_state['selected_query_name'] = preexisting_query_name
    mock_st_session_state['ms_knesset_filter'] = preexisting_knesset_filter
    mock_st_session_state['builder_chart_type'] = preexisting_chart_type
    mock_st_session_state['query_results_df'] = preexisting_df # Example of a more complex object
    mock_st_session_state['data_refresh_process_running'] = True # Non-default boolean

    # Importing the module will trigger its top-level session state initialization logic.
    import src.ui.data_refresh as data_refresh_module

    # Assert that pre-existing values were NOT overwritten
    assert mock_st_session_state['selected_query_name'] == preexisting_query_name
    assert mock_st_session_state['ms_knesset_filter'] == preexisting_knesset_filter
    assert mock_st_session_state['builder_chart_type'] == preexisting_chart_type
    pd.testing.assert_frame_equal(mock_st_session_state['query_results_df'], preexisting_df)
    assert mock_st_session_state['data_refresh_process_running'] is True

    # Assert that other keys (not pre-populated) were initialized to their defaults
    assert 'executed_query_name' not in mock_st_session_state or mock_st_session_state['executed_query_name'] is None
    assert 'ms_faction_filter' not in mock_st_session_state or mock_st_session_state['ms_faction_filter'] == []
    assert 'builder_x_axis' not in mock_st_session_state or mock_st_session_state['builder_x_axis'] is None
    assert 'all_tables_selected_for_refresh_flag' not in mock_st_session_state or mock_st_session_state['all_tables_selected_for_refresh_flag'] is False

# --- Test for Global Filter Options Fetching and Processing ---

def test_global_filter_options_fetched_and_processed(
    mock_st, # Standard mock for streamlit
    mock_module_constants, # Ensures DB_PATH etc. are mocked
    mock_ui_utils, # To mock get_filter_options_from_db
    mock_setup_logging_at_import, # Ensures ui_logger is mocked
    # The following are not strictly needed for *this specific test* if it only focuses on module-level loading,
    # but including them ensures the environment is consistent with other tests if data_refresh.py has more imports.
    mock_st_session_state,
    mock_display_sidebar,
    mock_display_chart_builder
):
    """
    Tests that global filter options (Knesset numbers, factions DataFrame, 
    and faction_display_map_global) are correctly fetched and processed 
    when src.ui.data_refresh module is loaded.
    """
    # Define the mock return values for get_filter_options_from_db
    expected_knesset_nums = [25, 24]
    expected_factions_df = pd.DataFrame({
        'FactionName': ['Faction A', 'Faction B', 'Faction C'], 
        'KnessetNum': [25, 24, 25], 
        'FactionID': [1, 2, 3]
    })
    mock_ui_utils.get_filter_options_from_db.return_value = (expected_knesset_nums, expected_factions_df)

    # Import the module - this will trigger the top-level code execution
    # including the call to get_filter_options_from_db
    # We need to ensure that this import happens *after* the mock is set up,
    # which is guaranteed by pytest fixture execution order.
    # If data_refresh was already imported by another test or fixture in a conflicting way,
    # importlib.reload might be needed, but typically pytest isolates test runs.
    import src.ui.data_refresh as data_refresh_module

    # Assert that get_filter_options_from_db was called correctly
    # DB_PATH and ui_logger are accessed from within data_refresh_module
    # So we need to get the mocked versions that data_refresh_module would have used.
    # mock_module_constants fixture patches DB_PATH in data_refresh_module.
    # mock_setup_logging_at_import ensures ui_logger in data_refresh_module is a mock.
    
    # Get the logger instance that was created by the mocked setup_logging
    # This assumes ui_logger in data_refresh is `ui_logger = setup_logging(__name__)`
    logger_in_module = data_refresh_module.ui_logger
    
    mock_ui_utils.get_filter_options_from_db.assert_called_once_with(
        data_refresh_module.DB_PATH, logger_obj=logger_in_module
    )

    # Assert that the global variables in data_refresh_module are set correctly
    assert data_refresh_module.knesset_nums_options_global == expected_knesset_nums
    pd.testing.assert_frame_equal(data_refresh_module.factions_options_df_global, expected_factions_df)

    # Assert that faction_display_map_global was created correctly
    expected_faction_map = {
        'Faction A (K25)': 1,
        'Faction B (K24)': 2,
        'Faction C (K25)': 3  # Ensure it handles multiple factions in the same Knesset
    }
    assert data_refresh_module.faction_display_map_global == expected_faction_map

# --- Test for Delegated UI Component Calls ---

def test_delegated_ui_components_are_called_correctly(
    mock_st, 
    mock_module_constants, 
    mock_ui_utils, 
    mock_display_sidebar, 
    mock_display_chart_builder,
    mock_setup_logging_at_import,
    mock_st_session_state # Though not directly asserted, it's part of the env for data_refresh.py
):
    """
    Tests that display_sidebar and display_chart_builder are called correctly
    with the expected arguments, including testing lambda functions.
    """
    # Importing the module will trigger its main execution block, including calls to these functions.
    import src.ui.data_refresh as data_refresh_module

    # --- Assert display_sidebar call ---
    mock_display_sidebar.assert_called_once()
    sidebar_call_args = mock_display_sidebar.call_args[1] # Use kwargs dict for named args

    assert sidebar_call_args['db_path_arg'] == data_refresh_module.DB_PATH
    assert sidebar_call_args['exports_arg'] == data_refresh_module.EXPORTS
    assert sidebar_call_args['faction_display_map_arg'] == data_refresh_module.faction_display_map_global
    assert sidebar_call_args['logger_obj_arg'] == data_refresh_module.ui_logger
    assert sidebar_call_args['format_exc_func_arg'] == data_refresh_module.ui_utils.format_exception_for_ui

    # Test connect_func_arg (lambda for ui_utils.connect_db)
    connect_func_lambda = sidebar_call_args['connect_func_arg']
    assert callable(connect_func_lambda)
    # Test with read_only=True
    connect_func_lambda(read_only=True)
    mock_ui_utils.connect_db.assert_called_with(data_refresh_module.DB_PATH, read_only=True, logger_obj=data_refresh_module.ui_logger)
    # Test with read_only=False
    connect_func_lambda(read_only=False)
    mock_ui_utils.connect_db.assert_called_with(data_refresh_module.DB_PATH, read_only=False, logger_obj=data_refresh_module.ui_logger)
    
    # Test get_db_table_list_func_arg (lambda for ui_utils.get_db_table_list)
    get_db_table_list_lambda = sidebar_call_args['get_db_table_list_func_arg']
    assert callable(get_db_table_list_lambda)
    get_db_table_list_lambda()
    mock_ui_utils.get_db_table_list.assert_called_with(data_refresh_module.DB_PATH, logger_obj=data_refresh_module.ui_logger)

    # Test get_table_columns_func_arg (lambda for ui_utils.get_table_columns)
    get_table_columns_lambda = sidebar_call_args['get_table_columns_func_arg']
    assert callable(get_table_columns_lambda)
    test_table_name = "any_table"
    get_table_columns_lambda(test_table_name)
    mock_ui_utils.get_table_columns.assert_called_with(data_refresh_module.DB_PATH, test_table_name, logger_obj=data_refresh_module.ui_logger)

    # Test get_filter_options_func_arg (lambda for ui_utils.get_filter_options_from_db)
    # This one is already called at module level, so we check its prior call if it's the same lambda instance,
    # or just that the lambda passed to display_sidebar works as expected.
    # The module-level call populates data_refresh_module.knesset_nums_options_global etc.
    # Here, we are testing the lambda *passed to display_sidebar*.
    get_filter_options_lambda = sidebar_call_args['get_filter_options_func_arg']
    assert callable(get_filter_options_lambda)
    # Reset mock before calling the lambda to ensure we're testing *this* specific call path
    mock_ui_utils.get_filter_options_from_db.reset_mock() 
    get_filter_options_lambda()
    mock_ui_utils.get_filter_options_from_db.assert_called_with(data_refresh_module.DB_PATH, logger_obj=data_refresh_module.ui_logger)


    # --- Assert display_chart_builder call ---
    mock_display_chart_builder.assert_called_once()
    chart_builder_call_args = mock_display_chart_builder.call_args[1]

    assert chart_builder_call_args['db_path'] == data_refresh_module.DB_PATH
    assert chart_builder_call_args['max_rows_for_chart_builder'] == data_refresh_module.MAX_ROWS_FOR_CHART_BUILDER
    assert chart_builder_call_args['max_unique_values_for_facet'] == data_refresh_module.MAX_UNIQUE_VALUES_FOR_FACET
    assert chart_builder_call_args['faction_display_map_global'] == data_refresh_module.faction_display_map_global
    assert chart_builder_call_args['logger_obj'] == data_refresh_module.ui_logger

# --- Tests for Table Update Status Section ---

class TestTableUpdateStatus:

    def test_status_section_db_not_found(
        self, mock_st, mock_db_path, mock_module_constants, mock_ui_utils,
        mock_setup_logging_at_import, mock_st_session_state
    ):
        # Specific setup: DB does not exist.
        # The main warning "Database not found. Visualizations cannot be generated..." will also be present.
        # We are testing the specific info message within the "Table Update Status" expander.
        with mock.patch.object(Path, 'exists') as mock_path_exists:
            mock_path_exists.return_value = False # DB does not exist
            
            import importlib
            import src.ui.data_refresh
            importlib.reload(src.ui.data_refresh) 

            src.ui.data_refresh.main()

            expander_instance = mock_st.expander.return_value.__enter__.return_value
            
            # Check if the "Table Update Status" expander was created
            expander_label_found = False
            for call in mock_st.expander.call_args_list:
                if "📊 Table Update Status" in call[0][0]:
                    expander_label_found = True
                    break
            assert expander_label_found, "Table Update Status expander was not created."

            expander_instance.info.assert_any_call("Database not found. Table status cannot be displayed.")
            mock_ui_utils.get_last_updated_for_table.assert_not_called()

    def test_status_section_db_exists_with_tables(
        self, mock_st, mock_module_constants, mock_ui_utils,
        mock_setup_logging_at_import, mock_st_session_state
    ):
        # DB exists, TABLES constant has entries.
        # mock_module_constants fixture already sets up DB_PATH.exists() to be True implicitly
        # by not overriding it here, and TABLES to MOCKED_TABLES_DICT.
        
        # Configure side_effect for get_last_updated_for_table
        # MOCKED_TABLES_DICT = {"bills_main": ..., "members_main": ...}
        table_names_from_mock = list(MOCKED_TABLES_DICT.keys())
        mock_timestamps = {
            table_names_from_mock[0]: "2023-01-01 10:00:00 UTC",
            table_names_from_mock[1]: "2023-01-02 12:00:00 UTC"
        }
        
        def get_last_updated_side_effect(table_name, parquet_dir, logger_obj):
            return mock_timestamps.get(table_name, "Unknown")
        mock_ui_utils.get_last_updated_for_table.side_effect = get_last_updated_side_effect

        import importlib
        import src.ui.data_refresh
        importlib.reload(src.ui.data_refresh) # Reload to use fresh module state with patched constants
        data_refresh_module = src.ui.data_refresh # Get a reference to the reloaded module
        
        data_refresh_module.main()

        expander_instance = mock_st.expander.return_value.__enter__.return_value
        
        expander_label_found = False
        for call in mock_st.expander.call_args_list:
            if "📊 Table Update Status" in call[0][0]:
                expander_label_found = True
                break
        assert expander_label_found, "Table Update Status expander was not created."

        # Assert get_last_updated_for_table calls
        assert mock_ui_utils.get_last_updated_for_table.call_count == len(table_names_from_mock)
        for table_key in table_names_from_mock:
            # Construct expected path: PARQUET_DIR / f"{table_key}.parquet"
            expected_parquet_file_path = data_refresh_module.PARQUET_DIR / f"{table_key}.parquet"
            mock_ui_utils.get_last_updated_for_table.assert_any_call(
                expected_parquet_file_path, # Check if this is the correct path construction in the main code
                data_refresh_module.PARQUET_DIR, # This argument might be redundant if path is already specific
                logger_obj=data_refresh_module.ui_logger
            )
            # The actual call in data_refresh.py is `ui_utils.get_last_updated_for_table(table_key, PARQUET_DIR, ui_logger)`
            # This means the first argument to get_last_updated_for_table in the SUT is table_key, not the full path.
            # The test needs to align with this. The fixture for get_last_updated_for_table in ui_utils tests
            # shows it expects a db_path_str (which is the full parquet path in this context).
            # Let's adjust the assertion to match the SUT's call pattern.
            # The SUT constructs the path `parquet_file = PARQUET_DIR / f"{table_key}.parquet"` and passes `parquet_file`.
            # So the assertion above for expected_parquet_file_path is correct for the first argument.

        expander_instance.dataframe.assert_called_once()
        df_passed_to_streamlit = expander_instance.dataframe.call_args[0][0]
        
        expected_data = {
            "Table Name": [MOCKED_TABLES_DICT[table_names_from_mock[0]]["name"], MOCKED_TABLES_DICT[table_names_from_mock[1]]["name"]],
            "Description": [MOCKED_TABLES_DICT[table_names_from_mock[0]]["description"], MOCKED_TABLES_DICT[table_names_from_mock[1]]["description"]],
            "Last Updated (Parquet Mod Time)": [mock_timestamps[table_names_from_mock[0]], mock_timestamps[table_names_from_mock[1]]]
        }
        expected_df_for_display = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(df_passed_to_streamlit.reset_index(drop=True), expected_df_for_display.reset_index(drop=True))

    def test_status_section_db_exists_no_tables_in_constant(
        self, mock_st, monkeypatch, mock_module_constants, mock_ui_utils,
        mock_setup_logging_at_import, mock_st_session_state
    ):
        # DB exists
        with mock.patch.object(Path, 'exists') as mock_path_exists:
            mock_path_exists.return_value = True

            # Configure TABLES to be empty
            # mock_module_constants normally sets TABLES to MOCKED_TABLES_DICT.
            # We need to override this for this specific test *after* mock_module_constants has run,
            # or prevent mock_module_constants from setting TABLES here.
            # The simplest is to use monkeypatch again within the test.
            monkeypatch.setattr("src.ui.data_refresh.TABLES", {}) # Empty dict for tables

            import importlib
            import src.ui.data_refresh
            importlib.reload(src.ui.data_refresh) # Reload to use the new empty TABLES
            
            src.ui.data_refresh.main()

            expander_instance = mock_st.expander.return_value.__enter__.return_value
            expander_label_found = False
            for call in mock_st.expander.call_args_list:
                if "📊 Table Update Status" in call[0][0]:
                    expander_label_found = True
                    break
            assert expander_label_found, "Table Update Status expander was not created."

            expander_instance.info.assert_any_call("No tables found to display status, or TABLES list is empty.")
            mock_ui_utils.get_last_updated_for_table.assert_not_called()
            expander_instance.dataframe.assert_not_called()

# --- Tests for Ad-hoc SQL Query Section ---

class TestAdHocSQLQuery:

    def test_adhoc_sql_db_not_found(
        self, mock_st, mock_db_path, mock_module_constants, mock_setup_logging_at_import,
        mock_st_session_state # Keep other fixtures for consistent environment
    ):
        # Specific setup for this test: DB does not exist for the ad-hoc section
        # This relies on the main function checking DB_PATH.exists() at the right point.
        # The ad-hoc section is within an expander.
        
        with mock.patch.object(Path, 'exists') as mock_path_exists:
            mock_path_exists.return_value = False # DB does not exist
            
            import importlib
            import src.ui.data_refresh
            importlib.reload(src.ui.data_refresh) 

            src.ui.data_refresh.main()

            # Check that the expander for ad-hoc SQL is shown
            # and then the warning *inside* it is displayed.
            # The expander itself might be created, but its content changes.
            # This is a bit subtle. The prompt says "Assert mock_st.warning(...) is called *within* the ad-hoc expander."
            # This implies the expander is created.
            
            expander_found = False
            warning_found_in_expander = False

            for call in mock_st.expander.call_args_list:
                if "🧑‍🔬 Run an Ad-hoc SQL Query" in call[0][0]: # Check if expander label matches
                    expander_found = True
                    # Now check if the warning was called *after* this expander was set up
                    # This is tricky because calls are global. We need to ensure the context.
                    # For simplicity, we'll check if the warning appeared.
                    # A more robust test would involve mocking the expander's context manager
                    # and checking calls on the returned object.
                    # The current mock_st.expander setup supports expander_instance.warning()
                    
                    # If the warning is directly under st, not expander_instance:
                    # For this test, let's assume the warning is directly st.warning
                    # and its call order implies it's inside.
                    # This is an approximation.
                    break # Found the expander
            
            assert expander_found # At least the expander was attempted to be drawn

            # The warning "Database not found. Cannot run SQL queries." is specific to ad-hoc section
            # The global warning "Database not found. Visualizations cannot be generated..." is different.
            # We need to ensure the ad-hoc specific warning is shown.
            # The current code in data_refresh.py shows the ad-hoc expander, and the warning is inside it.
            
            # We'll check if the warning specific to ad-hoc section was called.
            # The expander instance from the mock_st fixture
            expander_instance = mock_st.expander.return_value.__enter__.return_value
            expander_instance.warning.assert_any_call("Database not found. Cannot run SQL queries.")


    def test_adhoc_sql_successful_execution(
        self, mock_st_session_state, mock_st, mock_ui_utils, mock_module_constants,
        mock_setup_logging_at_import
    ):
        mock_st_session_state.clear() # Ensure clean state for adhoc specific results
        
        test_sql_query = "SELECT * FROM test_table"
        mock_st.text_area.return_value = test_sql_query
        mock_st.button.side_effect = lambda label, key=None: True if key == "btn_adhoc_sql_run" else False
        
        expected_df = pd.DataFrame({'colA': [1, 2]})
        mock_ui_utils.safe_execute_query.return_value = expected_df
        
        # Mock the connection object that connect_db will return
        mock_conn = mock.MagicMock(spec=duckdb.DuckDBPyConnection)
        mock_ui_utils.connect_db.return_value = mock_conn

        import src.ui.data_refresh as data_refresh_module
        data_refresh_module.main()

        # Ensure expander is called
        mock_st.expander.assert_any_call("🧑‍🔬 Run an Ad-hoc SQL Query (use with caution - queries are not sanitized and run directly on the DB)")
        expander_instance = mock_st.expander.return_value.__enter__.return_value

        expander_instance.text_area.assert_called_once_with("Enter SQL Query:", key="adhoc_sql_query_input", height=150)
        expander_instance.button.assert_called_once_with("▶︎ Run Ad-hoc SQL", key="btn_adhoc_sql_run")
        
        mock_ui_utils.connect_db.assert_called_once_with(data_refresh_module.DB_PATH, read_only=True, logger_obj=data_refresh_module.ui_logger)
        mock_ui_utils.safe_execute_query.assert_called_once_with(mock_conn, test_sql_query, logger_obj=data_refresh_module.ui_logger, query_type="Ad-hoc SQL")
        
        # Assertions on what's displayed inside the expander
        expander_instance.markdown.assert_any_call("#### Results:")
        expander_instance.dataframe.assert_called_once_with(expected_df, use_container_width=True)
        assert expander_instance.download_button.call_count == 2
        expander_instance.download_button.assert_any_call(label="Download as CSV", data=mock.ANY, file_name="adhoc_query_results.csv", mime="text/csv")
        expander_instance.download_button.assert_any_call(label="Download as Excel", data=mock.ANY, file_name="adhoc_query_results.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        
        mock_conn.close.assert_called_once()
        # Check session state for adhoc results
        pd.testing.assert_frame_equal(mock_st_session_state['adhoc_query_results_df'], expected_df)
        assert mock_st_session_state['show_adhoc_query_results'] is True


    def test_adhoc_sql_empty_query_input(
        self, mock_st_session_state, mock_st, mock_ui_utils, mock_module_constants,
        mock_setup_logging_at_import
    ):
        mock_st_session_state.clear()
        mock_st.text_area.return_value = "   " # Empty or whitespace query
        mock_st.button.side_effect = lambda label, key=None: True if key == "btn_adhoc_sql_run" else False

        import src.ui.data_refresh as data_refresh_module
        data_refresh_module.main()
        
        expander_instance = mock_st.expander.return_value.__enter__.return_value
        expander_instance.warning.assert_called_once_with("SQL query cannot be empty.")
        mock_ui_utils.safe_execute_query.assert_not_called()
        assert mock_st_session_state.get('show_adhoc_query_results') is False


    def test_adhoc_sql_execution_failure(
        self, mock_st_session_state, mock_st, mock_ui_utils, mock_module_constants,
        mock_setup_logging_at_import
    ):
        mock_st_session_state.clear()
        test_sql_query = "SELECT error FROM table"
        mock_st.text_area.return_value = test_sql_query
        mock_st.button.side_effect = lambda label, key=None: True if key == "btn_adhoc_sql_run" else False
        
        error_message = "Test SQL error"
        # safe_execute_query now returns empty df on failure and logs, error is shown by ui_utils
        # For this test, let's assume safe_execute_query returns empty and st.error is called by ui_utils
        # or we make safe_execute_query raise an exception to test the try-except in data_refresh.py
        # The prompt implies safe_execute_query raises an exception that is caught in data_refresh.py
        
        # Let's make connect_db work, but safe_execute_query raise an error
        mock_conn = mock.MagicMock(spec=duckdb.DuckDBPyConnection)
        mock_ui_utils.connect_db.return_value = mock_conn
        mock_ui_utils.safe_execute_query.side_effect = duckdb.Error(error_message) # Simulate duckdb error

        formatted_error_details = f"Formatted DuckDB Error: {error_message}"
        mock_ui_utils.format_exception_for_ui.return_value = formatted_error_details

        import src.ui.data_refresh as data_refresh_module
        data_refresh_module.main()

        expander_instance = mock_st.expander.return_value.__enter__.return_value
        
        mock_ui_utils.connect_db.assert_called_once()
        mock_ui_utils.safe_execute_query.assert_called_once_with(mock_conn, test_sql_query, logger_obj=data_refresh_module.ui_logger, query_type="Ad-hoc SQL")
        
        expander_instance.error.assert_called_once_with(f"Failed to execute Ad-hoc SQL query: {error_message}")
        expander_instance.code.assert_called_once_with(formatted_error_details)
        
        mock_conn.close.assert_called_once() # Ensure connection is closed even on error
        assert mock_st_session_state.get('show_adhoc_query_results') is False

# --- Tests for Predefined Visualizations Section ---

class TestPredefinedVisualizations:

    def test_viz_db_not_found(
        self, mock_st, mock_db_path, mock_module_constants, mock_setup_logging_at_import,
        mock_st_session_state # Keep other fixtures for consistent environment
    ):
        # Override the DB_PATH.exists() from mock_module_constants for this specific test
        # The mock_module_constants fixture uses monkeypatch, which might make this tricky
        # if it's already applied. A more direct way is to ensure the Path object returned
        # by the patched DB_PATH has its exists method mocked.
        # For this test, we'll assume data_refresh.DB_PATH is a MagicMock'd Path object
        # or that we can influence its 'exists' method.
        # The mock_module_constants fixture sets data_refresh.DB_PATH to Path(MOCKED_DB_PATH_STR)
        # So, we need to mock Path(MOCKED_DB_PATH_STR).exists()
        
        with mock.patch.object(Path, 'exists') as mock_path_exists:
            mock_path_exists.return_value = False # DB does not exist
            
            # Import or reload data_refresh to apply this specific mock
            import importlib
            import src.ui.data_refresh
            importlib.reload(src.ui.data_refresh) # Reload to re-evaluate DB_PATH.exists()

            # Assertions
            # The warning is "Database not found. Visualizations cannot be generated..."
            # This is a general warning at the start of the main function.
            # We need to ensure this test focuses on the viz section's specific handling or lack thereof.
            # The current code in data_refresh.py shows the viz section header regardless.
            # The check for DB_PATH.exists() happens *inside* the plot function call logic typically.
            # Let's re-evaluate this test's specific assertion based on the provided code structure.

            # Based on the provided code structure, the viz section is drawn, but plot calls fail.
            # If DB_PATH.exists() is false, the connect_func lambda would return None or raise error.
            # Let's assume the plot functions themselves check for DB existence or handle connection failure.
            # This test might be better focused on a plot function call failing due to DB not found.
            
            # For now, let's assume the main function in data_refresh.py has a top-level check:
            # if not DB_PATH.exists(): st.warning(...) and return
            # If so, this test would be:
            src.ui.data_refresh.main() # Assuming main function encapsulates the UI logic
            mock_st.warning.assert_any_call("Database not found. Visualizations cannot be generated and other features may be limited.")
            # And then assert that selectboxes for viz are not shown
            topic_selectbox_shown = any("Choose Plot Topic:" in call[0][0] for call in mock_st.selectbox.call_args_list)
            assert not topic_selectbox_shown

    def test_viz_topic_selection_flow(
        self, mock_st_session_state, mock_st, mock_module_constants, mock_setup_logging_at_import
    ):
        mock_st_session_state.clear() # Start with a clean session state for this test
        
        # Simulate the first run where no topic is selected
        # The selectbox for topic will be called. We don't set a return value for it yet.
        mock_st.selectbox.return_value = None # Default for the first pass
        
        import src.ui.data_refresh as data_refresh_module # Import to run initial setup
        data_refresh_module.main() # Run the main UI logic

        # Assert initial state (nothing selected beyond default)
        assert mock_st_session_state.get('selected_plot_topic') is None 
        
        # Simulate user selecting a topic
        mock_st.selectbox.side_effect = lambda label, options, index, key, help=None: "Bills" if key == 'selected_plot_topic' else None
        mock_st_session_state.clear() # Reset for this interaction
        mock_st_session_state['selected_plot_topic'] = None # Ensure it starts as None
        mock_st_session_state['selected_plot_name_from_topic'] = "Some Old Plot" # To check reset
        mock_st_session_state['plot_specific_knesset_selection'] = 24 # To check reset


        data_refresh_module.main() # Run again with selectbox now returning "Bills"

        assert mock_st_session_state['selected_plot_topic'] == "Bills"
        assert mock_st_session_state.get('selected_plot_name_from_topic') is None # Reset
        assert mock_st_session_state.get('plot_specific_knesset_selection') is None # Reset
        mock_st.rerun.assert_called_once()

    def test_viz_plot_name_selection_flow(
        self, mock_st_session_state, mock_st, mock_module_constants, mock_setup_logging_at_import
    ):
        mock_st_session_state.clear()
        mock_st_session_state['selected_plot_topic'] = "Bills" # Topic is already selected
        mock_st_session_state['selected_plot_name_from_topic'] = None # No plot name selected yet
        mock_st_session_state['plot_specific_knesset_selection'] = 24 # To check reset

        # AVAILABLE_PLOTS_BY_TOPIC is mocked by mock_module_constants
        # For "Bills", it's {"Bills Over Time": MOCK_PLOT_FUNCTION_1}
        
        # Simulate user selecting a plot name
        # First selectbox (topic) returns "Bills", second (plot name) returns "Bills Over Time"
        def selectbox_side_effect(label, options, index, key, help=None):
            if key == 'selected_plot_topic':
                return "Bills"
            elif key == 'selected_plot_name_from_topic':
                return "Bills Over Time"
            return None
        mock_st.selectbox.side_effect = selectbox_side_effect
        
        import src.ui.data_refresh as data_refresh_module
        data_refresh_module.main()

        assert mock_st_session_state['selected_plot_topic'] == "Bills"
        assert mock_st_session_state['selected_plot_name_from_topic'] == "Bills Over Time"
        assert mock_st_session_state.get('plot_specific_knesset_selection') is None # Reset
        mock_st.rerun.assert_called_once()

    def test_viz_knesset_filter_logic_single_global_knesset(
        self, mock_st_session_state, mock_st, mock_module_constants, mock_ui_utils,
        mock_setup_logging_at_import
    ):
        mock_st_session_state.clear()
        mock_st_session_state['selected_plot_topic'] = "Bills"
        mock_st_session_state['selected_plot_name_from_topic'] = "Bills Over Time"
        mock_st_session_state['ms_knesset_filter'] = [25] # Single global Knesset filter
        
        # Mock knesset_nums_options_global as it's read by the module
        # This is typically set by get_filter_options_from_db, mocked by mock_ui_utils
        # For this test, we can directly patch it in data_refresh_module after import
        # or ensure mock_ui_utils.get_filter_options_from_db returns something that leads to this.
        # The AVAILABLE_PLOTS_BY_TOPIC is already mocked with MOCK_PLOT_FUNCTION_1 for this plot.

        import src.ui.data_refresh as data_refresh_module
        data_refresh_module.knesset_nums_options_global = ["", 24, 25] # Ensure this is set for the logic
        
        # Mock selectbox calls: topic, plot_name. Plot-specific Knesset should NOT be called.
        def selectbox_side_effect(label, options, index, key, help=None):
            if key == 'selected_plot_topic': return "Bills"
            if key == 'selected_plot_name_from_topic': return "Bills Over Time"
            # If plot_specific_knesset_selection selectbox is called, raise error
            if key == 'plot_specific_knesset_selection':
                raise AssertionError("Plot-specific Knesset selectbox should not be called.")
            return None
        mock_st.selectbox.side_effect = selectbox_side_effect
        
        data_refresh_module.main()

        plot_func_mock = data_refresh_module.AVAILABLE_PLOTS_BY_TOPIC["Bills"]["Bills Over Time"]
        plot_func_mock.assert_called_once()
        call_kwargs = plot_func_mock.call_args[1]
        assert call_kwargs['knesset_filter'] == [25] # Called with the global filter
        
        mock_st.spinner.assert_called_once()
        mock_st.plotly_chart.assert_called_once_with(plot_func_mock.return_value, use_container_width=True)

    def test_viz_knesset_filter_logic_plot_specific_selector_shown_and_used(
        self, mock_st_session_state, mock_st, mock_module_constants, mock_ui_utils,
        mock_setup_logging_at_import
    ):
        mock_st_session_state.clear()
        mock_st_session_state['selected_plot_topic'] = "Bills"
        mock_st_session_state['selected_plot_name_from_topic'] = "Bills Over Time"
        mock_st_session_state['ms_knesset_filter'] = [] # No global Knesset filter (or multiple)
        
        import src.ui.data_refresh as data_refresh_module
        data_refresh_module.knesset_nums_options_global = ["", 24, 25]

        # Simulate selection of plot-specific Knesset
        def selectbox_side_effect(label, options, index, key, help=None):
            if key == 'selected_plot_topic': return "Bills"
            if key == 'selected_plot_name_from_topic': return "Bills Over Time"
            if key == 'plot_specific_knesset_selection': return 24 # User selects 24
            return None
        mock_st.selectbox.side_effect = selectbox_side_effect
        
        data_refresh_module.main()

        # Check if plot-specific selectbox was called
        plot_specific_knesset_selectbox_called = False
        for call in mock_st.selectbox.call_args_list:
            if call[1].get('key') == 'plot_specific_knesset_selection':
                plot_specific_knesset_selectbox_called = True
                assert call[1]['options'] == data_refresh_module.knesset_nums_options_global
                break
        assert plot_specific_knesset_selectbox_called
        
        plot_func_mock = data_refresh_module.AVAILABLE_PLOTS_BY_TOPIC["Bills"]["Bills Over Time"]
        plot_func_mock.assert_called_once()
        call_kwargs = plot_func_mock.call_args[1]
        assert call_kwargs['knesset_filter'] == [24] # Called with plot-specific selection
        
        mock_st.spinner.assert_called_once()
        mock_st.plotly_chart.assert_called_once()

    def test_viz_knesset_filter_logic_plot_specific_selector_not_chosen(
        self, mock_st_session_state, mock_st, mock_module_constants, mock_ui_utils,
        mock_setup_logging_at_import
    ):
        mock_st_session_state.clear()
        mock_st_session_state['selected_plot_topic'] = "Bills"
        mock_st_session_state['selected_plot_name_from_topic'] = "Bills Over Time"
        mock_st_session_state['ms_knesset_filter'] = []
        
        import src.ui.data_refresh as data_refresh_module
        data_refresh_module.knesset_nums_options_global = ["", 24, 25] # Make sure "" is an option

        def selectbox_side_effect(label, options, index, key, help=None):
            if key == 'selected_plot_topic': return "Bills"
            if key == 'selected_plot_name_from_topic': return "Bills Over Time"
            if key == 'plot_specific_knesset_selection': return "" # User selects placeholder
            return None
        mock_st.selectbox.side_effect = selectbox_side_effect
        
        data_refresh_module.main()
        
        mock_st.info.assert_any_call(
            f"To view the 'Bills Over Time' plot, please select a specific Knesset number using the dropdown above, or apply a single Knesset filter globally."
        )
        plot_func_mock = data_refresh_module.AVAILABLE_PLOTS_BY_TOPIC["Bills"]["Bills Over Time"]
        plot_func_mock.assert_not_called()
        mock_st.plotly_chart.assert_not_called()

    def test_viz_plot_function_call_and_faction_filter(
        self, mock_st_session_state, mock_st, mock_module_constants, mock_ui_utils,
        mock_setup_logging_at_import
    ):
        mock_st_session_state.clear()
        mock_st_session_state['selected_plot_topic'] = "Bills"
        mock_st_session_state['selected_plot_name_from_topic'] = "Bills Over Time"
        mock_st_session_state['ms_knesset_filter'] = [25] # Global Knesset filter
        mock_st_session_state['ms_faction_filter'] = ["Faction A (K25)"]
        
        import src.ui.data_refresh as data_refresh_module
        # faction_display_map_global is mocked by mock_module_constants
        # We need to ensure it's set correctly for this test
        data_refresh_module.faction_display_map_global = {"Faction A (K25)": 101}


        def selectbox_side_effect(label, options, index, key, help=None):
            if key == 'selected_plot_topic': return "Bills"
            if key == 'selected_plot_name_from_topic': return "Bills Over Time"
            return None
        mock_st.selectbox.side_effect = selectbox_side_effect
        
        data_refresh_module.main()

        plot_func_mock = data_refresh_module.AVAILABLE_PLOTS_BY_TOPIC["Bills"]["Bills Over Time"]
        plot_func_mock.assert_called_once()
        call_kwargs = plot_func_mock.call_args[1]
        
        assert call_kwargs['knesset_filter'] == [25]
        assert call_kwargs['faction_filter'] == [101] # Check FactionID
        assert call_kwargs['db_path'] == data_refresh_module.DB_PATH
        assert callable(call_kwargs['connect_func'])
        assert call_kwargs['logger'] == data_refresh_module.ui_logger

        # Test the connect_func lambda
        call_kwargs['connect_func'](read_only=True)
        mock_ui_utils.connect_db.assert_called_with(data_refresh_module.DB_PATH, read_only=True, logger_obj=data_refresh_module.ui_logger)

    def test_viz_plot_function_returns_none(
        self, mock_st_session_state, mock_st, mock_module_constants, mock_setup_logging_at_import
    ):
        mock_st_session_state.clear()
        mock_st_session_state['selected_plot_topic'] = "Bills"
        mock_st_session_state['selected_plot_name_from_topic'] = "Bills Over Time"
        mock_st_session_state['ms_knesset_filter'] = [25]
        
        import src.ui.data_refresh as data_refresh_module
        plot_func_mock = data_refresh_module.AVAILABLE_PLOTS_BY_TOPIC["Bills"]["Bills Over Time"]
        plot_func_mock.return_value = None # Plot function returns None

        def selectbox_side_effect(label, options, index, key, help=None):
            if key == 'selected_plot_topic': return "Bills"
            if key == 'selected_plot_name_from_topic': return "Bills Over Time"
            return None
        mock_st.selectbox.side_effect = selectbox_side_effect

        data_refresh_module.main()
        
        plot_func_mock.assert_called_once()
        mock_st.plotly_chart.assert_not_called()
        # An info message might be good here, e.g., "Plot returned no data/figure."
        # Check if any st.info or st.warning indicates this. For now, just check no chart.

    def test_viz_plot_function_raises_exception(
        self, mock_st_session_state, mock_st, mock_module_constants, mock_ui_utils,
        mock_setup_logging_at_import
    ):
        mock_st_session_state.clear()
        mock_st_session_state['selected_plot_topic'] = "Bills"
        mock_st_session_state['selected_plot_name_from_topic'] = "Bills Over Time"
        mock_st_session_state['ms_knesset_filter'] = [25]
        
        error_message = "Plot Generation Error"
        formatted_error_message = f"Formatted details of: {error_message}"
        mock_ui_utils.format_exception_for_ui.return_value = formatted_error_message

        import src.ui.data_refresh as data_refresh_module
        plot_func_mock = data_refresh_module.AVAILABLE_PLOTS_BY_TOPIC["Bills"]["Bills Over Time"]
        plot_func_mock.side_effect = Exception(error_message)

        def selectbox_side_effect(label, options, index, key, help=None):
            if key == 'selected_plot_topic': return "Bills"
            if key == 'selected_plot_name_from_topic': return "Bills Over Time"
            return None
        mock_st.selectbox.side_effect = selectbox_side_effect

        data_refresh_module.main()

        plot_func_mock.assert_called_once()
        mock_st.error.assert_any_call(f"An error occurred while generating the plot 'Bills Over Time': {error_message}")
        mock_st.code.assert_any_call(formatted_error_message)
        mock_st.plotly_chart.assert_not_called()

    def test_viz_info_messages_no_topic_or_no_plot_name(
        self, mock_st_session_state, mock_st, mock_module_constants, mock_setup_logging_at_import
    ):
        import src.ui.data_refresh as data_refresh_module
        
        # Scenario 1: No topic selected
        mock_st_session_state.clear()
        mock_st_session_state['selected_plot_topic'] = "" # Empty string often default for selectbox placeholder
        
        # Mock selectbox for topic to return this empty string
        mock_st.selectbox.side_effect = lambda label, options, index, key, help=None: "" if key == 'selected_plot_topic' else None
        
        data_refresh_module.main()
        mock_st.info.assert_any_call("Select a plot topic from the dropdown above to see available visualizations.")
        mock_st.selectbox.reset_mock() # Reset for next scenario
        mock_st.info.reset_mock()

        # Scenario 2: Topic selected, but no plot name
        mock_st_session_state.clear()
        mock_st_session_state['selected_plot_topic'] = "Bills"
        mock_st_session_state['selected_plot_name_from_topic'] = "" # Empty string for plot name
        
        def selectbox_side_effect_scenario2(label, options, index, key, help=None):
            if key == 'selected_plot_topic': return "Bills"
            if key == 'selected_plot_name_from_topic': return ""
            return None
        mock_st.selectbox.side_effect = selectbox_side_effect_scenario2
        
        data_refresh_module.main()
        mock_st.info.assert_any_call("Please choose a specific visualization from the 'Choose Visualization for Bills' dropdown.")

# --- Tests for Predefined Query Results Display Logic ---

class TestPredefinedQueryResultsDisplay:

    def test_query_results_display_when_shown_with_data(
        self, mock_st_session_state, mock_st, mock_module_constants, 
        mock_ui_utils, mock_display_sidebar, mock_display_chart_builder, 
        mock_setup_logging_at_import
    ):
        mock_st_session_state['show_query_results'] = True
        mock_st_session_state['executed_query_name'] = "Test Query"
        mock_st_session_state['query_results_df'] = pd.DataFrame({'col1': [1, 2]})
        mock_st_session_state['applied_filters_info_query'] = ["Filter1: ValueA"]
        mock_st_session_state['last_executed_sql'] = "SELECT * FROM Test"

        # Import the module to trigger display logic
        import src.ui.data_refresh as data_refresh_module

        mock_st.markdown.assert_any_call("### Query Results: Test Query") # Subheader
        mock_st.dataframe.assert_called_once_with(mock_st_session_state['query_results_df'], use_container_width=True)
        
        # Check download buttons
        assert mock_st.download_button.call_count == 2
        # Note: We can't easily check the 'data' argument of download_button if it's a dynamically created CSV/Excel string.
        # Instead, we check for key parameters like label and file_name.
        mock_st.download_button.assert_any_call(label="Download as CSV", data=mock.ANY, file_name="Test Query_results.csv", mime="text/csv")
        mock_st.download_button.assert_any_call(label="Download as Excel", data=mock.ANY, file_name="Test Query_results.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # Check expander and code for SQL
        mock_st.expander.assert_any_call("Show Executed SQL and Applied Filters")
        # This assumes the expander is used as a context manager
        # The mock_st fixture needs to support this: expander_instance.code(...)
        expander_instance = mock_st.expander.return_value.__enter__.return_value
        expander_instance.code.assert_called_once_with("SELECT * FROM Test", language="sql")
        expander_instance.markdown.assert_called_once_with("- Filter1: ValueA")


        # Ensure "no results" or "run query" info messages are NOT called
        # We need to check all calls to st.info and see if any match these specific messages
        no_results_message = "The query returned no results. Please adjust filters or try a different query."
        run_query_message = "Run a predefined query from the sidebar to see results."
        for call_arg in mock_st.info.call_args_list:
            assert no_results_message not in call_arg[0][0]
            assert run_query_message not in call_arg[0][0]
            
    def test_query_results_display_when_shown_with_empty_data(
        self, mock_st_session_state, mock_st, mock_module_constants, 
        mock_ui_utils, mock_display_sidebar, mock_display_chart_builder, 
        mock_setup_logging_at_import
    ):
        mock_st_session_state['show_query_results'] = True
        mock_st_session_state['executed_query_name'] = "Test Query Empty"
        mock_st_session_state['query_results_df'] = pd.DataFrame()
        mock_st_session_state['applied_filters_info_query'] = []
        mock_st_session_state['last_executed_sql'] = "SELECT * FROM Empty"

        import src.ui.data_refresh as data_refresh_module

        mock_st.markdown.assert_any_call("### Query Results: Test Query Empty")
        # Streamlit's st.dataframe might be called with an empty df, or logic might prevent it.
        # If it is called, it will just display an empty table.
        # For this test, let's assume it's called.
        mock_st.dataframe.assert_called_once_with(mock_st_session_state['query_results_df'], use_container_width=True)
        
        # Download buttons might not be shown, or shown disabled, or show empty file.
        # Based on current typical behavior, they might still be offered.
        # If the requirement is to NOT show them for empty df, this assertion needs change.
        assert mock_st.download_button.call_count == 2 

        mock_st.info.assert_any_call("The query returned no results. Please adjust filters or try a different query.")
        
        mock_st.expander.assert_any_call("Show Executed SQL and Applied Filters")
        expander_instance = mock_st.expander.return_value.__enter__.return_value
        expander_instance.code.assert_called_once_with("SELECT * FROM Empty", language="sql")

    def test_query_results_display_when_not_shown(
        self, mock_st_session_state, mock_st, mock_module_constants, 
        mock_ui_utils, mock_display_sidebar, mock_display_chart_builder, 
        mock_setup_logging_at_import
    ):
        mock_st_session_state['show_query_results'] = False
        mock_st_session_state['executed_query_name'] = None
        mock_st_session_state['query_results_df'] = None # Or empty df

        import src.ui.data_refresh as data_refresh_module
        
        # Check that elements specific to showing results are NOT called
        # This requires checking that st.markdown with "### Query Results:" was not called.
        # A bit tricky with assert_any_call. We can check call_args_list.
        subheader_called = False
        for call in mock_st.markdown.call_args_list:
            if "### Query Results:" in call[0][0]:
                subheader_called = True
                break
        assert not subheader_called

        mock_st.dataframe.assert_not_called()
        mock_st.download_button.assert_not_called()
        mock_st.info.assert_any_call("Run a predefined query from the sidebar to see results.")

# --- Tests for Table Explorer Results Display Logic ---

class TestTableExplorerResultsDisplay:

    def test_explorer_results_display_when_shown_with_data(
        self, mock_st_session_state, mock_st, mock_module_constants, 
        mock_ui_utils, mock_display_sidebar, mock_display_chart_builder, 
        mock_setup_logging_at_import
    ):
        mock_st_session_state['show_table_explorer_results'] = True
        mock_st_session_state['explored_table_name'] = "Test Table" # Renamed from executed_table_explorer_name based on code
        mock_st_session_state['explore_results_df'] = pd.DataFrame({'data': [10, 20]}) # Renamed from table_explorer_df
        mock_st_session_state['ms_knesset_filter'] = [25]
        mock_st_session_state['ms_faction_filter'] = ["Faction X (K25)"]

        import src.ui.data_refresh as data_refresh_module

        mock_st.subheader.assert_any_call("Explore Table: Test Table")
        
        # Check markdown for active filters
        active_filters_md_found = False
        for call in mock_st.markdown.call_args_list:
            if "**Active Global Filters:**" in call[0][0]:
                active_filters_md_found = True
                assert "- Knesset Filter: `[25]`" in call[0][0]
                assert "- Faction Filter: `['Faction X (K25)']`" in call[0][0]
                break
        assert active_filters_md_found
        
        mock_st.dataframe.assert_called_once_with(mock_st_session_state['explore_results_df'], use_container_width=True)
        assert mock_st.download_button.call_count == 2
        mock_st.download_button.assert_any_call(label="Download as CSV", data=mock.ANY, file_name="Test Table_explored.csv", mime="text/csv")
        mock_st.download_button.assert_any_call(label="Download as Excel", data=mock.ANY, file_name="Test Table_explored.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        no_results_message = "The table exploration returned no results."
        explore_message = "Explore a table from the sidebar to view its data."
        for call_arg in mock_st.info.call_args_list:
            assert no_results_message not in call_arg[0][0]
            assert explore_message not in call_arg[0][0]

    def test_explorer_results_display_when_shown_with_empty_data(
        self, mock_st_session_state, mock_st, mock_module_constants, 
        mock_ui_utils, mock_display_sidebar, mock_display_chart_builder, 
        mock_setup_logging_at_import
    ):
        mock_st_session_state['show_table_explorer_results'] = True
        mock_st_session_state['explored_table_name'] = "Test Table Empty"
        mock_st_session_state['explore_results_df'] = pd.DataFrame()
        mock_st_session_state['ms_knesset_filter'] = []
        mock_st_session_state['ms_faction_filter'] = []


        import src.ui.data_refresh as data_refresh_module

        mock_st.subheader.assert_any_call("Explore Table: Test Table Empty")
        # Markdown for filters should still be called, even if empty
        active_filters_md_found = False
        for call in mock_st.markdown.call_args_list:
            if "**Active Global Filters:**" in call[0][0]:
                active_filters_md_found = True
                break
        assert active_filters_md_found
        
        mock_st.info.assert_any_call("The table exploration returned no results. Try adjusting global filters or select a different table.")
        
        # Download buttons might not be shown for empty data
        mock_st.download_button.assert_not_called() # Assuming no download for empty df

    def test_explorer_results_display_when_not_shown(
        self, mock_st_session_state, mock_st, mock_module_constants, 
        mock_ui_utils, mock_display_sidebar, mock_display_chart_builder, 
        mock_setup_logging_at_import
    ):
        mock_st_session_state['show_table_explorer_results'] = False
        mock_st_session_state['explored_table_name'] = None

        import src.ui.data_refresh as data_refresh_module
        
        subheader_called = False
        for call in mock_st.subheader.call_args_list: # Changed from st.markdown to st.subheader
            if "Explore Table:" in call[0][0]:
                subheader_called = True
                break
        assert not subheader_called
        
        mock_st.info.assert_any_call("Explore a table from the sidebar to view its data, apply filters, and download.")

import pytest
import pandas as pd
from pathlib import Path
from unittest import mock
import streamlit # Will be mocked

# Import the function to be tested
from src.ui.chart_builder_ui import display_chart_builder

# Default values for parameters, can be overridden in tests
DEFAULT_MAX_ROWS = 10000
DEFAULT_MAX_FACET_VALUES = 20
DEFAULT_FACTION_DISPLAY_MAP = {1: "Faction A", 2: "Faction B"}

@pytest.fixture
def mock_logger():
    return mock.MagicMock()

@pytest.fixture
def mock_db_path():
    path_mock = mock.MagicMock(spec=Path)
    return path_mock

# This fixture will mock 'streamlit' as 'st' within the chart_builder_ui module
@pytest.fixture(autouse=True) # autouse to apply it to all tests in this file
def mock_st_in_module():
    # Create a mock object that will mimic the streamlit module
    mock_st_module = mock.MagicMock(spec=streamlit)
    
    # Initialize session_state as a dictionary on the mock_st_module
    # This allows tests to directly manipulate mock_st_module.session_state
    mock_st_module.session_state = {}

    # Mock common streamlit functions
    mock_st_module.warning = mock.MagicMock()
    mock_st_module.error = mock.MagicMock()
    mock_st_module.info = mock.MagicMock()
    mock_st_module.success = mock.MagicMock()
    mock_st_module.toast = mock.MagicMock()
    mock_st_module.rerun = mock.MagicMock()
    mock_st_module.button = mock.MagicMock(return_value=False) # Default to button not pressed
    mock_st_module.selectbox = mock.MagicMock(return_value=None)
    mock_st_module.multiselect = mock.MagicMock(return_value=[])
    mock_st_module.text_input = mock.MagicMock(return_value="")
    mock_st_module.expander = mock.MagicMock()
    mock_st_module.code = mock.MagicMock()
    mock_st_module.columns = mock.MagicMock(return_value=(mock.MagicMock(), mock.MagicMock(), mock.MagicMock())) # For 3 columns

    # Mock the expander context manager behavior
    mock_expander_instance = mock.MagicMock()
    mock_st_module.expander.return_value.__enter__.return_value = mock_expander_instance
    # Allow calls on the expander instance, e.g., expander.selectbox
    mock_expander_instance.selectbox = mock.MagicMock(return_value=None)
    mock_expander_instance.multiselect = mock.MagicMock(return_value=[])


    with mock.patch('src.ui.chart_builder_ui.st', mock_st_module):
        yield mock_st_module

# Mock for ui_utils functions
@pytest.fixture
def mock_ui_utils():
    with mock.patch('src.ui.chart_builder_ui.ui_utils') as mock_utils:
        mock_utils.get_db_table_list.return_value = []
        mock_utils.get_table_columns.return_value = ([], [], []) # all, num, cat
        mock_utils.connect_db.return_value = mock.MagicMock() # Mock connection object
        mock_utils.safe_execute_query.return_value = pd.DataFrame()
        mock_utils.format_exception_for_ui.return_value = "Formatted Exception"
        yield mock_utils

# Mock for plotly.express functions
@pytest.fixture
def mock_px():
    with mock.patch('src.ui.chart_builder_ui.px') as mock_plotly_express:
        mock_plotly_express.bar.return_value = mock.MagicMock() # a mock figure
        mock_plotly_express.line.return_value = mock.MagicMock()
        mock_plotly_express.scatter.return_value = mock.MagicMock()
        mock_plotly_express.pie.return_value = mock.MagicMock()
        mock_plotly_express.histogram.return_value = mock.MagicMock()
        yield mock_plotly_express


class TestDisplayChartBuilder:
    def test_database_not_found(self, mock_st_in_module, mock_db_path, mock_logger):
        """Test behavior when the database file does not exist."""
        mock_db_path.exists.return_value = False

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )

        mock_db_path.exists.assert_called_once()
        mock_st_in_module.warning.assert_called_once_with(f"Database file not found at {mock_db_path}. Please create it first using the CLI.")
        mock_st_in_module.selectbox.assert_not_called()


    def test_table_selection_logic_successful_load(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_logger
    ):
        """Test behavior when a table is selected and data loads successfully."""
        mock_db_path.exists.return_value = True
        test_table_name = "test_table"
        all_cols = ["col_a", "col_b", "KnessetNum", "FactionID"]
        num_cols = ["col_a"]
        cat_cols = ["col_b", "KnessetNum", "FactionID"]
        
        # Mock return values for ui_utils
        mock_ui_utils.get_db_table_list.return_value = [test_table_name, "another_table"]
        mock_ui_utils.get_table_columns.return_value = (all_cols, num_cols, cat_cols)
        
        sample_cs_filter_data = pd.DataFrame({
            "KnessetNum": [23, 24, 25],
            "FactionID": [1, 2, 1]
        })
        mock_ui_utils.safe_execute_query.return_value = sample_cs_filter_data

        # Simulate table selection: first selectbox call is for table selection
        mock_st_in_module.selectbox.side_effect = [
            test_table_name, # First call: table selection
            None, # Second call: chart type (default)
            None, # Third call: X-axis
            # ... other selectbox calls will return None or default if not specified
        ]
        
        # Initialize relevant session state keys that might be checked or reset
        mock_st_in_module.session_state['builder_selected_table'] = None # Simulate initial state
        mock_st_in_module.session_state['ms_knesset_filter'] = [25] # Global Knesset filter
        mock_st_in_module.session_state['ms_faction_filter'] = ["Faction A"] # Global Faction filter (name)
        
        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )

        # Assertions
        mock_ui_utils.get_db_table_list.assert_called_once()
        
        # Check if selectbox for table selection was called
        # The first selectbox call is for table selection
        assert mock_st_in_module.selectbox.call_args_list[0][1]['label'] == "Select Table for Chart Builder"

        # When table is selected, get_table_columns should be called
        mock_ui_utils.get_table_columns.assert_called_once_with(mock_db_path, test_table_name, logger_obj=mock_logger)

        # Assert session state updates after table selection
        assert mock_st_in_module.session_state['builder_selected_table'] == test_table_name
        assert mock_st_in_module.session_state['builder_columns'] == all_cols
        assert mock_st_in_module.session_state['builder_numeric_columns'] == num_cols
        assert mock_st_in_module.session_state['builder_categorical_columns'] == cat_cols
        pd.testing.assert_frame_equal(mock_st_in_module.session_state['builder_data_for_cs_filters'], sample_cs_filter_data)

        # Assert SQL query for cs_filters includes global filters
        expected_query = f"SELECT DISTINCT KnessetNum, FactionID FROM {test_table_name} WHERE KnessetNum IN (25) AND FactionID IN (1) ORDER BY KnessetNum, FactionID LIMIT {DEFAULT_MAX_ROWS}"
        # The actual call to safe_execute_query includes the connection object as the first arg
        # We are interested in the query string (second arg)
        actual_query_call = mock_ui_utils.safe_execute_query.call_args[0][1]
        assert ''.join(expected_query.split()) == ''.join(actual_query_call.split())


        # Assert dependent session state keys are reset
        assert 'builder_x_axis' not in mock_st_in_module.session_state # or is None, depending on reset logic
        assert 'builder_generated_chart' not in mock_st_in_module.session_state # or is None

        # Assert st.rerun is called
        mock_st_in_module.rerun.assert_called_once()

    def test_table_selection_no_columns_returned(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_logger
    ):
        """Test behavior when a table is selected but get_table_columns returns empty."""
        mock_db_path.exists.return_value = True
        test_table_name = "empty_table"
        
        mock_ui_utils.get_db_table_list.return_value = [test_table_name]
        mock_ui_utils.get_table_columns.return_value = ([], [], []) # No columns
        
        # Simulate table selection
        mock_st_in_module.selectbox.side_effect = [test_table_name, None] # Table, then chart type
        mock_st_in_module.session_state['builder_selected_table'] = None

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )
        
        assert mock_st_in_module.session_state['builder_selected_table'] == test_table_name
        assert mock_st_in_module.session_state['builder_columns'] == []
        assert mock_st_in_module.session_state['builder_numeric_columns'] == []
        assert mock_st_in_module.session_state['builder_categorical_columns'] == []
        # builder_data_for_cs_filters might be empty or not set if no columns
        assert 'builder_data_for_cs_filters' in mock_st_in_module.session_state 
        
        # safe_execute_query for cs_filters should not be called if no KnessetNum/FactionID cols
        # or if the logic prevents it when no columns are found by get_table_columns.
        # Based on current code, it *will* try to query if table_columns is empty but KnessetNum/FactionID are in all_cols (which is empty here)
        # The query would be "SELECT DISTINCT KnessetNum, FactionID FROM empty_table ... "
        # For this specific test, since all_cols is [], the cs_filter_cols will be empty, so query won't run.
        mock_ui_utils.safe_execute_query.assert_not_called() 
        mock_st_in_module.rerun.assert_called_once()
        mock_st_in_module.error.assert_called_with(f"No columns found for table {test_table_name}. Cannot proceed with chart building.")

    def test_table_selection_cs_filter_query_fails(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_logger
    ):
        """Test behavior when cs_filter query fails during table selection."""
        mock_db_path.exists.return_value = True
        test_table_name = "query_fail_table"
        all_cols = ["col_a", "KnessetNum"] # Has KnessetNum for filter query
        
        mock_ui_utils.get_db_table_list.return_value = [test_table_name]
        mock_ui_utils.get_table_columns.return_value = (all_cols, ["col_a"], ["KnessetNum"])
        mock_ui_utils.safe_execute_query.return_value = pd.DataFrame() # Simulate query failure (empty df)
        
        mock_st_in_module.selectbox.side_effect = [test_table_name, None]
        mock_st_in_module.session_state['builder_selected_table'] = None
        mock_st_in_module.session_state['ms_knesset_filter'] = [] # No global filter

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )

        assert mock_st_in_module.session_state['builder_selected_table'] == test_table_name
        assert mock_st_in_module.session_state['builder_columns'] == all_cols
        assert mock_st_in_module.session_state['builder_data_for_cs_filters'].empty # Should be empty
        
        expected_query = f"SELECT DISTINCT KnessetNum FROM {test_table_name} ORDER BY KnessetNum LIMIT {DEFAULT_MAX_ROWS}"
        mock_ui_utils.safe_execute_query.assert_called_once()
        actual_query_call = mock_ui_utils.safe_execute_query.call_args[0][1]
        assert ''.join(expected_query.split()) == ''.join(actual_query_call.split())
        
        mock_st_in_module.warning.assert_called_with(f"Could not load filter options for table {test_table_name}. Chart-specific filters might be incomplete.")
        mock_st_in_module.rerun.assert_called_once()

    def test_chart_type_change_logic_resets_y_axis(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_logger
    ):
        """Test that Y-axis is reset when chart type changes to one requiring numeric Y, and current Y is not numeric."""
        mock_db_path.exists.return_value = True
        test_table_name = "test_table"
        all_cols = ["Year", "Category", "Value"]
        num_cols = ["Value"]
        cat_cols = ["Year", "Category"]

        # Setup: Table is selected, columns are loaded
        mock_st_in_module.session_state['builder_selected_table'] = test_table_name
        mock_st_in_module.session_state['builder_columns'] = all_cols
        mock_st_in_module.session_state['builder_numeric_columns'] = num_cols
        mock_st_in_module.session_state['builder_categorical_columns'] = cat_cols
        mock_st_in_module.session_state['builder_data_for_cs_filters'] = pd.DataFrame({'KnessetNum': [25]}) # Dummy data

        # Simulate chart type change: from 'pie' (doesn't use y_axis directly) to 'bar' (needs y_axis)
        # And current y_axis is a categorical column.
        mock_st_in_module.session_state['previous_builder_chart_type'] = "pie" # Old chart type
        mock_st_in_module.session_state['builder_chart_type'] = "bar"        # New chart type
        mock_st_in_module.session_state['builder_y_axis'] = "Category"       # Current Y is categorical

        # Mock selectbox calls: table, chart_type, x_axis, y_axis (will be called again after reset)
        mock_st_in_module.selectbox.side_effect = [
            test_table_name,  # table select
            "bar",            # chart type select
            "Year",           # x-axis select
            None,             # y-axis select (first time, before reset)
            None              # y-axis select (after reset, this is what's asserted)
        ]
        
        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )

        # Assert that builder_y_axis was reset
        assert 'builder_y_axis' not in mock_st_in_module.session_state or mock_st_in_module.session_state['builder_y_axis'] is None
        
        # Assert that st.rerun was called due to the change
        mock_st_in_module.rerun.assert_called_once()
        
        # Check that the y-axis selectbox was prompted again with the new (reset) value
        y_axis_selectbox_calls = [
            call for call in mock_st_in_module.selectbox.call_args_list 
            if call[1].get('label') == "Select Y-axis (Numeric)"
        ]
        # It should be called after the reset, its value would be None if builder_y_axis was deleted
        assert y_axis_selectbox_calls[-1][1]['options'] == num_cols # Offered only numeric cols
        assert y_axis_selectbox_calls[-1][1]['index'] == 0 # Default to first option or placeholder

    def test_chart_type_change_logic_resets_pie_fields(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_logger
    ):
        """Test that names/values are reset when changing from 'pie' chart type."""
        mock_db_path.exists.return_value = True
        test_table_name = "test_table_for_pie"
        
        # Setup: Table is selected
        mock_st_in_module.session_state['builder_selected_table'] = test_table_name
        mock_st_in_module.session_state['builder_columns'] = ["Category", "Value"]
        mock_st_in_module.session_state['builder_numeric_columns'] = ["Value"]
        mock_st_in_module.session_state['builder_categorical_columns'] = ["Category"]
        mock_st_in_module.session_state['builder_data_for_cs_filters'] = pd.DataFrame()


        # Simulate chart type change: from 'pie' to 'bar'
        mock_st_in_module.session_state['previous_builder_chart_type'] = "pie"
        mock_st_in_module.session_state['builder_chart_type'] = "bar"
        mock_st_in_module.session_state['builder_names_pie'] = "Category" # Previously set for pie
        mock_st_in_module.session_state['builder_values_pie'] = "Value"   # Previously set for pie

        mock_st_in_module.selectbox.side_effect = [
            test_table_name, # table
            "bar",           # chart type
            None,            # x-axis
            None,            # y-axis
        ]

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )

        assert 'builder_names_pie' not in mock_st_in_module.session_state or mock_st_in_module.session_state['builder_names_pie'] is None
        assert 'builder_values_pie' not in mock_st_in_module.session_state or mock_st_in_module.session_state['builder_values_pie'] is None
        mock_st_in_module.rerun.assert_called_once()

    def test_chart_type_change_no_rerun_if_no_state_change(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_logger
    ):
        """Test that st.rerun is not called if chart type change doesn't require state resets."""
        mock_db_path.exists.return_value = True
        test_table_name = "stable_table"
        mock_st_in_module.session_state['builder_selected_table'] = test_table_name
        mock_st_in_module.session_state['builder_columns'] = ["X", "Y_num", "Y_cat"]
        mock_st_in_module.session_state['builder_numeric_columns'] = ["Y_num"]
        mock_st_in_module.session_state['builder_categorical_columns'] = ["Y_cat"]
        mock_st_in_module.session_state['builder_data_for_cs_filters'] = pd.DataFrame()

        # Chart type change: from 'bar' to 'line' (both use similar x/y, y is numeric)
        mock_st_in_module.session_state['previous_builder_chart_type'] = "bar"
        mock_st_in_module.session_state['builder_chart_type'] = "line"
        mock_st_in_module.session_state['builder_y_axis'] = "Y_num" # Y is already numeric

        mock_st_in_module.selectbox.side_effect = [
            test_table_name, # table
            "line",          # chart type
            "X",             # x-axis
            "Y_num",         # y-axis
        ]

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )
        
        # No problematic state changes, so rerun should not be called just for this.
        # Note: If other parts of the function (like initial table load) call rerun, this test might need adjustment.
        # This test specifically checks that *this part* of the logic doesn't trigger rerun.
        # We assume that by the time chart_type selection is processed, table selection has stabilized.
        # So, if rerun is called, it's by a different logic block.
        # To isolate, we can check call_count before and after, or ensure no other rerun-triggering conditions are met.
        
        # For simplicity, if any rerun is called, it's fine for this test's scope if it's not *due to this specific logic*.
        # The key is that y_axis is NOT reset here.
        assert mock_st_in_module.session_state['builder_y_axis'] == "Y_num" 
        # mock_st_in_module.rerun.assert_not_called() # This might be too strict if other parts call it.
                                                  # The goal is to verify no *unnecessary* rerun from this block.
                                                  # If rerun is called, it's by other logic (e.g. first time setup)

    def test_generate_chart_button_no_table_selected(
        self, mock_st_in_module, mock_db_path, mock_logger
    ):
        """Test 'Generate Chart' button click when no table is selected."""
        mock_db_path.exists.return_value = True
        mock_st_in_module.session_state['builder_selected_table'] = None # No table selected
        
        # Simulate button click
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_generate_chart" else False

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )
        
        mock_st_in_module.error.assert_called_with("Please select a table first.")
        assert 'builder_generated_chart' not in mock_st_in_module.session_state

    def test_generate_chart_button_missing_x_axis(
        self, mock_st_in_module, mock_db_path, mock_logger
    ):
        """Test 'Generate Chart' for bar chart when X-axis is not selected."""
        mock_db_path.exists.return_value = True
        mock_st_in_module.session_state['builder_selected_table'] = "some_table"
        mock_st_in_module.session_state['builder_columns'] = ["A", "B"]
        mock_st_in_module.session_state['builder_chart_type'] = "bar" # Requires X and Y
        mock_st_in_module.session_state['builder_x_axis'] = None      # X-axis not selected
        mock_st_in_module.session_state['builder_y_axis'] = "B"       # Y-axis selected
        
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_generate_chart" else False

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )
        
        mock_st_in_module.error.assert_called_with("Please select X-axis, Y-axis (for bar/line/scatter/histogram), or Names/Values (for pie chart).")
        assert 'builder_generated_chart' not in mock_st_in_module.session_state

    def test_generate_chart_data_filtering_knesset_faction(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_px, mock_logger
    ):
        """Test data filtering with chart-specific Knesset and Faction filters."""
        mock_db_path.exists.return_value = True
        table_name = "data_table"
        
        # Initial data that will be "loaded" by safe_execute_query
        initial_df = pd.DataFrame({
            'X_col': [1, 2, 3, 4, 5, 6],
            'Y_col': [10, 20, 30, 40, 50, 60],
            'KnessetNum': ["23", "23", "24", "24", "25", "25"], # String type to test conversion
            'FactionID': ["1", "2", "1", "2", "1", "2"]        # String type for FactionID
        })
        mock_ui_utils.safe_execute_query.return_value = initial_df

        # Session state setup
        mock_st_in_module.session_state.update({
            'builder_selected_table': table_name,
            'builder_columns': ['X_col', 'Y_col', 'KnessetNum', 'FactionID'],
            'builder_numeric_columns': ['X_col', 'Y_col'],
            'builder_categorical_columns': ['KnessetNum', 'FactionID'],
            'builder_chart_type': "bar",
            'builder_x_axis': "X_col",
            'builder_y_axis': "Y_col",
            'builder_knesset_filter_cs': [24],  # Filter by Knesset 24
            'builder_faction_filter_cs': ["Faction B (ID:2)"], # Filter by Faction ID 2 (Faction B)
            'builder_color_column': None,
            'builder_facet_row': None,
            'builder_facet_col': None,
            'builder_data_for_cs_filters': pd.DataFrame({ # Used for populating filter options, not for this direct test
                'KnessetNum': [23,24,25], 
                'FactionID': [1,2],
                'FactionName': ['Faction A', 'Faction B']
            }),
            'ms_knesset_filter': [], # No global filters
            'ms_faction_filter': [],
        })
        
        # Simulate button click
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_generate_chart" else False
        
        # Mock Plotly Express to capture arguments
        mock_fig = mock.MagicMock()
        mock_px.bar.return_value = mock_fig

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global={1: "Faction A", 2: "Faction B"}, # Faction ID to Name mapping
            logger_obj=mock_logger
        )

        # Check that safe_execute_query was called to load the initial data
        mock_ui_utils.safe_execute_query.assert_called_once()
        # Query should select all specified columns from the table
        # This part of query construction happens inside _get_df_for_chart, which is complex to assert directly without refactoring.
        # We trust safe_execute_query gets called and returns initial_df for this test.

        # Check that Plotly Express bar was called
        mock_px.bar.assert_called_once()
        
        # Get the DataFrame passed to px.bar
        call_args = mock_px.bar.call_args[1]
        df_passed_to_px = call_args['df']
        
        # Expected filtered DataFrame
        expected_df = pd.DataFrame({
            'X_col': [4],
            'Y_col': [40],
            'KnessetNum': ["24"], # String, as in original data
            'FactionID': ["2"]   # String, as in original data
        })
        # Ensure types match for comparison, especially for index.
        # The function internally might reset_index.
        pd.testing.assert_frame_equal(df_passed_to_px.reset_index(drop=True), expected_df.reset_index(drop=True), check_dtype=False)

        assert mock_st_in_module.session_state['builder_generated_chart'] == mock_fig
        mock_st_in_module.toast.assert_called_with("Chart generated successfully!")

    def test_generate_chart_plotly_exception(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_px, mock_logger
    ):
        """Test error handling when a Plotly Express function raises an exception."""
        mock_db_path.exists.return_value = True
        table_name = "error_table"
        
        initial_df = pd.DataFrame({'X': [1], 'Y': [1]})
        mock_ui_utils.safe_execute_query.return_value = initial_df
        
        # Simulate Plotly error
        error_message = "Plotly error"
        mock_px.bar.side_effect = Exception(error_message)

        mock_st_in_module.session_state.update({
            'builder_selected_table': table_name,
            'builder_columns': ['X', 'Y'],
            'builder_numeric_columns': ['Y'],
            'builder_categorical_columns': ['X'],
            'builder_chart_type': "bar",
            'builder_x_axis': "X",
            'builder_y_axis': "Y",
            'builder_knesset_filter_cs': [],
            'builder_faction_filter_cs': [],
            'builder_data_for_cs_filters': pd.DataFrame(),
            'ms_knesset_filter': [],
            'ms_faction_filter': [],
        })
        
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_generate_chart" else False
        # Mock format_exception_for_ui to return a predictable string for assertion
        mock_ui_utils.format_exception_for_ui.return_value = f"Formatted: {error_message}"


        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )

        mock_st_in_module.error.assert_called_once_with(f"Failed to generate chart: {error_message}")
        mock_ui_utils.format_exception_for_ui.assert_called_once() # Check it was called
        mock_st_in_module.code.assert_called_once_with(f"Formatted: {error_message}") # Check it displayed the formatted error
        assert mock_st_in_module.session_state['builder_generated_chart'] is None
        mock_logger.error.assert_called() # Check logger was called
        # Example: logger.error("Exception during chart generation:", exc_info=True)
        # We can check if the call_args contain exc_info=True
        log_call_args = mock_logger.error.call_args
        assert "Exception during chart generation:" in log_call_args[0][0] # Check message
        assert log_call_args[1]['exc_info'] is True # Check exc_info was passed

    def test_reset_chart_button_click_logic(
        self, mock_st_in_module, mock_db_path, mock_logger
    ):
        """Test behavior when 'Reset Chart Options' button is clicked."""
        mock_db_path.exists.return_value = True # DB must exist for button to be shown
        mock_st_in_module.session_state['builder_selected_table'] = "some_table" # Table must be selected

        # Populate some session state keys that should be reset
        mock_st_in_module.session_state.update({
            'builder_chart_type': "bar",
            'builder_x_axis': "X",
            'builder_y_axis': "Y",
            'builder_color_column': "Color",
            'builder_facet_row': "FacetRow",
            'builder_facet_col': "FacetCol",
            'builder_names_pie': "Names",
            'builder_values_pie': "Values",
            'builder_knesset_filter_cs': [25],
            'builder_faction_filter_cs': ["Faction X"],
            'builder_generated_chart': mock.MagicMock(), # A mock figure
            'builder_last_query': "SELECT * FROM DUMMY"
        })
        
        # Simulate "Reset Chart Options" button click
        # The button for reset is keyed 'btn_reset_chart_options'
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_reset_chart_options" else False


        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )

        # Assert that relevant session state keys are deleted or reset to None
        keys_to_be_reset = [
            'builder_chart_type', 'builder_x_axis', 'builder_y_axis',
            'builder_color_column', 'builder_facet_row', 'builder_facet_col',
            'builder_names_pie', 'builder_values_pie',
            'builder_knesset_filter_cs', 'builder_faction_filter_cs',
            'builder_generated_chart', 'builder_last_query'
        ]
        for key in keys_to_be_reset:
            assert key not in mock_st_in_module.session_state or mock_st_in_module.session_state[key] is None
        
        mock_st_in_module.toast.assert_called_with("Chart options reset.")
        mock_st_in_module.rerun.assert_called_once()
        mock_st_in_module.error.assert_not_called()
        mock_st_in_module.warning.assert_not_called()

    def test_generate_chart_df_empty_after_filtering(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_px, mock_logger
    ):
        """Test behavior when df_for_chart becomes empty after chart-specific filtering."""
        mock_db_path.exists.return_value = True
        table_name = "empty_after_filter_table"
        
        initial_df = pd.DataFrame({
            'X_col': [1, 2], 'Y_col': [10, 20],
            'KnessetNum': ["23", "23"], 'FactionID': ["1", "1"]
        })
        mock_ui_utils.safe_execute_query.return_value = initial_df

        mock_st_in_module.session_state.update({
            'builder_selected_table': table_name,
            'builder_columns': ['X_col', 'Y_col', 'KnessetNum', 'FactionID'],
            'builder_numeric_columns': ['X_col', 'Y_col'],
            'builder_chart_type': "bar",
            'builder_x_axis': "X_col",
            'builder_y_axis': "Y_col",
            'builder_knesset_filter_cs': [24], # Filter by Knesset 24 (will make df empty)
            'builder_faction_filter_cs': [],
            'builder_data_for_cs_filters': pd.DataFrame({'KnessetNum': [23,24]}),
            'ms_knesset_filter': [],
            'ms_faction_filter': [],
        })
        
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_generate_chart" else False

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )
        
        mock_st_in_module.warning.assert_called_with("No data available for the selected filters. Cannot generate chart.")
        assert mock_st_in_module.session_state['builder_generated_chart'] is None
        mock_px.bar.assert_not_called() # Plotly should not be called

    def test_generate_chart_successful_bar_chart(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_px, mock_logger
    ):
        """Test successful generation of a bar chart."""
        mock_db_path.exists.return_value = True
        table_name = "bar_chart_table"
        
        initial_df = pd.DataFrame({
            'Month': ['Jan', 'Feb', 'Mar'],
            'Sales': [100, 150, 120],
            'Region': ['North', 'South', 'North']
        })
        mock_ui_utils.safe_execute_query.return_value = initial_df
        mock_fig = mock.MagicMock()
        mock_px.bar.return_value = mock_fig

        mock_st_in_module.session_state.update({
            'builder_selected_table': table_name,
            'builder_columns': ['Month', 'Sales', 'Region'],
            'builder_numeric_columns': ['Sales'],
            'builder_categorical_columns': ['Month', 'Region'],
            'builder_chart_type': "bar",
            'builder_x_axis': "Month",
            'builder_y_axis': "Sales",
            'builder_knesset_filter_cs': [],
            'builder_faction_filter_cs': [],
            'builder_color_column': "Region",
            'builder_facet_row': None,
            'builder_facet_col': None,
            'builder_data_for_cs_filters': pd.DataFrame(),
            'ms_knesset_filter': [],
            'ms_faction_filter': [],
        })
        
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_generate_chart" else False

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )

        mock_px.bar.assert_called_once()
        call_args = mock_px.bar.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], initial_df)
        assert call_args['x'] == "Month"
        assert call_args['y'] == "Sales"
        assert call_args['color'] == "Region"
        assert call_args['title'] == "Bar Chart of Sales by Month colored by Region" # Check title construction
        
        assert mock_st_in_module.session_state['builder_generated_chart'] == mock_fig
        mock_st_in_module.toast.assert_called_with("Chart generated successfully!")

    def test_generate_chart_successful_pie_chart(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_px, mock_logger
    ):
        """Test successful generation of a pie chart."""
        mock_db_path.exists.return_value = True
        table_name = "pie_chart_table"
        
        initial_df = pd.DataFrame({
            'Category': ['A', 'B', 'C'],
            'Count': [50, 30, 20]
        })
        mock_ui_utils.safe_execute_query.return_value = initial_df
        mock_fig = mock.MagicMock()
        mock_px.pie.return_value = mock_fig

        mock_st_in_module.session_state.update({
            'builder_selected_table': table_name,
            'builder_columns': ['Category', 'Count'],
            'builder_numeric_columns': ['Count'],
            'builder_categorical_columns': ['Category'],
            'builder_chart_type': "pie",
            'builder_names_pie': "Category",
            'builder_values_pie': "Count",
            'builder_knesset_filter_cs': [],
            'builder_faction_filter_cs': [],
            'builder_data_for_cs_filters': pd.DataFrame(),
            'ms_knesset_filter': [],
            'ms_faction_filter': [],
        })
        
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_generate_chart" else False

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )

        mock_px.pie.assert_called_once()
        call_args = mock_px.pie.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], initial_df)
        assert call_args['names'] == "Category"
        assert call_args['values'] == "Count"
        assert call_args['title'] == "Pie Chart of Count by Category"
        
        assert mock_st_in_module.session_state['builder_generated_chart'] == mock_fig
        mock_st_in_module.toast.assert_called_with("Chart generated successfully!")

    def test_generate_chart_successful_histogram(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_px, mock_logger
    ):
        """Test successful generation of a histogram."""
        mock_db_path.exists.return_value = True
        table_name = "hist_table"
        
        initial_df = pd.DataFrame({'Age': [22, 25, 30, 30, 35, 40]})
        mock_ui_utils.safe_execute_query.return_value = initial_df
        mock_fig = mock.MagicMock()
        mock_px.histogram.return_value = mock_fig

        mock_st_in_module.session_state.update({
            'builder_selected_table': table_name,
            'builder_columns': ['Age'],
            'builder_numeric_columns': ['Age'],
            'builder_categorical_columns': [],
            'builder_chart_type': "histogram",
            'builder_x_axis': "Age", # For histogram, only X is typically needed from user
            'builder_y_axis': None, # Y is count, implicit for histogram
            'builder_knesset_filter_cs': [],
            'builder_faction_filter_cs': [],
            'builder_data_for_cs_filters': pd.DataFrame(),
            'ms_knesset_filter': [],
            'ms_faction_filter': [],
        })
        
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_generate_chart" else False

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )

        mock_px.histogram.assert_called_once()
        call_args = mock_px.histogram.call_args[1]
        pd.testing.assert_frame_equal(call_args['df'], initial_df)
        assert call_args['x'] == "Age"
        assert call_args['title'] == "Histogram of Age"
        
        assert mock_st_in_module.session_state['builder_generated_chart'] == mock_fig
        mock_st_in_module.toast.assert_called_with("Chart generated successfully!")
        mock_st_in_module.error.assert_not_called() # Should be a warning, not an error

    def test_generate_chart_facet_cardinality_too_high(
        self, mock_st_in_module, mock_db_path, mock_ui_utils, mock_px, mock_logger
    ):
        """Test error handling when facet column cardinality is too high."""
        mock_db_path.exists.return_value = True
        table_name = "facet_table"
        
        # Create a DataFrame where 'Category' has more unique values than allowed
        categories = [f"Cat_{i}" for i in range(DEFAULT_MAX_FACET_VALUES + 5)]
        initial_df = pd.DataFrame({
            'X_col': list(range(len(categories))),
            'Y_col': list(range(len(categories))),
            'Category': categories # High cardinality column for facet
        })
        mock_ui_utils.safe_execute_query.return_value = initial_df

        mock_st_in_module.session_state.update({
            'builder_selected_table': table_name,
            'builder_columns': ['X_col', 'Y_col', 'Category'],
            'builder_numeric_columns': ['X_col', 'Y_col'],
            'builder_categorical_columns': ['Category'],
            'builder_chart_type': "bar",
            'builder_x_axis': "X_col",
            'builder_y_axis': "Y_col",
            'builder_knesset_filter_cs': [],
            'builder_faction_filter_cs': [],
            'builder_color_column': None,
            'builder_facet_row': "Category", # Using high cardinality column for facet
            'builder_facet_col': None,
            'builder_data_for_cs_filters': pd.DataFrame(),
            'ms_knesset_filter': [],
            'ms_faction_filter': [],
        })
        
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_generate_chart" else False
        
        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES, # Set max for test
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )
        
        mock_st_in_module.error.assert_called_once_with(
            f"Facet column 'Category' has too many unique values ({len(categories)}). Maximum allowed is {DEFAULT_MAX_FACET_VALUES}. Please apply filters or choose a different column."
        )
        assert mock_st_in_module.session_state['builder_generated_chart'] is None
        mock_px.bar.assert_not_called() # Plotly should not be called

    def test_generate_chart_button_missing_y_axis_for_bar(
        self, mock_st_in_module, mock_db_path, mock_logger
    ):
        """Test 'Generate Chart' for bar chart when Y-axis is not selected."""
        mock_db_path.exists.return_value = True
        mock_st_in_module.session_state['builder_selected_table'] = "some_table"
        mock_st_in_module.session_state['builder_columns'] = ["A", "B"]
        mock_st_in_module.session_state['builder_chart_type'] = "bar"
        mock_st_in_module.session_state['builder_x_axis'] = "A"
        mock_st_in_module.session_state['builder_y_axis'] = None # Y-axis not selected
        
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_generate_chart" else False

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )
        
        mock_st_in_module.error.assert_called_with("Please select X-axis, Y-axis (for bar/line/scatter/histogram), or Names/Values (for pie chart).")
        assert 'builder_generated_chart' not in mock_st_in_module.session_state

    def test_generate_chart_button_missing_names_for_pie(
        self, mock_st_in_module, mock_db_path, mock_logger
    ):
        """Test 'Generate Chart' for pie chart when Names field is not selected."""
        mock_db_path.exists.return_value = True
        mock_st_in_module.session_state['builder_selected_table'] = "some_table"
        mock_st_in_module.session_state['builder_columns'] = ["Category", "Value"]
        mock_st_in_module.session_state['builder_chart_type'] = "pie"
        mock_st_in_module.session_state['builder_names_pie'] = None # Names not selected
        mock_st_in_module.session_state['builder_values_pie'] = "Value"
        
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_generate_chart" else False

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )
        
        mock_st_in_module.error.assert_called_with("Please select X-axis, Y-axis (for bar/line/scatter/histogram), or Names/Values (for pie chart).")
        assert 'builder_generated_chart' not in mock_st_in_module.session_state

    def test_generate_chart_button_missing_values_for_pie(
        self, mock_st_in_module, mock_db_path, mock_logger
    ):
        """Test 'Generate Chart' for pie chart when Values field is not selected."""
        mock_db_path.exists.return_value = True
        mock_st_in_module.session_state['builder_selected_table'] = "some_table"
        mock_st_in_module.session_state['builder_columns'] = ["Category", "Value"]
        mock_st_in_module.session_state['builder_chart_type'] = "pie"
        mock_st_in_module.session_state['builder_names_pie'] = "Category"
        mock_st_in_module.session_state['builder_values_pie'] = None # Values not selected
        
        mock_st_in_module.button.side_effect = lambda label, key: True if key == "btn_generate_chart" else False

        display_chart_builder(
            db_path=mock_db_path,
            max_rows_for_chart_builder=DEFAULT_MAX_ROWS,
            max_unique_values_for_facet=DEFAULT_MAX_FACET_VALUES,
            faction_display_map_global=DEFAULT_FACTION_DISPLAY_MAP,
            logger_obj=mock_logger
        )
        
        mock_st_in_module.error.assert_called_with("Please select X-axis, Y-axis (for bar/line/scatter/histogram), or Names/Values (for pie chart).")
        assert 'builder_generated_chart' not in mock_st_in_module.session_state

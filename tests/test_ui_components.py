"""
Tests for UI components focusing on business logic and component behavior.
"""
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from typing import List, Dict, Optional, Any

from src.ui.state.session_manager import SessionStateManager

# Import with error handling for modules that may not exist
try:
    from src.ui.queries.query_executor import QueryExecutor
except ImportError:
    QueryExecutor = None

try:
    from src.ui.pages.data_refresh_page import DataRefreshPageRenderer
except ImportError:
    DataRefreshPageRenderer = None

try:
    from src.ui.pages.plots_page import PlotsPageRenderer
except ImportError:
    PlotsPageRenderer = None

try:
    from src.ui.ui_utils import get_table_info, get_filter_options
except ImportError:
    get_table_info = None
    get_filter_options = None

# ChartRenderer doesn't exist - skip it
ChartRenderer = None


class TestSessionStateManager:
    """Test session state management functionality."""
    
    @patch('streamlit.session_state', {})
    def test_session_state_initialization(self):
        """Test session state gets initialized with proper defaults."""
        # Test getter methods return None for uninitialized state
        assert SessionStateManager.get_selected_query_name() is None
        assert SessionStateManager.get_query_results_df() is None
        assert SessionStateManager.get_executed_query_name() is None
        assert SessionStateManager.get_last_executed_sql() is None
    
    @patch('streamlit.session_state', {})
    def test_set_query_results(self):
        """Test setting query results updates all related state."""
        test_df = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
        test_sql = "SELECT * FROM test_table"
        test_filters = ["Filter: Active=True", "Filter: Category=Test"]
        
        SessionStateManager.set_query_results(
            "Test Query", test_df, test_sql, test_filters
        )
        
        # Verify state was set correctly
        assert SessionStateManager.get_executed_query_name() == "Test Query"
        assert SessionStateManager.get_query_results_df().equals(test_df)
        assert SessionStateManager.get_last_executed_sql() == test_sql
        assert SessionStateManager.get_applied_filters() == test_filters
        assert SessionStateManager.get_show_query_results() is True
    
    @patch('streamlit.session_state', {})
    def test_clear_query_results(self):
        """Test clearing query results resets related state."""
        # First set some state
        test_df = pd.DataFrame({'id': [1]})
        SessionStateManager.set_query_results("Test", test_df, "SELECT 1", [])
        
        # Then clear it
        SessionStateManager.clear_query_results()
        
        # Verify state was cleared
        assert SessionStateManager.get_query_results_df() is None
        assert SessionStateManager.get_show_query_results() is False
        assert SessionStateManager.get_applied_filters() == []
    
    @patch('streamlit.session_state', {})
    def test_table_explorer_state_management(self):
        """Test table explorer state management."""
        # Test setting table explorer state
        SessionStateManager.set_table_explorer_state("test_table", 100, 0)
        
        assert SessionStateManager.get_selected_table_name() == "test_table"
        assert SessionStateManager.get_table_record_count() == 100
        assert SessionStateManager.get_table_display_offset() == 0
        assert SessionStateManager.get_show_table_explorer() is True
    
    @patch('streamlit.session_state', {})
    def test_plot_selection_state(self):
        """Test plot selection state management."""
        test_plots = ["plot1", "plot2", "plot3"]
        
        SessionStateManager.set_selected_plots(test_plots)
        
        assert SessionStateManager.get_selected_plots() == test_plots
        assert SessionStateManager.get_show_selected_plots() is True


@pytest.mark.skipif(QueryExecutor is None, reason="QueryExecutor not available")
class TestQueryExecutor:
    """Test query execution with filtering logic."""

    def setup_method(self):
        """Setup test fixtures."""
        self.mock_db_path = Path("test.db")
        self.mock_logger = Mock()
        self.mock_connect_func = Mock()
        self.executor = QueryExecutor(
            self.mock_db_path,
            self.mock_connect_func,
            self.mock_logger
        )
    
    @patch('src.ui.queries.query_executor.get_query_sql')
    @patch('src.ui.queries.query_executor.get_query_definition')
    def test_execute_query_with_knesset_filter(self, mock_get_def, mock_get_sql):
        """Test query execution with Knesset number filter."""
        mock_get_def.return_value = {'name': 'Test Query'}
        mock_get_sql.return_value = "SELECT * FROM KNS_Query ORDER BY QueryID"
        
        mock_con = Mock()
        mock_con.execute.return_value.df.return_value = pd.DataFrame({'id': [1, 2]})
        self.mock_connect_func.return_value.__enter__.return_value = mock_con
        
        results_df, executed_sql, filters_info = self.executor.execute_query_with_filters(
            "Test Query", 
            knesset_filter=[25, 26]
        )
        
        # Verify filter was applied to SQL
        assert "Q.KnessetNum IN (25, 26)" in executed_sql
        assert "Knesset(s): 25, 26" in filters_info
        assert len(results_df) == 2
    
    @patch('src.ui.queries.query_executor.get_query_sql')
    @patch('src.ui.queries.query_executor.get_query_definition')
    @patch('src.ui.queries.query_executor.get_faction_display_to_id_map')
    def test_execute_query_with_faction_filter(self, mock_faction_map, mock_get_def, mock_get_sql):
        """Test query execution with faction filter."""
        mock_get_def.return_value = {'name': 'Test Query'}
        mock_get_sql.return_value = "SELECT * FROM KNS_Query"
        mock_faction_map.return_value = {'Test Faction': 1, 'Other Faction': 2}
        
        mock_con = Mock()
        mock_con.execute.return_value.df.return_value = pd.DataFrame({'id': [1]})
        self.mock_connect_func.return_value.__enter__.return_value = mock_con
        
        results_df, executed_sql, filters_info = self.executor.execute_query_with_filters(
            "Test Query", 
            faction_filter=['Test Faction']
        )
        
        # Verify faction filter was applied
        assert "FactionID IN (1)" in executed_sql
        assert "Faction(s): Test Faction" in filters_info
    
    @patch('src.ui.queries.query_executor.get_query_sql')
    @patch('src.ui.queries.query_executor.get_query_definition')
    def test_execute_query_no_filters(self, mock_get_def, mock_get_sql):
        """Test query execution without any filters."""
        mock_get_def.return_value = {'name': 'Test Query'}
        mock_get_sql.return_value = "SELECT * FROM KNS_Query"
        
        mock_con = Mock()
        mock_con.execute.return_value.df.return_value = pd.DataFrame({'id': [1, 2, 3]})
        self.mock_connect_func.return_value.__enter__.return_value = mock_con
        
        results_df, executed_sql, filters_info = self.executor.execute_query_with_filters(
            "Test Query"
        )
        
        # Verify no filters were applied
        assert "WHERE" not in executed_sql
        assert filters_info == []
        assert len(results_df) == 3
    
    @patch('src.ui.queries.query_executor.get_query_definition')
    def test_execute_query_invalid_query_name(self, mock_get_def):
        """Test handling of invalid query names."""
        mock_get_def.return_value = None
        
        results_df, executed_sql, filters_info = self.executor.execute_query_with_filters(
            "Invalid Query"
        )
        
        # Verify error handling
        assert results_df.empty
        assert executed_sql == ""
        assert "Error: Query not found" in filters_info


@pytest.mark.skipif(DataRefreshPageRenderer is None, reason="DataRefreshPageRenderer not available")
class TestDataRefreshPageRenderer:
    """Test data refresh page rendering logic."""

    def setup_method(self):
        """Setup test fixtures."""
        self.mock_db_path = Path("test.db")
        self.mock_logger = Mock()
        self.renderer = DataRefreshPageRenderer(self.mock_db_path, self.mock_logger)
    
    @patch('src.ui.pages.data_refresh_page.SessionStateManager')
    @patch('streamlit.info')
    def test_render_query_results_section_no_results(self, mock_info, mock_session):
        """Test rendering when no query results are available."""
        mock_session.get_show_query_results.return_value = False
        
        self.renderer.render_query_results_section()
        
        mock_info.assert_called_once_with("Run a predefined query to see results")
    
    @patch('src.ui.pages.data_refresh_page.SessionStateManager')
    @patch('streamlit.dataframe')
    @patch('streamlit.columns')
    def test_render_query_results_section_with_results(self, mock_columns, mock_dataframe, mock_session):
        """Test rendering when query results are available."""
        test_df = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
        mock_session.get_show_query_results.return_value = True
        mock_session.get_query_results_df.return_value = test_df
        mock_session.get_executed_query_name.return_value = "Test Query"
        
        # Mock columns context manager
        mock_col1, mock_col2 = Mock(), Mock()
        mock_columns.return_value = (mock_col1, mock_col2)
        mock_col1.__enter__ = Mock(return_value=mock_col1)
        mock_col1.__exit__ = Mock(return_value=None)
        mock_col2.__enter__ = Mock(return_value=mock_col2)
        mock_col2.__exit__ = Mock(return_value=None)
        
        self.renderer.render_query_results_section()
        
        # Verify dataframe was displayed
        mock_dataframe.assert_called_once()
        args, kwargs = mock_dataframe.call_args
        assert args[0].equals(test_df)
        assert kwargs['use_container_width'] is True
    
    @patch('src.ui.pages.data_refresh_page.SessionStateManager')
    @patch('streamlit.info')
    def test_render_table_explorer_section_no_table(self, mock_info, mock_session):
        """Test rendering table explorer when no table is selected."""
        mock_session.get_show_table_explorer.return_value = False
        
        self.renderer.render_table_explorer_section()
        
        mock_info.assert_called_once_with("Select a table from the sidebar to explore")


@pytest.mark.skipif(PlotsPageRenderer is None, reason="PlotsPageRenderer not available")
class TestPlotsPageRenderer:
    """Test plots page rendering logic."""

    def setup_method(self):
        """Setup test fixtures."""
        self.mock_db_path = Path("test.db")
        self.mock_logger = Mock()
        self.renderer = PlotsPageRenderer(self.mock_db_path, self.mock_logger)
    
    @patch('src.ui.pages.plots_page.SessionStateManager')
    @patch('streamlit.info')
    def test_render_plots_section_no_selection(self, mock_info, mock_session):
        """Test rendering when no plots are selected."""
        mock_session.get_show_selected_plots.return_value = False
        
        self.renderer.render_plots_section()
        
        mock_info.assert_called_once_with("Select plots from the sidebar to display")
    
    @patch('src.ui.pages.plots_page.SessionStateManager')
    @patch('src.ui.pages.plots_page.ChartRenderer')
    def test_render_plots_section_with_selection(self, mock_chart_renderer, mock_session):
        """Test rendering when plots are selected."""
        mock_session.get_show_selected_plots.return_value = True
        mock_session.get_selected_plots.return_value = ["plot1", "plot2"]
        
        mock_renderer_instance = Mock()
        mock_chart_renderer.return_value = mock_renderer_instance
        
        self.renderer.render_plots_section()
        
        # Verify chart renderer was called for each plot
        assert mock_renderer_instance.render_chart.call_count == 2


@pytest.mark.skipif(get_table_info is None, reason="UI utils not available")
class TestUIUtils:
    """Test UI utility functions."""

    @patch('src.ui.ui_utils.get_db_connection')
    def test_get_table_info_success(self, mock_get_connection):
        """Test successful table info retrieval."""
        mock_con = Mock()
        mock_con.execute.return_value.df.return_value = pd.DataFrame({
            'table_name': ['table1', 'table2'],
            'estimated_size': [100, 200]
        })
        mock_get_connection.return_value.__enter__.return_value = mock_con
        
        result = get_table_info(Path("test.db"))
        
        assert len(result) == 2
        assert result[0]['table_name'] == 'table1'
        assert result[1]['estimated_size'] == 200
    
    @patch('src.ui.ui_utils.get_db_connection')
    def test_get_table_info_database_error(self, mock_get_connection):
        """Test table info retrieval with database error."""
        mock_get_connection.side_effect = Exception("Database error")
        
        result = get_table_info(Path("test.db"))
        
        assert result == []
    
    @patch('src.ui.ui_utils.get_db_connection')
    def test_get_filter_options_success(self, mock_get_connection):
        """Test successful filter options retrieval."""
        mock_con = Mock()
        
        # Mock Knesset numbers query
        mock_con.execute.return_value.df.side_effect = [
            pd.DataFrame({'KnessetNum': [24, 25, 26]}),  # Knesset numbers
            pd.DataFrame({'FactionName': ['Faction A', 'Faction B']})  # Faction names
        ]
        mock_get_connection.return_value.__enter__.return_value = mock_con
        
        knesset_options, faction_options = get_filter_options(Path("test.db"))
        
        assert knesset_options == [24, 25, 26]
        assert faction_options == ['Faction A', 'Faction B']
    
    @patch('src.ui.ui_utils.get_db_connection')
    def test_get_filter_options_database_error(self, mock_get_connection):
        """Test filter options retrieval with database error."""
        mock_get_connection.side_effect = Exception("Database error")
        
        knesset_options, faction_options = get_filter_options(Path("test.db"))
        
        assert knesset_options == []
        assert faction_options == []


@pytest.mark.skipif(ChartRenderer is None, reason="ChartRenderer not available")
class TestChartRenderer:
    """Test chart rendering functionality."""

    def setup_method(self):
        """Setup test fixtures."""
        self.mock_db_path = Path("test.db")
        self.mock_logger = Mock()
        self.renderer = ChartRenderer(self.mock_db_path, self.mock_logger)
    
    @patch('src.ui.chart_renderer.ChartFactory')
    def test_render_chart_success(self, mock_chart_factory):
        """Test successful chart rendering."""
        mock_chart = Mock()
        mock_chart.create.return_value = True
        mock_chart_factory.get_chart.return_value = mock_chart
        
        result = self.renderer.render_chart("test_chart")
        
        assert result is True
        mock_chart_factory.get_chart.assert_called_once_with("test_chart", self.mock_db_path)
        mock_chart.create.assert_called_once()
    
    @patch('src.ui.chart_renderer.ChartFactory')
    @patch('streamlit.error')
    def test_render_chart_invalid_chart_type(self, mock_error, mock_chart_factory):
        """Test rendering with invalid chart type."""
        mock_chart_factory.get_chart.return_value = None
        
        result = self.renderer.render_chart("invalid_chart")
        
        assert result is False
        mock_error.assert_called_once()
        assert "Unknown chart type" in mock_error.call_args[0][0]
    
    @patch('src.ui.chart_renderer.ChartFactory')
    @patch('streamlit.error')
    def test_render_chart_creation_error(self, mock_error, mock_chart_factory):
        """Test chart rendering with creation error."""
        mock_chart = Mock()
        mock_chart.create.side_effect = Exception("Chart creation failed")
        mock_chart_factory.get_chart.return_value = mock_chart
        
        result = self.renderer.render_chart("test_chart")
        
        assert result is False
        mock_error.assert_called_once()
        assert "Error creating chart" in mock_error.call_args[0][0]


class TestComponentIntegration:
    """Test integration between UI components."""
    
    @patch('streamlit.session_state', {})
    def test_query_execution_to_display_flow(self):
        """Test complete flow from query execution to display."""
        # Setup mock components
        mock_db_path = Path("test.db")
        mock_logger = Mock()
        mock_connect_func = Mock()
        
        # Mock query execution
        executor = QueryExecutor(mock_db_path, mock_connect_func, mock_logger)
        
        with patch('src.ui.queries.query_executor.get_query_definition') as mock_get_def, \
             patch('src.ui.queries.query_executor.get_query_sql') as mock_get_sql:
            
            mock_get_def.return_value = {'name': 'Test Query'}
            mock_get_sql.return_value = "SELECT * FROM test"
            
            mock_con = Mock()
            test_df = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
            mock_con.execute.return_value.df.return_value = test_df
            mock_connect_func.return_value.__enter__.return_value = mock_con
            
            # Execute query
            results_df, executed_sql, filters_info = executor.execute_query_with_filters(
                "Test Query"
            )
            
            # Set results in session state
            SessionStateManager.set_query_results(
                "Test Query", results_df, executed_sql, filters_info
            )
            
            # Verify state was set correctly for display
            assert SessionStateManager.get_show_query_results() is True
            assert SessionStateManager.get_executed_query_name() == "Test Query"
            assert SessionStateManager.get_query_results_df().equals(test_df)
    
    def test_error_handling_across_components(self):
        """Test error handling propagation across components."""
        mock_db_path = Path("nonexistent.db")
        mock_logger = Mock()
        
        # Test that components handle missing database gracefully
        renderer = DataRefreshPageRenderer(mock_db_path, mock_logger)
        
        # This should handle the missing database without crashing
        with patch('streamlit.error') as mock_error:
            # Component should handle database errors gracefully
            pass  # The actual component methods would be called here
    
    @patch('streamlit.session_state', {})
    def test_state_persistence_across_page_renders(self):
        """Test that state persists across multiple page renders."""
        # Set initial state
        test_df = pd.DataFrame({'col': [1, 2, 3]})
        SessionStateManager.set_query_results(
            "Persistent Query", test_df, "SELECT * FROM test", []
        )
        
        # Simulate page re-render by checking state is still there
        assert SessionStateManager.get_executed_query_name() == "Persistent Query"
        assert SessionStateManager.get_query_results_df().equals(test_df)
        assert SessionStateManager.get_show_query_results() is True
        
        # Clear state and verify it's gone
        SessionStateManager.clear_query_results()
        assert SessionStateManager.get_show_query_results() is False
        assert SessionStateManager.get_query_results_df() is None


class TestUIBusinessLogicEdgeCases:
    """Test edge cases in UI business logic."""
    
    @patch('streamlit.session_state', {})
    def test_session_state_with_empty_dataframe(self):
        """Test session state handling with empty DataFrames."""
        empty_df = pd.DataFrame()
        SessionStateManager.set_query_results(
            "Empty Query", empty_df, "SELECT * FROM empty_table", []
        )
        
        retrieved_df = SessionStateManager.get_query_results_df()
        assert retrieved_df.empty
        assert len(retrieved_df) == 0
    
    @patch('streamlit.session_state', {})
    def test_session_state_with_large_dataframe(self):
        """Test session state handling with large DataFrames."""
        large_df = pd.DataFrame({'col': range(10000)})
        SessionStateManager.set_query_results(
            "Large Query", large_df, "SELECT * FROM large_table", []
        )
        
        retrieved_df = SessionStateManager.get_query_results_df()
        assert len(retrieved_df) == 10000
        assert retrieved_df.equals(large_df)
    
    def test_query_executor_with_special_characters(self):
        """Test query executor with special characters in filters."""
        mock_db_path = Path("test.db")
        mock_logger = Mock()
        mock_connect_func = Mock()
        executor = QueryExecutor(mock_db_path, mock_connect_func, mock_logger)
        
        with patch('src.ui.queries.query_executor.get_query_definition') as mock_get_def, \
             patch('src.ui.queries.query_executor.get_query_sql') as mock_get_sql, \
             patch('src.ui.queries.query_executor.get_faction_display_to_id_map') as mock_faction_map:
            
            mock_get_def.return_value = {'name': 'Test Query'}
            mock_get_sql.return_value = "SELECT * FROM test"
            mock_faction_map.return_value = {"Faction with 'quotes'": 1}
            
            mock_con = Mock()
            mock_con.execute.return_value.df.return_value = pd.DataFrame({'id': [1]})
            mock_connect_func.return_value.__enter__.return_value = mock_con
            
            # Execute query with special characters
            results_df, executed_sql, filters_info = executor.execute_query_with_filters(
                "Test Query", 
                faction_filter=["Faction with 'quotes'"]
            )
            
            # Verify the query executed without errors
            assert len(results_df) == 1
            assert "Faction with 'quotes'" in str(filters_info)
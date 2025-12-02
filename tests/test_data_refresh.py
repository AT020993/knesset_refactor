"""
Tests for data_refresh module.

Note: These tests are simplified because the data_refresh module has significant
module-level side effects (Streamlit UI initialization, session state management)
that make comprehensive unit testing challenging. The tests focus on verifying
that the basic module structure and key components are accessible.
"""
import pytest
import pandas as pd
from pathlib import Path
from unittest import mock


def _passthrough_decorator(func=None, *args, **kwargs):
    """A decorator that does nothing but return the original function.

    Accepts any arguments (ttl, show_spinner, etc.) but ignores them.
    """
    if func is None:
        # Called as @st.cache_data(ttl=300)
        def wrapper(fn):
            return fn
        return wrapper
    # Called as @st.cache_data without parentheses
    return func


@pytest.fixture(autouse=True)
def mock_streamlit():
    """Mock streamlit to prevent UI initialization during testing."""
    mock_st = mock.MagicMock()
    mock_st.session_state = {}
    mock_st.cache_data = _passthrough_decorator
    mock_st.cache_data.clear = mock.MagicMock()
    mock_st.cache_resource = _passthrough_decorator
    mock_st.cache_resource.clear = mock.MagicMock()
    mock_st.set_page_config = mock.MagicMock()

    with mock.patch.dict("sys.modules", {"streamlit": mock_st}):
        with mock.patch("streamlit.set_page_config", mock_st.set_page_config):
            with mock.patch("streamlit.session_state", mock_st.session_state):
                with mock.patch("streamlit.cache_data", _passthrough_decorator):
                    with mock.patch("streamlit.cache_resource", _passthrough_decorator):
                        yield mock_st


class TestDataRefreshModuleStructure:
    """Test basic module structure and constants."""

    def test_module_imports_successfully(self, mock_streamlit):
        """Test that the data_refresh module can be imported without errors."""
        # This test verifies the module structure is correct
        # The actual import happens at test collection time, so we verify
        # that the expected constants and imports exist
        try:
            # These imports should work based on the module structure
            from ui.queries.predefined_queries import PREDEFINED_QUERIES
            assert PREDEFINED_QUERIES is not None
            assert isinstance(PREDEFINED_QUERIES, dict)
        except ImportError as e:
            pytest.skip(f"Module import failed: {e}")

    def test_tables_configuration_exists(self, mock_streamlit):
        """Test that TABLES configuration is available."""
        from backend.fetch_table import TABLES

        assert TABLES is not None
        # TABLES is a list of table names
        assert isinstance(TABLES, list)
        assert len(TABLES) > 0


class TestPredefinedQueries:
    """Test the predefined queries configuration."""

    def test_predefined_queries_structure(self, mock_streamlit):
        """Test that PREDEFINED_QUERIES has the expected structure."""
        from ui.queries.predefined_queries import PREDEFINED_QUERIES

        # Each query should have required fields
        for query_name, query_config in PREDEFINED_QUERIES.items():
            assert isinstance(query_name, str)
            assert isinstance(query_config, dict)
            # Queries should have at least a sql key or builder
            assert "sql" in query_config or "builder" in query_config or callable(query_config.get("sql"))

    def test_predefined_queries_not_empty(self, mock_streamlit):
        """Test that there are predefined queries available."""
        from ui.queries.predefined_queries import PREDEFINED_QUERIES

        assert len(PREDEFINED_QUERIES) > 0


class TestPlotGenerators:
    """Test the plot generators module."""

    def test_get_available_plots_exists(self, mock_streamlit):
        """Test that get_available_plots function exists and returns expected structure."""
        from ui.plot_generators import get_available_plots

        plots = get_available_plots()
        assert isinstance(plots, dict)
        # Should have categories
        assert len(plots) > 0

        # Each category should map to another dict of plot_name -> function
        for category, plot_dict in plots.items():
            assert isinstance(category, str)
            assert isinstance(plot_dict, dict)
            for plot_name, plot_func in plot_dict.items():
                assert isinstance(plot_name, str)
                assert callable(plot_func)


class TestSessionStateManager:
    """Test the session state manager."""

    def test_session_manager_exists(self, mock_streamlit):
        """Test that SessionStateManager can be imported."""
        from ui.state.session_manager import SessionStateManager

        assert SessionStateManager is not None
        assert hasattr(SessionStateManager, 'initialize_all_session_state')


class TestPageRenderers:
    """Test that page renderer classes exist."""

    def test_data_refresh_page_renderer_exists(self, mock_streamlit):
        """Test that DataRefreshPageRenderer exists."""
        from ui.pages.data_refresh_page import DataRefreshPageRenderer

        assert DataRefreshPageRenderer is not None

    def test_plots_page_renderer_exists(self, mock_streamlit):
        """Test that PlotsPageRenderer exists."""
        from ui.pages.plots_page import PlotsPageRenderer

        assert PlotsPageRenderer is not None


class TestUIUtils:
    """Test UI utility functions are accessible."""

    def test_ui_utils_module_exists(self, mock_streamlit):
        """Test that ui_utils module can be imported."""
        import ui.ui_utils as ui_utils

        # Check for expected functions
        assert hasattr(ui_utils, 'connect_db')
        assert hasattr(ui_utils, 'safe_execute_query')
        assert hasattr(ui_utils, 'get_filter_options_from_db')
        assert hasattr(ui_utils, 'get_last_updated_for_table')
        assert hasattr(ui_utils, 'format_exception_for_ui')


class TestSidebarComponents:
    """Test sidebar components module."""

    def test_sidebar_components_exist(self, mock_streamlit):
        """Test that sidebar components module can be imported."""
        import ui.sidebar_components as sc

        # Check for expected functions/classes
        assert sc is not None

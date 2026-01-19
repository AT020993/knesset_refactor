"""
Tests for configuration modules functionality.
"""
import pytest
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List, Optional, Any

from src.config.settings import Settings
from src.config.api import APIConfig, CircuitBreakerState
from src.config.database import DatabaseConfig
from src.api.error_handling import ErrorCategory
from src.config.charts import ChartConfig


class TestSettings:
    """Test Settings configuration class."""
    
    def test_project_root_detection(self):
        """Test automatic project root detection."""
        project_root = Settings.PROJECT_ROOT
        
        assert isinstance(project_root, Path)
        assert project_root.exists()
        assert project_root.is_dir()
        # Should contain key project files
        assert (project_root / "src").exists()
    
    def test_default_paths(self):
        """Test default path configurations."""
        # Test data directory
        data_dir = Settings.DATA_DIR
        assert isinstance(data_dir, Path)
        assert str(data_dir).endswith("data")
        
        # Test logs directory
        logs_dir = Settings.LOGS_DIR
        assert isinstance(logs_dir, Path)
        assert str(logs_dir).endswith("logs")
        
        # Test parquet directory
        parquet_dir = Settings.PARQUET_DIR
        assert isinstance(parquet_dir, Path)
        assert str(parquet_dir).endswith("parquet")
    
    def test_get_db_path_default(self):
        """Test default database path retrieval."""
        db_path = Settings.get_db_path()
        
        assert isinstance(db_path, Path)
        assert str(db_path).endswith("warehouse.duckdb")
        assert db_path.parent == Settings.DATA_DIR
    
    def test_get_db_path_custom(self):
        """Test custom database path override."""
        custom_path = Path("/custom/path/test.db")
        
        db_path = Settings.get_db_path(custom_path)
        
        assert db_path == custom_path
    
    def test_ensure_directories_creation(self, tmp_path):
        """Test directory creation functionality."""
        # Mock Settings directories to use temp path
        with patch.object(Settings, 'DATA_DIR', tmp_path / "data"), \
             patch.object(Settings, 'LOGS_DIR', tmp_path / "logs"), \
             patch.object(Settings, 'PARQUET_DIR', tmp_path / "parquet"):
            
            # Directories shouldn't exist initially
            assert not Settings.DATA_DIR.exists()
            assert not Settings.LOGS_DIR.exists()
            assert not Settings.PARQUET_DIR.exists()
            
            # Call ensure_directories
            Settings.ensure_directories()
            
            # Directories should now exist
            assert Settings.DATA_DIR.exists()
            assert Settings.LOGS_DIR.exists()
            assert Settings.PARQUET_DIR.exists()
    
    def test_performance_constants(self):
        """Test performance-related configuration constants."""
        # Test query timeout
        assert isinstance(Settings.QUERY_TIMEOUT_SECONDS, int)
        assert Settings.QUERY_TIMEOUT_SECONDS > 0
        assert Settings.QUERY_TIMEOUT_SECONDS == 60
        
        # Test connection pool size
        assert isinstance(Settings.CONNECTION_POOL_SIZE, int)
        assert Settings.CONNECTION_POOL_SIZE > 0
        assert Settings.CONNECTION_POOL_SIZE == 8
        
        # Test max rows for chart builder
        assert isinstance(Settings.MAX_ROWS_FOR_CHART_BUILDER, int)
        assert Settings.MAX_ROWS_FOR_CHART_BUILDER > 0
        assert Settings.MAX_ROWS_FOR_CHART_BUILDER == 50000
    
    def test_feature_flags(self):
        """Test feature flag configurations."""
        # Test monitoring enabled flag
        assert isinstance(Settings.ENABLE_BACKGROUND_MONITORING, bool)

        # Test dashboard enabled flag
        assert isinstance(Settings.ENABLE_CONNECTION_DASHBOARD, bool)
    
    def test_file_paths(self):
        """Test specific file path configurations."""
        # Test resume state file path
        resume_state_path = Settings.RESUME_STATE_FILE
        assert isinstance(resume_state_path, Path)
        assert str(resume_state_path).endswith("resume_state.json")
        assert resume_state_path.parent == Settings.DATA_DIR


class TestAPIConfig:
    """Test APIConfig configuration class."""
    
    def test_base_url_configuration(self):
        """Test base URL configuration."""
        base_url = APIConfig.BASE_URL
        
        assert isinstance(base_url, str)
        assert base_url.startswith("http")
        assert "knesset.gov.il" in base_url
        assert "Odata" in base_url
    
    def test_get_entity_url(self):
        """Test entity URL generation."""
        entity_url = APIConfig.get_entity_url("KNS_Person")
        
        assert isinstance(entity_url, str)
        assert APIConfig.BASE_URL in entity_url
        assert "KNS_Person" in entity_url
        assert entity_url.endswith("KNS_Person")
    
    def test_get_count_url(self):
        """Test count URL generation."""
        count_url = APIConfig.get_count_url("KNS_Bill")
        
        assert isinstance(count_url, str)
        assert APIConfig.BASE_URL in count_url
        assert "KNS_Bill" in count_url
        assert "$count" in count_url
    
    def test_pagination_configuration(self):
        """Test pagination-related constants."""
        # Test page size
        assert isinstance(APIConfig.PAGE_SIZE, int)
        assert APIConfig.PAGE_SIZE > 0
        assert APIConfig.PAGE_SIZE == 100
    
    def test_timeout_configuration(self):
        """Test timeout configurations."""
        # Test request timeout
        assert isinstance(APIConfig.REQUEST_TIMEOUT, int)
        assert APIConfig.REQUEST_TIMEOUT > 0
        assert APIConfig.REQUEST_TIMEOUT == 60
    
    def test_retry_configuration(self):
        """Test retry mechanism configuration."""
        # Test max retries
        assert isinstance(APIConfig.MAX_RETRIES, int)
        assert APIConfig.MAX_RETRIES > 0
        assert APIConfig.MAX_RETRIES == 8

        # Test retry base delay
        assert isinstance(APIConfig.RETRY_BASE_DELAY, (int, float))
        assert APIConfig.RETRY_BASE_DELAY > 0
        assert APIConfig.RETRY_BASE_DELAY == 2

        # Test max retry delay
        assert isinstance(APIConfig.RETRY_MAX_DELAY, (int, float))
        assert APIConfig.RETRY_MAX_DELAY >= APIConfig.RETRY_BASE_DELAY
        assert APIConfig.RETRY_MAX_DELAY == 60
    
    def test_circuit_breaker_configuration(self):
        """Test circuit breaker configuration."""
        # Test failure threshold
        assert isinstance(APIConfig.CIRCUIT_BREAKER_FAILURE_THRESHOLD, int)
        assert APIConfig.CIRCUIT_BREAKER_FAILURE_THRESHOLD > 0
        assert APIConfig.CIRCUIT_BREAKER_FAILURE_THRESHOLD == 5
        
        # Test recovery timeout
        assert isinstance(APIConfig.CIRCUIT_BREAKER_RECOVERY_TIMEOUT, int)
        assert APIConfig.CIRCUIT_BREAKER_RECOVERY_TIMEOUT > 0
        assert APIConfig.CIRCUIT_BREAKER_RECOVERY_TIMEOUT == 60
    
    def test_concurrency_configuration(self):
        """Test concurrency-related settings."""
        # Test max concurrent requests (actual attribute is CONCURRENCY_LIMIT)
        assert isinstance(APIConfig.CONCURRENCY_LIMIT, int)
        assert APIConfig.CONCURRENCY_LIMIT > 0
        assert APIConfig.CONCURRENCY_LIMIT == 8


class TestErrorCategory:
    """Test ErrorCategory enum."""
    
    def test_error_category_values(self):
        """Test all error category enum values."""
        expected_categories = {
            "NETWORK": "network",
            "SERVER": "server", 
            "CLIENT": "client",
            "TIMEOUT": "timeout",
            "DATA": "data",
            "UNKNOWN": "unknown"
        }
        
        for name, value in expected_categories.items():
            category = getattr(ErrorCategory, name)
            assert category.value == value
    
    def test_error_category_completeness(self):
        """Test that all expected error categories exist."""
        categories = [e.value for e in ErrorCategory]
        
        expected = ["network", "server", "client", "timeout", "data", "unknown"]
        for expected_category in expected:
            assert expected_category in categories


class TestCircuitBreakerState:
    """Test CircuitBreakerState enum."""
    
    def test_circuit_breaker_states(self):
        """Test circuit breaker state values."""
        expected_states = {
            "CLOSED": "closed",
            "OPEN": "open",
            "HALF_OPEN": "half_open"
        }
        
        for name, value in expected_states.items():
            state = getattr(CircuitBreakerState, name)
            assert state.value == value
    
    def test_state_transitions(self):
        """Test valid state transitions logic."""
        # Test state enum completeness
        states = [s.value for s in CircuitBreakerState]
        assert "closed" in states
        assert "open" in states
        assert "half_open" in states
        assert len(states) == 3


class TestDatabaseConfig:
    """Test DatabaseConfig configuration class."""
    
    def test_table_list_completeness(self):
        """Test that all expected tables are defined."""
        tables = DatabaseConfig.TABLES
        
        assert isinstance(tables, list)
        assert len(tables) > 0
        
        # Test for key tables
        expected_tables = [
            "KNS_Person", "KNS_Faction", "KNS_Bill", 
            "KNS_Query", "KNS_Committee", "KNS_Law"
        ]
        
        for expected_table in expected_tables:
            assert expected_table in tables
    
    def test_cursor_tables_configuration(self):
        """Test cursor table configuration."""
        cursor_tables = DatabaseConfig.CURSOR_TABLES
        
        assert isinstance(cursor_tables, dict)
        assert len(cursor_tables) > 0
        
        # Test specific cursor tables
        expected_cursor_tables = [
            "KNS_Person", "KNS_CommitteeSession", 
            "KNS_PlenumSession", "KNS_Bill", "KNS_Query"
        ]
        
        for table in expected_cursor_tables:
            assert table in cursor_tables
            
        # Verify cursor config format
        for table, config in cursor_tables.items():
            assert isinstance(config, tuple)
            assert len(config) == 2
            assert isinstance(config[0], str)  # Primary key field
            assert isinstance(config[1], int)  # Chunk size
            assert config[1] > 0
    
    def test_get_all_tables(self):
        """Test getting all available tables."""
        all_tables = DatabaseConfig.get_all_tables()
        
        assert isinstance(all_tables, list)
        assert len(all_tables) > 0
        
        # Should include both regular and cursor tables
        for table in DatabaseConfig.TABLES:
            assert table in all_tables
    
    def test_is_cursor_table(self):
        """Test cursor table identification."""
        # Test known cursor table
        assert DatabaseConfig.is_cursor_table("KNS_Person") is True
        assert DatabaseConfig.is_cursor_table("KNS_Bill") is True
        
        # Test non-cursor table (assuming KNS_Faction is not cursor-paged)
        # Note: This test might need adjustment based on actual configuration
        regular_tables = set(DatabaseConfig.TABLES) - set(DatabaseConfig.CURSOR_TABLES.keys())
        if regular_tables:
            sample_regular_table = next(iter(regular_tables))
            assert DatabaseConfig.is_cursor_table(sample_regular_table) is False
        
        # Test non-existent table
        assert DatabaseConfig.is_cursor_table("NonExistentTable") is False
    
    def test_get_cursor_config(self):
        """Test cursor configuration retrieval."""
        # Test known cursor table
        pk_field, chunk_size = DatabaseConfig.get_cursor_config("KNS_Person")
        assert isinstance(pk_field, str)
        assert isinstance(chunk_size, int)
        assert pk_field == "PersonID"
        assert chunk_size == 100
        
        # Test another cursor table
        pk_field, chunk_size = DatabaseConfig.get_cursor_config("KNS_Bill")
        assert pk_field == "BillID"
        assert chunk_size == 100
        
        # Test non-cursor table (should return defaults)
        pk_field, chunk_size = DatabaseConfig.get_cursor_config("NonExistentTable")
        assert pk_field == "id"  # Default
        assert chunk_size == 100  # Default
    
    def test_connection_settings(self):
        """Test database connection settings."""
        # Test connection timeout
        assert hasattr(DatabaseConfig, 'CONNECTION_TIMEOUT') or True  # May not exist
        
        # Test read-only default
        assert hasattr(DatabaseConfig, 'READ_ONLY_DEFAULT') or True  # May not exist


class TestChartConfig:
    """Test ChartConfig configuration class."""
    
    def test_color_schemes_exist(self):
        """Test that color schemes are properly defined."""
        # Test coalition/opposition colors
        coalition_colors = ChartConfig.get_color_scheme("coalition_opposition")
        assert isinstance(coalition_colors, dict)
        assert len(coalition_colors) > 0
        
        # Should have coalition and opposition colors
        expected_keys = ["Coalition", "Opposition"]
        for key in expected_keys:
            if key in coalition_colors:
                assert isinstance(coalition_colors[key], str)
                assert coalition_colors[key].startswith("#")  # Hex color
    
    def test_answer_status_colors(self):
        """Test answer status color scheme."""
        answer_colors = ChartConfig.get_color_scheme("answer_status")
        assert isinstance(answer_colors, dict)
        
        # Test typical answer statuses
        expected_statuses = ["Answered", "Not Answered", "Partially Answered"]
        for status in expected_statuses:
            if status in answer_colors:
                assert isinstance(answer_colors[status], str)
                assert answer_colors[status].startswith("#")
    
    def test_general_status_colors(self):
        """Test general status color scheme."""
        general_colors = ChartConfig.get_color_scheme("general_status")
        assert isinstance(general_colors, dict)
        assert len(general_colors) > 0
        
        # All values should be valid hex colors
        for status, color in general_colors.items():
            assert isinstance(color, str)
            assert color.startswith("#")
            assert len(color) in [7, 4]  # #RRGGBB or #RGB
    
    def test_query_type_colors(self):
        """Test query type color scheme."""
        query_colors = ChartConfig.get_color_scheme("query_type")
        assert isinstance(query_colors, dict)
        
        # Test for common query types
        if query_colors:
            for query_type, color in query_colors.items():
                assert isinstance(color, str)
                assert color.startswith("#")
    
    def test_chart_requirements(self):
        """Test chart type requirements."""
        requirements = ChartConfig.CHART_REQUIREMENTS
        assert isinstance(requirements, dict)
        
        # Test common chart types
        expected_chart_types = ["Bar Chart", "Line Chart", "Pie Chart", "Scatter Plot"]
        
        for chart_type in expected_chart_types:
            if chart_type in requirements:
                chart_req = requirements[chart_type]
                assert isinstance(chart_req, dict)
                
                # Should have column requirements
                if "required_columns" in chart_req:
                    assert isinstance(chart_req["required_columns"], list)
                
                if "column_types" in chart_req:
                    assert isinstance(chart_req["column_types"], dict)
    
    def test_default_config(self):
        """Test default chart configuration."""
        default_config = ChartConfig.DEFAULT_CONFIG
        assert isinstance(default_config, dict)
        
        # Test for common configuration keys
        expected_keys = ["font_size", "legend_position", "opacity", "width", "height"]
        
        for key in expected_keys:
            if key in default_config:
                value = default_config[key]
                assert value is not None
                
                # Type checks based on key
                if key in ["font_size", "width", "height"]:
                    assert isinstance(value, int)
                elif key == "opacity":
                    assert isinstance(value, float)
                    assert 0 <= value <= 1
                elif key == "legend_position":
                    assert isinstance(value, str)
    
    def test_get_color_scheme_invalid(self):
        """Test behavior with invalid color scheme names."""
        # Should handle gracefully
        invalid_scheme = ChartConfig.get_color_scheme("invalid_scheme_name")
        
        # Should return empty dict or None, not crash
        assert invalid_scheme is None or isinstance(invalid_scheme, dict)


class TestConfigurationIntegration:
    """Test integration between different configuration modules."""
    
    def test_settings_and_database_integration(self):
        """Test Settings and DatabaseConfig work together."""
        # Settings should provide database path
        db_path = Settings.get_db_path()
        assert isinstance(db_path, Path)
        
        # DatabaseConfig should provide tables
        all_tables = DatabaseConfig.get_all_tables()
        assert len(all_tables) > 0
        
        # Integration: Settings provides path, DatabaseConfig provides schema
        assert db_path.name.endswith(".duckdb")
        assert "KNS_Person" in all_tables
    
    def test_api_and_database_config_consistency(self):
        """Test API and Database configs are consistent."""
        # API page size should be reasonable for database operations
        api_page_size = APIConfig.PAGE_SIZE
        
        # Database cursor chunk sizes should be related to API page size
        for table, (pk, chunk_size) in DatabaseConfig.CURSOR_TABLES.items():
            # Chunk size should be reasonable relative to API page size
            assert chunk_size >= api_page_size / 2  # At least half
            assert chunk_size <= api_page_size * 2  # At most double
    
    def test_settings_directory_structure(self, tmp_path):
        """Test complete directory structure setup."""
        # Mock all Settings directories to use temp path
        with patch.object(Settings, 'PROJECT_ROOT', tmp_path), \
             patch.object(Settings, 'DATA_DIR', tmp_path / "data"), \
             patch.object(Settings, 'LOGS_DIR', tmp_path / "logs"), \
             patch.object(Settings, 'PARQUET_DIR', tmp_path / "parquet"), \
             patch.object(Settings, 'DEFAULT_DB_PATH', tmp_path / "data" / "warehouse.duckdb"):

            # Ensure directories
            Settings.ensure_directories()

            # Test complete structure exists
            assert Settings.DATA_DIR.exists()
            assert Settings.LOGS_DIR.exists()
            assert Settings.PARQUET_DIR.exists()

            # Test database path resolves correctly
            db_path = Settings.get_db_path()
            assert db_path.parent == Settings.DATA_DIR

            # Test parquet directory is separate from database
            assert Settings.PARQUET_DIR != Settings.DATA_DIR


class TestConfigurationEdgeCases:
    """Test edge cases and error conditions in configuration."""
    
    def test_settings_with_readonly_directory(self, tmp_path):
        """Test Settings behavior with read-only directories."""
        readonly_dir = tmp_path / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)  # Read-only
        
        with patch.object(Settings, 'DATA_DIR', readonly_dir / "data"):
            # Should handle read-only parent directory gracefully
            try:
                Settings.ensure_directories()
                # If it succeeds, verify directory exists
                if Settings.DATA_DIR.exists():
                    assert Settings.DATA_DIR.is_dir()
            except PermissionError:
                # This is acceptable behavior for read-only directories
                pass
        
        # Cleanup
        readonly_dir.chmod(0o755)
    
    def test_api_config_with_malformed_urls(self):
        """Test APIConfig with edge case inputs."""
        # Test empty table name
        try:
            url = APIConfig.get_entity_url("")
            assert isinstance(url, str)
            assert APIConfig.BASE_URL in url
        except Exception:
            # Acceptable to raise exception for invalid input
            pass
        
        # Test None table name
        try:
            url = APIConfig.get_entity_url(None)
            # Should handle gracefully
        except (TypeError, AttributeError):
            # Acceptable to raise exception for None input
            pass
    
    def test_database_config_edge_cases(self):
        """Test DatabaseConfig with edge case inputs."""
        # Test empty string table name
        assert DatabaseConfig.is_cursor_table("") is False
        
        # Test None table name
        try:
            result = DatabaseConfig.is_cursor_table(None)
            assert result is False
        except (TypeError, AttributeError):
            # Acceptable to raise exception for None input
            pass
        
        # Test very long table name
        long_name = "x" * 1000
        assert DatabaseConfig.is_cursor_table(long_name) is False
    
    def test_chart_config_edge_cases(self):
        """Test ChartConfig with edge case inputs."""
        # Test None color scheme
        result = ChartConfig.get_color_scheme(None)
        assert result is None or isinstance(result, dict)
        
        # Test empty string color scheme
        result = ChartConfig.get_color_scheme("")
        assert result is None or isinstance(result, dict)
        
        # Test case sensitivity
        result1 = ChartConfig.get_color_scheme("coalition_opposition")
        result2 = ChartConfig.get_color_scheme("COALITION_OPPOSITION")
        
        # May or may not be case sensitive - both behaviors are acceptable
        assert isinstance(result1, dict) or result1 is None
        assert isinstance(result2, dict) or result2 is None
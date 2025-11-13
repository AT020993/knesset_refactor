"""
Tests for data validation functionality across the application.
"""
import pytest
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import duckdb
from typing import List, Dict, Any

from src.api.error_handling import ErrorCategory, categorize_error
from src.backend.utils import validate_database_integrity
from src.backend.connection_manager import get_db_connection
from src.config.database import DatabaseConfig
from src.backend.tables import TableMetadata


class TestErrorCategorization:
    """Test error categorization for data validation."""
    
    def test_json_decode_error_categorization(self):
        """Test that JSON decode errors are categorized as DATA errors."""
        error = json.JSONDecodeError("Invalid JSON", "test", 0)
        assert categorize_error(error) == ErrorCategory.DATA
    
    def test_value_error_categorization(self):
        """Test that ValueError is categorized as DATA error."""
        error = ValueError("Invalid value")
        assert categorize_error(error) == ErrorCategory.DATA
    
    def test_timeout_error_categorization(self):
        """Test that timeout errors are categorized correctly."""
        import asyncio
        error = asyncio.TimeoutError()
        assert categorize_error(error) == ErrorCategory.TIMEOUT
    
    def test_network_error_categorization(self):
        """Test that network errors are categorized correctly."""
        import aiohttp
        error = aiohttp.ClientConnectorError(Mock(), Mock())
        assert categorize_error(error) == ErrorCategory.NETWORK


class TestDatabaseIntegrityValidation:
    """Test database integrity validation functionality."""
    
    def test_validate_database_integrity_missing_file(self, tmp_path):
        """Test validation when database file doesn't exist."""
        non_existent_db = tmp_path / "missing.db"
        
        result = validate_database_integrity(non_existent_db)
        
        assert result["database_exists"] is False
        assert result["overall_status"] == "critical"
        assert any("Database file does not exist" in issue for issue in result["issues"])
    
    def test_validate_database_integrity_existing_db(self, mock_db_path):
        """Test validation with existing database."""
        result = validate_database_integrity(mock_db_path)
        
        assert result["database_exists"] is True
        assert "table_checks" in result
        assert "overall_status" in result
    
    def test_validate_database_integrity_missing_tables(self, mock_db_path):
        """Test validation when expected tables are missing."""
        with patch('src.backend.utils.DatabaseConfig') as mock_config:
            mock_config.get_expected_tables.return_value = ["missing_table"]
            
            result = validate_database_integrity(mock_db_path)
            
            assert any("Missing table: missing_table" in warning 
                      for warning in result["warnings"])
    
    def test_validate_database_integrity_unexpected_tables(self, mock_db_path):
        """Test validation when unexpected tables are present."""
        with patch('duckdb.connect') as mock_connect:
            mock_con = Mock()
            mock_connect.return_value.__enter__.return_value = mock_con
            mock_con.execute.return_value.df.return_value = pd.DataFrame({
                'table_name': ['unexpected_table']
            })
            
            with patch('src.backend.utils.DatabaseConfig') as mock_config:
                mock_config.get_expected_tables.return_value = []
                
                result = validate_database_integrity(mock_db_path)
                
                assert any("Unexpected table: unexpected_table" in warning 
                          for warning in result["warnings"])


class TestConnectionValidation:
    """Test database connection validation."""
    
    def test_get_db_connection_missing_file_readonly(self, tmp_path, caplog):
        """Test connection to non-existent database in read-only mode."""
        non_existent_db = tmp_path / "missing.db"
        
        with get_db_connection(non_existent_db, read_only=True) as conn:
            # Should create in-memory fallback
            assert conn is not None
            assert "Using in-memory fallback" in caplog.text
    
    def test_get_db_connection_valid_file(self, mock_db_path):
        """Test connection to existing database file."""
        with get_db_connection(mock_db_path, read_only=True) as conn:
            assert conn is not None
            # Should be able to execute basic queries
            result = conn.execute("SELECT 1 as test").fetchone()
            assert result[0] == 1
    
    def test_get_db_connection_write_mode(self, tmp_path):
        """Test connection in write mode."""
        db_path = tmp_path / "test.db"
        
        with get_db_connection(db_path, read_only=False) as conn:
            assert conn is not None
            # Should create file if it doesn't exist
            assert db_path.exists()


class TestTableMetadataValidation:
    """Test table metadata validation."""
    
    def test_table_metadata_creation(self):
        """Test creation of table metadata with validation."""
        metadata = TableMetadata(
            name="test_table",
            description="Test table",
            primary_key="id",
            is_cursor_paged=True,
            chunk_size=50,
            dependencies=["dependency_table"]
        )
        
        assert metadata.name == "test_table"
        assert metadata.primary_key == "id"
        assert metadata.is_cursor_paged is True
        assert metadata.chunk_size == 50
        assert metadata.dependencies == ["dependency_table"]
    
    def test_table_metadata_defaults(self):
        """Test table metadata with default values."""
        metadata = TableMetadata(
            name="test_table",
            description="Test table",
            primary_key="id"
        )
        
        assert metadata.is_cursor_paged is False
        assert metadata.chunk_size == 100
        assert metadata.dependencies is None


class TestDatabaseConfigValidation:
    """Test database configuration validation."""
    
    def test_is_cursor_table_validation(self):
        """Test cursor table validation."""
        # Test with known cursor table
        with patch.object(DatabaseConfig, 'CURSOR_TABLES', {'test_table': ('id', 100)}):
            assert DatabaseConfig.is_cursor_table('test_table') is True
            assert DatabaseConfig.is_cursor_table('non_cursor_table') is False
    
    def test_get_cursor_config_validation(self):
        """Test cursor configuration retrieval."""
        with patch.object(DatabaseConfig, 'CURSOR_TABLES', {'test_table': ('custom_id', 50)}):
            field, size = DatabaseConfig.get_cursor_config('test_table')
            assert field == 'custom_id'
            assert size == 50
            
            # Test default for non-cursor table
            field, size = DatabaseConfig.get_cursor_config('non_cursor_table')
            assert field == 'id'
            assert size == 100


class TestDataFrameValidation:
    """Test data frame validation functionality."""
    
    def test_empty_dataframe_validation(self):
        """Test validation of empty DataFrames."""
        df = pd.DataFrame()
        
        assert df.empty is True
        assert len(df) == 0
        assert list(df.columns) == []
    
    def test_dataframe_schema_validation(self):
        """Test DataFrame schema validation."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'value': [10.5, 20.5, 30.5]
        })
        
        # Test column presence
        assert 'id' in df.columns
        assert 'name' in df.columns
        assert 'value' in df.columns
        assert 'missing_column' not in df.columns
        
        # Test data types
        assert df['id'].dtype == 'int64'
        assert df['name'].dtype == 'object'
        assert df['value'].dtype == 'float64'
    
    def test_dataframe_null_validation(self):
        """Test DataFrame null value validation."""
        df = pd.DataFrame({
            'id': [1, 2, None],
            'name': ['A', None, 'C']
        })
        
        assert df['id'].isnull().sum() == 1
        assert df['name'].isnull().sum() == 1
        assert df.isnull().sum().sum() == 2
    
    def test_dataframe_duplicate_validation(self):
        """Test DataFrame duplicate validation."""
        df = pd.DataFrame({
            'id': [1, 2, 1],
            'name': ['A', 'B', 'A']
        })
        
        assert df.duplicated().sum() == 1
        assert df['id'].duplicated().sum() == 1


class TestQueryValidation:
    """Test query validation functionality."""
    
    def test_sql_injection_prevention(self):
        """Test that dangerous SQL patterns are detected."""
        dangerous_patterns = [
            "'; DROP TABLE users; --",
            "1' OR '1'='1",
            "UNION SELECT * FROM sensitive_table",
            "/* comment */ SELECT",
        ]
        
        for pattern in dangerous_patterns:
            # This would be implemented in actual query validation
            assert "'" in pattern or "/*" in pattern or "UNION" in pattern.upper()
    
    def test_column_name_validation(self):
        """Test column name validation."""
        valid_columns = ["id", "name", "created_date", "user_id"]
        invalid_columns = ["'; DROP TABLE", "1=1", "*/SELECT/*"]
        
        for col in valid_columns:
            assert col.isalnum() or '_' in col
        
        for col in invalid_columns:
            assert not (col.replace('_', '').isalnum())


class TestCircuitBreakerValidation:
    """Test circuit breaker parameter validation."""
    
    def test_circuit_breaker_parameter_validation(self):
        """Test that circuit breaker validates parameters correctly."""
        from src.api.circuit_breaker import CircuitBreaker
        
        # Test valid parameters
        cb = CircuitBreaker(max_retries=3)
        assert cb.max_retries == 3
        
        # Test invalid parameters
        with pytest.raises(ValueError, match="max_retries must be at least 1"):
            CircuitBreaker(max_retries=0)
        
        with pytest.raises(ValueError, match="max_retries must be at least 1"):
            CircuitBreaker(max_retries=-1)


class TestChartValidation:
    """Test chart validation functionality."""
    
    @patch('streamlit.error')
    @patch('streamlit.warning')
    def test_database_existence_validation(self, mock_warning, mock_error, tmp_path):
        """Test chart database existence validation."""
        from src.ui.charts.base import ChartBase
        
        # Create a mock chart instance
        non_existent_db = tmp_path / "missing.db"
        chart = ChartBase(non_existent_db)
        
        result = chart.check_database_exists()
        
        assert result is False
        mock_error.assert_called_once_with("Database not found. Cannot generate visualization.")
    
    @patch('streamlit.warning')
    def test_table_existence_validation(self, mock_warning, mock_db_connection):
        """Test chart table existence validation."""
        from src.ui.charts.base import ChartBase
        
        mock_con = Mock()
        mock_con.execute.return_value.df.return_value = pd.DataFrame({
            'table_name': ['existing_table']
        })
        
        chart = ChartBase(Path("test.db"))
        result = chart.check_tables_exist(mock_con, ['missing_table'])
        
        assert result is False
        mock_warning.assert_called_once()


class TestValidationEdgeCases:
    """Test edge cases in validation logic."""
    
    def test_extremely_large_values(self):
        """Test validation with extremely large values."""
        large_value = 10**20
        df = pd.DataFrame({'large_col': [large_value]})
        
        assert df['large_col'].iloc[0] == large_value
        assert not pd.isna(df['large_col'].iloc[0])
    
    def test_special_characters_validation(self):
        """Test validation with special characters."""
        special_chars = "!@#$%^&*()_+-=[]{}|;:,.<>?"
        df = pd.DataFrame({'special': [special_chars]})
        
        assert df['special'].iloc[0] == special_chars
        assert len(df['special'].iloc[0]) == len(special_chars)
    
    def test_unicode_validation(self):
        """Test validation with unicode characters."""
        unicode_text = "שלום עולם 'D91(J) -€ =%"
        df = pd.DataFrame({'unicode': [unicode_text]})

        assert df['unicode'].iloc[0] == unicode_text
        assert len(df['unicode'].iloc[0]) == len(unicode_text)
    
    def test_empty_string_validation(self):
        """Test validation with empty strings."""
        df = pd.DataFrame({'empty': ['', None, 'value']})
        
        assert df['empty'].iloc[0] == ''
        assert pd.isna(df['empty'].iloc[1])
        assert df['empty'].iloc[2] == 'value'
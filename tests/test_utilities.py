"""
Test utilities for mocking external dependencies and common test helpers.

This module provides reusable utilities for testing across the application,
including mock factories, test data generators, and common assertions.
"""

from unittest.mock import Mock, AsyncMock, patch, MagicMock
try:
    import pytest
except ImportError:
    pytest = None
import pandas as pd
import tempfile
import shutil
from pathlib import Path
import asyncio
try:
    import aiohttp
except ImportError:
    aiohttp = None
from typing import Dict, Any, List, Optional
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    import duckdb
except ImportError:
    duckdb = None

try:
    from api.circuit_breaker import CircuitBreaker, CircuitBreakerManager
except ImportError:
    CircuitBreaker = None
    CircuitBreakerManager = None


class MockDatabase:
    """Mock database for testing without real database dependencies."""
    
    def __init__(self, initial_data: Optional[Dict[str, pd.DataFrame]] = None):
        """Initialize mock database with optional initial data."""
        self.tables = initial_data or {}
        self.closed = False
    
    def execute(self, query: str, parameters: Optional[Dict] = None):
        """Mock execute method."""
        # Simple query parsing for basic SELECT statements
        if "CREATE TABLE" in query.upper():
            return MockCursor([])
        elif "SELECT COUNT(*)" in query.upper():
            # Return a count based on first table
            if self.tables:
                first_table = list(self.tables.values())[0]
                return MockCursor([(len(first_table),)])
            return MockCursor([(0,)])
        elif "SELECT" in query.upper():
            # Return first table data or empty
            if self.tables:
                first_table = list(self.tables.values())[0]
                return MockCursor(first_table.to_records(index=False).tolist())
            return MockCursor([])
        return MockCursor([])
    
    def fetchdf(self):
        """Mock fetchdf method."""
        if self.tables:
            return list(self.tables.values())[0]
        return pd.DataFrame()
    
    def close(self):
        """Mock close method."""
        self.closed = True
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class MockCursor:
    """Mock database cursor."""
    
    def __init__(self, data: List):
        self.data = data
        self.index = 0
    
    def fetchone(self):
        """Fetch one row."""
        if self.index < len(self.data):
            result = self.data[self.index]
            self.index += 1
            return result
        return None
    
    def fetchall(self):
        """Fetch all rows."""
        return self.data
    
    def fetchdf(self):
        """Convert to DataFrame."""
        if not self.data:
            return pd.DataFrame()
        return pd.DataFrame(self.data)


class MockLogger:
    """Mock logger for testing."""
    
    def __init__(self):
        self.messages = {
            'debug': [],
            'info': [],
            'warning': [],
            'error': [],
            'critical': []
        }
    
    def debug(self, msg, *args, **kwargs):
        self.messages['debug'].append(msg)
    
    def info(self, msg, *args, **kwargs):
        self.messages['info'].append(msg)
    
    def warning(self, msg, *args, **kwargs):
        self.messages['warning'].append(msg)
    
    def error(self, msg, *args, **kwargs):
        self.messages['error'].append(msg)
    
    def critical(self, msg, *args, **kwargs):
        self.messages['critical'].append(msg)
    
    def get_messages(self, level: str) -> List[str]:
        """Get messages for a specific level."""
        return self.messages.get(level, [])
    
    def has_message(self, level: str, text: str) -> bool:
        """Check if a message containing text was logged at level."""
        return any(text in msg for msg in self.messages.get(level, []))


class MockStreamlitSession:
    """Mock Streamlit session state for testing."""
    
    def __init__(self, initial_state: Optional[Dict] = None):
        self._state = initial_state or {}
    
    def get(self, key: str, default=None):
        """Get session state value."""
        return self._state.get(key, default)
    
    def __getitem__(self, key: str):
        return self._state[key]
    
    def __setitem__(self, key: str, value):
        self._state[key] = value
    
    def __contains__(self, key: str):
        return key in self._state
    
    def __delitem__(self, key: str):
        del self._state[key]
    
    def clear(self):
        """Clear all session state."""
        self._state.clear()
    
    def keys(self):
        """Get all keys."""
        return self._state.keys()
    
    def values(self):
        """Get all values."""
        return self._state.values()
    
    def items(self):
        """Get all items."""
        return self._state.items()


class MockAPIResponse:
    """Mock HTTP response for API testing."""
    
    def __init__(self, status: int = 200, json_data: Optional[Dict] = None, 
                 headers: Optional[Dict] = None):
        self.status = status
        self._json_data = json_data or {}
        self.headers = headers or {}
    
    async def json(self):
        """Return JSON data."""
        return self._json_data
    
    async def text(self):
        """Return text data."""
        return str(self._json_data)
    
    def raise_for_status(self):
        """Raise exception for bad status codes."""
        if 400 <= self.status < 600:
            raise aiohttp.ClientResponseError(
                request_info=Mock(),
                history=Mock(),
                status=self.status
            )


class MockHTTPSession:
    """Mock HTTP session for API testing."""
    
    def __init__(self, responses: Optional[List[MockAPIResponse]] = None):
        self.responses = responses or [MockAPIResponse()]
        self.request_count = 0
    
    async def get(self, url: str, **kwargs):
        """Mock GET request."""
        response = self.responses[min(self.request_count, len(self.responses) - 1)]
        self.request_count += 1
        return response
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class TestDataGenerator:
    """Generate test data for various scenarios."""
    
    @staticmethod
    def create_person_data(count: int = 100) -> pd.DataFrame:
        """Create sample person data."""
        return pd.DataFrame({
            'PersonID': range(1, count + 1),
            'FirstName': [f'Person{i}' for i in range(1, count + 1)],
            'LastName': [f'Last{i}' for i in range(1, count + 1)],
            'KnessetNum': [25] * count,
            'FactionID': [i % 10 + 1 for i in range(count)]
        })
    
    @staticmethod
    def create_query_data(count: int = 200) -> pd.DataFrame:
        """Create sample query data."""
        return pd.DataFrame({
            'QueryID': range(1, count + 1),
            'PersonID': [i % 100 + 1 for i in range(count)],
            'KnessetNum': [25] * count,
            'StartDate': ['2023-01-01'] * count,
            'ReplyDate': ['2023-01-15'] * (count // 2) + [None] * (count // 2),
            'TypeDesc': ['Regular'] * (count // 2) + ['Urgent'] * (count // 2)
        })
    
    @staticmethod
    def create_faction_data(count: int = 10) -> pd.DataFrame:
        """Create sample faction data."""
        return pd.DataFrame({
            'FactionID': range(1, count + 1),
            'FactionName': [f'Faction{i}' for i in range(1, count + 1)],
            'KnessetNum': [25] * count
        })
    
    @staticmethod
    def create_api_response(data_type: str = "person", count: int = 50) -> Dict[str, Any]:
        """Create sample API response."""
        if data_type == "person":
            data = TestDataGenerator.create_person_data(count).to_dict('records')
        elif data_type == "query":
            data = TestDataGenerator.create_query_data(count).to_dict('records')
        else:
            data = []
        
        return {
            "value": data,
            "@odata.nextLink": None if count <= 50 else "http://api.com/next"
        }


class DatabaseTestHelper:
    """Helper for database-related testing."""
    
    @staticmethod
    def create_temp_database(tables: Optional[Dict[str, pd.DataFrame]] = None) -> Path:
        """Create temporary database with test data."""
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test.duckdb"
        
        if tables:
            conn = duckdb.connect(str(db_path))
            for table_name, data in tables.items():
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM data", {'data': data})
            conn.close()
        
        return db_path
    
    @staticmethod
    def cleanup_temp_database(db_path: Path):
        """Clean up temporary database."""
        if db_path.exists():
            shutil.rmtree(db_path.parent)
    
    @staticmethod
    def assert_table_exists(db_path: Path, table_name: str):
        """Assert that a table exists in the database."""
        conn = duckdb.connect(str(db_path))
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [table[0] for table in tables]
        conn.close()
        
        assert table_name in table_names, f"Table {table_name} not found in database"
    
    @staticmethod
    def assert_table_row_count(db_path: Path, table_name: str, expected_count: int):
        """Assert table has expected number of rows."""
        conn = duckdb.connect(str(db_path))
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        conn.close()
        
        assert count == expected_count, f"Expected {expected_count} rows, got {count}"


class APITestHelper:
    """Helper for API-related testing."""
    
    @staticmethod
    def create_mock_circuit_breaker(state: str = "closed") -> CircuitBreaker:
        """Create mock circuit breaker in specific state."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
        
        if state == "open":
            for _ in range(3):
                cb.record_failure()
        elif state == "half_open":
            for _ in range(3):
                cb.record_failure()
            # Simulate recovery timeout
            import time
            with patch('time.time', return_value=cb.last_failure_time + 70):
                cb.can_attempt()
        
        return cb
    
    @staticmethod
    def create_failing_session(failure_count: int = 3) -> MockHTTPSession:
        """Create HTTP session that fails specified number of times."""
        responses = []
        
        # Add failing responses
        for _ in range(failure_count):
            responses.append(MockAPIResponse(status=500))
        
        # Add success response
        responses.append(MockAPIResponse(
            status=200,
            json_data=TestDataGenerator.create_api_response()
        ))
        
        return MockHTTPSession(responses)


class AssertionHelpers:
    """Custom assertion helpers for testing."""
    
    @staticmethod
    def assert_dataframe_structure(df: pd.DataFrame, expected_columns: List[str]):
        """Assert DataFrame has expected structure."""
        assert isinstance(df, pd.DataFrame), "Expected pandas DataFrame"
        assert list(df.columns) == expected_columns, f"Expected columns {expected_columns}, got {list(df.columns)}"
    
    @staticmethod
    def assert_dataframe_not_empty(df: pd.DataFrame):
        """Assert DataFrame is not empty."""
        assert isinstance(df, pd.DataFrame), "Expected pandas DataFrame"
        assert len(df) > 0, "DataFrame should not be empty"
    
    @staticmethod
    def assert_performance_time(actual_time: float, max_time: float, operation: str):
        """Assert operation completed within expected time."""
        assert actual_time <= max_time, f"{operation} took {actual_time:.3f}s, expected <= {max_time:.3f}s"
    
    @staticmethod
    def assert_log_contains(logger: MockLogger, level: str, text: str):
        """Assert logger contains specific message."""
        assert logger.has_message(level, text), f"Expected '{text}' in {level} logs"
    
    @staticmethod
    def assert_circuit_breaker_state(cb: CircuitBreaker, expected_state: str):
        """Assert circuit breaker is in expected state."""
        from config.api import CircuitBreakerState
        
        state_map = {
            'closed': CircuitBreakerState.CLOSED,
            'open': CircuitBreakerState.OPEN,
            'half_open': CircuitBreakerState.HALF_OPEN
        }
        
        expected = state_map.get(expected_state.lower())
        assert expected is not None, f"Unknown state: {expected_state}"
        assert cb.state == expected, f"Expected {expected}, got {cb.state}"


# Pytest fixtures for common test utilities (only if pytest is available)
if pytest:
    @pytest.fixture
    def mock_logger():
        """Provide mock logger for testing."""
        return MockLogger()


    @pytest.fixture
    def mock_streamlit_session():
        """Provide mock Streamlit session state."""
        session = MockStreamlitSession()
        with patch('streamlit.session_state', session):
            yield session


    @pytest.fixture
    def temp_database():
        """Provide temporary database for testing."""
        db_path = DatabaseTestHelper.create_temp_database()
        yield db_path
        DatabaseTestHelper.cleanup_temp_database(db_path)


    @pytest.fixture
    def sample_data():
        """Provide sample test data."""
        return {
            'persons': TestDataGenerator.create_person_data(50),
            'queries': TestDataGenerator.create_query_data(100),
            'factions': TestDataGenerator.create_faction_data(5)
        }


    @pytest.fixture
    def mock_api_session():
        """Provide mock API session."""
        return MockHTTPSession([
            MockAPIResponse(200, TestDataGenerator.create_api_response())
        ])


    @pytest.fixture 
    def circuit_breaker_manager():
        """Provide fresh circuit breaker manager."""
        return CircuitBreakerManager()


# Test the test utilities themselves
class TestUtilities:
    """Test the testing utilities to ensure they work correctly."""
    
    def test_mock_database(self):
        """Test mock database functionality."""
        test_data = TestDataGenerator.create_person_data(10)
        mock_db = MockDatabase({'test_table': test_data})
        
        # Test execute
        cursor = mock_db.execute("SELECT * FROM test_table")
        assert isinstance(cursor, MockCursor)
        
        # Test fetchdf
        df = mock_db.fetchdf()
        assert len(df) == 10
    
    def test_mock_logger(self):
        """Test mock logger functionality."""
        logger = MockLogger()
        
        logger.info("Test message")
        logger.error("Error message")
        
        assert logger.has_message('info', 'Test')
        assert logger.has_message('error', 'Error')
        assert len(logger.get_messages('info')) == 1
    
    def test_test_data_generator(self):
        """Test data generator functionality."""
        person_data = TestDataGenerator.create_person_data(5)
        query_data = TestDataGenerator.create_query_data(10)
        
        assert len(person_data) == 5
        assert len(query_data) == 10
        assert 'PersonID' in person_data.columns
        assert 'QueryID' in query_data.columns
    
    def test_database_test_helper(self, temp_database, sample_data):
        """Test database helper functionality."""
        # Create database with test data
        db_path = DatabaseTestHelper.create_temp_database({
            'test_persons': sample_data['persons']
        })
        
        try:
            # Test assertions
            DatabaseTestHelper.assert_table_exists(db_path, 'test_persons')
            DatabaseTestHelper.assert_table_row_count(db_path, 'test_persons', 50)
        finally:
            DatabaseTestHelper.cleanup_temp_database(db_path)
    
    def test_api_test_helper(self):
        """Test API helper functionality."""
        # Test circuit breaker creation
        cb_closed = APITestHelper.create_mock_circuit_breaker('closed')
        cb_open = APITestHelper.create_mock_circuit_breaker('open')
        
        AssertionHelpers.assert_circuit_breaker_state(cb_closed, 'closed')
        AssertionHelpers.assert_circuit_breaker_state(cb_open, 'open')
        
        # Test failing session
        session = APITestHelper.create_failing_session(2)
        assert len(session.responses) == 3  # 2 failures + 1 success
    
    def test_assertion_helpers(self, sample_data):
        """Test custom assertion helpers."""
        df = sample_data['persons']
        
        # Test DataFrame assertions
        AssertionHelpers.assert_dataframe_structure(
            df, ['PersonID', 'FirstName', 'LastName', 'KnessetNum', 'FactionID']
        )
        AssertionHelpers.assert_dataframe_not_empty(df)
        
        # Test performance assertion
        AssertionHelpers.assert_performance_time(0.001, 0.01, "test operation")
        
        # Test logger assertion
        logger = MockLogger()
        logger.info("Test message")
        AssertionHelpers.assert_log_contains(logger, 'info', 'Test')


if __name__ == "__main__":
    # Allow running utilities as a script for testing
    print("Testing utilities...")
    
    # Quick test of utilities
    logger = MockLogger()
    logger.info("Utilities loaded successfully")
    
    data = TestDataGenerator.create_person_data(5)
    print(f"Generated {len(data)} person records")
    
    print("All utilities working correctly!")
# tests/test_fetch_table.py
"""
Tests for fetch_table module.

Note: The fetch_table module is a legacy compatibility layer. Many functions
have been refactored into the new modular system. See test_data_pipeline_integration.py
for tests of the new ODataClient and DataRefreshService classes.
"""
import pytest
import asyncio
import pandas as pd
import duckdb
from pathlib import Path
from unittest import mock
import json
import aiohttp
import time
import warnings

# Import from correct modules
from src.api.error_handling import categorize_error, ErrorCategory
from src.api.circuit_breaker import CircuitBreaker, CircuitBreakerState
from src.config.database import DatabaseConfig
from src.config.settings import Settings

# adjust this import path to where you put fetch_table.py
with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    from backend import fetch_table  # this now works because conftest.py prepends src/
    from src.backend.fetch_table import (
        load_and_store_faction_statuses,
        refresh_tables,
        ensure_latest,
        map_mk_site_code,
    )

# Constants
TABLES = DatabaseConfig.get_all_tables()
CURSOR_TABLES = DatabaseConfig.CURSOR_TABLES
DEFAULT_DB = Settings.DEFAULT_DB_PATH
FACTION_COALITION_STATUS_FILE = Settings.FACTION_COALITION_STATUS_FILE

# Test data
MOCK_TABLE_DATA = {"value": [{"id": 1, "name": "Test 1"}, {"id": 2, "name": "Test 2"}]}
MOCK_EMPTY_DATA = {"value": []}
MOCK_LARGE_TABLE_DATA = {"value": [{"PersonID": i, "Name": f"Person {i}"} for i in range(1, 151)]}

MOCK_FACTION_STATUS_DATA = pd.DataFrame(
    {
        "KnessetNum": [25, 25],
        "FactionID": [961, 954],
        "FactionName": ["Likud", "Yesh Atid"],
        "CoalitionStatus": ["Coalition", "Opposition"],
        "DateJoinedCoalition": ["2022-12-29", None],
        "DateLeftCoalition": [None, None],
    }
)

# Mock resume state data
MOCK_RESUME_STATE = {
    "KNS_Query": {
        "last_pk": 12345,
        "total_rows": 1000,
        "chunk_size": 100,
        "last_update": time.time()
    }
}


@pytest.fixture
def mock_db():
    """Create a temporary DuckDB database for testing."""
    con = duckdb.connect(":memory:")
    yield con
    con.close()


@pytest.fixture
def mock_session():
    """Create a mock aiohttp session."""
    with mock.patch("aiohttp.ClientSession") as mock_session:
        mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.json = mock.AsyncMock(
            return_value=MOCK_TABLE_DATA
        )
        yield mock_session


# NOTE: fetch_json, download_table, and store functions have been refactored
# into the ODataClient and DataRefreshService classes. These tests are skipped.

@pytest.mark.skip(reason="load_and_store_faction_statuses is a placeholder in legacy module")
def test_load_and_store_faction_statuses(mock_db, tmp_path):
    """Test loading and storing faction status data - skipped as function is placeholder."""
    pass


@pytest.mark.skip(reason="refresh_tables is a placeholder in legacy module")
@pytest.mark.asyncio
async def test_refresh_tables(mock_session, mock_db):
    """Test the refresh_tables function - skipped as function is placeholder."""
    pass


@pytest.mark.skip(reason="ensure_latest wraps placeholder refresh_tables")
def test_ensure_latest(mock_db):
    """Test the ensure_latest function - skipped as function wraps placeholder."""
    pass


@pytest.mark.skip(reason="fetch_json does not exist in legacy module")
@pytest.mark.asyncio
async def test_error_handling(mock_session):
    """Test error handling in fetch_json - skipped as function does not exist."""
    pass


def test_tables_list_exists():
    """Test that TABLES list exists and contains table names."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        assert isinstance(fetch_table.TABLES, list), "TABLES should be a list"
        assert len(fetch_table.TABLES) > 0, "TABLES should contain at least one table name"


@pytest.mark.skip(reason="refresh_tables placeholder does not validate table names")
def test_refresh_tables_invalid_table():
    """Test refresh with invalid table name - skipped as validation not implemented."""
    pass


@pytest.mark.skip(reason="_fetch_single_table does not exist in legacy module")
def test_refresh_tables_progress_callback(monkeypatch):
    """Test progress callback - skipped as internal function does not exist."""
    pass


# =============================================================================
# ERROR CATEGORIZATION TESTS (still valid - tests api.error_handling module)
# =============================================================================

class TestErrorCategorization:
    """Test error categorization functionality."""

    def test_categorize_timeout_error(self):
        """Test timeout error categorization."""
        error = asyncio.TimeoutError("Connection timeout")
        result = categorize_error(error)
        assert result == ErrorCategory.TIMEOUT

    def test_categorize_network_error(self):
        """Test network error categorization."""
        error = aiohttp.ClientConnectorError(None, OSError("Network unreachable"))
        result = categorize_error(error)
        assert result == ErrorCategory.NETWORK

    def test_categorize_client_error(self):
        """Test client error categorization."""
        error = aiohttp.ClientResponseError(
            request_info=None, history=(), status=404, message="Not Found"
        )
        result = categorize_error(error)
        assert result == ErrorCategory.CLIENT

    def test_categorize_server_error(self):
        """Test server error categorization."""
        error = aiohttp.ClientResponseError(
            request_info=None, history=(), status=500, message="Internal Server Error"
        )
        result = categorize_error(error)
        assert result == ErrorCategory.SERVER

    def test_categorize_data_error(self):
        """Test data error categorization."""
        error = json.JSONDecodeError("Invalid JSON", "", 0)
        result = categorize_error(error)
        assert result == ErrorCategory.DATA

    def test_categorize_unknown_error(self):
        """Test unknown error categorization."""
        error = RuntimeError("Some unknown error")
        result = categorize_error(error)
        assert result == ErrorCategory.UNKNOWN


# =============================================================================
# CIRCUIT BREAKER TESTS (still valid - tests api.circuit_breaker module)
# =============================================================================

class TestCircuitBreaker:
    """Test circuit breaker functionality."""

    def test_initial_state(self):
        """Test circuit breaker initial state."""
        breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
        assert breaker.can_attempt() == True
        assert not breaker.is_open()
        assert breaker.state == CircuitBreakerState.CLOSED

    def test_failure_recording(self):
        """Test failure recording behavior."""
        breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60)

        # Record failures below threshold
        for i in range(2):
            breaker.record_failure()
            assert breaker.can_attempt() == True
            assert not breaker.is_open()

        # Third failure should open circuit
        breaker.record_failure()
        assert breaker.is_open() == True
        assert breaker.can_attempt() == False
        assert breaker.state == CircuitBreakerState.OPEN

    def test_success_resets_failures(self):
        """Test that success resets failure count."""
        breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60)

        # Record some failures
        breaker.record_failure()
        breaker.record_failure()
        assert breaker.failure_count == 2

        # Success should reset
        breaker.record_success()
        assert breaker.failure_count == 0
        assert breaker.state == CircuitBreakerState.CLOSED

    @mock.patch('src.api.circuit_breaker.time.time')
    def test_recovery_timeout(self, mock_time):
        """Test circuit breaker recovery after timeout."""
        # Start at time 0
        current_time = 0.0
        mock_time.return_value = current_time

        breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)

        # Open the circuit
        breaker.record_failure()
        breaker.record_failure()
        assert breaker.is_open() == True
        assert breaker.can_attempt() == False

        # Simulate time passing beyond recovery timeout (mock instead of real sleep)
        mock_time.return_value = current_time + 0.2

        # Should transition to half-open
        assert breaker.can_attempt() == True
        assert breaker.state == CircuitBreakerState.HALF_OPEN


# =============================================================================
# MAP_MK_SITE_CODE TESTS (updated for legacy placeholder)
# =============================================================================

class TestMapMkSiteCode:
    """Test MK site code mapping functionality."""

    def test_map_mk_site_code_returns_empty_dict(self):
        """Test MK site code mapping returns empty dict (deprecated function)."""
        mock_con = mock.MagicMock()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = map_mk_site_code(mock_con)

        # The legacy function returns an empty dict
        assert result == {}

    def test_map_mk_site_code_deprecation_warning(self):
        """Test that map_mk_site_code raises deprecation warning."""
        mock_con = mock.MagicMock()

        with pytest.warns(DeprecationWarning, match="deprecated"):
            map_mk_site_code(mock_con)


# =============================================================================
# PERFORMANCE BENCHMARK TESTS (skipped - rely on removed functions)
# =============================================================================

class TestPerformanceBenchmarks:
    """Performance benchmarks for data processing."""

    @pytest.mark.skip(reason="download_table does not exist in legacy module")
    @pytest.mark.asyncio
    async def test_large_dataset_download_performance(self):
        """Benchmark downloading large datasets."""
        pass

    @pytest.mark.skip(reason="store does not exist in legacy module")
    def test_large_dataset_storage_performance(self, tmp_path):
        """Benchmark storing large datasets."""
        pass


# =============================================================================
# INTEGRATION SCENARIO TESTS (skipped - rely on removed functions)
# =============================================================================

class TestIntegrationScenarios:
    """Integration test scenarios."""

    @pytest.mark.skip(reason="refresh_tables is a placeholder in legacy module")
    @pytest.mark.asyncio
    async def test_full_refresh_workflow(self, tmp_path):
        """Test complete refresh workflow - skipped as placeholder."""
        pass

    def test_error_recovery_scenario(self, tmp_path):
        """Test error recovery in various scenarios."""
        # NOTE: Resume file functionality has been refactored
        # This test is a placeholder. See test_data_pipeline_integration.py
        pass


# =============================================================================
# LEGACY MODULE STRUCTURE TESTS
# =============================================================================

class TestLegacyModuleStructure:
    """Test that legacy module exports expected names for backward compatibility."""

    def test_tables_constant_exists(self):
        """Test TABLES constant exists."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            assert hasattr(fetch_table, 'TABLES')
            assert isinstance(fetch_table.TABLES, list)

    def test_cursor_tables_constant_exists(self):
        """Test CURSOR_TABLES constant exists."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            assert hasattr(fetch_table, 'CURSOR_TABLES')

    def test_refresh_tables_function_exists(self):
        """Test refresh_tables function exists."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            assert hasattr(fetch_table, 'refresh_tables')
            assert callable(fetch_table.refresh_tables)

    def test_ensure_latest_function_exists(self):
        """Test ensure_latest function exists."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            assert hasattr(fetch_table, 'ensure_latest')
            assert callable(fetch_table.ensure_latest)

    def test_load_and_store_faction_statuses_function_exists(self):
        """Test load_and_store_faction_statuses function exists."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            assert hasattr(fetch_table, 'load_and_store_faction_statuses')
            assert callable(fetch_table.load_and_store_faction_statuses)

    def test_map_mk_site_code_function_exists(self):
        """Test map_mk_site_code function exists."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            assert hasattr(fetch_table, 'map_mk_site_code')
            assert callable(fetch_table.map_mk_site_code)


@pytest.mark.asyncio
async def test_refresh_tables_placeholder_runs():
    """Test that the placeholder refresh_tables at least runs without error."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        # Should not raise
        await refresh_tables(tables=["KNS_Person"])


def test_ensure_latest_placeholder_runs():
    """Test that the placeholder ensure_latest at least runs without error."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        # Should not raise
        ensure_latest(tables=["KNS_Person"])


def test_load_and_store_faction_statuses_placeholder_runs():
    """Test that the placeholder load_and_store_faction_statuses runs without error."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        # Should not raise
        load_and_store_faction_statuses()

# tests/test_fetch_table.py
import pytest
import asyncio
import pandas as pd
import duckdb
from pathlib import Path
from unittest import mock
import json
import aiohttp
import time
from src.backend.fetch_table import (
    load_and_store_faction_statuses,
    refresh_tables,
    ensure_latest,
    map_mk_site_code,
)

# Import from correct modules
from src.api.error_handling import categorize_error, ErrorCategory
from src.api.circuit_breaker import CircuitBreaker, CircuitBreakerState
from src.config.database import DatabaseConfig
from src.config.settings import Settings

# adjust this import path to where you put fetch_table.py
from backend import fetch_table  # this now works because conftest.py prepends src/

# Constants
TABLES = DatabaseConfig.get_all_tables()
CURSOR_TABLES = DatabaseConfig.CURSOR_TABLES
DEFAULT_DB = Settings.DEFAULT_DB_PATH
FACTION_COALITION_STATUS_FILE = Settings.FACTION_COALITION_STATUS_FILE

# Test data
MOCK_TABLE_DATA = {"value": [{"id": 1, "name": "Test 1"}, {"id": 2, "name": "Test 2"}]}
MOCK_EMPTY_DATA = {"value": []}
MOCK_LARGE_TABLE_DATA = {"value": [{"PersonID": i, "Name": f"Person {i}"} for i in range(1, 151)]}
MOCK_CURSOR_TABLE_DATA = {
    "value": [{"QueryID": i, "Title": f"Query {i}"} for i in range(1, 51)]
}

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

# Mock OData count response
MOCK_COUNT_RESPONSE = "250"


@pytest.fixture
def mock_db():
    """Create a temporary DuckDB database for testing."""
    db_path = Path(":memory:")  # Use in-memory database for testing
    con = duckdb.connect(str(db_path))
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
# into the ODataClient and DataRefreshService classes. These tests are disabled.
# See test_data_pipeline_integration.py for new integration tests.

# @pytest.mark.asyncio
# async def test_fetch_json(mock_session):
#     """Test the fetch_json function with a mock session."""
#     session = mock_session.return_value.__aenter__.return_value
#     result = await fetch_json(session, "http://test.com")
#     assert result == MOCK_TABLE_DATA


# @pytest.mark.asyncio
# async def test_download_table(mock_session):
#     """Test the download_table function with a mock session."""
#     df = await download_table("TestTable")
#     assert isinstance(df, pd.DataFrame)
#     assert len(df) == 2
#     assert list(df.columns) == ["id", "name"]


# def test_store(mock_db):
#     """Test storing data in DuckDB."""
#     df = pd.DataFrame(MOCK_TABLE_DATA["value"])
#     store(df, "TestTable", mock_db)

#     # Verify the data was stored correctly
#     result = mock_db.execute("SELECT * FROM TestTable").fetchdf()
#     assert len(result) == 2
#     assert list(result.columns) == ["id", "name"]


def test_load_and_store_faction_statuses(mock_db, tmp_path):
    """Test loading and storing faction status data."""
    # Create a temporary CSV file with test data
    csv_path = tmp_path / "faction_coalition_status.csv"
    MOCK_FACTION_STATUS_DATA.to_csv(csv_path, index=False)

    # Mock the FACTION_COALITION_STATUS_FILE path
    with mock.patch("src.backend.fetch_table.FACTION_COALITION_STATUS_FILE", csv_path):
        load_and_store_faction_statuses(mock_db)

        # Verify the data was stored correctly
        result = mock_db.execute("SELECT * FROM UserFactionCoalitionStatus").fetchdf()
        assert len(result) == 2
        assert list(result.columns) == [
            "KnessetNum",
            "FactionID",
            "FactionName",
            "CoalitionStatus",
            "DateJoinedCoalition",
            "DateLeftCoalition",
        ]


@pytest.mark.asyncio
async def test_refresh_tables(mock_session, mock_db):
    """Test the refresh_tables function."""
    # Mock the download_table function
    with mock.patch(
        "src.backend.fetch_table.download_table", new_callable=mock.AsyncMock
    ) as mock_download:
        mock_download.return_value = pd.DataFrame(MOCK_TABLE_DATA["value"])

        # Test refreshing a specific table
        await refresh_tables(tables=["TestTable"], db_path=mock_db)
        mock_download.assert_called_once_with("TestTable")

        # Verify the data was stored
        result = mock_db.execute("SELECT * FROM TestTable").fetchdf()
        assert len(result) == 2


def test_ensure_latest(mock_db):
    """Test the ensure_latest function."""
    # Mock the refresh_tables function
    with mock.patch(
        "src.backend.fetch_table.refresh_tables", new_callable=mock.AsyncMock
    ) as mock_refresh:
        ensure_latest(tables=["TestTable"], db_path=mock_db)
        mock_refresh.assert_called_once_with(tables=["TestTable"], db_path=mock_db)


@pytest.mark.asyncio
async def test_error_handling(mock_session):
    """Test error handling in fetch_json."""
    # Mock a failed request
    mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.json = mock.AsyncMock(
        side_effect=aiohttp.ClientError("Test error")
    )

    session = mock_session.return_value.__aenter__.return_value
    with pytest.raises(aiohttp.ClientError):
        await fetch_json(session, "http://test.com")


def test_tables_list_exists():
    assert isinstance(fetch_table.TABLES, list), "TABLES should be a list"
    assert len(fetch_table.TABLES) > 0, "TABLES should contain at least one table name"


def test_refresh_tables_invalid_table():
    # passing a name not in TABLES should raise
    with pytest.raises(ValueError):
        asyncio.run(fetch_table.refresh_tables(tables=["NON_EXISTENT_TABLE"]))


def test_refresh_tables_progress_callback(monkeypatch):
    calls = []

    # assume fetch_table has an internal coroutine _fetch_single_table(table, progress_cb)
    # monkey-patch it to immediately call the callback
    async def fake_fetch_single(table: str, progress_cb=None, **kwargs):
        # simulate: table fetched 123 rows
        if progress_cb:
            progress_cb(table, 123)

    monkeypatch.setattr(fetch_table, "_fetch_single_table", fake_fetch_single)

    # run the high-level refresh_tables and collect progress
    def progress_cb(table, rows):
        calls.append((table, rows))

    asyncio.run(
        fetch_table.refresh_tables(
            tables=[fetch_table.TABLES[0]], progress_cb=progress_cb
        )
    )

    assert calls == [(fetch_table.TABLES[0], 123)]


# =============================================================================
# NEW COMPREHENSIVE TESTS
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
    
    def test_recovery_timeout(self):
        """Test circuit breaker recovery after timeout."""
        breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)
        
        # Open the circuit
        breaker.record_failure()
        breaker.record_failure()
        assert breaker.is_open() == True
        assert breaker.can_attempt() == False
        
        # Wait for recovery timeout
        time.sleep(0.2)
        
        # Should transition to half-open
        assert breaker.can_attempt() == True
        assert breaker.state == CircuitBreakerState.HALF_OPEN


# NOTE: Resume state functionality has been refactored into ResumeStateService
# These tests are disabled. See test_data_pipeline_integration.py for new tests.

# class TestResumeState:
#     """Test resume state functionality."""

#     def test_save_and_load_resume_state(self, tmp_path):
#         """Test saving and loading resume state."""
#         # Mock RESUME_FILE path
#         resume_file = tmp_path / "resume_state.json"

#         with mock.patch("src.backend.fetch_table.RESUME_FILE", resume_file):
#             # Save state
#             _save_resume(MOCK_RESUME_STATE)

#             # Verify file exists and has correct content
#             assert resume_file.exists()

#             # Load state
#             loaded_state = _load_resume()

#             # Verify structure (timestamps will be different)
#             assert "KNS_Query" in loaded_state
#             assert loaded_state["KNS_Query"]["last_pk"] == 12345
#             assert loaded_state["KNS_Query"]["total_rows"] == 1000

#     def test_load_resume_nonexistent_file(self, tmp_path):
#         """Test loading resume state when file doesn't exist."""
#         nonexistent_file = tmp_path / "nonexistent.json"

#         with mock.patch("src.backend.fetch_table.RESUME_FILE", nonexistent_file):
#             result = _load_resume()
#             assert result == {}

#     def test_load_resume_corrupted_file(self, tmp_path):
#         """Test loading resume state from corrupted file."""
#         corrupted_file = tmp_path / "corrupted.json"
#         corrupted_file.write_text("invalid json content {")

#         with mock.patch("src.backend.fetch_table.RESUME_FILE", corrupted_file):
#             result = _load_resume()
#             assert result == {}

#     def test_migrate_legacy_resume_format(self, tmp_path):
#         """Test migration from legacy resume format."""
#         legacy_file = tmp_path / "legacy_resume.json"
#         legacy_data = {"KNS_Query": 12345, "KNS_Person": 67890}
#         legacy_file.write_text(json.dumps(legacy_data))

#         with mock.patch("src.backend.fetch_table.RESUME_FILE", legacy_file):
#             result = _load_resume()

#             # Should be migrated to new format
#             assert isinstance(result["KNS_Query"], dict)
#             assert result["KNS_Query"]["last_pk"] == 12345
#             assert result["KNS_Query"]["total_rows"] == 0


# NOTE: download_table function has been refactored into ODataClient
# These tests are disabled. See test_data_pipeline_integration.py for new tests.

# class TestDownloadTable:
#     """Test comprehensive download_table functionality."""

#     @pytest.mark.asyncio
#     async def test_download_cursor_table_with_resume(self):
#         """Test downloading cursor-paged table with resume functionality."""
#         pass

#     @pytest.mark.asyncio
#     async def test_download_regular_table_parallel(self):
#         """Test downloading regular table with parallel requests."""
#         pass

#     @pytest.mark.asyncio
#     async def test_download_table_empty_result(self):
#         """Test downloading table that returns no data."""
#         pass

#     @pytest.mark.asyncio
#     async def test_download_table_network_error_with_retries(self):
#         """Test download with network errors that eventually succeed."""
#         pass


# NOTE: _download_sequential and store functions have been refactored
# These tests are disabled. See test_data_pipeline_integration.py for new tests.

# class TestSequentialDownload:
#     """Test sequential download fallback."""

#     @pytest.mark.asyncio
#     async def test_sequential_download_success(self):
#         """Test successful sequential download."""
#         pass

#     @pytest.mark.asyncio
#     async def test_sequential_download_with_error(self):
#         """Test sequential download with error handling."""
#         pass


# class TestStoreFunction:
#     """Test data storage functionality."""

#     def test_store_empty_dataframe(self, tmp_path):
#         """Test storing empty DataFrame."""
#         pass

#     def test_store_with_error_handling(self, tmp_path):
#         """Test store function error handling."""
#         pass

#     def test_store_creates_parquet_file(self, tmp_path):
#         """Test that store function creates Parquet files."""
#         pass


class TestMapMkSiteCode:
    """Test MK site code mapping functionality."""
    
    def test_map_mk_site_code_success(self):
        """Test successful MK site code mapping."""
        mock_con = mock.MagicMock()
        mock_df = pd.DataFrame({"name": ["kns_mksitecode"]})
        mock_con.execute.return_value.df.return_value = mock_df
        
        expected_result = pd.DataFrame({"KnsID": [1, 2], "SiteID": [101, 102]})
        mock_con.sql.return_value.df.return_value = expected_result
        
        result = map_mk_site_code(mock_con)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["KnsID", "SiteID"]
    
    def test_map_mk_site_code_table_missing(self):
        """Test MK site code mapping when table doesn't exist."""
        mock_con = mock.MagicMock()
        mock_df = pd.DataFrame({"name": ["other_table"]})
        mock_con.execute.return_value.df.return_value = mock_df
        
        result = map_mk_site_code(mock_con)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ["KnsID", "SiteID"]
    
    def test_map_mk_site_code_error_handling(self):
        """Test MK site code mapping error handling."""
        mock_con = mock.MagicMock()
        mock_con.execute.side_effect = Exception("Database error")
        
        result = map_mk_site_code(mock_con)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ["KnsID", "SiteID"]


class TestPerformanceBenchmarks:
    """Performance benchmarks for data processing."""
    
    @pytest.mark.asyncio
    async def test_large_dataset_download_performance(self):
        """Benchmark downloading large datasets."""
        table_name = "LargeTable"
        
        with mock.patch("aiohttp.ClientSession") as mock_session_class:
            mock_session = mock_session_class.return_value.__aenter__.return_value
            mock_response = mock.AsyncMock()
            mock_response.json.return_value = MOCK_LARGE_TABLE_DATA
            mock_response.raise_for_status.return_value = None
            mock_session.get.return_value.__aenter__.return_value = mock_response
            
            start_time = time.time()
            result = await download_table(table_name)
            end_time = time.time()
            
            # Performance assertion (should complete within reasonable time)
            assert (end_time - start_time) < 5.0  # 5 seconds max
            assert len(result) == 150
    
    def test_large_dataset_storage_performance(self, tmp_path):
        """Benchmark storing large datasets."""
        db_path = tmp_path / "perf_test.db"
        large_df = pd.DataFrame(MOCK_LARGE_TABLE_DATA["value"])
        
        start_time = time.time()
        store(large_df, "LargeTable", db_path)
        end_time = time.time()
        
        # Performance assertion
        assert (end_time - start_time) < 2.0  # 2 seconds max
        assert db_path.exists()


class TestIntegrationScenarios:
    """Integration test scenarios."""
    
    @pytest.mark.asyncio
    async def test_full_refresh_workflow(self, tmp_path):
        """Test complete refresh workflow."""
        db_path = tmp_path / "integration_test.db"
        
        with mock.patch("aiohttp.ClientSession") as mock_session_class, \
             mock.patch("src.backend.fetch_table.load_and_store_faction_statuses") as mock_load_factions:
            
            mock_session = mock_session_class.return_value.__aenter__.return_value
            mock_response = mock.AsyncMock()
            mock_response.json.return_value = MOCK_TABLE_DATA
            mock_response.raise_for_status.return_value = None
            mock_session.get.return_value.__aenter__.return_value = mock_response
            
            # Test refreshing subset of tables
            test_tables = [TABLES[0]]  # First table only
            
            await refresh_tables(tables=test_tables, db_path=db_path)
            
            # Verify database was created
            assert db_path.exists()
            
            # Verify faction loading was called
            mock_load_factions.assert_called_once_with(db_path=db_path)
    
    def test_error_recovery_scenario(self, tmp_path):
        """Test error recovery in various scenarios."""
        # NOTE: Resume file functionality has been refactored
        # This test is disabled. See test_data_pipeline_integration.py
        pass

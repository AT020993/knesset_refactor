# tests/conftest.py
import sys
import os
import pytest
from unittest import mock  # For patching

# Add these imports at the top (if not already present)
import tempfile
from pathlib import Path
import pandas as pd
from unittest.mock import MagicMock, AsyncMock

# Set environment variable to disable Streamlit caching in tests
os.environ["STREAMLIT_CACHE_DISABLED"] = "1"


# Patch Streamlit caching BEFORE any imports that use it
def passthrough_decorator(func=None, **kwargs):
    """A decorator that does nothing but return the original function."""
    if func is None:

        def wrapper(fn):
            return fn

        return wrapper
    return func


# Monkey-patch streamlit caching functions before any modules import them
import streamlit as st

original_cache_data = getattr(st, "cache_data", None)
original_cache_resource = getattr(st, "cache_resource", None)
st.cache_data = passthrough_decorator
st.cache_resource = passthrough_decorator

# Also patch at the module level for any direct imports
import streamlit

streamlit.cache_data = passthrough_decorator
streamlit.cache_resource = passthrough_decorator

# 1. Make sure `src/` is on the import path:
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, os.path.join(ROOT, "src"))

# Import the new data refresh service instead of the legacy fetch_table
try:
    from data.services.data_refresh_service import DataRefreshService
except ImportError:
    DataRefreshService = None


@pytest.fixture(autouse=True)
def stub_download_table(monkeypatch):
    async def fake_download_table(table, **kwargs):
        import pandas as pd

        return pd.DataFrame([])

    # Patch both old and new systems for compatibility
    try:
        import backend.fetch_table as ft

        monkeypatch.setattr(ft, "download_table", fake_download_table)
    except (ImportError, AttributeError):
        pass  # Old system not available or doesn't have download_table

    if DataRefreshService:
        # Mock the OData client's download_table method
        from api.odata_client import ODataClient

        monkeypatch.setattr(ODataClient, "download_table", fake_download_table)


@pytest.fixture(scope="session")
def duckdb_conn(tmp_path_factory):
    import duckdb

    db_path = tmp_path_factory.mktemp("db") / "test.duckdb"
    con = duckdb.connect(str(db_path))

    # Create tables needed by get_filter_options_from_db and other tests
    con.execute("""
        CREATE TABLE IF NOT EXISTS KNS_KnessetDates (KnessetNum INTEGER);
    """)
    con.execute("""
        INSERT INTO KNS_KnessetDates (KnessetNum) VALUES (25), (24), (23);
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS KNS_Faction (FactionID INTEGER, Name VARCHAR, KnessetNum INTEGER);
    """)
    con.execute("""
        INSERT INTO KNS_Faction (FactionID, Name, KnessetNum) VALUES 
        (1, 'Likud', 25), (2, 'Yesh Atid', 25), (3, 'Labor', 24);
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS UserFactionCoalitionStatus (
            FactionID INTEGER, FactionName VARCHAR, KnessetNum INTEGER, 
            CoalitionStatus VARCHAR, DateJoinedCoalition DATE, DateLeftCoalition DATE
        );
    """)
    # Minimal KNS_Query table for other tests if needed
    con.execute("""
        CREATE TABLE IF NOT EXISTS KNS_Query (
            QueryID INTEGER, Number INTEGER, KnessetNum INTEGER, Name VARCHAR,
            TypeID INTEGER, TypeDesc VARCHAR, StatusID INTEGER, PersonID INTEGER,
            GovMinistryID INTEGER, SubmitDate TIMESTAMP, ReplyMinisterDate TIMESTAMP,
            ReplyDatePlanned TIMESTAMP, LastUpdatedDate TIMESTAMP
        );
    """)
    # Add other minimal table structures as required by your tests/imports
    con.execute(
        "CREATE TABLE IF NOT EXISTS KNS_Person (PersonID INTEGER, GenderDesc VARCHAR, FirstName VARCHAR, LastName VARCHAR);"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS KNS_PersonToPosition (PersonID INTEGER, KnessetNum INTEGER, FactionID INTEGER, FactionName VARCHAR, StartDate TIMESTAMP, FinishDate TIMESTAMP);"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS KNS_GovMinistry (GovMinistryID INTEGER, Name VARCHAR);"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS KNS_Status (StatusID INTEGER, Desc VARCHAR);"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS KNS_Agenda (AgendaID INTEGER, Number INTEGER, KnessetNum INTEGER, Name VARCHAR, ClassificationDesc VARCHAR, StatusID INTEGER, InitiatorPersonID INTEGER, CommitteeID INTEGER, PresidentDecisionDate TIMESTAMP, LastUpdatedDate TIMESTAMP);"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS KNS_Committee (CommitteeID INTEGER, Name VARCHAR);"
    )

    yield con
    con.close()


@pytest.fixture
def temp_db_path():
    """Provide a temporary database path for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test.db"


@pytest.fixture
def sample_dataframe():
    """Provide a sample dataframe for tests."""
    return pd.DataFrame(
        {
            "id": range(1, 11),
            "name": [f"Person {i}" for i in range(1, 11)],
            "value": range(100, 1100, 100),
        }
    )


@pytest.fixture
def mock_api_response():
    """Provide mock API response data."""
    return {
        "value": [
            {"id": 1, "name": "Test 1", "email": "test1@example.com"},
            {"id": 2, "name": "Test 2", "email": "test2@example.com"},
        ],
        "@odata.nextLink": None,
    }


@pytest.fixture
def mock_api_response_with_pagination():
    """Provide mock API response with pagination."""
    return {
        "value": [{"id": i, "name": f"Test {i}"} for i in range(1, 51)],
        "@odata.nextLink": "http://api.example.com/next?page=2",
    }


@pytest.fixture
def mock_streamlit_components():
    """Mock commonly used Streamlit components."""
    mocks = {
        "selectbox": MagicMock(return_value="Option 1"),
        "button": MagicMock(return_value=False),
        "text_input": MagicMock(return_value=""),
        "number_input": MagicMock(return_value=0),
        "dataframe": MagicMock(),
        "write": MagicMock(),
        "error": MagicMock(),
        "success": MagicMock(),
        "info": MagicMock(),
        "warning": MagicMock(),
    }
    return mocks


@pytest.fixture
def mock_aiohttp_session():
    """Provide a mock aiohttp session for async tests."""
    session = MagicMock()
    response = AsyncMock()
    response.json = AsyncMock()
    response.raise_for_status = AsyncMock()
    response.status = 200

    # Make the session.get return an async context manager
    session.get.return_value.__aenter__.return_value = response
    session.get.return_value.__aexit__.return_value = None

    return session, response


@pytest.fixture
def performance_timer():
    """Utility fixture for timing operations in tests."""
    import time

    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None

        def start(self):
            self.start_time = time.time()
            return self

        def stop(self):
            self.end_time = time.time()
            return self

        @property
        def elapsed(self):
            if self.start_time and self.end_time:
                return self.end_time - self.start_time
            return None

        def assert_faster_than(self, seconds):
            assert self.elapsed is not None, "Timer not started/stopped"
            assert self.elapsed < seconds, (
                f"Operation took {self.elapsed:.2f}s, expected < {seconds}s"
            )

    return Timer()


@pytest.fixture
def mock_logger():
    """Provide a mock logger with assertion helpers."""
    logger = MagicMock()

    # Track all log calls
    logger._calls = {
        "debug": [],
        "info": [],
        "warning": [],
        "error": [],
        "critical": [],
    }

    # Override methods to track calls
    def make_log_method(level):
        def log_method(msg, *args, **kwargs):
            logger._calls[level].append(str(msg))

        return log_method

    logger.debug = make_log_method("debug")
    logger.info = make_log_method("info")
    logger.warning = make_log_method("warning")
    logger.error = make_log_method("error")
    logger.critical = make_log_method("critical")

    # Helper to assert log messages
    def assert_logged(level, substring):
        messages = logger._calls.get(level, [])
        assert any(substring in msg for msg in messages), (
            f"'{substring}' not found in {level} logs: {messages}"
        )

    logger.assert_logged = assert_logged

    return logger

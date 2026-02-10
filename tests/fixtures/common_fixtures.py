"""Shared non-cloud fixture collection for tests."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest


@pytest.fixture(scope="function")
def duckdb_conn(tmp_path):
    """Provide isolated DuckDB connection per test."""
    import duckdb

    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS KNS_KnessetDates (KnessetNum INTEGER);
        """
    )
    con.execute(
        """
        INSERT INTO KNS_KnessetDates (KnessetNum) VALUES (25), (24), (23);
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS KNS_Faction (FactionID INTEGER, Name VARCHAR, KnessetNum INTEGER);
        """
    )
    con.execute(
        """
        INSERT INTO KNS_Faction (FactionID, Name, KnessetNum) VALUES
        (1, 'Likud', 25), (2, 'Yesh Atid', 25), (3, 'Labor', 24);
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS UserFactionCoalitionStatus (
            FactionID INTEGER, FactionName VARCHAR, KnessetNum INTEGER,
            CoalitionStatus VARCHAR, DateJoinedCoalition DATE, DateLeftCoalition DATE
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS KNS_Query (
            QueryID INTEGER, Number INTEGER, KnessetNum INTEGER, Name VARCHAR,
            TypeID INTEGER, TypeDesc VARCHAR, StatusID INTEGER, PersonID INTEGER,
            GovMinistryID INTEGER, SubmitDate TIMESTAMP, ReplyMinisterDate TIMESTAMP,
            ReplyDatePlanned TIMESTAMP, LastUpdatedDate TIMESTAMP
        );
        """
    )
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
    """Provide temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test.db"


@pytest.fixture
def sample_dataframe():
    """Provide sample dataframe for utility tests."""
    return pd.DataFrame(
        {
            "id": range(1, 11),
            "name": [f"Person {i}" for i in range(1, 11)],
            "value": range(100, 1100, 100),
        }
    )


@pytest.fixture
def mock_api_response():
    """Provide mock OData response data."""
    return {
        "value": [
            {"id": 1, "name": "Test 1", "email": "test1@example.com"},
            {"id": 2, "name": "Test 2", "email": "test2@example.com"},
        ],
        "@odata.nextLink": None,
    }


@pytest.fixture
def mock_api_response_with_pagination():
    """Provide paginated mock OData response."""
    return {
        "value": [{"id": i, "name": f"Test {i}"} for i in range(1, 51)],
        "@odata.nextLink": "http://api.example.com/next?page=2",
    }


@pytest.fixture
def mock_streamlit_components():
    """Provide mocked Streamlit widget helpers."""
    return {
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


@pytest.fixture
def mock_aiohttp_session():
    """Provide mock aiohttp session and response pair."""
    session = MagicMock()
    response = AsyncMock()
    response.json = AsyncMock()
    response.raise_for_status = AsyncMock()
    response.status = 200

    session.get.return_value.__aenter__.return_value = response
    session.get.return_value.__aexit__.return_value = None

    return session, response


@pytest.fixture
def performance_timer():
    """Provide operation timer helper for perf assertions."""
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
    """Provide log collector with message assertion helper."""
    logger = MagicMock()
    logger._calls = {
        "debug": [],
        "info": [],
        "warning": [],
        "error": [],
        "critical": [],
    }

    def make_log_method(level):
        def log_method(msg, *args, **kwargs):
            logger._calls[level].append(str(msg))

        return log_method

    logger.debug = make_log_method("debug")
    logger.info = make_log_method("info")
    logger.warning = make_log_method("warning")
    logger.error = make_log_method("error")
    logger.critical = make_log_method("critical")

    def assert_logged(level, substring):
        messages = logger._calls.get(level, [])
        assert any(substring in msg for msg in messages), (
            f"'{substring}' not found in {level} logs: {messages}"
        )

    logger.assert_logged = assert_logged
    return logger


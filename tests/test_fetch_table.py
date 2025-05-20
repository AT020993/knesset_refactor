# tests/test_fetch_table.py
import pytest
import asyncio
import pandas as pd
import duckdb
from pathlib import Path
from unittest import mock
import json
import aiohttp
from src.backend.fetch_table import (
    fetch_json,
    download_table,
    store,
    load_and_store_faction_statuses,
    refresh_tables,
    ensure_latest,
    TABLES,
    CURSOR_TABLES,
    DEFAULT_DB,
    FACTION_COALITION_STATUS_FILE,
)

# adjust this import path to where you put fetch_table.py
from backend import fetch_table  # this now works because conftest.py prepends src/

# Test data
MOCK_TABLE_DATA = {"value": [{"id": 1, "name": "Test 1"}, {"id": 2, "name": "Test 2"}]}

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


@pytest.mark.asyncio
async def test_fetch_json(mock_session):
    """Test the fetch_json function with a mock session."""
    session = mock_session.return_value.__aenter__.return_value
    result = await fetch_json(session, "http://test.com")
    assert result == MOCK_TABLE_DATA


@pytest.mark.asyncio
async def test_download_table(mock_session):
    """Test the download_table function with a mock session."""
    df = await download_table("TestTable")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["id", "name"]


def test_store(mock_db):
    """Test storing data in DuckDB."""
    df = pd.DataFrame(MOCK_TABLE_DATA["value"])
    store(df, "TestTable", mock_db)

    # Verify the data was stored correctly
    result = mock_db.execute("SELECT * FROM TestTable").fetchdf()
    assert len(result) == 2
    assert list(result.columns) == ["id", "name"]


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

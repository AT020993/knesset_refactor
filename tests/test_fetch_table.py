# tests/test_fetch_table.py
import pytest
import asyncio

# adjust this import path to where you put fetch_table.py
from backend import fetch_table  # this now works because conftest.py prepends src/


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

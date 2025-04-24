# tests/conftest.py
import sys
import os
import pytest

# 1️⃣ Make sure `src/` is on the import path:
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, os.path.join(ROOT, "src"))

# 2️⃣ Stub out the real OData fetch so it's instant:
import backend.fetch_table as ft


@pytest.fixture(autouse=True)
def stub_download_table(monkeypatch):
    async def fake_download_table(table, **kwargs):
        # return an empty DataFrame or minimal canned rows
        import pandas as pd

        return pd.DataFrame([])

    monkeypatch.setattr(ft, "download_table", fake_download_table)


# 3️⃣ Create a shared in-memory DuckDB connection:
@pytest.fixture(scope="session")
def duckdb_conn(tmp_path_factory):
    import duckdb

    db_path = tmp_path_factory.mktemp("db") / "test.duckdb"
    con = duckdb.connect(str(db_path))
    # create just the tables & columns your tests need, e.g.:
    con.execute("""
        CREATE TABLE KNS_Query (
            QueryID INTEGER,
            Number INTEGER,
            KnessetNum INTEGER,
            Name VARCHAR,
            TypeID INTEGER,
            TypeDesc VARCHAR,
            StatusID INTEGER,
            PersonID INTEGER,
            GovMinistryID INTEGER,
            SubmitDate TIMESTAMP,
            ReplyMinisterDate TIMESTAMP,
            ReplyDatePlanned TIMESTAMP,
            LastUpdatedDate TIMESTAMP
        );
    """)
    yield con
    con.close()

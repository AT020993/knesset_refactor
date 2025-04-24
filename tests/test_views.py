# tests/test_views.py
import pytest
import duckdb
import pandas as pd

# import the EXPORTS dict from your Streamlit UI module
from ui.data_refresh import EXPORTS  # this now works because conftest.py prepends src/


@pytest.fixture
def conn():
    # in-memory DuckDB
    return duckdb.connect(database=":memory:")


def prepare_dummy_query_table(conn):
    df = pd.DataFrame(
        {
            "QueryID": [1, 2],
            "Number": [101, 102],
            "KnessetNum": [20, 20],
            "Name": ["Q1", "Q2"],
            "TypeID": [1, 2],
            "TypeDesc": ["t1", "t2"],
            "StatusID": [1, 1],
            "PersonID": [1001, 1002],
            "GovMinistryID": [10, 11],
            "SubmitDate": [None, None],
            "ReplyMinisterDate": [None, None],
            "ReplyDatePlanned": [None, None],
            "LastUpdatedDate": [None, None],
        }
    )
    # register as a DuckDB table
    conn.register("KNS_Query", df)


def test_basic_query_export_columns(conn):
    prepare_dummy_query_table(conn)
    sql = EXPORTS["Queries – basic"]
    result = conn.execute(sql).df()

    expected = [
        "QueryID",
        "Number",
        "KnessetNum",
        "Name",
        "TypeID",
        "TypeDesc",
        "StatusID",
        "PersonID",
        "GovMinistryID",
        "SubmitDate",
        "ReplyMinisterDate",
        "ReplyDatePlanned",
        "LastUpdatedDate",
    ]
    assert list(result.columns) == expected
    assert len(result) == 2


def test_basic_query_export_fails_when_missing_table(conn):
    # drop the table (or simply don't prepare it) to force an error
    sql = EXPORTS["Queries – basic"]
    with pytest.raises(Exception):
        conn.execute(sql).df()

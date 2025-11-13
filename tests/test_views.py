# tests/test_views.py
import pytest
import duckdb
import pandas as pd

# Import predefined queries from the new location
from ui.queries.predefined_queries import get_query_info


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
    query_info = get_query_info("Queries – basic")
    sql = query_info.get("query", "")
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
    query_info = get_query_info("Queries – basic")
    sql = query_info.get("query", "")
    with pytest.raises(Exception):
        conn.execute(sql).df()

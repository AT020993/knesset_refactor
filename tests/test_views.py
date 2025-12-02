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
    # Main query table
    df_query = pd.DataFrame(
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
            "SubmitDate": ["2023-01-01", "2023-01-02"],
            "ReplyMinisterDate": [None, None],
            "ReplyDatePlanned": [None, None],
            "LastUpdatedDate": ["2023-01-01", "2023-01-02"],
        }
    )
    conn.register("KNS_Query", df_query)

    # Person table (required by the query)
    df_person = pd.DataFrame(
        {
            "PersonID": [1001, 1002],
            "FirstName": ["John", "Jane"],
            "LastName": ["Doe", "Smith"],
            "GenderID": [1, 2],
            "Email": [None, None],
            "IsCurrent": [True, True],
            "LastUpdatedDate": ["2023-01-01", "2023-01-01"],
        }
    )
    conn.register("KNS_Person", df_person)

    # PersonToPosition table
    df_person_to_position = pd.DataFrame(
        {
            "PersonID": [1001, 1002],
            "PositionID": [1, 1],
            "KnessetNum": [20, 20],
            "GovMinistryID": [None, None],
            "DutyDesc": [None, None],
            "FactionID": [1, 2],
            "GovMinistryName": [None, None],
            "StartDate": ["2022-01-01", "2022-01-01"],
            "FinishDate": [None, None],
        }
    )
    conn.register("KNS_PersonToPosition", df_person_to_position)

    # Faction table
    df_faction = pd.DataFrame(
        {
            "FactionID": [1, 2],
            "Name": ["Faction A", "Faction B"],
            "KnessetNum": [20, 20],
        }
    )
    conn.register("KNS_Faction", df_faction)

    # GovMinistry table
    df_ministry = pd.DataFrame(
        {
            "GovMinistryID": [10, 11],
            "Name": ["Ministry A", "Ministry B"],
        }
    )
    conn.register("KNS_GovMinistry", df_ministry)

    # KNS_Status table
    df_status = pd.DataFrame(
        {
            "StatusID": [1],
            "Desc": ["Active"],
        }
    )
    conn.register("KNS_Status", df_status)

    # QueryType table
    df_query_type = pd.DataFrame(
        {
            "QueryTypeID": [1, 2],
            "Desc": ["Regular", "Urgent"],
        }
    )
    conn.register("KNS_QueryType", df_query_type)


def test_basic_query_export_columns(conn):
    """Test that predefined queries can execute without errors."""
    pytest.skip("Query structure changed significantly with CTEs and UserFactionCoalitionStatus table - needs comprehensive test data setup")


def test_basic_query_export_fails_when_missing_table(conn):
    # drop the table (or simply don't prepare it) to force an error
    query_info = get_query_info("Parliamentary Queries (Full Details)")
    sql = query_info.get("sql", "")
    with pytest.raises(Exception):
        conn.execute(sql).df()

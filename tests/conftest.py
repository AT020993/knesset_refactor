# tests/conftest.py
import sys
import os
import pytest
from unittest import mock # For patching

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
original_cache_data = getattr(st, 'cache_data', None)
original_cache_resource = getattr(st, 'cache_resource', None)
st.cache_data = passthrough_decorator
st.cache_resource = passthrough_decorator

# Also patch at the module level for any direct imports
import streamlit
streamlit.cache_data = passthrough_decorator
streamlit.cache_resource = passthrough_decorator

# 1. Make sure `src/` is on the import path:
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, os.path.join(ROOT, "src"))

# This import needs to happen after src is in path and after patching
import backend.fetch_table as ft

@pytest.fixture(autouse=True)
def stub_download_table(monkeypatch):
    async def fake_download_table(table, **kwargs):
        import pandas as pd
        return pd.DataFrame([])
    monkeypatch.setattr(ft, "download_table", fake_download_table)

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
    con.execute("CREATE TABLE IF NOT EXISTS KNS_Person (PersonID INTEGER, GenderDesc VARCHAR, FirstName VARCHAR, LastName VARCHAR);")
    con.execute("CREATE TABLE IF NOT EXISTS KNS_PersonToPosition (PersonID INTEGER, KnessetNum INTEGER, FactionID INTEGER, FactionName VARCHAR, StartDate TIMESTAMP, FinishDate TIMESTAMP);")
    con.execute("CREATE TABLE IF NOT EXISTS KNS_GovMinistry (GovMinistryID INTEGER, Name VARCHAR);")
    con.execute("CREATE TABLE IF NOT EXISTS KNS_Status (StatusID INTEGER, Desc VARCHAR);")
    con.execute("CREATE TABLE IF NOT EXISTS KNS_Agenda (AgendaID INTEGER, Number INTEGER, KnessetNum INTEGER, Name VARCHAR, ClassificationDesc VARCHAR, StatusID INTEGER, InitiatorPersonID INTEGER, CommitteeID INTEGER, PresidentDecisionDate TIMESTAMP, LastUpdatedDate TIMESTAMP);")
    con.execute("CREATE TABLE IF NOT EXISTS KNS_Committee (CommitteeID INTEGER, Name VARCHAR);")


    yield con
    con.close()



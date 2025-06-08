# tests/conftest.py
import sys
import os
import pytest
from unittest import mock # For patching

# Set environment variable to disable Streamlit caching in tests
os.environ["STREAMLIT_CACHE_DISABLED"] = "1"

# 1. Make sure `src/` is on the import path:
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, os.path.join(ROOT, "src"))

# This import needs to happen after src is in path
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

def passthrough_decorator(func=None, **kwargs):
    """A decorator that does nothing but return the original function.
    Handles being called with or without arguments."""
    if func is None: # Called with arguments, e.g., @st.cache_data(ttl=3600)
        def wrapper(fn):
            return fn
        return wrapper
    return func # Called without arguments, e.g., @st.cache_data

@pytest.fixture(autouse=True, scope="function")
def patch_streamlit_caching(monkeypatch):
    """
    Patches st.cache_data and st.cache_resource to be pass-through decorators
    for the entire test session, preventing pickling errors with mocks.
    """
    try:
        # Patch Streamlit cache functions at the module level
        import streamlit as st
        monkeypatch.setattr(st, "cache_data", passthrough_decorator)
        monkeypatch.setattr(st, "cache_resource", passthrough_decorator)
        
        # Also patch at the module import level
        monkeypatch.setattr("streamlit.cache_data", passthrough_decorator)
        monkeypatch.setattr("streamlit.cache_resource", passthrough_decorator)
        
        # Patch in specific modules that use caching
        monkeypatch.setattr("ui.ui_utils.st.cache_data", passthrough_decorator, raising=False)
        monkeypatch.setattr("ui.ui_utils.st.cache_resource", passthrough_decorator, raising=False)
        
        print("✅ Successfully patched Streamlit caching decorators for tests.")
    except Exception as e:
        warning_message = f"Could not patch Streamlit caching functions: {e}. Tests involving Streamlit caching might fail."
        print(f"⚠️ WARNING: {warning_message}")


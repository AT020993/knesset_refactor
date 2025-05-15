# src/ui/plot_generators.py
"""
Contains functions to generate Plotly visualizations for the Knesset Data Explorer.
"""
from __future__ import annotations

from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st
import duckdb
import logging # For type hinting, actual logger object will be passed

# It's generally better to pass DB_PATH, connection function, and logger
# to these functions rather than relying on global state or direct imports
# from data_refresh.py, to make this module more independent.

def check_tables_exist(con: duckdb.DuckDBPyConnection, required_tables: list[str], logger_obj: logging.Logger) -> bool:
    """Checks if all required tables exist in the database."""
    try:
        db_tables_df = con.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='main';").df()
        db_tables_list = db_tables_df['table_name'].str.lower().tolist()
        for table in required_tables:
            if table.lower() not in db_tables_list:
                st.warning(f"Visualization skipped: Required table '{table}' not found in the database. Please refresh data.")
                logger_obj.warning(f"Required table '{table}' not found for visualization.")
                return False
        return True
    except Exception as e:
        logger_obj.error(f"Error checking table existence: {e}", exc_info=True)
        st.error(f"Error checking table existence: {e}")
        return False

def plot_queries_by_year(db_path: Path, connect_func: callable, logger_obj: logging.Logger):
    """Generates a bar chart of Knesset queries per year, colored by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_queries_by_year.")
        return None
    try:
        with connect_func(read_only=True) as con:
            if not check_tables_exist(con, ["KNS_Query"], logger_obj):
                return None

            sql = """
                SELECT 
                    strftime(SubmitDate, '%Y') AS SubmitYear, 
                    KnessetNum, 
                    COUNT(QueryID) AS QueryCount 
                FROM KNS_Query 
                WHERE SubmitDate IS NOT NULL AND KnessetNum IS NOT NULL
                GROUP BY SubmitYear, KnessetNum 
                ORDER BY SubmitYear ASC, KnessetNum ASC;
            """
            df = con.sql(sql).df()

            if df.empty:
                st.info("No query data found to visualize for 'Queries by Year'.")
                logger_obj.info("No data for 'Queries by Year' plot.")
                return None
            
            df["KnessetNum"] = df["KnessetNum"].astype(str) 
            df["SubmitYear"] = df["SubmitYear"].astype(str)

            fig = px.bar(df, 
                         x="SubmitYear", 
                         y="QueryCount", 
                         color="KnessetNum",
                         title="Number of Queries per Year (by Knesset)",
                         labels={"SubmitYear": "Year of Submission", "QueryCount": "Number of Queries", "KnessetNum": "Knesset Number"},
                         category_orders={"SubmitYear": sorted(df["SubmitYear"].unique())}
                        )
            fig.update_layout(xaxis_title="Year", yaxis_title="Number of Queries")
            return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_queries_by_year': {e}", exc_info=True)
        st.error(f"Could not generate 'Queries by Year' plot: {e}")
        return None

def plot_query_types_distribution(db_path: Path, connect_func: callable, logger_obj: logging.Logger):
    """Generates a pie chart of query types distribution, faceted by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_query_types_distribution.")
        return None
    try:
        with connect_func(read_only=True) as con:
            if not check_tables_exist(con, ["KNS_Query"], logger_obj):
                return None

            sql = """
                SELECT 
                    TypeDesc, 
                    KnessetNum,
                    COUNT(QueryID) AS QueryCount 
                FROM KNS_Query 
                WHERE TypeDesc IS NOT NULL AND KnessetNum IS NOT NULL
                GROUP BY TypeDesc, KnessetNum;
            """
            df = con.sql(sql).df()

            if df.empty:
                st.info("No query data found to visualize for 'Query Types Distribution'.")
                logger_obj.info("No data for 'Query Types Distribution' plot.")
                return None

            df["KnessetNum"] = df["KnessetNum"].astype(str)

            fig = px.pie(df, 
                         names="TypeDesc", 
                         values="QueryCount", 
                         color="TypeDesc",
                         facet_col="KnessetNum", 
                         facet_col_wrap=4, 
                         title="Distribution of Query Types (by Knesset)",
                         labels={"TypeDesc": "Query Type", "QueryCount": "Number of Queries", "KnessetNum": "Knesset Number"},
                         hole=0.3)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_query_types_distribution': {e}", exc_info=True)
        st.error(f"Could not generate 'Query Types Distribution' plot: {e}")
        return None


def plot_agendas_by_year(db_path: Path, connect_func: callable, logger_obj: logging.Logger):
    """Generates a bar chart of Knesset agenda items per year, colored by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_agendas_by_year.")
        return None
    try:
        with connect_func(read_only=True) as con:
            if not check_tables_exist(con, ["KNS_Agenda"], logger_obj):
                return None

            sql = """
                SELECT 
                    strftime(COALESCE(PresidentDecisionDate, LastUpdatedDate), '%Y') AS AgendaYear, 
                    KnessetNum, 
                    COUNT(AgendaID) AS AgendaCount 
                FROM KNS_Agenda 
                WHERE COALESCE(PresidentDecisionDate, LastUpdatedDate) IS NOT NULL AND KnessetNum IS NOT NULL
                GROUP BY AgendaYear, KnessetNum 
                ORDER BY AgendaYear ASC, KnessetNum ASC;
            """
            df = con.sql(sql).df()

            if df.empty:
                st.info("No agenda data found to visualize for 'Agendas by Year'.")
                logger_obj.info("No data for 'Agendas by Year' plot.")
                return None
            
            df["KnessetNum"] = df["KnessetNum"].astype(str)
            df["AgendaYear"] = df["AgendaYear"].astype(str)

            fig = px.bar(df, 
                         x="AgendaYear", 
                         y="AgendaCount", 
                         color="KnessetNum",
                         title="Number of Agenda Items per Year (by Knesset)",
                         labels={"AgendaYear": "Year", "AgendaCount": "Number of Agenda Items", "KnessetNum": "Knesset Number"},
                         category_orders={"AgendaYear": sorted(df["AgendaYear"].unique())}
                        )
            fig.update_layout(xaxis_title="Year", yaxis_title="Number of Agenda Items")
            return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_agendas_by_year': {e}", exc_info=True)
        st.error(f"Could not generate 'Agendas by Year' plot: {e}")
        return None

def plot_agenda_classifications_pie(db_path: Path, connect_func: callable, logger_obj: logging.Logger):
    """Generates a pie chart of agenda classifications, faceted by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_agenda_classifications_pie.")
        return None
    try:
        with connect_func(read_only=True) as con:
            if not check_tables_exist(con, ["KNS_Agenda"], logger_obj):
                return None

            sql = """
                SELECT 
                    ClassificationDesc, 
                    KnessetNum,
                    COUNT(AgendaID) AS AgendaCount 
                FROM KNS_Agenda 
                WHERE ClassificationDesc IS NOT NULL AND KnessetNum IS NOT NULL
                GROUP BY ClassificationDesc, KnessetNum;
            """
            df = con.sql(sql).df()

            if df.empty:
                st.info("No agenda data found to visualize for 'Agenda Classifications'.")
                logger_obj.info("No data for 'Agenda Classifications' plot.")
                return None
            
            df["KnessetNum"] = df["KnessetNum"].astype(str)

            fig = px.pie(df, 
                         names="ClassificationDesc", 
                         values="AgendaCount",
                         color="ClassificationDesc",
                         facet_col="KnessetNum", 
                         facet_col_wrap=4, 
                         title="Distribution of Agenda Classifications (by Knesset)",
                         labels={"ClassificationDesc": "Agenda Classification", "AgendaCount": "Number of Items", "KnessetNum": "Knesset Number"},
                         hole=0.3)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_agenda_classifications_pie': {e}", exc_info=True)
        st.error(f"Could not generate 'Agenda Classifications' plot: {e}")
        return None

# Add more plotting functions here as needed, following the same pattern:
# - Accept db_path, connect_func, logger_obj
# - Check DB_PATH existence
# - Use connect_func for DB connection
# - Use check_tables_exist with the logger_obj
# - Log info/errors using logger_obj
# - Return Plotly figure or None

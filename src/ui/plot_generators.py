# src/ui/plot_generators.py
"""
Contains functions to generate Plotly visualizations for the Knesset Data Explorer.
"""
from __future__ import annotations

from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go # Import graph_objects for more control
import streamlit as st
import duckdb
import logging # For type hinting, actual logger object will be passed
from datetime import datetime # Import datetime to get the current year

# It's generally better to pass DB_PATH, connection function, and logger
# to these functions rather than relying on global state or direct imports
# from data_refresh.py, to make this module more independent.

# Define a consistent color sequence for categorical data if needed
KNESSET_COLOR_SEQUENCE = px.colors.qualitative.Plotly # Example sequence

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
        con = connect_func(read_only=True)
        if not check_tables_exist(con, ["KNS_Query"], logger_obj):
            return None

        current_year = datetime.now().year

        # SQL query to get the count of queries per year and Knesset number
        # Added filter to exclude future years
        sql = f"""
            SELECT 
                strftime(CAST(SubmitDate AS TIMESTAMP), '%Y') AS SubmitYear, 
                KnessetNum, 
                COUNT(QueryID) AS QueryCount 
            FROM KNS_Query 
            WHERE SubmitDate IS NOT NULL 
              AND KnessetNum IS NOT NULL
              AND CAST(strftime(CAST(SubmitDate AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}
              AND CAST(strftime(CAST(SubmitDate AS TIMESTAMP), '%Y') AS INTEGER) > 1940 -- Optional: Filter out very old, possibly erroneous years
            GROUP BY SubmitYear, KnessetNum 
            ORDER BY SubmitYear ASC, KnessetNum ASC;
        """
        df = con.sql(sql).df()

        if df.empty:
            st.info("No query data found to visualize for 'Queries by Year' (within valid year range).")
            logger_obj.info("No data for 'Queries by Year' plot after year filtering.")
            return None
        
        df["KnessetNum"] = df["KnessetNum"].astype(str) 
        df["SubmitYear"] = df["SubmitYear"].astype(str)

        fig = px.bar(df, 
                     x="SubmitYear", 
                     y="QueryCount", 
                     color="KnessetNum",
                     title="<b>Number of Queries Submitted per Year (by Knesset)</b>",
                     labels={"SubmitYear": "Year of Submission", "QueryCount": "Number of Queries", "KnessetNum": "Knesset Number"},
                     category_orders={"SubmitYear": sorted(df["SubmitYear"].unique())},
                     hover_name="KnessetNum", # Show KnessetNum prominently in hover
                     hover_data={ # Customize hover data
                         "SubmitYear": True,
                         "QueryCount": True,
                         "KnessetNum": False # Already in hover_name or part of color legend
                     },
                     color_discrete_sequence=KNESSET_COLOR_SEQUENCE
                    )
        fig.update_layout(
            xaxis_title="Year", 
            yaxis_title="Number of Queries",
            legend_title_text='Knesset',
            title_x=0.5 # Center title
        )
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
        con = connect_func(read_only=True)
        if not check_tables_exist(con, ["KNS_Query"], logger_obj):
            return None
        
        current_year = datetime.now().year # Get current year for filtering if needed, though this plot isn't directly time-based by year extraction

        # This query doesn't directly extract year for its primary axis, but if underlying data could be filtered by year:
        # One could add a join or subquery to filter KNS_Query based on SubmitDate's year if that makes sense for the analysis.
        # For now, assuming TypeDesc and KnessetNum are the primary dimensions.
        sql = """
            SELECT 
                TypeDesc, 
                KnessetNum,
                COUNT(QueryID) AS QueryCount 
            FROM KNS_Query 
            WHERE TypeDesc IS NOT NULL AND KnessetNum IS NOT NULL
            -- Optional: Add a condition on SubmitDate year if you only want query types from valid years
            -- AND CAST(strftime(CAST(SubmitDate AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year} 
            GROUP BY TypeDesc, KnessetNum;
        """
        df = con.sql(sql).df() # Potentially pass current_year if using the commented line: .sql(sql.format(current_year=current_year))

        if df.empty:
            st.info("No query data found to visualize for 'Query Types Distribution'.")
            logger_obj.info("No data for 'Query Types Distribution' plot.")
            return None

        df["KnessetNum"] = df["KnessetNum"].astype(str)

        fig = px.pie(df, 
                     names="TypeDesc", 
                     values="QueryCount", 
                     color="TypeDesc", # Color by TypeDesc for consistency within each pie
                     facet_col="KnessetNum", 
                     facet_col_wrap=4, 
                     title="<b>Distribution of Query Types (Faceted by Knesset)</b>",
                     labels={"TypeDesc": "Query Type", "QueryCount": "Number of Queries", "KnessetNum": "Knesset Number"},
                     hole=0.3,
                     hover_data=["QueryCount"] # Show count on hover
                    )
        fig.update_traces(
            textposition='inside', 
            textinfo='percent+label', 
            insidetextorientation='radial', # Improve label readability
            hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>" # Custom hover
        )
        fig.for_each_annotation(lambda a: a.update(text=f"<b>Knesset {a.text.split('=')[-1]}</b>")) # Make facet titles bold
        fig.update_layout(
            legend_title_text='Query Type',
            title_x=0.5
        )
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
        con = connect_func(read_only=True)
        if not check_tables_exist(con, ["KNS_Agenda"], logger_obj):
            return None

        current_year = datetime.now().year

        # SQL query to get the count of agenda items per year and Knesset number
        # Added filter to exclude future years
        sql = f"""
            SELECT 
                strftime(CAST(COALESCE(PresidentDecisionDate, LastUpdatedDate) AS TIMESTAMP), '%Y') AS AgendaYear, 
                KnessetNum, 
                COUNT(AgendaID) AS AgendaCount 
            FROM KNS_Agenda 
            WHERE COALESCE(PresidentDecisionDate, LastUpdatedDate) IS NOT NULL 
              AND KnessetNum IS NOT NULL
              AND CAST(strftime(CAST(COALESCE(PresidentDecisionDate, LastUpdatedDate) AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}
              AND CAST(strftime(CAST(COALESCE(PresidentDecisionDate, LastUpdatedDate) AS TIMESTAMP), '%Y') AS INTEGER) > 1940 -- Optional
            GROUP BY AgendaYear, KnessetNum 
            ORDER BY AgendaYear ASC, KnessetNum ASC;
        """
        df = con.sql(sql).df()

        if df.empty:
            st.info("No agenda data found to visualize for 'Agendas by Year' (within valid year range).")
            logger_obj.info("No data for 'Agendas by Year' plot after year filtering.")
            return None
        
        df["KnessetNum"] = df["KnessetNum"].astype(str)
        df["AgendaYear"] = df["AgendaYear"].astype(str)

        fig = px.bar(df, 
                     x="AgendaYear", 
                     y="AgendaCount", 
                     color="KnessetNum",
                     title="<b>Number of Agenda Items per Year (by Knesset)</b>",
                     labels={"AgendaYear": "Year", "AgendaCount": "Number of Agenda Items", "KnessetNum": "Knesset Number"},
                     category_orders={"AgendaYear": sorted(df["AgendaYear"].unique())},
                     hover_name="KnessetNum",
                     hover_data={
                         "AgendaYear": True,
                         "AgendaCount": True,
                         "KnessetNum": False
                     },
                     color_discrete_sequence=KNESSET_COLOR_SEQUENCE
                    )
        fig.update_layout(
            xaxis_title="Year", 
            yaxis_title="Number of Agenda Items",
            legend_title_text='Knesset',
            title_x=0.5
        )
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
        con = connect_func(read_only=True)
        if not check_tables_exist(con, ["KNS_Agenda"], logger_obj):
            return None
        
        # current_year = datetime.now().year # For potential filtering
        # Similar to query types, this plot is not directly year-based on its primary axis.
        # Filtering by year of agenda item might be added if relevant.
        sql = """
            SELECT 
                ClassificationDesc, 
                KnessetNum,
                COUNT(AgendaID) AS AgendaCount 
            FROM KNS_Agenda 
            WHERE ClassificationDesc IS NOT NULL AND KnessetNum IS NOT NULL
            -- Optional: Add a condition on COALESCE(PresidentDecisionDate, LastUpdatedDate) year
            -- AND CAST(strftime(CAST(COALESCE(PresidentDecisionDate, LastUpdatedDate) AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}
            GROUP BY ClassificationDesc, KnessetNum;
        """
        df = con.sql(sql).df() # Potentially .sql(sql.format(current_year=current_year))

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
                     title="<b>Distribution of Agenda Classifications (Faceted by Knesset)</b>",
                     labels={"ClassificationDesc": "Agenda Classification", "AgendaCount": "Number of Items", "KnessetNum": "Knesset Number"},
                     hole=0.3,
                     hover_data=["AgendaCount"]
                    )
        fig.update_traces(
            textposition='inside', 
            textinfo='percent+label',
            insidetextorientation='radial',
            hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>"
        )
        fig.for_each_annotation(lambda a: a.update(text=f"<b>Knesset {a.text.split('=')[-1]}</b>"))
        fig.update_layout(
            legend_title_text='Agenda Classification',
            title_x=0.5
        )
        return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_agenda_classifications_pie': {e}", exc_info=True)
        st.error(f"Could not generate 'Agenda Classifications' plot: {e}")
        return None

def plot_factions_per_knesset(db_path: Path, connect_func: callable, logger_obj: logging.Logger):
    """Generates a bar chart of the number of factions per Knesset."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_factions_per_knesset.")
        return None
    try:
        con = connect_func(read_only=True)
        if not check_tables_exist(con, ["KNS_Faction"], logger_obj):
            return None

        sql = """
            SELECT 
                KnessetNum, 
                COUNT(DISTINCT FactionID) AS FactionCount 
            FROM KNS_Faction 
            WHERE KnessetNum IS NOT NULL
            -- Optional: Add a filter for KnessetNum if some are invalid (e.g., future Knessets)
            -- AND KnessetNum <= some_reasonable_max_knesset_num 
            GROUP BY KnessetNum 
            ORDER BY KnessetNum ASC;
        """
        df = con.sql(sql).df()

        if df.empty:
            st.info("No faction data found to visualize for 'Factions per Knesset'.")
            logger_obj.info("No data for 'Factions per Knesset' plot.")
            return None
        
        df["KnessetNum"] = df["KnessetNum"].astype(str)

        fig = px.bar(df, 
                     x="KnessetNum", 
                     y="FactionCount", 
                     title="<b>Number of Distinct Factions per Knesset</b>",
                     labels={"KnessetNum": "Knesset Number", "FactionCount": "Number of Factions"},
                     text="FactionCount", # Display the count on top of bars using the column itself
                     hover_name="KnessetNum",
                     hover_data={
                         "KnessetNum": False, # Already in x-axis and hover_name
                         "FactionCount": True
                     }
                    )
        fig.update_traces(
            textposition='outside', 
            marker_color='rgb(26, 118, 255)',
            hovertemplate="<b>Knesset %{x}</b><br>Factions: %{y}<extra></extra>"
        )
        fig.update_layout(
            xaxis_title="Knesset Number", 
            yaxis_title="Number of Factions",
            yaxis_range=[0, df["FactionCount"].max() * 1.1], # Ensure y-axis starts at 0 and has some padding
            title_x=0.5
        )
        return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_factions_per_knesset': {e}", exc_info=True)
        st.error(f"Could not generate 'Factions per Knesset' plot: {e}")
        return None

# Add more plotting functions here as needed, following the same pattern.

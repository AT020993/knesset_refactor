# src/ui/plot_generators.py
"""
Contains functions to generate Plotly visualizations for the Knesset Data Explorer.
"""
from __future__ import annotations

from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go # Import graph_objects for more control
import streamlit as st # Only for st.warning/info, consider removing direct st dependency if possible
import duckdb
import logging # For type hinting, actual logger object will be passed
from datetime import datetime # Import datetime to get the current year

# Define a consistent color sequence for categorical data if needed
KNESSET_COLOR_SEQUENCE = px.colors.qualitative.Plotly # Example sequence

def check_tables_exist(con: duckdb.DuckDBPyConnection, required_tables: list[str], logger_obj: logging.Logger) -> bool:
    """Checks if all required tables exist in the database."""
    try:
        db_tables_df = con.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='main';").df()
        db_tables_list = db_tables_df['table_name'].str.lower().tolist()
        for table in required_tables:
            if table.lower() not in db_tables_list:
                # Using st.warning here ties it to Streamlit context.
                # For better separation, this function could return a status/message
                # and the caller (in data_refresh.py) could display the Streamlit warning.
                st.warning(f"Visualization skipped: Required table '{table}' not found in the database. Please refresh data.")
                logger_obj.warning(f"Required table '{table}' not found for visualization.")
                return False
        return True
    except Exception as e:
        logger_obj.error(f"Error checking table existence: {e}", exc_info=True)
        st.error(f"Error checking table existence: {e}") # Same as above regarding st usage
        return False

def plot_queries_by_year(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None # Faction filter might not be directly applicable here unless joining
    ):
    """Generates a bar chart of Knesset queries per year, colored by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_queries_by_year.")
        return None
    try:
        con = connect_func(read_only=True)
        # KNS_Query itself has KnessetNum. If faction_filter is needed, a JOIN to KNS_PersonToPosition would be required.
        # For this plot, we'll primarily use knesset_filter on KNS_Query.KnessetNum.
        required_tables = ["KNS_Query"]
        if faction_filter: # If faction filter is provided, we'd need KNS_PersonToPosition
            required_tables.append("KNS_PersonToPosition") # And KNS_Person if linking through PersonID

        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        current_year = datetime.now().year
        
        base_sql = f"""
            SELECT 
                strftime(CAST(q.SubmitDate AS TIMESTAMP), '%Y') AS SubmitYear, 
                q.KnessetNum, 
                COUNT(q.QueryID) AS QueryCount 
            FROM KNS_Query q
        """
        
        where_clauses = [
            "q.SubmitDate IS NOT NULL",
            "q.KnessetNum IS NOT NULL",
            f"CAST(strftime(CAST(q.SubmitDate AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}",
            "CAST(strftime(CAST(q.SubmitDate AS TIMESTAMP), '%Y') AS INTEGER) > 1940"
        ]

        # Apply Knesset Number Filter
        if knesset_filter:
            knesset_nums_str = ', '.join(map(str, knesset_filter))
            where_clauses.append(f"q.KnessetNum IN ({knesset_nums_str})")
        
        # Apply Faction Filter (Requires JOIN)
        # This plot doesn't inherently show factions. If we were to filter by submitter's faction,
        # we'd need a JOIN. For simplicity, this example will only filter by KnessetNum on KNS_Query.
        # If faction_filter is passed and needs to be applied, the query would become more complex:
        # Example (conceptual, adjust based on actual schema links for query submitters):
        # FROM KNS_Query q
        # JOIN KNS_Person p ON q.PersonID = p.PersonID
        # JOIN KNS_PersonToPosition p2p ON p.PersonID = p2p.PersonID AND q.KnessetNum = p2p.KnessetNum
        # AND q.SubmitDate BETWEEN p2p.StartDate AND COALESCE(p2p.FinishDate, '9999-12-31')
        # ... and then add p2p.FactionID to where_clauses if faction_filter is present.

        if where_clauses:
            base_sql += " WHERE " + " AND ".join(where_clauses)

        base_sql += " GROUP BY SubmitYear, q.KnessetNum ORDER BY SubmitYear ASC, q.KnessetNum ASC;"
        
        logger_obj.debug(f"Executing SQL for plot_queries_by_year: {base_sql}")
        df = con.sql(base_sql).df()

        if df.empty:
            st.info("No query data found to visualize for 'Queries by Year' with the current filters.")
            logger_obj.info("No data for 'Queries by Year' plot after filtering.")
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
                     hover_name="KnessetNum",
                     hover_data={
                         "SubmitYear": True,
                         "QueryCount": True,
                         "KnessetNum": False
                     },
                     color_discrete_sequence=KNESSET_COLOR_SEQUENCE
                    )
        fig.update_layout(
            xaxis_title="Year", 
            yaxis_title="Number of Queries",
            legend_title_text='Knesset',
            title_x=0.5
        )
        return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_queries_by_year': {e}", exc_info=True)
        st.error(f"Could not generate 'Queries by Year' plot: {e}")
        return None

def plot_query_types_distribution(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None # Similar to above, faction filter would require joins
    ):
    """Generates a pie chart of query types distribution, faceted by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_query_types_distribution.")
        return None
    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query"]
        # Add KNS_PersonToPosition if faction_filter is to be implemented
        if not check_tables_exist(con, required_tables, logger_obj):
            return None
        
        base_sql = """
            SELECT 
                q.TypeDesc, 
                q.KnessetNum,
                COUNT(q.QueryID) AS QueryCount 
            FROM KNS_Query q
        """
        where_clauses = [
            "q.TypeDesc IS NOT NULL",
            "q.KnessetNum IS NOT NULL"
        ]

        if knesset_filter:
            knesset_nums_str = ', '.join(map(str, knesset_filter))
            where_clauses.append(f"q.KnessetNum IN ({knesset_nums_str})")
        
        # Add faction_filter logic here if joining with submitter's faction
        
        if where_clauses:
            base_sql += " WHERE " + " AND ".join(where_clauses)
            
        base_sql += " GROUP BY q.TypeDesc, q.KnessetNum;"
        
        logger_obj.debug(f"Executing SQL for plot_query_types_distribution: {base_sql}")
        df = con.sql(base_sql).df()

        if df.empty:
            st.info("No query data found to visualize for 'Query Types Distribution' with the current filters.")
            logger_obj.info("No data for 'Query Types Distribution' plot after filtering.")
            return None

        df["KnessetNum"] = df["KnessetNum"].astype(str)

        fig = px.pie(df, 
                     names="TypeDesc", 
                     values="QueryCount", 
                     color="TypeDesc",
                     facet_col="KnessetNum", 
                     facet_col_wrap=4, 
                     title="<b>Distribution of Query Types (Faceted by Knesset)</b>",
                     labels={"TypeDesc": "Query Type", "QueryCount": "Number of Queries", "KnessetNum": "Knesset Number"},
                     hole=0.3,
                     hover_data=["QueryCount"]
                    )
        fig.update_traces(
            textposition='inside', 
            textinfo='percent+label', 
            insidetextorientation='radial',
            hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>"
        )
        fig.for_each_annotation(lambda a: a.update(text=f"<b>Knesset {a.text.split('=')[-1]}</b>"))
        fig.update_layout(
            legend_title_text='Query Type',
            title_x=0.5
        )
        return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_query_types_distribution': {e}", exc_info=True)
        st.error(f"Could not generate 'Query Types Distribution' plot: {e}")
        return None


def plot_agendas_by_year(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None # Agenda items are submitted by MKs, so faction filter is relevant via join
    ):
    """Generates a bar chart of Knesset agenda items per year, colored by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_agendas_by_year.")
        return None
    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Agenda"]
        # If faction_filter is applied, we need KNS_Person and KNS_PersonToPosition
        # to link KNS_Agenda.InitiatorPersonID to a faction.
        if faction_filter:
            required_tables.extend(["KNS_Person", "KNS_PersonToPosition"])

        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        current_year = datetime.now().year

        base_sql = f"""
            SELECT 
                strftime(CAST(COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate) AS TIMESTAMP), '%Y') AS AgendaYear, 
                a.KnessetNum, 
                COUNT(a.AgendaID) AS AgendaCount 
            FROM KNS_Agenda a
        """
        
        join_clauses = ""
        where_clauses = [
            "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate) IS NOT NULL",
            "a.KnessetNum IS NOT NULL",
            f"CAST(strftime(CAST(COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate) AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}",
            "CAST(strftime(CAST(COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate) AS TIMESTAMP), '%Y') AS INTEGER) > 1940"
        ]

        if knesset_filter:
            knesset_nums_str = ', '.join(map(str, knesset_filter))
            where_clauses.append(f"a.KnessetNum IN ({knesset_nums_str})")

        if faction_filter:
            join_clauses = """
            JOIN KNS_Person p ON a.InitiatorPersonID = p.PersonID
            JOIN KNS_PersonToPosition p2p ON p.PersonID = p2p.PersonID 
                AND a.KnessetNum = p2p.KnessetNum
                AND CAST(COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate) AS TIMESTAMP) 
                    BETWEEN CAST(p2p.StartDate AS TIMESTAMP) 
                    AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
            """
            faction_ids_str = ', '.join(map(str, faction_filter))
            where_clauses.append(f"p2p.FactionID IN ({faction_ids_str})")

        final_sql = base_sql + join_clauses
        if where_clauses:
            final_sql += " WHERE " + " AND ".join(where_clauses)
        final_sql += " GROUP BY AgendaYear, a.KnessetNum ORDER BY AgendaYear ASC, a.KnessetNum ASC;"
        
        logger_obj.debug(f"Executing SQL for plot_agendas_by_year: {final_sql}")
        df = con.sql(final_sql).df()

        if df.empty:
            st.info("No agenda data found to visualize for 'Agendas by Year' with the current filters.")
            logger_obj.info("No data for 'Agendas by Year' plot after filtering.")
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

def plot_agenda_classifications_pie(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None # Also relevant via join to initiator's faction
    ):
    """Generates a pie chart of agenda classifications, faceted by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_agenda_classifications_pie.")
        return None
    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Agenda"]
        if faction_filter:
            required_tables.extend(["KNS_Person", "KNS_PersonToPosition"])
        
        if not check_tables_exist(con, required_tables, logger_obj):
            return None
        
        base_sql = """
            SELECT 
                a.ClassificationDesc, 
                a.KnessetNum,
                COUNT(a.AgendaID) AS AgendaCount 
            FROM KNS_Agenda a
        """
        join_clauses = ""
        where_clauses = [
            "a.ClassificationDesc IS NOT NULL",
            "a.KnessetNum IS NOT NULL"
        ]

        if knesset_filter:
            knesset_nums_str = ', '.join(map(str, knesset_filter))
            where_clauses.append(f"a.KnessetNum IN ({knesset_nums_str})")

        if faction_filter:
            join_clauses = """
            JOIN KNS_Person p ON a.InitiatorPersonID = p.PersonID
            JOIN KNS_PersonToPosition p2p ON p.PersonID = p2p.PersonID 
                AND a.KnessetNum = p2p.KnessetNum
                AND CAST(COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate) AS TIMESTAMP) 
                    BETWEEN CAST(p2p.StartDate AS TIMESTAMP) 
                    AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
            """
            faction_ids_str = ', '.join(map(str, faction_filter))
            where_clauses.append(f"p2p.FactionID IN ({faction_ids_str})")

        final_sql = base_sql + join_clauses
        if where_clauses:
            final_sql += " WHERE " + " AND ".join(where_clauses)
        final_sql += " GROUP BY a.ClassificationDesc, a.KnessetNum;"

        logger_obj.debug(f"Executing SQL for plot_agenda_classifications_pie: {final_sql}")
        df = con.sql(final_sql).df()

        if df.empty:
            st.info("No agenda data found to visualize for 'Agenda Classifications' with the current filters.")
            logger_obj.info("No data for 'Agenda Classifications' plot after filtering.")
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

def plot_factions_per_knesset(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None # Faction filter is directly applicable here on FactionID
    ):
    """Generates a bar chart of the number of factions per Knesset."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_factions_per_knesset.")
        return None
    try:
        con = connect_func(read_only=True)
        if not check_tables_exist(con, ["KNS_Faction"], logger_obj):
            return None

        base_sql = """
            SELECT 
                f.KnessetNum, 
                COUNT(DISTINCT f.FactionID) AS FactionCount 
            FROM KNS_Faction f
        """
        where_clauses = ["f.KnessetNum IS NOT NULL"]

        if knesset_filter:
            knesset_nums_str = ', '.join(map(str, knesset_filter))
            where_clauses.append(f"f.KnessetNum IN ({knesset_nums_str})")
        
        if faction_filter: # This will count distinct factions that are in the filter list, per Knesset
            faction_ids_str = ', '.join(map(str, faction_filter))
            where_clauses.append(f"f.FactionID IN ({faction_ids_str})")
            
        if where_clauses:
            base_sql += " WHERE " + " AND ".join(where_clauses)
            
        base_sql += " GROUP BY f.KnessetNum ORDER BY f.KnessetNum ASC;"
        
        logger_obj.debug(f"Executing SQL for plot_factions_per_knesset: {base_sql}")
        df = con.sql(base_sql).df()

        if df.empty:
            st.info("No faction data found to visualize for 'Factions per Knesset' with the current filters.")
            logger_obj.info("No data for 'Factions per Knesset' plot after filtering.")
            return None
        
        df["KnessetNum"] = df["KnessetNum"].astype(str) # For categorical display

        fig = px.bar(df, 
                     x="KnessetNum", 
                     y="FactionCount", 
                     title="<b>Number of Distinct Factions per Knesset</b>",
                     labels={"KnessetNum": "Knesset Number", "FactionCount": "Number of Factions"},
                     text="FactionCount", 
                     hover_name="KnessetNum",
                     hover_data={
                         "KnessetNum": False, 
                         "FactionCount": True
                     }
                    )
        fig.update_traces(
            textposition='outside', 
            marker_color='rgb(26, 118, 255)', # Example color
            hovertemplate="<b>Knesset %{x}</b><br>Factions: %{y}<extra></extra>"
        )
        fig.update_layout(
            xaxis_title="Knesset Number", 
            yaxis_title="Number of Factions",
            yaxis_range=[0, df["FactionCount"].max() * 1.15 if not df.empty else 10], # Dynamic y-axis
            title_x=0.5
        )
        return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_factions_per_knesset': {e}", exc_info=True)
        st.error(f"Could not generate 'Factions per Knesset' plot: {e}")
        return None

# src/ui/plot_generators.py
"""
Contains functions to generate Plotly visualizations for the Knesset Data Explorer.
"""
from __future__ import annotations

from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go 
import streamlit as st 
import duckdb
import logging 
from datetime import datetime 

# Define a consistent color sequence for categorical data if needed
KNESSET_COLOR_SEQUENCE = px.colors.qualitative.Plotly 
COALITION_OPPOSITION_COLORS = {"Coalition": "#1f77b4", "Opposition": "#ff7f0e", "Unknown": "#7f7f7f", "": "#c7c7c7"} # Adjusted for better contrast


def check_tables_exist(con: duckdb.DuckDBPyConnection, required_tables: list[str], logger_obj: logging.Logger) -> bool:
    """Checks if all required tables exist in the database."""
    try:
        db_tables_df = con.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='main';").df()
        db_tables_list = db_tables_df['table_name'].str.lower().tolist()
        missing_tables = [table for table in required_tables if table.lower() not in db_tables_list]
        if missing_tables:
            st.warning(f"Visualization skipped: Required table(s) '{', '.join(missing_tables)}' not found. Please refresh data.")
            logger_obj.warning(f"Required table(s) '{', '.join(missing_tables)}' not found for visualization.")
            return False
        return True
    except Exception as e:
        logger_obj.error(f"Error checking table existence: {e}", exc_info=True)
        st.error(f"Error checking table existence: {e}")
        return False

def plot_queries_by_year(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None 
    ):
    """Generates a bar chart of Knesset queries per year, colored by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_queries_by_year.")
        return None
    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query"]
        # If faction_filter is to be applied by submitter's faction, joins would be needed.
        # This plot currently doesn't implement direct filtering by submitter's faction for simplicity,
        # as it would require joining KNS_Query -> KNS_Person -> KNS_PersonToPosition.
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
            f"CAST(strftime(CAST(q.SubmitDate AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}", # Ensure no future dates
            "CAST(strftime(CAST(q.SubmitDate AS TIMESTAMP), '%Y') AS INTEGER) > 1940" # Basic sanity check
        ]

        if knesset_filter:
            knesset_nums_str = ', '.join(map(str, knesset_filter))
            where_clauses.append(f"q.KnessetNum IN ({knesset_nums_str})")
        
        # Note: Faction filter on query submitter would require joins.
        # If faction_filter is provided and needs to be applied on the submitter's faction:
        # The query would need to join KNS_Query with KNS_Person and KNS_PersonToPosition
        # and then filter on p2p.FactionID. This is omitted for this specific plot's simplicity.

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
                     category_orders={"SubmitYear": sorted(df["SubmitYear"].unique())}, # Ensure years are sorted
                     hover_name="KnessetNum",
                     hover_data={"SubmitYear": True, "QueryCount": True, "KnessetNum": False}, # Control hover info
                     color_discrete_sequence=KNESSET_COLOR_SEQUENCE
                    )
        fig.update_layout(xaxis_title="Year", yaxis_title="Number of Queries", legend_title_text='Knesset', title_x=0.5)
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
    faction_filter: list | None = None # Not directly applied here for simplicity
    ):
    """Generates a pie chart of query types distribution, faceted by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_query_types_distribution.")
        return None
    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None
        
        base_sql = """
            SELECT 
                q.TypeDesc, 
                q.KnessetNum,
                COUNT(q.QueryID) AS QueryCount 
            FROM KNS_Query q
        """
        where_clauses = ["q.TypeDesc IS NOT NULL", "q.KnessetNum IS NOT NULL"]

        if knesset_filter:
            knesset_nums_str = ', '.join(map(str, knesset_filter))
            where_clauses.append(f"q.KnessetNum IN ({knesset_nums_str})")
        
        # Faction filter would require joins if applied to submitter's faction
        
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
                     color="TypeDesc", # Color by TypeDesc for distinct colors
                     facet_col="KnessetNum", 
                     facet_col_wrap=4, # Adjust as needed
                     title="<b>Distribution of Query Types (Faceted by Knesset)</b>",
                     labels={"TypeDesc": "Query Type", "QueryCount": "Number of Queries", "KnessetNum": "Knesset Number"},
                     hole=0.3, # Donut chart style
                     hover_data=["QueryCount"]
                    )
        fig.update_traces(
            textposition='inside', 
            textinfo='percent+label', 
            insidetextorientation='radial',
            hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>"
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


def plot_agendas_by_year(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None 
    ):
    """Generates a bar chart of Knesset agenda items per year, colored by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_agendas_by_year.")
        return None
    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Agenda"]
        if faction_filter: # If filtering by initiator's faction
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
                     hover_data={"AgendaYear": True, "AgendaCount": True, "KnessetNum": False},
                     color_discrete_sequence=KNESSET_COLOR_SEQUENCE
                    )
        fig.update_layout(xaxis_title="Year", yaxis_title="Number of Agenda Items", legend_title_text='Knesset', title_x=0.5)
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
    faction_filter: list | None = None 
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
        where_clauses = ["a.ClassificationDesc IS NOT NULL", "a.KnessetNum IS NOT NULL"]

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
        fig.update_traces(textposition='inside', textinfo='percent+label', insidetextorientation='radial',
                          hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>")
        fig.for_each_annotation(lambda a: a.update(text=f"<b>Knesset {a.text.split('=')[-1]}</b>"))
        fig.update_layout(legend_title_text='Agenda Classification', title_x=0.5)
        return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_agenda_classifications_pie': {e}", exc_info=True)
        st.error(f"Could not generate 'Agenda Classifications' plot: {e}")
        return None

# --- NEW PLOT 1: Queries by Submitting Faction (Coalition/Opposition Status) ---
def plot_queries_by_faction_status(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None 
):
    """
    Generates a bar chart of queries submitted by factions, colored by their coalition/opposition status.
    """
    if not db_path.exists():
        st.error("Database not found. Cannot generate 'Queries by Faction Status' visualization.")
        logger_obj.error("Database not found for plot_queries_by_faction_status.")
        return None
    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query", "KNS_Person", "KNS_PersonToPosition", "UserFactionCoalitionStatus", "KNS_Faction"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        sql_query = """
        WITH QueryFactionInfo AS (
            SELECT
                q.QueryID,
                q.KnessetNum,
                COALESCE(p2p.FactionName, f_fallback.Name, 'Unknown Faction') AS FactionName,
                p2p.FactionID,
                q.SubmitDate
            FROM KNS_Query q
            JOIN KNS_Person p ON q.PersonID = p.PersonID
            LEFT JOIN KNS_PersonToPosition p2p ON q.PersonID = p2p.PersonID
                AND q.KnessetNum = p2p.KnessetNum
                AND CAST(q.SubmitDate AS TIMESTAMP) BETWEEN CAST(p2p.StartDate AS TIMESTAMP) AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
            LEFT JOIN KNS_Faction f_fallback ON p2p.FactionID = f_fallback.FactionID AND q.KnessetNum = f_fallback.KnessetNum
            WHERE q.SubmitDate IS NOT NULL AND p2p.FactionID IS NOT NULL 
        )
        SELECT
            qfi.KnessetNum,
            qfi.FactionName,
            COALESCE(ufs.CoalitionStatus, 'Unknown') AS CoalitionStatus,
            COUNT(DISTINCT qfi.QueryID) AS QueryCount
        FROM QueryFactionInfo qfi
        LEFT JOIN UserFactionCoalitionStatus ufs ON qfi.FactionID = ufs.FactionID AND qfi.KnessetNum = ufs.KnessetNum
        """
        
        where_clauses = []
        if knesset_filter:
            where_clauses.append(f"qfi.KnessetNum IN ({', '.join(map(str, knesset_filter))})")
        if faction_filter: 
            where_clauses.append(f"qfi.FactionID IN ({', '.join(map(str, faction_filter))})")

        if where_clauses:
            sql_query += " WHERE " + " AND ".join(where_clauses)
        
        sql_query += """
        GROUP BY
            qfi.KnessetNum,
            qfi.FactionName,
            ufs.CoalitionStatus
        HAVING QueryCount > 0 -- Only include factions with queries
        ORDER BY
            qfi.KnessetNum DESC,
            QueryCount DESC;
        """

        logger_obj.debug(f"Executing SQL for plot_queries_by_faction_status: {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info("No query data found to visualize for 'Queries by Faction Status' with the current filters.")
            logger_obj.info("No data for 'Queries by Faction Status' plot after filtering.")
            return None

        df["KnessetNum"] = df["KnessetNum"].astype(str)
        df["QueryCount"] = pd.to_numeric(df["QueryCount"], errors='coerce').fillna(0)
        df["FactionName"] = df["FactionName"].fillna("Unknown Faction")
        df["CoalitionStatus"] = df["CoalitionStatus"].fillna("Unknown")


        fig = px.bar(df,
                     x="FactionName",
                     y="QueryCount",
                     color="CoalitionStatus",
                     facet_col="KnessetNum",
                     facet_col_wrap=2, 
                     title="<b>Queries by Faction (Coalition/Opposition Status)</b>",
                     labels={"FactionName": "Faction", "QueryCount": "Number of Queries", 
                             "CoalitionStatus": "Status", "KnessetNum": "Knesset"},
                     color_discrete_map=COALITION_OPPOSITION_COLORS,
                     hover_name="FactionName",
                     hover_data={"KnessetNum": True, "QueryCount": True, "CoalitionStatus": True}
                     )
        
        fig.update_xaxes(categoryorder="total descending", tickangle=-45) 
        fig.for_each_annotation(lambda a: a.update(text=f"<b>Knesset {a.text.split('=')[-1]}</b>"))
        fig.update_layout(
            legend_title_text='Coalition Status',
            title_x=0.5,
            height=max(600, 250 * ((len(df["KnessetNum"].unique()) + 1) // 2) ) # Dynamic height based on facets
        )
        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_queries_by_faction_status': {e}", exc_info=True)
        st.error(f"Could not generate 'Queries by Faction Status' plot: {e}")
        return None


# --- NEW PLOT 2: Agenda Item Status Distribution ---
def plot_agenda_status_distribution(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None 
):
    """
    Generates a pie chart showing the distribution of agenda items by their status for selected Knessets.
    """
    if not db_path.exists():
        st.error("Database not found. Cannot generate 'Agenda Item Status Distribution' visualization.")
        logger_obj.error("Database not found for plot_agenda_status_distribution.")
        return None
    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Agenda", "KNS_Status"]
        if faction_filter: # If filtering by initiator's faction
             required_tables.extend(["KNS_Person", "KNS_PersonToPosition"])
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        sql_query = """
        SELECT
            a.KnessetNum,
            s.Desc AS StatusDescription,
            COUNT(DISTINCT a.AgendaID) AS AgendaCount
        FROM KNS_Agenda a
        JOIN KNS_Status s ON a.StatusID = s.StatusID
        """
        join_clauses_agenda_status = ""
        where_clauses = ["s.Desc IS NOT NULL"] 
        if knesset_filter:
            where_clauses.append(f"a.KnessetNum IN ({', '.join(map(str, knesset_filter))})")
        
        if faction_filter:
            join_clauses_agenda_status = """
            JOIN KNS_Person p ON a.InitiatorPersonID = p.PersonID
            JOIN KNS_PersonToPosition p2p ON p.PersonID = p2p.PersonID 
                AND a.KnessetNum = p2p.KnessetNum
                AND CAST(COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate) AS TIMESTAMP) 
                    BETWEEN CAST(p2p.StartDate AS TIMESTAMP) 
                    AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
            """
            where_clauses.append(f"p2p.FactionID IN ({', '.join(map(str, faction_filter))})")

        sql_query += join_clauses_agenda_status
        if where_clauses:
            sql_query += " WHERE " + " AND ".join(where_clauses)
        
        sql_query += """
        GROUP BY
            a.KnessetNum,
            s.Desc
        HAVING AgendaCount > 0
        ORDER BY
            a.KnessetNum,
            AgendaCount DESC;
        """

        logger_obj.debug(f"Executing SQL for plot_agenda_status_distribution: {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info("No agenda data found to visualize for 'Agenda Item Status Distribution' with the current filters.")
            logger_obj.info("No data for 'Agenda Item Status Distribution' plot after filtering.")
            return None

        df["KnessetNum"] = df["KnessetNum"].astype(str)
        df["AgendaCount"] = pd.to_numeric(df["AgendaCount"], errors='coerce').fillna(0)
        df["StatusDescription"] = df["StatusDescription"].fillna("Unknown Status")

        # Determine if faceting is needed
        num_knessets_in_data = len(df["KnessetNum"].unique())
        facet_by_knesset = num_knessets_in_data > 1
        if knesset_filter and len(knesset_filter) == 1: # If user explicitly selected only one Knesset
            facet_by_knesset = False


        if facet_by_knesset:
            fig = px.pie(df,
                         names="StatusDescription",
                         values="AgendaCount",
                         color="StatusDescription",
                         facet_col="KnessetNum",
                         facet_col_wrap=min(3, num_knessets_in_data), 
                         title="<b>Distribution of Agenda Item Statuses (Faceted by Knesset)</b>",
                         labels={"StatusDescription": "Status", "AgendaCount": "Number of Agenda Items", "KnessetNum": "Knesset"},
                         hole=0.3)
            fig.for_each_annotation(lambda a: a.update(text=f"<b>Knesset {a.text.split('=')[-1]}</b>"))
        else: 
            knesset_num_display = df["KnessetNum"].unique()[0] if num_knessets_in_data > 0 else "Selected"
            # Aggregate data if multiple knessets are present but not faceting (e.g. no knesset_filter)
            if num_knessets_in_data > 1 and not knesset_filter:
                df_agg = df.groupby("StatusDescription", as_index=False)["AgendaCount"].sum()
                knesset_num_display = "All Selected Knessets"
            else:
                df_agg = df # Use as is if only one Knesset's data or specific Knesset filtered

            fig = px.pie(df_agg,
                         names="StatusDescription",
                         values="AgendaCount",
                         color="StatusDescription",
                         title=f"<b>Distribution of Agenda Item Statuses for Knesset {knesset_num_display}</b>",
                         labels={"StatusDescription": "Status", "AgendaCount": "Number of Agenda Items"},
                         hole=0.3)

        fig.update_traces(textposition='inside', textinfo='percent+label',
                          hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>")
        fig.update_layout(legend_title_text='Agenda Status', title_x=0.5)
        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_agenda_status_distribution': {e}", exc_info=True)
        st.error(f"Could not generate 'Agenda Item Status Distribution' plot: {e}")
        return None

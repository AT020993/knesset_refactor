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
import sys # For ui_utils.format_exception_for_ui if needed

# Define a consistent color sequence for categorical data if needed
KNESSET_COLOR_SEQUENCE = px.colors.qualitative.Plotly
COALITION_OPPOSITION_COLORS = {"Coalition": "#1f77b4", "Opposition": "#ff7f0e", "Unknown": "#7f7f7f", "": "#c7c7c7"}
ANSWER_STATUS_COLORS = {"Answered": "#2ca02c", "Not Answered": "#d62728", "Other/In Progress": "#ffbb78", "Unknown": "#c7c7c7"}
# General status colors, can be expanded
GENERAL_STATUS_COLORS = {
    "Approved": "#2ca02c", "Passed": "#2ca02c", "נענתה": "#2ca02c",
    "Rejected": "#d62728", "Failed": "#d62728", "לא נענתה": "#d62728", "נדחתה": "#d62728",
    "In Progress": "#ffbb78", "בטיפול": "#ffbb78", "הועברה": "#ffbb78", "הוסרה": "#ffbb78",
    "Unknown": "#c7c7c7"
}


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

def plot_queries_by_time_period( # Renamed from plot_queries_by_year
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None,
    aggregation_level: str = "Yearly", # New: Yearly, Monthly, Quarterly
    show_average_line: bool = False # New: Statistical Overlay
    ):
    """Generates a bar chart of Knesset queries per time period, colored by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_queries_by_time_period.")
        return None
    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        current_year = datetime.now().year

        time_period_sql_select = ""
        time_period_alias = "TimePeriod"
        x_axis_label = "Time Period"
        date_column_to_use = "q.SubmitDate" # Assuming SubmitDate is the primary date

        if aggregation_level == "Monthly":
            time_period_sql_select = f"strftime(CAST({date_column_to_use} AS TIMESTAMP), '%Y-%m')"
            x_axis_label = "Year-Month"
        elif aggregation_level == "Quarterly":
            # DuckDB specific quarter extraction: 'quarter(date)' gives 1,2,3,4
            # To get YYYY-Qx format:
            time_period_sql_select = f"strftime(CAST({date_column_to_use} AS TIMESTAMP), '%Y') || '-Q' || CAST(quarter(CAST({date_column_to_use} AS TIMESTAMP)) AS VARCHAR)"
            x_axis_label = "Year-Quarter"
        else: # Default to Yearly
            aggregation_level = "Yearly" # Ensure it's set for clarity
            time_period_sql_select = f"strftime(CAST({date_column_to_use} AS TIMESTAMP), '%Y')"
            x_axis_label = "Year"

        base_sql = f"""
            SELECT
                {time_period_sql_select} AS {time_period_alias},
                q.KnessetNum,
                COUNT(q.QueryID) AS QueryCount
            FROM KNS_Query q
        """

        where_clauses = [
            f"{date_column_to_use} IS NOT NULL",
            "q.KnessetNum IS NOT NULL",
            # Ensure the year extracted from date_column_to_use is within a reasonable range
            f"CAST(strftime(CAST({date_column_to_use} AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}",
            f"CAST(strftime(CAST({date_column_to_use} AS TIMESTAMP), '%Y') AS INTEGER) > 1940"
        ]

        if knesset_filter:
            knesset_nums_str = ', '.join(map(str, knesset_filter))
            where_clauses.append(f"q.KnessetNum IN ({knesset_nums_str})")

        # Faction filter would require joins, similar to other plots.
        # Add faction filter logic here if it becomes a requirement for this plot.

        if where_clauses:
            base_sql += " WHERE " + " AND ".join(where_clauses)

        base_sql += f" GROUP BY {time_period_alias}, q.KnessetNum ORDER BY {time_period_alias} ASC, q.KnessetNum ASC;"

        logger_obj.debug(f"Executing SQL for plot_queries_by_time_period ({aggregation_level}): {base_sql}")
        df = con.sql(base_sql).df()

        if df.empty:
            st.info(f"No query data found to visualize for 'Queries by {x_axis_label}' with the current filters.")
            logger_obj.info(f"No data for 'Queries by {x_axis_label}' plot after filtering.")
            return None

        df["KnessetNum"] = df["KnessetNum"].astype(str)
        df[time_period_alias] = df[time_period_alias].astype(str) # Ensure time period is treated as categorical for x-axis

        # Enhanced Tooltip Data
        df_total_per_period = df.groupby(time_period_alias)['QueryCount'].sum().reset_index(name='TotalQueriesInPeriod')
        df = pd.merge(df, df_total_per_period, on=time_period_alias, how='left')
        df['PercentageOfPeriod'] = ((df['QueryCount'] / df['TotalQueriesInPeriod'].replace(0, pd.NA)) * 100)

        fig = px.bar(df,
                     x=time_period_alias,
                     y="QueryCount",
                     color="KnessetNum",
                     title=f"<b>Number of Queries Submitted per {aggregation_level.replace('ly','')} (by Knesset)</b>",
                     labels={time_period_alias: x_axis_label, "QueryCount": "Number of Queries", "KnessetNum": "Knesset Number"},
                     category_orders={time_period_alias: sorted(df[time_period_alias].unique())}, # Keep chronological order
                     hover_name="KnessetNum",
                     custom_data=[time_period_alias, "KnessetNum", "QueryCount", "TotalQueriesInPeriod", "PercentageOfPeriod"], # For enhanced tooltip
                     color_discrete_sequence=KNESSET_COLOR_SEQUENCE
                    )

        fig.update_traces(
            hovertemplate="<b>Period:</b> %{customdata[0]}<br>" +
                          "<b>Knesset:</b> %{customdata[1]}<br>" +
                          "<b>Queries (this Knesset):</b> %{customdata[2]}<br>" +
                          "<b>Total Queries (all Knessets this period):</b> %{customdata[3]}<br>" +
                          "<b>Share of Period's Total:</b> %{customdata[4]:.1f}%<extra></extra>"
        )

        fig.update_layout(
            xaxis_title=x_axis_label,
            yaxis_title="Number of Queries",
            legend_title_text='Knesset',
            title_x=0.5,
            xaxis_type='category' # Important for time periods like YYYY-MM
        )

        if show_average_line and not df.empty:
            # Calculate average queries PER PERIOD across all Knessets shown
            avg_queries_per_period = df.groupby(time_period_alias)['QueryCount'].sum().mean()
            if pd.notna(avg_queries_per_period):
                fig.add_hline(y=avg_queries_per_period,
                              line_dash="dash",
                              line_color="red",
                              annotation_text=f"Avg Queries/Period: {avg_queries_per_period:.1f}",
                              annotation_position="bottom right",
                              annotation_font_size=10,
                              annotation_font_color="red"
                              )
        return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_queries_by_time_period': {e}", exc_info=True)
        st.error(f"Could not generate 'Queries by {x_axis_label}' plot: {e}")
        return None
    finally:
        if con:
            con.close()

def plot_query_types_distribution(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None
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

        # Add faction filter logic here if needed

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
                     custom_data=["TypeDesc", "QueryCount", "KnessetNum"] # For tooltip
                    )
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            insidetextorientation='radial',
            hovertemplate="<b>Type:</b> %{customdata[0]}<br>" +
                          "<b>Knesset:</b> %{customdata[2]}<br>" +
                          "<b>Count:</b> %{value}<br>" +
                          "<b>Percentage:</b> %{percent}<extra></extra>"
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
    finally:
        if con:
            con.close()


def plot_agendas_by_time_period( # Renamed from plot_agendas_by_year
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None,
    aggregation_level: str = "Yearly", # New: Yearly, Monthly, Quarterly
    show_average_line: bool = False # New: Statistical Overlay
    ):
    """Generates a bar chart of Knesset agenda items per time period, colored by KnessetNum."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_agendas_by_time_period.")
        return None
    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Agenda"]
        if faction_filter:
            required_tables.extend(["KNS_Person", "KNS_PersonToPosition"])

        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        current_year = datetime.now().year
        date_column_to_use = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)" # Prioritize PresidentDecisionDate
        time_period_sql_select = ""
        time_period_alias = "TimePeriod"
        x_axis_label = "Time Period"

        if aggregation_level == "Monthly":
            time_period_sql_select = f"strftime(CAST({date_column_to_use} AS TIMESTAMP), '%Y-%m')"
            x_axis_label = "Year-Month"
        elif aggregation_level == "Quarterly":
            time_period_sql_select = f"strftime(CAST({date_column_to_use} AS TIMESTAMP), '%Y') || '-Q' || CAST(quarter(CAST({date_column_to_use} AS TIMESTAMP)) AS VARCHAR)"
            x_axis_label = "Year-Quarter"
        else: # Default to Yearly
            aggregation_level = "Yearly"
            time_period_sql_select = f"strftime(CAST({date_column_to_use} AS TIMESTAMP), '%Y')"
            x_axis_label = "Year"

        base_sql = f"""
            SELECT
                {time_period_sql_select} AS {time_period_alias},
                a.KnessetNum,
                COUNT(a.AgendaID) AS AgendaCount
            FROM KNS_Agenda a
        """

        join_clauses = ""
        where_clauses = [
            f"{date_column_to_use} IS NOT NULL",
            "a.KnessetNum IS NOT NULL",
            f"CAST(strftime(CAST({date_column_to_use} AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}",
            f"CAST(strftime(CAST({date_column_to_use} AS TIMESTAMP), '%Y') AS INTEGER) > 1940"
        ]

        if knesset_filter:
            knesset_nums_str = ', '.join(map(str, knesset_filter))
            where_clauses.append(f"a.KnessetNum IN ({knesset_nums_str})")

        if faction_filter:
            # Ensure date_column_to_use is correctly referenced in the join condition's BETWEEN clause
            join_clauses = f"""
            JOIN KNS_Person p ON a.InitiatorPersonID = p.PersonID
            JOIN KNS_PersonToPosition p2p ON p.PersonID = p2p.PersonID
                AND a.KnessetNum = p2p.KnessetNum
                AND CAST({date_column_to_use} AS TIMESTAMP)
                    BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                    AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
            """
            faction_ids_str = ', '.join(map(str, faction_filter))
            where_clauses.append(f"p2p.FactionID IN ({faction_ids_str})")

        final_sql = base_sql + join_clauses
        if where_clauses:
            final_sql += " WHERE " + " AND ".join(where_clauses)
        final_sql += f" GROUP BY {time_period_alias}, a.KnessetNum ORDER BY {time_period_alias} ASC, a.KnessetNum ASC;"

        logger_obj.debug(f"Executing SQL for plot_agendas_by_time_period ({aggregation_level}): {final_sql}")
        df = con.sql(final_sql).df()

        if df.empty:
            st.info(f"No agenda data found to visualize for 'Agendas by {x_axis_label}' with the current filters.")
            logger_obj.info(f"No data for 'Agendas by {x_axis_label}' plot after filtering.")
            return None

        df["KnessetNum"] = df["KnessetNum"].astype(str)
        df[time_period_alias] = df[time_period_alias].astype(str)

        # Enhanced Tooltip Data
        df_total_per_period = df.groupby(time_period_alias)['AgendaCount'].sum().reset_index(name='TotalAgendasInPeriod')
        df = pd.merge(df, df_total_per_period, on=time_period_alias, how='left')
        df['PercentageOfPeriod'] = ((df['AgendaCount'] / df['TotalAgendasInPeriod'].replace(0, pd.NA)) * 100)


        fig = px.bar(df,
                     x=time_period_alias,
                     y="AgendaCount",
                     color="KnessetNum",
                     title=f"<b>Number of Agenda Items per {aggregation_level.replace('ly','')} (by Knesset)</b>",
                     labels={time_period_alias: x_axis_label, "AgendaCount": "Number of Agenda Items", "KnessetNum": "Knesset Number"},
                     category_orders={time_period_alias: sorted(df[time_period_alias].unique())},
                     hover_name="KnessetNum",
                     custom_data=[time_period_alias, "KnessetNum", "AgendaCount", "TotalAgendasInPeriod", "PercentageOfPeriod"],
                     color_discrete_sequence=KNESSET_COLOR_SEQUENCE
                    )

        fig.update_traces(
            hovertemplate="<b>Period:</b> %{customdata[0]}<br>" +
                          "<b>Knesset:</b> %{customdata[1]}<br>" +
                          "<b>Agenda Items (this Knesset):</b> %{customdata[2]}<br>" +
                          "<b>Total Items (all Knessets this period):</b> %{customdata[3]}<br>" +
                          "<b>Share of Period's Total:</b> %{customdata[4]:.1f}%<extra></extra>"
        )

        fig.update_layout(
            xaxis_title=x_axis_label,
            yaxis_title="Number of Agenda Items",
            legend_title_text='Knesset',
            title_x=0.5,
            xaxis_type='category'
            )

        if show_average_line and not df.empty:
            avg_agendas_per_period = df.groupby(time_period_alias)['AgendaCount'].sum().mean()
            if pd.notna(avg_agendas_per_period):
                fig.add_hline(y=avg_agendas_per_period,
                              line_dash="dash",
                              line_color="blue", # Different color for distinction
                              annotation_text=f"Avg Items/Period: {avg_agendas_per_period:.1f}",
                              annotation_position="top right",
                              annotation_font_size=10,
                              annotation_font_color="blue"
                              )
        return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_agendas_by_time_period': {e}", exc_info=True)
        st.error(f"Could not generate 'Agendas by {x_axis_label}' plot: {e}")
        return None
    finally:
        if con:
            con.close()

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
        date_column_for_faction_join = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"
        where_clauses = ["a.ClassificationDesc IS NOT NULL", "a.KnessetNum IS NOT NULL"]

        if knesset_filter:
            knesset_nums_str = ', '.join(map(str, knesset_filter))
            where_clauses.append(f"a.KnessetNum IN ({knesset_nums_str})")

        if faction_filter:
            join_clauses = f"""
            JOIN KNS_Person p ON a.InitiatorPersonID = p.PersonID
            JOIN KNS_PersonToPosition p2p ON p.PersonID = p2p.PersonID
                AND a.KnessetNum = p2p.KnessetNum
                AND CAST({date_column_for_faction_join} AS TIMESTAMP)
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
                     custom_data=["ClassificationDesc", "AgendaCount", "KnessetNum"]
                    )
        fig.update_traces(textposition='inside', textinfo='percent+label', insidetextorientation='radial',
                          hovertemplate="<b>Classification:</b> %{customdata[0]}<br>" +
                                        "<b>Knesset:</b> %{customdata[2]}<br>" +
                                        "<b>Count:</b> %{value}<br>" +
                                        "<b>Percentage:</b> %{percent}<extra></extra>")
        fig.for_each_annotation(lambda a: a.update(text=f"<b>Knesset {a.text.split('=')[-1]}</b>"))
        fig.update_layout(legend_title_text='Agenda Classification', title_x=0.5)
        return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_agenda_classifications_pie': {e}", exc_info=True)
        st.error(f"Could not generate 'Agenda Classifications' plot: {e}")
        return None
    finally:
        if con:
            con.close()

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
        HAVING QueryCount > 0
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
                     custom_data=["KnessetNum", "FactionName", "CoalitionStatus", "QueryCount"]
                     )
        fig.update_traces(
            hovertemplate="<b>Faction:</b> %{customdata[1]}<br>" +
                          "<b>Knesset:</b> %{customdata[0]}<br>" +
                          "<b>Status:</b> %{customdata[2]}<br>" +
                          "<b>Query Count:</b> %{customdata[3]}<extra></extra>"
        )

        fig.update_xaxes(categoryorder="total descending", tickangle=-45)
        fig.for_each_annotation(lambda a: a.update(text=f"<b>Knesset {a.text.split('=')[-1]}</b>"))
        fig.update_layout(
            legend_title_text='Coalition Status',
            title_x=0.5,
            height=max(600, 250 * ((len(df["KnessetNum"].unique()) + 1) // 2) )
        )
        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_queries_by_faction_status': {e}", exc_info=True)
        st.error(f"Could not generate 'Queries by Faction Status' plot: {e}")
        return None
    finally:
        if con:
            con.close()

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
        date_column_for_faction_join_agenda = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"
        if faction_filter:
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
            join_clauses_agenda_status = f"""
            JOIN KNS_Person p ON a.InitiatorPersonID = p.PersonID
            JOIN KNS_PersonToPosition p2p ON p.PersonID = p2p.PersonID
                AND a.KnessetNum = p2p.KnessetNum
                AND CAST({date_column_for_faction_join_agenda} AS TIMESTAMP)
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

        num_knessets_in_data = len(df["KnessetNum"].unique())
        facet_by_knesset = num_knessets_in_data > 1
        if knesset_filter and len(knesset_filter) == 1:
            facet_by_knesset = False


        if facet_by_knesset:
            fig = px.pie(df,
                         names="StatusDescription",
                         values="AgendaCount",
                         color="StatusDescription",
                         color_discrete_map=GENERAL_STATUS_COLORS,
                         facet_col="KnessetNum",
                         facet_col_wrap=min(3, num_knessets_in_data),
                         title="<b>Distribution of Agenda Item Statuses (Faceted by Knesset)</b>",
                         labels={"StatusDescription": "Status", "AgendaCount": "Number of Agenda Items", "KnessetNum": "Knesset"},
                         hole=0.3,
                         custom_data=["StatusDescription", "AgendaCount", "KnessetNum"])
            fig.for_each_annotation(lambda a: a.update(text=f"<b>Knesset {a.text.split('=')[-1]}</b>"))
            hovertemplate_pie = ("<b>Status:</b> %{customdata[0]}<br>" +
                                 "<b>Knesset:</b> %{customdata[2]}<br>" +
                                 "<b>Count:</b> %{value}<br>" +
                                 "<b>Percentage:</b> %{percent}<extra></extra>")
        else:
            knesset_num_display = df["KnessetNum"].unique()[0] if num_knessets_in_data > 0 else "Selected"
            if num_knessets_in_data > 1 and not knesset_filter : # Aggregating if multiple knessets but no specific single one is chosen for faceting
                df_agg = df.groupby("StatusDescription", as_index=False)["AgendaCount"].sum()
                knesset_num_display = "All Selected Knessets"
                # Need to re-add KnessetNum for customdata if it's used in hovertemplate and expected
                df_agg["KnessetNum"] = knesset_num_display
            else: # Single Knesset or already filtered to one
                df_agg = df
                if not df_agg.empty: # Ensure df_agg is not empty before accessing
                     knesset_num_display = df_agg["KnessetNum"].unique()[0]


            fig = px.pie(df_agg,
                         names="StatusDescription",
                         values="AgendaCount",
                         color="StatusDescription",
                         color_discrete_map=GENERAL_STATUS_COLORS,
                         title=f"<b>Distribution of Agenda Item Statuses for Knesset {knesset_num_display}</b>",
                         labels={"StatusDescription": "Status", "AgendaCount": "Number of Agenda Items"},
                         hole=0.3,
                         custom_data=["StatusDescription", "AgendaCount", "KnessetNum"]) # KnessetNum might be aggregated string here
            hovertemplate_pie = ("<b>Status:</b> %{customdata[0]}<br>" +
                                 ("<b>Knesset:</b> %{customdata[2]}<br>" if "KnessetNum" in df_agg.columns and not isinstance(df_agg["KnessetNum"].iloc[0], pd.DataFrame) else "") + # Check if KnessetNum is a valid column
                                 "<b>Count:</b> %{value}<br>" +
                                 "<b>Percentage:</b> %{percent}<extra></extra>")


        fig.update_traces(textposition='inside', textinfo='percent+label',
                          hovertemplate=hovertemplate_pie)
        fig.update_layout(legend_title_text='Agenda Status', title_x=0.5)
        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_agenda_status_distribution': {e}", exc_info=True)
        st.error(f"Could not generate 'Agenda Item Status Distribution' plot: {e}")
        return None
    finally:
        if con:
            con.close()

def plot_queries_per_faction_in_knesset(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None
):
    """
    Generates a bar chart of queries per faction for a specific Knesset.
    """
    if not db_path.exists():
        st.error("Database not found. Cannot generate 'Queries per Faction' visualization.")
        logger_obj.error("Database not found for plot_queries_per_faction_in_knesset.")
        return None

    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset using the plot-specific filter to view 'Queries per Faction'.")
        return None

    single_knesset_num = knesset_filter[0]

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query", "KNS_Person", "KNS_PersonToPosition", "KNS_Faction"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        sql_query = f"""
        SELECT
            COALESCE(p2p.FactionName, f_fallback.Name, 'Unknown Faction') AS FactionName,
            p2p.FactionID,
            COUNT(DISTINCT q.QueryID) AS QueryCount
        FROM KNS_Query q
        JOIN KNS_Person p ON q.PersonID = p.PersonID
        LEFT JOIN KNS_PersonToPosition p2p ON q.PersonID = p2p.PersonID
            AND q.KnessetNum = p2p.KnessetNum
            AND CAST(q.SubmitDate AS TIMESTAMP) BETWEEN CAST(p2p.StartDate AS TIMESTAMP) AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
        LEFT JOIN KNS_Faction f_fallback ON p2p.FactionID = f_fallback.FactionID AND q.KnessetNum = f_fallback.KnessetNum
        WHERE q.KnessetNum = {single_knesset_num} AND p2p.FactionID IS NOT NULL
        """

        if faction_filter:
            sql_query += f" AND p2p.FactionID IN ({', '.join(map(str, faction_filter))})"

        sql_query += """
        GROUP BY COALESCE(p2p.FactionName, f_fallback.Name, 'Unknown Faction'), p2p.FactionID
        HAVING QueryCount > 0
        ORDER BY QueryCount DESC;
        """

        logger_obj.debug(f"Executing SQL for plot_queries_per_faction_in_knesset (Knesset {single_knesset_num}): {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No query data found for Knesset {single_knesset_num} with the current filters.")
            logger_obj.info(f"No data for 'Queries per Faction' plot for Knesset {single_knesset_num}.")
            return None

        df["QueryCount"] = pd.to_numeric(df["QueryCount"], errors='coerce').fillna(0)
        df["FactionName"] = df["FactionName"].fillna("Unknown Faction")

        fig = px.bar(df,
                     x="FactionName",
                     y="QueryCount",
                     color="FactionName",
                     title=f"<b>Number of Queries per Faction in Knesset {single_knesset_num}</b>",
                     labels={"FactionName": "Faction", "QueryCount": "Number of Queries"},
                     hover_name="FactionName",
                     custom_data=["FactionName", "QueryCount"]
                     )
        fig.update_traces(
            hovertemplate="<b>Faction:</b> %{customdata[0]}<br>" +
                          "<b>Query Count:</b> %{customdata[1]}<extra></extra>"
        )

        fig.update_layout(
            xaxis_title="Faction",
            yaxis_title="Number of Queries",
            title_x=0.5,
            xaxis_tickangle=-45,
            showlegend=False
        )
        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_queries_per_faction_in_knesset': {e}", exc_info=True)
        st.error(f"Could not generate 'Queries per Faction' plot: {e}")
        return None
    finally:
        if con:
            con.close()

def plot_queries_by_coalition_and_answer_status(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None
):
    """
    Generates a grouped bar chart of queries by coalition/opposition status,
    further grouped by whether the query was answered, for a specific Knesset.
    """
    if not db_path.exists():
        st.error("Database not found. Cannot generate 'Queries by Coalition & Answer Status' visualization.")
        return None

    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset using the plot-specific filter to view 'Queries by Coalition & Answer Status'.")
        return None

    single_knesset_num = knesset_filter[0]

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query", "KNS_Person", "KNS_PersonToPosition", "UserFactionCoalitionStatus", "KNS_Status"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        answer_status_case_sql = """
            CASE
                WHEN s.Desc LIKE '%נענתה%' AND s.Desc NOT LIKE '%לא נענתה%' THEN 'Answered'
                WHEN s.Desc LIKE '%לא נענתה%' THEN 'Not Answered'
                WHEN s.Desc LIKE '%הועברה%' THEN 'Other/In Progress'
                WHEN s.Desc LIKE '%בטיפול%' THEN 'Other/In Progress'
                WHEN s.Desc LIKE '%נדחתה%' THEN 'Not Answered'
                WHEN s.Desc LIKE '%הוסרה%' THEN 'Other/In Progress'
                ELSE 'Unknown'
            END AS AnswerStatus
        """

        sql_query = f"""
        WITH QueryDetails AS (
            SELECT
                q.QueryID,
                q.KnessetNum,
                p2p.FactionID,
                q.SubmitDate,
                {answer_status_case_sql}
            FROM KNS_Query q
            JOIN KNS_Status s ON q.StatusID = s.StatusID
            JOIN KNS_Person p ON q.PersonID = p.PersonID
            LEFT JOIN KNS_PersonToPosition p2p ON q.PersonID = p2p.PersonID
                AND q.KnessetNum = p2p.KnessetNum
                AND CAST(q.SubmitDate AS TIMESTAMP) BETWEEN CAST(p2p.StartDate AS TIMESTAMP) AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
            WHERE q.KnessetNum = {single_knesset_num} AND p2p.FactionID IS NOT NULL
        )
        SELECT
            COALESCE(ufs.CoalitionStatus, 'Unknown') AS CoalitionStatus,
            qd.AnswerStatus,
            COUNT(DISTINCT qd.QueryID) AS QueryCount
        FROM QueryDetails qd
        LEFT JOIN UserFactionCoalitionStatus ufs ON qd.FactionID = ufs.FactionID AND qd.KnessetNum = ufs.KnessetNum
        """

        faction_where_clause = ""
        if faction_filter:
            faction_where_clause = f" WHERE qd.FactionID IN ({', '.join(map(str, faction_filter))})"

        sql_query += faction_where_clause

        sql_query += """
        GROUP BY CoalitionStatus, qd.AnswerStatus
        HAVING QueryCount > 0
        ORDER BY CoalitionStatus, qd.AnswerStatus;
        """

        logger_obj.debug(f"Executing SQL for plot_queries_by_coalition_and_answer_status (Knesset {single_knesset_num}): {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No query data found for Knesset {single_knesset_num} with the current filters for 'Queries by Coalition & Answer Status'.")
            return None

        df["QueryCount"] = pd.to_numeric(df["QueryCount"], errors='coerce').fillna(0)
        df["CoalitionStatus"] = df["CoalitionStatus"].fillna("Unknown")
        df["AnswerStatus"] = df["AnswerStatus"].fillna("Unknown")

        all_coalition_statuses = ["Coalition", "Opposition", "Unknown"]
        all_answer_statuses_ordered = ["Answered", "Not Answered", "Other/In Progress", "Unknown"]

        present_coalition_statuses = df["CoalitionStatus"].unique()
        present_answer_statuses = df["AnswerStatus"].unique()

        # Ensure all combinations are present for consistent bar grouping
        idx = pd.MultiIndex.from_product([all_coalition_statuses, all_answer_statuses_ordered], names=['CoalitionStatus', 'AnswerStatus'])
        df_complete = df.set_index(['CoalitionStatus', 'AnswerStatus']).reindex(idx, fill_value=0).reset_index()
        # Filter out combinations that were not in the original data's scope (e.g. if 'Opposition' had no queries at all)
        # This keeps the structure but avoids plotting empty groups if they truly had no data.
        df_complete = df_complete[df_complete['CoalitionStatus'].isin(present_coalition_statuses)]


        fig = px.bar(df_complete,
                     x="CoalitionStatus",
                     y="QueryCount",
                     color="AnswerStatus",
                     barmode="group",
                     title=f"<b>Queries by Coalition/Opposition and Answer Status (Knesset {single_knesset_num})</b>",
                     labels={"CoalitionStatus": "Submitter's Coalition Status",
                             "QueryCount": "Number of Queries",
                             "AnswerStatus": "Query Outcome"},
                     color_discrete_map=ANSWER_STATUS_COLORS,
                     category_orders={
                         "AnswerStatus": all_answer_statuses_ordered,
                         "CoalitionStatus": all_coalition_statuses
                         },
                     custom_data=["CoalitionStatus", "AnswerStatus", "QueryCount"]
                     )
        fig.update_traces(
            hovertemplate="<b>Coalition Status:</b> %{customdata[0]}<br>" +
                          "<b>Query Outcome:</b> %{customdata[1]}<br>" +
                          "<b>Count:</b> %{customdata[2]}<extra></extra>"
        )

        fig.update_layout(
            xaxis_title="Submitter's Coalition Status",
            yaxis_title="Number of Queries",
            legend_title_text='Query Outcome',
            title_x=0.5
        )
        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_queries_by_coalition_and_answer_status': {e}", exc_info=True)
        st.error(f"Could not generate 'Queries by Coalition & Answer Status' plot: {e}")
        return None
    finally:
        if con:
            con.close()

def plot_queries_by_ministry_and_status(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None # Faction filter might not be directly applicable here unless joining through query submitter
):
    """
    Generates a stacked bar chart showing query distribution and reply percentage by ministry
    for a specific Knesset.
    """
    if not db_path.exists():
        st.error("Database not found. Cannot generate 'Query Performance by Ministry' visualization.")
        return None

    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset using the plot-specific filter to view 'Query Performance by Ministry'.")
        return None

    single_knesset_num = knesset_filter[0]

    try:
        con = connect_func(read_only=True)
        # Faction filter related tables would only be needed if we filter queries based on submitter's faction
        required_tables = ["KNS_Query", "KNS_GovMinistry", "KNS_Status"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        answer_status_case_sql = """
            CASE
                WHEN s.Desc LIKE '%נענתה%' AND s.Desc NOT LIKE '%לא נענתה%' THEN 'Answered'
                WHEN s.Desc LIKE '%לא נענתה%' THEN 'Not Answered'
                WHEN s.Desc LIKE '%הועברה%' THEN 'Other/In Progress'
                WHEN s.Desc LIKE '%בטיפול%' THEN 'Other/In Progress'
                WHEN s.Desc LIKE '%נדחתה%' THEN 'Not Answered'
                WHEN s.Desc LIKE '%הוסרה%' THEN 'Other/In Progress'
                ELSE 'Unknown'
            END AS AnswerStatus
        """

        sql_query = f"""
        WITH MinistryQueryStats AS (
            SELECT
                q.GovMinistryID,
                m.Name AS MinistryName,
                {answer_status_case_sql},
                COUNT(q.QueryID) AS QueryCount
            FROM KNS_Query q
            JOIN KNS_GovMinistry m ON q.GovMinistryID = m.GovMinistryID
            JOIN KNS_Status s ON q.StatusID = s.StatusID
            WHERE q.KnessetNum = {single_knesset_num} AND q.GovMinistryID IS NOT NULL
            -- Add faction filter join and condition here if needed, e.g.,
            -- JOIN KNS_Person p_submitter ON q.PersonID = p_submitter.PersonID
            -- JOIN KNS_PersonToPosition p2p_submitter ON p_submitter.PersonID = p2p_submitter.PersonID
            --     AND q.KnessetNum = p2p_submitter.KnessetNum
            --     AND CAST(q.SubmitDate AS TIMESTAMP) BETWEEN CAST(p2p_submitter.StartDate AS TIMESTAMP) AND CAST(COALESCE(p2p_submitter.FinishDate, '9999-12-31') AS TIMESTAMP)
            -- {"AND p2p_submitter.FactionID IN (" + ", ".join(map(str, faction_filter)) + ")" if faction_filter else ""}
            GROUP BY q.GovMinistryID, m.Name, AnswerStatus
        )
        SELECT
            MinistryName,
            AnswerStatus,
            QueryCount,
            SUM(QueryCount) OVER (PARTITION BY MinistryName) AS TotalQueriesForMinistry,
            SUM(CASE WHEN AnswerStatus = 'Answered' THEN QueryCount ELSE 0 END) OVER (PARTITION BY MinistryName) AS AnsweredQueriesForMinistry
        FROM MinistryQueryStats
        ORDER BY TotalQueriesForMinistry DESC, MinistryName, AnswerStatus;
        """

        logger_obj.debug(f"Executing SQL for plot_queries_by_ministry_and_status (Knesset {single_knesset_num}): {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No query data found for ministries in Knesset {single_knesset_num} with the current filters.")
            return None

        df["QueryCount"] = pd.to_numeric(df["QueryCount"], errors='coerce').fillna(0)
        df["TotalQueriesForMinistry"] = pd.to_numeric(df["TotalQueriesForMinistry"], errors='coerce').fillna(0)
        df["AnsweredQueriesForMinistry"] = pd.to_numeric(df["AnsweredQueriesForMinistry"], errors='coerce').fillna(0)

        df["ReplyPercentage"] = ((df["AnsweredQueriesForMinistry"] / df["TotalQueriesForMinistry"].replace(0, pd.NA)) * 100) # Keep more precision for tooltip
        df["ReplyPercentageText"] = df["ReplyPercentage"].apply(lambda x: f"{x:.1f}% replied" if pd.notna(x) else "N/A replied")


        df_annotations = df.drop_duplicates(subset=['MinistryName']).sort_values(by="TotalQueriesForMinistry", ascending=False)


        fig = px.bar(df,
                     x="MinistryName",
                     y="QueryCount",
                     color="AnswerStatus",
                     title=f"<b>Query Distribution and Reply Rate by Ministry (Knesset {single_knesset_num})</b>",
                     labels={"MinistryName": "Ministry",
                             "QueryCount": "Number of Queries",
                             "AnswerStatus": "Query Outcome"},
                     color_discrete_map=ANSWER_STATUS_COLORS,
                     category_orders={
                         "AnswerStatus": ["Answered", "Not Answered", "Other/In Progress", "Unknown"],
                         "MinistryName": df_annotations["MinistryName"].tolist()
                         }
                     )

        fig.update_traces(
            customdata=df[['MinistryName','AnswerStatus', 'QueryCount', 'TotalQueriesForMinistry', 'ReplyPercentage']],
            hovertemplate="<b>Ministry:</b> %{customdata[0]}<br>" +
                          "<b>Status:</b> %{customdata[1]}<br>" +
                          "<b>Count (this status):</b> %{customdata[2]}<br>" +
                          "<b>Total Queries (Ministry):</b> %{customdata[3]}<br>" +
                          "<b>Reply Rate (Ministry):</b> %{customdata[4]:.1f}%<extra></extra>"
        )


        fig.update_layout(
            xaxis_title="Ministry",
            yaxis_title="Number of Queries",
            legend_title_text='Query Outcome',
            title_x=0.5,
            xaxis_tickangle=-45,
            height=700
        )
        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_queries_by_ministry_and_status': {e}", exc_info=True)
        st.error(f"Could not generate 'Query Performance by Ministry' plot: {e}")
        return None
    finally:
        if con:
            con.close()

# --- NEW PLOT: Agendas per Faction in a specific Knesset ---
def plot_agendas_per_faction_in_knesset(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None, # Expected to be a single Knesset
    faction_filter: list | None = None # For filtering specific factions within that Knesset
):
    """
    Generates a bar chart of agenda items per initiating faction for a specific Knesset.
    """
    if not db_path.exists():
        st.error("Database not found. Cannot generate 'Agendas per Faction' visualization.")
        return None

    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset using the plot-specific filter to view 'Agendas per Faction'.")
        return None

    single_knesset_num = knesset_filter[0]

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Agenda", "KNS_Person", "KNS_PersonToPosition", "KNS_Faction"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None
        date_column_for_faction_join_agenda_detail = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"

        sql_query = f"""
        SELECT
            COALESCE(p2p.FactionName, f_fallback.Name, 'Unknown Faction') AS FactionName,
            p2p.FactionID,
            COUNT(DISTINCT a.AgendaID) AS AgendaCount
        FROM KNS_Agenda a
        JOIN KNS_Person p ON a.InitiatorPersonID = p.PersonID
        LEFT JOIN KNS_PersonToPosition p2p ON p.PersonID = p2p.PersonID
            AND a.KnessetNum = p2p.KnessetNum
            AND CAST({date_column_for_faction_join_agenda_detail} AS TIMESTAMP)
                BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
        LEFT JOIN KNS_Faction f_fallback ON p2p.FactionID = f_fallback.FactionID AND a.KnessetNum = f_fallback.KnessetNum
        WHERE a.KnessetNum = {single_knesset_num} AND a.InitiatorPersonID IS NOT NULL AND p2p.FactionID IS NOT NULL
        """

        if faction_filter:
            sql_query += f" AND p2p.FactionID IN ({', '.join(map(str, faction_filter))})"

        sql_query += """
        GROUP BY COALESCE(p2p.FactionName, f_fallback.Name, 'Unknown Faction'), p2p.FactionID
        HAVING AgendaCount > 0
        ORDER BY AgendaCount DESC;
        """

        logger_obj.debug(f"Executing SQL for plot_agendas_per_faction_in_knesset (Knesset {single_knesset_num}): {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No agenda data found for Knesset {single_knesset_num} with the current filters.")
            return None

        df["AgendaCount"] = pd.to_numeric(df["AgendaCount"], errors='coerce').fillna(0)
        df["FactionName"] = df["FactionName"].fillna("Unknown Faction")

        fig = px.bar(df,
                     x="FactionName",
                     y="AgendaCount",
                     color="FactionName",
                     title=f"<b>Number of Agenda Items per Initiating Faction (Knesset {single_knesset_num})</b>",
                     labels={"FactionName": "Faction", "AgendaCount": "Number of Agenda Items"},
                     hover_name="FactionName",
                     custom_data=["FactionName", "AgendaCount"]
                     )
        fig.update_traces(
            hovertemplate="<b>Faction:</b> %{customdata[0]}<br>" +
                          "<b>Agenda Items:</b> %{customdata[1]}<extra></extra>"
        )

        fig.update_layout(
            xaxis_title="Initiating Faction",
            yaxis_title="Number of Agenda Items",
            title_x=0.5,
            xaxis_tickangle=-45,
            showlegend=False
        )
        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_agendas_per_faction_in_knesset': {e}", exc_info=True)
        st.error(f"Could not generate 'Agendas per Faction' plot: {e}")
        return None
    finally:
        if con:
            con.close()

# --- NEW PLOT: Agendas by Coalition/Opposition and Status ---
def plot_agendas_by_coalition_and_status(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None, # Expected to be a single Knesset
    faction_filter: list | None = None
):
    """
    Generates a grouped bar chart of agenda items by initiator's coalition/opposition status,
    further grouped by agenda status, for a specific Knesset.
    """
    if not db_path.exists():
        st.error("Database not found. Cannot generate 'Agendas by Coalition & Status' visualization.")
        return None

    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset using the plot-specific filter to view 'Agendas by Coalition & Status'.")
        return None

    single_knesset_num = knesset_filter[0]

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Agenda", "KNS_Person", "KNS_PersonToPosition", "UserFactionCoalitionStatus", "KNS_Status"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None
        date_column_for_faction_join_agenda_status = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"

        sql_query = f"""
        WITH AgendaDetails AS (
            SELECT
                a.AgendaID,
                a.KnessetNum,
                p2p.FactionID,
                {date_column_for_faction_join_agenda_status} AS RelevantDate,
                s.Desc AS AgendaStatusDescription
            FROM KNS_Agenda a
            JOIN KNS_Status s ON a.StatusID = s.StatusID
            JOIN KNS_Person p ON a.InitiatorPersonID = p.PersonID
            LEFT JOIN KNS_PersonToPosition p2p ON p.PersonID = p2p.PersonID
                AND a.KnessetNum = p2p.KnessetNum
                AND CAST({date_column_for_faction_join_agenda_status} AS TIMESTAMP)
                    BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                    AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
            WHERE a.KnessetNum = {single_knesset_num} AND a.InitiatorPersonID IS NOT NULL AND p2p.FactionID IS NOT NULL
        )
        SELECT
            COALESCE(ufs.CoalitionStatus, 'Unknown') AS CoalitionStatus,
            ad.AgendaStatusDescription,
            COUNT(DISTINCT ad.AgendaID) AS AgendaCount
        FROM AgendaDetails ad
        LEFT JOIN UserFactionCoalitionStatus ufs ON ad.FactionID = ufs.FactionID AND ad.KnessetNum = ufs.KnessetNum
        """

        faction_where_clause = ""
        if faction_filter:
            faction_where_clause = f" WHERE ad.FactionID IN ({', '.join(map(str, faction_filter))})"

        sql_query += faction_where_clause

        sql_query += """
        GROUP BY CoalitionStatus, ad.AgendaStatusDescription
        HAVING AgendaCount > 0
        ORDER BY CoalitionStatus, ad.AgendaStatusDescription;
        """

        logger_obj.debug(f"Executing SQL for plot_agendas_by_coalition_and_status (Knesset {single_knesset_num}): {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No agenda data found for Knesset {single_knesset_num} with the current filters for 'Agendas by Coalition & Status'.")
            return None

        df["AgendaCount"] = pd.to_numeric(df["AgendaCount"], errors='coerce').fillna(0)
        df["CoalitionStatus"] = df["CoalitionStatus"].fillna("Unknown")
        df["AgendaStatusDescription"] = df["AgendaStatusDescription"].fillna("Unknown Status")

        all_coalition_statuses = ["Coalition", "Opposition", "Unknown"]
        all_agenda_statuses_ordered = sorted(list(GENERAL_STATUS_COLORS.keys())) # Use keys from your defined map for order

        present_coalition_statuses = df["CoalitionStatus"].unique()
        # Ensure all_agenda_statuses_ordered only contains statuses present in the data or a predefined complete list
        actual_agenda_statuses_in_data = df["AgendaStatusDescription"].unique()
        relevant_agenda_statuses_ordered = [s for s in all_agenda_statuses_ordered if s in actual_agenda_statuses_in_data]


        idx = pd.MultiIndex.from_product([all_coalition_statuses, relevant_agenda_statuses_ordered], names=['CoalitionStatus', 'AgendaStatusDescription'])
        df_complete = df.set_index(['CoalitionStatus', 'AgendaStatusDescription']).reindex(idx, fill_value=0).reset_index()
        df_complete = df_complete[df_complete['CoalitionStatus'].isin(present_coalition_statuses)]


        fig = px.bar(df_complete,
                     x="CoalitionStatus",
                     y="AgendaCount",
                     color="AgendaStatusDescription",
                     barmode="group",
                     title=f"<b>Agendas by Initiator's Coalition/Opposition and Item Status (Knesset {single_knesset_num})</b>",
                     labels={"CoalitionStatus": "Initiator's Coalition Status",
                             "AgendaCount": "Number of Agenda Items",
                             "AgendaStatusDescription": "Agenda Item Status"},
                     color_discrete_map=GENERAL_STATUS_COLORS, # Use general status colors
                     category_orders={
                         "AgendaStatusDescription": relevant_agenda_statuses_ordered,
                         "CoalitionStatus": all_coalition_statuses
                         },
                     custom_data=["CoalitionStatus", "AgendaStatusDescription", "AgendaCount"]
                     )
        fig.update_traces(
            hovertemplate="<b>Coalition Status:</b> %{customdata[0]}<br>" +
                          "<b>Agenda Status:</b> %{customdata[1]}<br>" +
                          "<b>Count:</b> %{customdata[2]}<extra></extra>"
        )

        fig.update_layout(
            xaxis_title="Initiator's Coalition Status",
            yaxis_title="Number of Agenda Items",
            legend_title_text='Agenda Item Status',
            title_x=0.5
        )
        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_agendas_by_coalition_and_status': {e}", exc_info=True)
        st.error(f"Could not generate 'Agendas by Coalition & Status' plot: {e}")
        return None
    finally:
        if con:
            con.close()

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
QUERY_TYPE_COLORS = {"רגילה": "#1f77b4", "דחופה": "#ff7f0e", "ישירה": "#2ca02c"}


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

def plot_queries_by_time_period(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None, # Can be None (all), single, or multiple Knessets
    faction_filter: list | None = None,
    aggregation_level: str = "Yearly",
    show_average_line: bool = False
    ):
    """Generates a bar chart of Knesset queries per time period.
       If knesset_filter has one Knesset, shows data for that Knesset.
       If knesset_filter has multiple or is None, colors by KnessetNum.
    """
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_queries_by_time_period.")
        return None

    title_knesset_part = "All Knessets"
    is_single_knesset_view = False
    if knesset_filter and len(knesset_filter) == 1:
        title_knesset_part = f"Knesset {knesset_filter[0]}"
        is_single_knesset_view = True
    elif knesset_filter: # Multiple Knessets selected
        title_knesset_part = f"Knessets: {', '.join(map(str, knesset_filter))}"


    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        current_year = datetime.now().year
        time_period_sql_select = ""
        time_period_alias = "TimePeriod"
        x_axis_label = "Time Period"
        date_column_to_use = "q.SubmitDate"

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

        knesset_num_select_sql = "q.KnessetNum," if not is_single_knesset_view else ""
        
        base_sql = f"""
            SELECT
                {time_period_sql_select} AS {time_period_alias},
                {knesset_num_select_sql}
                COUNT(q.QueryID) AS QueryCount
            FROM KNS_Query q
        """

        where_clauses = [
            f"{date_column_to_use} IS NOT NULL",
            "q.KnessetNum IS NOT NULL",
            f"CAST(strftime(CAST({date_column_to_use} AS TIMESTAMP), '%Y') AS INTEGER) <= {current_year}",
            f"CAST(strftime(CAST({date_column_to_use} AS TIMESTAMP), '%Y') AS INTEGER) > 1940"
        ]

        if knesset_filter:
            knesset_nums_str = ', '.join(map(str, knesset_filter))
            where_clauses.append(f"q.KnessetNum IN ({knesset_nums_str})")

        if where_clauses:
            base_sql += " WHERE " + " AND ".join(where_clauses)

        group_by_terms = [time_period_alias]
        if not is_single_knesset_view:
            group_by_terms.append("q.KnessetNum")
        
        base_sql += f" GROUP BY {', '.join(group_by_terms)}"
        base_sql += f" ORDER BY {', '.join(group_by_terms)}, QueryCount DESC;"


        logger_obj.debug(f"Executing SQL for plot_queries_by_time_period ({aggregation_level}): {base_sql}")
        df = con.sql(base_sql).df()

        if df.empty:
            st.info(f"No query data found for '{title_knesset_part}' to visualize 'Queries by {x_axis_label}' with the current filters.")
            logger_obj.info(f"No data for 'Queries by {x_axis_label}' plot after filtering.")
            return None

        if "KnessetNum" in df.columns: 
            df["KnessetNum"] = df["KnessetNum"].astype(str)
        df[time_period_alias] = df[time_period_alias].astype(str)

        plot_title = f"<b>Queries per {aggregation_level.replace('ly','')} for {title_knesset_part}</b>"
        color_param = "KnessetNum" if "KnessetNum" in df.columns and len(df["KnessetNum"].unique()) > 1 else None
        
        custom_data_cols = [time_period_alias, "QueryCount"]
        if "KnessetNum" in df.columns: 
            custom_data_cols.append("KnessetNum")

        fig = px.bar(df,
                     x=time_period_alias,
                     y="QueryCount",
                     color=color_param,
                     title=plot_title,
                     labels={time_period_alias: x_axis_label, "QueryCount": "Number of Queries", "KnessetNum": "Knesset Number"},
                     category_orders={time_period_alias: sorted(df[time_period_alias].unique())},
                     custom_data=custom_data_cols, 
                     color_discrete_sequence=KNESSET_COLOR_SEQUENCE if color_param else px.colors.qualitative.Plotly 
                    )
        
        hovertemplate_str = "<b>Period:</b> %{customdata[0]}<br>" + \
                            "<b>Queries:</b> %{y}" 
        if "KnessetNum" in df.columns and len(custom_data_cols) > 2: 
            hovertemplate_str = "<b>Period:</b> %{customdata[0]}<br>" + \
                                "<b>Knesset:</b> %{customdata[2]}<br>" + \
                                "<b>Queries:</b> %{y}"
        hovertemplate_str += "<extra></extra>"
        fig.update_traces(hovertemplate=hovertemplate_str)


        fig.update_layout(
            xaxis_title=x_axis_label,
            yaxis_title="Number of Queries",
            legend_title_text='Knesset' if color_param else None,
            showlegend=bool(color_param),
            title_x=0.5,
            xaxis_type='category'
        )

        if show_average_line and not df.empty:
            avg_queries_per_period = df.groupby(time_period_alias)['QueryCount'].sum().mean()
            if pd.notna(avg_queries_per_period):
                fig.add_hline(y=avg_queries_per_period,
                              line_dash="dash",
                              line_color="red",
                              annotation_text=f"Avg Queries/Period (overall): {avg_queries_per_period:.1f}",
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

def plot_parliamentary_activity_heatmap(db_path, connect_func, logger_obj, **kwargs):
    """Calendar heatmap showing daily parliamentary activity intensity"""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_parliamentary_activity_heatmap.")
        return None

    knesset_filter = kwargs.get('knesset_filter')
    
    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset to view the 'Parliamentary Activity Heatmap'.")
        logger_obj.info("plot_parliamentary_activity_heatmap requires a single Knesset filter.")
        return None
    single_knesset_num = knesset_filter[0]

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query", "KNS_Agenda"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        sql_query = f"""
        WITH DailyActivity AS (
            SELECT 
                CAST(q.SubmitDate AS DATE) AS ActivityDate,
                'Query' AS ActivityType,
                COUNT(*) AS ActivityCount
            FROM KNS_Query q
            WHERE q.KnessetNum = {single_knesset_num} 
                AND q.SubmitDate IS NOT NULL
            GROUP BY CAST(q.SubmitDate AS DATE)
            
            UNION ALL
            
            SELECT 
                CAST(COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate) AS DATE) AS ActivityDate,
                'Agenda' AS ActivityType,
                COUNT(*) AS ActivityCount
            FROM KNS_Agenda a
            WHERE a.KnessetNum = {single_knesset_num} 
                AND COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate) IS NOT NULL
            GROUP BY CAST(COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate) AS DATE)
        ),
        AggregatedActivity AS (
            SELECT 
                ActivityDate,
                SUM(ActivityCount) AS TotalActivity,
                MAX(CASE WHEN ActivityType = 'Query' THEN ActivityCount ELSE 0 END) AS QueryCount,
                MAX(CASE WHEN ActivityType = 'Agenda' THEN ActivityCount ELSE 0 END) AS AgendaCount
            FROM DailyActivity
            WHERE ActivityDate >= DATE '2000-01-01' 
                AND ActivityDate <= CURRENT_DATE
            GROUP BY ActivityDate
        )
        SELECT 
            ActivityDate,
            TotalActivity,
            QueryCount,
            AgendaCount,
            strftime('%Y', ActivityDate) AS Year,
            strftime('%m', ActivityDate) AS Month,
            strftime('%d', ActivityDate) AS Day,
            strftime('%w', ActivityDate) AS Weekday
        FROM AggregatedActivity
        ORDER BY ActivityDate
        """

        logger_obj.debug(f"Executing SQL for plot_parliamentary_activity_heatmap (Knesset {single_knesset_num}): {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No activity data found for Knesset {single_knesset_num}.")
            logger_obj.info(f"No data for 'Parliamentary Activity Heatmap' plot for Knesset {single_knesset_num}.")
            return None

        df['ActivityDate'] = pd.to_datetime(df['ActivityDate'])
        df['Year'] = df['Year'].astype(int)
        df['Month'] = df['Month'].astype(int)
        df['Day'] = df['Day'].astype(int)
        df['Weekday'] = df['Weekday'].astype(int)
        df['TotalActivity'] = pd.to_numeric(df['TotalActivity'], errors='coerce').fillna(0)

        # Create month-year for grouping
        df['MonthYear'] = df['ActivityDate'].dt.to_period('M').astype(str)
        
        # Create day of week labels
        weekday_labels = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        df['WeekdayName'] = df['Weekday'].map(lambda x: weekday_labels[x])

        fig = px.density_heatmap(df,
                                x='MonthYear',
                                y='WeekdayName',
                                z='TotalActivity',
                                title=f"<b>Daily Parliamentary Activity Intensity (Knesset {single_knesset_num})</b>",
                                labels={'MonthYear': 'Month-Year', 
                                       'WeekdayName': 'Day of Week',
                                       'TotalActivity': 'Total Activity Count'},
                                color_continuous_scale='Viridis',
                                hover_data=['QueryCount', 'AgendaCount']
                                )

        fig.update_traces(
            hovertemplate="<b>Period:</b> %{x}<br>" +
                          "<b>Day:</b> %{y}<br>" +
                          "<b>Total Activity:</b> %{z}<br>" +
                          "<b>Queries:</b> %{customdata[0]}<br>" +
                          "<b>Agenda Items:</b> %{customdata[1]}<extra></extra>"
        )

        fig.update_layout(
            title_x=0.5,
            xaxis_title="Month-Year",
            yaxis_title="Day of Week",
            height=500,
            xaxis_tickangle=-45
        )

        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_parliamentary_activity_heatmap': {e}", exc_info=True)
        st.error(f"Could not generate 'Parliamentary Activity Heatmap' plot: {e}")
        return None
    finally:
        if con:
            con.close()

def plot_ministry_workload_sunburst(db_path, connect_func, logger_obj, **kwargs):
    """Hierarchical view: Ministry -> Query Type -> Status"""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_ministry_workload_sunburst.")
        return None

    knesset_filter = kwargs.get('knesset_filter')
    
    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset to view the 'Ministry Workload Sunburst'.")
        logger_obj.info("plot_ministry_workload_sunburst requires a single Knesset filter.")
        return None
    single_knesset_num = knesset_filter[0]

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query", "KNS_GovMinistry", "KNS_Status"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        answer_status_case_sql = """
            CASE
                WHEN s.Desc LIKE '%נענתה%' AND s.Desc NOT LIKE '%לא נענתה%' THEN 'Answered'
                WHEN s.Desc LIKE '%לא נענתה%' THEN 'Not Answered'
                WHEN s.Desc LIKE '%הועברה%' THEN 'In Progress'
                WHEN s.Desc LIKE '%בטיפול%' THEN 'In Progress'
                WHEN s.Desc LIKE '%נדחתה%' THEN 'Rejected'
                WHEN s.Desc LIKE '%הוסרה%' THEN 'Removed'
                ELSE 'Other'
            END AS AnswerStatus
        """

        sql_query = f"""
        SELECT 
            COALESCE(m.Name, 'Unknown Ministry') AS MinistryName,
            COALESCE(q.TypeDesc, 'Unknown Type') AS QueryType,
            {answer_status_case_sql},
            COUNT(q.QueryID) AS QueryCount
        FROM KNS_Query q
        LEFT JOIN KNS_GovMinistry m ON q.GovMinistryID = m.GovMinistryID
        JOIN KNS_Status s ON q.StatusID = s.StatusID
        WHERE q.KnessetNum = {single_knesset_num}
        GROUP BY MinistryName, QueryType, AnswerStatus
        HAVING QueryCount > 0
        ORDER BY MinistryName, QueryType, AnswerStatus
        """

        logger_obj.debug(f"Executing SQL for plot_ministry_workload_sunburst (Knesset {single_knesset_num}): {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No ministry workload data found for Knesset {single_knesset_num}.")
            logger_obj.info(f"No data for 'Ministry Workload Sunburst' plot for Knesset {single_knesset_num}.")
            return None

        df['QueryCount'] = pd.to_numeric(df['QueryCount'], errors='coerce').fillna(0)

        # Create hierarchical structure for sunburst
        df['ids'] = df['MinistryName'] + ' / ' + df['QueryType'] + ' / ' + df['AnswerStatus']
        df['parents'] = df['MinistryName'] + ' / ' + df['QueryType']
        df['labels'] = df['AnswerStatus']
        
        # Add intermediate levels
        ministry_df = df.groupby('MinistryName')['QueryCount'].sum().reset_index()
        ministry_df['ids'] = ministry_df['MinistryName']
        ministry_df['parents'] = ''
        ministry_df['labels'] = ministry_df['MinistryName']
        
        type_df = df.groupby(['MinistryName', 'QueryType'])['QueryCount'].sum().reset_index()
        type_df['ids'] = type_df['MinistryName'] + ' / ' + type_df['QueryType']
        type_df['parents'] = type_df['MinistryName']
        type_df['labels'] = type_df['QueryType']

        # Combine all levels
        sunburst_df = pd.concat([
            ministry_df[['ids', 'parents', 'labels', 'QueryCount']],
            type_df[['ids', 'parents', 'labels', 'QueryCount']],
            df[['ids', 'parents', 'labels', 'QueryCount']]
        ], ignore_index=True)

        fig = px.sunburst(sunburst_df,
                         ids='ids',
                         parents='parents',
                         values='QueryCount',
                         names='labels',
                         title=f"<b>Ministry Workload Hierarchy (Knesset {single_knesset_num})</b>",
                         color='QueryCount',
                         color_continuous_scale='Viridis',
                         maxdepth=3
                         )

        fig.update_traces(
            hovertemplate="<b>%{label}</b><br>" +
                          "<b>Query Count:</b> %{value}<br>" +
                          "<b>Percentage:</b> %{percentParent}<extra></extra>"
        )

        fig.update_layout(
            title_x=0.5,
            height=700,
            font_size=10
        )

        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_ministry_workload_sunburst': {e}", exc_info=True)
        st.error(f"Could not generate 'Ministry Workload Sunburst' plot: {e}")
        return None
    finally:
        if con:
            con.close()

def plot_mk_collaboration_network(db_path, connect_func, logger_obj, **kwargs):
    """Network graph of MKs based on co-participation patterns"""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_mk_collaboration_network.")
        return None

    knesset_filter = kwargs.get('knesset_filter')
    
    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset to view the 'MK Collaboration Network'.")
        logger_obj.info("plot_mk_collaboration_network requires a single Knesset filter.")
        return None
    single_knesset_num = knesset_filter[0]

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query", "KNS_Person", "KNS_PersonToPosition", "KNS_GovMinistry"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        # Find MKs who frequently submit queries to the same ministries
        sql_query = f"""
        WITH MKMinistryActivity AS (
            SELECT 
                p.PersonID,
                p.FirstName || ' ' || p.LastName AS MKName,
                COALESCE(p2p.FactionName, 'Unknown') AS FactionName,
                q.GovMinistryID,
                m.Name AS MinistryName,
                COUNT(q.QueryID) AS QueryCount
            FROM KNS_Query q
            JOIN KNS_Person p ON q.PersonID = p.PersonID
            LEFT JOIN KNS_PersonToPosition p2p ON q.PersonID = p2p.PersonID
                AND q.KnessetNum = p2p.KnessetNum
                AND CAST(q.SubmitDate AS TIMESTAMP) BETWEEN CAST(p2p.StartDate AS TIMESTAMP) 
                AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
            LEFT JOIN KNS_GovMinistry m ON q.GovMinistryID = m.GovMinistryID
            WHERE q.KnessetNum = {single_knesset_num}
                AND q.GovMinistryID IS NOT NULL
            GROUP BY p.PersonID, MKName, FactionName, q.GovMinistryID, MinistryName
            HAVING QueryCount >= 2
        ),
        MKPairs AS (
            SELECT 
                mk1.PersonID AS MK1_ID,
                mk1.MKName AS MK1_Name,
                mk1.FactionName AS MK1_Faction,
                mk2.PersonID AS MK2_ID,
                mk2.MKName AS MK2_Name,
                mk2.FactionName AS MK2_Faction,
                COUNT(DISTINCT mk1.GovMinistryID) AS SharedMinistries,
                SUM(mk1.QueryCount + mk2.QueryCount) AS TotalQueries
            FROM MKMinistryActivity mk1
            JOIN MKMinistryActivity mk2 ON mk1.GovMinistryID = mk2.GovMinistryID
                AND mk1.PersonID < mk2.PersonID
            GROUP BY mk1.PersonID, mk1.MKName, mk1.FactionName, 
                     mk2.PersonID, mk2.MKName, mk2.FactionName
            HAVING SharedMinistries >= 2
        )
        SELECT *
        FROM MKPairs
        ORDER BY SharedMinistries DESC, TotalQueries DESC
        LIMIT 50
        """

        logger_obj.debug(f"Executing SQL for plot_mk_collaboration_network (Knesset {single_knesset_num}): {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No collaboration patterns found for Knesset {single_knesset_num}.")
            logger_obj.info(f"No data for 'MK Collaboration Network' plot for Knesset {single_knesset_num}.")
            return None

        # Get unique MKs for nodes
        mk1_df = df[['MK1_ID', 'MK1_Name', 'MK1_Faction']].rename(columns={
            'MK1_ID': 'PersonID', 'MK1_Name': 'MKName', 'MK1_Faction': 'FactionName'
        })
        mk2_df = df[['MK2_ID', 'MK2_Name', 'MK2_Faction']].rename(columns={
            'MK2_ID': 'PersonID', 'MK2_Name': 'MKName', 'MK2_Faction': 'FactionName'
        })
        nodes_df = pd.concat([mk1_df, mk2_df]).drop_duplicates(subset=['PersonID'])

        # Create a simple network visualization using scatter plot
        # Since Plotly doesn't have built-in network graphs, we'll simulate it
        
        # Position nodes in a circle
        import math
        n_nodes = len(nodes_df)
        nodes_df = nodes_df.reset_index(drop=True)
        nodes_df['x'] = [math.cos(2 * math.pi * i / n_nodes) for i in range(n_nodes)]
        nodes_df['y'] = [math.sin(2 * math.pi * i / n_nodes) for i in range(n_nodes)]
        
        # Create edge traces
        edge_x = []
        edge_y = []
        edge_info = []
        
        for _, row in df.iterrows():
            mk1_pos = nodes_df[nodes_df['PersonID'] == row['MK1_ID']].iloc[0]
            mk2_pos = nodes_df[nodes_df['PersonID'] == row['MK2_ID']].iloc[0]
            
            edge_x.extend([mk1_pos['x'], mk2_pos['x'], None])
            edge_y.extend([mk1_pos['y'], mk2_pos['y'], None])
            edge_info.append(f"{row['MK1_Name']} - {row['MK2_Name']}: {row['SharedMinistries']} shared ministries")

        # Create the plot
        fig = go.Figure()

        # Add edges
        fig.add_trace(go.Scatter(x=edge_x, y=edge_y,
                                line=dict(width=1, color='lightblue'),
                                hoverinfo='none',
                                mode='lines',
                                showlegend=False))

        # Add nodes
        faction_colors = {faction: color for faction, color in 
                         zip(nodes_df['FactionName'].unique(), 
                             px.colors.qualitative.Plotly[:len(nodes_df['FactionName'].unique())])}

        for faction in nodes_df['FactionName'].unique():
            faction_nodes = nodes_df[nodes_df['FactionName'] == faction]
            fig.add_trace(go.Scatter(x=faction_nodes['x'], y=faction_nodes['y'],
                                    mode='markers+text',
                                    marker=dict(size=10, color=faction_colors[faction]),
                                    text=faction_nodes['MKName'],
                                    textposition="middle center",
                                    textfont=dict(size=8),
                                    name=faction,
                                    hovertemplate="<b>%{text}</b><br>" +
                                                 f"<b>Faction:</b> {faction}<extra></extra>"))

        fig.update_layout(
            title=f"<b>MK Collaboration Network (Knesset {single_knesset_num})</b>",
            title_x=0.5,
            showlegend=True,
            height=700,
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            annotations=[
                dict(text="Connections represent MKs who submit queries to the same ministries",
                     showarrow=False, xref="paper", yref="paper",
                     x=0.5, y=-0.1, xanchor='center', yanchor='top')
            ]
        )

        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_mk_collaboration_network': {e}", exc_info=True)
        st.error(f"Could not generate 'MK Collaboration Network' plot: {e}")
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
    """Generates a bar chart of query types distribution for a single Knesset."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_query_types_distribution.")
        return None

    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset to view the 'Query Types Distribution' plot.")
        logger_obj.info("plot_query_types_distribution requires a single Knesset filter.")
        return None
    single_knesset_num = knesset_filter[0]

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        base_sql = f"""
            SELECT
                q.TypeDesc,
                COUNT(q.QueryID) AS QueryCount
            FROM KNS_Query q
            WHERE q.KnessetNum = ? AND q.TypeDesc IS NOT NULL
        """
        sql_params = [single_knesset_num]

        if faction_filter:
            valid_faction_ids = [int(fid) for fid in faction_filter if str(fid).isdigit()]
            if valid_faction_ids:
                placeholders = ','.join('?' * len(valid_faction_ids))
                base_sql += f"""
                    AND q.PersonID IN (
                        SELECT DISTINCT p2p.PersonID
                        FROM KNS_PersonToPosition p2p
                        WHERE p2p.KnessetNum = ? AND p2p.FactionID IN ({placeholders})
                    )
                """
                sql_params.append(single_knesset_num) 
                sql_params.extend(valid_faction_ids)
            else:
                logger_obj.warning("Faction filter provided but contained no valid numeric IDs.")


        base_sql += " GROUP BY q.TypeDesc ORDER BY q.TypeDesc;"

        logger_obj.debug(f"Executing SQL for plot_query_types_distribution (Knesset {single_knesset_num}): {base_sql} with params {sql_params}")
        df = con.execute(base_sql, sql_params).df()


        if df.empty:
            st.info(f"No query data found for Knesset {single_knesset_num} to visualize 'Query Types Distribution'.")
            logger_obj.info(f"No data for 'Query Types Distribution' plot for Knesset {single_knesset_num}.")
            return None

        df["TypeDesc"] = pd.Categorical(df["TypeDesc"], categories=sorted(df["TypeDesc"].unique()), ordered=True)

        fig = px.bar(df,
                     x="TypeDesc",
                     y="QueryCount",
                     color="TypeDesc",
                     title=f"<b>Distribution of Query Types for Knesset {single_knesset_num}</b>",
                     labels={"TypeDesc": "Query Type", "QueryCount": "Number of Queries"},
                     color_discrete_map=QUERY_TYPE_COLORS,
                     custom_data=["TypeDesc"] 
                    )

        fig.update_traces(
            hovertemplate="<b>Query Type:</b> %{customdata[0]}<br>" +
                          "<b>Count:</b> %{y}<extra></extra>"
        )

        fig.update_layout(
            legend_title_text='Query Type',
            title_x=0.5,
            xaxis_title="Query Type",
            yaxis_title="Number of Queries"
        )
        return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_query_types_distribution' for Knesset {single_knesset_num}: {e}", exc_info=True)
        st.error(f"Could not generate 'Query Types Distribution' plot: {e}")
        return None
    finally:
        if con:
            con.close()


def plot_agendas_by_time_period(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None, 
    faction_filter: list | None = None,
    aggregation_level: str = "Yearly",
    show_average_line: bool = False
    ):
    """Generates a bar chart of Knesset agenda items per time period."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_agendas_by_time_period.")
        return None

    title_knesset_part = "All Knessets"
    is_single_knesset_view = False
    if knesset_filter and len(knesset_filter) == 1:
        title_knesset_part = f"Knesset {knesset_filter[0]}"
        is_single_knesset_view = True
    elif knesset_filter:
        title_knesset_part = f"Knessets: {', '.join(map(str, knesset_filter))}"


    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Agenda"]
        if faction_filter:
            required_tables.extend(["KNS_Person", "KNS_PersonToPosition"])

        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        current_year = datetime.now().year
        date_column_to_use = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"
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

        knesset_num_select_sql = "a.KnessetNum," if not is_single_knesset_view else ""
        
        base_sql = f"""
            SELECT
                {time_period_sql_select} AS {time_period_alias},
                {knesset_num_select_sql}
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

        group_by_terms = [time_period_alias]
        if not is_single_knesset_view:
            group_by_terms.append("a.KnessetNum")
        
        final_sql += f" GROUP BY {', '.join(group_by_terms)}"
        final_sql += f" ORDER BY {', '.join(group_by_terms)}, AgendaCount DESC;"


        logger_obj.debug(f"Executing SQL for plot_agendas_by_time_period ({aggregation_level}): {final_sql}")
        df = con.sql(final_sql).df()

        if df.empty:
            st.info(f"No agenda data for '{title_knesset_part}' to visualize 'Agendas by {x_axis_label}' with the current filters.")
            logger_obj.info(f"No data for 'Agendas by {x_axis_label}' plot after filtering.")
            return None

        if "KnessetNum" in df.columns:
            df["KnessetNum"] = df["KnessetNum"].astype(str)
        df[time_period_alias] = df[time_period_alias].astype(str)

        plot_title = f"<b>Agenda Items per {aggregation_level.replace('ly','')} for {title_knesset_part}</b>"
        color_param = "KnessetNum" if "KnessetNum" in df.columns and len(df["KnessetNum"].unique()) > 1 else None
        
        custom_data_cols = [time_period_alias, "AgendaCount"]
        if "KnessetNum" in df.columns:
            custom_data_cols.append("KnessetNum")

        fig = px.bar(df,
                     x=time_period_alias,
                     y="AgendaCount",
                     color=color_param,
                     title=plot_title,
                     labels={time_period_alias: x_axis_label, "AgendaCount": "Number of Agenda Items", "KnessetNum": "Knesset Number"},
                     category_orders={time_period_alias: sorted(df[time_period_alias].unique())},
                     custom_data=custom_data_cols,
                     color_discrete_sequence=KNESSET_COLOR_SEQUENCE if color_param else px.colors.qualitative.Plotly
                    )

        hovertemplate_str = "<b>Period:</b> %{customdata[0]}<br>" + \
                            "<b>Agenda Items:</b> %{y}"
        if "KnessetNum" in df.columns and len(custom_data_cols) > 2:
            hovertemplate_str = "<b>Period:</b> %{customdata[0]}<br>" + \
                                "<b>Knesset:</b> %{customdata[2]}<br>" + \
                                "<b>Agenda Items:</b> %{y}"
        hovertemplate_str += "<extra></extra>"
        fig.update_traces(hovertemplate=hovertemplate_str)

        fig.update_layout(
            xaxis_title=x_axis_label,
            yaxis_title="Number of Agenda Items",
            legend_title_text='Knesset' if color_param else None,
            showlegend=bool(color_param),
            title_x=0.5,
            xaxis_type='category'
            )

        if show_average_line and not df.empty:
            avg_agendas_per_period = df.groupby(time_period_alias)['AgendaCount'].sum().mean()
            if pd.notna(avg_agendas_per_period):
                fig.add_hline(y=avg_agendas_per_period,
                              line_dash="dash",
                              line_color="blue",
                              annotation_text=f"Avg Items/Period (overall): {avg_agendas_per_period:.1f}",
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
    """Generates a pie chart of agenda classifications for a single Knesset."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_agenda_classifications_pie.")
        return None

    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset to view the 'Agenda Classifications' plot.")
        logger_obj.info("plot_agenda_classifications_pie requires a single Knesset filter.")
        return None
    single_knesset_num = knesset_filter[0]


    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Agenda"]
        if faction_filter:
            required_tables.extend(["KNS_Person", "KNS_PersonToPosition"])

        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        base_sql = f"""
            SELECT
                a.ClassificationDesc,
                COUNT(a.AgendaID) AS AgendaCount
            FROM KNS_Agenda a
            WHERE a.KnessetNum = ? AND a.ClassificationDesc IS NOT NULL
        """
        sql_params = [single_knesset_num]
        date_column_for_faction_join = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"


        if faction_filter:
            valid_faction_ids = [int(fid) for fid in faction_filter if str(fid).isdigit()]
            if valid_faction_ids:
                placeholders = ','.join('?' * len(valid_faction_ids))
                base_sql += f"""
                    AND a.InitiatorPersonID IN (
                        SELECT DISTINCT p2p.PersonID
                        FROM KNS_PersonToPosition p2p
                        WHERE p2p.KnessetNum = ?
                        AND p2p.FactionID IN ({placeholders})
                        AND CAST({date_column_for_faction_join} AS TIMESTAMP)
                            BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                            AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                    )
                """
                sql_params.append(single_knesset_num) 
                sql_params.extend(valid_faction_ids)
            else:
                logger_obj.warning("Faction filter provided for agenda classifications but contained no valid numeric IDs.")


        base_sql += " GROUP BY a.ClassificationDesc ORDER BY AgendaCount DESC;" 

        logger_obj.debug(f"Executing SQL for plot_agenda_classifications_pie (Knesset {single_knesset_num}): {base_sql} with params {sql_params}")
        df = con.execute(base_sql, sql_params).df()


        if df.empty:
            st.info(f"No agenda data for Knesset {single_knesset_num} to visualize 'Agenda Classifications' with the current filters.")
            logger_obj.info(f"No data for 'Agenda Classifications' plot (Knesset {single_knesset_num}).")
            return None


        fig = px.pie(df,
                     names="ClassificationDesc",
                     values="AgendaCount",
                     color="ClassificationDesc",
                     title=f"<b>Distribution of Agenda Classifications for Knesset {single_knesset_num}</b>",
                     labels={"ClassificationDesc": "Agenda Classification", "AgendaCount": "Number of Items"},
                     hole=0.3,
                     custom_data=["ClassificationDesc"] 
                    )
        fig.update_traces(textposition='inside', textinfo='percent+label', insidetextorientation='radial',
                          hovertemplate="<b>Classification:</b> %{customdata[0]}<br>" + 
                                        "<b>Count:</b> %{value}<br>" +
                                        "<b>Percentage:</b> %{percent}<extra></extra>")
        fig.update_layout(legend_title_text='Agenda Classification', title_x=0.5)
        return fig
    except Exception as e:
        logger_obj.error(f"Error generating 'plot_agenda_classifications_pie' for Knesset {single_knesset_num}: {e}", exc_info=True)
        st.error(f"Could not generate 'Agenda Classifications' plot: {e}")
        return None
    finally:
        if con:
            con.close()

def plot_query_status_by_faction(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None, 
    faction_filter: list | None = None,
    start_date: str | None = None,
    end_date: str | None = None
):
    """
    Generates a sunburst chart showing query status descriptions as the outer ring 
    with faction breakdowns as inner segments for a single specified Knesset.
    Can be filtered by date range using start_date and end_date.
    
    Args:
        start_date: Optional start date in YYYY-MM-DD format
        end_date: Optional end date in YYYY-MM-DD format
    """
    if not db_path.exists():
        st.error("Database not found. Cannot generate 'Query Status by Faction' visualization.")
        logger_obj.error("Database not found for plot_query_status_by_faction.")
        return None

    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset to view the 'Query Status by Faction' plot.")
        logger_obj.info("plot_query_status_by_faction requires a single Knesset filter.")
        return None
    single_knesset_num = knesset_filter[0]

    # Date validation
    if start_date and end_date and start_date > end_date:
        st.error("Start date must be before or equal to end date.")
        logger_obj.error(f"Invalid date range: start_date={start_date}, end_date={end_date}")
        return None

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query", "KNS_Person", "KNS_PersonToPosition", "KNS_Status", "KNS_Faction"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        # Build date filtering conditions
        date_conditions = []
        if start_date:
            date_conditions.append(f"CAST(q.SubmitDate AS DATE) >= '{start_date}'")
        if end_date:
            date_conditions.append(f"CAST(q.SubmitDate AS DATE) <= '{end_date}'")
        
        date_filter_sql = " AND ".join(date_conditions)
        if date_filter_sql:
            date_filter_sql = f" AND {date_filter_sql}"

        # Create faction filter SQL
        faction_filter_sql = ""
        if faction_filter:
            faction_filter_sql = f" AND p2p.FactionID IN ({', '.join(map(str, faction_filter))})"

        sql_query = f"""
        WITH QueryStatusFactionInfo AS (
            SELECT
                q.QueryID,
                COALESCE(s.Desc, 'Unknown Status') AS StatusDescription,
                COALESCE(p2p.FactionName, f_fallback.Name, 'Unknown Faction') AS FactionName,
                p2p.FactionID
            FROM KNS_Query q
            JOIN KNS_Person p ON q.PersonID = p.PersonID
            LEFT JOIN KNS_Status s ON q.StatusID = s.StatusID
            LEFT JOIN KNS_PersonToPosition p2p ON q.PersonID = p2p.PersonID
                AND q.KnessetNum = p2p.KnessetNum
                AND CAST(q.SubmitDate AS TIMESTAMP) BETWEEN CAST(p2p.StartDate AS TIMESTAMP) AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
            LEFT JOIN KNS_Faction f_fallback ON p2p.FactionID = f_fallback.FactionID AND q.KnessetNum = f_fallback.KnessetNum
            WHERE q.KnessetNum = {single_knesset_num} AND q.SubmitDate IS NOT NULL AND p2p.FactionID IS NOT NULL
            {faction_filter_sql}{date_filter_sql}
        )
        SELECT
            qsfi.StatusDescription,
            qsfi.FactionName,
            COUNT(DISTINCT qsfi.QueryID) AS QueryCount
        FROM QueryStatusFactionInfo qsfi
        GROUP BY
            qsfi.StatusDescription,
            qsfi.FactionName
        HAVING QueryCount > 0
        ORDER BY
            qsfi.StatusDescription, QueryCount DESC;
        """

        # Log date filter application
        date_filter_info = ""
        if start_date or end_date:
            date_filter_info = f" with date filter: {start_date or 'None'} to {end_date or 'None'}"
        logger_obj.debug(f"Executing SQL for plot_query_status_by_faction (Knesset {single_knesset_num}){date_filter_info}: {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No query data for Knesset {single_knesset_num} to visualize 'Query Status by Faction' with the current filters.")
            logger_obj.info(f"No data for 'Query Status by Faction' plot (Knesset {single_knesset_num}).")
            return None

        df["QueryCount"] = pd.to_numeric(df["QueryCount"], errors='coerce').fillna(0)
        df["StatusDescription"] = df["StatusDescription"].fillna("Unknown Status")
        df["FactionName"] = df["FactionName"].fillna("Unknown Faction")

        # Prepare data for sunburst chart
        # Create hierarchical structure: Status -> Faction
        ids = []
        labels = []
        parents = []
        values = []
        
        # Add status-level entries (top level)
        status_totals = df.groupby('StatusDescription')['QueryCount'].sum().reset_index()
        for _, row in status_totals.iterrows():
            status = row['StatusDescription']
            ids.append(status)
            labels.append(f"{status}<br>({row['QueryCount']} queries)")
            parents.append("")
            values.append(row['QueryCount'])
        
        # Add faction-level entries (nested under status)
        for _, row in df.iterrows():
            status = row['StatusDescription']
            faction = row['FactionName']
            count = row['QueryCount']
            
            faction_id = f"{status} - {faction}"
            ids.append(faction_id)
            labels.append(f"{faction}<br>({count} queries)")
            parents.append(status)
            values.append(count)

        # Build title with date range if applicable
        title = f"<b>Query Status with Faction Breakdown for Knesset {single_knesset_num}</b>"
        if start_date or end_date:
            date_range_text = ""
            if start_date and end_date:
                date_range_text = f" ({start_date} to {end_date})"
            elif start_date:
                date_range_text = f" (from {start_date})"
            elif end_date:
                date_range_text = f" (until {end_date})"
            title = f"<b>Query Status with Faction Breakdown for Knesset {single_knesset_num}{date_range_text}</b>"

        # Create sunburst chart
        fig = go.Figure(go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            hovertemplate="<b>%{label}</b><br>Queries: %{value}<br>Percentage: %{percentParent}<extra></extra>",
            maxdepth=2,
        ))

        fig.update_layout(
            title=title,
            title_x=0.5,
            font_size=12,
            margin=dict(t=50, l=0, r=0, b=0)
        )
        
        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_query_status_by_faction' for Knesset {single_knesset_num}: {e}", exc_info=True)
        st.error(f"Could not generate 'Query Status by Faction' plot: {e}")
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
    Generates a pie chart showing the distribution of agenda items by their status for a single Knesset.
    """
    if not db_path.exists():
        st.error("Database not found. Cannot generate 'Agenda Item Status Distribution' visualization.")
        logger_obj.error("Database not found for plot_agenda_status_distribution.")
        return None

    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset to view the 'Agenda Item Status Distribution' plot.")
        logger_obj.info("plot_agenda_status_distribution requires a single Knesset filter.")
        return None
    single_knesset_num = knesset_filter[0]

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Agenda", "KNS_Status"]
        date_column_for_faction_join_agenda = "COALESCE(a.PresidentDecisionDate, a.LastUpdatedDate)"
        if faction_filter:
             required_tables.extend(["KNS_Person", "KNS_PersonToPosition"])
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        sql_query = f"""
        SELECT
            s.Desc AS StatusDescription,
            COUNT(DISTINCT a.AgendaID) AS AgendaCount
        FROM KNS_Agenda a
        JOIN KNS_Status s ON a.StatusID = s.StatusID
        WHERE a.KnessetNum = ? AND s.Desc IS NOT NULL
        """
        sql_params = [single_knesset_num]


        if faction_filter:
            valid_faction_ids = [int(fid) for fid in faction_filter if str(fid).isdigit()]
            if valid_faction_ids:
                placeholders = ','.join('?' * len(valid_faction_ids))
                sql_query += f"""
                    AND a.InitiatorPersonID IN (
                        SELECT DISTINCT p2p.PersonID
                        FROM KNS_PersonToPosition p2p
                        WHERE p2p.KnessetNum = ?
                        AND p2p.FactionID IN ({placeholders})
                        AND CAST({date_column_for_faction_join_agenda} AS TIMESTAMP)
                            BETWEEN CAST(p2p.StartDate AS TIMESTAMP)
                            AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
                    )
                """
                sql_params.append(single_knesset_num) 
                sql_params.extend(valid_faction_ids)
            else:
                logger_obj.warning("Faction filter provided for agenda status but contained no valid numeric IDs.")


        sql_query += """
        GROUP BY s.Desc
        HAVING AgendaCount > 0
        ORDER BY AgendaCount DESC;
        """

        logger_obj.debug(f"Executing SQL for plot_agenda_status_distribution (Knesset {single_knesset_num}): {sql_query} with params {sql_params}")
        df = con.execute(sql_query, sql_params).df()


        if df.empty:
            st.info(f"No agenda data for Knesset {single_knesset_num} to visualize 'Agenda Item Status Distribution'.")
            logger_obj.info(f"No data for 'Agenda Item Status Distribution' plot (Knesset {single_knesset_num}).")
            return None

        df["AgendaCount"] = pd.to_numeric(df["AgendaCount"], errors='coerce').fillna(0)
        df["StatusDescription"] = df["StatusDescription"].fillna("Unknown Status")

        fig = px.pie(df,
                     names="StatusDescription",
                     values="AgendaCount",
                     color="StatusDescription",
                     color_discrete_map=GENERAL_STATUS_COLORS,
                     title=f"<b>Distribution of Agenda Item Statuses for Knesset {single_knesset_num}</b>",
                     labels={"StatusDescription": "Status", "AgendaCount": "Number of Agenda Items"},
                     hole=0.3,
                     custom_data=["StatusDescription"] 
                    ) 

        fig.update_traces(textposition='inside', textinfo='percent+label',
                          hovertemplate="<b>Status:</b> %{customdata[0]}<br>" + 
                                        "<b>Count:</b> %{value}<br>" +
                                        "<b>Percentage:</b> %{percent}<extra></extra>")
        fig.update_layout(legend_title_text='Agenda Status', title_x=0.5)
        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_agenda_status_distribution' for Knesset {single_knesset_num}: {e}", exc_info=True)
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
                     custom_data=["QueryCount"] 
                     )
        fig.update_traces(
            hovertemplate="<b>Faction:</b> %{x}<br>" +
                          "<b>Query Count:</b> %{customdata[0]}<extra></extra>"
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
        
        cte_sql = f"""
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
            {'AND p2p.FactionID IN (' + ', '.join(map(str, faction_filter)) + ')' if faction_filter else ''}
        )
        """
        sql_query = cte_sql + f"""
        SELECT
            COALESCE(ufs.CoalitionStatus, 'Unknown') AS CoalitionStatus,
            qd.AnswerStatus,
            COUNT(DISTINCT qd.QueryID) AS QueryCount
        FROM QueryDetails qd
        LEFT JOIN UserFactionCoalitionStatus ufs ON qd.FactionID = ufs.FactionID AND qd.KnessetNum = ufs.KnessetNum
        GROUP BY CoalitionStatus, qd.AnswerStatus
        HAVING QueryCount > 0
        ORDER BY CoalitionStatus, qd.AnswerStatus;
        """


        logger_obj.debug(f"Executing SQL for plot_queries_by_coalition_and_answer_status (Knesset {single_knesset_num}): {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No query data for Knesset {single_knesset_num} to visualize 'Queries by Coalition & Answer Status'.")
            return None

        df["QueryCount"] = pd.to_numeric(df["QueryCount"], errors='coerce').fillna(0)
        df["CoalitionStatus"] = df["CoalitionStatus"].fillna("Unknown")
        df["AnswerStatus"] = df["AnswerStatus"].fillna("Unknown")

        all_coalition_statuses = ["Coalition", "Opposition", "Unknown"]
        all_answer_statuses_ordered = ["Answered", "Not Answered", "Other/In Progress", "Unknown"]

        present_coalition_statuses = df["CoalitionStatus"].unique()
        actual_answer_statuses_in_data = df["AnswerStatus"].unique()
        relevant_answer_statuses_ordered = [s for s in all_answer_statuses_ordered if s in actual_answer_statuses_in_data]


        idx = pd.MultiIndex.from_product([all_coalition_statuses, relevant_answer_statuses_ordered], names=['CoalitionStatus', 'AnswerStatus'])
        df_complete = df.set_index(['CoalitionStatus', 'AnswerStatus']).reindex(idx, fill_value=0).reset_index()
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
                         "AnswerStatus": relevant_answer_statuses_ordered,
                         "CoalitionStatus": all_coalition_statuses
                         },
                     custom_data=["AnswerStatus", "QueryCount"] 
                     )
        fig.update_traces(
            hovertemplate="<b>Coalition Status:</b> %{x}<br>" +
                          "<b>Query Outcome:</b> %{customdata[0]}<br>" + 
                          "<b>Count:</b> %{customdata[1]}<extra></extra>" 
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
    faction_filter: list | None = None
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

        sql_query_cte = f"""
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
            """
        if faction_filter:
            valid_faction_ids = [int(fid) for fid in faction_filter if str(fid).isdigit()]
            if valid_faction_ids:
                sql_query_cte += f"""
                    AND q.PersonID IN (
                        SELECT DISTINCT p2p.PersonID
                        FROM KNS_PersonToPosition p2p
                        WHERE p2p.KnessetNum = {single_knesset_num}
                        AND p2p.FactionID IN ({','.join(map(str, valid_faction_ids))})
                    )
                """
            else:
                logger_obj.warning("Faction filter provided for ministry performance but contained no valid numeric IDs.")
        sql_query_cte += """
            GROUP BY q.GovMinistryID, m.Name, AnswerStatus
        )
        """
        sql_query_main = """
        SELECT
            MinistryName,
            AnswerStatus,
            QueryCount,
            SUM(QueryCount) OVER (PARTITION BY MinistryName) AS TotalQueriesForMinistry,
            SUM(CASE WHEN AnswerStatus = 'Answered' THEN QueryCount ELSE 0 END) OVER (PARTITION BY MinistryName) AS AnsweredQueriesForMinistry
        FROM MinistryQueryStats
        ORDER BY TotalQueriesForMinistry DESC, MinistryName, AnswerStatus;
        """
        sql_query = sql_query_cte + sql_query_main


        logger_obj.debug(f"Executing SQL for plot_queries_by_ministry_and_status (Knesset {single_knesset_num}): {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No query data for Knesset {single_knesset_num} to visualize 'Query Performance by Ministry'.")
            return None

        df["QueryCount"] = pd.to_numeric(df["QueryCount"], errors='coerce').fillna(0)
        df["TotalQueriesForMinistry"] = pd.to_numeric(df["TotalQueriesForMinistry"], errors='coerce').fillna(0)
        df["AnsweredQueriesForMinistry"] = pd.to_numeric(df["AnsweredQueriesForMinistry"], errors='coerce').fillna(0)
        df["ReplyPercentage"] = ((df["AnsweredQueriesForMinistry"] / df["TotalQueriesForMinistry"].replace(0, pd.NA)) * 100)

        # Create the pre-formatted hover text string
        df['hover_text'] = (
            "<b>Ministry:</b> " + df['MinistryName'] + "<br>" +
            "<b>Status:</b> " + df['AnswerStatus'] + "<br>" +
            "<b>Count (this status):</b> " + df['QueryCount'].astype(str) + "<br>" +
            "<b>Total Queries (Ministry):</b> " + df['TotalQueriesForMinistry'].astype(str) + "<br>" +
            "<b>Reply Rate (Ministry):</b> " + df['ReplyPercentage'].round(1).astype(str) + "%"
        )

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
                     },
                     custom_data=['hover_text'] # Pass the pre-formatted hover text
                    )

        fig.update_traces(
            hovertemplate="%{customdata[0]}<extra></extra>" # Display the pre-formatted text
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

def plot_agendas_per_faction_in_knesset(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
    faction_filter: list | None = None
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
                     custom_data=["AgendaCount"]
                     )
        fig.update_traces(
            hovertemplate="<b>Faction:</b> %{x}<br>" +
                          "<b>Agenda Items:</b> %{customdata[0]}<extra></extra>"
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

def plot_agendas_by_coalition_and_status(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    knesset_filter: list | None = None,
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

        cte_sql = f"""
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
            {'AND p2p.FactionID IN (' + ', '.join(map(str, faction_filter)) + ')' if faction_filter else ''}
        )
        """
        sql_query = cte_sql + f"""
        SELECT
            COALESCE(ufs.CoalitionStatus, 'Unknown') AS CoalitionStatus,
            ad.AgendaStatusDescription,
            COUNT(DISTINCT ad.AgendaID) AS AgendaCount
        FROM AgendaDetails ad
        LEFT JOIN UserFactionCoalitionStatus ufs ON ad.FactionID = ufs.FactionID AND ad.KnessetNum = ufs.KnessetNum
        GROUP BY CoalitionStatus, ad.AgendaStatusDescription
        HAVING AgendaCount > 0
        ORDER BY CoalitionStatus, ad.AgendaStatusDescription;
        """

        logger_obj.debug(f"Executing SQL for plot_agendas_by_coalition_and_status (Knesset {single_knesset_num}): {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No agenda data for Knesset {single_knesset_num} to visualize 'Agendas by Coalition & Status'.")
            return None

        df["AgendaCount"] = pd.to_numeric(df["AgendaCount"], errors='coerce').fillna(0)
        df["CoalitionStatus"] = df["CoalitionStatus"].fillna("Unknown")
        df["AgendaStatusDescription"] = df["AgendaStatusDescription"].fillna("Unknown Status")

        all_coalition_statuses = ["Coalition", "Opposition", "Unknown"]
        all_agenda_statuses_ordered = sorted(list(GENERAL_STATUS_COLORS.keys()))

        present_coalition_statuses = df["CoalitionStatus"].unique()
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
                     color_discrete_map=GENERAL_STATUS_COLORS,
                     category_orders={
                         "AgendaStatusDescription": relevant_agenda_statuses_ordered,
                         "CoalitionStatus": all_coalition_statuses
                         },
                     custom_data=["AgendaStatusDescription", "AgendaCount"] 
                     )
        fig.update_traces(
            hovertemplate="<b>Coalition Status:</b> %{x}<br>" +
                          "<b>Agenda Status:</b> %{customdata[0]}<br>" + 
                          "<b>Count:</b> %{customdata[1]}<extra></extra>" 
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


def plot_coalition_timeline_gantt(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
):
    """Coalition periods and government changes over time."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_coalition_timeline_gantt.")
        return None

    try:
        con = connect_func(read_only=True)
        required_tables = ["UserFactionCoalitionStatus"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        sql_query = """
            SELECT
                FactionName,
                DateJoinedCoalition AS StartDate,
                COALESCE(DateLeftCoalition, CURRENT_DATE) AS EndDate,
                CoalitionStatus
            FROM UserFactionCoalitionStatus
            WHERE DateJoinedCoalition IS NOT NULL
            ORDER BY DateJoinedCoalition
        """

        logger_obj.debug(
            f"Executing SQL for plot_coalition_timeline_gantt: {sql_query}"
        )
        df = con.sql(sql_query).df()

        if df.empty:
            st.info("No coalition timeline data found.")
            logger_obj.info("No data for plot_coalition_timeline_gantt.")
            return None

        df["FactionName"] = df["FactionName"].fillna("Unknown Faction")
        df["CoalitionStatus"] = df["CoalitionStatus"].fillna("Unknown")

        fig = px.timeline(
            df,
            x_start="StartDate",
            x_end="EndDate",
            y="FactionName",
            color="CoalitionStatus",
            title="<b>Coalition Participation Timeline</b>",
            color_discrete_map=COALITION_OPPOSITION_COLORS,
        )
        fig.update_yaxes(autorange="reversed")
        fig.update_layout(title_x=0.5)
        return fig
    except Exception as e:
        logger_obj.error(
            f"Error generating 'plot_coalition_timeline_gantt': {e}",
            exc_info=True,
        )
        st.error(f"Could not generate 'Coalition Timeline' plot: {e}")
        return None
    finally:
        if con:
            con.close()


def plot_mk_tenure_gantt(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    person_filter: list | None = None,
):
    """MK service periods across Knessets."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_mk_tenure_gantt.")
        return None

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Person", "KNS_PersonToPosition"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        sql_query = """
            SELECT
                p.FirstName || ' ' || p.LastName AS MKName,
                p2p.StartDate AS StartDate,
                COALESCE(p2p.FinishDate, CURRENT_DATE) AS EndDate,
                p2p.KnessetNum
            FROM KNS_PersonToPosition p2p
            JOIN KNS_Person p ON p2p.PersonID = p.PersonID
            WHERE p2p.StartDate IS NOT NULL
        """
        if person_filter:
            placeholders = ", ".join(map(str, person_filter))
            sql_query += f" AND p2p.PersonID IN ({placeholders})"
        sql_query += " ORDER BY MKName, StartDate"

        logger_obj.debug(f"Executing SQL for plot_mk_tenure_gantt: {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info("No MK tenure data found.")
            logger_obj.info("No data for plot_mk_tenure_gantt.")
            return None

        df["KnessetNum"] = df["KnessetNum"].astype(str)

        fig = px.timeline(
            df,
            x_start="StartDate",
            x_end="EndDate",
            y="MKName",
            color="KnessetNum",
            title="<b>MK Tenure Timeline</b>",
            color_discrete_sequence=KNESSET_COLOR_SEQUENCE,
        )
        fig.update_yaxes(autorange="reversed")
        fig.update_layout(title_x=0.5)
        return fig
    except Exception as e:
        logger_obj.error(
            f"Error generating 'plot_mk_tenure_gantt': {e}",
            exc_info=True,
        )
        st.error(f"Could not generate 'MK Tenure Timeline' plot: {e}")
        return None
    finally:
        if con:
            con.close()


def plot_ministry_leadership_timeline(
    db_path: Path,
    connect_func: callable,
    logger_obj: logging.Logger,
    ministry_filter: list | None = None,
):
    """Ministers and their tenure periods."""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_ministry_leadership_timeline.")
        return None

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Person", "KNS_PersonToPosition", "KNS_GovMinistry"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        sql_query = """
            SELECT
                m.Name AS MinistryName,
                p.FirstName || ' ' || p.LastName AS MinisterName,
                p2p.StartDate AS StartDate,
                COALESCE(p2p.FinishDate, CURRENT_DATE) AS EndDate
            FROM KNS_PersonToPosition p2p
            JOIN KNS_Person p ON p2p.PersonID = p.PersonID
            JOIN KNS_GovMinistry m ON p2p.GovMinistryID = m.GovMinistryID
            WHERE p2p.GovMinistryID IS NOT NULL
        """
        if ministry_filter:
            placeholders = ", ".join(map(str, ministry_filter))
            sql_query += f" AND p2p.GovMinistryID IN ({placeholders})"
        sql_query += " ORDER BY MinistryName, StartDate"

        logger_obj.debug(
            f"Executing SQL for plot_ministry_leadership_timeline: {sql_query}"
        )
        df = con.sql(sql_query).df()

        if df.empty:
            st.info("No ministry leadership data found.")
            logger_obj.info("No data for plot_ministry_leadership_timeline.")
            return None

        fig = px.timeline(
            df,
            x_start="StartDate",
            x_end="EndDate",
            y="MinisterName",
            color="MinistryName",
            title="<b>Ministry Leadership Timeline</b>",
            color_discrete_sequence=KNESSET_COLOR_SEQUENCE,
        )
        fig.update_yaxes(autorange="reversed")
        fig.update_layout(title_x=0.5)
        return fig
    except Exception as e:
        logger_obj.error(
            f"Error generating 'plot_ministry_leadership_timeline': {e}",
            exc_info=True,
        )
        st.error(f"Could not generate 'Ministry Leadership Timeline' plot: {e}")
        return None
    finally:
        if con:
            con.close()

def plot_query_response_times(db_path, connect_func, logger_obj, **kwargs):
    """Box plot showing query response times by ministry/faction"""
    if not db_path.exists():
        st.error("Database not found. Cannot generate visualization.")
        logger_obj.error("Database not found for plot_query_response_times.")
        return None

    knesset_filter = kwargs.get('knesset_filter')
    faction_filter = kwargs.get('faction_filter')
    
    if not knesset_filter or len(knesset_filter) != 1:
        st.info("Please select a single Knesset to view the 'Query Response Times' plot.")
        logger_obj.info("plot_query_response_times requires a single Knesset filter.")
        return None
    single_knesset_num = knesset_filter[0]

    try:
        con = connect_func(read_only=True)
        required_tables = ["KNS_Query", "KNS_GovMinistry", "KNS_Person", "KNS_PersonToPosition", "UserFactionCoalitionStatus"]
        if not check_tables_exist(con, required_tables, logger_obj):
            return None

        sql_query = f"""
        WITH QueryResponseData AS (
            SELECT
                q.QueryID,
                q.SubmitDate,
                q.ReplyMinisterDate,
                DATEDIFF('day', CAST(q.SubmitDate AS DATE), CAST(q.ReplyMinisterDate AS DATE)) AS ResponseDays,
                m.Name AS MinistryName,
                COALESCE(p2p.FactionName, 'Unknown Faction') AS FactionName,
                COALESCE(ufs.CoalitionStatus, 'Unknown') AS CoalitionStatus,
                q.TypeDesc AS QueryType
            FROM KNS_Query q
            LEFT JOIN KNS_GovMinistry m ON q.GovMinistryID = m.GovMinistryID
            JOIN KNS_Person p ON q.PersonID = p.PersonID
            LEFT JOIN KNS_PersonToPosition p2p ON q.PersonID = p2p.PersonID
                AND q.KnessetNum = p2p.KnessetNum
                AND CAST(q.SubmitDate AS TIMESTAMP) BETWEEN CAST(p2p.StartDate AS TIMESTAMP) 
                AND CAST(COALESCE(p2p.FinishDate, '9999-12-31') AS TIMESTAMP)
            LEFT JOIN UserFactionCoalitionStatus ufs ON p2p.FactionID = ufs.FactionID 
                AND q.KnessetNum = ufs.KnessetNum
            WHERE q.KnessetNum = {single_knesset_num}
                AND q.SubmitDate IS NOT NULL
                AND q.ReplyMinisterDate IS NOT NULL
                AND CAST(q.SubmitDate AS DATE) <= CAST(q.ReplyMinisterDate AS DATE)
        """
        
        if faction_filter:
            valid_faction_ids = [int(fid) for fid in faction_filter if str(fid).isdigit()]
            if valid_faction_ids:
                sql_query += f" AND p2p.FactionID IN ({','.join(map(str, valid_faction_ids))})"
        
        sql_query += """
        )
        SELECT *
        FROM QueryResponseData
        WHERE ResponseDays >= 0 AND ResponseDays <= 365
        ORDER BY ResponseDays
        """

        logger_obj.debug(f"Executing SQL for plot_query_response_times (Knesset {single_knesset_num}): {sql_query}")
        df = con.sql(sql_query).df()

        if df.empty:
            st.info(f"No query response time data found for Knesset {single_knesset_num} with the current filters.")
            logger_obj.info(f"No data for 'Query Response Times' plot for Knesset {single_knesset_num}.")
            return None

        df["ResponseDays"] = pd.to_numeric(df["ResponseDays"], errors='coerce')
        df = df.dropna(subset=["ResponseDays"])
        
        if df.empty:
            st.info(f"No valid response time data found for Knesset {single_knesset_num}.")
            return None

        df["MinistryName"] = df["MinistryName"].fillna("Unknown Ministry")
        df["CoalitionStatus"] = df["CoalitionStatus"].fillna("Unknown")
        df["QueryType"] = df["QueryType"].fillna("Unknown Type")

        fig = px.box(df,
                     x="MinistryName",
                     y="ResponseDays",
                     color="CoalitionStatus",
                     title=f"<b>Query Response Times by Ministry (Knesset {single_knesset_num})</b>",
                     labels={"MinistryName": "Ministry", 
                             "ResponseDays": "Response Time (Days)",
                             "CoalitionStatus": "Submitter Status"},
                     color_discrete_map=COALITION_OPPOSITION_COLORS,
                     hover_data={"QueryType": True, "FactionName": True}
                     )

        fig.update_traces(
            hovertemplate="<b>Ministry:</b> %{x}<br>" +
                          "<b>Response Days:</b> %{y}<br>" +
                          "<b>Submitter Status:</b> %{fullData.name}<br>" +
                          "<b>Query Type:</b> %{customdata[0]}<br>" +
                          "<b>Faction:</b> %{customdata[1]}<extra></extra>"
        )

        fig.update_layout(
            xaxis_title="Ministry",
            yaxis_title="Response Time (Days)",
            legend_title_text='Submitter Coalition Status',
            title_x=0.5,
            xaxis_tickangle=-45,
            height=600
        )
        
        avg_response_time = df["ResponseDays"].mean()
        if pd.notna(avg_response_time):
            fig.add_hline(y=avg_response_time,
                          line_dash="dash",
                          line_color="red",
                          annotation_text=f"Avg Response Time: {avg_response_time:.1f} days",
                          annotation_position="top right",
                          annotation_font_size=10,
                          annotation_font_color="red")

        return fig

    except Exception as e:
        logger_obj.error(f"Error generating 'plot_query_response_times': {e}", exc_info=True)
        st.error(f"Could not generate 'Query Response Times' plot: {e}")
        return None
    finally:
        if con:
            con.close()

# ──────────────────────────────────────────────────────────────────────────────
#  src/ui/data_refresh.py  – self-service GUI for the researcher
#
#  Launch with:
#     streamlit run src/ui/data_refresh.py
#
#  Key features
#  ------------
#  ▸ Select which tables to refresh (or all of them)
#  ▸ Watch live progress + see "last updated" timestamps
#  ▸ Download ready-made CSV / Excel views
#  ▸ (Optional) run ad-hoc SQL against the DuckDB warehouse
#
#  Dependencies:
#     pip install streamlit duckdb pandas openpyxl
# ──────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

# ─── make sibling packages importable ─────────────────────────────────────────
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  #  …/src
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
# ──────────────────────────────────────────────────────────────────────────────

import asyncio
from datetime import datetime, timezone
from textwrap import dedent

import duckdb
import pandas as pd
import streamlit as st

# local modules  – keep alias `ft` for simplicity
import backend.fetch_table as ft

DB = Path("data/warehouse.duckdb")
PARQUET_DIR = Path("data/parquet")

st.set_page_config(page_title="Knesset OData – refresh & export", layout="wide")


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _connect(read_only: bool = True):
    return duckdb.connect(DB.as_posix(), read_only=read_only)


def _human_ts(ts):
    if not ts:
        return "never"
    return datetime.fromtimestamp(ts.timestamp(), tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC"
    )


def _last_loaded(table: str) -> str:
    try:
        with _connect() as con:
            return _human_ts(
                con.sql(f"SELECT MAX(LastUpdatedDate) FROM {table}").fetchone()[0]
            )
    except Exception:
        return "never"


# --- NEW: Function to get filter options ---
@st.cache_data(ttl=3600)
def get_filter_options():
    """Fetches distinct Knesset numbers and Faction details from the DB."""
    knesset_nums = []
    factions = pd.DataFrame(columns=["FactionID", "Name", "KnessetNum"])
    if not DB.exists():
        return knesset_nums, factions
    try:
        with _connect() as con:
            tables_in_db = (
                con.execute(
                    "SELECT table_name FROM duckdb_tables() WHERE schema_name='main';"
                )
                .df()["table_name"]
                .tolist()
            )
            if "KNS_KnessetDates" in tables_in_db:
                knesset_nums = (
                    con.execute(
                        "SELECT DISTINCT KnessetNum FROM KNS_KnessetDates ORDER BY KnessetNum DESC"
                    )
                    .df()["KnessetNum"]
                    .tolist()
                )
            elif "KNS_Faction" in tables_in_db:
                knesset_nums = (
                    con.execute(
                        "SELECT DISTINCT KnessetNum FROM KNS_Faction ORDER BY KnessetNum DESC"
                    )
                    .df()["KnessetNum"]
                    .tolist()
                )
            if "KNS_Faction" in tables_in_db:
                factions = con.execute(
                    "SELECT FactionID, Name, KnessetNum FROM KNS_Faction ORDER BY KnessetNum DESC, Name"
                ).df()
    except Exception as e:
        st.warning(f"Could not fetch filter options: {e}")
    return knesset_nums, factions


# ──────────────────────────────────────────────────────────────────────────────
# Sidebar  – fetch controls
# ──────────────────────────────────────────────────────────────────────────────
st.sidebar.header("🔄 Refresh controls")

all_tables = ft.TABLES
selected = st.sidebar.multiselect(
    "Which tables to refresh (blank = all)", all_tables, placeholder="all tables"
)

run_now = st.sidebar.button("🚀 Fetch selected tables now")

# --- NEW: Filter Widgets in Sidebar ---
knesset_nums_options, factions_options_df = get_filter_options()

faction_display_map = {
    f"{row['Name']} (Knesset {row['KnessetNum']})": row["FactionID"]
    for index, row in factions_options_df.iterrows()
}
faction_id_to_display_map = {v: k for k, v in faction_display_map.items()}

st.sidebar.divider()
st.sidebar.header("📊 Data Filters")

selected_knessets = st.sidebar.multiselect(
    "Filter by Knesset Number:",
    options=knesset_nums_options,
)

selected_faction_names = st.sidebar.multiselect(
    "Filter by Faction:",
    options=list(faction_display_map.keys()),
)
selected_faction_ids = [faction_display_map[name] for name in selected_faction_names]

# ──────────────────────────────────────────────────────────────────────────────
# Main area  – status + optional SQL + downloads
# ──────────────────────────────────────────────────────────────────────────────
st.title("Knesset warehouse – self-service console")

# last-update summary
lasts = {t: _last_loaded(t) for t in all_tables}
last_df = pd.DataFrame({"Table": lasts.keys(), "Last updated": lasts.values()})

col1, col2 = st.columns([2, 1])
col1.dataframe(last_df, hide_index=True, use_container_width=True)

with col2:
    st.markdown(
        dedent(
            """
            #### How this works
            * **Select** the tables on the left (or leave empty for *all*).
            * Click **Fetch** – the app calls `fetch_table.refresh_tables()` and streams progress below.
            * When done, use the **Downloads** section to grab the ready-made views.
            """
        )
    )

progress_area = st.empty()

if run_now:

    def _progress_cb(table, rows):
        progress_area.write(f"✔ **{table}** – {rows:,} rows done")

    with st.status("Downloading …", expanded=True):
        tables = selected if selected else None
        asyncio.run(ft.refresh_tables(tables=tables, progress_cb=_progress_cb))
    st.success("Finished! Scroll down to download your data.")
    # Clear filter cache and rerun to update options
    st.cache_data.clear()
    st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# NEW: Interactive Data Explorer
# ──────────────────────────────────────────────────────────────────────────────
st.divider()
st.header("🔬 Interactive Data Explorer")

explorer_col1, explorer_col2 = st.columns([1, 3])

with explorer_col1:
    try:
        with _connect() as con:
            available_tables_in_db = (
                con.execute(
                    "SELECT table_name FROM duckdb_tables() WHERE schema_name='main';"
                )
                .df()["table_name"]
                .tolist()
            )
            tables_for_explorer = [t for t in ft.TABLES if t in available_tables_in_db]
            if not tables_for_explorer:
                st.warning("No tables found in the database for exploration.")
                selected_table_to_explore = None
            else:
                selected_table_to_explore = st.selectbox(
                    "Select table to explore:", options=tables_for_explorer
                )
    except Exception as e:
        st.warning(f"Could not list tables from DB: {e}")
        selected_table_to_explore = None

with explorer_col2:
    st.write(f"Filters applied:")
    st.write(f"* **Knesset(s):** {selected_knessets or 'All'}")
    st.write(f"* **Faction(s):** {selected_faction_names or 'All'}")

if selected_table_to_explore:
    try:
        with _connect() as con:
            # Base query parts
            select_clause = f"SELECT DISTINCT P.*"  # Select distinct persons
            from_clause = (
                f"FROM {selected_table_to_explore} P"  # Alias the main table as P
            )
            join_clause = ""
            where_clauses = []

            # Get columns of the main selected table
            main_table_columns = (
                con.execute(f"PRAGMA table_info('{selected_table_to_explore}')")
                .df()["name"]
                .tolist()
            )

            # Check if filtering requires joining with KNS_PersonToPosition
            needs_join_for_filters = False
            target_filter_table = "P"  # Default to filtering the main table

            if selected_table_to_explore == "KNS_Person" and (
                selected_knessets or selected_faction_ids
            ):
                # Check if KNS_PersonToPosition table exists
                tables_in_db = (
                    con.execute(
                        "SELECT table_name FROM duckdb_tables() WHERE schema_name='main';"
                    )
                    .df()["table_name"]
                    .tolist()
                )
                if "KNS_PersonToPosition" in tables_in_db:
                    needs_join_for_filters = True
                    target_filter_table = "P2P"
                    join_clause = f"INNER JOIN KNS_PersonToPosition P2P ON P.PersonID = P2P.PersonID"
                    # Only MKs or Ministers (PositionID 61, 43)
                    where_clauses.append(f"P2P.PositionID IN (61, 43)")
                else:
                    st.warning(
                        "Cannot filter KNS_Person by Knesset/Faction: KNS_PersonToPosition table not found in database."
                    )

            # Add KnessetNum filter (applied to correct table alias)
            if selected_knessets:
                if needs_join_for_filters or "KnessetNum" in main_table_columns:
                    knesset_list_str = ", ".join(map(str, selected_knessets))
                    where_clauses.append(
                        f"{target_filter_table}.KnessetNum IN ({knesset_list_str})"
                    )

            # Add FactionID filter (applied to correct table alias)
            if selected_faction_ids:
                if needs_join_for_filters or "FactionID" in main_table_columns:
                    faction_list_str = ", ".join(map(str, selected_faction_ids))
                    where_clauses.append(
                        f"{target_filter_table}.FactionID IN ({faction_list_str})"
                    )

            # --- Combine Query Parts ---
            if selected_table_to_explore == "KNS_Person" and (
                selected_knessets or selected_faction_ids
            ):
                query = select_clause + " " + from_clause + " " + join_clause
            else:
                # Default: no join, no aliasing
                query = f"SELECT * FROM {selected_table_to_explore}"
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
                if "LastUpdatedDate" in main_table_columns:
                    query += " ORDER BY LastUpdatedDate DESC"
                elif any(
                    pk in main_table_columns
                    for pk in [
                        "BillID",
                        "PersonID",
                        "CommitteeSessionID",
                        "PlenumSessionID",
                        "QueryID",
                    ]
                ):
                    pk_col = next(
                        pk
                        for pk in [
                            "BillID",
                            "PersonID",
                            "CommitteeSessionID",
                            "PlenumSessionID",
                            "QueryID",
                        ]
                        if pk in main_table_columns
                    )
                    query += f" ORDER BY {pk_col} DESC"
                query += " LIMIT 1000"

            if selected_table_to_explore == "KNS_Person" and (
                selected_knessets or selected_faction_ids
            ):
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
                if "LastUpdatedDate" in main_table_columns:
                    query += " ORDER BY P.LastUpdatedDate DESC"
                elif "PersonID" in main_table_columns:
                    query += f" ORDER BY P.PersonID DESC"
                query += " LIMIT 1000"

            st.write(f"Running query:")
            st.code(query, language="sql")
            filtered_df = con.execute(query).df()
            st.dataframe(filtered_df, use_container_width=True)
            if not filtered_df.empty:
                csv_filtered = filtered_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="⬇️ Download Filtered Data (CSV)",
                    data=csv_filtered,
                    file_name=f"filtered_{selected_table_to_explore}.csv",
                    mime="text/csv",
                )
    except Exception as e:
        st.error(f"❌ Failed to query table '{selected_table_to_explore}': {e}")

# ──────────────────────────────────────────────────────────────────────────────
# Download buttons for curated views
# ──────────────────────────────────────────────────────────────────────────────
st.divider()
st.header("📥 Ready-made exports")

EXPORTS = {
    # Level 1 – raw slice
    "Queries – basic": """
        SELECT QueryID, Number, KnessetNum, Name, TypeID, TypeDesc,
               StatusID, PersonID, GovMinistryID, SubmitDate,
               ReplyMinisterDate, ReplyDatePlanned, LastUpdatedDate
        FROM   KNS_Query
        ORDER  BY QueryID
    """,
    # Level 2 – enriched
    "Queries + ministry / MK names": """
        SELECT q.QueryID, q.Number, q.KnessetNum, q.Name, q.TypeDesc,
               q.StatusID,
               p.FirstName || ' ' || p.LastName AS MK,
               m.Name AS MinistryName,
               q.SubmitDate, q.ReplyMinisterDate, q.ReplyDatePlanned
        FROM   KNS_Query q
        LEFT   JOIN KNS_Person      p ON p.PersonID      = q.PersonID
        LEFT   JOIN KNS_GovMinistry m ON m.GovMinistryID = q.GovMinistryID
        ORDER  BY q.QueryID
    """,
    # Level 3 – quick aggregation
    "Query counts by ministry & year": (
        """
        WITH q AS (
          SELECT
            GovMinistryID,
            strftime('%Y', CAST(SubmitDate AS TIMESTAMP)) AS Yr
          FROM   KNS_Query
        )
        SELECT
          COALESCE(m.Name, 'Unknown ministry') AS Ministry,
          Yr,
          COUNT(*) AS Queries
        FROM   q
        LEFT JOIN KNS_GovMinistry m USING (GovMinistryID)
        GROUP BY 1, 2
        ORDER BY 1, 2;
        """
    ),
}

for label, sql in EXPORTS.items():
    try:
        with _connect() as con:
            df = con.sql(sql).df()
    except Exception as e:
        st.warning(f"{label}: {e}")
        continue

    # CSV
    csv_bytes = df.to_csv(index=False).encode()

    # Excel (temp file because pandas needs a path)
    tmp_xlsx = Path("/tmp/tmp.xlsx")
    with pd.ExcelWriter(tmp_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    xlsx_bytes = tmp_xlsx.read_bytes()

    col_a, col_b = st.columns(2)
    with col_a:
        st.download_button(
            f"⬇️ {label} (CSV)", csv_bytes, file_name=f"{label}.csv", mime="text/csv"
        )
    with col_b:
        st.download_button(
            f"⬇️ {label} (Excel)",
            xlsx_bytes,
            file_name=f"{label}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

# ──────────────────────────────────────────────────────────────────────────────
# Optional – sandbox SQL for power users
# ──────────────────────────────────────────────────────────────────────────────
with st.expander("🧑‍🔬 Run an ad-hoc SQL query (optional)", expanded=False):
    query = st.text_area(
        "SQL to run against the warehouse",
        "SELECT table_name,\n"
        "       estimated_size/1024/1024 AS size_mb\n"
        "FROM   duckdb_tables()\n"
        "WHERE  schema_name = 'main';",
        height=140,
    )

    if st.button("▶︎ Run SQL"):
        try:
            with _connect() as con:  # <- keeps the connection alive
                df = con.sql(query).df()
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"❌ {e}")

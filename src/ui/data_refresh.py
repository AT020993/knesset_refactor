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


# ──────────────────────────────────────────────────────────────────────────────
# Sidebar  – fetch controls
# ──────────────────────────────────────────────────────────────────────────────
st.sidebar.header("🔄 Refresh controls")

all_tables = ft.TABLES
selected = st.sidebar.multiselect(
    "Which tables to refresh (blank = all)", all_tables, placeholder="all tables"
)

run_now = st.sidebar.button("🚀 Fetch selected tables now")

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

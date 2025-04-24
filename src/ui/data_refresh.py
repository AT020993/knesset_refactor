### src/ui/data_refresh.py – self‑service GUI for the researcher
"""Streamlit front‑end that lets a *non‑coder*:
1. Pick which tables to refresh (or all).
2. See live progress + when each table was last updated.
3. Download the curated views you prepare for them (CSV / Excel).
4. (Optional power corner) run an ad‑hoc SQL query against the warehouse.

How to launch
-------------
streamlit run src/ui/data_refresh.py

Dependencies
------------
pip install streamlit duckdb pandas openpyxl  # rest already installed
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

import duckdb
import pandas as pd
import streamlit as st

# local modules
import fetch_table as ft  # the big helper you just refactored

DB = Path("data/warehouse.duckdb")
PARQUET_DIR = Path("data/parquet")

st.set_page_config(page_title="Knesset OData – refresh & export", layout="wide")

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _connect(read_only: bool = True):
    return duckdb.connect(DB.as_posix(), read_only=read_only)


def _human_ts(ts):
    if not ts:
        return "never"
    return datetime.fromtimestamp(ts.timestamp(), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _last_loaded(table: str) -> str:
    try:
        with _connect() as con:
            return _human_ts(con.sql(f"SELECT MAX(LastUpdatedDate) FROM {table}").fetchone()[0])
    except Exception:
        return "never"

# -----------------------------------------------------------------------------
# Sidebar – fetch controls
# -----------------------------------------------------------------------------

st.sidebar.header("🔄 Refresh controls")

all_tables = ft.TABLES
selected = st.sidebar.multiselect(
    "Which tables to refresh (blank = all)", all_tables, placeholder="all tables"
)

run_now = st.sidebar.button("🚀 Fetch selected tables now")

# -----------------------------------------------------------------------------
# Main area – status + optional SQL + downloads
# -----------------------------------------------------------------------------

st.title("Knesset warehouse – self‑service console")

# Show last‑update summary table
lasts = {t: _last_loaded(t) for t in all_tables}
last_df = pd.DataFrame({"Table": list(lasts.keys()), "Last updated": list(lasts.values())})
col1, col2 = st.columns([2, 1])
col1.dataframe(last_df, hide_index=True, use_container_width=True)

# quick info box
with col2:
    st.markdown(
        dedent(
            """
            #### How this works
            * **Select** the tables on the left (or leave empty for *all*).
            * Click **Fetch** – the app calls `fetch_table.refresh_tables()` and streams progress below.
            * When done, use the **Downloads** section to grab the ready‑made views.
            """
        )
    )

# Progress placeholder
progress_area = st.empty()

if run_now:

    def _progress_cb(table, rows):
        progress_area.write(f"✔ **{table}** – {rows:,} rows done")

    with st.status("Downloading …", expanded=True):
        tables = selected if selected else None
        asyncio.run(ft.refresh_tables(tables=tables, progress_cb=_progress_cb))
    st.success("Finished! Scroll down to download your data.")

# -----------------------------------------------------------------------------
# Download buttons for curated views
# -----------------------------------------------------------------------------

st.divider()
st.header("📥 Ready‑made exports")

EXPORTS = {
    "Queries – basic": "SELECT QueryID, Number, KnessetNum, Name, TypeID, TypeDesc, StatusID, PersonID, GovMinistryID, SubmitDate, ReplyMinisterDate, ReplyDatePlanned, LastUpdatedDate FROM KNS_Query ORDER BY QueryID",
    # add more curated SQL views here ↓↓↓
}

with _connect() as con:
    for label, sql in EXPORTS.items():
        try:
            df = con.sql(sql).df()
        except Exception as e:
            st.warning(f"{label}: {e}")
            continue
        csv_bytes = df.to_csv(index=False).encode()
        xlsx_bytes = pd.ExcelWriter | None
        with pd.ExcelWriter("/tmp/tmp.xlsx", engine="openpyxl") as writer:  # type: ignore[arg-type]
            df.to_excel(writer, index=False)
        xlsx_bytes = Path("/tmp/tmp.xlsx").read_bytes()

        col_a, col_b = st.columns(2)
        with col_a:
            st.download_button(f"⬇️ {label} (CSV)", csv_bytes, file_name=f"{label}.csv", mime="text/csv")
        with col_b:
            st.download_button(f"⬇️ {label} (Excel)", xlsx_bytes, file_name=f"{label}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# -----------------------------------------------------------------------------
# Optional – sandbox SQL for power users
# -----------------------------------------------------------------------------

with st.expander("🧑‍🔬 Run an ad‑hoc SQL query (optional)", expanded=False):
    query = st.text_area("SQL", "SELECT table_name, row_count FROM duckdb_tables();", height=120)
    if st.button("▶︎ Run SQL"):
        try:
            df = _connect().sql(query).df()
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(e)

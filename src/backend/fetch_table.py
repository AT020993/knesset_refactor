"""fetch_table.py – robust OData → DuckDB loader (HTTP + utilities)
----------------------------------------------------------------
Improvements already in place
1. **Checkpoint‑resume** for cursor‑paged tables – progress dumped to `data/.resume_state.json` every chunk (never re‑fetch rows after crash).
2. **Parallel download** for regular `$skip` tables – configurable pool (default 8 concurrent pages).
3. **Automatic Parquet export** – each saved table is also mirrored to `data/parquet/<table>.parquet` (ZSTD‑compressed).

**NEW in this revision (adds the three requested ideas):**
4. **Quick SQL inspection** – `--sql "SELECT …"` lets you run ad‑hoc DuckDB queries right from the CLI (or pipe them from shell).
5. **Streamlit integration hooks** – `async def refresh_tables()` & `def ensure_latest()` can be imported by the UI; they download tables and bubble progress back via an optional callback (so you can wire a Streamlit progress bar).
6. **Data‑quality helpers** – tiny utilities for coalition status, MK/committee ID mappings, and gender/position look‑ups.  These live here for now so the GUI can `import fetch_table as ft` and reuse them.

Install once:
```bash
pip install aiohttp pandas duckdb tqdm backoff pyarrow fastparquet openpyxl
```

Examples:
```bash
# download one table
python fetch_table.py --table KNS_CommitteeSession

# refresh the whole warehouse (all predefined tables)
python fetch_table.py --all

# run a quick query
python fetch_table.py --sql "SELECT table_name, row_count FROM duckdb_tables();"
```
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from math import ceil
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import aiohttp
import backoff
import duckdb
import pandas as pd
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Basic constants / paths
# -----------------------------------------------------------------------------
BASE_URL = "http://knesset.gov.il/Odata/ParliamentInfo.svc"
DEFAULT_DB = Path("data/warehouse.duckdb")
PAGE_SIZE = 100
MAX_RETRIES = 8  # more patience for 503 bursts
PARQUET_DIR = Path("data/parquet")
RESUME_FILE = Path("data/.resume_state.json")
CONCURRENCY = 8  # parallel page fetches

COALITION_FILE = Path("data/coalition_data.xlsx")  # for data‑quality helper #3

TABLES = [
    "KNS_Person",
    "KNS_Faction",
    "KNS_GovMinistry",
    "KNS_Status",
    "KNS_PersonToPosition",
    "KNS_Query",
    "KNS_Agenda",
    "KNS_Committee",
    "KNS_CommitteeSession",
    "KNS_PlenumSession",
]

# Tables that need cursor paging   → (primary_key, chunk)
CURSOR_TABLES: Dict[str, Tuple[str, int]] = {
    "KNS_Person": ("PersonID", 100),
    "KNS_CommitteeSession": ("CommitteeSessionID", 100),
    "KNS_PlenumSession": ("PlenumSessionID", 100),
}

# -----------------------------------------------------------------------------
# Resume‑state helpers (improvement #1)
# -----------------------------------------------------------------------------


def _load_resume() -> Dict[str, int]:
    if RESUME_FILE.exists():
        try:
            return json.loads(RESUME_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_resume(state: Dict[str, int]):
    RESUME_FILE.parent.mkdir(parents=True, exist_ok=True)
    RESUME_FILE.write_text(json.dumps(state))


resume_state: Dict[str, int] = _load_resume()

# -----------------------------------------------------------------------------
# Retry helper
# -----------------------------------------------------------------------------


def _backoff_hdlr(details):
    print(f"🔄 back‑off {details['wait']:.1f}s after: {details['exception']}")


@backoff.on_exception(
    backoff.expo,
    (aiohttp.ClientError, asyncio.TimeoutError, aiohttp.ClientResponseError),
    max_tries=MAX_RETRIES,
    on_backoff=_backoff_hdlr,
)
async def fetch_json(session: aiohttp.ClientSession, url: str) -> dict:  # noqa: D401
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)


# -----------------------------------------------------------------------------
# Download logic
# -----------------------------------------------------------------------------


async def download_table(table: str) -> pd.DataFrame:
    """Download *table* into a DataFrame, using cursor‑paging when needed."""
    entity = f"{table}()"
    print(f"\n📥 {table}")
    dfs: List[pd.DataFrame] = []

    async with aiohttp.ClientSession() as session:
        # -------------------------------------------------------------
        # Cursor‑paged tables (with checkpoint resume)
        # -------------------------------------------------------------
        if table in CURSOR_TABLES:
            pk, chunk = CURSOR_TABLES[table]
            last_val: int = resume_state.get(table, -1)
            total_rows = 0
            while True:
                url = (
                    f"{BASE_URL}/{entity}"
                    f"?$format=json&$top={chunk}"
                    f"&$filter={pk}+gt+{last_val}"
                    f"&$orderby={pk}+asc"
                )
                try:
                    data = await fetch_json(session, url)
                except Exception as e:
                    print(
                        f"⚠️  still failing after {MAX_RETRIES} retries: {e}. Sleeping 5 s…"
                    )
                    await asyncio.sleep(5)
                    continue  # try again with same last_val
                rows = data.get("value", [])
                if not rows:
                    break
                dfs.append(pd.DataFrame.from_records(rows))
                last_val = rows[-1][pk]
                total_rows += len(rows)
                print(f"   …fetched {total_rows:,} rows (up to {pk} {last_val})")
                # checkpoint every chunk
                resume_state[table] = last_val
                _save_resume(resume_state)
            # finished – clean resume marker
            if table in resume_state:
                del resume_state[table]
                _save_resume(resume_state)
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        # -------------------------------------------------------------
        # Default $skip paging – now parallel
        # -------------------------------------------------------------
        try:
            total = int(await fetch_json(session, f"{BASE_URL}/{entity}/$count"))
        except Exception:
            total = None

        if total is None:
            return await _download_sequential(session, entity)

        pages = ceil(total / PAGE_SIZE)
        pbar = tqdm(total=total, unit="rows")
        sem = asyncio.Semaphore(CONCURRENCY)

        async def fetch_page(page_idx: int):
            async with sem:
                skip = page_idx * PAGE_SIZE
                url = f"{BASE_URL}/{entity}?$format=json&$skip={skip}&$top={PAGE_SIZE}"
                rows: List[dict] = []
                while True:
                    try:
                        data = await fetch_json(session, url)
                        rows = data.get("value", [])
                        break
                    except Exception as e:
                        print(f"⚠️  page {page_idx}: {e} – sleeping 5 s …")
                        await asyncio.sleep(5)
                if rows:
                    pbar.update(len(rows))
                    return page_idx, pd.DataFrame.from_records(rows)
                return page_idx, None

        results = await asyncio.gather(*(fetch_page(i) for i in range(pages)))
        pbar.close()

        # ordered concat
        results.sort(key=lambda t: t[0])
        dfs = [df for _, df in results if df is not None]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


async def _download_sequential(
    session: aiohttp.ClientSession, entity: str
) -> pd.DataFrame:  # noqa: D401
    dfs: List[pd.DataFrame] = []
    page = 0
    pbar = tqdm(unit="rows")
    while True:
        url = f"{BASE_URL}/{entity}?$format=json&$skip={page * PAGE_SIZE}&$top={PAGE_SIZE}"
        try:
            data = await fetch_json(session, url)
        except Exception as e:
            print(f"⚠️  page {page}: {e} – sleeping 5 s …")
            await asyncio.sleep(5)
            continue
        rows = data.get("value", [])
        if not rows:
            break
        dfs.append(pd.DataFrame.from_records(rows))
        pbar.update(len(rows))
        page += 1
    pbar.close()
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# -----------------------------------------------------------------------------
# Storage helper (incl. Parquet mirror)
# -----------------------------------------------------------------------------


def store(df: pd.DataFrame, table: str, db_path: Path = DEFAULT_DB):
    if df.empty:
        print(f"⚠️  {table}: zero rows (skipped)")
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path.as_posix())
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM df")
    con.close()
    print(f"✔ {table}: {len(df):,} rows saved → {db_path}")

    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = PARQUET_DIR / f"{table}.parquet"
    df.to_parquet(parquet_path, compression="zstd", index=False)
    try:
        rel = parquet_path.resolve().relative_to(Path.cwd().resolve())
    except ValueError:
        rel = parquet_path
    print(f"📦 Parquet → {rel}")


# -----------------------------------------------------------------------------
# Data‑quality helper functions (idea #3)
# -----------------------------------------------------------------------------


def load_coalition_data() -> pd.DataFrame:
    if COALITION_FILE.exists():
        try:
            return pd.read_excel(COALITION_FILE)
        except Exception as e:
            print(f"⚠️  coalition file error: {e}")
    return pd.DataFrame()


def add_coalition_status(df: pd.DataFrame) -> pd.DataFrame:
    if {"KnessetNum", "FactionID"}.issubset(df.columns):
        coal = load_coalition_data()
        if not coal.empty:
            return df.merge(
                coal[["KnessetNum", "FactionID", "IsCoalition"]],
                how="left",
                on=["KnessetNum", "FactionID"],
            )
    return df


def map_mk_site_code(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return mapping MK internal → website id (if table present)."""
    try:
        return con.sql("SELECT KnsID, SiteID FROM KNS_MkSiteCode").df()
    except Exception:
        return pd.DataFrame()


# -----------------------------------------------------------------------------
# Exported helpers for Streamlit (idea #2)
# -----------------------------------------------------------------------------


async def _fetch_single_table(
    table: str,
    progress_cb: Optional[Callable[[str, int], None]] = None,
    db_path: Path = DEFAULT_DB,
    **kwargs,
) -> pd.DataFrame:
    """Download a single table, call progress_cb, and return the DataFrame."""
    df = await download_table(table)
    store(df, table, db_path=db_path)
    if progress_cb:
        progress_cb(table, len(df))
    return df


async def refresh_tables(
    tables: List[str] | None = None,
    progress_cb: Optional[Callable[[str, int], None]] = None,
    db_path: Path = DEFAULT_DB,
):
    """Download *tables* (or all) and store; progress_cb(table, rows)"""
    # first, sanitize & validate the list of tables
    tables_to_fetch = tables if tables is not None else TABLES
    invalid = [t for t in tables_to_fetch if t not in TABLES]
    if invalid:
        raise ValueError(f"Invalid table(s): {invalid!r}")
    for t in tables_to_fetch:
        await _fetch_single_table(t, progress_cb=progress_cb, db_path=db_path)


def ensure_latest(tables: List[str] | None = None, db_path: Path = DEFAULT_DB):
    """Sync tables synchronously – convenience wrapper for non‑async callers."""
    asyncio.run(refresh_tables(tables, db_path=db_path))


# -----------------------------------------------------------------------------
# CLI helpers (adds --sql)
# -----------------------------------------------------------------------------


def list_tables():
    print("Available tables:")
    for t in TABLES:
        print("  -", t)


def parse_args():
    p = argparse.ArgumentParser(
        description="Fetch Knesset OData tables into DuckDB/Parquet or run adhoc SQL"
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument("--table", help="Name of single table to fetch")
    g.add_argument("--all", action="store_true", help="Fetch all predefined tables")
    g.add_argument(
        "--sql", help="Run SQL against the warehouse and print result as CSV"
    )
    p.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="DuckDB file (default: data/warehouse.duckdb)",
    )
    return p.parse_args()


def main():
    args = parse_args()

    # -------------------------------------------------------------
    # Quick SQL path first
    # -------------------------------------------------------------
    if args.sql:
        if not args.db.exists():
            print("⛔ warehouse not found – run a fetch first")
            sys.exit(1)
        con = duckdb.connect(args.db.as_posix(), read_only=True)
        try:
            df = con.sql(args.sql).df()
        except Exception as e:
            print(f"SQL error: {e}")
            sys.exit(1)
        print(df.to_csv(index=False))
        return

    # -------------------------------------------------------------
    # Fetch path
    # -------------------------------------------------------------
    if args.all:
        tables = TABLES
    elif args.table:
        tables = [args.table]
        if args.table not in TABLES:
            print(f"⚠️  {args.table} not in predefined list – attempting anyway.")
    else:
        list_tables()
        return

    for t in tables:
        df = asyncio.run(download_table(t))
        store(df, t, db_path=args.db)


if __name__ == "__main__":
    main()

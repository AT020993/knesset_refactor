"""DuckDB table writer + atomic Parquet snapshot writer."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

TABLE_NAME = "bill_classifications"


def write_duckdb_table(df: pd.DataFrame, *, db_path: Path) -> None:
    """Replace the ``bill_classifications`` table with ``df`` contents.

    Uses DuckDB's DataFrame registration to avoid row-by-row inserts.
    Table is dropped and recreated each call (cheap — ~25K rows).
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    df_stable = df.sort_values("BillID", kind="stable").reset_index(drop=True)

    con = duckdb.connect(str(db_path), read_only=False)
    try:
        con.register("df_in", df_stable)
        con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM df_in ORDER BY BillID")
        con.unregister("df_in")
    finally:
        con.close()
    log.info("Wrote %d rows to DuckDB table %s", len(df_stable), TABLE_NAME)


def write_parquet_snapshot(df: pd.DataFrame, output_path: Path) -> None:
    """Write DataFrame to Parquet with atomic rename + stable ORDER BY.

    Replicates the idempotence pattern from src/data/snapshots/exporter.py:
    write to ``<path>.new`` then ``os.replace`` into place.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df_stable = df.sort_values("BillID", kind="stable").reset_index(drop=True)
    table = pa.Table.from_pandas(df_stable, preserve_index=False)

    tmp = output_path.with_suffix(output_path.suffix + ".new")
    pq.write_table(table, tmp, compression="zstd", use_dictionary=True)
    os.replace(tmp, output_path)
    log.info("Wrote Parquet snapshot: %s (%d rows)", output_path, len(df_stable))

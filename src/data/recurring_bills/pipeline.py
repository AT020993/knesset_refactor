"""Orchestrator: fetch -> classify -> write outputs."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from data.recurring_bills.classify import (
    build_k16_k18_fallback,
    build_tal_classifications,
    merge_all,
)
from data.recurring_bills.fetch_tal import download_bulk_csv, fetch_many_details
from data.recurring_bills.report import compute_stats, render_markdown
from data.recurring_bills.storage import write_duckdb_table, write_parquet_snapshot

log = logging.getLogger(__name__)


def _bills_needing_detail(bulk_csv: Path) -> list[int]:
    """Return bill_ids from the bulk CSV with is_original = 0 (need patient_zero lookup)."""
    df = pd.read_csv(bulk_csv, usecols=["bill_id", "is_original"])
    return df.loc[df["is_original"] == 0, "bill_id"].astype(int).tolist()


def run_pipeline(
    *,
    mode: str,
    excel_path: Path,
    cache_dir: Path,
    bulk_csv: Path,
    db_path: Path,
    parquet_path: Path,
    report_path: Path,
    delay_s: float = 0.3,
    force_refresh: bool = False,
) -> dict:
    """Top-level orchestrator.

    Modes:
    - ``refresh``  : pull latest bulk CSV + fetch any missing detail, then classify + write
    - ``rebuild``  : classify from existing cache, write outputs (no network)
    - ``report``   : classify + write only the coverage report (skip DuckDB + Parquet)
    """
    assert mode in {"refresh", "rebuild", "report"}

    # Coerce to Path for robustness with Path-like arguments
    excel_path = Path(excel_path)
    cache_dir = Path(cache_dir)
    bulk_csv = Path(bulk_csv)
    db_path = Path(db_path)
    parquet_path = Path(parquet_path)
    report_path = Path(report_path)

    if mode == "refresh":
        bulk_csv = download_bulk_csv(bulk_csv)
        needing = _bills_needing_detail(bulk_csv)
        log.info("Fetching detail for %d recurring bills (delay=%.1fs)", len(needing), delay_s)
        fetch_many_details(needing, cache_dir, delay_s=delay_s, force_refresh=force_refresh)

    tal = build_tal_classifications(bulk_csv=bulk_csv, cache_dir=cache_dir)
    fb = build_k16_k18_fallback(excel_path)
    df = merge_all(tal=tal, fallback=fb)

    stats = compute_stats(df)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_markdown(stats))

    if mode != "report":
        write_parquet_snapshot(df, parquet_path)
        write_duckdb_table(df, db_path=db_path)

    log.info("Pipeline %s complete: %d bills", mode, stats["total"])
    return stats

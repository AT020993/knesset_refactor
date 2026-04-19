"""Orchestrator: fetch -> classify -> write outputs."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd

from data.recurring_bills.cap_view import create_cap_view
from data.recurring_bills.classify import (
    build_k16_k18_doc_based,
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
    k16_k18_method: str = "doc",
    knesset_docs_cache_dir: Path | None = None,
) -> dict:
    """Top-level orchestrator.

    Modes:
    - ``refresh``  : pull latest bulk CSV + fetch any missing detail, then classify + write
    - ``rebuild``  : classify from existing cache, write outputs (no network)
    - ``report``   : classify + write only the coverage report (skip DuckDB + Parquet)

    K16-K18 method (``k16_k18_method``):
    - ``doc``  (default): doc-based classification via fs.knesset.gov.il — Tal's method
    - ``name``: fast name-matching fallback (no network, no doc parsing)
    """
    assert mode in {"refresh", "rebuild", "report"}
    assert k16_k18_method in {"doc", "name"}

    # Coerce to Path for robustness with Path-like arguments
    excel_path = Path(excel_path)
    cache_dir = Path(cache_dir)
    bulk_csv = Path(bulk_csv)
    db_path = Path(db_path)
    parquet_path = Path(parquet_path)
    report_path = Path(report_path)
    if knesset_docs_cache_dir is None:
        knesset_docs_cache_dir = Path("data/external/knesset_docs")
    knesset_docs_cache_dir = Path(knesset_docs_cache_dir)

    if mode == "refresh":
        bulk_csv = download_bulk_csv(bulk_csv)
        needing = _bills_needing_detail(bulk_csv)
        log.info("Fetching detail for %d recurring bills (delay=%.1fs)", len(needing), delay_s)
        fetch_many_details(needing, cache_dir, delay_s=delay_s, force_refresh=force_refresh)

    tal = build_tal_classifications(bulk_csv=bulk_csv, cache_dir=cache_dir)

    if k16_k18_method == "doc":
        log.info("Building K16-K18 classification via doc analysis (cache=%s)", knesset_docs_cache_dir)
        fb = build_k16_k18_doc_based(
            excel_path=excel_path,
            cache_dir=knesset_docs_cache_dir,
            warehouse_path=db_path,
            delay_s=delay_s,
        )
    else:
        log.info("Building K16-K18 classification via name-matching")
        fb = build_k16_k18_fallback(excel_path)

    df = merge_all(tal=tal, fallback=fb)

    stats = compute_stats(df)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_markdown(stats))

    if mode != "report":
        write_parquet_snapshot(df, parquet_path)
        write_duckdb_table(df, db_path=db_path)
        try:
            create_cap_view(db_path=db_path)
        except duckdb.CatalogException:
            log.warning("UserBillCAP not present in warehouse; skipping CAP view creation")

    log.info("Pipeline %s complete: %d bills", mode, stats["total"])
    return stats

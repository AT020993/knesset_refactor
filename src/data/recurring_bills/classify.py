"""Merge Tal Alovitz's classifications + K16-K18 fallback into one DataFrame."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from data.recurring_bills.normalize import normalize_name

log = logging.getLogger(__name__)


def build_tal_classifications(
    *,
    bulk_csv: Path,
    cache_dir: Path,
) -> pd.DataFrame:
    """Build the tal_alovitz slice of the classifications table.

    Reads the bulk CSV for every bill's summary fields, then overlays each
    bill's cached detail JSON for ``patient_zero_bill_id``, ``predecessor_bill_ids``,
    and ``family_size``. Bills with no cached detail fall back to
    ``original_bill_id = bill_id`` (self) and NULL family info.
    """
    bulk = pd.read_csv(bulk_csv)
    bulk = bulk.rename(columns={"bill_id": "BillID", "knesset_num": "KnessetNum", "bill_name": "Name"})

    detail_rows: dict[int, dict] = {}
    cache_dir = Path(cache_dir)
    if cache_dir.exists():
        for path in cache_dir.glob("*.json"):
            try:
                payload = json.loads(path.read_text())
                detail_rows[int(payload["bill_id"])] = payload
            except (json.JSONDecodeError, KeyError) as exc:
                log.warning("Skipping malformed detail cache %s: %s", path, exc)

    def _patient_zero(row: pd.Series) -> int:
        d = detail_rows.get(int(row["BillID"]))
        if d and d.get("patient_zero_bill_id") is not None:
            return int(d["patient_zero_bill_id"])
        return int(row["BillID"])

    def _predecessors(row: pd.Series) -> list[int]:
        d = detail_rows.get(int(row["BillID"]))
        if d and d.get("predecessor_bill_ids"):
            return [int(x) for x in d["predecessor_bill_ids"]]
        return []

    def _family_size(row: pd.Series):
        d = detail_rows.get(int(row["BillID"]))
        return int(d["family_size"]) if d and d.get("family_size") is not None else None

    now = datetime.now(timezone.utc)
    bulk["is_original"] = bulk["is_original"].astype(bool)
    bulk["is_cross_term"] = bulk["is_cross_term"].astype(bool)
    bulk["is_within_term_dup"] = bulk["is_within_term_dup"].astype(bool)
    bulk["is_self_resubmission"] = bulk["is_self_resubmission"].astype(bool)
    bulk["original_bill_id"] = bulk.apply(_patient_zero, axis=1)
    bulk["predecessor_bill_ids"] = bulk.apply(_predecessors, axis=1)
    bulk["family_size"] = bulk.apply(_family_size, axis=1)
    bulk["tal_category"] = bulk["category"]
    bulk["classification_source"] = "tal_alovitz"
    bulk["tal_fetched_at"] = now
    bulk["last_updated"] = now

    keep = [
        "BillID", "KnessetNum", "Name",
        "is_original", "original_bill_id",
        "tal_category", "is_cross_term", "is_within_term_dup", "is_self_resubmission",
        "family_size", "predecessor_bill_ids",
        "classification_source", "tal_fetched_at", "last_updated",
    ]
    return bulk[keep].copy()


def build_k16_k18_fallback(excel_path: Path) -> pd.DataFrame:
    """Name-based fallback classification for K16-K18 bills from Amnon's Excel.

    Groups rows by ``normalize_name(Name)`` restricted to K16-K18. Within each
    group the earliest ``KnessetNum`` is marked original (ties broken by
    lowest ``BillID``); all others are reprises with ``original_bill_id``
    pointing to that earliest row.
    """
    xl = pd.read_excel(excel_path)
    xl = xl.loc[xl["KnessetNum"].between(16, 18)].copy()
    xl["_norm"] = xl["Name"].apply(normalize_name)

    # Sort so groupby picks earliest KnessetNum, then lowest BillID
    xl = xl.sort_values(["_norm", "KnessetNum", "BillID"], kind="stable")

    # First row in each normalized-name group is the original
    originals = xl.drop_duplicates("_norm", keep="first")[["_norm", "BillID"]].rename(
        columns={"BillID": "original_bill_id"}
    )
    xl = xl.merge(originals, on="_norm", how="left")
    xl["is_original"] = xl["BillID"] == xl["original_bill_id"]

    xl["predecessor_bill_ids"] = xl.apply(
        lambda r: [] if r["is_original"] else [int(r["original_bill_id"])],
        axis=1,
    )
    xl["classification_source"] = "name_fallback_k16_k18"
    xl["tal_category"] = None
    xl["is_cross_term"] = None
    xl["is_within_term_dup"] = None
    xl["is_self_resubmission"] = None
    xl["family_size"] = None
    xl["tal_fetched_at"] = None
    xl["last_updated"] = datetime.now(timezone.utc)

    keep = [
        "BillID", "KnessetNum", "Name",
        "is_original", "original_bill_id",
        "tal_category", "is_cross_term", "is_within_term_dup", "is_self_resubmission",
        "family_size", "predecessor_bill_ids",
        "classification_source", "tal_fetched_at", "last_updated",
    ]
    return xl[keep].reset_index(drop=True)


def build_k16_k18_doc_based(
    *,
    excel_path: Path,
    cache_dir: Path,
    warehouse_path: Path,
    delay_s: float = 0.3,
    progress_cb=None,
) -> pd.DataFrame:
    """Doc-based K16-K18 classification — Tal's method, locally reimplemented.

    For each K16-K18 bill in Amnon's Excel:
    - Download the first ``documents`` URL from ``fs.knesset.gov.il`` (cached)
    - Extract text via ``textutil`` (.doc/.docx) or ``pypdf`` (.pdf)
    - Scan for Hebrew recurrence markers
    - Resolve ``פ/NNN`` references to BillIDs via KNS_Bill.PrivateNumber

    Bills that fail fetch/parse, or match no pattern, are returned with
    ``is_original=True`` and ``classification_source='doc_based_k16_k18'``.
    Bills where the ancestor BillID couldn't be resolved report
    ``classification_source='doc_based_unresolved_k16_k18'`` for downstream audit.

    ``progress_cb`` is an optional callable ``cb(i, total, bill_id, method)``
    invoked after each bill — useful for logging/TUIs.
    """
    import duckdb

    from data.recurring_bills.knesset_docs import classify_bill_from_doc

    xl = pd.read_excel(excel_path)
    xl = xl.loc[xl["KnessetNum"].between(16, 18)].copy().reset_index(drop=True)

    con = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        results: list[dict] = []
        total = len(xl)
        for i, row in xl.iterrows():
            bid = int(row["BillID"])
            docs = row.get("documents")
            if not isinstance(docs, str) or not docs.strip():
                # No document URL — fall back to self-reference
                results.append({
                    "is_original": True,
                    "original_bill_id": bid,
                    "matched_phrase": None,
                    "method": "no_doc_url",
                })
            else:
                first_url = docs.split("\n")[0].strip()
                r = classify_bill_from_doc(
                    bill_id=bid,
                    current_knesset=int(row["KnessetNum"]),
                    doc_url=first_url,
                    cache_dir=cache_dir,
                    warehouse_con=con,
                    delay_s=delay_s,
                )
                is_recurring = r["is_recurring"]
                results.append({
                    "is_original": not is_recurring,
                    "original_bill_id": r["original_bill_id"] if r["original_bill_id"] else bid,
                    "matched_phrase": r["matched_phrase"],
                    "method": r["method"],
                })
            if progress_cb:
                progress_cb(int(i) + 1, total, bid, results[-1]["method"])
    finally:
        con.close()

    res_df = pd.DataFrame(results)
    xl = pd.concat([xl.reset_index(drop=True), res_df], axis=1)

    xl["predecessor_bill_ids"] = xl.apply(
        lambda r: [] if r["is_original"] else [int(r["original_bill_id"])],
        axis=1,
    )
    # Finer-grained source so downstream audit can distinguish confidence
    xl["classification_source"] = xl["method"].map({
        "doc_pattern_linked":     "doc_based_k16_k18",
        "doc_pattern_unresolved": "doc_based_unresolved_k16_k18",
        "doc_no_pattern":         "doc_based_k16_k18",
        "doc_fetch_failed":       "doc_based_fetch_failed_k16_k18",
        "no_doc_url":             "no_doc_url_k16_k18",
    }).fillna("doc_based_k16_k18")
    xl["tal_category"] = None
    xl["is_cross_term"] = None
    xl["is_within_term_dup"] = None
    xl["is_self_resubmission"] = None
    xl["family_size"] = None
    xl["tal_fetched_at"] = None
    xl["last_updated"] = datetime.now(timezone.utc)

    keep = [
        "BillID", "KnessetNum", "Name",
        "is_original", "original_bill_id",
        "tal_category", "is_cross_term", "is_within_term_dup", "is_self_resubmission",
        "family_size", "predecessor_bill_ids",
        "classification_source", "tal_fetched_at", "last_updated",
    ]
    return xl[keep].reset_index(drop=True)


def merge_all(*, tal: pd.DataFrame, fallback: pd.DataFrame) -> pd.DataFrame:
    """Union the Tal and K16-K18 fallback DataFrames.

    On BillID collisions (rare — Tal and fallback are supposed to be disjoint),
    Tal's row wins. Returns a stable-sorted DataFrame (by BillID) ready for
    writing.
    """
    fallback_filtered = fallback.loc[~fallback["BillID"].isin(set(tal["BillID"]))]
    combined = pd.concat([tal, fallback_filtered], ignore_index=True)
    return combined.sort_values("BillID", kind="stable").reset_index(drop=True)

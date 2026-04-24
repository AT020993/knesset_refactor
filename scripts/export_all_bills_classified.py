#!/usr/bin/env python
"""Export the COMPLETE K1-K25 private-bill classification — warehouse-backed.

Per Prof. Amnon's 2026-04-21 follow-up: he disowns his original Excel
(wasn't involved in collecting it, doubts reliability) and wants a
complete dataset backed by the Knesset OData warehouse (``KNS_Bill``)
rather than his file.

This script produces ``data/snapshots/All_Private_Bills_K1_K25_classified.xlsx``
covering EVERY private bill in the warehouse (51,673 rows across K1-K25)
with:

- Bill identity: BillID, KnessetNum, Name, PrivateNumber, SubTypeDesc
- Document link: doc_url (the fs.knesset.gov.il link our classifier read)
- Direct citation columns: direct_reference, cited_references, cited_bill_ids
- Effective classification: is_effective_original, effective_original_bill_id,
  effective_original_knesset_num, effective_original_private_number
- Method provenance: method, matched_phrase, classification_source
- Option-C presentation: is_recurring_upstream, effective_original_reason

Option-C post-pass keeps the coding workflow intact: every recurring
bill's ``original_bill_id`` is transitively flattened to the deepest
raw-original ancestor inside the 51,673-row universe. The only bills
promoted to "effective original" are those with no resolvable ancestor
via our regex (``doc_fetch_failed``, ``no_doc_url``, or truly never
cited a predecessor).

Run:
    source .venv/bin/activate
    PYTHONPATH="./src" python scripts/export_all_bills_classified.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from data.recurring_bills.export_resolution import (  # noqa: E402
    add_reference_summary_columns,
    add_source_audit_columns,
    apply_option_c_post_pass,
    build_reference_resolution_sheet,
    classify_recurrence_type,
    enrich_from_final_original_bill_id,
    ensure_columns,
    sanitize_submission_dates,
    suppress_source_metadata_reference_resolutions,
    strip_timezone_columns,
    verify_effective_originals,
)


def _build_data_dictionary() -> pd.DataFrame:
    rows = [
        (
            "source_knesset",
            "both",
            "raw/source",
            "Knesset number of the source bill/document.",
            "1-25",
        ),
        (
            "source_bill_id",
            "both",
            "raw/source",
            "KNS_Bill.BillID for the source bill.",
            "Integer",
        ),
        (
            "source_doc_id",
            "both",
            "extracted/source",
            "Document id parsed from the source fs.knesset.gov.il URL when available.",
            "Integer or blank",
        ),
        (
            "source_url",
            "both",
            "raw/source",
            "Document URL read by the classifier.",
            "URL or blank",
        ),
        (
            "reference_index",
            "Reference Resolution",
            "derived",
            "1-based index of the extracted reference evidence within a source bill/document.",
            "1, 2, 3...",
        ),
        (
            "reference_text_raw",
            "Reference Resolution",
            "raw/extracted",
            "Raw local text snippet that carried the recurrence/reference evidence.",
            "Text",
        ),
        (
            "target_knesset_extracted",
            "Reference Resolution",
            "extracted/target",
            "Target Knesset number explicitly present in the reference text. Never copied from source URL or source bill metadata.",
            "1-25 or blank",
        ),
        (
            "target_bill_number_extracted",
            "Reference Resolution",
            "extracted/target",
            "Target private bill number explicitly present in the reference text.",
            "Integer or blank",
        ),
        (
            "target_url_extracted",
            "Reference Resolution",
            "extracted/target",
            "Target URL explicitly present in the reference text. Current parser does not extract target URLs from these documents.",
            "URL or blank",
        ),
        (
            "target_resolution_status",
            "both",
            "final",
            "Final target-resolution status after suppressing source-metadata fallbacks.",
            "resolved; unresolved_no_link_or_number; unresolved_missing_target_knesset; unresolved_no_matching_bill; unresolved_ambiguous; unresolved_suspicious_self_reference; not_applicable_no_recurring_phrase",
        ),
        (
            "target_resolution_method",
            "both",
            "final",
            "Resolution method or unresolved reason emitted by the classifier/export guard.",
            "Method string",
        ),
        (
            "target_resolution_confidence",
            "both",
            "derived score",
            "Heuristic confidence from the resolution method. It is not manual-validation accuracy. Blank for unresolved rows.",
            "0.0-1.0 or blank",
        ),
        (
            "explicit_relation_type",
            "both",
            "extracted",
            "Relation class from explicit Hebrew phrases such as זהה or דומה. No legal amendment text comparison is performed.",
            "identical; similar; blank",
        ),
        (
            "final_relation_type",
            "both",
            "final",
            "Final relation type used in the deliverable. Currently the same as explicit_relation_type because classification is phrase-based only.",
            "identical; similar; unknown; blank",
        ),
        (
            "relation_evidence_text",
            "Reference Resolution",
            "raw/extracted",
            "Text snippet supporting explicit_relation_type/final_relation_type.",
            "Text",
        ),
        (
            "notes",
            "both",
            "derived",
            "Additional clarification, including suppressed stale fallback resolutions.",
            "Text or blank",
        ),
        (
            "warnings",
            "both",
            "derived",
            "Warnings for unresolved, ambiguous, suspicious, or low-confidence rows.",
            "Text or blank",
        ),
        (
            "resolved_target_bill_id",
            "Reference Resolution",
            "final/target",
            "Resolved KNS_Bill.BillID for the target when independently resolved.",
            "Integer or blank",
        ),
        (
            "source_metadata_resolution_suppressed",
            "Reference Resolution",
            "derived",
            "True when a stale same/prior-Knesset or name fallback was suppressed.",
            "True/False",
        ),
        (
            "BillID",
            "Classified Bills",
            "raw/source",
            "Legacy source bill id alias retained for compatibility.",
            "Integer",
        ),
        (
            "KnessetNum",
            "Classified Bills",
            "raw/source",
            "Legacy source Knesset alias retained for compatibility.",
            "1-25",
        ),
        (
            "Name",
            "Classified Bills",
            "raw/source",
            "Source bill title from KNS_Bill.",
            "Text",
        ),
        (
            "PrivateNumber",
            "Classified Bills",
            "raw/source",
            "Source bill private number from KNS_Bill.",
            "Integer or blank",
        ),
        (
            "SubTypeDesc",
            "Classified Bills",
            "raw/source",
            "Source bill subtype description from KNS_Bill.",
            "Text",
        ),
        (
            "doc_url",
            "Classified Bills",
            "raw/source",
            "Legacy source document URL alias retained for compatibility.",
            "URL or blank",
        ),
        (
            "direct_reference_bill_id",
            "Classified Bills",
            "derived/target",
            "Selected direct target BillID only when independently resolved.",
            "Integer or blank",
        ),
        (
            "direct_reference_knesset_num",
            "Classified Bills",
            "derived/target",
            "Knesset number for direct_reference_bill_id from KNS_Bill.",
            "1-25 or blank",
        ),
        (
            "direct_reference_private_number",
            "Classified Bills",
            "derived/target",
            "Private number for direct_reference_bill_id from KNS_Bill.",
            "Integer or blank",
        ),
        (
            "direct_reference",
            "Classified Bills",
            "derived/target",
            "Readable direct reference as Knesset/private-number.",
            "e.g. 18/3068 or blank",
        ),
        (
            "cited_reference_count",
            "Classified Bills",
            "derived",
            "Number of independently resolved candidate target bills surfaced in reference_candidates.",
            "Integer",
        ),
        (
            "cited_bill_ids",
            "Classified Bills",
            "derived/target",
            "Semicolon-separated resolved candidate BillIDs.",
            "Text or blank",
        ),
        (
            "cited_references",
            "Classified Bills",
            "derived/target",
            "Semicolon-separated Knesset/private-number labels for resolved candidates.",
            "Text or blank",
        ),
        (
            "is_effective_original",
            "Classified Bills",
            "final",
            "True when this row should be treated as codable original after unresolved ancestor promotion.",
            "True/False",
        ),
        (
            "effective_original_bill_id",
            "Classified Bills",
            "final",
            "Final codable ancestor BillID. Equals source bill for originals or unresolved references.",
            "Integer",
        ),
        (
            "effective_original_knesset_num",
            "Classified Bills",
            "final",
            "Knesset number for effective_original_bill_id.",
            "1-25 or blank",
        ),
        (
            "effective_original_private_number",
            "Classified Bills",
            "final",
            "Private number for effective_original_bill_id.",
            "Integer or blank",
        ),
        (
            "is_recurring_upstream",
            "Classified Bills",
            "derived",
            "True when the doc scan found an explicit recurrence phrase before effective-original promotion.",
            "True/False",
        ),
        (
            "effective_original_reason",
            "Classified Bills",
            "final",
            "Why an unresolved recurring row was promoted to effective original.",
            "doc_no_ancestor_found; ambiguous_doc_reference; suspicious_self_reference_only; doc_fetch_failed; no_doc_url; ancestor_outside_universe; blank",
        ),
        (
            "method",
            "Classified Bills",
            "derived",
            "Classifier method outcome for the source bill.",
            "doc_pattern_linked; doc_pattern_unresolved; doc_no_pattern; doc_fetch_failed; no_doc_url",
        ),
        (
            "matched_phrase",
            "Classified Bills",
            "extracted",
            "First explicit Hebrew recurrence phrase matched in the source text.",
            "Text or blank",
        ),
        (
            "classification_source",
            "Classified Bills",
            "raw/derived",
            "Classification table source label.",
            "doc_based_full",
        ),
        (
            "reference_candidate_count",
            "Classified Bills",
            "derived",
            "Number of raw reference evidence candidates stored for the source bill.",
            "Integer",
        ),
        (
            "reference_resolution_reason",
            "Classified Bills",
            "derived",
            "Legacy resolution reason; target_resolution_method is the clearer alias.",
            "Method string or blank",
        ),
        (
            "reference_resolution_confidence",
            "Classified Bills",
            "derived score",
            "Legacy heuristic confidence; target_resolution_confidence is the clearer alias.",
            "0.0-1.0 or blank",
        ),
        (
            "multiple_references_detected",
            "Classified Bills",
            "derived",
            "True when more than one reference evidence item was detected.",
            "True/False",
        ),
        (
            "suspicious_self_resolution",
            "Classified Bills",
            "derived",
            "True when the only resolution points back to the source bill.",
            "True/False",
        ),
        (
            "ambiguous_reference_resolution",
            "Classified Bills",
            "derived",
            "True when multiple equally strong target candidates prevent a single final target.",
            "True/False",
        ),
        (
            "ambiguous_reference_reason",
            "Classified Bills",
            "derived",
            "Details for ambiguous_reference_resolution.",
            "Text or blank",
        ),
        (
            "submission_date",
            "Classified Bills",
            "extracted/source",
            "Source bill submission date parsed from bottom submission boilerplate and plausibility-checked.",
            "YYYY-MM-DD or blank",
        ),
    ]
    return pd.DataFrame(
        rows,
        columns=["column", "worksheet", "kind", "description", "possible_values"],
    )


def _reason_for_doc_row(row: pd.Series) -> str:
    method = row.get("method")
    if method == "doc_fetch_failed":
        return "doc_fetch_failed"
    if method == "no_doc_url":
        return "no_doc_url"
    if bool(row.get("ambiguous_reference_resolution", False)):
        return "ambiguous_doc_reference"
    if bool(row.get("suspicious_self_resolution", False)):
        return "suspicious_self_reference_only"
    orig_id = row.get("original_bill_id")
    if not pd.isna(orig_id) and int(orig_id) == int(row["BillID"]):
        return "doc_no_ancestor_found"
    return "ancestor_outside_universe"


def export(*, db_path: Path, output_path: Path) -> dict:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        cls = con.execute("SELECT * FROM bill_classifications_doc_full").df()
        bill_meta = con.execute(
            """
            SELECT
                BillID,
                SubTypeDesc
            FROM KNS_Bill
            """
        ).df()
        bill_ref = con.execute(
            """
            SELECT BillID AS original_bill_id,
                   KnessetNum AS original_knesset_num,
                   PrivateNumber AS original_private_number
            FROM KNS_Bill
            WHERE PrivateNumber IS NOT NULL
            """
        ).df()
    finally:
        con.close()

    cls = ensure_columns(
        cls,
        {
            "reference_candidates": "[]",
            "reference_candidate_count": 0,
            "reference_resolution_reason": None,
            "reference_resolution_confidence": None,
            "multiple_references_detected": False,
            "submission_date": None,
            "suspicious_self_resolution": False,
            "ambiguous_reference_resolution": False,
            "ambiguous_reference_reason": None,
        },
    )
    df = cls.merge(bill_meta, on="BillID", how="left").sort_values(
        ["KnessetNum", "BillID"], kind="stable"
    )

    raw_originals = int((df["is_original"] == True).sum())  # noqa: E712
    raw_recurring = int((df["is_original"] == False).sum())  # noqa: E712

    df = suppress_source_metadata_reference_resolutions(df)
    df["explicit_relation_type"] = df["matched_phrase"].apply(classify_recurrence_type)
    df = add_reference_summary_columns(df, bill_ref)

    df = apply_option_c_post_pass(df, reason_for=_reason_for_doc_row)
    df = enrich_from_final_original_bill_id(df, bill_ref)
    df["is_effective_original"] = df["is_original"]
    df["effective_original_bill_id"] = df["original_bill_id"]
    df["effective_original_knesset_num"] = df["original_knesset_num"]
    df["effective_original_private_number"] = df["original_private_number"]
    df = sanitize_submission_dates(df)
    df = add_source_audit_columns(df)
    reference_df = build_reference_resolution_sheet(df)
    dictionary_df = _build_data_dictionary()
    violations = verify_effective_originals(
        df,
        outside_label="recurring_ancestor_outside_universe",
    )

    # Column ordering — most useful columns first for Amnon
    col_order = [
        "source_knesset",
        "source_bill_id",
        "source_doc_id",
        "source_url",
        "BillID",
        "KnessetNum",
        "Name",
        "PrivateNumber",
        "SubTypeDesc",
        "doc_url",
        "direct_reference_bill_id",
        "direct_reference_knesset_num",
        "direct_reference_private_number",
        "direct_reference",
        "cited_reference_count",
        "cited_bill_ids",
        "cited_references",
        "is_effective_original",
        "effective_original_bill_id",
        "effective_original_knesset_num",
        "effective_original_private_number",
        "is_recurring_upstream",
        "explicit_relation_type",
        "final_relation_type",
        "effective_original_reason",
        "target_resolution_status",
        "target_resolution_method",
        "target_resolution_confidence",
        "warnings",
        "notes",
        "method",
        "matched_phrase",
        "classification_source",
        "reference_candidate_count",
        "reference_resolution_reason",
        "reference_resolution_confidence",
        "multiple_references_detected",
        "suspicious_self_resolution",
        "ambiguous_reference_resolution",
        "ambiguous_reference_reason",
        "submission_date",
    ]
    df = df[col_order]
    df = strip_timezone_columns(df)
    reference_df = strip_timezone_columns(reference_df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Classified Bills", index=False)
        reference_df.to_excel(writer, sheet_name="Reference Resolution", index=False)
        dictionary_df.to_excel(writer, sheet_name="Data Dictionary", index=False)

    return {
        "total_rows": len(df),
        "reference_rows": len(reference_df),
        "unresolved_references": (
            int(
                reference_df["target_resolution_status"]
                .astype(str)
                .str.startswith("unresolved")
                .sum()
            )
            if len(reference_df)
            else 0
        ),
        "raw_originals": raw_originals,
        "raw_recurring": raw_recurring,
        "effective_originals": int(df["is_effective_original"].eq(True).sum()),
        "effective_recurring": int(df["is_effective_original"].eq(False).sum()),
        "is_recurring_upstream_true": int(df["is_recurring_upstream"].sum()),
        "promoted_to_effective_original": int(
            df["effective_original_reason"].notna().sum()
        ),
        "by_method": df["method"].value_counts(dropna=False).to_dict(),
        "by_explicit_relation_type": df["explicit_relation_type"]
        .value_counts(dropna=False)
        .to_dict(),
        "by_target_resolution_status": (
            reference_df["target_resolution_status"]
            .value_counts(dropna=False)
            .to_dict()
            if len(reference_df)
            else {}
        ),
        "by_reason": df["effective_original_reason"]
        .value_counts(dropna=False)
        .to_dict(),
        "per_knesset": df.groupby("KnessetNum")
        .agg(
            total=("BillID", "count"),
            recurring=("is_recurring_upstream", "sum"),
        )
        .reset_index()
        .to_dict("records"),
        "violations": violations,
        "output_path": str(output_path),
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, default=_REPO_ROOT / "data" / "warehouse.duckdb")
    p.add_argument(
        "--output",
        type=Path,
        default=_REPO_ROOT
        / "data"
        / "snapshots"
        / "All_Private_Bills_K1_K25_classified.xlsx",
    )
    args = p.parse_args()

    if not args.db.exists():
        print(f"ERROR: warehouse not found: {args.db}", file=sys.stderr)
        return 1

    stats = export(db_path=args.db, output_path=args.output)

    print(f"Total rows (all K1-K25 private bills): {stats['total_rows']}")
    print(f"Reference-resolution rows: {stats['reference_rows']}")
    print(f"Unresolved reference rows: {stats['unresolved_references']}")
    print()
    print("Raw (from our doc-scan):")
    print(f"  Originals: {stats['raw_originals']}")
    print(
        f"  Recurring: {stats['raw_recurring']}  ({100 * stats['raw_recurring'] / stats['total_rows']:.2f}%)"
    )
    print()
    print("Effective (after Option-C post-pass):")
    print(f"  Originals: {stats['effective_originals']}")
    print(f"  Recurring: {stats['effective_recurring']}")
    print(
        f"  Promoted to effective-original: {stats['promoted_to_effective_original']}"
    )
    print()
    print("Per-Knesset recurrence rates:")
    for r in stats["per_knesset"]:
        pct = 100 * r["recurring"] / r["total"] if r["total"] else 0
        print(
            f"  K{int(r['KnessetNum']):<3}  {r['total']:>6}  recurring={r['recurring']:>5}  ({pct:5.2f}%)"
        )
    print()
    print("Explicit relation type (phrase-based only):")
    for t, n in stats["by_explicit_relation_type"].items():
        print(f"  {t}: {n}")
    print()
    print("Target resolution status:")
    for t, n in stats["by_target_resolution_status"].items():
        print(f"  {t}: {n}")
    print()
    print("Method distribution:")
    for m, n in sorted(
        stats["by_method"].items(), key=lambda kv: -kv[1] if kv[1] is not pd.NA else 0
    ):
        print(f"  {m}: {n}")
    print()
    print("Effective-original reasons:")
    for r, n in stats["by_reason"].items():
        print(f"  {r}: {n}")
    print()
    print("Integrity (all must be 0):")
    all_ok = True
    for k, v in stats["violations"].items():
        mark = "OK" if v == 0 else "FAIL"
        print(f"  [{mark}] {k}: {v}")
        if v != 0:
            all_ok = False
    print()
    print(f"Wrote: {stats['output_path']}")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())

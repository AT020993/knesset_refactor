"""Audit the full bills CSV export against the current DuckDB warehouse."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from data.queries.predefined_queries import PREDEFINED_QUERIES
from ui.renderers.data_refresh.dataset_exporter import DatasetExporter


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for column in normalized.columns:
        normalized[column] = normalized[column].map(
            lambda value: "<NULL>" if pd.isna(value) else str(value)
        )
    return normalized


def _frame_digest(df: pd.DataFrame) -> str:
    row_strings = _normalize_frame(df).agg("\u241f".join, axis=1)
    digest = hashlib.sha256()
    for value in row_strings:
        digest.update(value.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _int_dict(row: dict[str, Any]) -> dict[str, int]:
    return {key: int(value) for key, value in row.items()}


def audit_export(csv_path: Path, db_path: Path, report_path: Path, json_path: Path) -> dict[str, Any]:
    logger = logging.getLogger("bills_export_audit")
    exporter = DatasetExporter(db_path, logger)
    query_sql = PREDEFINED_QUERIES["Bills & Legislation (Full Details)"]["sql"]
    full_sql = exporter.remove_limit_offset_from_query(query_sql)
    count_sql = exporter.build_count_query(query_sql)

    with csv_path.open("rb") as handle:
        has_utf8_bom = handle.read(3) == b"\xef\xbb\xbf"

    csv_df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
    fresh_csv_bytes, fresh_csv_rows = exporter.export_full_dataset_csv_bytes(query_sql)
    fresh_csv_df = pd.read_csv(
        io.BytesIO(fresh_csv_bytes), dtype=str, keep_default_na=False, low_memory=False
    )

    with duckdb.connect(db_path.as_posix(), read_only=True) as con:
        warehouse_counts = con.execute(
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT BillID) AS distinct_bill_ids,
                MIN(KnessetNum) AS min_knesset,
                MAX(KnessetNum) AS max_knesset,
                SUM(CASE WHEN BillID IS NULL THEN 1 ELSE 0 END) AS null_bill_id,
                SUM(CASE WHEN KnessetNum IS NULL THEN 1 ELSE 0 END) AS null_knesset_num,
                SUM(CASE WHEN Name IS NULL OR TRIM(Name) = '' THEN 1 ELSE 0 END) AS blank_name
            FROM KNS_Bill
            """
        ).df().iloc[0].to_dict()
        warehouse_counts = _int_dict(warehouse_counts)

        query_count = int(con.execute(count_sql).fetchone()[0])
        query_df = con.execute(full_sql).df()

        base_mismatch_counts = con.execute(
            f"""
            WITH export_query AS ({full_sql})
            SELECT
                SUM(CASE WHEN q.BillName IS DISTINCT FROM b.Name THEN 1 ELSE 0 END) AS bill_name_mismatches,
                SUM(CASE WHEN q.KnessetNum IS DISTINCT FROM b.KnessetNum THEN 1 ELSE 0 END) AS knesset_mismatches,
                SUM(CASE WHEN q.BillStatusID IS DISTINCT FROM b.StatusID THEN 1 ELSE 0 END) AS status_id_mismatches,
                SUM(CASE WHEN q.PrivateNumber IS DISTINCT FROM b.PrivateNumber THEN 1 ELSE 0 END) AS private_number_mismatches,
                SUM(CASE WHEN q.BillSubTypeID IS DISTINCT FROM b.SubTypeID THEN 1 ELSE 0 END) AS subtype_id_mismatches,
                SUM(CASE WHEN q.BillNumber IS DISTINCT FROM b.Number THEN 1 ELSE 0 END) AS bill_number_mismatches
            FROM export_query q
            JOIN KNS_Bill b ON q.BillID = b.BillID
            """
        ).df().iloc[0].to_dict()
        base_mismatch_counts = _int_dict(base_mismatch_counts)

        id_coverage = con.execute(
            f"""
            WITH export_query AS ({full_sql}),
            q AS (SELECT BillID FROM export_query),
            base AS (SELECT BillID FROM KNS_Bill)
            SELECT
                (SELECT COUNT(*) FROM base WHERE BillID NOT IN (SELECT BillID FROM q)) AS missing_from_export,
                (SELECT COUNT(*) FROM q WHERE BillID NOT IN (SELECT BillID FROM base)) AS extra_in_export,
                (SELECT COUNT(*) FROM q) AS export_query_rows,
                (SELECT COUNT(DISTINCT BillID) FROM q) AS export_query_distinct_bill_ids
            """
        ).df().iloc[0].to_dict()
        id_coverage = _int_dict(id_coverage)

        document_count_mismatches = int(
            con.execute(
                f"""
                WITH export_query AS ({full_sql}),
                doc_counts AS (
                    SELECT BillID, COUNT(*) AS expected_count
                    FROM KNS_DocumentBill
                    WHERE FilePath IS NOT NULL
                    GROUP BY BillID
                )
                SELECT COUNT(*) AS mismatches
                FROM export_query q
                LEFT JOIN doc_counts d ON q.BillID = d.BillID
                WHERE COALESCE(q.BillDocumentCount, 0) != COALESCE(d.expected_count, 0)
                """
            ).fetchone()[0]
        )

        initiator_count_mismatches = int(
            con.execute(
                f"""
                WITH export_query AS ({full_sql}),
                initiator_counts AS (
                    SELECT BillID, COUNT(DISTINCT PersonID) AS expected_count
                    FROM KNS_BillInitiator
                    GROUP BY BillID
                )
                SELECT COUNT(*) AS mismatches
                FROM export_query q
                LEFT JOIN initiator_counts i ON q.BillID = i.BillID
                WHERE COALESCE(q.BillTotalMemberCount, 0) != COALESCE(i.expected_count, 0)
                """
            ).fetchone()[0]
        )

        status_breakdown = con.execute(
            f"""
            WITH export_query AS ({full_sql})
            SELECT COALESCE(BillStatusDesc, 'Unknown') AS status, COUNT(*) AS rows
            FROM export_query
            GROUP BY 1
            ORDER BY rows DESC, status
            LIMIT 12
            """
        ).df().to_dict(orient="records")

        rows_by_knesset = con.execute(
            f"""
            WITH export_query AS ({full_sql})
            SELECT KnessetNum, COUNT(*) AS rows
            FROM export_query
            GROUP BY 1
            ORDER BY KnessetNum
            """
        ).df().to_dict(orient="records")

        max_last_updated = con.execute("SELECT MAX(LastUpdatedDate) FROM KNS_Bill").fetchone()[0]

        document_without_primary_breakdown = con.execute(
            f"""
            WITH q AS ({full_sql})
            SELECT
                COALESCE(db.GroupTypeDesc, '<null>') AS group_type,
                COALESCE(db.ApplicationDesc, '<null>') AS application,
                COUNT(*) AS row_count
            FROM q
            JOIN KNS_DocumentBill db ON q.BillID = db.BillID
            WHERE q.BillDocumentCount > 0
              AND q.BillPrimaryDocumentURL IS NULL
              AND db.FilePath IS NOT NULL
            GROUP BY 1, 2
            ORDER BY row_count DESC, group_type, application
            """
        ).df().to_dict(orient="records")

        sample_ids = [
            str(query_df.iloc[0]["BillID"]),
            str(query_df.iloc[len(query_df) // 2]["BillID"]),
            str(query_df.iloc[-1]["BillID"]),
        ]
        sample_checks = []
        for bill_id in sample_ids:
            db_row = con.execute(
                "SELECT BillID, KnessetNum, Name, StatusID, PrivateNumber FROM KNS_Bill WHERE BillID = ?",
                [bill_id],
            ).df().iloc[0].to_dict()
            csv_row = csv_df[csv_df["BillID"].astype(str) == bill_id].iloc[0].to_dict()
            sample_checks.append(
                {
                    "BillID": bill_id,
                    "csv_knesset": csv_row["KnessetNum"],
                    "db_knesset": str(db_row["KnessetNum"]),
                    "name_matches": csv_row["BillName"] == str(db_row["Name"]),
                    "status_matches": csv_row["BillStatusID"] == str(db_row["StatusID"]),
                    "private_number_matches": csv_row["PrivateNumber"]
                    == ("" if pd.isna(db_row["PrivateNumber"]) else str(db_row["PrivateNumber"])),
                }
            )

    csv_numeric = csv_df.copy()
    numeric_columns = [
        "BillID",
        "KnessetNum",
        "BillStatusID",
        "BillTotalMemberCount",
        "BillMainInitiatorCount",
        "BillSupportingMemberCount",
        "BillCoalitionMemberCount",
        "BillOppositionMemberCount",
        "BillDocumentCount",
    ]
    for column in numeric_columns:
        csv_numeric[column] = pd.to_numeric(csv_numeric[column], errors="coerce")

    anomalies = {
        "duplicate_bill_ids": int(csv_numeric["BillID"].duplicated().sum()),
        "blank_bill_id": int(csv_numeric["BillID"].isna().sum()),
        "blank_knesset_num": int(csv_numeric["KnessetNum"].isna().sum()),
        "blank_bill_name": int((csv_df["BillName"].astype(str).str.strip() == "").sum()),
        "knesset_outside_0_25": int((~csv_numeric["KnessetNum"].between(0, 25)).sum()),
        "negative_member_counts": int(
            (
                csv_numeric[
                    [
                        "BillTotalMemberCount",
                        "BillMainInitiatorCount",
                        "BillSupportingMemberCount",
                        "BillCoalitionMemberCount",
                        "BillOppositionMemberCount",
                    ]
                ]
                < 0
            )
            .any(axis=1)
            .sum()
        ),
        "negative_document_counts": int((csv_numeric["BillDocumentCount"] < 0).sum()),
        "primary_document_url_without_count": int(
            (
                (csv_df["BillPrimaryDocumentURL"].astype(str).str.strip() != "")
                & (csv_numeric["BillDocumentCount"].fillna(0) == 0)
            ).sum()
        ),
        "document_count_without_primary_url": int(
            (
                (csv_numeric["BillDocumentCount"].fillna(0) > 0)
                & (csv_df["BillPrimaryDocumentURL"].astype(str).str.strip() == "")
            ).sum()
        ),
        "website_url_missing_or_malformed": int(
            (
                ~csv_df["BillKnessetWebsiteURL"].astype(str).str.contains(
                    r"lawitemid=\d+", regex=True, na=False
                )
            ).sum()
        ),
        "cap_code_without_category": int(
            (
                (csv_df["CAPCode"].astype(str).str.strip() != "")
                & (csv_df["CAPMajorCategory"].astype(str).str.strip() == "")
            ).sum()
        ),
        "coding_major_without_minor": int(
            (
                (csv_df["CodingMajorIL"].astype(str).str.strip() != "")
                & (csv_df["CodingMinorIL"].astype(str).str.strip() == "")
            ).sum()
        ),
    }

    date_anomalies = {}
    for column in [col for col in csv_df.columns if "Date" in col or col.endswith("Formatted")]:
        values = csv_df[column].astype(str).str.strip()
        nonempty = values[values != ""]
        parsed = pd.to_datetime(nonempty, errors="coerce")
        date_anomalies[column] = {
            "nonempty": int(len(nonempty)),
            "parse_fail": int(parsed.isna().sum()),
            "future_after_2026_05_14": int((parsed.dt.date > date(2026, 5, 14)).sum()),
        }

    percentage_anomalies = {}
    for column in ["BillCoalitionMemberPercentage", "BillOppositionMemberPercentage"]:
        values = pd.to_numeric(csv_df[column], errors="coerce")
        percentage_anomalies[column] = {
            "nan": int(values.isna().sum()),
            "outside_0_100": int((~values.between(0, 100)).sum()),
        }

    member_total = pd.to_numeric(csv_df["BillTotalMemberCount"], errors="coerce")
    main_count = pd.to_numeric(csv_df["BillMainInitiatorCount"], errors="coerce")
    support_count = pd.to_numeric(csv_df["BillSupportingMemberCount"], errors="coerce")
    coalition_count = pd.to_numeric(csv_df["BillCoalitionMemberCount"], errors="coerce")
    opposition_count = pd.to_numeric(csv_df["BillOppositionMemberCount"], errors="coerce")
    count_relationship_anomalies = {
        "main_plus_support_gt_total": int(((main_count + support_count) > member_total).sum()),
        "coalition_plus_opposition_gt_total": int(
            ((coalition_count + opposition_count) > member_total).sum()
        ),
    }

    result = {
        "csv_path": str(csv_path),
        "db_path": str(db_path),
        "csv_sha256": _sha256_file(csv_path),
        "csv_size_bytes": csv_path.stat().st_size,
        "has_utf8_bom": has_utf8_bom,
        "csv_rows": len(csv_df),
        "csv_columns": len(csv_df.columns),
        "query_rows": len(query_df),
        "query_columns": len(query_df.columns),
        "query_count": query_count,
        "warehouse_counts": warehouse_counts,
        "row_count_matches_query": len(csv_df) == len(query_df) == query_count,
        "columns_match_query": list(csv_df.columns) == list(query_df.columns),
        "fresh_streaming_export_rows": fresh_csv_rows,
        "fresh_streaming_export_size_bytes": len(fresh_csv_bytes),
        "fresh_streaming_export_sha256": hashlib.sha256(fresh_csv_bytes).hexdigest(),
        "fresh_streaming_export_matches_file": fresh_csv_bytes == csv_path.read_bytes(),
        "csv_cells_match_fresh_streaming_export": _frame_digest(csv_df)
        == _frame_digest(fresh_csv_df),
        "cell_digest_matches_query": _frame_digest(csv_df) == _frame_digest(query_df),
        "csv_digest": _frame_digest(csv_df),
        "query_digest": _frame_digest(query_df),
        "id_coverage": id_coverage,
        "base_mismatch_counts": base_mismatch_counts,
        "document_count_mismatches": document_count_mismatches,
        "initiator_count_mismatches": initiator_count_mismatches,
        "anomalies": anomalies,
        "date_anomalies": date_anomalies,
        "percentage_anomalies": percentage_anomalies,
        "count_relationship_anomalies": count_relationship_anomalies,
        "document_without_primary_breakdown": document_without_primary_breakdown,
        "status_breakdown_top12": status_breakdown,
        "rows_by_knesset": rows_by_knesset,
        "sample_checks": sample_checks,
        "warehouse_kNS_bill_max_last_updated": str(max_last_updated),
        "audit_boundary": (
            "Verified CSV against current DuckDB warehouse and exact query. "
            "Did not re-fetch live Knesset OData in this audit."
        ),
    }

    checks = [
        ("CSV parses with expected BOM", result["has_utf8_bom"]),
        ("CSV row count matches exact query", result["row_count_matches_query"]),
        ("CSV columns match exact query order", result["columns_match_query"]),
        ("Fresh DuckDB streaming export matches CSV bytes", result["fresh_streaming_export_matches_file"]),
        (
            "CSV cells match fresh streaming export",
            result["csv_cells_match_fresh_streaming_export"],
        ),
        (
            "Export covers every KNS_Bill exactly once",
            id_coverage["missing_from_export"] == 0
            and id_coverage["extra_in_export"] == 0
            and id_coverage["export_query_rows"]
            == id_coverage["export_query_distinct_bill_ids"]
            == warehouse_counts["rows"],
        ),
        ("Key base fields match KNS_Bill", all(value == 0 for value in base_mismatch_counts.values())),
        ("Document counts match KNS_DocumentBill", document_count_mismatches == 0),
        ("Initiator counts match KNS_BillInitiator", initiator_count_mismatches == 0),
        (
            "Dates parse and are not future-dated",
            all(
                value["parse_fail"] == 0 and value["future_after_2026_05_14"] == 0
                for value in date_anomalies.values()
            ),
        ),
        (
            "Percentages and count relationships are valid",
            all(value["outside_0_100"] == 0 for value in percentage_anomalies.values())
            and all(value == 0 for value in count_relationship_anomalies.values()),
        ),
    ]
    result["core_checks"] = [{"name": name, "passed": bool(passed)} for name, passed in checks]
    result["all_core_checks_pass"] = all(passed for _, passed in checks)

    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text(_render_markdown(result), encoding="utf-8")
    return result


def _render_markdown(result: dict[str, Any]) -> str:
    lines = [
        "# Bills Full Details CSV Audit - 2026-05-14",
        "",
        "## Scope",
        f"- Artifact: `{result['csv_path']}`",
        f"- Warehouse: `{result['db_path']}`",
        "- Query: `Bills & Legislation (Full Details)` with display `LIMIT/OFFSET` removed.",
        "- Boundary: this verifies the CSV against the current local DuckDB warehouse. It does not prove the warehouse is freshly synchronized with live Knesset OData.",
        "",
        "## Summary",
    ]
    for check in result["core_checks"]:
        lines.append(f"- {'PASS' if check['passed'] else 'FAIL'}: {check['name']}")

    lines.extend(
        [
            "",
            "## Metrics",
            f"- CSV rows: {result['csv_rows']:,}",
            f"- CSV columns: {result['csv_columns']:,}",
            f"- CSV size: {result['csv_size_bytes']:,} bytes",
            f"- CSV SHA-256: `{result['csv_sha256']}`",
            f"- Fresh streaming export SHA-256: `{result['fresh_streaming_export_sha256']}`",
            f"- Warehouse `KNS_Bill` rows: {result['warehouse_counts']['rows']:,}",
            f"- Warehouse `KNS_Bill` distinct BillIDs: {result['warehouse_counts']['distinct_bill_ids']:,}",
            f"- Knesset range: {result['warehouse_counts']['min_knesset']} to {result['warehouse_counts']['max_knesset']}",
            f"- Max `KNS_Bill.LastUpdatedDate`: {result['warehouse_kNS_bill_max_last_updated']}",
            "",
            "## Anomaly Scan",
        ]
    )
    for key, value in result["anomalies"].items():
        lines.append(f"- {key}: {value:,}")
    for key, value in result["count_relationship_anomalies"].items():
        lines.append(f"- {key}: {value:,}")
    for key, value in result["percentage_anomalies"].items():
        lines.append(
            f"- {key}: nan={value['nan']:,}, outside_0_100={value['outside_0_100']:,}"
        )

    lines.append("")
    lines.append("## Date Checks")
    for key, value in result["date_anomalies"].items():
        lines.append(
            f"- {key}: nonempty={value['nonempty']:,}, parse_fail={value['parse_fail']:,}, future_after_2026_05_14={value['future_after_2026_05_14']:,}"
        )

    lines.append("")
    lines.append("## Base Table Reconciliation")
    for key, value in result["base_mismatch_counts"].items():
        lines.append(f"- {key}: {value:,}")
    lines.append(f"- document_count_mismatches: {result['document_count_mismatches']:,}")
    lines.append(f"- initiator_count_mismatches: {result['initiator_count_mismatches']:,}")

    lines.append("")
    lines.append("## Coverage")
    for key, value in result["id_coverage"].items():
        lines.append(f"- {key}: {value:,}")

    lines.append("")
    lines.append("## Top Statuses")
    for row in result["status_breakdown_top12"]:
        lines.append(f"- {row['status']}: {int(row['rows']):,}")

    lines.append("")
    lines.append("## Sample Row Checks")
    for row in result["sample_checks"]:
        lines.append(
            f"- BillID {row['BillID']}: name={row['name_matches']}, "
            f"status={row['status_matches']}, knesset={row['csv_knesset']} vs "
            f"{row['db_knesset']}, private_number={row['private_number_matches']}"
        )

    lines.extend(
        [
            "",
            "## Notes",
            "- `document_count_without_primary_url` can be nonzero when source document rows exist but no prioritized primary document type/path is available for the UI link field.",
        ]
    )
    if result["document_without_primary_breakdown"]:
        lines.append("- Document rows without a primary URL are source document types outside the prioritized primary-link categories:")
        for row in result["document_without_primary_breakdown"]:
            lines.append(
                f"  - {row['group_type']} / {row['application']}: {int(row['row_count']):,}"
            )
    if result["warehouse_counts"]["min_knesset"] == 0:
        lines.append(
            "- `KnessetNum = 0` rows are present in the warehouse and are included because the export is the full unfiltered bills dataset."
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("data/exports/bills_full_details_2026-05-14.csv"),
    )
    parser.add_argument("--warehouse", type=Path, default=Path("data/warehouse.duckdb"))
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("data/exports/bills_full_details_2026-05-14_audit.md"),
    )
    parser.add_argument(
        "--json",
        type=Path,
        default=Path("data/exports/bills_full_details_2026-05-14_audit.json"),
    )
    args = parser.parse_args()

    result = audit_export(args.csv, args.warehouse, args.report, args.json)
    print(
        json.dumps(
            {
                "report": str(args.report),
                "json": str(args.json),
                "all_core_checks_pass": result["all_core_checks_pass"],
                "csv_rows": result["csv_rows"],
                "csv_columns": result["csv_columns"],
                "anomalies": result["anomalies"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

"""Regression tests for recurring-bill export normalization."""

from __future__ import annotations

from pathlib import Path
import runpy

import duckdb
import pandas as pd

from data.recurring_bills.export_resolution import classify_recurrence_type


_REPO_ROOT = Path(__file__).resolve().parent.parent
_EXPORT_ALL = runpy.run_path(str(_REPO_ROOT / "scripts" / "export_all_bills_classified.py"))["export"]
_EXPORT_OUR_SCAN = runpy.run_path(str(_REPO_ROOT / "scripts" / "export_amnon_from_our_scan.py"))["export"]


def _build_db(db_path: Path) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE KNS_Bill (
                BillID BIGINT,
                KnessetNum BIGINT,
                Name VARCHAR,
                PrivateNumber BIGINT,
                SubTypeDesc VARCHAR
            )
            """
        )
        con.executemany(
            """
            INSERT INTO KNS_Bill (BillID, KnessetNum, Name, PrivateNumber, SubTypeDesc)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (1, 18, "Original bill", 111, "Private"),
                (2, 19, "Current bill", 222, "Private"),
                (3, 19, "Mid-chain bill", 333, "Private"),
                (4, 1, "Historic bill", 444, "Private"),
                (5, 19, "Plural recurring bill", 555, "Private"),
            ],
        )
        con.execute(
            """
            CREATE TABLE bill_classifications_doc_full (
                BillID BIGINT,
                KnessetNum BIGINT,
                Name VARCHAR,
                PrivateNumber BIGINT,
                is_original BOOLEAN,
                original_bill_id BIGINT,
                matched_phrase VARCHAR,
                method VARCHAR,
                reference_candidates VARCHAR,
                reference_candidate_count BIGINT,
                reference_resolution_reason VARCHAR,
                reference_resolution_confidence DOUBLE,
                multiple_references_detected BOOLEAN,
                submission_date VARCHAR,
                suspicious_self_resolution BOOLEAN,
                ambiguous_reference_resolution BOOLEAN,
                ambiguous_reference_reason VARCHAR,
                doc_url VARCHAR,
                classification_source VARCHAR,
                last_updated TIMESTAMP
            )
            """
        )
        con.executemany(
            """
            INSERT INTO bill_classifications_doc_full VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    1, 18, "Original bill", 111, True, 1,
                    None, "doc_no_pattern", "[]", 0, None, None, False,
                    "2013-01-01", False, False, None, "https://example/1.doc", "doc_based_full",
                    pd.Timestamp("2026-04-23 00:00:00"),
                ),
                (
                    2, 19, "Current bill", 222, False, 3,
                    "הצעת חוק זהה", "doc_pattern_linked", "[]", 1,
                    "explicit_private_number_and_knesset", 0.99, False,
                    "2014-01-15", False, False, None, "https://example/2.doc", "doc_based_full",
                    pd.Timestamp("2026-04-23 00:00:00"),
                ),
                (
                    3, 19, "Mid-chain bill", 333, False, 1,
                    "הצעת חוק דומה", "doc_pattern_linked", "[]", 1,
                    "same_knesset_private_number_fallback", 0.78, False,
                    "2014-01-10", False, False, None, "https://example/3.doc", "doc_based_full",
                    pd.Timestamp("2026-04-23 00:00:00"),
                ),
                (
                    4, 1, "Historic bill", 444, True, 4,
                    None, "doc_no_pattern", "[]", 0, None, None, False,
                    "2049-11-23", False, False, None, "https://example/4.doc", "doc_based_full",
                    pd.Timestamp("2026-04-23 00:00:00"),
                ),
                (
                    5, 19, "Plural recurring bill", 555, False, 1,
                    "הצעות  חוק  דומות  בעיקרן", "doc_pattern_linked", "[]", 1,
                    "explicit_private_number_and_knesset", 0.99, False,
                    "2014-02-24", False, False, None, "https://example/5.doc", "doc_based_full",
                    pd.Timestamp("2026-04-23 00:00:00"),
                ),
            ],
        )
    finally:
        con.close()


class TestExportResolution:
    def test_export_scripts_share_final_original_enrichment(self, tmp_path: Path):
        db_path = tmp_path / "warehouse.duckdb"
        excel_path = tmp_path / "amnon.xlsx"
        output_scan = tmp_path / "our_scan.xlsx"
        output_all = tmp_path / "all.xlsx"

        _build_db(db_path)
        pd.DataFrame(
            [
                {"BillID": 1, "Name": "Original bill"},
                {"BillID": 2, "Name": "Current bill"},
                {"BillID": 3, "Name": "Mid-chain bill"},
                {"BillID": 4, "Name": "Historic bill"},
                {"BillID": 5, "Name": "Plural recurring bill"},
            ]
        ).to_excel(excel_path, index=False)

        _EXPORT_OUR_SCAN(excel_path=excel_path, db_path=db_path, output_path=output_scan)
        _EXPORT_ALL(db_path=db_path, output_path=output_all)

        df_scan = pd.read_excel(output_scan)
        df_all = pd.read_excel(output_all)

        row_scan = df_scan.loc[df_scan["BillID"] == 2].iloc[0]
        row_all = df_all.loc[df_all["BillID"] == 2].iloc[0]

        assert row_scan["original_bill_id"] == 1
        assert row_all["original_bill_id"] == 1
        assert row_scan["original_knesset_num"] == 18
        assert row_all["original_knesset_num"] == 18
        assert row_scan["original_private_number"] == 111
        assert row_all["original_private_number"] == 111
        assert row_scan["submission_date"] == "2014-01-15"
        assert row_all["submission_date"] == "2014-01-15"

    def test_export_scripts_drop_implausible_submission_dates(self, tmp_path: Path):
        db_path = tmp_path / "warehouse.duckdb"
        excel_path = tmp_path / "amnon.xlsx"
        output_scan = tmp_path / "our_scan.xlsx"
        output_all = tmp_path / "all.xlsx"

        _build_db(db_path)
        pd.DataFrame(
            [
                {"BillID": 1, "Name": "Original bill"},
                {"BillID": 2, "Name": "Current bill"},
                {"BillID": 3, "Name": "Mid-chain bill"},
                {"BillID": 4, "Name": "Historic bill"},
                {"BillID": 5, "Name": "Plural recurring bill"},
            ]
        ).to_excel(excel_path, index=False)

        _EXPORT_OUR_SCAN(excel_path=excel_path, db_path=db_path, output_path=output_scan)
        _EXPORT_ALL(db_path=db_path, output_path=output_all)

        df_scan = pd.read_excel(output_scan)
        df_all = pd.read_excel(output_all)

        row_scan = df_scan.loc[df_scan["BillID"] == 4].iloc[0]
        row_all = df_all.loc[df_all["BillID"] == 4].iloc[0]

        assert pd.isna(row_scan["submission_date"])
        assert pd.isna(row_all["submission_date"])

    def test_classify_recurrence_type_handles_plural_variants(self):
        assert classify_recurrence_type("הצעות חוק זהות") == "identical"
        assert classify_recurrence_type("הצעות חוק דומות") == "similar"
        assert classify_recurrence_type("הצעות חוק דומות בעיקרן") == "similar"
        assert classify_recurrence_type("הצעות  חוק  דומות  בעיקרן") == "similar"

    def test_final_export_writes_corrected_submission_date_and_recurrence_type(self, tmp_path: Path):
        db_path = tmp_path / "warehouse.duckdb"
        output_all = tmp_path / "all.xlsx"

        _build_db(db_path)
        _EXPORT_ALL(db_path=db_path, output_path=output_all)

        df_all = pd.read_excel(output_all)
        historic_row = df_all.loc[df_all["BillID"] == 4].iloc[0]
        plural_row = df_all.loc[df_all["BillID"] == 5].iloc[0]

        assert pd.isna(historic_row["submission_date"])
        assert plural_row["submission_date"] == "2014-02-24"
        assert plural_row["recurrence_type"] == "similar"

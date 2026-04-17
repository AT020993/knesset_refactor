"""Unit tests for src/data/recurring_bills/classify.py."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from data.recurring_bills.classify import build_tal_classifications


FIXTURES = Path(__file__).parent / "fixtures" / "recurring_bills"


class TestBuildTalClassifications:
    def test_columns_and_source(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        for bid in (477119, 477120, 477137):
            src = FIXTURES / f"tal_detail_{bid}.json"
            (cache_dir / f"{bid}.json").write_text(src.read_text())

        df = build_tal_classifications(
            bulk_csv=FIXTURES / "tal_bulk_sample.csv",
            cache_dir=cache_dir,
        )

        expected_cols = {
            "BillID", "KnessetNum", "Name",
            "is_original", "original_bill_id",
            "tal_category", "is_cross_term", "is_within_term_dup", "is_self_resubmission",
            "family_size", "predecessor_bill_ids",
            "classification_source", "tal_fetched_at",
        }
        assert expected_cols.issubset(df.columns)
        assert (df["classification_source"] == "tal_alovitz").all()

    def test_original_bill_id_from_patient_zero(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        for bid in (477119, 477120, 477137):
            src = FIXTURES / f"tal_detail_{bid}.json"
            (cache_dir / f"{bid}.json").write_text(src.read_text())

        df = build_tal_classifications(
            bulk_csv=FIXTURES / "tal_bulk_sample.csv",
            cache_dir=cache_dir,
        )

        row_new = df.loc[df["BillID"] == 477119].iloc[0]
        row_reprise = df.loc[df["BillID"] == 477137].iloc[0]

        assert row_new["is_original"] is True or row_new["is_original"] == 1
        assert row_new["original_bill_id"] == 477119

        assert row_reprise["is_original"] is False or row_reprise["is_original"] == 0
        assert row_reprise["original_bill_id"] == 477119

    def test_missing_detail_json_defaults_to_self(self, tmp_path: Path):
        """Bill in bulk CSV but no detail fetched — fall back to self-reference."""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        # Only fetch 477119, skip the others
        (cache_dir / "477119.json").write_text((FIXTURES / "tal_detail_477119.json").read_text())

        df = build_tal_classifications(
            bulk_csv=FIXTURES / "tal_bulk_sample.csv",
            cache_dir=cache_dir,
        )

        row_no_detail = df.loc[df["BillID"] == 477120].iloc[0]
        assert row_no_detail["original_bill_id"] == 477120  # self — no patient_zero known
        assert pd.isna(row_no_detail["family_size"])


from data.recurring_bills.classify import build_k16_k18_fallback


class TestK16K18Fallback:
    def test_earliest_knesset_is_original(self):
        df = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")

        # Group 1: BillID 10001 (K16), 10002 (K17), 10003 (K18) — chinuch-chova
        row_earliest = df.loc[df["BillID"] == 10001].iloc[0]
        row_mid = df.loc[df["BillID"] == 10002].iloc[0]
        row_last = df.loc[df["BillID"] == 10003].iloc[0]

        assert row_earliest["is_original"] == True
        assert row_earliest["original_bill_id"] == 10001
        assert row_mid["is_original"] == False
        assert row_mid["original_bill_id"] == 10001
        assert row_last["is_original"] == False
        assert row_last["original_bill_id"] == 10001

    def test_singleton_group_is_original(self):
        df = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")
        row = df.loc[df["BillID"] == 10004].iloc[0]  # bituach le'umi — singleton
        assert row["is_original"] == True
        assert row["original_bill_id"] == 10004

    def test_same_knesset_tie_breaker_lowest_billid(self):
        df = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")
        # Both in K18, same name: 10007 (lower) and 10008
        row_lower = df.loc[df["BillID"] == 10007].iloc[0]
        row_higher = df.loc[df["BillID"] == 10008].iloc[0]
        assert row_lower["is_original"] == True
        assert row_higher["is_original"] == False
        assert row_higher["original_bill_id"] == 10007

    def test_source_is_name_fallback(self):
        df = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")
        assert (df["classification_source"] == "name_fallback_k16_k18").all()

    def test_tal_specific_columns_are_null(self):
        df = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")
        for col in ["tal_category", "is_cross_term", "is_within_term_dup",
                    "is_self_resubmission", "family_size", "tal_fetched_at"]:
            assert df[col].isna().all()

    def test_predecessor_list_contains_original_for_reprises(self):
        df = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")
        row_mid = df.loc[df["BillID"] == 10002].iloc[0]
        assert row_mid["predecessor_bill_ids"] == [10001]

        row_earliest = df.loc[df["BillID"] == 10001].iloc[0]
        assert row_earliest["predecessor_bill_ids"] == []

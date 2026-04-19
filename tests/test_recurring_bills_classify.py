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
            "classification_source", "tal_fetched_at", "last_updated",
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


import duckdb

from data.recurring_bills.storage import write_duckdb_table, write_parquet_snapshot


class TestStorage:
    def _fixture_df(self) -> pd.DataFrame:
        ts = pd.Timestamp("2026-04-17", tz="UTC")
        return pd.DataFrame([
            {"BillID": 1, "KnessetNum": 20, "Name": "a", "is_original": True,
             "original_bill_id": 1, "tal_category": "new", "is_cross_term": False,
             "is_within_term_dup": False, "is_self_resubmission": False,
             "family_size": 1, "predecessor_bill_ids": [],
             "classification_source": "tal_alovitz",
             "tal_fetched_at": ts, "last_updated": ts},
            {"BillID": 2, "KnessetNum": 21, "Name": "b", "is_original": False,
             "original_bill_id": 1, "tal_category": "cross", "is_cross_term": True,
             "is_within_term_dup": False, "is_self_resubmission": False,
             "family_size": 2, "predecessor_bill_ids": [1],
             "classification_source": "tal_alovitz",
             "tal_fetched_at": ts, "last_updated": ts},
        ])

    def test_write_duckdb_creates_table_and_rows(self, tmp_path: Path):
        db = tmp_path / "w.duckdb"
        df = self._fixture_df()
        write_duckdb_table(df, db_path=db)

        con = duckdb.connect(str(db), read_only=True)
        count = con.execute("SELECT COUNT(*) FROM bill_classifications").fetchone()[0]
        con.close()
        assert count == 2

    def test_write_duckdb_replaces_on_rerun(self, tmp_path: Path):
        db = tmp_path / "w.duckdb"
        write_duckdb_table(self._fixture_df(), db_path=db)
        write_duckdb_table(self._fixture_df().head(1), db_path=db)  # smaller

        con = duckdb.connect(str(db), read_only=True)
        count = con.execute("SELECT COUNT(*) FROM bill_classifications").fetchone()[0]
        con.close()
        assert count == 1

    def test_write_parquet_atomic_and_idempotent(self, tmp_path: Path):
        out = tmp_path / "bill_classifications.parquet"
        df = self._fixture_df()

        write_parquet_snapshot(df, out)
        first_bytes = out.read_bytes()

        write_parquet_snapshot(df, out)
        assert out.read_bytes() == first_bytes  # byte-idempotent


from data.recurring_bills.classify import merge_all


class TestMergeAll:
    def test_union_preserves_both_sources(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        for bid in (477119, 477120, 477137):
            (cache_dir / f"{bid}.json").write_text((FIXTURES / f"tal_detail_{bid}.json").read_text())

        tal = build_tal_classifications(
            bulk_csv=FIXTURES / "tal_bulk_sample.csv",
            cache_dir=cache_dir,
        )
        fb = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")

        merged = merge_all(tal=tal, fallback=fb)

        assert len(merged) == len(tal) + len(fb)
        assert set(merged["classification_source"].unique()) == {"tal_alovitz", "name_fallback_k16_k18"}

    def test_dedup_prefers_tal_over_fallback_on_collision(self, tmp_path: Path):
        """If the same BillID appears in both frames (shouldn't happen in real life,
        since Tal is K19-K25 and fallback is K16-K18), prefer Tal."""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        (cache_dir / "10001.json").write_text(json.dumps({
            "bill_id": 10001, "patient_zero_bill_id": 99999,
            "family_size": 3, "predecessor_bill_ids": [99999], "category": "cross",
        }))
        tal = pd.DataFrame([{
            "BillID": 10001, "KnessetNum": 19, "Name": "x",
            "is_original": False, "original_bill_id": 99999,
            "tal_category": "cross", "is_cross_term": True, "is_within_term_dup": False,
            "is_self_resubmission": False, "family_size": 3, "predecessor_bill_ids": [99999],
            "classification_source": "tal_alovitz",
            "tal_fetched_at": pd.Timestamp.now(tz="UTC"),
            "last_updated": pd.Timestamp.now(tz="UTC"),
        }])
        fb = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")
        # Manually force a collision: rewrite fallback row BillID to match Tal's
        fb.loc[0, "BillID"] = 10001

        merged = merge_all(tal=tal, fallback=fb)
        collision = merged.loc[merged["BillID"] == 10001].iloc[0]
        assert collision["classification_source"] == "tal_alovitz"
        assert collision["original_bill_id"] == 99999


from unittest.mock import patch

from data.recurring_bills.pipeline import run_pipeline


class TestPipeline:
    def test_rebuild_mode_skips_network_and_builds_outputs(self, tmp_path: Path):
        # Stage fixtures into a working data/ tree
        work = tmp_path
        (work / "external" / "tal_bill_details").mkdir(parents=True)
        for bid in (477119, 477120, 477137):
            (work / "external" / "tal_bill_details" / f"{bid}.json").write_text(
                (FIXTURES / f"tal_detail_{bid}.json").read_text()
            )
        (work / "external" / "tal_alovitz_bills.csv").write_bytes(
            (FIXTURES / "tal_bulk_sample.csv").read_bytes()
        )

        out_parquet = work / "snapshots" / "bill_classifications.parquet"
        out_report = work / "recurring_bills" / "coverage_report.md"
        db = work / "warehouse.duckdb"

        result = run_pipeline(
            mode="rebuild",
            excel_path=FIXTURES / "excel_sample.xlsx",
            cache_dir=work / "external" / "tal_bill_details",
            bulk_csv=work / "external" / "tal_alovitz_bills.csv",
            db_path=db,
            parquet_path=out_parquet,
            report_path=out_report,
            k16_k18_method="name",  # no warehouse in test env
        )

        assert result["total"] >= 10  # 6 from Tal fixture + K16-K18 fallback
        assert out_parquet.exists()
        assert out_report.exists()
        assert "# Recurring Bills Classification Coverage" in out_report.read_text()

    def test_refresh_mode_triggers_fetch(self, tmp_path: Path):
        """refresh mode should call download_bulk_csv + fetch_many_details."""
        with patch("data.recurring_bills.pipeline.download_bulk_csv") as mock_bulk, \
             patch("data.recurring_bills.pipeline.fetch_many_details") as mock_many:
            mock_bulk.return_value = FIXTURES / "tal_bulk_sample.csv"
            mock_many.return_value = []

            # Stage cache_dir fixtures so classify still works
            cache_dir = tmp_path / "cache"
            cache_dir.mkdir()
            for bid in (477119, 477120, 477137):
                (cache_dir / f"{bid}.json").write_text(
                    (FIXTURES / f"tal_detail_{bid}.json").read_text()
                )

            run_pipeline(
                mode="refresh",
                excel_path=FIXTURES / "excel_sample.xlsx",
                cache_dir=cache_dir,
                bulk_csv=tmp_path / "bulk.csv",
                db_path=tmp_path / "w.duckdb",
                parquet_path=tmp_path / "snap.parquet",
                report_path=tmp_path / "report.md",
                delay_s=0,
                k16_k18_method="name",
            )

        assert mock_bulk.call_count == 1
        assert mock_many.call_count == 1

    def test_rebuild_produces_stable_parquet_excluding_timestamps(self, tmp_path: Path):
        """Two consecutive rebuild runs over the same cache produce Parquets
        that are identical EXCEPT for the `tal_fetched_at` and `last_updated`
        columns, which are expected to change per run.
        """
        work = tmp_path
        (work / "external" / "tal_bill_details").mkdir(parents=True)
        for bid in (477119, 477120, 477137):
            (work / "external" / "tal_bill_details" / f"{bid}.json").write_text(
                (FIXTURES / f"tal_detail_{bid}.json").read_text()
            )
        (work / "external" / "tal_alovitz_bills.csv").write_bytes(
            (FIXTURES / "tal_bulk_sample.csv").read_bytes()
        )

        common_kwargs = dict(
            mode="rebuild",
            excel_path=FIXTURES / "excel_sample.xlsx",
            cache_dir=work / "external" / "tal_bill_details",
            bulk_csv=work / "external" / "tal_alovitz_bills.csv",
            db_path=work / "warehouse.duckdb",
            report_path=work / "report.md",
            k16_k18_method="name",
        )

        first_parquet = work / "snapshots" / "first.parquet"
        second_parquet = work / "snapshots" / "second.parquet"
        run_pipeline(parquet_path=first_parquet, **common_kwargs)
        run_pipeline(parquet_path=second_parquet, **common_kwargs)

        first = pd.read_parquet(first_parquet).drop(columns=["tal_fetched_at", "last_updated"])
        second = pd.read_parquet(second_parquet).drop(columns=["tal_fetched_at", "last_updated"])
        pd.testing.assert_frame_equal(first, second)


from data.recurring_bills.cap_view import create_cap_view


class TestCapView:
    def test_creates_view_with_expected_columns(self, tmp_path: Path):
        db = tmp_path / "w.duckdb"
        con = duckdb.connect(str(db), read_only=False)
        con.execute("CREATE TABLE UserBillCAP (BillID INTEGER, CAPMinorCode VARCHAR)")
        con.execute("INSERT INTO UserBillCAP VALUES (1, '100'), (2, '200')")
        con.execute("""
            CREATE TABLE bill_classifications (
                BillID INTEGER PRIMARY KEY, KnessetNum INTEGER, Name VARCHAR,
                is_original BOOLEAN, original_bill_id INTEGER, tal_category VARCHAR,
                classification_source VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO bill_classifications VALUES
            (1, 20, 'a', TRUE, 1, 'new', 'tal_alovitz'),
            (2, 21, 'a', FALSE, 1, 'cross', 'tal_alovitz')
        """)
        con.close()

        create_cap_view(db_path=db)

        con = duckdb.connect(str(db), read_only=True)
        rows = con.execute("""
            SELECT BillID, CAPMinorCode, is_original, original_bill_id
            FROM v_cap_bills_with_recurrence ORDER BY BillID
        """).fetchall()
        con.close()

        assert rows == [(1, '100', True, 1), (2, '200', False, 1)]

    def test_view_handles_cap_bills_without_classification(self, tmp_path: Path):
        """Bills in UserBillCAP that aren't in bill_classifications get NULL."""
        db = tmp_path / "w.duckdb"
        con = duckdb.connect(str(db), read_only=False)
        con.execute("CREATE TABLE UserBillCAP (BillID INTEGER, CAPMinorCode VARCHAR)")
        con.execute("INSERT INTO UserBillCAP VALUES (99, 'xxx')")
        con.execute("""
            CREATE TABLE bill_classifications (
                BillID INTEGER PRIMARY KEY, KnessetNum INTEGER, Name VARCHAR,
                is_original BOOLEAN, original_bill_id INTEGER, tal_category VARCHAR,
                classification_source VARCHAR
            )
        """)
        con.close()

        create_cap_view(db_path=db)

        con = duckdb.connect(str(db), read_only=True)
        row = con.execute(
            "SELECT is_original FROM v_cap_bills_with_recurrence WHERE BillID = 99"
        ).fetchone()
        con.close()
        assert row == (None,)

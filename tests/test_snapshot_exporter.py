"""Tests for ``data.snapshots.exporter`` — the Parquet snapshot contract."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from data.snapshots.exporter import SNAPSHOTS, export_all
from data.snapshots.manifest import read_manifest


@pytest.fixture()
def tiny_warehouse(tmp_path: Path) -> Path:
    """Minimal warehouse with just the tables the exporter reads.

    Schema-compatible with ``data/warehouse.duckdb`` for the columns each
    MVP query references. One to two rows per table — enough to exercise
    every SQL path without depending on the real production dump."""
    wh = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(wh))
    con.execute(
        """
        CREATE TABLE KNS_Person (
            PersonID BIGINT, LastName VARCHAR, FirstName VARCHAR,
            GenderID BIGINT, GenderDesc VARCHAR, Email VARCHAR,
            IsCurrent BOOLEAN, LastUpdatedDate VARCHAR
        );
        INSERT INTO KNS_Person VALUES
            (1, 'כהן', 'יעל', 2, 'נקבה', NULL, TRUE, '2026-01-01'),
            (2, 'לוי', 'דן', 1, 'זכר', NULL, TRUE, '2026-01-01');

        CREATE TABLE KNS_PersonToPosition (
            PersonToPositionID BIGINT, PersonID BIGINT, PositionID BIGINT,
            KnessetNum DOUBLE, StartDate VARCHAR, FinishDate VARCHAR,
            GovMinistryID DOUBLE, GovMinistryName VARCHAR, DutyDesc VARCHAR,
            FactionID DOUBLE, FactionName VARCHAR, GovernmentNum DOUBLE
        );
        INSERT INTO KNS_PersonToPosition VALUES
            (1, 1, 10, 26.0, '2025-11-01', NULL, NULL, NULL, 'חברת כנסת', 1001.0, 'מפלגה א', NULL),
            (2, 2, 10, 26.0, '2025-11-01', NULL, NULL, NULL, 'חבר כנסת',  1002.0, 'מפלגה ב', NULL);

        CREATE TABLE KNS_Faction (
            FactionID BIGINT, Name VARCHAR, KnessetNum BIGINT,
            StartDate VARCHAR, FinishDate VARCHAR, IsCurrent BOOLEAN, LastUpdatedDate VARCHAR
        );
        INSERT INTO KNS_Faction VALUES
            (1001, 'מפלגה א', 26, '2025-11-01', NULL, TRUE, '2026-01-01'),
            (1002, 'מפלגה ב', 26, '2025-11-01', NULL, TRUE, '2026-01-01');

        CREATE TABLE UserFactionCoalitionStatus (
            KnessetNum BIGINT, FactionID BIGINT, FactionName VARCHAR,
            CoalitionStatus VARCHAR, NewFactionName VARCHAR,
            DateJoinedCoalition TIMESTAMP_NS, DateLeftCoalition TIMESTAMP_NS
        );
        INSERT INTO UserFactionCoalitionStatus VALUES
            (26, 1001, 'מפלגה א', 'Coalition',  'מפלגה א', NULL, NULL),
            (26, 1002, 'מפלגה ב', 'Opposition', 'מפלגה ב', NULL, NULL);

        CREATE TABLE KNS_Committee (
            CommitteeID BIGINT, Name VARCHAR, CategoryID DOUBLE, CategoryDesc VARCHAR,
            KnessetNum BIGINT, CommitteeTypeID BIGINT, CommitteeTypeDesc VARCHAR,
            Email VARCHAR, StartDate VARCHAR, FinishDate VARCHAR,
            AdditionalTypeID DOUBLE, AdditionalTypeDesc VARCHAR
        );
        INSERT INTO KNS_Committee VALUES
            (500, 'ועדת חוץ וביטחון', 1.0, 'קבועה', 26, 1, 'קבועה', NULL, '2025-11-01', NULL, NULL, NULL);

        CREATE TABLE KNS_CommitteeSession (
            CommitteeSessionID BIGINT, CommitteeID BIGINT, KnessetNum BIGINT, StartDate VARCHAR
        );
        INSERT INTO KNS_CommitteeSession VALUES
            (9001, 500, 26, '2026-01-10'),
            (9002, 500, 26, '2026-01-17');

        CREATE TABLE UserCAPTaxonomy (
            MajorCode INTEGER, MajorTopic_HE VARCHAR, MajorTopic_EN VARCHAR,
            MinorCode INTEGER, MinorTopic_HE VARCHAR, MinorTopic_EN VARCHAR,
            Description_HE VARCHAR, Examples_HE VARCHAR
        );
        INSERT INTO UserCAPTaxonomy VALUES
            (1, 'מוסדות שלטון', 'Government Institutions', 100, 'תת-נושא', 'Sub', NULL, NULL),
            (2, 'כלכלה',         'Economy',                200, 'תת-נושא', 'Sub', NULL, NULL);

        CREATE TABLE UserBillCAP (
            AnnotationID INTEGER, BillID INTEGER, ResearcherID INTEGER,
            CAPMinorCode INTEGER, AssignedDate TIMESTAMP, Confidence VARCHAR,
            Notes VARCHAR, Source VARCHAR, SubmissionDate VARCHAR
        );
        INSERT INTO UserBillCAP VALUES
            (1, 7002, 1, 100, NULL, 'high', NULL, 'RA', NULL);

        CREATE TABLE UserBillCoding (
            BillID INTEGER, MajorIL INTEGER, MinorIL INTEGER,
            MajorCAP INTEGER, MinorCAP INTEGER, StateReligion INTEGER,
            Territories INTEGER, Source VARCHAR, ImportedAt TIMESTAMP
        );
        INSERT INTO UserBillCoding VALUES
            (7001, 20, 2001, 2, 200, 0, 0, 'legacy-import', NULL);

        CREATE TABLE KNS_Bill (
            BillID BIGINT, KnessetNum BIGINT, Name VARCHAR,
            SubTypeID BIGINT, SubTypeDesc VARCHAR, PrivateNumber DOUBLE,
            CommitteeID DOUBLE, StatusID BIGINT, Number DOUBLE,
            PostponementReasonID DOUBLE, PostponementReasonDesc VARCHAR,
            PublicationDate VARCHAR, MagazineNumber DOUBLE, PageNumber DOUBLE,
            IsContinuationBill BOOLEAN, SummaryLaw VARCHAR,
            PublicationSeriesID DOUBLE, PublicationSeriesDesc VARCHAR,
            PublicationSeriesFirstCall DOUBLE, LastUpdatedDate VARCHAR
        );
        INSERT INTO KNS_Bill VALUES
            (7001, 26, 'הצעת חוק לדוגמה', 1, 'פרטית', NULL, NULL, 1, NULL,
             NULL, NULL, '2026-02-01', NULL, NULL, FALSE, NULL, NULL, NULL, NULL, NULL),
            (7002, 26, 'הצעת חוק עם קידוד CAP', 1, 'פרטית', NULL, NULL, 1, NULL,
             NULL, NULL, '2026-02-02', NULL, NULL, FALSE, NULL, NULL, NULL, NULL, NULL);

        CREATE TABLE KNS_BillInitiator (
            BillInitiatorID BIGINT, BillID BIGINT, PersonID BIGINT,
            IsInitiator BOOLEAN, Ordinal BIGINT, LastUpdatedDate VARCHAR
        );
        INSERT INTO KNS_BillInitiator VALUES
            (1, 7001, 1, TRUE, 1, '2026-02-01'),
            (2, 7002, 2, TRUE, 1, '2026-02-02');

        CREATE TABLE KNS_Query (
            QueryID BIGINT, Number DOUBLE, KnessetNum BIGINT, Name VARCHAR,
            TypeID BIGINT, TypeDesc VARCHAR, StatusID BIGINT,
            PersonID BIGINT, GovMinistryID BIGINT, SubmitDate VARCHAR
        );
        INSERT INTO KNS_Query VALUES
            (8001, 1.0, 26, 'שאילתה לדוגמה', 1, 'דחופה', 1, 2, 1, '2026-02-10');

        CREATE TABLE UserQueryCoding (
            QueryID INTEGER, MajorIL INTEGER, MinorIL INTEGER,
            MajorCAP INTEGER, MinorCAP INTEGER, Religion INTEGER,
            Territories INTEGER, Source VARCHAR, ImportedAt TIMESTAMP
        );
        INSERT INTO UserQueryCoding VALUES
            (8001, 2, 200, 2, 200, 0, 0, 'RA', NULL);

        CREATE TABLE KNS_Agenda (
            AgendaID BIGINT, Number DOUBLE, ClassificationID BIGINT,
            ClassificationDesc VARCHAR, LeadingAgendaID DOUBLE,
            KnessetNum BIGINT, Name VARCHAR, SubTypeID BIGINT, SubTypeDesc VARCHAR,
            StatusID BIGINT, InitiatorPersonID DOUBLE, GovRecommendationID INTEGER,
            GovRecommendationDesc INTEGER, PresidentDecisionDate VARCHAR,
            PostopenmentReasonID DOUBLE, PostopenmentReasonDesc VARCHAR,
            CommitteeID DOUBLE, RecommendCommitteeID DOUBLE,
            MinisterPersonID DOUBLE, LastUpdatedDate VARCHAR
        );
        INSERT INTO KNS_Agenda VALUES
            (9001, 1.0, 1, NULL, NULL, 26, 'הצעה לסדר יום', 1, 'דחופה',
             1, 1.0, NULL, NULL, '2026-02-15', NULL, NULL, NULL, NULL, NULL, NULL);

        CREATE TABLE UserAgendaCoding (
            AgendaID INTEGER, MajorIL INTEGER, MinorIL INTEGER,
            Religion INTEGER, Territories INTEGER,
            MatchMethod VARCHAR, MatchConfidence DECIMAL(3,2),
            Source VARCHAR, ImportedAt TIMESTAMP
        );
        INSERT INTO UserAgendaCoding VALUES
            (9001, 1, 100, 0, 0, 'exact', 1.00, 'RA', NULL);
        """
    )
    con.close()
    return wh


def test_export_produces_all_snapshots_and_manifest(tiny_warehouse: Path, tmp_path: Path) -> None:
    out = tmp_path / "snapshots"
    manifest = export_all(tiny_warehouse, out)

    expected_names = {name for name, _sql in SNAPSHOTS}
    assert set(manifest.snapshots.keys()) == expected_names
    # Every named file exists and its sha256 matches the manifest.
    for name in expected_names:
        f = out / f"{name}.parquet"
        assert f.exists(), f"missing {f}"
        assert hashlib.sha256(f.read_bytes()).hexdigest() == manifest.snapshots[name].sha256
    # Manifest round-trips.
    on_disk = read_manifest(out / "manifest.json")
    assert on_disk.snapshots == manifest.snapshots
    # Sanity: non-empty output for at least one pack on this fixture data.
    assert manifest.snapshots["mk_summary"].rows >= 2


def test_export_is_idempotent_for_parquet_bytes(tiny_warehouse: Path, tmp_path: Path) -> None:
    """Re-running on an unchanged warehouse produces byte-identical Parquet files.

    Guards the manifest's ability to serve as a cache key for downstream
    consumers; only ``generated_at_utc`` may change between runs."""
    out = tmp_path / "snapshots"
    m1 = export_all(tiny_warehouse, out)
    m2 = export_all(tiny_warehouse, out)
    for name in m1.snapshots:
        assert m1.snapshots[name].sha256 == m2.snapshots[name].sha256, (
            f"{name} is not byte-stable"
        )


def test_export_leaves_no_dot_new_files_on_success(tiny_warehouse: Path, tmp_path: Path) -> None:
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    assert list(out.glob("*.new")) == []


def test_rename_failure_does_not_corrupt_prior_snapshot(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    """If ``os.replace`` fails partway through, the previous ``manifest.json``
    and the prior ``*.parquet`` files remain untouched — readers still see
    a consistent snapshot set."""
    out = tmp_path / "snapshots"
    m1 = export_all(tiny_warehouse, out)
    original = {
        name: (out / f"{name}.parquet").read_bytes()
        for name in m1.snapshots
    }
    original_manifest_bytes = (out / "manifest.json").read_bytes()

    call_count = {"n": 0}

    def replace_that_fails_on_third_call(src: str | Path, dst: str | Path, /) -> None:
        call_count["n"] += 1
        if call_count["n"] == 3:
            raise OSError("simulated rename failure")
        import os as _os
        _os.rename(src, dst)  # fall through to real rename

    with patch("data.snapshots.exporter.os.replace", side_effect=replace_that_fails_on_third_call):
        with pytest.raises(OSError, match="simulated rename failure"):
            export_all(tiny_warehouse, out)

    # Prior snapshots untouched (some later-in-order parquets may have been
    # replaced before the third call, but none AFTER the failure).
    assert (out / "manifest.json").read_bytes() == original_manifest_bytes, (
        "manifest.json must NOT reflect the failed run — it is written last"
    )
    # Every parquet file still exists (either untouched or successfully replaced).
    for name in original:
        assert (out / f"{name}.parquet").exists()

    # A clean rerun recovers to a consistent new manifest.
    m3 = export_all(tiny_warehouse, out)
    assert set(m3.snapshots.keys()) == set(m1.snapshots.keys())


def test_manifest_records_warehouse_mtime(tiny_warehouse: Path, tmp_path: Path) -> None:
    out = tmp_path / "snapshots"
    m = export_all(tiny_warehouse, out)
    assert m.warehouse_mtime_utc.endswith("Z")
    # Load from disk and confirm the field survives JSON round-trip.
    raw = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert raw["warehouse_mtime_utc"] == m.warehouse_mtime_utc


def test_mk_bills_exports_major_cap_from_supported_sources(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)

    con = duckdb.connect()
    rows = con.execute(
        """
        SELECT bill_id, cap_code
        FROM read_parquet(?)
        ORDER BY bill_id
        """,
        [str(out / "mk_bills.parquet")],
    ).fetchall()
    con.close()

    assert rows == [
        (7001, 2),  # legacy UserBillCoding.MajorCAP path
        (7002, 1),  # UserBillCAP.CAPMinorCode -> UserCAPTaxonomy.MajorCode fallback
    ]

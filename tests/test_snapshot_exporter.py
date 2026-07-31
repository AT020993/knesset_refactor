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
            (9002, 500, 26, '2026-01-17'),
            (9003, 500, 26, '2026-01-24');

        CREATE TABLE KNS_CmtSessionItem (
            CmtSessionItemID BIGINT, ItemID BIGINT, CommitteeSessionID BIGINT,
            Ordinal BIGINT, StatusID BIGINT, Name VARCHAR, ItemTypeID BIGINT,
            LastUpdatedDate VARCHAR
        );
        -- session 9001 → legislation (ItemTypeID 2), 9002 → oversight (11),
        -- 9003 → no items (folds into 'אחר'); exercises every derivation branch.
        INSERT INTO KNS_CmtSessionItem VALUES
            (1, 101, 9001, 1, 1, 'חוק לדוגמה', 2, '2026-01-10'),
            (2, 102, 9002, 1, 1, 'סקירה כללית', 11, '2026-01-17');

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
            (7001, 20, 2001, 2, 200, 0, 0, 'legacy-import', NULL),
            -- Recent Knessets (K21-25) were coded in MajorIL only; MajorCAP is
            -- NULL. cap_code must fall back to MajorIL so their topic data shows.
            (7005, 21, 2101, NULL, NULL, 0, 0, 'k25-il-only', NULL);

        CREATE TABLE KNS_Status (
            StatusID BIGINT, "Desc" VARCHAR, TypeID BIGINT, TypeDesc VARCHAR,
            OrderTransition BIGINT, IsActive BOOLEAN, LastUpdatedDate VARCHAR
        );
        INSERT INTO KNS_Status VALUES
            (118, 'התקבלה בקריאה שלישית', 2, 'הצעת חוק', NULL, TRUE, '2026-01-01'),
            (104, 'הונחה על שולחן הכנסת לדיון מוקדם', 2, 'הצעת חוק', NULL, TRUE, '2026-01-01'),
            (141, 'עברה קריאה טרומית', 2, 'הצעת חוק', NULL, TRUE, '2026-01-01'),
            (9,   'נענתה', 1, 'שאילתה', NULL, TRUE, '2026-01-01'),
            (304, 'לדיון בוועדה', 4, 'הצעה לסדר היום', NULL, TRUE, '2026-01-01');

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
        -- StatusID values are real ladder-mapped ids (118 = law, 104 = tabled,
        -- 141 = preliminary reading) so bills_list has a law and a tabled bill to
        -- assert on, and so 7005 (private, K25) doesn't fall out of the ladder.
        -- 7003/7004 keep an arbitrary unmapped id (1): they are government/
        -- committee bills, excluded from bills_list by its SubTypeDesc filter,
        -- so their status never reaches the ladder-guard test.
        INSERT INTO KNS_Bill VALUES
            (7001, 26, 'הצעת חוק לדוגמה', 1, 'פרטית', NULL, NULL, 118, NULL,
             NULL, NULL, '2026-02-01', NULL, NULL, FALSE, NULL, NULL, NULL, NULL, NULL),
            (7002, 26, 'הצעת חוק עם קידוד CAP', 1, 'פרטית', NULL, NULL, 104, NULL,
             NULL, NULL, '2026-02-02', NULL, NULL, FALSE, NULL, NULL, NULL, NULL, NULL),
            -- Government + committee bills carry MK initiator rows too (a minister
            -- who is an MK signs the government bill), but they are NOT the MK's own
            -- legislative initiative, so mk_bills must exclude them. See 7003/7004.
            (7003, 26, 'הצעת חוק ממשלתית', 2, 'ממשלתית', NULL, NULL, 1, NULL,
             NULL, NULL, '2026-02-03', NULL, NULL, FALSE, NULL, NULL, NULL, NULL, NULL),
            (7004, 26, 'הצעת חוק ועדה', 3, 'ועדה', NULL, NULL, 1, NULL,
             NULL, NULL, '2026-02-04', NULL, NULL, FALSE, NULL, NULL, NULL, NULL, NULL),
            (7005, 25, 'הצעת חוק מקודדת ב-MajorIL בלבד', 1, 'פרטית', NULL, NULL, 141, NULL,
             NULL, NULL, '2026-02-05', NULL, NULL, FALSE, NULL, NULL, NULL, NULL, NULL);

        CREATE TABLE KNS_BillInitiator (
            BillInitiatorID BIGINT, BillID BIGINT, PersonID BIGINT,
            IsInitiator BOOLEAN, Ordinal BIGINT, LastUpdatedDate VARCHAR
        );
        INSERT INTO KNS_BillInitiator VALUES
            (1, 7001, 1, TRUE, 1, '2026-02-01'),
            (2, 7002, 2, TRUE, 1, '2026-02-02'),
            (3, 7003, 2, TRUE, 1, '2026-02-03'),
            (4, 7004, 2, TRUE, 1, '2026-02-04'),
            (5, 7005, 1, TRUE, 1, '2026-02-05');

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

        CREATE TABLE WebVoteHeader (
            vote_id BIGINT, knesset_num INTEGER, vote_date VARCHAR,
            vote_type VARCHAR, item_title VARCHAR, is_accepted BOOLEAN,
            is_electronic BOOLEAN, total_for INTEGER, total_against INTEGER,
            total_abstain INTEGER, total_present INTEGER
        );
        INSERT INTO WebVoteHeader VALUES
            (10, 26, '2026-06-01T00:00:00', 'אלקטרונית', 'הצבעה לדוגמה', TRUE, TRUE, 2, 1, 0, 0);

        CREATE TABLE WebVoteMk (
            vote_id BIGINT, mk_id BIGINT, mk_name VARCHAR,
            faction_name VARCHAR, position VARCHAR
        );
        INSERT INTO WebVoteMk VALUES
            (10, 1, 'כהן יעל', 'מפלגה א', 'for'),
            (10, 2, 'לוי דן',  'מפלגה ב', 'against');

        CREATE TABLE WebMkCv (
            mk_id BIGINT, birth_date VARCHAR, birth_place_he VARCHAR,
            education_he VARCHAR, military_service_he VARCHAR, languages_he VARCHAR
        );
        INSERT INTO WebMkCv VALUES
            (1, '14/12/1952', 'תל אביב, ישראל', 'תואר ראשון', 'סרן', 'עברית, אנגלית'),
            (2, '01/01/1970', 'חיפה, ישראל',    'תואר שני',  NULL,   'עברית');

        CREATE TABLE WebMkCommittee (
            mk_id BIGINT, knesset_num BIGINT, committee_name_he VARCHAR,
            role_he VARCHAR, from_date VARCHAR, to_date VARCHAR
        );
        -- mk 1: current member of committee 500 (name matches KNS_Committee after
        -- normalisation). mk 2: a PAST membership (to_date set) → excluded from
        -- committee_members_by_faction but must still appear in mk_committees.
        -- mk 1 also sits on an ad-hoc sub-committee with no KNS_Committee row at
        -- all (mirrors the ~595 real unresolved names) — its membership must
        -- still surface in mk_committees with a NULL committee_id, not vanish.
        INSERT INTO WebMkCommittee VALUES
            (1, 26, 'ועדת חוץ וביטחון', 'חבר בוועדת חוץ וביטחון', '2025-11-17T00:00:00', NULL),
            (2, 26, 'ועדת חוץ וביטחון', 'חבר בוועדת חוץ וביטחון', '2025-11-17T00:00:00', '2026-01-01T00:00:00'),
            (1, 26, 'ועדת משנה לנושא שאינו קיים', 'חבר', '2025-12-01T00:00:00', NULL);
        """
    )
    con.close()
    return wh


def test_export_produces_all_snapshots_and_manifest(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    out = tmp_path / "snapshots"
    manifest = export_all(tiny_warehouse, out)

    expected_names = {name for name, _sql in SNAPSHOTS}
    assert set(manifest.snapshots.keys()) == expected_names
    # Every named file exists and its sha256 matches the manifest.
    for name in expected_names:
        f = out / f"{name}.parquet"
        assert f.exists(), f"missing {f}"
        assert (
            hashlib.sha256(f.read_bytes()).hexdigest()
            == manifest.snapshots[name].sha256
        )
    # Manifest round-trips.
    on_disk = read_manifest(out / "manifest.json")
    assert on_disk.snapshots == manifest.snapshots
    # Sanity: non-empty output for at least one pack on this fixture data.
    assert manifest.snapshots["mk_summary"].rows >= 2


def test_export_is_idempotent_for_parquet_bytes(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
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


def test_export_leaves_no_dot_new_files_on_success(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
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
    original = {name: (out / f"{name}.parquet").read_bytes() for name in m1.snapshots}
    original_manifest_bytes = (out / "manifest.json").read_bytes()

    call_count = {"n": 0}

    def replace_that_fails_on_third_call(src: str | Path, dst: str | Path, /) -> None:
        call_count["n"] += 1
        if call_count["n"] == 3:
            raise OSError("simulated rename failure")
        import os as _os

        _os.rename(src, dst)  # fall through to real rename

    with patch(
        "data.snapshots.exporter.os.replace",
        side_effect=replace_that_fails_on_third_call,
    ):
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
        (7001, 2),  # UserBillCoding.MajorCAP wins over its own MajorIL (20)
        (7002, 1),  # UserBillCAP.CAPMinorCode -> UserCAPTaxonomy.MajorCode fallback
        (7005, 21),  # MajorCAP is NULL -> falls back to MajorIL (K21-25 case)
    ]


def test_mk_bills_excludes_government_and_committee_bills(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    """mk_bills is an MK's *own* legislative initiatives, so only private member
    bills (SubTypeDesc = 'פרטית') belong. Government ('ממשלתית') and committee
    ('ועדה') bills list ministers/MKs as initiators upstream but are not the MK's
    own bills; they must not leak into the snapshot (which feeds every bill count
    and list across the API)."""
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)

    con = duckdb.connect()
    stages = con.execute(
        "SELECT DISTINCT stage FROM read_parquet(?) ORDER BY stage",
        [str(out / "mk_bills.parquet")],
    ).fetchall()
    bill_ids = con.execute(
        "SELECT DISTINCT bill_id FROM read_parquet(?) ORDER BY bill_id",
        [str(out / "mk_bills.parquet")],
    ).fetchall()
    con.close()

    assert stages == [("פרטית",)]
    # 7003 (ממשלתית) + 7004 (ועדה) excluded; 7005 (private) kept
    assert bill_ids == [(7001,), (7002,), (7005,)]


def test_curated_snapshots_match_api_contract(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    """party_metadata + committee_topics_ministries are read from the committed
    seed CSVs and must expose exactly the columns the FastAPI handlers SELECT."""
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    try:
        pm_cols = {
            c[0]
            for c in con.execute(
                f"DESCRIBE SELECT * FROM '{out / 'party_metadata.parquet'}'"
            ).fetchall()
        }
        assert pm_cols == {
            "party_id",
            "standardised_name",
            "founded_date",
            "platform_url",
            "bylaws_url",
            "website_url",
            "ideology_he",
            "source_url",
        }
        # Likud (1096) ideology is authored — non-null.
        ideology = con.execute(
            f"SELECT ideology_he FROM '{out / 'party_metadata.parquet'}' WHERE party_id = 1096"
        ).fetchone()
        assert ideology is not None and ideology[0].startswith("סיעת הליכוד")

        tm_cols = {
            c[0]
            for c in con.execute(
                f"DESCRIBE SELECT * FROM '{out / 'committee_topics_ministries.parquet'}'"
            ).fetchall()
        }
        assert tm_cols == {
            "committee_id",
            "knesset_num",
            "cap_code",
            "cap_label_he",
            "ministry_he",
            "notes_he",
        }
        # Finance committee (4186) oversees the Finance Minister; CAP still pending.
        fin = con.execute(
            f"SELECT cap_code, ministry_he FROM '{out / 'committee_topics_ministries.parquet'}'"
            " WHERE committee_id = 4186"
        ).fetchall()
        assert ("שר האוצר",) in {(m,) for _cap, m in fin}
        assert all(cap is None for cap, _m in fin)
    finally:
        con.close()


def test_mk_cv_snapshot_matches_api_contract(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    try:
        cols = [
            c[0]
            for c in con.execute(
                f"DESCRIBE SELECT * FROM '{out / 'mk_cv.parquet'}'"
            ).fetchall()
        ]
        assert cols == [
            "mk_id",
            "birth_date",
            "birth_place_he",
            "education_he",
            "military_service_he",
            "languages_he",
        ]
        row = con.execute(
            f"SELECT birth_date, languages_he FROM '{out / 'mk_cv.parquet'}' WHERE mk_id = 1"
        ).fetchone()
        assert row == ("14/12/1952", "עברית, אנגלית")
    finally:
        con.close()


def test_committee_members_resolve_id_and_drop_past_memberships(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    """Committee NAME resolves to committees_list id, each member carries their
    faction, and memberships that have ended (to_date set) are excluded."""
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    try:
        cols = [
            c[0]
            for c in con.execute(
                f"DESCRIBE SELECT * FROM '{out / 'committee_members_by_faction.parquet'}'"
            ).fetchall()
        ]
        assert cols == [
            "committee_id",
            "knesset_num",
            "mk_id",
            "mk_name_he",
            "faction_id",
            "faction_name",
            "role_he",
            "is_current",
        ]
        rows = con.execute(
            f"SELECT committee_id, mk_id, faction_name FROM "
            f"'{out / 'committee_members_by_faction.parquet'}'"
        ).fetchall()
        # mk 1 (current) resolves to committee 500 with faction 'מפלגה א';
        # mk 2's ended membership is dropped.
        assert rows == [(500, 1, "מפלגה א")]
    finally:
        con.close()


def test_mk_committees_exports_full_history(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    """mk_committees is a WebMkCommittee projection — unlike
    committee_members_by_faction it deliberately keeps PAST memberships too
    (to_date set): an MK's profile needs their whole committee history, not
    just current seats."""
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    try:
        cols = [
            c[0]
            for c in con.execute(
                f"DESCRIBE SELECT * FROM '{out / 'mk_committees.parquet'}'"
            ).fetchall()
        ]
        assert cols == [
            "mk_id",
            "knesset_num",
            "committee_id",
            "committee_name_he",
            "role_he",
            "from_date",
            "to_date",
        ]
        rows = con.execute(
            f"SELECT mk_id, knesset_num, committee_id, committee_name_he, role_he, to_date "
            f"FROM read_parquet('{out}/mk_committees.parquet')"
        ).fetchall()
        assert rows
        for mk_id, kn, _committee_id, name, _role, _to_date in rows:
            assert mk_id is not None and kn is not None
            assert name, "a membership with no committee name is not useful"
        # mk 2's membership ended in the fixture (to_date set) but must still
        # be present — this is the one behaviour that distinguishes this
        # snapshot from committee_members_by_faction, which drops it. A test
        # that only checked non-null columns would pass identically whether
        # or not that filter existed, so pin it explicitly.
        mk_ids = {mk_id for mk_id, *_rest in rows}
        assert mk_ids == {1, 2}
        ended = [r for r in rows if r[5] is not None]
        assert ended, "past memberships (to_date set) must be retained, not dropped"
    finally:
        con.close()


def test_mk_committees_resolves_committee_id_but_keeps_unresolved_rows(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    """committee_id resolves through the same name-normalisation as
    committee_members_by_faction, but — unlike that inner-joined query —
    an unresolvable committee name must NOT drop the row: the MK's
    membership is real even when we can't link it to a committee page."""
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT mk_id, committee_name_he, committee_id "
            f"FROM read_parquet('{out}/mk_committees.parquet')"
        ).fetchall()
        by_name = {(mk_id, name): cid for mk_id, name, cid in rows}
        # mk 1's and mk 2's 'ועדת חוץ וביטחון' membership resolves to
        # committee 500 (matches KNS_Committee after normalisation).
        assert by_name[(1, "ועדת חוץ וביטחון")] == 500
        assert by_name[(2, "ועדת חוץ וביטחון")] == 500
        # mk 1's ad-hoc sub-committee has no KNS_Committee row at all (mirrors
        # the real ~595 unresolved names) — it must still appear, with a NULL
        # committee_id rather than being silently dropped.
        assert (1, "ועדת משנה לנושא שאינו קיים") in by_name
        assert by_name[(1, "ועדת משנה לנושא שאינו קיים")] is None
    finally:
        con.close()


def test_committee_sessions_by_type_derivation_and_reconciliation(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    """Each session is labelled by its primary item type, item-less sessions fall
    to 'אחר', and the per-type counts sum back to the committees_list total."""
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    try:
        rows = dict(
            con.execute(
                f"SELECT type_he, session_count FROM '{out / 'committee_sessions_by_type.parquet'}'"
                " WHERE committee_id = 500 ORDER BY type_he"
            ).fetchall()
        )
        assert rows == {"חקיקה": 1, "דיון כללי (פיקוח)": 1, "אחר": 1}
        # Reconciles with committees_list.session_count (3 sessions on committee 500).
        total_by_type = sum(rows.values())
        list_count = con.execute(
            f"SELECT session_count FROM '{out / 'committees_list.parquet'}' WHERE committee_id = 500"
        ).fetchone()[0]
        assert total_by_type == list_count == 3
    finally:
        con.close()


def test_bills_list_carries_title_status_and_rung(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT bill_id, name, status_id, status_desc, status_rung, "
        f"status_rung_order FROM read_parquet('{out}/bills_list.parquet') "
        f"ORDER BY bill_id"
    ).fetchall()
    assert rows, "bills_list must not be empty on the fixture"
    for bill_id, name, status_id, desc, rung, order in rows:
        assert name, f"bill {bill_id} has no title"
        assert desc, f"bill {bill_id} status {status_id} did not decode"
        assert rung, f"bill {bill_id} status {status_id} has no rung"
        assert order is not None


def test_bills_list_is_one_row_per_bill(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    """Keyed on bill_id — not per initiator, which is what mk_bills is for."""
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    total, distinct = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT bill_id) "
        f"FROM read_parquet('{out}/bills_list.parquet')"
    ).fetchone()
    assert total == distinct


def test_bills_list_scoped_to_private_member_bills(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    """A bills_list carrying government/committee bills would let a join
    silently reintroduce the bills mk_bills deliberately excludes."""
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    sub_types = con.execute(
        f"SELECT DISTINCT sub_type FROM read_parquet('{out}/bills_list.parquet')"
    ).fetchall()
    bill_ids = con.execute(
        f"SELECT DISTINCT bill_id FROM read_parquet('{out}/bills_list.parquet') ORDER BY bill_id"
    ).fetchall()
    assert sub_types == [("פרטית",)]
    # 7003 (ממשלתית) + 7004 (ועדה) excluded; 7001/7002/7005 (private) kept
    assert bill_ids == [(7001,), (7002,), (7005,)]


def test_bills_list_rung_order_matches_the_ladder(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    from data.snapshots.bill_status import RUNG_ORDER

    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    for rung, order in con.execute(
        f"SELECT DISTINCT status_rung, status_rung_order "
        f"FROM read_parquet('{out}/bills_list.parquet')"
    ).fetchall():
        assert RUNG_ORDER.index(rung) == order


def test_no_bill_status_falls_outside_the_ladder(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    """The ladder is hand-authored, so a future Knesset introducing an
    unmapped status must break loudly rather than silently fall out of
    every rung."""
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    unmapped = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out}/bills_list.parquet') "
        f"WHERE status_rung IS NULL"
    ).fetchone()[0]
    assert unmapped == 0

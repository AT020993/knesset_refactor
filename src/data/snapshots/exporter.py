"""Parquet snapshot exporter.

Reads the read-only DuckDB warehouse, runs a fixed set of queries, and writes
one Parquet per API-endpoint shape plus a ``manifest.json`` commit marker.
Every file is produced atomically via ``<name>.new`` → ``os.replace``, and the
manifest is always written last so readers see a consistent old-or-new state.

CLI::

    python -m data.snapshots.exporter \\
        --warehouse data/warehouse.duckdb \\
        --output-dir data/snapshots/
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from data.queries.packs.bills import BILLS_QUERIES
from data.queries.packs.committees import COMMITTEES_QUERIES
from data.queries.packs.mks import MK_QUERIES
from data.queries.packs.parties import PARTIES_QUERIES
from data.queries.packs.votes import VOTES_QUERIES
from data.snapshots.manifest import Manifest, SnapshotEntry, write_manifest

log = logging.getLogger("data.snapshots.exporter")

# Snapshot-specific "by MK" joins live here (not in the Streamlit packs)
# because they serve the FastAPI contract, not the UI. Phase 4 may fold them
# into a dedicated ``mk_activity`` pack once the API surface solidifies.
_MK_BILLS_SQL = """
SELECT
    bi.PersonID                     AS mk_id,
    bi.BillID                       AS bill_id,
    CAST(b.KnessetNum AS INTEGER)   AS knesset_num,
    b.SubTypeDesc                   AS stage,
    COALESCE(ubcap_tax.MajorCode, ubcoding.MajorCAP) AS cap_code,
    b.PublicationDate               AS submit_date,
    CAST(bi.Ordinal AS INTEGER)     AS initiator_ordinal,
    bi.IsInitiator                  AS is_main_initiator
FROM KNS_BillInitiator bi
JOIN KNS_Bill b ON bi.BillID = b.BillID
LEFT JOIN UserBillCoding ubcoding ON bi.BillID = ubcoding.BillID
LEFT JOIN UserBillCAP ubcap ON bi.BillID = ubcap.BillID
LEFT JOIN UserCAPTaxonomy ubcap_tax ON ubcap.CAPMinorCode = ubcap_tax.MinorCode
WHERE bi.PersonID IS NOT NULL
ORDER BY bi.BillID, bi.Ordinal, bi.PersonID
""".strip()

_MK_QUESTIONS_SQL = """
SELECT
    q.PersonID                      AS mk_id,
    q.QueryID                       AS question_id,
    CAST(q.KnessetNum AS INTEGER)   AS knesset_num,
    CAST(q.StatusID AS INTEGER)     AS status_id,
    q.TypeDesc                      AS type_he,
    uqc.MajorCAP                    AS cap_code,
    q.SubmitDate                    AS submit_date
FROM KNS_Query q
LEFT JOIN UserQueryCoding uqc ON q.QueryID = uqc.QueryID
WHERE q.PersonID IS NOT NULL
ORDER BY q.QueryID
""".strip()

_MK_MOTIONS_SQL = """
SELECT
    CAST(a.InitiatorPersonID AS BIGINT) AS mk_id,
    a.AgendaID                          AS motion_id,
    CAST(a.KnessetNum AS INTEGER)       AS knesset_num,
    CAST(a.StatusID AS INTEGER)         AS status_id,
    a.SubTypeDesc                       AS type_he,
    uac.MajorIL                         AS cap_code,
    a.PresidentDecisionDate             AS decision_date
FROM KNS_Agenda a
LEFT JOIN UserAgendaCoding uac ON a.AgendaID = uac.AgendaID
WHERE a.InitiatorPersonID IS NOT NULL
ORDER BY a.AgendaID, a.InitiatorPersonID
""".strip()

# --- Curated snapshots -------------------------------------------------------
# Two snapshots have NO source in the OData warehouse: their content is
# editorially authored (product spec deck) and lives in committed seed CSVs
# under ``data/seeds/`` (regenerate via ``scripts/seeds/build_curated_seeds.py``).
# The warehouse connection is read-only, so we read the CSVs straight from disk
# via an absolute path rather than staging them into the warehouse.
_SEEDS_DIR = Path(__file__).resolve().parents[3] / "data" / "seeds"

# party_metadata — faction ideology (deck slide 6). Keyed by party_id (primary)
# and standardised_name. founded_date / URLs / source_url are not authored yet
# → emitted as NULL so the API's PartyMetadata shape stays complete.
_PARTY_METADATA_SQL = f"""
SELECT
    CAST(party_id AS BIGINT)  AS party_id,
    standardised_name,
    CAST(NULL AS VARCHAR)     AS founded_date,
    CAST(NULL AS VARCHAR)     AS platform_url,
    CAST(NULL AS VARCHAR)     AS bylaws_url,
    CAST(NULL AS VARCHAR)     AS website_url,
    ideology_he,
    CAST(NULL AS VARCHAR)     AS source_url
FROM read_csv(
    '{_SEEDS_DIR / "party_metadata.csv"}',
    header = true,
    columns = {{'party_id': 'BIGINT', 'standardised_name': 'VARCHAR', 'ideology_he': 'VARCHAR'}}
)
ORDER BY party_id
""".strip()

# committee_topics_ministries — ministries each committee oversees (deck slide 8
# notes). CAP topic mapping is still pending (deck: "שחף מתייעץ עם אמנון") so
# cap_code / cap_label_he are NULL for now.
_COMMITTEE_TOPICS_MINISTRIES_SQL = f"""
SELECT
    CAST(committee_id AS BIGINT)  AS committee_id,
    CAST(knesset_num AS INTEGER)  AS knesset_num,
    CAST(NULL AS INTEGER)         AS cap_code,
    CAST(NULL AS VARCHAR)         AS cap_label_he,
    ministry_he,
    notes_he
FROM read_csv(
    '{_SEEDS_DIR / "committee_topics_ministries.csv"}',
    header = true,
    columns = {{'committee_id': 'BIGINT', 'knesset_num': 'INTEGER', 'ministry_he': 'VARCHAR', 'notes_he': 'VARCHAR'}}
)
ORDER BY committee_id, ministry_he
""".strip()

# committee_sessions_by_type — discussion counts by type. KNS_CommitteeSession's
# own TypeDesc is only open/confidential, so the *purpose* of a session is
# derived from its agenda items (KNS_CmtSessionItem.ItemTypeID): each session is
# labelled by its most-frequent item type (deterministic tiebreak), then sessions
# are counted per (committee, knesset, label). Item types observed:
#   2 → bills (legislation), 11 → general/oversight, 4 → rapid-debate proposals,
#   6000/6003 & item-less sessions → other/procedural.
# Secondary legislation (חקיקת משנה) has no distinct item type in this data, so it
# legitimately does not appear. Totals reconcile with committees_list.session_count.
_COMMITTEE_SESSIONS_BY_TYPE_SQL = """
WITH item_type_counts AS (
    SELECT
        CommitteeSessionID AS session_id,
        ItemTypeID,
        COUNT(*)           AS n,
        ROW_NUMBER() OVER (
            PARTITION BY CommitteeSessionID
            ORDER BY COUNT(*) DESC, ItemTypeID ASC
        )                  AS rn
    FROM KNS_CmtSessionItem
    GROUP BY CommitteeSessionID, ItemTypeID
),
session_primary AS (
    SELECT session_id, ItemTypeID AS primary_item_type
    FROM item_type_counts
    WHERE rn = 1
)
SELECT
    CAST(cs.CommitteeID AS BIGINT) AS committee_id,
    CAST(cs.KnessetNum AS INTEGER) AS knesset_num,
    CASE sp.primary_item_type
        WHEN 2  THEN 'חקיקה'
        WHEN 11 THEN 'דיון כללי (פיקוח)'
        WHEN 4  THEN 'הצעות לדיון מהיר (הלס"י)'
        ELSE 'אחר'
    END                            AS type_he,
    COUNT(DISTINCT cs.CommitteeSessionID) AS session_count
FROM KNS_CommitteeSession cs
LEFT JOIN session_primary sp ON cs.CommitteeSessionID = sp.session_id
WHERE cs.CommitteeID IS NOT NULL
GROUP BY committee_id, knesset_num, type_he
ORDER BY committee_id, knesset_num, session_count DESC, type_he
""".strip()

# mk_cv — MK biographical fields, ingested from the Knesset site backend into
# WebMkCv (see data.mk_details.ingest). Columns map 1:1 to the API's MkCv shape.
_MK_CV_SQL = """
SELECT
    mk_id,
    birth_date,
    birth_place_he,
    education_he,
    military_service_he,
    languages_he
FROM WebMkCv
ORDER BY mk_id
""".strip()

# committee_members_by_faction — currently-serving members per committee, grouped
# downstream by faction. Source is WebMkCommittee (site backend); we keep only
# current memberships (to_date IS NULL), resolve the committee NAME to a
# committees_list id by normalised match (whitespace/punctuation stripped — this
# matches the permanent committees; ad-hoc sub/joint committees with divergent
# names simply don't resolve and are dropped, matching the spec's scope), and
# attach each MK's latest faction for that term (same logic as mk_summary).
_COMMITTEE_MEMBERS_SQL = r"""
WITH latest_faction AS (
    SELECT
        PersonID, KnessetNum, FactionID, FactionName,
        ROW_NUMBER() OVER (
            PARTITION BY PersonID, KnessetNum
            ORDER BY TRY_CAST(StartDate AS TIMESTAMP) DESC NULLS LAST,
                     PersonToPositionID DESC
        ) AS rn
    FROM KNS_PersonToPosition
    WHERE FactionID IS NOT NULL
),
committee_ids AS (
    SELECT
        CAST(CommitteeID AS BIGINT) AS committee_id,
        KnessetNum,
        regexp_replace(TRIM(Name), '[\s,"'']', '', 'g') AS norm_name
    FROM KNS_Committee
)
SELECT DISTINCT
    ci.committee_id                       AS committee_id,
    CAST(wmc.knesset_num AS INTEGER)      AS knesset_num,
    CAST(wmc.mk_id AS BIGINT)             AS mk_id,
    TRIM(p.FirstName || ' ' || p.LastName) AS mk_name_he,
    CAST(lf.FactionID AS BIGINT)          AS faction_id,
    lf.FactionName                        AS faction_name,
    wmc.role_he                           AS role_he,
    p.IsCurrent                           AS is_current
FROM WebMkCommittee wmc
JOIN KNS_Person p ON p.PersonID = wmc.mk_id
JOIN committee_ids ci
    ON ci.KnessetNum = wmc.knesset_num
    AND ci.norm_name = regexp_replace(TRIM(wmc.committee_name_he), '[\s,"'']', '', 'g')
LEFT JOIN latest_faction lf
    ON lf.PersonID = wmc.mk_id AND lf.KnessetNum = wmc.knesset_num AND lf.rn = 1
WHERE wmc.to_date IS NULL
ORDER BY committee_id, faction_name NULLS LAST, mk_name_he, mk_id, role_he
""".strip()

# (snapshot_name, SQL) tuples in stable order. Stable order is important
# for reproducibility guarantees (byte-equivalent manifest on unchanged data).
SNAPSHOTS: tuple[tuple[str, str], ...] = (
    ("mk_summary", MK_QUERIES["mk_summary"]["sql"]),
    ("mk_bills", _MK_BILLS_SQL),
    ("mk_questions", _MK_QUESTIONS_SQL),
    ("mk_motions", _MK_MOTIONS_SQL),
    ("parties_list", PARTIES_QUERIES["party_list"]["sql"]),
    ("committees_list", COMMITTEES_QUERIES["committee_list"]["sql"]),
    ("votes_list", VOTES_QUERIES["votes_list"]["sql"]),
    ("mk_votes", VOTES_QUERIES["mk_votes"]["sql"]),
    ("party_metadata", _PARTY_METADATA_SQL),
    ("committee_topics_ministries", _COMMITTEE_TOPICS_MINISTRIES_SQL),
    ("committee_sessions_by_type", _COMMITTEE_SESSIONS_BY_TYPE_SQL),
    ("mk_cv", _MK_CV_SQL),
    ("committee_members_by_faction", _COMMITTEE_MEMBERS_SQL),
)

# Keep BILLS_QUERIES referenced so lint doesn't drop the import —
# Phase 4 will switch mk_bills to a real helper inside bills.py.
_ = BILLS_QUERIES


def _sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _utc_isoformat(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def export_snapshot(
    con: duckdb.DuckDBPyConnection, name: str, sql: str, output_dir: Path
) -> SnapshotEntry:
    """Export one query to ``<name>.parquet`` atomically. Returns manifest entry."""
    final_path = output_dir / f"{name}.parquet"
    tmp_path = output_dir / f"{name}.parquet.new"
    # COPY … TO … FORMAT PARQUET streams directly from DuckDB's columnar engine;
    # no pandas roundtrip. COMPRESSION ZSTD trades a bit of CPU for ~2x smaller
    # files vs. snappy on our data shapes.
    con.execute(f"COPY ({sql}) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp_path}')").fetchone()
    assert row is not None  # COUNT(*) always returns one row
    rows = int(row[0])
    size_bytes = tmp_path.stat().st_size
    digest = _sha256_of_file(tmp_path)
    os.replace(tmp_path, final_path)
    log.info(
        "exported %s: rows=%d bytes=%d sha256=%s…", name, rows, size_bytes, digest[:12]
    )
    return SnapshotEntry(rows=int(rows), sha256=digest, bytes=int(size_bytes))


def export_all(warehouse: Path, output_dir: Path) -> Manifest:
    """Run all snapshots. Manifest is written last; individual parquets first."""
    output_dir.mkdir(parents=True, exist_ok=True)
    warehouse_mtime = warehouse.stat().st_mtime
    started_at = datetime.now(tz=timezone.utc)
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        entries: dict[str, SnapshotEntry] = {}
        for name, sql in SNAPSHOTS:
            entries[name] = export_snapshot(con, name, sql, output_dir)
    finally:
        con.close()
    manifest = Manifest(
        version=1,
        generated_at_utc=started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        warehouse_mtime_utc=_utc_isoformat(warehouse_mtime),
        snapshots=entries,
    )
    write_manifest(output_dir / "manifest.json", manifest)
    log.info("manifest committed → %s", output_dir / "manifest.json")
    # Belt-and-braces: clean up any stray `.new` files if an earlier run
    # crashed between tempfile write and replace. os.replace already
    # handled the happy path; this only catches leftover sidecars.
    for stray in output_dir.glob("*.new"):
        stray.unlink(missing_ok=True)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="data.snapshots.exporter")
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=Path("data/warehouse.duckdb"),
        help="Path to the DuckDB warehouse (read-only).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/snapshots"),
        help="Destination directory for Parquet snapshots + manifest.json.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    if not args.warehouse.exists():
        log.error("warehouse not found: %s", args.warehouse)
        return 2
    export_all(args.warehouse, args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())

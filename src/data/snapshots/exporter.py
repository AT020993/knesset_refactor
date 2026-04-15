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
from data.queries.packs.topics import TOPICS_QUERIES
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
    ubc.CAPMinorCode                AS cap_code,
    b.PublicationDate               AS submit_date,
    CAST(bi.Ordinal AS INTEGER)     AS initiator_ordinal,
    bi.IsInitiator                  AS is_main_initiator
FROM KNS_BillInitiator bi
JOIN KNS_Bill b ON bi.BillID = b.BillID
LEFT JOIN UserBillCAP ubc ON bi.BillID = ubc.BillID
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

# (snapshot_name, SQL) tuples in stable order. Stable order is important
# for reproducibility guarantees (byte-equivalent manifest on unchanged data).
SNAPSHOTS: tuple[tuple[str, str], ...] = (
    ("mk_summary", MK_QUERIES["mk_summary"]["sql"]),
    ("mk_bills", _MK_BILLS_SQL),
    ("mk_questions", _MK_QUESTIONS_SQL),
    ("mk_motions", _MK_MOTIONS_SQL),
    ("parties_list", PARTIES_QUERIES["party_list"]["sql"]),
    ("committees_list", COMMITTEES_QUERIES["committee_list"]["sql"]),
    ("topics_list", TOPICS_QUERIES["topic_list"]["sql"]),
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
    con.execute(
        f"COPY ({sql}) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp_path}')").fetchone()
    assert row is not None  # COUNT(*) always returns one row
    rows = int(row[0])
    size_bytes = tmp_path.stat().st_size
    digest = _sha256_of_file(tmp_path)
    os.replace(tmp_path, final_path)
    log.info("exported %s: rows=%d bytes=%d sha256=%s…", name, rows, size_bytes, digest[:12])
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

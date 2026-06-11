"""Ingest MK CV + committee membership into the warehouse.

Pipeline:
  1. Resolve ``{PersonID: SiteId}`` from OData ``KNS_MkSiteCode``.
  2. Target MKs who served in the given Knesset (``KNS_PersonToPosition``).
  3. Concurrently fetch each MK's CV (``GetMkDetailsContent``) and positions
     (``GetMkPositions``) from the site backend.
  4. Replace two warehouse tables: ``WebMkCv`` (one row per MK) and
     ``WebMkCommittee`` (one row per MK-committee membership for the Knesset).

Run (PYTHONPATH=src, from the project root)::

    python -m data.mk_details.ingest --warehouse data/warehouse.duckdb --knesset 25

The snapshot exporter then shapes these into ``mk_cv.parquet`` and
``committee_members_by_faction.parquet``.
"""

from __future__ import annotations

import argparse
import html
import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from data.mk_details.mk_details_client import MkDetailsClient

log = logging.getLogger("data.mk_details.ingest")

CV_TABLE = "WebMkCv"
COMMITTEE_TABLE = "WebMkCommittee"

_GREGORIAN_DATE = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})")


def _clean(value: Any) -> str | None:
    """Unescape HTML entities, flatten CR/LF, collapse whitespace; '' → None."""
    if value is None:
        return None
    text = html.unescape(str(value))
    text = re.sub(r"\s+", " ", text.replace("\r", " ").replace("\n", " ")).strip()
    return text or None


def _birth_date(value: Any) -> str | None:
    """Keep the Gregorian DD/MM/YYYY if present (the field also carries a Hebrew
    date), else the cleaned raw string."""
    cleaned = _clean(value)
    if not cleaned:
        return None
    match = _GREGORIAN_DATE.search(cleaned)
    return match.group(1) if match else cleaned


def _target_person_ids(con: duckdb.DuckDBPyConnection, knesset: int) -> list[int]:
    rows = con.execute(
        """
        SELECT DISTINCT PersonID
        FROM KNS_PersonToPosition
        WHERE KnessetNum = ? AND PersonID IS NOT NULL
        ORDER BY PersonID
        """,
        [knesset],
    ).fetchall()
    return [int(r[0]) for r in rows]


def _cv_row(mk_id: int, cv: dict[str, Any] | None) -> dict[str, Any] | None:
    if not cv:
        return None
    row = {
        "mk_id": mk_id,
        "birth_date": _birth_date(cv.get("DateOfBirth")),
        "birth_place_he": _clean(cv.get("PlaceOfBirth")),
        "education_he": _clean(cv.get("Education")),
        "military_service_he": _clean(cv.get("MilitaryService")),
        "languages_he": _clean(cv.get("Languages")),
    }
    # Skip MKs the site has no biographical content for at all.
    if any(row[k] for k in row if k != "mk_id"):
        return row
    return None


def _committee_rows(
    mk_id: int, positions: list[dict[str, Any]], knesset: int
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for block in positions:
        if block.get("KnessetId") != knesset:
            continue
        for c in block.get("Committee") or []:
            name = _clean(c.get("CommitteeName"))
            if not name:
                continue
            rows.append(
                {
                    "mk_id": mk_id,
                    "knesset_num": knesset,
                    "committee_name_he": name,
                    "role_he": _clean(c.get("Name")),
                    "from_date": _clean(c.get("FromDate")),
                    "to_date": _clean(c.get("ToDate")),
                }
            )
    return rows


def ingest(
    warehouse: Path,
    knesset: int,
    *,
    max_workers: int = 6,
    limit: int | None = None,
) -> tuple[int, int]:
    """Fetch + replace ``WebMkCv`` / ``WebMkCommittee``. Returns (cv_rows, cmt_rows)."""
    client = MkDetailsClient()
    con = duckdb.connect(str(warehouse), read_only=False)
    try:
        site_map = client.site_code_map()
        person_ids = _target_person_ids(con, knesset)
        targets = [(pid, site_map[pid]) for pid in person_ids if pid in site_map]
        log.info(
            "knesset %d: %d members, %d with a SiteId (%d unmapped, skipped)",
            knesset,
            len(person_ids),
            len(targets),
            len(person_ids) - len(targets),
        )
        if limit is not None:
            targets = targets[:limit]

        cv_rows: list[dict[str, Any]] = []
        committee_rows: list[dict[str, Any]] = []

        def fetch(pid_site: tuple[int, int]) -> tuple[int, Any, Any]:
            pid, site_id = pid_site
            return pid, client.cv(site_id), client.positions(site_id)

        done = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(fetch, t): t for t in targets}
            for fut in as_completed(futures):
                pid = futures[fut][0]
                done += 1
                try:
                    _, cv, positions = fut.result()
                except Exception as exc:  # noqa: BLE001 — log and continue
                    log.warning("mk %s details failed: %s", pid, exc)
                    continue
                row = _cv_row(pid, cv)
                if row:
                    cv_rows.append(row)
                committee_rows.extend(_committee_rows(pid, positions, knesset))
                if done % 25 == 0:
                    log.info("fetched %d/%d MKs", done, len(targets))

        _replace(con, CV_TABLE, pd.DataFrame(cv_rows), _CV_COLUMNS)
        _replace(con, COMMITTEE_TABLE, pd.DataFrame(committee_rows), _COMMITTEE_COLUMNS)
        log.info(
            "stored %d CV rows, %d committee-membership rows",
            len(cv_rows),
            len(committee_rows),
        )
        return (len(cv_rows), len(committee_rows))
    finally:
        con.close()


_CV_COLUMNS = [
    "mk_id",
    "birth_date",
    "birth_place_he",
    "education_he",
    "military_service_he",
    "languages_he",
]
_COMMITTEE_COLUMNS = [
    "mk_id",
    "knesset_num",
    "committee_name_he",
    "role_he",
    "from_date",
    "to_date",
]


def _replace(
    con: duckdb.DuckDBPyConnection,
    table: str,
    df_new: pd.DataFrame,
    columns: list[str],
) -> None:
    """Full-refresh a table — current MK detail is a small, replace-in-full set."""
    frame = (
        pd.DataFrame({c: pd.Series(dtype="object") for c in columns})
        if df_new.empty
        else df_new[columns]
    )
    con.register("df_new", frame)
    con.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM df_new')
    con.unregister("df_new")
    log.info("wrote %d rows to %s", len(frame), table)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="data.mk_details.ingest")
    p.add_argument("--warehouse", type=Path, default=Path("data/warehouse.duckdb"))
    p.add_argument("--knesset", type=int, default=25)
    p.add_argument("--max-workers", type=int, default=6)
    p.add_argument("--limit", type=int, default=None, help="Cap MKs (for testing).")
    p.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    args = p.parse_args(argv)
    logging.basicConfig(
        level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    if not args.warehouse.exists():
        log.error("warehouse not found: %s", args.warehouse)
        return 2
    cv_rows, committee_rows = ingest(
        args.warehouse,
        args.knesset,
        max_workers=args.max_workers,
        limit=args.limit,
    )
    log.info("done: %d CV rows, %d committee rows", cv_rows, committee_rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())

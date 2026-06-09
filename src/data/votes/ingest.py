"""Ingest live plenum votes into the warehouse (``WebVoteHeader`` + ``WebVoteMk``).

Pipeline:
  1. Build a name→PersonID matcher scoped to the target Knesset's members.
  2. Pull all vote headers; keep the target Knesset; skip vote_ids already stored
     (incremental — re-runs only fetch genuinely new votes).
  3. Fetch per-vote details concurrently; resolve each MkName to a PersonID.
  4. Append to the two warehouse tables.

Run (PYTHONPATH=src, from the project root)::

    python -m data.votes.ingest --warehouse data/warehouse.duckdb --knesset 25

The snapshot exporter then shapes ``WebVoteHeader``/``WebVoteMk`` into
``votes_list.parquet`` and ``mk_votes.parquet`` (see queries/packs/votes.py).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from data.votes.mk_matcher import MkNameMatcher
from data.votes.web_votes_client import RESULT_ID_TO_POSITION, WebVotesClient

log = logging.getLogger("data.votes.ingest")

HEADER_TABLE = "WebVoteHeader"
MK_TABLE = "WebVoteMk"

# Hebrew vote-counter titles → canonical position, for show-of-hands votes that
# carry no per-MK breakdown (only VoteCounters).
_COUNTER_TITLE_TO_POS = {
    "בעד": "for",
    "נגד": "against",
    "נמנע": "abstain",
    "נוכח": "present",
}


def _build_matcher(con: duckdb.DuckDBPyConnection, knesset: int) -> MkNameMatcher:
    rows = con.execute(
        """
        SELECT DISTINCT p.PersonID, p.FirstName, p.LastName
        FROM KNS_Person p
        WHERE p.PersonID IN (
            SELECT DISTINCT PersonID FROM KNS_PersonToPosition WHERE KnessetNum = ?
        )
        """,
        [knesset],
    ).fetchall()
    log.info("matcher built from %d Knesset-%d members", len(rows), knesset)
    return MkNameMatcher([(int(r[0]), r[1] or "", r[2] or "") for r in rows])


def _existing_vote_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    tbl = con.execute(
        "SELECT 1 FROM duckdb_tables() WHERE table_name = ?", [HEADER_TABLE]
    ).fetchone()
    if not tbl:
        return set()
    return {
        int(r[0])
        for r in con.execute(f'SELECT vote_id FROM "{HEADER_TABLE}"').fetchall()
    }


def _counter_totals(details: dict[str, Any]) -> dict[str, int]:
    totals = {"for": 0, "against": 0, "abstain": 0, "present": 0}
    for c in details.get("VoteCounters", []) or []:
        pos = _COUNTER_TITLE_TO_POS.get((c.get("Title") or "").strip())
        if pos:
            totals[pos] += int(c.get("countOfResult") or 0)
    return totals


def _parse_vote(
    vid: int,
    det: dict[str, Any],
    header: dict[str, Any],
    knesset: int,
    matcher: MkNameMatcher,
) -> tuple[dict[str, Any], list[dict[str, Any]], int, set[Any]]:
    """Turn one vote's detail payload into a header row + per-MK rows."""
    vh = det.get("VoteHeader") or [{}]
    vh0 = vh[0] if vh else {}
    per_mk = det.get("VoteDetails") or []
    is_electronic = len(per_mk) > 0
    mk_rows: list[dict[str, Any]] = []
    unresolved = 0
    unknown: set[Any] = set()

    if is_electronic:
        totals = {"for": 0, "against": 0, "abstain": 0, "present": 0}
        for m in per_mk:
            rid = m.get("VoteResultId")
            pos = RESULT_ID_TO_POSITION.get(rid)
            if pos is None:
                unknown.add(rid)
                pos = _COUNTER_TITLE_TO_POS.get(
                    (m.get("Title") or "").strip(), "present"
                )
            totals[pos] = totals.get(pos, 0) + 1
            mk_id = matcher.resolve(m.get("MkName") or "")
            if mk_id is None:
                unresolved += 1
            mk_rows.append(
                {
                    "vote_id": vid,
                    "mk_id": mk_id,
                    "mk_name": m.get("MkName"),
                    "faction_name": m.get("FactionName"),
                    "position": pos,
                }
            )
    else:
        totals = _counter_totals(det)

    accepted = vh0.get("IsForAccepted")
    header_row = {
        "vote_id": vid,
        "knesset_num": int(knesset),
        "vote_date": header.get("VoteDate"),
        "vote_type": header.get("VoteType"),
        "item_title": header.get("ItemTitle"),
        "is_accepted": bool(accepted) if accepted is not None else None,
        "is_electronic": is_electronic,
        "total_for": totals["for"],
        "total_against": totals["against"],
        "total_abstain": totals["abstain"],
        "total_present": totals["present"],
    }
    return header_row, mk_rows, unresolved, unknown


def ingest(
    warehouse: Path,
    knesset: int,
    *,
    max_workers: int = 4,
    batch_size: int = 250,
    limit: int | None = None,
) -> tuple[int, int]:
    """Fetch + persist new votes in batches (progress survives interruptions).

    Returns (new_votes, unresolved_mk_rows).
    """
    client = WebVotesClient()
    con = duckdb.connect(str(warehouse), read_only=False)
    try:
        matcher = _build_matcher(con, knesset)
        existing = _existing_vote_ids(con)

        headers = client.get_headers()
        target = [h for h in headers if str(h.get("KnessetId")) == str(knesset)]
        new_headers = [h for h in target if int(h["VoteId"]) not in existing]
        if limit is not None:
            new_headers = new_headers[:limit]
        log.info(
            "knesset %d: %d votes total, %d already stored, %d to fetch",
            knesset,
            len(target),
            len(existing),
            len(new_headers),
        )
        if not new_headers:
            return (0, 0)

        total_new = 0
        total_unresolved = 0
        unknown_codes: set[Any] = set()
        for start in range(0, len(new_headers), batch_size):
            batch = new_headers[start : start + batch_size]
            header_by_id = {int(h["VoteId"]): h for h in batch}
            details = client.fetch_details_concurrent(
                list(header_by_id), max_workers=max_workers
            )
            header_rows: list[dict[str, Any]] = []
            mk_rows: list[dict[str, Any]] = []
            for vid, det in details.items():
                hr, mks, unres, unk = _parse_vote(
                    vid, det, header_by_id[vid], knesset, matcher
                )
                header_rows.append(hr)
                mk_rows.extend(mks)
                total_unresolved += unres
                unknown_codes |= unk
            if header_rows:
                _append(con, HEADER_TABLE, pd.DataFrame(header_rows))
            if mk_rows:
                _append(con, MK_TABLE, pd.DataFrame(mk_rows))
            total_new += len(header_rows)
            log.info(
                "batch %d-%d: stored %d votes (%d/%d done)",
                start,
                start + len(batch),
                len(header_rows),
                min(start + batch_size, len(new_headers)),
                len(new_headers),
            )

        if unknown_codes:
            log.warning(
                "unknown VoteResultId values (mapped via Title): %s", unknown_codes
            )
        return (total_new, total_unresolved)
    finally:
        con.close()


def _append(con: duckdb.DuckDBPyConnection, table: str, df_new: pd.DataFrame) -> None:
    """Create the table, or append the new rows to the existing one."""
    exists = con.execute(
        "SELECT 1 FROM duckdb_tables() WHERE table_name = ?", [table]
    ).fetchone()
    con.register("df_new", df_new)
    if exists:
        con.execute(f'INSERT INTO "{table}" SELECT * FROM df_new')
    else:
        con.execute(f'CREATE TABLE "{table}" AS SELECT * FROM df_new')
    con.unregister("df_new")
    log.info("wrote %d rows to %s", len(df_new), table)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="data.votes.ingest")
    p.add_argument("--warehouse", type=Path, default=Path("data/warehouse.duckdb"))
    p.add_argument("--knesset", type=int, default=25)
    p.add_argument("--max-workers", type=int, default=4)
    p.add_argument(
        "--batch-size", type=int, default=250, help="Votes per persisted batch."
    )
    p.add_argument(
        "--limit", type=int, default=None, help="Cap new votes (for testing)."
    )
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
    new_votes, unresolved = ingest(
        args.warehouse,
        args.knesset,
        max_workers=args.max_workers,
        batch_size=args.batch_size,
        limit=args.limit,
    )
    log.info(
        "done: %d new votes ingested, %d unresolved MK rows", new_votes, unresolved
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

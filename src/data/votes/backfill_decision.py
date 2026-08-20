"""Backfill ``WebVoteHeader.decision`` for votes ingested before it was kept.

``data.votes.ingest`` is incremental — it skips vote_ids already stored — so
adding a field to ``_parse_vote`` only affects votes fetched from then on. The
7,574 rows already in the warehouse were persisted without it, and the value
was never held anywhere else, so recovering it means re-fetching each vote's
detail payload.

Why the field is worth the fetch: it is the ONLY thing distinguishing two votes
on the same bill in the same sitting. Votes 46700 and 46699 share a title, a
date, a minute and a tally; their decisions are ``'לקבל בקריאה שנייה'`` and
``'לקבל את הצעת החוק בקריאה שלישית'`` — a second and a third reading, two
distinct constitutional acts that the site was rendering as duplicate rows.

Design notes:

* **Resumable.** Selects only rows where ``decision IS NULL`` and commits after
  each batch, so an interrupted run resumes where it stopped. Re-running when
  nothing is missing is a no-op.
* **Newest first.** Ordered ``vote_date DESC`` so a run that dies partway has
  already fixed the most-visible surface — the recent-votes list on the home
  page — rather than the oldest votes nobody is looking at.
* **Politeness.** Same ``max_workers`` default as ``ingest`` (4). This is an
  undocumented endpoint on a government site; do not raise it.
* **Secret votes have no decision.** Verified against one vote of each type
  before running: אלקטרונית, שמית and הרמת יד all return one, חשאית returns
  ``None``. So ~8 rows are expected to stay NULL, and ``--report`` prints the
  residual by vote type so an expected gap is distinguishable from a failure.

Run (PYTHONPATH=src, from the project root)::

    python -m data.votes.backfill_decision --warehouse data/warehouse.duckdb
    python -m data.votes.backfill_decision --report
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb

from data.votes.web_votes_client import WebVotesClient

log = logging.getLogger("data.votes.backfill_decision")

HEADER_TABLE = "WebVoteHeader"


def ensure_column(con: duckdb.DuckDBPyConnection) -> None:
    """Add ``decision`` if it is not there yet. ALTER appends it last, which is
    the same position ``_parse_vote`` now emits it in — the two orderings have
    to agree for any positional insert that predates the named-column fix."""
    cols = {r[1] for r in con.execute(f'PRAGMA table_info("{HEADER_TABLE}")').fetchall()}
    if "decision" in cols:
        return
    con.execute(f'ALTER TABLE "{HEADER_TABLE}" ADD COLUMN decision VARCHAR')
    log.info("added %s.decision", HEADER_TABLE)


def _header(payload: dict) -> dict:
    h = payload.get("VoteHeader")
    if isinstance(h, list):
        return h[0] if h else {}
    return h or {}


def report(con: duckdb.DuckDBPyConnection) -> None:
    total, missing = con.execute(
        f'SELECT count(*), count(*) FILTER (WHERE decision IS NULL) FROM "{HEADER_TABLE}"'
    ).fetchone()
    print(f"{HEADER_TABLE}: {total} rows, {missing} without a decision")
    if missing:
        print("\nmissing by vote type (חשאית is expected — the API returns none):")
        for vote_type, n in con.execute(
            f'SELECT vote_type, count(*) FROM "{HEADER_TABLE}" '
            "WHERE decision IS NULL GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall():
            print(f"  {vote_type or '(none)':12} {n}")
    print("\nmost common decisions:")
    for dec, n in con.execute(
        f'SELECT decision, count(*) FROM "{HEADER_TABLE}" '
        "WHERE decision IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 15"
    ).fetchall():
        print(f"  {n:5}  {dec}")
    distinct = con.execute(
        f'SELECT count(DISTINCT decision) FROM "{HEADER_TABLE}"'
    ).fetchone()[0]
    print(f"\ndistinct decisions: {distinct}")


def backfill(
    warehouse: Path, *, max_workers: int = 4, batch_size: int = 250,
    limit: int | None = None,
) -> int:
    client = WebVotesClient()
    con = duckdb.connect(str(warehouse), read_only=False)
    try:
        ensure_column(con)
        pending = [
            int(r[0])
            for r in con.execute(
                f'SELECT vote_id FROM "{HEADER_TABLE}" WHERE decision IS NULL '
                "ORDER BY vote_date DESC, vote_id DESC"
            ).fetchall()
        ]
        if limit is not None:
            pending = pending[:limit]
        log.info("%d votes need a decision", len(pending))
        if not pending:
            return 0

        filled = 0
        for start in range(0, len(pending), batch_size):
            batch = pending[start : start + batch_size]
            details = client.fetch_details_concurrent(batch, max_workers=max_workers)
            pairs = []
            for vid, payload in details.items():
                decision = _header(payload).get("Decision")
                if decision:
                    pairs.append((int(vid), str(decision)))
            if pairs:
                con.executemany(
                    f'UPDATE "{HEADER_TABLE}" SET decision = ? WHERE vote_id = ?',
                    [(d, v) for v, d in pairs],
                )
                con.commit()
                filled += len(pairs)
            log.info(
                "batch %d-%d: filled %d (%d/%d done, %d filled overall)",
                start, start + len(batch), len(pairs),
                min(start + batch_size, len(pending)), len(pending), filled,
            )
        return filled
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="data.votes.backfill_decision")
    p.add_argument("--warehouse", type=Path, default=Path("data/warehouse.duckdb"))
    p.add_argument("--max-workers", type=int, default=4)
    p.add_argument("--batch-size", type=int, default=250)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--report", action="store_true", help="print coverage and exit")
    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    if not args.warehouse.exists():
        log.error("warehouse not found: %s", args.warehouse)
        return 2
    if args.report:
        con = duckdb.connect(str(args.warehouse), read_only=True)
        try:
            report(con)
        finally:
            con.close()
        return 0
    filled = backfill(
        args.warehouse,
        max_workers=args.max_workers,
        batch_size=args.batch_size,
        limit=args.limit,
    )
    log.info("done: filled %d decisions", filled)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Vote snapshot query definitions.

Shapes the warehouse vote tables (``WebVoteHeader`` + ``WebVoteMk``, populated
by ``data.votes.ingest`` from the live Knesset WebSiteApi) into the two Parquet
snapshots the FastAPI contract consumes:

  * ``votes_list``  — one row per plenum vote (header + tallies).
  * ``mk_votes``    — one row per (MK, vote) with the MK's canonical position.

``position`` is language-neutral: ``for`` / ``against`` / ``abstain`` /
``present`` (UI maps to Hebrew labels). Faction is resolved downstream by
joining ``mk_id`` to ``mk_summary`` so cohesion uses the platform's own faction
definition, not the API's per-vote faction string.
"""

from __future__ import annotations

from typing import Any

VOTES_QUERIES: dict[str, dict[str, Any]] = {
    "votes_list": {
        "sql": """
SELECT
    CAST(vote_id AS BIGINT)        AS vote_id,
    CAST(knesset_num AS INTEGER)   AS knesset_num,
    vote_date,
    vote_type,
    item_title,
    is_accepted,
    is_electronic,
    CAST(total_for AS INTEGER)     AS total_for,
    CAST(total_against AS INTEGER) AS total_against,
    CAST(total_abstain AS INTEGER) AS total_abstain,
    CAST(total_present AS INTEGER) AS total_present,
    -- The plenum's decision, verbatim ("לקבל בקריאה שנייה", "להעביר את הצעת
    -- החוק לוועדה להכנה לקריאה ראשונה"). The ONLY field separating two votes
    -- on the same bill in the same sitting: 46700 and 46699 match on title,
    -- date, minute and tally, and are a second and a third reading.
    --
    -- NULL for חשאית (secret) votes — the live API returns none for those,
    -- verified one vote per type before the backfill. Consumers must treat it
    -- as optional; a null is the API's own behaviour, not a fetch failure.
    decision
FROM WebVoteHeader
ORDER BY knesset_num, vote_date DESC, vote_id DESC
""".strip(),
        "knesset_filter_column": "knesset_num",
        "faction_filter_column": None,
        "description": "Plenum votes (header, tallies, and the verbatim plenum decision) from the live Knesset votes API.",
    },
    "mk_votes": {
        "sql": """
SELECT
    CAST(m.mk_id AS BIGINT)        AS mk_id,
    CAST(m.vote_id AS BIGINT)      AS vote_id,
    CAST(h.knesset_num AS INTEGER) AS knesset_num,
    m.position                     AS position,
    h.vote_date                    AS vote_date
FROM WebVoteMk m
JOIN WebVoteHeader h ON m.vote_id = h.vote_id
WHERE m.mk_id IS NOT NULL
ORDER BY m.mk_id, h.vote_date DESC, m.vote_id DESC
""".strip(),
        "knesset_filter_column": "h.knesset_num",
        "faction_filter_column": None,
        "description": "Per-MK vote positions (resolved to PersonID) for cohesion + history.",
    },
}

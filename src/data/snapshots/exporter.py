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
from data.snapshots.bill_status import (
    BILL_STATUS_RUNGS,
    MAPPED_STATUS_IDS,
    RUNG_ORDER,
    UnmappedBillStatusError,
)
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
    -- MajorIL is the last fallback: recent Knessets (K21-25) were coded only in
    -- the MAJORIL scheme (MajorCAP is NULL), and MajorIL == MajorCAP in 99.99% of
    -- the K10-20 overlap, so it is the same major-topic code. Without this, all
    -- CAP/topic data for K23-25 renders empty. MajorCAP still wins where present.
    COALESCE(ubcap_tax.MajorCode, ubcoding.MajorCAP, ubcoding.MajorIL) AS cap_code,
    b.PublicationDate               AS submit_date,
    CAST(bi.Ordinal AS INTEGER)     AS initiator_ordinal,
    bi.IsInitiator                  AS is_main_initiator
FROM KNS_BillInitiator bi
JOIN KNS_Bill b ON bi.BillID = b.BillID
LEFT JOIN UserBillCoding ubcoding ON bi.BillID = ubcoding.BillID
LEFT JOIN UserBillCAP ubcap ON bi.BillID = ubcap.BillID
LEFT JOIN UserCAPTaxonomy ubcap_tax ON ubcap.CAPMinorCode = ubcap_tax.MinorCode
-- Only private member bills are an MK's own legislative initiative. Government
-- ('ממשלתית') and committee ('ועדה') bills also list MKs as initiators upstream
-- (a minister who is an MK signs the government bill), but crediting them to the
-- MK misrepresents their legislative record, so exclude them here at the source.
-- The ``stage`` column is thus always 'פרטית' by construction.
WHERE bi.PersonID IS NOT NULL
  AND b.SubTypeDesc = 'פרטית'
ORDER BY bi.BillID, bi.Ordinal, bi.PersonID
""".strip()


def _bill_status_rung_case_sql(column: str) -> str:
    """``CASE`` mapping a bill status id to its rung label.

    Generated from ``BILL_STATUS_RUNGS`` (Task 1) so the ladder has exactly
    one definition — the SQL below must never retype the status ids.
    """
    lines = ["    CASE"]
    for rung, ids in BILL_STATUS_RUNGS.items():
        id_list = ", ".join(str(i) for i in ids)
        escaped = rung.replace("'", "''")
        lines.append(f"        WHEN {column} IN ({id_list}) THEN '{escaped}'")
    lines.append("        ELSE NULL")
    lines.append("    END")
    return "\n".join(lines)


def _bill_status_rung_order_case_sql(column: str) -> str:
    """``CASE`` mapping a bill status id to its rung's position in
    ``RUNG_ORDER`` — lets the consumer sort by "furthest reading reached"
    without re-deriving the ladder's order itself."""
    lines = ["    CASE"]
    for order, rung in enumerate(RUNG_ORDER):
        id_list = ", ".join(str(i) for i in BILL_STATUS_RUNGS[rung])
        lines.append(f"        WHEN {column} IN ({id_list}) THEN {order}")
    lines.append("        ELSE NULL")
    lines.append("    END")
    return "\n".join(lines)


#: The one bill sub-type the snapshot bundle covers. Shared by
#: ``_BILLS_LIST_SQL`` and the pre-flight status guard so the guard cannot
#: check a wider population than the snapshot actually exports.
_PRIVATE_MEMBER_SUB_TYPE = "פרטית"

# bills_list — titles + decoded status + reading-stage rung, keyed on
# bill_id. Kept as its own snapshot rather than denormalised into
# mk_bills: titles average 70 chars and mk_bills has 165k rows against 59k
# distinct bills, so denormalising would repeat each title 2.8x (11.6 MB vs
# 4.1 MB). The platform already carries bill_id and can join on it.
_BILLS_LIST_SQL = f"""
SELECT
    b.BillID                        AS bill_id,
    CAST(b.KnessetNum AS INTEGER)   AS knesset_num,
    b.Name                          AS name,
    b.SubTypeDesc                   AS sub_type,
    CAST(b.PrivateNumber AS BIGINT) AS private_number,
    b.IsContinuationBill            AS is_continuation_bill,
    fp.first_plenum_date            AS first_plenum_date,
    CAST(b.StatusID AS INTEGER)     AS status_id,
    s."Desc"                        AS status_desc,
{_bill_status_rung_case_sql("b.StatusID")} AS status_rung,
{_bill_status_rung_order_case_sql("b.StatusID")} AS status_rung_order
FROM KNS_Bill b
-- StatusID happens to be globally unique across KNS_Status today (verified
-- against production: 81 rows, 81 distinct ids), so this TypeDesc predicate
-- changes no output right now. It is defensive, not redundant: StatusID is
-- only *semantically* scoped to a TypeDesc family (bill/question/motion
-- share the table), and nothing in the schema enforces that a future id
-- can't collide across families. Without this, such a collision would
-- silently attach a question/motion's status label to a bill. Do not
-- delete this as a no-op — keep it even though no fixture can prove it
-- fires today.
LEFT JOIN KNS_Status s ON b.StatusID = s.StatusID AND s.TypeDesc = 'הצעת חוק'
-- Earliest plenum sitting at which the bill appeared — in practice its
-- preliminary reading (דיון מוקדם).
--
-- 🔴 This is NOT the submission date, and must not be renamed to suggest it
-- is. The warehouse has no submission date: KNS_Bill.PublicationDate is the
-- official gazette date and lands AFTER passage (172 of 6,674 K25 private
-- bills, median 366 days after this field), and KNS_BillHistoryInitiator is a
-- removal log — it records people ceasing to be initiators (חדל להיות חבר
-- כנסת, מינוי לתפקיד שר בממשלה), not joining.
--
-- What it is good for: dating a bill to a point inside the term, which
-- PublicationDate cannot do at 2.6% coverage. Consumers attributing a bill to
-- an MK's faction-at-the-time should use this and accept that submission
-- preceded it — a bill switching factions between submission and preliminary
-- reading is misattributed, which is still far better than attributing the
-- whole term to wherever the MK ended up.
--
-- Validated: 99.2% coverage on K25 private bills, and all 172 bills that also
-- have a PublicationDate have this date at or before it.
LEFT JOIN (
    SELECT p.ItemID AS BillID,
           MIN(TRY_CAST(ps.StartDate AS TIMESTAMP)) AS first_plenum_date
    FROM KNS_PlmSessionItem p
    JOIN KNS_PlenumSession ps ON p.PlenumSessionID = ps.PlenumSessionID
    WHERE p.ItemTypeDesc = 'הצעת חוק'
    GROUP BY p.ItemID
) fp ON b.BillID = fp.BillID
-- Scoped to private-member bills to match mk_bills, which is 'פרטית'-only by
-- construction (see _MK_BILLS_SQL) — a bills_list carrying government/
-- committee bills would let a join silently reintroduce them.
WHERE b.SubTypeDesc = '{_PRIVATE_MEMBER_SUB_TYPE}'
ORDER BY b.BillID
""".strip()

_MK_QUESTIONS_SQL = """
SELECT
    q.PersonID                      AS mk_id,
    q.QueryID                       AS question_id,
    CAST(q.KnessetNum AS INTEGER)   AS knesset_num,
    CAST(q.StatusID AS INTEGER)     AS status_id,
    s."Desc"                        AS status_desc,
    q.TypeDesc                      AS type_he,
    uqc.MajorCAP                    AS cap_code,
    q.SubmitDate                    AS submit_date
FROM KNS_Query q
LEFT JOIN UserQueryCoding uqc ON q.QueryID = uqc.QueryID
-- Denormalising status_desc straight into this row (rather than a separate
-- status_decode snapshot, the way _BILLS_LIST_SQL above is kept separate
-- from mk_bills) is the opposite call from bill titles, made deliberately:
-- there are only 15 short status labels total across questions and motions
-- combined, vs. titles averaging 70 chars over 165k bill rows. Joining the
-- label here spares every consumer a join and a second lookup table; the
-- two decisions look similar but are not in tension.
-- StatusID is only unique *within* a TypeDesc family in KNS_Status (bill/
-- question/motion share the table) — this predicate is defensive, not
-- currently load-bearing: verified against production, zero status ids
-- are reused across families today. Keep it anyway; an unscoped join would
-- silently attach a bill/motion status label to a question if that ever
-- changes. See _BILLS_LIST_SQL for the same pattern.
LEFT JOIN KNS_Status s ON q.StatusID = s.StatusID AND s.TypeDesc = 'שאילתה'
WHERE q.PersonID IS NOT NULL
ORDER BY q.QueryID
""".strip()

_MK_MOTIONS_SQL = """
SELECT
    CAST(a.InitiatorPersonID AS BIGINT) AS mk_id,
    a.AgendaID                          AS motion_id,
    CAST(a.KnessetNum AS INTEGER)       AS knesset_num,
    CAST(a.StatusID AS INTEGER)         AS status_id,
    s."Desc"                            AS status_desc,
    a.SubTypeDesc                       AS type_he,
    uac.MajorIL                         AS cap_code,
    a.PresidentDecisionDate             AS decision_date
FROM KNS_Agenda a
LEFT JOIN UserAgendaCoding uac ON a.AgendaID = uac.AgendaID
-- Denormalised status_desc for the same reason as _MK_QUESTIONS_SQL above
-- (few, short values — spares every consumer a join and a lookup table),
-- deliberately the opposite call from _BILLS_LIST_SQL's separate snapshot.
-- Same defensive TypeDesc scoping as _MK_QUESTIONS_SQL above: StatusID is
-- only semantically unique within a TypeDesc family, even though no
-- production collision exists today.
LEFT JOIN KNS_Status s ON a.StatusID = s.StatusID AND s.TypeDesc = 'הצעה לסדר היום'
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

def _committee_name_norm_sql(column: str) -> str:
    """Whitespace/punctuation-stripped normalisation for matching a free-text
    committee name (``WebMkCommittee.committee_name_he``, from the site
    backend) to ``KNS_Committee.Name``. Factored out so
    ``_COMMITTEE_MEMBERS_SQL`` and ``_MK_COMMITTEES_SQL`` share one
    definition — see ``_COMMITTEE_IDS_CTE_SQL`` below — instead of each
    re-typing the regex and silently drifting apart."""
    return f"regexp_replace(TRIM({column}), '[\\s,\"'']', '', 'g')"


# committee_ids — shared CTE resolving every KNS_Committee row to its
# normalised name. Embedded (via an f-string) into both queries below that
# need to match a WebMkCommittee free-text committee name to a
# committees_list id, so the resolution logic lives in exactly one place.
_COMMITTEE_IDS_CTE_SQL = f"""
committee_ids AS (
    SELECT
        CAST(CommitteeID AS BIGINT) AS committee_id,
        KnessetNum,
        {_committee_name_norm_sql("Name")} AS norm_name
    FROM KNS_Committee
)
""".strip()

# committee_members_by_faction — currently-serving members per committee, grouped
# downstream by faction. Source is WebMkCommittee (site backend); we keep only
# current memberships (to_date IS NULL), resolve the committee NAME to a
# committees_list id by normalised match (whitespace/punctuation stripped — this
# matches the permanent committees; ad-hoc sub/joint committees with divergent
# names simply don't resolve and are dropped, matching the spec's scope), and
# attach each MK's latest faction for that term (same logic as mk_summary).
_COMMITTEE_MEMBERS_SQL = f"""
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
{_COMMITTEE_IDS_CTE_SQL}
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
    AND ci.norm_name = {_committee_name_norm_sql("wmc.committee_name_he")}
LEFT JOIN latest_faction lf
    ON lf.PersonID = wmc.mk_id AND lf.KnessetNum = wmc.knesset_num AND lf.rn = 1
WHERE wmc.to_date IS NULL
ORDER BY committee_id, faction_name NULLS LAST, mk_name_he, mk_id, role_he
""".strip()

# mk_committees — full committee-membership HISTORY for one MK, straight off
# WebMkCommittee (site backend; see data.mk_details.ingest). Unlike
# committee_members_by_faction above, this deliberately does NOT filter
# to_date IS NULL: an MK's profile needs past memberships too, not just
# current seats, so both are kept.
#
# committee_id resolves through the same committee_ids CTE/normalisation as
# committee_members_by_faction (LEFT, not that query's inner JOIN — see
# below) so the site can link straight to /he/committee/{id} instead of
# reimplementing the name-normalisation match on its own side of the repo
# boundary. Verified against production: 1,000 of 1,595 rows (62.7%)
# resolve; of the other 595, 491 (40 distinct names) have no KNS_Committee
# row under ANY Knesset — genuine ad-hoc sub/joint committees never
# separately catalogued (same "ad-hoc … simply don't resolve" scope note as
# committee_members_by_faction above), e.g. "ועדת משנה לספורט" or a joint
# committee named after the bill it's convened for. The remaining 104 (6
# distinct names, e.g. "ועדת משנה לקידום עסקים קטנים ובינוניים") DO have a
# same-name KNS_Committee row, just tagged to a different KnessetNum than
# this membership's own 25 — the join is scoped to KnessetNum for both this
# query and committee_members_by_faction, so these don't resolve either;
# that's an existing characteristic of the shared resolution (not something
# this task introduces), and widening the join's Knesset scope is out of
# scope here. Unlike committee_members_by_faction — which only ever wanted
# resolved rows and so can inner-join and drop the rest — an MK's membership
# in any of these 595 is still real service, so the row is kept with
# committee_id = NULL rather than dropped; dropping it would understate that
# MK's committee record.
#
# LIMITATION (belongs here, not just in a plan doc, so it travels with the
# data): WebMkCommittee covers Knesset 25 ONLY — verified against production,
# `SELECT COUNT(DISTINCT knesset_num) FROM WebMkCommittee` = 1, 1,595 rows
# across 134 MKs. An MK who served solely in an earlier Knesset will have
# ZERO rows here. Do not treat that as a bug to special-case — the consumer
# must render an honest empty state (the site's existing `data_gaps`
# convention on the committee page) rather than implying no committee
# service ever occurred.
#
# The mk_id/knesset_num/committee_name_he WHERE clause below is defensive,
# not a real-world filter today: on production data it excludes 0 of 1,595
# rows (verified) since the ingest step already skips blank committee names.
# Kept anyway so a future ingest regression can't silently emit a membership
# with no committee name — such a row is useless to the consumer, not just
# incomplete.
_MK_COMMITTEES_SQL = f"""
WITH {_COMMITTEE_IDS_CTE_SQL}
SELECT
    CAST(wmc.mk_id AS BIGINT)        AS mk_id,
    CAST(wmc.knesset_num AS INTEGER) AS knesset_num,
    ci.committee_id                  AS committee_id,
    wmc.committee_name_he            AS committee_name_he,
    wmc.role_he                      AS role_he,
    wmc.from_date                    AS from_date,
    wmc.to_date                      AS to_date
FROM WebMkCommittee wmc
LEFT JOIN committee_ids ci
    ON ci.KnessetNum = wmc.knesset_num
    AND ci.norm_name = {_committee_name_norm_sql("wmc.committee_name_he")}
WHERE wmc.mk_id IS NOT NULL
  AND wmc.knesset_num IS NOT NULL
  AND wmc.committee_name_he IS NOT NULL
  AND TRIM(wmc.committee_name_he) != ''
ORDER BY wmc.mk_id, wmc.knesset_num, wmc.committee_name_he, wmc.role_he,
    wmc.from_date, wmc.to_date
""".strip()

# committee_bills — sessions-per-bill "depth" metric: for each (committee,
# bill) pair, how many DISTINCT sessions actually discussed it. Item/session
# counts elsewhere (committees_list.session_count, committee_sessions_by_type)
# answer "how many things is this committee doing" but not "how much work did
# it put into THIS bill" — a bill discussed once and one discussed across nine
# sessions look identical today. Join path: KNS_CmtSessionItem ->
# KNS_CommitteeSession on CommitteeSessionID (which carries CommitteeID),
# filtered to ItemTypeID = 2 (הצעת חוק).
#
# knesset_num comes from KNS_Committee (the committee's own term), NOT from
# KNS_CommitteeSession.KnessetNum. Session terms are occasionally stale
# relative to their committee's, and at least one (committee, bill) pair spans
# two session terms — grouping by the session's term would split it and break
# the one-row-per-pair contract this snapshot makes. The committee's term is
# 1:1 with CommitteeID and is also what committees_list keys on, so a consumer
# joining the two on (committee_id, knesset_num) never hits an orphan.
# Measured figures behind this live in docs/phase-c-snapshot-scope.md; they are
# re-export-dependent and deliberately not frozen here.
_COMMITTEE_BILLS_SQL = """
SELECT
    CAST(cs.CommitteeID AS BIGINT)         AS committee_id,
    CAST(c.KnessetNum AS INTEGER)          AS knesset_num,
    CAST(csi.ItemID AS BIGINT)             AS bill_id,
    COUNT(DISTINCT cs.CommitteeSessionID)  AS session_count
FROM KNS_CmtSessionItem csi
JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
JOIN KNS_Committee c ON c.CommitteeID = cs.CommitteeID
WHERE csi.ItemTypeID = 2
  AND cs.CommitteeID IS NOT NULL
  AND csi.ItemID IS NOT NULL
GROUP BY cs.CommitteeID, c.KnessetNum, csi.ItemID
ORDER BY committee_id, knesset_num, bill_id
""".strip()

# (snapshot_name, SQL) tuples in stable order. Stable order is important
# for reproducibility guarantees (byte-equivalent manifest on unchanged data).
_MK_ROLES_SQL = """
-- Executive office held by a person, with dates. One row per posting, so an
-- MK who moved between ministries in one term has several.
--
-- ``GovMinistryName IS NOT NULL`` IS the executive predicate. It selects
-- exactly nine PositionIDs — 31 vice-PM, 39/57 minister, 40/59 deputy
-- minister, 45 PM, 50 deputy PM, 51 acting PM, 73 alternate PM — every one of
-- which carries both a ministry and a DutyDesc, and nothing else in the table
-- carries a ministry at all. This is preferred over a hardcoded PositionID
-- list because the warehouse has no position codelist to check such a list
-- against; the predicate is self-describing and cannot silently drift.
--
-- Why this exists: ministers and deputy ministers may not submit private
-- bills, so a per-MK bill average is structurally depressed for whichever
-- bloc is governing. Consumers need the DATES, not just the fact of office —
-- 24 K25 members held a post for only 3.2% of the term (the outgoing
-- government's ministers, who then sat in opposition), so a flat "ever held
-- office" exclusion would be badly wrong.
--
-- ``is_current`` is deliberately not carried: ``finish_date IS NULL`` says
-- the same thing and is what date arithmetic actually needs.
SELECT
    PersonID                                AS mk_id,
    CAST(KnessetNum AS INTEGER)             AS knesset_num,
    CAST(PositionID AS INTEGER)             AS position_id,
    DutyDesc                                AS duty_desc,
    CAST(GovMinistryID AS BIGINT)           AS ministry_id,
    GovMinistryName                         AS ministry_name,
    CAST(GovernmentNum AS INTEGER)          AS government_num,
    StartDate                               AS start_date,
    FinishDate                              AS finish_date
FROM KNS_PersonToPosition
WHERE GovMinistryName IS NOT NULL
  AND KnessetNum IS NOT NULL
ORDER BY PersonID, KnessetNum, StartDate
"""


_MK_FACTION_SPANS_SQL = """
-- Dated faction membership per (MK, Knesset), as a NON-OVERLAPPING timeline.
--
-- ``mk_summary`` is last-faction-wins: it keeps one faction per MK per term,
-- so an MK who crossed the floor has their whole term attributed to wherever
-- they ended up. That makes any time-aware question unanswerable — most
-- sharply for coalition/opposition cohesion, where it silently scored an
-- MK's pre-switch votes against the bloc they had not yet joined.
--
-- The raw rows cannot be used directly. They duplicate (the same faction
-- appears two or three times per MK with different date ranges) and they
-- overlap (a superseded faction and its successor both start on the day of
-- the split). A consumer joining votes to raw spans would count one MK twice
-- in a single tally. Two passes fix that:
--
--   collapsed — one row per (MK, Knesset, faction). An open row anywhere in
--               the group wins, because a NULL FinishDate means still serving
--               and MAX() would otherwise prefer a stale closed duplicate.
--   trimmed   — each span ends where the next one begins, so the timeline is
--               a partition rather than a set of intervals. Spans left with
--               zero or negative length are same-day supersessions and are
--               dropped: the successor already covers that instant.
--
-- The result is guaranteed at most one faction per MK per instant.
WITH raw AS (
    SELECT
        PersonID,
        CAST(KnessetNum AS INTEGER)             AS KnessetNum,
        FactionID,
        FactionName,
        TRY_CAST(StartDate AS TIMESTAMP)        AS sd,
        TRY_CAST(FinishDate AS TIMESTAMP)       AS fd
    FROM KNS_PersonToPosition
    WHERE FactionID IS NOT NULL
      AND KnessetNum IS NOT NULL
      AND StartDate IS NOT NULL
),
collapsed AS (
    SELECT
        PersonID,
        KnessetNum,
        FactionID,
        ANY_VALUE(FactionName)                  AS FactionName,
        MIN(sd)                                 AS sd,
        CASE
            WHEN COUNT(*) FILTER (WHERE fd IS NULL) > 0 THEN NULL
            ELSE MAX(fd)
        END                                     AS fd
    FROM raw
    GROUP BY PersonID, KnessetNum, FactionID
),
seq AS (
    SELECT
        *,
        LEAD(sd) OVER (
            PARTITION BY PersonID, KnessetNum
            ORDER BY sd, FactionID
        )                                       AS next_sd
    FROM collapsed
),
trimmed AS (
    SELECT
        PersonID                                AS mk_id,
        KnessetNum                              AS knesset_num,
        CAST(FactionID AS BIGINT)               AS faction_id,
        FactionName                             AS faction_name,
        sd                                      AS start_date,
        CASE
            WHEN next_sd IS NOT NULL AND (fd IS NULL OR next_sd < fd)
            THEN next_sd
            ELSE fd
        END                                     AS finish_date
    FROM seq
)
SELECT *
FROM trimmed
WHERE finish_date IS NULL OR finish_date > start_date
ORDER BY mk_id, knesset_num, start_date
"""


SNAPSHOTS: tuple[tuple[str, str], ...] = (
    ("mk_summary", MK_QUERIES["mk_summary"]["sql"]),
    ("mk_roles", _MK_ROLES_SQL),
    ("mk_faction_spans", _MK_FACTION_SPANS_SQL),
    ("mk_bills", _MK_BILLS_SQL),
    ("bills_list", _BILLS_LIST_SQL),
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
    ("mk_committees", _MK_COMMITTEES_SQL),
    ("committee_bills", _COMMITTEE_BILLS_SQL),
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


def assert_every_bill_status_is_mapped(con: duckdb.DuckDBPyConnection) -> None:
    """Raise if any exportable bill carries a status outside the ladder.

    Scoped to ``_PRIVATE_MEMBER_SUB_TYPE`` because that is exactly what
    ``bills_list`` exports. Checking all of ``KNS_Bill`` would fail the
    nightly run on a government-bill status that never reaches a snapshot —
    the fixture alone carries two such rows (status 1 on ממשלתית/ועדה).

    NULL ``StatusID`` is not this guard's business: it would surface as a
    NULL rung too, but it means *absent* data rather than an unmapped
    status, and reporting it here would produce a nonsense id list. It is
    100% populated upstream today.
    """
    id_list = ", ".join(str(i) for i in sorted(MAPPED_STATUS_IDS))
    rows = con.execute(
        f"""
        SELECT DISTINCT CAST(StatusID AS INTEGER) AS status_id
        FROM KNS_Bill
        WHERE SubTypeDesc = '{_PRIVATE_MEMBER_SUB_TYPE}'
          AND StatusID IS NOT NULL
          AND CAST(StatusID AS INTEGER) NOT IN ({id_list})
        ORDER BY status_id
        """
    ).fetchall()
    if rows:
        unmapped = [int(r[0]) for r in rows]
        raise UnmappedBillStatusError(
            f"unmapped status ids in KNS_Bill: {unmapped} — add them to "
            f"BILL_STATUS_RUNGS in data/snapshots/bill_status.py. The ladder "
            f"is hand-authored (KNS_Status.OrderTransition is NULL upstream), "
            f"so a new status must be placed on a rung deliberately. Refusing "
            f"to export rather than emit NULL status_rung for these bills."
        )


def export_all(warehouse: Path, output_dir: Path) -> Manifest:
    """Run all snapshots. Manifest is written last; individual parquets first."""
    warehouse_mtime = warehouse.stat().st_mtime
    started_at = datetime.now(tz=timezone.utc)
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        # Pre-flight, before mkdir and before a single byte is written. A
        # raise from inside the export loop would leave fresh parquets beside
        # a stale manifest — a state worse than either, and one the consumer
        # reads live. Failing here leaves the previous bundle fully intact.
        assert_every_bill_status_is_mapped(con)
        output_dir.mkdir(parents=True, exist_ok=True)
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

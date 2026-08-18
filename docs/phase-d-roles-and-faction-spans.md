# Phase D — executive roles and dated faction membership

Two new snapshots, one corrected column. Both exist to make *time-aware*
questions answerable: who held executive office when, and who sat in which
faction when.

## `mk_roles`

One row per executive posting, with dates.

| column | note |
|---|---|
| `mk_id`, `knesset_num` | |
| `position_id`, `duty_desc` | e.g. 39 / `שר האוצר` |
| `ministry_id`, `ministry_name` | |
| `government_num` | |
| `start_date`, `finish_date` | `finish_date IS NULL` = still serving |

**The predicate is `GovMinistryName IS NOT NULL`**, not a PositionID list. It
selects exactly nine ids — 31 vice-PM, 39/57 minister, 40/59 deputy minister,
45 PM, 50 deputy PM, 51 acting PM, 73 alternate PM — every one of which
carries both a ministry and a `DutyDesc`, and nothing else in
`KNS_PersonToPosition` carries a ministry at all. The warehouse has no
position codelist, so a hardcoded id list would be an unverifiable guess.

**Why the dates are load-bearing.** Ministers and deputy ministers may not
submit private bills, so a bloc's per-MK bill average is structurally
depressed while it governs. But a flat "held office this term" exclusion is
badly wrong: of K25's office-holders, 24 held a post for only **3.2%** of the
term — the outgoing government's ministers, who then sat in opposition for the
remaining four years. Consumers must weight by tenure, not by a boolean.

K25: 132 rows, 72 people, 64 of whom also hold a faction seat.

## `mk_faction_spans`

Dated faction membership per (MK, Knesset), as a **non-overlapping** timeline.

`mk_summary` is last-faction-wins — one faction per MK per term, so an MK who
crossed the floor has their whole term attributed to wherever they ended up.
That makes bloc attribution unanswerable, and it silently scored an MK's
pre-switch votes against a bloc they had not yet joined.

The raw rows cannot be used directly. They **duplicate** (the same faction
appears two or three times per MK with different ranges) and they **overlap**
(a superseded faction and its successor both carry the day of the split). A
consumer joining votes to raw spans counts one MK twice inside a single tally.
Two passes fix it:

- **collapse** to one row per (MK, Knesset, faction). An open row anywhere in
  the group wins — a NULL `FinishDate` means still serving, and `MAX()` would
  otherwise prefer a stale closed duplicate.
- **trim** each span to end where the next begins, making the timeline a
  partition rather than a set of intervals. Zero-length results are same-day
  supersessions and are dropped; the successor already covers that instant.

Guaranteed: at most one faction per MK per instant. Asserted by
`test_faction_spans_are_never_overlapping`.

4,423 rows overall, 172 for K25.

## `mk_summary.current_role` was unfillable by construction

It was NULL on all 3,410 rows. Not an upstream gap: the column mapped
`lpt.DutyDesc`, while the `LatestPerTerm` CTE filtered
`WHERE FactionID IS NOT NULL` — and faction-seat rows and executive rows are
**disjoint** in `KNS_PersonToPosition`:

```
11,090 rows
  FactionID IS NULL     → 6,432 rows, 2,599 with DutyDesc
  FactionID IS NOT NULL → 4,658 rows,     0 with DutyDesc
```

`DutyDesc` was non-null on exactly zero of the rows the exporter kept. It now
comes from its own scan of the executive roster (latest post in the term).
`mk_roles` carries the full set for anyone who needs more than one label.

## Contract

Major version bump — `5.0.0`. Two added snapshots and one column that changes
from always-NULL to sometimes-populated. Consumers that treated
`current_role` as absent keep working; it is rendered where present.

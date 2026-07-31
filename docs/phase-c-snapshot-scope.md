# Phase C — upstream snapshot scope (`knesset_refactor` → v4.0.0)

**Date:** 2026-07-30
**Consumer:** [`knesset-platform`](https://github.com/AT020993/knesset-platform), Phase C of `docs/superpowers/specs/2026-07-29-stakeholder-feedback-round-2-design.md`
**Status:** scoped, not started.

---

## Why this repo blocks six stakeholder comments

The platform reads Parquet snapshots from this repo and never queries the warehouse. Six comments from the stakeholder review are blocked here, not there:

| Comment | Needs |
|---|---|
| MK-7, סיעות-2 | bill status |
| קואליציה-2 (full) | bill status, for pass-rate by bloc |
| MK-3 | MK committee membership |
| ועדות-2 | committee legislation |
| ועדות-3 | committee activity depth |
| MK-2 residual (platform issue #34) | question/motion status decode |

Every one of these exists in `data/warehouse.duckdb` today. **None is a data-acquisition problem; all are export gaps.** The platform's `CLAUDE.md` was corrected in July to say so, after the stale claim that titles and status "don't exist" had steered design away from buildable features for months.

## What is verified present

Measured against the live warehouse, not inferred:

| Source | Rows | Note |
|---|---|---|
| `KNS_Bill.Name` | 59,015 / 59,015 | 100% populated, real Hebrew titles, every Knesset |
| `KNS_Bill.StatusID` | 59,015 / 59,015 | 100% populated |
| `KNS_Status` (`הצעת חוק`) | 35 statuses | `OrderTransition` is **entirely NULL** — ladder must be hand-authored |
| `KNS_Status` (`שאילתה`) | 4 statuses | incl. `נענתה`, `השר סירב לענות` |
| `KNS_Status` (`הצעה לסדר היום`) | 11 statuses | incl. `לדיון בוועדה`, `נעצרה` |
| `WebMkCommittee` | 1,595 | 134 MKs, **Knesset 25 only** |
| `KNS_CmtSessionItem` (`ItemTypeID=2`) | 40,284 | → 10,685 bills across 729 committees; all 40,282 join to `KNS_Bill` |

## The status ladder is validated

The platform spec's seven rungs were checked against every status that actually occurs:

**29 distinct statuses in use across 59,015 bills. The ladder covers all of them. Zero fall-through.**

| Rung | `StatusID` |
|---|---|
| הונחה — טרם נדונה | 104, 150 |
| עברה קריאה טרומית | 101, 106, 108, 109, 111, 141, 142, 167 |
| עברה קריאה ראשונה | 113, 114, 115, 117, 130, 131, 178, 179 |
| **התקבלה — הפכה לחוק** | 118 |
| נעצרה / הוסרה | 110, 140, 143, 176, 177 |
| מוזגה / פוצלה / הוסבה | 122, 124, 126, 158, 161, 162, 165, 169 |
| דין רציפות | 120, 175, 181 |

The ladder is **ordered**; "the last reading a bill passed" means the highest rung reached. It must live here, not in the platform, so the snapshot carries a resolved label and rung rather than a raw ID the consumer has to decode.

**Guard this with a test.** A future Knesset introducing status 182 would silently fall out of every rung. Assert that every `StatusID` present in `KNS_Bill` maps to a rung, and fail the export if not.

---

## Proposed snapshots

Four new, two extended. The exporter's `SNAPSHOTS` tuple in `src/data/snapshots/exporter.py` takes `(name, sql)` pairs, so each addition is one SQL constant plus one registry entry.

### 1. `bills_list` (new) — titles + status

Keyed on `bill_id`. Columns: `bill_id`, `knesset_num`, `name`, `sub_type`, `status_id`, `status_desc`, `status_rung`, `status_rung_order`.

**Why a separate snapshot rather than extending `mk_bills`:** titles average 70 characters. `mk_bills` has 165,461 rows against 59,015 distinct bills, so denormalising would repeat each title **2.8×** — roughly 11.6 MB against 4.1 MB. The platform joins on `bill_id`, which it already carries.

This one snapshot unblocks MK-7, סיעות-2, קואליציה-2, and makes ועדות-2's bill list readable rather than a column of IDs.

### 2. `mk_questions` / `mk_motions` (extend) — decoded status

Add `status_desc` by joining `KNS_Status` on the matching `TypeDesc`. Denormalising is right here, unlike titles: the values are short, there are only 15 of them, and it spares the platform a join and a second lookup table.

Closes platform issue #34, which currently tracks four removed UI blocks that rendered `סטטוס 304` to the public.

### 3. `mk_committees` (new)

Straight export of `WebMkCommittee`: `mk_id`, `knesset_num`, `committee_name_he`, `role_he`, `from_date`, `to_date`.

**Ships with a stated limitation: Knesset 25 only.** The platform must render an honest empty state for historical MKs, as its committee page already does for `data_gaps`. Do not let this look like "this MK sat on no committees".

### 4. `committee_bills` (new)

13,398 distinct (committee, bill) pairs. Columns: `committee_id`, `knesset_num`, `bill_id`, `session_count`.

`session_count` is the ועדות-3 depth metric — how many sessions a committee spent on each bill. The distribution is a real long tail: 3,201 bills got one session, 2,627 got two, down through 7+. That is exactly the "how much did they actually work on it" signal the stakeholder asked for, and it is not derivable from the item counts the page shows today.

Join path: `KNS_CmtSessionItem` → `KNS_CommitteeSession` (has `CommitteeID`) → filter `ItemTypeID = 2`.

---

## Release mechanics

`pyproject.toml` is at **3.0.0**; tags exist for v1.0.0 and v2.0.0 but **not v3.0.0** — check whether that is an oversight before tagging v4.0.0.

`MANIFEST_VERSION` in `manifest.py` is a separate schema version, currently 1. Adding snapshots does not change the manifest *schema*, so it stays at 1 unless the manifest's own shape changes.

**This is a major bump.** Extending `mk_questions` / `mk_motions` changes existing snapshot shapes, which the platform's `CLAUDE.md` states requires a `knesset_refactor` major version.

Sequence, which the platform plan must not treat as a one-repo change:

```
here: SQL + registry entry + ladder test + regression check
  → bump to 4.0.0, tag, re-export
  → platform: KNESSET_SNAPSHOT_DIR refresh
  → API routes read the new columns
  → regenerate openapi.json + packages/api-types (types-sync CI fails on drift)
  → pnpm build → launchctl kickstart
```

`scripts/check_snapshot_regression.py` and `tests/test_snapshot_exporter.py` already exist — both need extending for the new snapshots rather than bypassing.

---

## Also in Phase C, on the platform side

Recorded here so the upstream work is sized against the full downstream need:

- **Platform issue #31** — קואליציה-1's acceptance asks that matrix cells link to filtered bill lists. That needs a pair-filtered bills endpoint (`/v1/coalition/collaborators/{a}/{b}/bills`), which Phase A's no-API-changes rule forbade. Confirmed as Phase C scope. It becomes far more useful once `bills_list` lands — today such a list would render as bare IDs.
- **Platform issue #34** — reinstating question/motion status as a decoded chart, unblocked by snapshot 2 above.

## Explicitly not in scope

- **Protocol ingestion.** Would unlock literal MK quotes and committee attendance. Neither is in any of the 43 warehouse tables. Separate project.
- **CAP coding gaps.** K21 has 5 of 557 bills coded, K22 has 38 of 1,423, K25 is missing ~1,970. `UserBillCoding` has the rows; the topic columns are blank. This is research work, not engineering, and no export changes it.

---

## Measured figures behind the `committee_bills` `knesset_num` choice

Recorded here rather than frozen in a code comment, since they change on re-export.

`knesset_num` reads from `KNS_Committee`, not `KNS_CommitteeSession`. Verified against the warehouse on 2026-07-30:

| Check | Result |
|---|---|
| Grouping by the **committee's** term | **13,398** rows — matches the distinct (committee, bill) pair count |
| Grouping by the **session's** term | **13,399** rows — one pair splits across two session terms |
| Session rows whose term disagrees with their committee's | 83 (e.g. committee 25 is `KnessetNum` 16, one of its sessions is logged under 15) |
| `CommitteeID`s carrying more than one `KnessetNum` | **0** — the committee's term is stable |

The one-row-per-pair contract is what the choice protects. `committees_list` keys every `CommitteeID` to the same `KNS_Committee.KnessetNum`, so a consumer joining `committee_bills` to it on `(committee_id, knesset_num)` never hits an orphan.

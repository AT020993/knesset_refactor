# Phase C Snapshots — Implementation Plan (`knesset_refactor` v4.0.0)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Export the four snapshot changes that unblock six stakeholder comments on `knesset-platform`, and release them as v4.0.0.

**Architecture:** Each snapshot is a SQL constant plus one entry in the `SNAPSHOTS` tuple in `src/data/snapshots/exporter.py`. The bill-status ladder is authored here — not in the consumer — so the snapshot carries a resolved label and rung rather than a raw ID. Tests extend the existing `tiny_warehouse` fixture; the harness derives its expectations from `SNAPSHOTS`, so a new snapshot is automatically covered.

**Tech Stack:** Python 3.12, uv, DuckDB, Parquet, pytest.

## Global Constraints

- Scope: `docs/phase-c-snapshot-scope.md` in this repo. Read it first — it carries the verified row counts and the validated ladder.
- **Additive where possible.** `bills_list`, `mk_committees`, `committee_bills` are new. Only `mk_questions` and `mk_motions` change shape, and only by adding a column.
- **The ladder lives here, never in the consumer.** The platform must receive `status_rung` and `status_desc`, not a raw `status_id` to decode.
- Every SQL constant goes in `src/data/snapshots/exporter.py` beside its siblings, following their style: named `_SNAKE_CASE_SQL`, `.strip()`-ed, with a comment explaining any non-obvious filter or join.
- The `tiny_warehouse` fixture in `tests/test_snapshot_exporter.py` already creates `KNS_Bill`, `KNS_CmtSessionItem`, `KNS_CommitteeSession`, and `WebMkCommittee`. **Only `KNS_Status` must be added.**
- Run tests with `uv run pytest tests -q --ignore=tests/test_e2e.py` from the repo root. **The `--ignore` is required and pre-existing:** `tests/test_e2e.py` is a Playwright suite for the Streamlit app, `playwright` is not in the default dev env, and it fails at *import* time — so the `e2e` marker declared in `pyproject.toml` cannot skip it and a bare `pytest tests` aborts collection on `main` too. It is unrelated to the exporter.
- Do not regenerate `data/snapshots/*.parquet` until Task 6 — the platform reads that directory live via `KNESSET_SNAPSHOT_DIR`, and a half-finished export would break the running site.
- Commit per task.

---

## Task 1: The bill-status ladder

The riskiest piece, so it goes first and stands alone. `KNS_Status.OrderTransition` is entirely NULL, so the ordering is hand-authored.

**Files:**
- Create: `src/data/snapshots/bill_status.py`
- Test: `tests/test_bill_status.py`

**Interfaces:**
- Produces: `BILL_STATUS_RUNGS: dict[str, tuple[int, ...]]`, `RUNG_ORDER: tuple[str, ...]`, `rung_for(status_id: int) -> str | None`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_bill_status.py
from __future__ import annotations

import pytest

from data.snapshots.bill_status import (
    BILL_STATUS_RUNGS,
    RUNG_ORDER,
    rung_for,
)


def test_every_rung_in_order_is_defined():
    assert set(RUNG_ORDER) == set(BILL_STATUS_RUNGS)


def test_no_status_id_appears_in_two_rungs():
    seen: dict[int, str] = {}
    for rung, ids in BILL_STATUS_RUNGS.items():
        for sid in ids:
            assert sid not in seen, f"{sid} in both {seen.get(sid)} and {rung}"
            seen[sid] = rung


def test_rung_for_resolves_known_statuses():
    assert rung_for(118) == "התקבלה — הפכה לחוק"
    assert rung_for(104) == "הונחה — טרם נדונה"
    assert rung_for(113) == "עברה קריאה ראשונה"


def test_rung_for_returns_none_for_unknown():
    assert rung_for(999) is None


def test_law_rung_is_the_highest_reading_milestone():
    """Ordering is the point: 'the last reading passed' means the highest
    rung reached, so a bill that became law must outrank one still in
    first-reading prep."""
    assert RUNG_ORDER.index("התקבלה — הפכה לחוק") > RUNG_ORDER.index(
        "עברה קריאה ראשונה"
    )
    assert RUNG_ORDER.index("עברה קריאה ראשונה") > RUNG_ORDER.index(
        "עברה קריאה טרומית"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_bill_status.py -q`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement**

```python
# src/data/snapshots/bill_status.py
"""Reading-stage ladder for private-member bills.

``KNS_Status`` carries 35 statuses of TypeDesc 'הצעת חוק' but its
``OrderTransition`` column is entirely NULL, so the progression a bill
makes through readings cannot be derived — it is authored here.

This lives upstream, not in the consumer, so snapshots carry a resolved
label and rung rather than a raw id every consumer would have to decode
identically. Validated against the live warehouse: 29 distinct statuses
occur across 59,015 bills and every one maps to a rung.
"""

from __future__ import annotations

LAID = "הונחה — טרם נדונה"
PRELIMINARY = "עברה קריאה טרומית"
FIRST = "עברה קריאה ראשונה"
LAW = "התקבלה — הפכה לחוק"
STOPPED = "נעצרה / הוסרה"
MERGED = "מוזגה / פוצלה / הוסבה"
CONTINUITY = "דין רציפות"

#: Ordered lowest → highest. "The last reading a bill passed" means the
#: highest rung it reached, so order is semantic, not cosmetic. The three
#: terminal-but-not-progress rungs sit after the reading ladder.
RUNG_ORDER: tuple[str, ...] = (
    LAID,
    PRELIMINARY,
    FIRST,
    LAW,
    STOPPED,
    MERGED,
    CONTINUITY,
)

BILL_STATUS_RUNGS: dict[str, tuple[int, ...]] = {
    LAID: (104, 150),
    PRELIMINARY: (101, 106, 108, 109, 111, 141, 142, 167),
    FIRST: (113, 114, 115, 117, 130, 131, 178, 179),
    LAW: (118,),
    STOPPED: (110, 140, 143, 176, 177),
    MERGED: (122, 124, 126, 158, 161, 162, 165, 169),
    CONTINUITY: (120, 175, 181),
}

_BY_STATUS: dict[int, str] = {
    sid: rung for rung, ids in BILL_STATUS_RUNGS.items() for sid in ids
}


def rung_for(status_id: int) -> str | None:
    """Rung for a bill status id, or None if unmapped.

    None is a signal, not a default — Task 2's export test fails the build
    on any unmapped status so a new Knesset introducing one cannot silently
    fall out of every rung.
    """
    return _BY_STATUS.get(status_id)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_bill_status.py -q`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add src/data/snapshots/bill_status.py tests/test_bill_status.py
git commit -m "feat(snapshots): add the hand-authored bill reading-stage ladder

KNS_Status.OrderTransition is entirely NULL, so the progression cannot be
derived. Lives upstream so snapshots carry a resolved rung rather than a
raw id every consumer would decode identically."
```

---

## Task 2: `bills_list` snapshot

**Files:**
- Modify: `src/data/snapshots/exporter.py`
- Test: `tests/test_snapshot_exporter.py`

**Interfaces:**
- Consumes: `rung_for`, `BILL_STATUS_RUNGS` (Task 1)
- Produces: snapshot `bills_list` with `bill_id`, `knesset_num`, `name`, `sub_type`, `status_id`, `status_desc`, `status_rung`, `status_rung_order`

- [ ] **Step 1: Add `KNS_Status` to the `tiny_warehouse` fixture**

It is the only source table the four new snapshots need that the fixture lacks. Add it beside the other `CREATE TABLE` blocks, with rows covering at least one bill status, one question status, and one motion status — Task 4 needs the latter two:

```sql
CREATE TABLE KNS_Status (
    StatusID BIGINT, "Desc" VARCHAR, TypeID BIGINT, TypeDesc VARCHAR,
    OrderTransition BIGINT, IsActive BOOLEAN, LastUpdatedDate VARCHAR
);
INSERT INTO KNS_Status VALUES
    (118, 'התקבלה בקריאה שלישית', 2, 'הצעת חוק', NULL, TRUE, '2026-01-01'),
    (104, 'הונחה על שולחן הכנסת לדיון מוקדם', 2, 'הצעת חוק', NULL, TRUE, '2026-01-01'),
    (9,   'נענתה', 1, 'שאילתה', NULL, TRUE, '2026-01-01'),
    (304, 'לדיון בוועדה', 4, 'הצעה לסדר היום', NULL, TRUE, '2026-01-01');
```

Check the existing `KNS_Bill` fixture rows and make sure at least one carries `StatusID = 118` and one `104`, so the export test has both a law and a tabled bill to assert on. Adjust those rows if needed.

- [ ] **Step 2: Write the failing test**

```python
# tests/test_snapshot_exporter.py  (append, following the file's existing style)
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_snapshot_exporter.py -q -k bills_list`
Expected: FAIL — no `bills_list.parquet`.

- [ ] **Step 4: Implement the SQL and register it**

Build the rung mapping into the SQL as a `CASE` generated from `BILL_STATUS_RUNGS`, so the ladder has exactly one definition. Add `_BILLS_LIST_SQL` beside the other constants and an entry to `SNAPSHOTS`. Scope to `SubTypeDesc = 'פרטית'` to match `mk_bills`, which is private-member-only by construction — a `bills_list` carrying government bills would let a join silently reintroduce them.

- [ ] **Step 5: Run the tests**

Run: `uv run pytest tests -q --ignore=tests/test_e2e.py`
Expected: PASS, including the pre-existing suite.

- [ ] **Step 6: Guard against an unmapped status**

Add a test that fails the build if any status present in `KNS_Bill` has no rung. This is the whole reason the ladder is risky: it is hand-authored, so a future Knesset introducing status 182 must break loudly rather than fall out of every rung.

```python
def test_no_bill_status_falls_outside_the_ladder(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    unmapped = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out}/bills_list.parquet') "
        f"WHERE status_rung IS NULL"
    ).fetchone()[0]
    assert unmapped == 0
```

Then run the same check against the **real** warehouse and report the result — the fixture has four statuses; production has 29.

```bash
uv run python -c "
import duckdb
from data.snapshots.bill_status import BILL_STATUS_RUNGS
mapped = {s for ids in BILL_STATUS_RUNGS.values() for s in ids}
c = duckdb.connect('data/warehouse.duckdb', read_only=True)
rows = c.sql('select distinct \"StatusID\" from KNS_Bill').fetchall()
missing = [r[0] for r in rows if r[0] not in mapped]
print('unmapped in production:', missing or 'none')
"
```

- [ ] **Step 7: Commit**

```bash
git add src/data/snapshots/exporter.py tests/test_snapshot_exporter.py
git commit -m "feat(snapshots): add bills_list with titles, status and rung

Keyed on bill_id rather than denormalised into mk_bills: titles average
70 chars and mk_bills has 165k rows against 59k distinct bills, so
denormalising would repeat each title 2.8x (11.6 MB vs 4.1 MB)."
```

---

## Task 3: `mk_committees` snapshot

**Files:**
- Modify: `src/data/snapshots/exporter.py`
- Test: `tests/test_snapshot_exporter.py`

**Interfaces:**
- Produces: snapshot `mk_committees` with `mk_id`, `knesset_num`, `committee_name_he`, `role_he`, `from_date`, `to_date`

- [ ] **Step 1: Write the failing test**

```python
def test_mk_committees_exports_membership(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT mk_id, knesset_num, committee_name_he, role_he "
        f"FROM read_parquet('{out}/mk_committees.parquet')"
    ).fetchall()
    assert rows
    for mk_id, kn, name, role in rows:
        assert mk_id is not None and kn is not None
        assert name, "a membership with no committee name is not useful"
```

- [ ] **Step 2: Run it, then implement**

Run: `uv run pytest tests/test_snapshot_exporter.py -q -k mk_committees` → FAIL.

`WebMkCommittee` is already in the fixture. The export is close to a straight projection; add `_MK_COMMITTEES_SQL` and register it.

**Document the limitation in the SQL comment:** this source covers **Knesset 25 only** (1,595 rows, 134 MKs). The consumer must render an honest empty state for historical MKs rather than implying they sat on no committees. State it here so the constraint travels with the data.

- [ ] **Step 3: Run the tests, then commit**

Run: `uv run pytest tests -q --ignore=tests/test_e2e.py`

```bash
git add src/data/snapshots/exporter.py tests/test_snapshot_exporter.py
git commit -m "feat(snapshots): add mk_committees (Knesset 25 only)

Source covers K25 alone; the limitation is documented in the query so it
travels with the data rather than surfacing as an apparent absence of
committee service for historical MKs."
```

---

## Task 4: Decode question and motion status

**Files:**
- Modify: `src/data/snapshots/exporter.py`
- Test: `tests/test_snapshot_exporter.py`

**Interfaces:**
- Produces: `mk_questions` and `mk_motions` each gain `status_desc`

- [ ] **Step 1: Write the failing test**

```python
def test_question_and_motion_status_decode(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    """These render to the public. An undecoded status_id reaches a reader
    as 'סטטוס 304', which is why four UI blocks had to be removed
    downstream (knesset-platform issue #34)."""
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    for pack in ("mk_questions", "mk_motions"):
        rows = con.execute(
            f"SELECT status_id, status_desc "
            f"FROM read_parquet('{out}/{pack}.parquet') "
            f"WHERE status_id IS NOT NULL"
        ).fetchall()
        assert rows, f"{pack} fixture must have at least one status"
        for status_id, desc in rows:
            assert desc, f"{pack} status {status_id} did not decode"
```

- [ ] **Step 2: Run it, then implement**

Run: `uv run pytest tests/test_snapshot_exporter.py -q -k status_decode` → FAIL.

Join `KNS_Status` in `_MK_QUESTIONS_SQL` and `_MK_MOTIONS_SQL`. **The join must be scoped by `TypeDesc`** — status ids are only unique within a type, so joining on `StatusID` alone would cross-contaminate a question status with a motion or bill status carrying the same number. Use `TypeDesc = 'שאילתה'` and `'הצעה לסדר היום'` respectively, and say so in a comment.

Denormalising is right here, unlike titles in Task 2: there are 15 short values total, and it spares every consumer a join and a second lookup table.

- [ ] **Step 3: Note that the type-scoping is defensive, not load-bearing**

I checked: **zero status ids are currently reused across `TypeDesc`**, so an unscoped join would produce the same result today. Keep the scoping anyway — it is free, and it encodes the actual key (`StatusID` is unique *within* a type, which is a property of the source schema, not a guarantee about today's rows). But do **not** write a test asserting the scoping changes an outcome, because it does not; a test that cannot fail is worse than none. Record the finding in a SQL comment instead.

- [ ] **Step 4: Run the tests, then commit**

Run: `uv run pytest tests -q --ignore=tests/test_e2e.py`

```bash
git add src/data/snapshots/exporter.py tests/test_snapshot_exporter.py
git commit -m "feat(snapshots): decode question and motion status

Joined type-scoped, since status ids are only unique within a TypeDesc.
Unblocks knesset-platform #34, where four UI blocks rendering raw
'סטטוס 304' to the public had to be removed."
```

---

## Task 5: `committee_bills` snapshot

**Files:**
- Modify: `src/data/snapshots/exporter.py`
- Test: `tests/test_snapshot_exporter.py`

**Interfaces:**
- Produces: snapshot `committee_bills` with `committee_id`, `knesset_num`, `bill_id`, `session_count`

- [ ] **Step 1: Write the failing test**

```python
def test_committee_bills_counts_sessions_per_bill(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    """session_count is the ועדות-3 depth metric — how much work a
    committee actually put into each bill, which the item counts the
    consumer shows today cannot express."""
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT committee_id, bill_id, session_count "
        f"FROM read_parquet('{out}/committee_bills.parquet')"
    ).fetchall()
    assert rows
    for cid, bid, n in rows:
        assert cid is not None and bid is not None
        assert n >= 1, "a (committee, bill) pair implies at least one session"


def test_committee_bills_is_one_row_per_committee_bill_pair(
    tiny_warehouse: Path, tmp_path: Path
) -> None:
    out = tmp_path / "snapshots"
    export_all(tiny_warehouse, out)
    con = duckdb.connect()
    total, distinct = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT (committee_id, bill_id)) "
        f"FROM read_parquet('{out}/committee_bills.parquet')"
    ).fetchone()
    assert total == distinct
```

- [ ] **Step 2: Run it, then implement**

Run: `uv run pytest tests/test_snapshot_exporter.py -q -k committee_bills` → FAIL.

Join path, all three tables already in the fixture:
`KNS_CmtSessionItem` → `KNS_CommitteeSession` on `CommitteeSessionID` (carries `CommitteeID`) → filter `ItemTypeID = 2` (`הצעת חוק`) → group by committee and `ItemID`, counting `DISTINCT CommitteeSessionID`.

Verified in production: 40,284 rows → 13,398 distinct (committee, bill) pairs across 729 committees, and all 40,282 session-items join back to `KNS_Bill`.

- [ ] **Step 3: Run the tests, then commit**

Run: `uv run pytest tests -q --ignore=tests/test_e2e.py`

```bash
git add src/data/snapshots/exporter.py tests/test_snapshot_exporter.py
git commit -m "feat(snapshots): add committee_bills with sessions-per-bill depth

session_count answers 'how much did the committee actually work on this',
which item counts alone cannot. Distribution is a long tail: 3,201 bills
got one session, 2,627 got two."
```

---

## Task 6: Release v4.0.0

- [x] **Step 1: Full suite and lint**

> **Corrected during execution.** The original step said `ruff check src tests` and `mypy src` must pass. Neither has ever passed on `main`: `src` carries **1,220 pre-existing ruff errors** and **36 mypy errors in 13 files**, and CI (`.github/workflows/ci.yml`) gates both on a narrow **"wave scope"** allowlist that does not include `src/data/snapshots`. Demanding a repo-wide clean would have blocked a release on unrelated legacy debt. The honest gate is the full suite plus lint on the code this phase touched, measured against `main` as the baseline.

```bash
uv run pytest tests -q --ignore=tests/test_e2e.py          # 771 pass, exit 0
uv run ruff check src/data/snapshots tests/test_snapshot_exporter.py tests/test_bill_status.py
uv run mypy src                                            # compare the count to main
```

Result: **771 tests pass**; ruff clean on the snapshot module; mypy **36 errors in 13 files on both `main` and the branch, none in `src/data/snapshots`** — Phase C added one source file and zero type errors.

Note the exit-code trap: `pyproject.toml` sets `addopts = "-q --durations=10"`, which suppresses the "N passed" summary, and piping pytest through `tail` makes `$?` the *tail's* status. Redirect to a file and read `$?` directly.

- [x] **Step 2: Resolve the missing v3.0.0 tag**

`pyproject.toml` says `3.0.0` but tags stop at `v2.0.0`. The consumer's `CLAUDE.md` cites `knesset_refactor@v3.0.0` as its contract, so that reference currently points at nothing. Tag the commit that shipped 3.0.0 retroactively, or note in the release why it was skipped — do not leave a dangling contract reference.

Resolved: it was an oversight. The 3.0.0 bump shipped inside feature commit `294d32c` rather than a dedicated release commit, so the tag step was simply missed. Tagged retroactively (annotated, explaining the gap) at **`0a50eda`** — the last commit that changed exporter behaviour while the version was 3.0.0, i.e. the code that produced the snapshots the platform actually consumed. The two later commits on `main` at 3.0.0 are Phase C planning docs and do not affect the exported artifact.

- [x] **Step 3: Bump and export**

> **Corrected during execution, three ways.** (a) The count was wrong: it is **16 snapshots (13 existing + 3 new)**, not 17 — Task 4 *extended* `mk_questions`/`mk_motions` rather than adding snapshots, so four *changes* produce three new files. (b) The exporter flag is `--output-dir`, not `--output`. (c) **Do not export in place.** `data/snapshots` is read live by the running platform; if the Task 2 ladder guard trips on the full warehouse, an in-place run leaves a half-written live directory. Export to a candidate dir, verify, then swap — which Step 4 gives for free.

Set `version = "4.0.0"` in `pyproject.toml` and commit. Back up the live directory, then let Step 4's `compare` produce the candidate export.

Confirm the manifest lists **16** snapshots, that `mk_questions` and `mk_motions` carry `status_desc`, and check each new file against these verified figures: **`bills_list` 51,704** (the private-member subset of 59,015 total), `mk_committees` 1,595, `committee_bills` 13,398.

Result: all three row counts matched exactly; manifest went 13 → 16 with `generated_at_utc` the only other changed field; `status_desc` decoded for 100% of rows in both extended snapshots; `status_rung` null for 0 of 51,704 bills.

- [x] **Step 4: Run the regression check**

> **Corrected during execution.** The original step ran the script bare and claimed "it compares against the previous export". It does neither: it requires an `action` argument, and **both** `baseline` and `compare` run the exporter with *current* code. Running `baseline` then `compare` now would export the same code twice and report BYTE-IDENTICAL — proving nothing. The baseline must come from **v3.0.0** code.

The live `data/snapshots/` *is* a v3.0.0 artifact — launchd exported it at 12:30 from `main`. Verify the warehouse is older than that export (else the comparison confounds data drift with code change), then seed the baseline from it. Seed from the **manifest's snapshot keys**, not a `*.parquet` glob: `bill_classifications.parquet` lives in that directory and is not exporter output.

```bash
stat -f "%Sm %N" -t "%Y-%m-%d %H:%M" data/warehouse.duckdb data/snapshots/manifest.json
# warehouse older → seed /tmp/knesset_snapshot_baseline from the live dir, then:
uv run python scripts/check_snapshot_regression.py compare
```

**Expect exit 2, and read the table, not the exit code.** The script is a *dependency-upgrade* tool, so a deliberate shape change reads as red (the two extended snapshots trip `ROWS DIFFER`, and a changed `manifest.json` sets `any_row_diff` at line 178). The pass condition is: **3 × NEW FILE, 2 × ROWS DIFFER at identical row counts, 11 × BYTE-IDENTICAL** — which is exactly what it returned.

- [x] **Step 5: Verify the consumer still runs**

> **Corrected during execution, twice.** (a) The hand-rolled sequence never restarts the **API** — only the site — so a broken snapshot read would have stayed cached behind a live FastAPI process. The project's own standing rule names `scripts/redeploy.sh`; `--skip-export` runs exactly the right steps (restart API → clear ISR cache → rebuild → restart site → verify) and skips only the re-export, which we did by hand above. (b) The five-page check does not exercise the only shape change. `mk_questions`/`mk_motions` gaining `status_desc` is the sole break vector, and issue #34 records that the UI blocks rendering them were *removed* — so all five pages can return 200 while nothing touches the changed data. Hit the API endpoints directly.

```bash
cd ~/Projects/knesset-platform && ./scripts/redeploy.sh --skip-export
for u in /v1/mks/4395/questions /v1/mks/4395/motions \
         /v1/parties/1096/questions /v1/parties/1096/motions \
         /v1/meta/weekly-activity /v1/meta/freshness /v1/coalition /v1/topics/1; do
  printf "  %-38s %s\n" "$u" "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:8080$u")"
done
```

Result: all 8 endpoints 200, all 11 site pages 200, `/v1/meta/freshness` advertising 16 snapshots. A static read backs this up — every query against the two changed snapshots uses an **explicit column list**, and the only two `SELECT *` are inner subqueries over `mk_summary` wrapped in explicit outer projections, so an added column cannot reach a Pydantic model.

- [x] **Step 6: Tag and push**

> **Corrected during execution.** `git push origin main --tags` violates the standing rule that code ships via PR — this is a 10-commit branch. Also, `git add data/snapshots` is a no-op: `.gitignore` excludes `/data/*`, so the snapshots are **not tracked**; the release commit is the version bump alone, and the export is a filesystem artifact the consumer reads directly.

```bash
git commit pyproject.toml -m "release: v4.0.0 — Phase C snapshots"
git push -u origin feat/phase-c-snapshots
gh pr create --base main   # watch CI, merge on green, then tag v4.0.0 on the merge commit
```

- [x] **Step 7: Report what the consumer can now build**

Comment on `knesset-platform` issues #31 and #34 that the upstream export has landed, naming the new snapshot columns each needs.

Done — with the status **distributions**, not just the column names, because both comments needed a design warning the column list alone would not have surfaced:

- **#34**: only **3** of the 4 `KNS_Status` question statuses actually occur, and 97.0% are `נענתה`. **`השר סירב לענות` — the case the issue was built around — appears in zero of 42,757 rows.** A questions chart is therefore one bar plus two slivers, and any "answered vs refused" framing is unsupportable. Motions are the opposite: 11 statuses, top at 46%, genuinely worth charting, with a five-category sub-1% tail to group.
- **#31**: `bills_list.name` is 100% populated (median 69 chars, K1–K25), so cell-level drill-down would render real titles rather than bare IDs — but `bills_list` is private-member only, so the join is safe only within that subset.

## Release outcome

Merged as `8fab3b8` (squash, all 5 CI checks green); **v4.0.0** tagged and pushed; `v3.0.0` tagged retroactively at `0a50eda`; `uv.lock` synced in `77d2ad1`. Consumer verified live on the new snapshots.

**One gap left open, deliberately:** the unmapped-status guard runs against the 4-status test fixture, not the 29-status warehouse, so a future status would emit NULL `status_rung` silently. Task 2 Step 6 of this plan is what weakened the scope doc's "fail the export if not" to a fixture test. Filed as **#53** rather than widened mid-release.

---

## Self-Review

**Scope coverage.** The scope doc's four items map to Tasks 2 (`bills_list`), 3 (`mk_committees`), 4 (question/motion decode), 5 (`committee_bills`), with Task 1 extracting the ladder they depend on and Task 6 releasing. Every stakeholder comment the scope names is served.

**Risk concentration.** The ladder is the only hand-authored artefact and the only thing that can silently go wrong — hence its own task, its own module, its own test file, plus an export-time guard in Task 2 Step 6 and a production check run against all 29 live statuses.

**Ordering rationale.** Task 1 before Task 2 because the ladder is `bills_list`'s hardest input. Task 6 last because the export writes to a directory the running platform reads — Global Constraints forbid regenerating earlier.

**Known risk.** Task 4 changes two existing snapshot shapes. That is what makes this a major bump, and it is the only place a consumer could break. The regression check in Task 6 Step 4 exists to catch a lost column or row-count change there specifically.

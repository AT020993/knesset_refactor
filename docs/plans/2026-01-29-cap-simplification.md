# CAP Annotation Simplification Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove the Direction field from the CAP annotation system and add Bill ID search for editing annotations.

**Architecture:** Two independent changes: (1) Remove Direction column from database and all UI/service layers, (2) Add text input filter to the edit annotation section. Both changes are backwards-compatible once complete.

**Tech Stack:** Python, Streamlit, DuckDB, pytest

---

## Task 1: Add Database Migration for Direction Removal

**Files:**
- Modify: `src/ui/services/cap/taxonomy.py:176-358`
- Test: `tests/test_cap_services.py`

**Step 1: Write the failing test**

Add to `tests/test_cap_services.py`:

```python
def test_direction_column_removed_from_schema(test_db_path):
    """Verify Direction column no longer exists in UserBillCAP table."""
    from ui.services.cap.taxonomy import CAPTaxonomyService

    service = CAPTaxonomyService(test_db_path)
    service.ensure_tables_exist()

    with get_db_connection(test_db_path, read_only=True) as conn:
        columns = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'UserBillCAP'"
        ).fetchdf()
        column_names = columns["column_name"].str.lower().tolist()

    assert "direction" not in column_names, "Direction column should be removed"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cap_services.py::test_direction_column_removed_from_schema -v`
Expected: FAIL - Direction column still exists

**Step 3: Add migration function to taxonomy.py**

Add new method to `CAPTaxonomyService` class after `_migrate_to_multi_annotator`:

```python
def _remove_direction_column(self, conn) -> None:
    """
    Remove the Direction column from UserBillCAP table.

    This migration removes the Direction field that is no longer used
    by researchers. Uses safe table swap pattern.

    Args:
        conn: Active DuckDB connection
    """
    # Check if Direction column exists
    columns = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'UserBillCAP'"
    ).fetchdf()

    column_names = columns["column_name"].str.lower().tolist()

    if "direction" not in column_names:
        self.logger.info("Direction column already removed - skipping migration")
        return

    self.logger.info("Removing Direction column from UserBillCAP...")

    # Get current count for verification
    existing_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM UserBillCAP"
    ).fetchone()[0]

    # Drop any leftover temp table
    conn.execute("DROP TABLE IF EXISTS UserBillCAP_new")

    # Create new table without Direction column
    conn.execute("""
        CREATE TABLE UserBillCAP_new (
            AnnotationID INTEGER PRIMARY KEY DEFAULT nextval('seq_annotation_id'),
            BillID INTEGER NOT NULL,
            ResearcherID INTEGER NOT NULL,
            CAPMinorCode INTEGER NOT NULL,
            AssignedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            Confidence VARCHAR DEFAULT 'Medium',
            Notes VARCHAR,
            Source VARCHAR DEFAULT 'Database',
            SubmissionDate VARCHAR,
            FOREIGN KEY (CAPMinorCode) REFERENCES UserCAPTaxonomy(MinorCode),
            FOREIGN KEY (ResearcherID) REFERENCES UserResearchers(ResearcherID),
            UNIQUE(BillID, ResearcherID)
        )
    """)

    # Copy data (excluding Direction)
    conn.execute("""
        INSERT INTO UserBillCAP_new
        (AnnotationID, BillID, ResearcherID, CAPMinorCode, AssignedDate,
         Confidence, Notes, Source, SubmissionDate)
        SELECT AnnotationID, BillID, ResearcherID, CAPMinorCode, AssignedDate,
               Confidence, Notes, Source, SubmissionDate
        FROM UserBillCAP
    """)

    # Verify count
    new_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM UserBillCAP_new"
    ).fetchone()[0]

    if new_count != existing_count:
        conn.execute("DROP TABLE UserBillCAP_new")
        raise RuntimeError(
            f"Migration verification failed: expected {existing_count}, got {new_count}"
        )

    # Swap tables
    conn.execute("DROP TABLE UserBillCAP")
    conn.execute("ALTER TABLE UserBillCAP_new RENAME TO UserBillCAP")

    self.logger.info(f"Successfully removed Direction column ({new_count} annotations preserved)")
```

**Step 4: Call migration in ensure_tables_exist**

In `ensure_tables_exist()`, add after line 98 (`self._migrate_to_multi_annotator(conn)`):

```python
# Remove Direction column if present (v2 simplification)
self._remove_direction_column(conn)
```

**Step 5: Update table creation in _migrate_to_multi_annotator**

Update the CREATE TABLE statements in `_migrate_to_multi_annotator` (lines 203-218, 258-274, 301-317) to NOT include Direction column. Remove this line from all three:
```sql
Direction INTEGER NOT NULL DEFAULT 0,
```

**Step 6: Run test to verify it passes**

Run: `pytest tests/test_cap_services.py::test_direction_column_removed_from_schema -v`
Expected: PASS

**Step 7: Commit**

```bash
git add src/ui/services/cap/taxonomy.py tests/test_cap_services.py
git commit -m "feat(cap): add migration to remove Direction column

Database migration removes unused Direction field from UserBillCAP.
Uses safe table swap pattern with count verification.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 2: Remove Direction from Repository Layer

**Files:**
- Modify: `src/ui/services/cap/repository.py:522-661`

**Step 1: Update save_annotation method signature**

In `repository.py`, change `save_annotation` method (line 522):

From:
```python
def save_annotation(
    self,
    bill_id: int,
    cap_minor_code: int,
    direction: int,
    researcher_id: int,
    ...
```

To:
```python
def save_annotation(
    self,
    bill_id: int,
    cap_minor_code: int,
    researcher_id: int,
    ...
```

**Step 2: Remove Direction from UPDATE query**

In `save_annotation`, update the UPDATE query (lines 604-626):

From:
```python
conn.execute(
    """
    UPDATE UserBillCAP SET
        CAPMinorCode = ?,
        Direction = ?,
        AssignedDate = CURRENT_TIMESTAMP,
        ...
    WHERE BillID = ? AND ResearcherID = ?
""",
    [
        cap_minor_code,
        direction,
        confidence,
        ...
    ],
)
```

To:
```python
conn.execute(
    """
    UPDATE UserBillCAP SET
        CAPMinorCode = ?,
        AssignedDate = CURRENT_TIMESTAMP,
        Confidence = ?,
        Notes = ?,
        Source = ?,
        SubmissionDate = ?
    WHERE BillID = ? AND ResearcherID = ?
""",
    [
        cap_minor_code,
        confidence,
        notes,
        source,
        submission_date,
        bill_id,
        researcher_id,
    ],
)
```

**Step 3: Remove Direction from INSERT query**

In `save_annotation`, update the INSERT query (lines 632-648):

From:
```python
conn.execute(
    """
    INSERT INTO UserBillCAP
    (BillID, ResearcherID, CAPMinorCode, Direction, Confidence, Notes, Source, SubmissionDate)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""",
    [
        bill_id,
        researcher_id,
        cap_minor_code,
        direction,
        confidence,
        ...
    ],
)
```

To:
```python
conn.execute(
    """
    INSERT INTO UserBillCAP
    (BillID, ResearcherID, CAPMinorCode, Confidence, Notes, Source, SubmissionDate)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""",
    [
        bill_id,
        researcher_id,
        cap_minor_code,
        confidence,
        notes,
        source,
        submission_date,
    ],
)
```

**Step 4: Remove Direction from SELECT queries**

In `get_coded_bills` (line 215), remove:
```python
CAP.Direction,
```

In `get_recent_annotations` (line 290), remove:
```python
CAP.Direction,
```

In `get_bills_with_status` (line 356), remove:
```python
my_cap.Direction,
```

In `get_annotation_by_bill_id` (line 438), remove:
```python
CAP.Direction,
```

In `get_all_annotations_for_bill` (line 499), remove:
```python
CAP.Direction,
```

**Step 5: Run tests**

Run: `pytest tests/test_cap_services.py -v -k "save_annotation or get_coded"`
Expected: PASS (tests may need updating if they check Direction)

**Step 6: Commit**

```bash
git add src/ui/services/cap/repository.py
git commit -m "refactor(cap): remove Direction from repository layer

Remove Direction parameter from save_annotation and all SELECT queries.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 3: Remove Direction from Service Facade

**Files:**
- Modify: `src/ui/services/cap_service.py:145-172`

**Step 1: Update save_annotation signature**

In `cap_service.py`, update `save_annotation` method (line 145):

From:
```python
def save_annotation(
    self,
    bill_id: int,
    cap_minor_code: int,
    direction: int,
    researcher_id: int,
    ...
```

To:
```python
def save_annotation(
    self,
    bill_id: int,
    cap_minor_code: int,
    researcher_id: int,
    confidence: str = "Medium",
    notes: str = "",
    source: str = "Database",
    submission_date: str = "",
) -> bool:
```

**Step 2: Update delegation call**

Update the return statement (lines 163-172):

From:
```python
return self._repository.save_annotation(
    bill_id=bill_id,
    cap_minor_code=cap_minor_code,
    direction=direction,
    researcher_id=researcher_id,
    ...
)
```

To:
```python
return self._repository.save_annotation(
    bill_id=bill_id,
    cap_minor_code=cap_minor_code,
    researcher_id=researcher_id,
    confidence=confidence,
    notes=notes,
    source=source,
    submission_date=submission_date,
)
```

**Step 3: Remove Direction constants**

Remove lines 44-47:
```python
DIRECTION_STRENGTHENING = CAPTaxonomyService.DIRECTION_STRENGTHENING
DIRECTION_WEAKENING = CAPTaxonomyService.DIRECTION_WEAKENING
DIRECTION_NEUTRAL = CAPTaxonomyService.DIRECTION_NEUTRAL
DIRECTION_LABELS = CAPTaxonomyService.DIRECTION_LABELS
```

**Step 4: Update module docstring**

Update the docstring at top of file (lines 1-18) to remove Direction references:
- Remove line: `- Direction coding (+1=Strengthening, -1=Weakening, 0=Other)`

**Step 5: Run tests**

Run: `pytest tests/test_cap_services.py -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/ui/services/cap_service.py
git commit -m "refactor(cap): remove Direction from service facade

Update save_annotation signature and remove Direction constants.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 4: Remove Direction from New Annotation Form

**Files:**
- Modify: `src/ui/renderers/cap/form_renderer.py:111-198`

**Step 1: Remove _render_direction_selector method**

Delete lines 185-198:
```python
def _render_direction_selector(self, default_index: int = None, key: str = None) -> int:
    """Render direction radio selector."""
    return st.radio(
        ...
    )
```

**Step 2: Remove direction from render_annotation_form**

In `render_annotation_form` (line 111), remove lines 143-151:
```python
# Direction selection
direction = self._render_direction_selector()

# Submission date info
if direction in [1, -1]:
    if submission_date:
        st.info(f"📅 **Submission Date:** {submission_date} (from database)")
    else:
        st.warning("⚠️ Submission date not available in database")
```

Replace with just showing submission date unconditionally:
```python
# Submission date info
if submission_date:
    st.info(f"📅 **Submission Date:** {submission_date}")
```

**Step 3: Update _handle_form_submission call**

In `render_annotation_form`, update the call (lines 167-178):

From:
```python
return self._handle_form_submission(
    bill_id,
    selected_major,
    selected_minor,
    direction,
    researcher_id,
    ...
)
```

To:
```python
return self._handle_form_submission(
    bill_id,
    selected_major,
    selected_minor,
    researcher_id,
    confidence,
    notes,
    "Database",
    submission_date,
)
```

**Step 4: Update _handle_form_submission signature**

Change method signature (line 239):

From:
```python
def _handle_form_submission(
    self,
    bill_id: int,
    selected_major: Optional[int],
    selected_minor: Optional[int],
    direction: int,
    researcher_id: int,
    ...
```

To:
```python
def _handle_form_submission(
    self,
    bill_id: int,
    selected_major: Optional[int],
    selected_minor: Optional[int],
    researcher_id: int,
    confidence: str,
    notes: str,
    source: str,
    submission_date: str,
) -> bool:
```

**Step 5: Update save_annotation call in _handle_form_submission**

Update lines 258-268:

From:
```python
success = self.service.save_annotation(
    bill_id=bill_id,
    cap_minor_code=selected_minor,
    direction=direction,
    researcher_id=researcher_id,
    ...
)
```

To:
```python
success = self.service.save_annotation(
    bill_id=bill_id,
    cap_minor_code=selected_minor,
    researcher_id=researcher_id,
    confidence=confidence,
    notes=notes,
    source=source,
    submission_date=submission_date,
)
```

**Step 6: Remove direction from render_api_annotation_form**

In `render_api_annotation_form` (line 286), remove lines 313-321:
```python
# Direction
direction = self._render_direction_selector(key="api_direction")

# Submission date info
if direction in [1, -1]:
    if submission_date:
        st.info(f"📅 **Submission Date:** {submission_date} (from API)")
    else:
        st.warning("⚠️ Submission date not available from API")
```

Replace with:
```python
# Submission date info
if submission_date:
    st.info(f"📅 **Submission Date:** {submission_date}")
```

**Step 7: Update _handle_form_submission call in render_api_annotation_form**

Update lines 338-349:

From:
```python
success = self._handle_form_submission(
    bill_id,
    selected_major,
    selected_minor,
    direction,
    researcher_id,
    confidence,
    notes,
    "API",
    submission_date,
)
```

To:
```python
success = self._handle_form_submission(
    bill_id,
    selected_major,
    selected_minor,
    researcher_id,
    confidence,
    notes,
    "API",
    submission_date,
)
```

**Step 8: Run app to verify form works**

Run: `streamlit run src/ui/data_refresh.py --server.port 8501`
Navigate to CAP Annotation, verify form no longer shows Direction radio buttons.

**Step 9: Commit**

```bash
git add src/ui/renderers/cap/form_renderer.py
git commit -m "refactor(cap): remove Direction from annotation forms

Remove direction selector from new annotation and API annotation forms.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 5: Remove Direction from Edit Form and Table

**Files:**
- Modify: `src/ui/renderers/cap/coded_bills_renderer.py:198-498`

**Step 1: Remove Direction from _render_bills_table**

In `_render_bills_table` (line 198), update display_cols (line 201):

From:
```python
display_cols = ["BillID", "KnessetNum", "BillName", "CAPTopic_HE", "Direction"]
col_names = ["ID", "Knesset", "Bill Name", "Category", "Direction"]
```

To:
```python
display_cols = ["BillID", "KnessetNum", "BillName", "CAPTopic_HE"]
col_names = ["ID", "Knesset", "Bill Name", "Category"]
```

**Step 2: Remove Direction mapping**

Remove line 219:
```python
display_df["Direction"] = display_df["Direction"].map({1: "+1", -1: "-1", 0: "0"})
```

**Step 3: Remove Direction display from _render_edit_section**

In `_render_edit_section` (line 223), remove lines 261-268:
```python
with col2:
    dir_map = {
        1: "+1 (Strengthening)",
        -1: "-1 (Weakening)",
        0: "0 (Other)",
    }
    st.write(
        f"↔️ Direction: {dir_map.get(selected_bill['Direction'], selected_bill['Direction'])}"
    )
```

Update columns from 3 to 2:
```python
col1, col2 = st.columns(2)
with col1:
    st.write(f"📁 Category: {selected_bill['CAPTopic_HE']}")
with col2:
    st.write(f"📅 Date: {selected_bill['AssignedDate']}")
```

**Step 4: Remove Direction from _render_edit_form**

In `_render_edit_form` (line 297), remove lines 371-390:
```python
# Direction selection
current_direction = current_annotation.get("Direction", 0)
direction_options = [1, -1, 0]
direction_idx = (
    direction_options.index(current_direction)
    if current_direction in direction_options
    else 2
)

direction = st.radio(
    "Direction *",
    options=direction_options,
    index=direction_idx,
    format_func=lambda x: {
        1: "+1 הרחבה/חיזוק (Strengthening)",
        -1: "-1 צמצום/פגיעה (Weakening)",
        0: "0 אחר (Other)",
    }[x],
    horizontal=True,
    key=f"edit_direction_{bill_id}",
)
```

**Step 5: Update save_annotation call in _render_edit_form**

Update lines 429-438:

From:
```python
success = self.service.save_annotation(
    bill_id=bill_id,
    cap_minor_code=selected_minor,
    direction=direction,
    researcher_id=researcher_id,
    ...
)
```

To:
```python
success = self.service.save_annotation(
    bill_id=bill_id,
    cap_minor_code=selected_minor,
    researcher_id=researcher_id,
    confidence=confidence,
    notes=notes,
    source=current_annotation.get("Source", "Database"),
    submission_date=submission_date,
)
```

**Step 6: Run app to verify edit form works**

Run: `streamlit run src/ui/data_refresh.py --server.port 8501`
Navigate to CAP Annotation → Coded Bills, verify table and edit form no longer show Direction.

**Step 7: Commit**

```bash
git add src/ui/renderers/cap/coded_bills_renderer.py
git commit -m "refactor(cap): remove Direction from coded bills view

Remove Direction from table display and edit form.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 6: Add Bill ID Search to Edit Section

**Files:**
- Modify: `src/ui/renderers/cap/coded_bills_renderer.py:223-238`

**Step 1: Add search input before selectbox**

In `_render_edit_section` (line 223), add before the selectbox (around line 231):

```python
def _render_edit_section(self, coded_bills, researcher_id: Optional[int] = None):
    """
    Render bill selection and edit form.

    Args:
        coded_bills: DataFrame of coded bills
        researcher_id: Current researcher's ID for edits
    """
    # Search by Bill ID
    search_bill_id = st.text_input(
        "🔍 Search by Bill ID",
        key="edit_search_bill_id",
        placeholder="Enter Bill ID to filter...",
    )

    # Filter coded_bills if search is provided
    filtered_bills = coded_bills
    if search_bill_id:
        search_bill_id = search_bill_id.strip()
        # Filter where BillID contains the search string
        filtered_bills = coded_bills[
            coded_bills["BillID"].astype(str).str.contains(search_bill_id, na=False)
        ]

        if filtered_bills.empty:
            st.warning(f"No annotations found for Bill ID containing: {search_bill_id}")
            return

    # Bill selection for editing
    edit_idx = st.selectbox(
        "Select annotation to edit",
        options=range(len(filtered_bills)),
        format_func=lambda i: self._format_edit_option(filtered_bills.iloc[i]),
        key="edit_bill_select",
    )

    if edit_idx is not None:
        selected_bill = filtered_bills.iloc[edit_idx]
        # ... rest of the method continues with selected_bill
```

**Step 2: Update the rest of _render_edit_section to use filtered_bills**

Ensure all references after the filter use `filtered_bills` instead of `coded_bills`.

**Step 3: Run app to test search**

Run: `streamlit run src/ui/data_refresh.py --server.port 8501`
Navigate to CAP Annotation → Coded Bills:
- Verify search box appears
- Type a Bill ID, verify dropdown filters
- Clear search, verify all bills show again

**Step 4: Commit**

```bash
git add src/ui/renderers/cap/coded_bills_renderer.py
git commit -m "feat(cap): add Bill ID search to edit annotations section

Researchers can now quickly find specific annotations by Bill ID.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 7: Update Tests and Clean Up

**Files:**
- Modify: `tests/test_cap_services.py`
- Modify: `tests/test_cap_renderers.py` (if exists)

**Step 1: Update any tests that use Direction parameter**

Search for tests passing `direction=` and remove that parameter:

```bash
grep -r "direction=" tests/
```

Update each found test to remove the direction parameter.

**Step 2: Run full test suite**

Run: `pytest tests/test_cap_services.py tests/test_cap_integration.py -v`
Expected: All pass

**Step 3: Update CLAUDE.md documentation**

In `src/ui/services/cap/CLAUDE.md`, update:
- Remove "Direction values: +1/-1/0" from Quick Reference
- Remove Direction from UserBillCAP schema example

**Step 4: Commit**

```bash
git add tests/ src/ui/services/cap/CLAUDE.md
git commit -m "test(cap): update tests for Direction removal

Remove direction parameter from test assertions and update docs.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 8: Final Verification

**Step 1: Run database migration**

Start the app to trigger migration:
```bash
streamlit run src/ui/data_refresh.py --server.port 8501
```

**Step 2: Verify Direction column is removed**

```bash
cd /Users/amir/Library/CloudStorage/OneDrive-Personal/Work/Research\ Assistant/knesset_refactor
python -c "
import duckdb
conn = duckdb.connect('data/warehouse.duckdb', read_only=True)
cols = conn.execute(\"SELECT column_name FROM information_schema.columns WHERE table_name = 'UserBillCAP'\").fetchdf()
print('Columns:', cols['column_name'].tolist())
assert 'Direction' not in cols['column_name'].tolist(), 'Direction should be removed'
print('✅ Direction column successfully removed!')
"
```

**Step 3: Verify existing annotations preserved**

Check annotation count is unchanged from before migration.

**Step 4: Test new annotation without Direction**

1. Go to CAP Annotation
2. Select a bill to annotate
3. Verify no Direction selector appears
4. Save annotation
5. Verify it appears in Coded Bills

**Step 5: Test Bill ID search**

1. Go to Coded Bills section
2. Type a Bill ID in search box
3. Verify dropdown filters correctly
4. Edit the annotation
5. Verify save works

**Step 6: Final commit**

```bash
git add -A
git commit -m "docs: update design plan with implementation complete

All tasks completed:
- Direction field removed from database and UI
- Bill ID search added to edit section

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Database migration | `taxonomy.py` |
| 2 | Repository layer | `repository.py` |
| 3 | Service facade | `cap_service.py` |
| 4 | New annotation form | `form_renderer.py` |
| 5 | Edit form and table | `coded_bills_renderer.py` |
| 6 | Bill ID search | `coded_bills_renderer.py` |
| 7 | Tests and docs | `test_cap_*.py`, `CLAUDE.md` |
| 8 | Final verification | Manual testing |

**Estimated time:** 45-60 minutes

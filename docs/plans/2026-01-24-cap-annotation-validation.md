# CAP Annotation System Validation & Hardening Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ensure the CAP annotation system is production-ready for multiple researchers, with proper data validation, no race conditions, and robust error handling.

**Architecture:** The system uses a 3-tier architecture (Repository → Service Facade → UI Renderers) with DuckDB storage. Multi-annotator support allows multiple researchers to independently annotate the same bills. Session state tracks authentication via `cap_user_id` (int).

**Tech Stack:** Python 3.12, Streamlit, DuckDB, bcrypt, pytest

---

## Task 1: Fix DuckDB Auto-Increment Race Condition in User Service

**Priority:** 🔴 CRITICAL - Concurrent user creation can cause duplicate IDs

**Files:**
- Modify: `src/ui/services/cap/user_service.py:267-328`
- Test: `tests/test_cap_services.py`

**Step 1: Write the failing test for sequence-based user creation**

Add to `tests/test_cap_services.py`:

```python
class TestCAPUserServiceSequence:
    """Tests for user service with proper DuckDB sequences."""

    def test_create_user_uses_sequence(self, user_service, db_path):
        """Verify user creation uses sequence, not MAX()+1."""
        # Create first user
        user_service.create_user(
            username="seq_test_1",
            display_name="Seq Test 1",
            password="password123",
            role="researcher",
        )

        # Check sequence exists
        with get_db_connection(db_path, read_only=True) as conn:
            result = conn.execute(
                "SELECT sequence_name FROM information_schema.sequences "
                "WHERE sequence_name = 'seq_researcher_id'"
            ).fetchone()
            assert result is not None, "Sequence seq_researcher_id should exist"

    def test_create_multiple_users_no_id_collision(self, user_service):
        """Verify multiple users get unique IDs via sequence."""
        ids = []
        for i in range(5):
            success = user_service.create_user(
                username=f"multi_user_{i}",
                display_name=f"Multi User {i}",
                password="password123",
                role="researcher",
            )
            assert success, f"Failed to create user {i}"

        users_df = user_service.get_all_users()
        multi_users = users_df[users_df["Username"].str.startswith("multi_user_")]
        ids = multi_users["ResearcherID"].tolist()

        # All IDs should be unique
        assert len(ids) == len(set(ids)), f"Duplicate IDs found: {ids}"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cap_services.py::TestCAPUserServiceSequence -v`
Expected: FAIL with "Sequence seq_researcher_id should exist"

**Step 3: Create the sequence in ensure_table_exists**

In `src/ui/services/cap/user_service.py`, modify `ensure_table_exists()`:

```python
def ensure_table_exists(self) -> bool:
    """
    Ensure the UserResearchers table exists.

    This is called automatically before any database queries to handle
    the case where the table hasn't been created yet.

    Returns:
        True if table exists or was created, False on error
    """
    if self._table_ensured:
        return True

    try:
        with get_db_connection(
            self.db_path, read_only=False, logger_obj=self.logger
        ) as conn:
            # Create sequence for thread-safe auto-increment
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS seq_researcher_id START 1
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS UserResearchers (
                    ResearcherID INTEGER PRIMARY KEY DEFAULT nextval('seq_researcher_id'),
                    Username VARCHAR NOT NULL UNIQUE,
                    DisplayName VARCHAR NOT NULL,
                    PasswordHash VARCHAR NOT NULL,
                    Role VARCHAR NOT NULL DEFAULT 'researcher',
                    IsActive BOOLEAN NOT NULL DEFAULT TRUE,
                    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    LastLoginAt TIMESTAMP,
                    CreatedBy VARCHAR
                )
            """)
            self._table_ensured = True
            self.logger.debug("UserResearchers table ensured with sequence")
            return True

    except Exception as e:
        self.logger.error(f"Error ensuring UserResearchers table: {e}", exc_info=True)
        return False
```

**Step 4: Update create_user to use sequence**

In `src/ui/services/cap/user_service.py`, modify `create_user()`:

```python
def create_user(
    self,
    username: str,
    display_name: str,
    password: str,
    role: str = ROLE_RESEARCHER,
    created_by: Optional[str] = None,
) -> bool:
    """
    Create a new user account.

    Args:
        username: Unique username for login
        display_name: Display name shown in UI
        password: Plain text password (will be hashed)
        role: 'admin' or 'researcher'
        created_by: Username of admin who created this account

    Returns:
        True if successful, False otherwise
    """
    try:
        # Validate inputs
        if not username or not display_name or not password:
            self.logger.error("Missing required fields for user creation")
            return False

        if role not in [self.ROLE_ADMIN, self.ROLE_RESEARCHER]:
            self.logger.error(f"Invalid role: {role}")
            return False

        if len(password) < 6:
            self.logger.error("Password too short (minimum 6 characters)")
            return False

        password_hash = self.hash_password(password)

        self.ensure_table_exists()
        with get_db_connection(
            self.db_path, read_only=False, logger_obj=self.logger
        ) as conn:
            # Use sequence for thread-safe auto-increment (not MAX()+1)
            conn.execute(
                """
                INSERT INTO UserResearchers
                (Username, DisplayName, PasswordHash, Role, IsActive, CreatedAt, CreatedBy)
                VALUES (?, ?, ?, ?, TRUE, ?, ?)
                """,
                [username, display_name, password_hash, role, datetime.now(), created_by],
            )
            self.logger.info(f"Created user: {username} with role: {role}")
            return True

    except Exception as e:
        if "UNIQUE constraint" in str(e):
            self.logger.error(f"Username already exists: {username}")
        else:
            self.logger.error(f"Error creating user: {e}", exc_info=True)
        return False
```

**Step 5: Run test to verify it passes**

Run: `pytest tests/test_cap_services.py::TestCAPUserServiceSequence -v`
Expected: PASS

**Step 6: Run full CAP test suite**

Run: `pytest tests/test_cap_services.py -v`
Expected: All 35+ tests PASS

**Step 7: Commit**

```bash
git add src/ui/services/cap/user_service.py tests/test_cap_services.py
git commit -m "fix: use DuckDB sequence for user ID generation

Replaces MAX()+1 pattern with proper sequence to prevent race
conditions when multiple admins create users simultaneously.

- Add seq_researcher_id sequence in ensure_table_exists()
- Remove manual ID calculation from create_user()
- Add tests for sequence-based ID generation"
```

---

## Task 2: Add Foreign Key Validation for Annotations

**Priority:** 🔴 CRITICAL - Invalid data could corrupt the database

**Files:**
- Modify: `src/ui/services/cap/repository.py:540-620`
- Test: `tests/test_cap_services.py`

**Step 1: Write failing tests for FK validation**

Add to `tests/test_cap_services.py`:

```python
class TestCAPAnnotationValidation:
    """Tests for annotation validation."""

    def test_save_annotation_rejects_nonexistent_researcher(self, repository, db_path):
        """Verify annotation fails if researcher doesn't exist."""
        # Ensure tables exist
        repository._ensure_table()

        result = repository.save_annotation(
            bill_id=12345,
            cap_minor_code=100,
            direction=0,
            researcher_id=99999,  # Non-existent researcher
            confidence="Medium",
            notes="Test",
            source="Database",
        )

        assert result is False, "Should reject non-existent researcher"

    def test_save_annotation_rejects_invalid_cap_code(self, repository, taxonomy_service):
        """Verify annotation fails if CAP code doesn't exist."""
        # Load taxonomy first
        taxonomy_service.ensure_tables_exist()
        taxonomy_service.load_taxonomy_from_csv()

        result = repository.save_annotation(
            bill_id=12345,
            cap_minor_code=99999,  # Non-existent CAP code
            direction=0,
            researcher_id=1,
            confidence="Medium",
            notes="Test",
            source="Database",
        )

        assert result is False, "Should reject non-existent CAP code"
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_cap_services.py::TestCAPAnnotationValidation -v`
Expected: FAIL - Currently no validation happens

**Step 3: Add validation to save_annotation**

In `src/ui/services/cap/repository.py`, modify `save_annotation()`:

```python
def save_annotation(
    self,
    bill_id: int,
    cap_minor_code: int,
    direction: int,
    researcher_id: int,
    confidence: str = "Medium",
    notes: str = "",
    source: str = "Database",
    submission_date: str = "",
) -> bool:
    """
    Save or update a bill annotation for a specific researcher.

    Args:
        bill_id: The bill's ID
        cap_minor_code: Minor category code from taxonomy
        direction: Democracy direction (+1, -1, 0)
        researcher_id: The researcher's database ID (must be int, not name!)
        confidence: Confidence level (High/Medium/Low)
        notes: Optional notes
        source: Source of bill data ('Database' or 'API')
        submission_date: Bill submission date string

    Returns:
        True if saved successfully, False otherwise
    """
    # Type validation for researcher_id
    if not isinstance(researcher_id, int):
        self.logger.error(
            f"researcher_id must be int, got {type(researcher_id).__name__}: {researcher_id}. "
            "Did you pass cap_researcher_name instead of cap_user_id?"
        )
        return False

    self._ensure_table()

    try:
        with get_db_connection(
            self.db_path, read_only=False, logger_obj=self.logger
        ) as conn:
            # Validate researcher exists
            researcher_exists = conn.execute(
                "SELECT 1 FROM UserResearchers WHERE ResearcherID = ? AND IsActive = TRUE",
                [researcher_id],
            ).fetchone()
            if not researcher_exists:
                self.logger.error(
                    f"Researcher ID {researcher_id} not found or inactive. "
                    "Cannot save annotation."
                )
                return False

            # Validate CAP minor code exists
            cap_exists = conn.execute(
                "SELECT 1 FROM UserCAPTaxonomy WHERE MinorCode = ?",
                [cap_minor_code],
            ).fetchone()
            if not cap_exists:
                self.logger.error(
                    f"CAP Minor Code {cap_minor_code} not found in taxonomy. "
                    "Cannot save annotation."
                )
                return False

            # Check if annotation exists (upsert pattern)
            existing = conn.execute(
                """
                SELECT AnnotationID FROM UserBillCAP
                WHERE BillID = ? AND ResearcherID = ?
                """,
                [bill_id, researcher_id],
            ).fetchone()

            if existing:
                # Update existing annotation
                conn.execute(
                    """
                    UPDATE UserBillCAP
                    SET CAPMinorCode = ?,
                        Direction = ?,
                        Confidence = ?,
                        Notes = ?,
                        Source = ?,
                        SubmissionDate = ?,
                        AssignedDate = CURRENT_TIMESTAMP
                    WHERE BillID = ? AND ResearcherID = ?
                    """,
                    [
                        cap_minor_code,
                        direction,
                        confidence,
                        notes,
                        source,
                        submission_date,
                        bill_id,
                        researcher_id,
                    ],
                )
                self.logger.info(
                    f"Updated annotation for bill {bill_id} by researcher {researcher_id}"
                )
            else:
                # Insert new annotation
                conn.execute(
                    """
                    INSERT INTO UserBillCAP
                    (BillID, ResearcherID, CAPMinorCode, Direction, Confidence,
                     Notes, Source, SubmissionDate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        bill_id,
                        researcher_id,
                        cap_minor_code,
                        direction,
                        confidence,
                        notes,
                        source,
                        submission_date,
                    ],
                )
                self.logger.info(
                    f"Created annotation for bill {bill_id} by researcher {researcher_id}"
                )

            # Clear cache to refresh counts
            clear_annotation_counts_cache()
            return True

    except Exception as e:
        self.logger.error(f"Error saving annotation: {e}", exc_info=True)
        return False
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_cap_services.py::TestCAPAnnotationValidation -v`
Expected: PASS

**Step 5: Run full test suite**

Run: `pytest tests/test_cap_services.py -v`
Expected: All tests PASS

**Step 6: Commit**

```bash
git add src/ui/services/cap/repository.py tests/test_cap_services.py
git commit -m "fix: add foreign key validation before saving annotations

Validates that researcher and CAP code exist before INSERT/UPDATE
to prevent orphaned or invalid annotations.

- Check researcher exists and is active
- Check CAP minor code exists in taxonomy
- Return False with clear error message on validation failure"
```

---

## Task 3: Add Type Hints for Researcher ID Safety

**Priority:** 🟠 HIGH - Prevents ID/Name confusion at development time

**Files:**
- Modify: `src/ui/services/cap/repository.py` (function signatures)
- Modify: `src/ui/services/cap_service.py` (function signatures)
- Modify: `src/ui/renderers/cap/form_renderer.py` (function signatures)

**Step 1: Add type hints to repository.py**

In `src/ui/services/cap/repository.py`, update function signatures:

```python
from typing import Optional, List, Dict, Any

def save_annotation(
    self,
    bill_id: int,
    cap_minor_code: int,
    direction: int,
    researcher_id: int,  # Already has type hint
    confidence: str = "Medium",
    notes: str = "",
    source: str = "Database",
    submission_date: str = "",
) -> bool:
    ...

def get_uncoded_bills(
    self,
    researcher_id: int,
    knesset_num: Optional[int] = None,
    limit: int = 100,
) -> pd.DataFrame:
    ...

def get_coded_bills(
    self,
    researcher_id: int,
    knesset_num: Optional[int] = None,
) -> pd.DataFrame:
    ...

def get_bills_with_status(
    self,
    researcher_id: int,
    knesset_num: Optional[int] = None,
    search_term: Optional[str] = None,
    show_annotated: bool = False,
    limit: int = 100,
) -> pd.DataFrame:
    ...

def delete_annotation(
    self,
    bill_id: int,
    researcher_id: int,
) -> bool:
    ...
```

**Step 2: Add type hints to cap_service.py facade**

In `src/ui/services/cap_service.py`:

```python
def save_annotation(
    self,
    bill_id: int,
    cap_minor_code: int,
    direction: int,
    researcher_id: int,
    **kwargs,
) -> bool:
    """Save annotation. researcher_id must be int (cap_user_id), not string."""
    return self.repository.save_annotation(
        bill_id, cap_minor_code, direction, researcher_id, **kwargs
    )

def get_uncoded_bills(
    self,
    researcher_id: int,
    knesset_num: Optional[int] = None,
    limit: int = 100,
) -> pd.DataFrame:
    """Get bills not yet annotated by this researcher."""
    return self.repository.get_uncoded_bills(researcher_id, knesset_num, limit)
```

**Step 3: Add type hints to form_renderer.py**

In `src/ui/renderers/cap/form_renderer.py`:

```python
def render_annotation_form(
    self,
    bill_id: int,
    researcher_id: int,
    submission_date: str = "",
) -> None:
    """
    Render the annotation form for a bill.

    Args:
        bill_id: The bill's database ID
        researcher_id: The researcher's ID (int from cap_user_id, NOT name string!)
        submission_date: Optional submission date string
    """
    ...

def render_bill_queue(
    self,
    researcher_id: int,
) -> tuple[Optional[int], Optional[str]]:
    """
    Render the bill queue for annotation.

    Args:
        researcher_id: The researcher's ID (int from cap_user_id)

    Returns:
        Tuple of (selected_bill_id, submission_date) or (None, None)
    """
    ...
```

**Step 4: Verify no type errors**

Run: `python -c "from ui.services.cap.repository import CAPAnnotationRepository; print('OK')"`
Expected: "OK" (no import errors)

**Step 5: Commit**

```bash
git add src/ui/services/cap/repository.py src/ui/services/cap_service.py src/ui/renderers/cap/form_renderer.py
git commit -m "docs: add type hints to researcher_id parameters

Adds explicit int type hints to all researcher_id parameters
to catch ID/Name confusion at development time.

- repository.py: All methods with researcher_id
- cap_service.py: Facade methods
- form_renderer.py: UI methods with docstring warnings"
```

---

## Task 4: Fix Session State Reset on Error

**Priority:** 🟡 MEDIUM - Prevents stale UI state

**Files:**
- Modify: `src/ui/renderers/cap/form_renderer.py:308-335`

**Step 1: Identify the issue**

In `form_renderer.py`, the `_fetch_api_bills()` method doesn't reset session state on empty results or errors, leaving stale data.

**Step 2: Fix the API fetch method**

In `src/ui/renderers/cap/form_renderer.py`, update `_fetch_api_bills()`:

```python
def _fetch_api_bills(self, knesset_num: int, bill_type: str, limit: int) -> None:
    """Fetch bills from the Knesset API."""
    try:
        api_service = get_cap_api_service(self.logger)

        with st.spinner(f"Fetching {bill_type} bills from Knesset {knesset_num}..."):
            api_bills = api_service.fetch_bills_for_annotation(
                knesset_num=knesset_num,
                bill_type=bill_type,
                limit=limit,
            )

        if api_bills.empty:
            # Reset state to prevent showing stale results
            st.session_state.api_fetched_bills = pd.DataFrame()
            st.warning(f"No {bill_type} bills found for Knesset {knesset_num}")
            return

        st.session_state.api_fetched_bills = api_bills
        st.success(f"Fetched {len(api_bills)} bills from API")

    except Exception as e:
        # Reset state on error to prevent confusion
        st.session_state.api_fetched_bills = pd.DataFrame()
        self.logger.error(f"API fetch error: {e}", exc_info=True)
        st.error(f"Failed to fetch bills: {e}")
```

**Step 3: Verify the fix**

Run the app and test:
1. Fetch bills from API successfully
2. Change to a Knesset with no bills
3. Verify the list clears (no stale data)

**Step 4: Commit**

```bash
git add src/ui/renderers/cap/form_renderer.py
git commit -m "fix: reset session state on API fetch error or empty results

Prevents stale bill data from appearing in UI when API fetch
fails or returns empty results."
```

---

## Task 5: Add Null Checks in Bill Details Renderer

**Priority:** 🟡 LOW - Defensive programming

**Files:**
- Modify: `src/ui/renderers/cap/bill_queue_renderer.py:236-280`

**Step 1: Add defensive null checks**

In `src/ui/renderers/cap/bill_queue_renderer.py`, update `_render_bill_details()`:

```python
def _render_bill_details(self, bill: pd.Series) -> None:
    """Render detailed bill information."""
    try:
        bill_name = bill.get("BillName", "Unknown Bill")
        if pd.isna(bill_name):
            bill_name = "Unknown Bill"

        knesset_num = bill.get("KnessetNum", "?")
        if pd.isna(knesset_num):
            knesset_num = "?"

        status = bill.get("StatusName", "Unknown")
        if pd.isna(status):
            status = "Unknown"

        st.markdown(f"### {bill_name}")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Knesset:** {knesset_num}")
            st.markdown(f"**Status:** {status}")
        with col2:
            bill_id = bill.get("BillID", "?")
            st.markdown(f"**Bill ID:** {bill_id}")

            submission_date = bill.get("SubmissionDate", "")
            if submission_date and not pd.isna(submission_date):
                st.markdown(f"**Submitted:** {submission_date}")

    except Exception as e:
        self.logger.warning(f"Error rendering bill details: {e}")
        st.warning("Could not display bill details")
```

**Step 2: Commit**

```bash
git add src/ui/renderers/cap/bill_queue_renderer.py
git commit -m "fix: add null checks in bill details renderer

Prevents crashes when bill data has missing or NaN values.
Displays 'Unknown' placeholders for missing fields."
```

---

## Task 6: Add Integration Test for Full Annotation Workflow

**Priority:** 🟠 HIGH - Validates end-to-end flow

**Files:**
- Create: `tests/test_cap_integration.py`

**Step 1: Create integration test file**

```python
"""
CAP Annotation Integration Tests

Tests the full annotation workflow from researcher login to annotation persistence.
"""

import pytest
from pathlib import Path
import tempfile

from ui.services.cap.user_service import CAPUserService
from ui.services.cap.taxonomy import CAPTaxonomyService
from ui.services.cap.repository import CAPAnnotationRepository
from ui.services.cap_service import get_cap_service
from backend.connection_manager import get_db_connection


class TestCAPAnnotationWorkflow:
    """Integration tests for full annotation workflow."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary database for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            yield db_path

    @pytest.fixture
    def setup_system(self, temp_db):
        """Set up complete CAP system with users and taxonomy."""
        user_service = CAPUserService(temp_db)
        taxonomy_service = CAPTaxonomyService(temp_db)
        repository = CAPAnnotationRepository(temp_db)

        # Create tables
        user_service.ensure_table_exists()
        taxonomy_service.ensure_tables_exist()
        taxonomy_service.load_taxonomy_from_csv()
        repository._ensure_table()

        # Create test users
        user_service.create_user(
            username="researcher_a",
            display_name="Researcher A",
            password="test123",
            role="researcher",
        )
        user_service.create_user(
            username="researcher_b",
            display_name="Researcher B",
            password="test456",
            role="researcher",
        )

        return {
            "db_path": temp_db,
            "user_service": user_service,
            "taxonomy_service": taxonomy_service,
            "repository": repository,
        }

    def test_full_annotation_workflow(self, setup_system):
        """Test complete workflow: login -> annotate -> verify persistence."""
        db_path = setup_system["db_path"]
        user_service = setup_system["user_service"]
        repository = setup_system["repository"]

        # Step 1: Authenticate researcher A
        user_a = user_service.authenticate("researcher_a", "test123")
        assert user_a is not None
        researcher_a_id = user_a["id"]

        # Step 2: Get taxonomy for valid CAP code
        taxonomy_service = setup_system["taxonomy_service"]
        major_cats = taxonomy_service.get_major_categories()
        assert len(major_cats) > 0
        first_major = major_cats[0]["MajorCode"]
        minor_cats = taxonomy_service.get_minor_categories(first_major)
        assert len(minor_cats) > 0
        valid_minor_code = minor_cats[0]["MinorCode"]

        # Step 3: Researcher A annotates bill 100
        result = repository.save_annotation(
            bill_id=100,
            cap_minor_code=valid_minor_code,
            direction=1,
            researcher_id=researcher_a_id,
            confidence="High",
            notes="Test annotation by A",
            source="Database",
        )
        assert result is True

        # Step 4: Verify annotation persisted
        with get_db_connection(db_path, read_only=True) as conn:
            annotation = conn.execute(
                "SELECT * FROM UserBillCAP WHERE BillID = 100 AND ResearcherID = ?",
                [researcher_a_id],
            ).fetchone()
            assert annotation is not None
            assert annotation[3] == valid_minor_code  # CAPMinorCode
            assert annotation[4] == 1  # Direction

        # Step 5: Researcher B authenticates
        user_b = user_service.authenticate("researcher_b", "test456")
        assert user_b is not None
        researcher_b_id = user_b["id"]

        # Step 6: Researcher B also annotates bill 100 (multi-annotator)
        result = repository.save_annotation(
            bill_id=100,
            cap_minor_code=valid_minor_code,
            direction=-1,  # Different direction
            researcher_id=researcher_b_id,
            confidence="Medium",
            notes="Test annotation by B",
            source="Database",
        )
        assert result is True

        # Step 7: Verify both annotations exist
        all_annotations = repository.get_all_annotations_for_bill(100)
        assert len(all_annotations) == 2

        # Step 8: Verify annotations are distinct per researcher
        a_annotation = all_annotations[all_annotations["ResearcherID"] == researcher_a_id]
        b_annotation = all_annotations[all_annotations["ResearcherID"] == researcher_b_id]
        assert len(a_annotation) == 1
        assert len(b_annotation) == 1
        assert a_annotation.iloc[0]["Direction"] == 1
        assert b_annotation.iloc[0]["Direction"] == -1

    def test_annotation_visible_to_other_researchers(self, setup_system):
        """Test that one researcher's annotations are visible to others."""
        repository = setup_system["repository"]
        user_service = setup_system["user_service"]
        taxonomy_service = setup_system["taxonomy_service"]

        # Get valid CAP code
        major_cats = taxonomy_service.get_major_categories()
        first_major = major_cats[0]["MajorCode"]
        minor_cats = taxonomy_service.get_minor_categories(first_major)
        valid_minor_code = minor_cats[0]["MinorCode"]

        # Researcher A annotates
        user_a = user_service.authenticate("researcher_a", "test123")
        repository.save_annotation(
            bill_id=200,
            cap_minor_code=valid_minor_code,
            direction=0,
            researcher_id=user_a["id"],
            confidence="Low",
            notes="Visible to B",
            source="Database",
        )

        # Researcher B should see A's annotation
        user_b = user_service.authenticate("researcher_b", "test456")
        all_annotations = repository.get_all_annotations_for_bill(200)

        # B sees A's annotation
        assert len(all_annotations) == 1
        assert all_annotations.iloc[0]["ResearcherID"] == user_a["id"]
        assert all_annotations.iloc[0]["Notes"] == "Visible to B"
```

**Step 2: Run integration tests**

Run: `pytest tests/test_cap_integration.py -v`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add tests/test_cap_integration.py
git commit -m "test: add integration tests for CAP annotation workflow

Tests full workflow from authentication through annotation
persistence and multi-annotator visibility."
```

---

## Task 7: Run Full Test Suite and Verify System Health

**Priority:** 🟢 FINAL - Validation complete

**Step 1: Run all CAP tests**

Run: `pytest tests/test_cap_services.py tests/test_cap_integration.py -v --tb=short`
Expected: All tests PASS

**Step 2: Run fast unit tests**

Run: `pytest tests/ --ignore=tests/test_api_integration.py --ignore=tests/test_e2e.py --ignore=tests/test_data_pipeline_integration.py --ignore=tests/test_connection_leaks.py --tb=short -q`
Expected: ~367 passed, ~26 skipped, 0 failures

**Step 3: Manual smoke test**

1. Launch app: `streamlit run src/ui/data_refresh.py --server.port 8501`
2. Navigate to Bill Annotation
3. Login as admin/knesset2026
4. Select a bill from queue
5. Annotate with any category
6. Verify annotation saved (green success message)
7. Logout
8. Login as different user (create one first in Admin panel)
9. Verify same bill shows in their queue
10. Verify they can see "Other Annotations" section showing first user's annotation

**Step 4: Final commit**

```bash
git add -A
git commit -m "chore: CAP annotation system validation complete

All validation tasks completed:
- DuckDB sequence for thread-safe user IDs
- FK validation for annotations
- Type hints for researcher_id safety
- Session state reset on errors
- Null checks in bill renderer
- Integration tests for full workflow

System ready for production use by multiple researchers."
```

---

## Summary Checklist

| Task | Priority | Status |
|------|----------|--------|
| 1. Fix DuckDB auto-increment race condition | 🔴 CRITICAL | ⬜ |
| 2. Add FK validation for annotations | 🔴 CRITICAL | ⬜ |
| 3. Add type hints for researcher_id | 🟠 HIGH | ⬜ |
| 4. Fix session state reset on error | 🟡 MEDIUM | ⬜ |
| 5. Add null checks in bill renderer | 🟡 LOW | ⬜ |
| 6. Add integration tests | 🟠 HIGH | ⬜ |
| 7. Run full test suite | 🟢 FINAL | ⬜ |

**Estimated Time:** 45-60 minutes for all tasks

**After Completion:** The CAP annotation system will be production-ready for multiple concurrent researchers with proper data validation and error handling.

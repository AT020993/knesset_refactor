# Researcher Platform QA & Hardening Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ensure the Knesset CAP annotation platform is bug-free and production-ready for researchers on Streamlit Cloud.

**Architecture:** Fix 9 identified issues across authentication, error handling, cloud sync, and UI. Add tests for 5 untested renderer files. Perform end-to-end validation on Streamlit Cloud.

**Tech Stack:** Python, Streamlit, DuckDB, pytest, GCS (Google Cloud Storage)

---

## Summary of Issues to Fix

| Priority | Issue | File | Impact |
|----------|-------|------|--------|
| 🔴 HIGH | No session timeout | auth_handler.py | Security risk on shared computers |
| 🔴 HIGH | Inactive users can still use active sessions | auth_handler.py | Deactivated users keep working |
| 🔴 HIGH | Cloud sync failures silent | form_renderer.py | Data loss if sync fails |
| 🟡 MED | API errors indistinguishable from no results | cap_api_service.py | Confusing UX |
| 🟡 MED | PDF fetch failures silent | pdf_viewer.py | User sees "No documents" for network errors |
| 🟡 MED | Minor category not validated against major | form_renderer.py | Potential FK constraint error |
| 🟡 MED | upload_to_gcs.py hardcoded bucket | upload_to_gcs.py | Deployment friction |
| 🟢 LOW | No startup log when cloud storage disabled | gcs_factory.py | Hard to debug |
| 🟢 LOW | Duplicate user creation relies on DB constraint | user_service.py | Poor error message |

## Test Coverage Gaps

| Renderer File | Lines | Current Tests | Status |
|---------------|-------|---------------|--------|
| admin_renderer.py | 250+ | 0 | ❌ UNTESTED |
| auth_handler.py | 229 | 0 | ❌ UNTESTED |
| form_renderer.py | 400+ | 0 | ❌ UNTESTED |
| coded_bills_renderer.py | 200+ | 0 | ❌ UNTESTED |
| stats_renderer.py | 150+ | 0 | ❌ UNTESTED |

---

## Task 1: Add Session Timeout (Security)

**Files:**
- Modify: `src/ui/renderers/cap/auth_handler.py`
- Test: `tests/test_cap_renderers.py`

**Step 1: Write the failing test**

```python
# In tests/test_cap_renderers.py

class TestCAPAuthHandler:
    """Tests for CAP authentication handler."""

    def test_session_timeout_check_valid_session(self):
        """Test that recent sessions are considered valid."""
        from datetime import datetime, timedelta
        from unittest.mock import patch, MagicMock

        with patch('streamlit.session_state', {
            'cap_authenticated': True,
            'cap_login_time': datetime.now() - timedelta(minutes=30)
        }):
            from src.ui.renderers.cap.auth_handler import CAPAuthHandler
            # Session less than 2 hours old should be valid
            assert CAPAuthHandler.is_session_valid() is True

    def test_session_timeout_check_expired_session(self):
        """Test that old sessions are considered expired."""
        from datetime import datetime, timedelta
        from unittest.mock import patch

        with patch('streamlit.session_state', {
            'cap_authenticated': True,
            'cap_login_time': datetime.now() - timedelta(hours=3)
        }):
            from src.ui.renderers.cap.auth_handler import CAPAuthHandler
            # Session older than 2 hours should be expired
            assert CAPAuthHandler.is_session_valid() is False

    def test_session_timeout_missing_login_time(self):
        """Test that sessions without login time are invalid."""
        from unittest.mock import patch

        with patch('streamlit.session_state', {
            'cap_authenticated': True
            # No cap_login_time
        }):
            from src.ui.renderers.cap.auth_handler import CAPAuthHandler
            assert CAPAuthHandler.is_session_valid() is False
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cap_renderers.py::TestCAPAuthHandler::test_session_timeout_check_valid_session -v`
Expected: FAIL with "AttributeError: type object 'CAPAuthHandler' has no attribute 'is_session_valid'"

**Step 3: Write minimal implementation**

```python
# In src/ui/renderers/cap/auth_handler.py
# Add at top of file:
from datetime import datetime, timedelta

# Add this constant after imports:
SESSION_TIMEOUT_HOURS = 2

# Add this static method to CAPAuthHandler class:
@staticmethod
def is_session_valid() -> bool:
    """Check if the current session is still valid (not timed out).

    Returns:
        True if session is valid, False if expired or missing login time.
    """
    if not st.session_state.get('cap_authenticated', False):
        return False

    login_time = st.session_state.get('cap_login_time')
    if login_time is None:
        return False

    elapsed = datetime.now() - login_time
    return elapsed < timedelta(hours=SESSION_TIMEOUT_HOURS)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cap_renderers.py::TestCAPAuthHandler -v`
Expected: PASS

**Step 5: Update check_authentication to use session timeout**

```python
# In src/ui/renderers/cap/auth_handler.py
# Modify the check_authentication method:

@staticmethod
def check_authentication() -> tuple[bool, str]:
    """Check if user is authenticated and session is valid.

    Returns:
        Tuple of (is_authenticated, researcher_name)
    """
    # Check session timeout first
    if st.session_state.get('cap_authenticated', False):
        if not CAPAuthHandler.is_session_valid():
            # Session expired - clear auth state
            CAPAuthHandler._clear_session()
            st.warning("⏰ Your session has expired. Please log in again.")
            return False, ""

    is_authenticated = st.session_state.get('cap_authenticated', False)
    researcher_name = st.session_state.get('cap_researcher_name', '')
    return is_authenticated, researcher_name

@staticmethod
def _clear_session():
    """Clear all CAP session state."""
    keys_to_clear = [
        'cap_authenticated', 'cap_user_id', 'cap_user_role',
        'cap_researcher_name', 'cap_username', 'cap_login_time'
    ]
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]
```

**Step 6: Add test for expired session handling**

```python
def test_check_authentication_clears_expired_session(self):
    """Test that expired sessions are cleared and user is prompted to re-login."""
    from datetime import datetime, timedelta
    from unittest.mock import patch, MagicMock

    mock_session = {
        'cap_authenticated': True,
        'cap_login_time': datetime.now() - timedelta(hours=3),
        'cap_user_id': 1,
        'cap_researcher_name': 'Test User'
    }

    with patch('streamlit.session_state', mock_session):
        with patch('streamlit.warning') as mock_warning:
            from src.ui.renderers.cap.auth_handler import CAPAuthHandler
            is_auth, name = CAPAuthHandler.check_authentication()

            assert is_auth is False
            assert name == ""
            mock_warning.assert_called_once()
            assert 'expired' in mock_warning.call_args[0][0].lower()
```

**Step 7: Run all auth tests**

Run: `pytest tests/test_cap_renderers.py::TestCAPAuthHandler -v`
Expected: All PASS

**Step 8: Commit**

```bash
git add src/ui/renderers/cap/auth_handler.py tests/test_cap_renderers.py
git commit -m "$(cat <<'EOF'
feat(cap): add 2-hour session timeout for security

- Add is_session_valid() to check if session is within timeout period
- Modify check_authentication() to clear expired sessions
- Add _clear_session() helper to clean up all CAP session state
- Show warning message when session expires

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Validate Active User Status on Each Request

**Files:**
- Modify: `src/ui/renderers/cap/auth_handler.py`
- Modify: `src/ui/services/cap/user_service.py`
- Test: `tests/test_cap_renderers.py`

**Step 1: Write the failing test**

```python
def test_check_authentication_validates_user_still_active(self):
    """Test that deactivated users are logged out on next request."""
    from datetime import datetime
    from unittest.mock import patch, MagicMock

    mock_session = {
        'cap_authenticated': True,
        'cap_login_time': datetime.now(),
        'cap_user_id': 999,  # User ID that will be "deactivated"
        'cap_researcher_name': 'Deactivated User'
    }

    mock_user_service = MagicMock()
    mock_user_service.is_user_active.return_value = False  # User deactivated

    with patch('streamlit.session_state', mock_session):
        with patch('streamlit.warning') as mock_warning:
            from src.ui.renderers.cap.auth_handler import CAPAuthHandler
            # Inject mock service
            is_auth, name = CAPAuthHandler.check_authentication(
                user_service=mock_user_service
            )

            assert is_auth is False
            mock_warning.assert_called()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cap_renderers.py::TestCAPAuthHandler::test_check_authentication_validates_user_still_active -v`
Expected: FAIL

**Step 3: Add is_user_active method to UserService**

```python
# In src/ui/services/cap/user_service.py
# Add this method to CAPUserService class:

def is_user_active(self, user_id: int) -> bool:
    """Check if a user is currently active.

    Args:
        user_id: The user's database ID

    Returns:
        True if user exists and is active, False otherwise
    """
    try:
        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
            result = conn.execute("""
                SELECT IsActive FROM UserResearchers WHERE ResearcherID = ?
            """, [user_id]).fetchone()

            if result is None:
                return False
            return bool(result[0])
    except Exception as e:
        self.logger.error(f"Error checking user active status: {e}")
        return False  # Fail secure - treat as inactive
```

**Step 4: Update check_authentication to validate active status**

```python
# In src/ui/renderers/cap/auth_handler.py
# Modify check_authentication:

@staticmethod
def check_authentication(user_service=None) -> tuple[bool, str]:
    """Check if user is authenticated, session valid, and user still active.

    Args:
        user_service: Optional CAPUserService for active status check.
                     If None, skips active status validation.

    Returns:
        Tuple of (is_authenticated, researcher_name)
    """
    if not st.session_state.get('cap_authenticated', False):
        return False, ""

    # Check session timeout
    if not CAPAuthHandler.is_session_valid():
        CAPAuthHandler._clear_session()
        st.warning("⏰ Your session has expired. Please log in again.")
        return False, ""

    # Check if user is still active (if service provided)
    if user_service is not None:
        user_id = st.session_state.get('cap_user_id')
        if user_id and not user_service.is_user_active(user_id):
            CAPAuthHandler._clear_session()
            st.warning("🚫 Your account has been deactivated. Please contact an administrator.")
            return False, ""

    researcher_name = st.session_state.get('cap_researcher_name', '')
    return True, researcher_name
```

**Step 5: Run tests**

Run: `pytest tests/test_cap_renderers.py::TestCAPAuthHandler -v`
Expected: PASS

**Step 6: Add test for is_user_active in user service tests**

```python
# In tests/test_cap_services.py
# Add to TestCAPUserService class:

def test_is_user_active_returns_true_for_active_user(self, initialized_db):
    """Test is_user_active returns True for active users."""
    service = CAPUserService(initialized_db)
    # Create an active user
    user_id = service.create_user("activeuser", "Active User", "password123", "researcher")

    assert service.is_user_active(user_id) is True

def test_is_user_active_returns_false_for_deactivated_user(self, initialized_db):
    """Test is_user_active returns False for deactivated users."""
    service = CAPUserService(initialized_db)
    user_id = service.create_user("tobedeactivated", "Soon Inactive", "password123", "researcher")
    service.deactivate_user(user_id)

    assert service.is_user_active(user_id) is False

def test_is_user_active_returns_false_for_nonexistent_user(self, initialized_db):
    """Test is_user_active returns False for non-existent user IDs."""
    service = CAPUserService(initialized_db)

    assert service.is_user_active(99999) is False
```

**Step 7: Run all related tests**

Run: `pytest tests/test_cap_services.py::TestCAPUserService -v`
Expected: PASS

**Step 8: Commit**

```bash
git add src/ui/renderers/cap/auth_handler.py src/ui/services/cap/user_service.py tests/test_cap_renderers.py tests/test_cap_services.py
git commit -m "$(cat <<'EOF'
feat(cap): validate user active status on each request

- Add is_user_active() to CAPUserService
- Update check_authentication() to verify user still active
- Deactivated users are logged out with clear message
- Fail secure: treat errors as inactive

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Add User Feedback for Cloud Sync Failures

**Files:**
- Modify: `src/ui/renderers/cap/form_renderer.py`
- Modify: `src/ui/renderers/cap/coded_bills_renderer.py`
- Test: `tests/test_cap_renderers.py`

**Step 1: Write the failing test**

```python
class TestCAPFormRenderer:
    """Tests for CAP annotation form renderer."""

    def test_sync_to_cloud_returns_success_status(self):
        """Test that _sync_to_cloud returns success/failure status."""
        from unittest.mock import patch, MagicMock

        mock_sync_service = MagicMock()
        mock_sync_service.is_enabled.return_value = True
        mock_sync_service.gcs_manager.upload_file.return_value = True

        with patch('src.ui.renderers.cap.form_renderer.StorageSyncService', return_value=mock_sync_service):
            from src.ui.renderers.cap.form_renderer import CAPFormRenderer
            renderer = CAPFormRenderer(db_path='/tmp/test.db', service=MagicMock())

            success = renderer._sync_to_cloud()
            assert success is True

    def test_sync_to_cloud_returns_false_on_failure(self):
        """Test that _sync_to_cloud returns False when upload fails."""
        from unittest.mock import patch, MagicMock

        mock_sync_service = MagicMock()
        mock_sync_service.is_enabled.return_value = True
        mock_sync_service.gcs_manager.upload_file.return_value = False

        with patch('src.ui.renderers.cap.form_renderer.StorageSyncService', return_value=mock_sync_service):
            from src.ui.renderers.cap.form_renderer import CAPFormRenderer
            renderer = CAPFormRenderer(db_path='/tmp/test.db', service=MagicMock())

            success = renderer._sync_to_cloud()
            assert success is False
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cap_renderers.py::TestCAPFormRenderer::test_sync_to_cloud_returns_success_status -v`
Expected: FAIL (method doesn't return status)

**Step 3: Modify _sync_to_cloud to return status**

```python
# In src/ui/renderers/cap/form_renderer.py
# Modify _sync_to_cloud method:

def _sync_to_cloud(self) -> bool:
    """Sync database to cloud storage after annotation.

    Returns:
        True if sync succeeded or was skipped (not enabled),
        False if sync was attempted but failed.
    """
    try:
        from src.ui.services.gcs_factory import create_storage_sync_service
        sync_service = create_storage_sync_service(self.logger)

        if sync_service is None or not sync_service.is_enabled():
            self.logger.debug("Cloud storage not enabled, skipping sync")
            return True  # Not enabled = success (nothing to do)

        success = sync_service.gcs_manager.upload_file(
            str(self.db_path),
            "data/warehouse.duckdb"
        )

        if success:
            self.logger.info("Database synced to cloud storage")
        else:
            self.logger.warning("Cloud sync upload returned False")

        return success

    except Exception as e:
        self.logger.warning(f"Cloud sync failed: {e}")
        return False
```

**Step 4: Update form submission to show sync status**

```python
# In src/ui/renderers/cap/form_renderer.py
# In the form submission handler, after saving annotation:

if success:
    st.success("✅ Annotation saved successfully!")

    # Sync to cloud and show status
    sync_success = self._sync_to_cloud()
    if not sync_success:
        st.warning(
            "⚠️ Annotation saved locally, but cloud sync failed. "
            "Your work is safe but may not be visible to other researchers until sync succeeds."
        )

    # Clear form state for next annotation
    self._clear_annotation_state()
else:
    st.error("❌ Error saving annotation. Please try again.")
```

**Step 5: Run tests**

Run: `pytest tests/test_cap_renderers.py::TestCAPFormRenderer -v`
Expected: PASS

**Step 6: Apply same pattern to coded_bills_renderer.py**

```python
# In src/ui/renderers/cap/coded_bills_renderer.py
# Same pattern for delete and update operations

def _handle_delete(self, annotation_id: int) -> bool:
    """Handle annotation deletion with cloud sync feedback."""
    success = self.service.delete_annotation(annotation_id)

    if success:
        st.success("✅ Annotation deleted successfully!")

        sync_success = self._sync_to_cloud()
        if not sync_success:
            st.warning(
                "⚠️ Deletion saved locally, but cloud sync failed. "
                "Change may not be visible to other researchers until sync succeeds."
            )
        return True
    else:
        st.error("❌ Error deleting annotation.")
        return False
```

**Step 7: Commit**

```bash
git add src/ui/renderers/cap/form_renderer.py src/ui/renderers/cap/coded_bills_renderer.py tests/test_cap_renderers.py
git commit -m "$(cat <<'EOF'
feat(cap): show user feedback on cloud sync failures

- _sync_to_cloud() now returns success/failure status
- Show warning when sync fails (annotation still saved locally)
- Users know their work is safe but may need manual sync
- Apply pattern to both form_renderer and coded_bills_renderer

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Distinguish API Errors from Empty Results

**Files:**
- Modify: `src/ui/services/cap_api_service.py`
- Test: `tests/test_cap_services.py`

**Step 1: Write the failing test**

```python
# In tests/test_cap_services.py
# Add new test class:

class TestCAPApiService:
    """Tests for CAP API service error handling."""

    def test_search_bills_returns_error_tuple_on_api_failure(self):
        """Test that API failures return error info, not empty list."""
        from unittest.mock import patch, MagicMock, AsyncMock
        from src.ui.services.cap_api_service import CAPApiService

        service = CAPApiService()

        # Mock a network error
        with patch.object(service, '_fetch_from_api', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = Exception("Network timeout")

            result, error = service.search_bills_by_name("test")

            assert result == []
            assert error is not None
            assert "timeout" in error.lower() or "error" in error.lower()

    def test_search_bills_returns_none_error_on_success(self):
        """Test that successful searches return None for error."""
        from unittest.mock import patch, MagicMock, AsyncMock
        from src.ui.services.cap_api_service import CAPApiService

        service = CAPApiService()

        with patch.object(service, '_fetch_from_api', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = []  # Empty but successful

            result, error = service.search_bills_by_name("nonexistent")

            assert result == []
            assert error is None  # No error, just no results
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cap_services.py::TestCAPApiService -v`
Expected: FAIL (returns list, not tuple)

**Step 3: Modify search_bills_by_name to return tuple**

```python
# In src/ui/services/cap_api_service.py
# Change return type and add error handling:

from typing import Optional

def search_bills_by_name(self, search_term: str, limit: int = 50) -> tuple[list[dict], Optional[str]]:
    """Search for bills by name from the Knesset API.

    Args:
        search_term: Text to search for in bill names
        limit: Maximum results to return

    Returns:
        Tuple of (results_list, error_message).
        - On success: (list_of_bills, None)
        - On error: ([], "Error description")
    """
    try:
        results = self._run_async(self._search_bills_async(search_term, limit))
        return results, None
    except asyncio.TimeoutError:
        self.logger.error(f"API timeout searching for '{search_term}'")
        return [], "Request timed out. The Knesset API may be slow. Please try again."
    except aiohttp.ClientError as e:
        self.logger.error(f"Network error searching for '{search_term}': {e}")
        return [], f"Network error: {str(e)}"
    except Exception as e:
        self.logger.error(f"Error searching for '{search_term}': {e}")
        return [], f"Unexpected error: {str(e)}"
```

**Step 4: Update UI to show specific error messages**

```python
# In src/ui/renderers/cap/form_renderer.py (or wherever API results are displayed)
# Update the search results handling:

results, error = self.api_service.search_bills_by_name(search_term)

if error:
    st.error(f"🔴 API Error: {error}")
elif not results:
    st.info("No bills found matching your search. Try different keywords.")
else:
    # Display results
    for bill in results:
        # ...render bill
```

**Step 5: Run tests**

Run: `pytest tests/test_cap_services.py::TestCAPApiService -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/ui/services/cap_api_service.py src/ui/renderers/cap/form_renderer.py tests/test_cap_services.py
git commit -m "$(cat <<'EOF'
feat(cap): distinguish API errors from empty results

- search_bills_by_name now returns (results, error) tuple
- Specific error messages for timeout, network, and unexpected errors
- UI shows appropriate message based on error vs no results
- Better UX when Knesset API is slow or unavailable

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Add PDF Fetch Error Feedback

**Files:**
- Modify: `src/ui/renderers/cap/pdf_viewer.py`
- Test: `tests/test_cap_renderers.py`

**Step 1: Write the failing test**

```python
class TestCAPPDFViewer:
    """Tests for PDF viewer error handling."""

    def test_fetch_pdf_returns_error_on_network_failure(self):
        """Test that network errors return error message, not just None."""
        from unittest.mock import patch, MagicMock
        from src.ui.renderers.cap.pdf_viewer import CAPPDFViewer

        viewer = CAPPDFViewer()

        with patch('requests.get') as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout("Connection timed out")

            data, error = viewer.fetch_pdf_as_base64("http://example.com/doc.pdf")

            assert data is None
            assert error is not None
            assert "timeout" in error.lower()

    def test_fetch_pdf_returns_none_error_on_success(self):
        """Test successful fetch returns data and no error."""
        from unittest.mock import patch, MagicMock
        from src.ui.renderers.cap.pdf_viewer import CAPPDFViewer

        viewer = CAPPDFViewer()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'%PDF-1.4 fake pdf content'

        with patch('requests.get', return_value=mock_response):
            data, error = viewer.fetch_pdf_as_base64("http://example.com/doc.pdf")

            assert data is not None
            assert error is None
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cap_renderers.py::TestCAPPDFViewer::test_fetch_pdf_returns_error_on_network_failure -v`
Expected: FAIL

**Step 3: Modify fetch_pdf_as_base64 to return error info**

```python
# In src/ui/renderers/cap/pdf_viewer.py

import requests
from typing import Optional
import base64

def fetch_pdf_as_base64(self, pdf_url: str) -> tuple[Optional[str], Optional[str]]:
    """Fetch PDF from URL and return as base64.

    Args:
        pdf_url: URL of the PDF document

    Returns:
        Tuple of (base64_data, error_message).
        - On success: (base64_string, None)
        - On error: (None, "Error description")
    """
    try:
        response = requests.get(pdf_url, timeout=15)

        if response.status_code == 404:
            return None, "Document not found (404)"
        elif response.status_code == 403:
            return None, "Access denied to document (403)"
        elif response.status_code != 200:
            return None, f"Server returned error: HTTP {response.status_code}"

        # Check content type
        content_type = response.headers.get('Content-Type', '')
        if 'pdf' not in content_type.lower() and not response.content.startswith(b'%PDF'):
            return None, "Document is not a valid PDF"

        base64_data = base64.b64encode(response.content).decode('utf-8')
        return base64_data, None

    except requests.exceptions.Timeout:
        return None, "Request timed out. The document server may be slow."
    except requests.exceptions.ConnectionError:
        return None, "Could not connect to document server. Check your network."
    except requests.exceptions.SSLError:
        return None, "SSL certificate error. The document server may have security issues."
    except Exception as e:
        self.logger.error(f"Error fetching PDF: {e}")
        return None, f"Unexpected error: {str(e)}"
```

**Step 4: Update render_pdf to show error messages**

```python
# In pdf_viewer.py render method:

def render_pdf(self, bill_id: int, documents: list[dict]):
    """Render PDF viewer with error feedback."""
    if not documents:
        st.info("📄 No documents available for this bill.")
        return

    # Select document (prioritize Published Law)
    doc = self._select_best_document(documents)
    pdf_url = doc.get('FilePath')

    if not pdf_url:
        st.warning("⚠️ Document URL is missing.")
        return

    with st.spinner("Loading document..."):
        base64_data, error = self.fetch_pdf_as_base64(pdf_url)

    if error:
        st.error(f"📄 Could not load document: {error}")
        st.markdown(f"[Open document in new tab]({pdf_url})")
        return

    # Render the PDF
    self._render_embedded_pdf(base64_data)
```

**Step 5: Run tests**

Run: `pytest tests/test_cap_renderers.py::TestCAPPDFViewer -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/ui/renderers/cap/pdf_viewer.py tests/test_cap_renderers.py
git commit -m "$(cat <<'EOF'
feat(cap): show specific errors when PDF loading fails

- fetch_pdf_as_base64 now returns (data, error) tuple
- Specific messages for timeout, 404, 403, SSL, network errors
- Validates PDF content type before rendering
- Shows fallback link to open document in new tab

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Validate Minor Category Belongs to Major

**Files:**
- Modify: `src/ui/renderers/cap/form_renderer.py`
- Test: `tests/test_cap_renderers.py`

**Step 1: Write the failing test**

```python
def test_validate_category_selection_rejects_mismatched_categories(self):
    """Test that minor category must belong to selected major."""
    from unittest.mock import patch, MagicMock
    from src.ui.renderers.cap.form_renderer import CAPFormRenderer

    mock_service = MagicMock()
    # Major 1 has minors 101-108, Major 2 has 201-204
    mock_service.get_minor_categories.return_value = [
        {'MinorCode': 101, 'MinorTopic_HE': 'Test'}
    ]

    renderer = CAPFormRenderer(db_path='/tmp/test.db', service=mock_service)

    # Select major 1 but minor 201 (belongs to major 2)
    is_valid, error = renderer._validate_category_selection(
        major_code=1,
        minor_code=201
    )

    assert is_valid is False
    assert "does not belong" in error.lower() or "invalid" in error.lower()

def test_validate_category_selection_accepts_valid_combination(self):
    """Test that valid major/minor combinations are accepted."""
    from unittest.mock import MagicMock
    from src.ui.renderers.cap.form_renderer import CAPFormRenderer

    mock_service = MagicMock()
    mock_service.get_minor_categories.return_value = [
        {'MinorCode': 101, 'MinorTopic_HE': 'Test'},
        {'MinorCode': 102, 'MinorTopic_HE': 'Test 2'}
    ]

    renderer = CAPFormRenderer(db_path='/tmp/test.db', service=mock_service)

    is_valid, error = renderer._validate_category_selection(
        major_code=1,
        minor_code=101
    )

    assert is_valid is True
    assert error is None
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cap_renderers.py -k "validate_category" -v`
Expected: FAIL (method doesn't exist)

**Step 3: Add validation method**

```python
# In src/ui/renderers/cap/form_renderer.py

def _validate_category_selection(
    self,
    major_code: Optional[int],
    minor_code: Optional[int]
) -> tuple[bool, Optional[str]]:
    """Validate that selected categories are valid and compatible.

    Args:
        major_code: Selected major category code
        minor_code: Selected minor category code

    Returns:
        Tuple of (is_valid, error_message)
    """
    if major_code is None:
        return False, "Please select a Major Category"

    if minor_code is None:
        return False, "Please select a Minor Category"

    # Get valid minor codes for this major
    valid_minors = self.service.get_minor_categories(major_code)
    valid_minor_codes = {m['MinorCode'] for m in valid_minors}

    if minor_code not in valid_minor_codes:
        return False, f"Selected minor category does not belong to major category {major_code}"

    return True, None
```

**Step 4: Use validation in form submission**

```python
# In form submission handler:

# Validate category selection
is_valid, error = self._validate_category_selection(selected_major, selected_minor)
if not is_valid:
    st.error(f"❌ {error}")
    return

# Proceed with saving annotation
success = self.service.save_annotation(...)
```

**Step 5: Run tests**

Run: `pytest tests/test_cap_renderers.py -k "validate_category" -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/ui/renderers/cap/form_renderer.py tests/test_cap_renderers.py
git commit -m "$(cat <<'EOF'
feat(cap): validate minor category belongs to selected major

- Add _validate_category_selection() method
- Check minor code is in valid set for major before saving
- Prevents FK constraint errors from invalid combinations
- Better error messages for category selection issues

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Make upload_to_gcs.py Bucket Configurable

**Files:**
- Modify: `upload_to_gcs.py`
- No tests needed (utility script)

**Step 1: Read current file**

Read: `upload_to_gcs.py`

**Step 2: Modify to use environment variable**

```python
#!/usr/bin/env python3
"""Upload local data files to Google Cloud Storage.

Usage:
    # Set bucket name via environment variable
    export GCS_BUCKET_NAME="your-bucket-name"
    export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"
    python upload_to_gcs.py

    # Or pass bucket name as argument
    python upload_to_gcs.py --bucket your-bucket-name
"""

import os
import sys
import argparse
from pathlib import Path
from google.cloud import storage


def get_bucket_name(args_bucket: str = None) -> str:
    """Get bucket name from args, env var, or prompt user."""
    # Priority: CLI arg > env var > prompt
    if args_bucket:
        return args_bucket

    env_bucket = os.environ.get('GCS_BUCKET_NAME')
    if env_bucket:
        return env_bucket

    # No bucket specified - error out with helpful message
    print("ERROR: No GCS bucket specified.")
    print("\nPlease specify bucket name via:")
    print("  1. Command line: python upload_to_gcs.py --bucket YOUR_BUCKET")
    print("  2. Environment variable: export GCS_BUCKET_NAME=YOUR_BUCKET")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Upload data to GCS')
    parser.add_argument('--bucket', '-b', help='GCS bucket name')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be uploaded')
    args = parser.parse_args()

    bucket_name = get_bucket_name(args.bucket)
    print(f"Using GCS bucket: {bucket_name}")

    # Files to upload
    files_to_upload = [
        ('data/warehouse.duckdb', 'data/warehouse.duckdb'),
        ('data/faction_coalition_status.csv', 'data/faction_coalition_status.csv'),
    ]

    # Add all parquet files
    parquet_dir = Path('data/parquet')
    if parquet_dir.exists():
        for pq_file in parquet_dir.glob('*.parquet'):
            files_to_upload.append(
                (str(pq_file), f'data/parquet/{pq_file.name}')
            )

    if args.dry_run:
        print("\nDry run - would upload:")
        for local, remote in files_to_upload:
            if Path(local).exists():
                size = Path(local).stat().st_size / 1024 / 1024
                print(f"  {local} -> gs://{bucket_name}/{remote} ({size:.1f} MB)")
            else:
                print(f"  {local} -> SKIPPED (file not found)")
        return

    # Upload files
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for local_path, remote_path in files_to_upload:
        if not Path(local_path).exists():
            print(f"Skipping {local_path} (not found)")
            continue

        print(f"Uploading {local_path}...")
        blob = bucket.blob(remote_path)
        blob.upload_from_filename(local_path)
        print(f"  -> gs://{bucket_name}/{remote_path}")

    print("\nUpload complete!")


if __name__ == '__main__':
    main()
```

**Step 3: Commit**

```bash
git add upload_to_gcs.py
git commit -m "$(cat <<'EOF'
refactor: make upload_to_gcs.py bucket name configurable

- Accept bucket name via --bucket CLI argument
- Fall back to GCS_BUCKET_NAME environment variable
- Add --dry-run option to preview uploads
- Remove hardcoded bucket name
- Better error messages when bucket not specified

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Add Startup Log When Cloud Storage Disabled

**Files:**
- Modify: `src/ui/services/gcs_factory.py`

**Step 1: Read current file**

Read: `src/ui/services/gcs_factory.py`

**Step 2: Add informative logging**

```python
# In src/ui/services/gcs_factory.py

def create_storage_sync_service(logger=None) -> Optional[StorageSyncService]:
    """Create storage sync service if credentials are available.

    Returns:
        StorageSyncService if configured, None otherwise.
        Logs clear message about cloud storage status.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    try:
        # Check if Streamlit secrets has GCS config
        if not hasattr(st, 'secrets'):
            logger.info("☁️ Cloud storage: DISABLED (not running in Streamlit)")
            return None

        gcp_secrets = st.secrets.get('gcp_service_account', {})
        storage_secrets = st.secrets.get('storage', {})

        bucket_name = storage_secrets.get('gcs_bucket_name')
        if not bucket_name:
            logger.info("☁️ Cloud storage: DISABLED (no bucket configured in secrets)")
            return None

        # Check for any credential format
        has_credentials = any([
            gcp_secrets.get('credentials_base64'),
            gcp_secrets.get('credentials_json'),
            gcp_secrets.get('client_email'),  # Direct fields
        ])

        if not has_credentials:
            logger.info("☁️ Cloud storage: DISABLED (no GCP credentials in secrets)")
            return None

        # Credentials present - try to create service
        service = StorageSyncService(logger_obj=logger)
        if service.is_enabled():
            logger.info(f"☁️ Cloud storage: ENABLED (bucket: {bucket_name})")
            return service
        else:
            logger.warning("☁️ Cloud storage: DISABLED (service creation failed)")
            return None

    except Exception as e:
        logger.warning(f"☁️ Cloud storage: DISABLED (error: {e})")
        return None
```

**Step 3: Commit**

```bash
git add src/ui/services/gcs_factory.py
git commit -m "$(cat <<'EOF'
feat: add clear startup log for cloud storage status

- Log whether cloud storage is enabled/disabled at startup
- Show specific reason when disabled (no bucket, no credentials, error)
- Use ☁️ emoji for easy visual scanning in logs
- Helps debug deployment issues on Streamlit Cloud

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Pre-Check Username Before User Creation

**Files:**
- Modify: `src/ui/services/cap/user_service.py`
- Test: `tests/test_cap_services.py`

**Step 1: Write the failing test**

```python
def test_create_user_returns_error_for_duplicate_username(self, initialized_db):
    """Test that duplicate username returns None with clear reason."""
    service = CAPUserService(initialized_db)

    # Create first user
    user_id = service.create_user("duplicateuser", "First User", "password123", "researcher")
    assert user_id is not None

    # Try to create second user with same username
    user_id2, error = service.create_user_with_validation("duplicateuser", "Second User", "password456", "researcher")

    assert user_id2 is None
    assert error is not None
    assert "already exists" in error.lower()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cap_services.py -k "duplicate_username" -v`
Expected: FAIL (method doesn't exist)

**Step 3: Add create_user_with_validation method**

```python
# In src/ui/services/cap/user_service.py

def user_exists(self, username: str) -> bool:
    """Check if a username already exists.

    Args:
        username: Username to check

    Returns:
        True if username exists, False otherwise
    """
    try:
        with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
            result = conn.execute(
                "SELECT 1 FROM UserResearchers WHERE Username = ?",
                [username]
            ).fetchone()
            return result is not None
    except Exception as e:
        self.logger.error(f"Error checking username existence: {e}")
        return True  # Fail secure - assume exists if error

def create_user_with_validation(
    self,
    username: str,
    display_name: str,
    password: str,
    role: str
) -> tuple[Optional[int], Optional[str]]:
    """Create user with pre-validation checks.

    Args:
        username: Unique login name
        display_name: Name shown in UI
        password: Plain text password (will be hashed)
        role: 'admin' or 'researcher'

    Returns:
        Tuple of (user_id, error_message).
        - On success: (user_id, None)
        - On error: (None, "Error description")
    """
    # Validate username format
    if not username or len(username) < 3:
        return None, "Username must be at least 3 characters"

    if not username.isalnum() and '_' not in username:
        return None, "Username can only contain letters, numbers, and underscores"

    # Validate password
    if not password or len(password) < 6:
        return None, "Password must be at least 6 characters"

    # Validate display name
    if not display_name or not display_name.strip():
        return None, "Display name is required"

    # Validate role
    if role not in ('admin', 'researcher'):
        return None, "Role must be 'admin' or 'researcher'"

    # Check for duplicate username
    if self.user_exists(username):
        return None, f"Username '{username}' already exists"

    # Create user
    try:
        user_id = self.create_user(username, display_name, password, role)
        if user_id:
            return user_id, None
        else:
            return None, "Failed to create user (unknown error)"
    except Exception as e:
        self.logger.error(f"Error creating user: {e}")
        return None, f"Database error: {str(e)}"
```

**Step 4: Update admin_renderer to use new method**

```python
# In src/ui/renderers/cap/admin_renderer.py
# Update the add user form handler:

user_id, error = self.user_service.create_user_with_validation(
    username=username,
    display_name=display_name,
    password=password,
    role=role
)

if error:
    st.error(f"❌ {error}")
else:
    st.success(f"✅ User '{display_name}' created successfully!")
```

**Step 5: Run tests**

Run: `pytest tests/test_cap_services.py -k "create_user" -v`
Expected: PASS

**Step 6: Commit**

```bash
git add src/ui/services/cap/user_service.py src/ui/renderers/cap/admin_renderer.py tests/test_cap_services.py
git commit -m "$(cat <<'EOF'
feat(cap): pre-validate user creation to prevent duplicate usernames

- Add user_exists() to check username availability
- Add create_user_with_validation() with full input validation
- Clear error messages for: duplicate username, short password, invalid role
- Update admin panel to use validation method

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Add Tests for Admin Renderer

**Files:**
- Test: `tests/test_cap_renderers.py`

**Step 1: Add test class for admin renderer**

```python
class TestCAPAdminRenderer:
    """Tests for CAP admin panel renderer."""

    def test_render_user_list_shows_all_users(self):
        """Test that user list displays all registered users."""
        from unittest.mock import patch, MagicMock
        from src.ui.renderers.cap.admin_renderer import CAPAdminRenderer

        mock_service = MagicMock()
        mock_service.get_all_users.return_value = [
            {'ResearcherID': 1, 'DisplayName': 'Admin', 'Username': 'admin', 'Role': 'admin', 'IsActive': True},
            {'ResearcherID': 2, 'DisplayName': 'Researcher', 'Username': 'user1', 'Role': 'researcher', 'IsActive': True},
        ]

        renderer = CAPAdminRenderer(user_service=mock_service)
        users = renderer._get_user_list()

        assert len(users) == 2
        assert users[0]['DisplayName'] == 'Admin'

    def test_prevent_self_deletion(self):
        """Test that admin cannot delete their own account."""
        from unittest.mock import patch, MagicMock
        from src.ui.renderers.cap.admin_renderer import CAPAdminRenderer

        renderer = CAPAdminRenderer(user_service=MagicMock())

        with patch('streamlit.session_state', {'cap_user_id': 1}):
            can_delete, reason = renderer._can_delete_user(user_id=1)

            assert can_delete is False
            assert "yourself" in reason.lower() or "own" in reason.lower()

    def test_prevent_deletion_of_user_with_annotations(self):
        """Test that users with annotations cannot be deleted."""
        from unittest.mock import MagicMock
        from src.ui.renderers.cap.admin_renderer import CAPAdminRenderer

        mock_service = MagicMock()
        mock_service.get_user_annotation_count.return_value = 5  # Has annotations

        renderer = CAPAdminRenderer(user_service=mock_service)

        with patch('streamlit.session_state', {'cap_user_id': 99}):  # Different user
            can_delete, reason = renderer._can_delete_user(user_id=2)

            assert can_delete is False
            assert "annotation" in reason.lower()

    def test_role_change_validation(self):
        """Test that role changes are validated."""
        from unittest.mock import MagicMock
        from src.ui.renderers.cap.admin_renderer import CAPAdminRenderer

        renderer = CAPAdminRenderer(user_service=MagicMock())

        # Invalid role
        is_valid, error = renderer._validate_role_change("invalid_role")
        assert is_valid is False

        # Valid role
        is_valid, error = renderer._validate_role_change("researcher")
        assert is_valid is True
```

**Step 2: Run tests**

Run: `pytest tests/test_cap_renderers.py::TestCAPAdminRenderer -v`
Expected: PASS (after implementing helper methods)

**Step 3: Add helper methods to admin_renderer if missing**

```python
# In src/ui/renderers/cap/admin_renderer.py

def _can_delete_user(self, user_id: int) -> tuple[bool, str]:
    """Check if a user can be deleted.

    Returns:
        Tuple of (can_delete, reason_if_not)
    """
    # Can't delete yourself
    current_user_id = st.session_state.get('cap_user_id')
    if user_id == current_user_id:
        return False, "You cannot delete your own account"

    # Can't delete users with annotations
    annotation_count = self.user_service.get_user_annotation_count(user_id)
    if annotation_count > 0:
        return False, f"User has {annotation_count} annotations. Deactivate instead of deleting."

    return True, ""

def _validate_role_change(self, new_role: str) -> tuple[bool, Optional[str]]:
    """Validate a role change.

    Returns:
        Tuple of (is_valid, error_message)
    """
    valid_roles = ('admin', 'researcher')
    if new_role not in valid_roles:
        return False, f"Invalid role. Must be one of: {', '.join(valid_roles)}"
    return True, None
```

**Step 4: Commit**

```bash
git add tests/test_cap_renderers.py src/ui/renderers/cap/admin_renderer.py
git commit -m "$(cat <<'EOF'
test(cap): add tests for admin panel renderer

- Test user list display
- Test self-deletion prevention
- Test deletion blocked for users with annotations
- Test role change validation
- Add helper methods for testability

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: Add Tests for Auth Handler

**Files:**
- Test: `tests/test_cap_renderers.py`

**Step 1: Add comprehensive auth handler tests**

```python
class TestCAPAuthHandler:
    """Tests for CAP authentication handler."""

    def test_is_feature_enabled_returns_true_when_enabled(self):
        """Test feature flag detection."""
        from unittest.mock import patch, MagicMock

        mock_secrets = MagicMock()
        mock_secrets.get.return_value = {'enabled': True}

        with patch('streamlit.secrets', mock_secrets):
            from src.ui.renderers.cap.auth_handler import CAPAuthHandler
            assert CAPAuthHandler.is_feature_enabled() is True

    def test_is_feature_enabled_returns_false_when_disabled(self):
        """Test feature flag when disabled."""
        from unittest.mock import patch, MagicMock

        mock_secrets = MagicMock()
        mock_secrets.get.return_value = {'enabled': False}

        with patch('streamlit.secrets', mock_secrets):
            from src.ui.renderers.cap.auth_handler import CAPAuthHandler
            assert CAPAuthHandler.is_feature_enabled() is False

    def test_logout_clears_all_session_state(self):
        """Test that logout clears all CAP session variables."""
        from unittest.mock import patch

        mock_session = {
            'cap_authenticated': True,
            'cap_user_id': 1,
            'cap_user_role': 'admin',
            'cap_researcher_name': 'Test',
            'cap_username': 'testuser',
            'cap_login_time': 'some_time',
            'other_state': 'should_remain'
        }

        with patch('streamlit.session_state', mock_session):
            from src.ui.renderers.cap.auth_handler import CAPAuthHandler
            CAPAuthHandler.logout()

            # CAP state should be cleared
            assert 'cap_authenticated' not in mock_session
            assert 'cap_user_id' not in mock_session
            # Other state should remain
            assert mock_session.get('other_state') == 'should_remain'

    def test_login_sets_correct_session_state(self):
        """Test that successful login sets all required session state."""
        from unittest.mock import patch, MagicMock
        from datetime import datetime

        mock_session = {}
        mock_user_service = MagicMock()
        mock_user_service.authenticate.return_value = {
            'ResearcherID': 42,
            'DisplayName': 'Test Researcher',
            'Username': 'testuser',
            'Role': 'researcher'
        }

        with patch('streamlit.session_state', mock_session):
            from src.ui.renderers.cap.auth_handler import CAPAuthHandler
            handler = CAPAuthHandler(user_service=mock_user_service)

            success = handler._process_login('testuser', 'password123')

            assert success is True
            assert mock_session['cap_authenticated'] is True
            assert mock_session['cap_user_id'] == 42
            assert mock_session['cap_user_role'] == 'researcher'
            assert mock_session['cap_researcher_name'] == 'Test Researcher'
            assert 'cap_login_time' in mock_session
```

**Step 2: Run tests**

Run: `pytest tests/test_cap_renderers.py::TestCAPAuthHandler -v`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/test_cap_renderers.py
git commit -m "$(cat <<'EOF'
test(cap): add comprehensive auth handler tests

- Test feature flag detection (enabled/disabled)
- Test logout clears all CAP session state
- Test login sets correct session state variables
- Test session timeout (from Task 1)

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: Add Tests for Stats Renderer

**Files:**
- Test: `tests/test_cap_renderers.py`

**Step 1: Add stats renderer tests**

```python
class TestCAPStatsRenderer:
    """Tests for CAP statistics renderer."""

    def test_format_coverage_percentage(self):
        """Test percentage formatting."""
        from src.ui.renderers.cap.stats_renderer import CAPStatsRenderer

        renderer = CAPStatsRenderer(service=MagicMock())

        assert renderer._format_percentage(0.5) == "50.0%"
        assert renderer._format_percentage(0.333) == "33.3%"
        assert renderer._format_percentage(1.0) == "100.0%"
        assert renderer._format_percentage(0) == "0.0%"

    def test_get_summary_metrics(self):
        """Test summary metrics calculation."""
        from unittest.mock import MagicMock
        from src.ui.renderers.cap.stats_renderer import CAPStatsRenderer

        mock_service = MagicMock()
        mock_service.get_annotation_statistics.return_value = {
            'total_bills': 100,
            'coded_bills': 25,
            'coverage_pct': 25.0
        }

        renderer = CAPStatsRenderer(service=mock_service)
        metrics = renderer._get_summary_metrics()

        assert metrics['total_bills'] == 100
        assert metrics['coded_bills'] == 25
        assert metrics['coverage_pct'] == 25.0

    def test_handles_empty_database(self):
        """Test graceful handling when no annotations exist."""
        from unittest.mock import MagicMock
        from src.ui.renderers.cap.stats_renderer import CAPStatsRenderer

        mock_service = MagicMock()
        mock_service.get_annotation_statistics.return_value = {
            'total_bills': 0,
            'coded_bills': 0,
            'coverage_pct': 0.0
        }

        renderer = CAPStatsRenderer(service=mock_service)
        metrics = renderer._get_summary_metrics()

        # Should not raise division by zero
        assert metrics['coverage_pct'] == 0.0
```

**Step 2: Run tests**

Run: `pytest tests/test_cap_renderers.py::TestCAPStatsRenderer -v`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/test_cap_renderers.py
git commit -m "$(cat <<'EOF'
test(cap): add tests for statistics renderer

- Test percentage formatting
- Test summary metrics calculation
- Test empty database handling (division by zero protection)

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: Run Full Test Suite

**Files:**
- None (verification only)

**Step 1: Run fast unit tests**

Run: `pytest tests/ --ignore=tests/test_api_integration.py --ignore=tests/test_e2e.py --ignore=tests/test_data_pipeline_integration.py --ignore=tests/test_connection_leaks.py --tb=short -q`
Expected: All PASS (400+ tests)

**Step 2: Check for any new failures**

Run: `pytest tests/ --ignore=tests/test_api_integration.py --ignore=tests/test_e2e.py --ignore=tests/test_data_pipeline_integration.py --ignore=tests/test_connection_leaks.py -v --tb=line | grep -E "(PASSED|FAILED|ERROR)"`
Expected: No FAILED or ERROR

**Step 3: Run CAP-specific tests**

Run: `pytest tests/test_cap_services.py tests/test_cap_integration.py tests/test_cap_renderers.py -v`
Expected: All PASS (90+ tests)

**Step 4: Commit test verification**

No commit needed - verification only.

---

## Task 14: Manual Streamlit Cloud Testing Checklist

**Files:**
- Create: `docs/STREAMLIT_QA_CHECKLIST.md`

**Step 1: Create manual testing checklist**

```markdown
# Streamlit Cloud QA Checklist

## Pre-Deployment Verification

- [ ] All secrets configured in Streamlit Cloud dashboard
- [ ] GCS credentials using base64 format
- [ ] Bootstrap admin password is secure (not default)
- [ ] Database uploaded to GCS bucket

## Authentication Flow

- [ ] Login page loads without errors
- [ ] Can select researcher from dropdown
- [ ] Correct password allows login
- [ ] Wrong password shows error (not crash)
- [ ] Session persists across page refreshes
- [ ] Session expires after 2 hours (test with modified time)
- [ ] Logout clears session completely
- [ ] Deactivated user cannot access after admin deactivation

## Annotation Workflow

- [ ] Bill queue loads with bills
- [ ] Search filter works (by name, by Knesset)
- [ ] PDF documents load and display
- [ ] PDF load failure shows error message (not blank)
- [ ] Category selector shows all major categories
- [ ] Minor categories filter based on major selection
- [ ] Save button saves annotation
- [ ] Success message appears after save
- [ ] Cloud sync status shown (success or warning)
- [ ] Bill removed from queue after annotation

## API Fetch Tab

- [ ] Search by name returns results
- [ ] Empty search shows "no results" message
- [ ] API timeout shows error (not empty results)
- [ ] Can annotate API-fetched bill

## Coded Bills Tab

- [ ] Shows previously annotated bills
- [ ] Can edit existing annotation
- [ ] Can delete annotation (with confirmation)
- [ ] Multi-annotator: see other researchers' annotations

## Statistics Tab

- [ ] Summary metrics display correctly
- [ ] Charts render without errors
- [ ] Coverage breakdown by Knesset works
- [ ] Zero annotations handled gracefully

## Admin Panel (Admin Users Only)

- [ ] User list displays all users
- [ ] Can add new researcher
- [ ] Duplicate username shows error
- [ ] Can reset user password
- [ ] Can change user role
- [ ] Can deactivate user
- [ ] Cannot delete self
- [ ] Cannot delete user with annotations

## Performance

- [ ] Initial load < 30 seconds
- [ ] Page navigation < 5 seconds
- [ ] Annotation save < 3 seconds
- [ ] No memory errors in logs

## Error Recovery

- [ ] App recovers from database connection error
- [ ] App recovers from GCS connection error
- [ ] Browser refresh doesn't lose session
- [ ] Multiple tabs work correctly
```

**Step 2: Commit checklist**

```bash
git add docs/STREAMLIT_QA_CHECKLIST.md
git commit -m "$(cat <<'EOF'
docs: add Streamlit Cloud QA testing checklist

- Pre-deployment verification steps
- Authentication flow tests
- Annotation workflow tests
- API fetch tab tests
- Admin panel tests
- Performance benchmarks
- Error recovery scenarios

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 15: Final Integration Test on Streamlit Cloud

**Files:**
- None (manual testing)

**Step 1: Deploy to Streamlit Cloud**

1. Push all changes to main branch
2. Verify app deploys successfully
3. Check logs for any startup errors

**Step 2: Run through QA checklist**

Follow `docs/STREAMLIT_QA_CHECKLIST.md` completely.

**Step 3: Document any issues found**

Create GitHub issues for any problems discovered during QA.

**Step 4: Verify data persistence**

1. Create test annotation
2. Wait for cloud sync
3. Force app restart (Settings → Reboot app)
4. Verify annotation persists

**Step 5: Test multi-researcher scenario**

1. Create two researcher accounts
2. Have both annotate same bill
3. Verify each sees other's annotations
4. Verify independent queues work correctly

---

## Summary

This plan contains **15 tasks** covering:

| Category | Tasks | Files Modified |
|----------|-------|----------------|
| Security | 1-2 | auth_handler.py, user_service.py |
| Error Handling | 3-5 | form_renderer.py, cap_api_service.py, pdf_viewer.py |
| Validation | 6, 9 | form_renderer.py, user_service.py |
| Configuration | 7-8 | upload_to_gcs.py, gcs_factory.py |
| Testing | 10-13 | test_cap_renderers.py |
| Documentation | 14 | STREAMLIT_QA_CHECKLIST.md |
| Verification | 15 | Manual testing |

**Estimated total: 90 bite-sized steps**

After completing all tasks, the platform will be hardened for researcher use with:
- ✅ Session timeout security
- ✅ Active user validation
- ✅ Clear error messages
- ✅ Cloud sync feedback
- ✅ Input validation
- ✅ Comprehensive test coverage
- ✅ QA checklist for verification

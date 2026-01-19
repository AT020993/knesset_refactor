# CLAUDE.md

Guidance for Claude Code when working with this Knesset parliamentary data analysis platform.

## Learning System (Auto-Check)

Before starting any debugging, error fixing, or complex task:
1. Automatically search `.claude/lessons/` for relevant lessons
2. Check `.claude/troubleshooting.md` for known errors
3. Apply past lessons to avoid repeating mistakes
Prioritize lessons with:
- 🔴 Critical severity
- High "failed attempts" count
- Matching error messages or tags

## Development Commands

```bash
# Fast unit tests (~10s)
pytest tests/ --ignore=tests/test_api_integration.py --ignore=tests/test_e2e.py --ignore=tests/test_data_pipeline_integration.py --ignore=tests/test_connection_leaks.py --tb=short -q

# Data refresh
PYTHONPATH="./src" python -m backend.fetch_table --all

# Launch app
streamlit run src/ui/data_refresh.py --server.port 8501
```

## Architecture

**Data Flow**: Knesset OData API → DuckDB/Parquet → Processing → Streamlit UI (21+ visualizations)

| Layer | Location | Purpose |
|-------|----------|---------|
| API | `src/api/` | Async OData client with circuit breaker |
| Data | `src/data/` | Repository pattern with DI |
| UI | `src/ui/` | Component-based Streamlit |
| Charts | `src/ui/charts/` | BaseChart-derived visualizations |
| Queries | `src/ui/queries/` | SQL templates and predefined queries |

## Connection Management

**Always use `get_db_connection()` context manager** - never use `duckdb.connect()` directly:
```python
# Correct - enables connection monitoring and leak detection
from backend.connection_manager import get_db_connection, safe_execute_query

with get_db_connection(db_path, read_only=False, logger_obj=self.logger) as conn:
    conn.execute("CREATE TABLE ...")

# Wrong - bypasses monitoring
conn = duckdb.connect(db_path)  # DON'T DO THIS
```

**Error Handling**: `ErrorCategory` enum is defined in `src/api/error_handling.py` (canonical location). Import from there, not from `config/api.py`.

## Bill Analytics Rules

### Status Categories (All Charts)
- 🔴 **Stopped**: All StatusIDs except those below
- 🔵 **First Reading**: StatusID 104,108,111,141,109,101,106,142,150,113,130,114
- 🟢 **Passed**: StatusID 118

### Critical Patterns

**Date-Based Faction Matching** (required for accuracy):
```sql
LEFT JOIN KNS_PersonToPosition ptp ON item.PersonID = ptp.PersonID
    AND item.KnessetNum = ptp.KnessetNum
    AND CAST(item.SubmitDate AS TIMESTAMP)
        BETWEEN CAST(ptp.StartDate AS TIMESTAMP)
        AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)
```

**Bill Submission Date**: Use `BillFirstSubmission` CTE (99.1% coverage) - finds MIN date from initiators, committee/plenum sessions, publication.

**Bill Origin Filter**: `PrivateNumber IS NOT NULL` = private, `IS NULL` = governmental

## Coalition Status

**Only Knesset 25 has coalition data** in `data/faction_coalition_status.csv`. Other Knessets show "Unknown" or "Unmapped".

To add more: Edit CSV → Run data refresh → Charts auto-update.

## CAP Annotation System

Multi-user bill classification system for democratic erosion research with role-based access control.

### Authentication & User Management

**Multi-User System**: Researchers authenticate with individual accounts (username/password). Passwords are hashed with bcrypt (cost factor 12).

**Roles**:
- `admin`: Full access including user management panel
- `researcher`: Can annotate bills, view statistics, export data

**Bootstrap Admin**: On first run, creates admin from `.streamlit/secrets.toml`:
```toml
[cap_annotation]
enabled = true
bootstrap_admin_username = "admin"
bootstrap_admin_display_name = "Administrator"
bootstrap_admin_password = "change-me-immediately"
```

**Session State Keys**:
- `cap_authenticated`: Boolean login status
- `cap_user_id`: Researcher's database ID
- `cap_user_role`: "admin" or "researcher"
- `cap_username`: Login username
- `cap_researcher_name`: Display name (shown in UI, stored in annotations)

### Database Tables

| Table | Purpose |
|-------|---------|
| `UserResearchers` | User accounts (ID, username, password hash, role, active status) |
| `UserCAPTaxonomy` | Category codes (Major/Minor topics with Hebrew/English labels) |
| `UserBillCAP` | Annotations (BillID, CAPMinorCode, Direction, AssignedBy, Notes) |

**Direction Values**: +1 = strengthens democracy, -1 = weakens, 0 = neutral

### Admin Panel Features

Accessible only to users with `role='admin'`:
- View all researchers with status (active/inactive, last login)
- Add new researchers with role assignment
- Edit display names
- Reset passwords
- Change roles (researcher ↔ admin)
- Deactivate users (soft delete - preserves annotations)
- Permanently delete users (only if no annotations exist)

### Key Files

| File | Purpose |
|------|---------|
| `src/ui/services/cap/user_service.py` | User CRUD, authentication, password hashing |
| `src/ui/renderers/cap/auth_handler.py` | Login form, session management |
| `src/ui/renderers/cap/admin_renderer.py` | Admin panel UI |

**Cache**: Call `_clear_query_cache()` after annotation changes to refresh predefined queries.

## Network Charts

4 collaboration visualizations in `src/ui/charts/network/`:

| Chart | Algorithm | Key Parameter |
|-------|-----------|---------------|
| MK Network | Weighted force-directed | 3+ bill threshold |
| Faction Network | Weighted force-directed | No threshold |
| Collaboration Matrix | Heatmap | Axes: Primary vs Supporting |
| Coalition Breakdown | Stacked bars | Coalition/Opposition split |

**Layout Formula**: Distance ∝ 1/collaboration_count (more collabs = closer nodes)

## SQL Templates

Reusable CTEs in `src/ui/queries/sql_templates.py`:
- `STANDARD_FACTION_LOOKUP` - Faction with ROW_NUMBER deduplication
- `BILL_FIRST_SUBMISSION` - Earliest bill activity date
- `BILL_STATUS_CASE_HE/EN` - Status categorization
- `QUERY_STATUS_CASE` - Query answer status

## Key File References

| Category | Files |
|----------|-------|
| **Charts** | `comparison.py`, `time_series.py`, `distribution.py`, `network/*.py` |
| **Base** | `base.py` (BaseChart, `@chart_error_handler`) |
| **Queries** | `predefined_queries.py`, `sql_templates.py` |
| **CAP** | `cap_annotation_page.py`, `cap_service.py`, `cap_api_service.py`, `user_service.py`, `admin_renderer.py` |
| **UI** | `plots_page.py`, `data_refresh_page.py`, `sidebar_components.py` |
| **Data Refresh** | `data_refresh_service.py`, `data_refresh_handler.py`, `odata_client.py` |
| **Data** | `data/faction_coalition_status.csv`, `data/taxonomies/*.csv` |
| **Config** | `src/config/database.py`, `src/config/api.py`, `src/backend/tables.py` |
| **Connection** | `src/backend/connection_manager.py` (get_db_connection context manager) |

## Streamlit Patterns

**Widget Selection Fix**: Use `on_change` callbacks, not post-render comparison:
```python
# Correct
st.selectbox(..., on_change=self._on_change, kwargs={...})

# Wrong (causes double-click bug)
if st.selectbox(...) != session_state_value:
    update()
```

**Tab State Persistence**: Use `st.radio` with session state instead of `st.tabs` (tabs don't persist across form submissions):
```python
# Correct - radio buttons with session state
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = options[0]
selected = st.radio("Nav", options, horizontal=True, key="tab_selector")
st.session_state.active_tab = selected

# Wrong - st.tabs resets to first tab after form submission
tab1, tab2 = st.tabs(["A", "B"])
```

**Avoid Redundant `st.rerun()`**: Form submissions and `on_click` callbacks auto-rerun. Extra `st.rerun()` calls can reset UI state.

**Double-Click Button Prevention**:
```python
# Wrong - causes double execution
if st.button("Action"):
    do_something()  # Updates session state
    st.rerun()  # REDUNDANT - button click already triggers rerun

# Correct - let Streamlit handle the rerun
if st.button("Action"):
    do_something()  # Updates session state
    # No st.rerun() needed - state is already updated for next render
```

**Disable Buttons During Long Operations**:
```python
# Correct - prevents double-clicks during async operations
is_running = st.session_state.get("operation_running", False)
if st.button("Start Operation", disabled=is_running):
    st.session_state.operation_running = True
    try:
        await long_operation()
    finally:
        st.session_state.operation_running = False
```

**Async Code in Streamlit** (🔴 Critical):

Streamlit uses Tornado with its own event loop. `asyncio.run()` fails with "This event loop is already running". Use thread isolation:
```python
def run_async_in_streamlit(async_func, *args):
    """Run async code in Streamlit context."""
    try:
        loop = asyncio.get_running_loop()
        # Streamlit context - run in separate thread
        import concurrent.futures

        def run_in_thread():
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                return new_loop.run_until_complete(async_func(*args))
            finally:
                new_loop.close()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            return executor.submit(run_in_thread).result(timeout=600)
    except RuntimeError:
        # CLI context - no running loop
        return asyncio.run(async_func(*args))
```

**tqdm in Streamlit Threads** (🔴 Critical):

`tqdm` causes `BrokenPipeError` when `sys.stderr` isn't connected (thread context). Use dummy progress bars:
```python
class _DummyProgressBar:
    def __init__(self, *args, **kwargs): pass
    def update(self, n=1): pass
    def __enter__(self): return self
    def __exit__(self, *args): pass

# Auto-detect context
if not sys.stderr.isatty():
    progress = _DummyProgressBar()
else:
    progress = tqdm(...)
```

**Long-Running Operations UI**: Use `st.sidebar.status()` instead of custom progress callbacks (thread-safe):
```python
with st.sidebar.status("Processing...", expanded=True) as status:
    result = long_operation()  # Runs in thread
    status.update(label="Complete!", state="complete")
```

## Test Status

328 passed, 26 skipped, 0 failures. Run fast tests before commits.

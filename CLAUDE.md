# CLAUDE.md

Guidance for Claude Code when working with this Knesset parliamentary data analysis platform.

## Quick Navigation

- [Development Commands](#development-commands) - Setup, tests, launch, git
- [Architecture](#architecture) - Data flow, layers
- [Connection Management](#connection-management) - Database access patterns
- [Error Handling Patterns](#error-handling-patterns) - Return tuples, fail-secure
- [DuckDB Patterns](#duckdb-patterns) - Sequences, migrations, gotchas
- [Bill Analytics Rules](#bill-analytics-rules) - Status categories, date matching
- [CAP Annotation System](#cap-annotation-system) - Summary → full docs in `src/ui/services/cap/`
- [Streamlit Cloud Deployment](#streamlit-cloud-deployment) - GCS, secrets
- [Streamlit Patterns](#streamlit-patterns) - Performance, widgets, async
- [Quick Troubleshooting](#quick-troubleshooting) - Common errors
- [Test Status](#test-status) - Current test coverage

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
# Setup (first time) - Requires Python 3.12+
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Fast unit tests (~10s)
pytest tests/ --ignore=tests/test_api_integration.py --ignore=tests/test_e2e.py --ignore=tests/test_data_pipeline_integration.py --ignore=tests/test_connection_leaks.py --tb=short -q

# Data refresh
PYTHONPATH="./src" python -m backend.fetch_table --all

# Launch app
streamlit run src/ui/data_refresh.py --server.port 8501
```

**Git Commit Messages** (🔴 Sandbox Limitation):

HEREDOC syntax fails in sandboxed environments with "can't create temp file". Use inline strings:
```bash
# Wrong - fails in sandbox
git commit -m "$(cat <<'EOF'
Message here
EOF
)"

# Correct - use inline multiline
git commit -m "feat: add feature

Details here.

Co-Authored-By: Claude <noreply@anthropic.com>"
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

## Error Handling Patterns

**Return Tuple Pattern** (for operations that can fail):

Use `(result, Optional[str])` tuples to distinguish success from errors with specific messages:
```python
# Returns (data, None) on success, (None, "error message") on failure
def fetch_data(self, id: int) -> tuple[Optional[dict], Optional[str]]:
    try:
        result = self._query(id)
        if not result:
            return None, "No data found for ID"
        return result, None
    except TimeoutError:
        return None, "Request timed out after 30 seconds"
    except ConnectionError:
        return None, "Network connection failed"

# Caller can show specific error to user
data, error = service.fetch_data(123)
if error:
    st.error(f"⚠️ {error}")
else:
    display(data)
```

**Fail-Secure Pattern**: On database errors, return False/inactive rather than raising:
```python
def is_user_active(self, user_id: int) -> bool:
    try:
        result = self._query(user_id)
        return result.get("IsActive", False)
    except Exception:
        return False  # Fail secure - treat errors as inactive
```

## DuckDB Patterns

**Auto-Increment Primary Keys** (🔴 Critical):

Unlike SQLite, DuckDB does NOT auto-increment `INTEGER PRIMARY KEY`. Use sequences:
```sql
-- Create sequence (idempotent)
CREATE SEQUENCE IF NOT EXISTS seq_my_id START 1;

-- Use in table definition
CREATE TABLE MyTable (
    ID INTEGER PRIMARY KEY DEFAULT nextval('seq_my_id'),
    ...
);

-- INSERT without ID - sequence handles it
INSERT INTO MyTable (Name, ...) VALUES (?, ...);
```

**🔴 FK Dependency Limitation**: DuckDB can't ALTER a column's DEFAULT when other tables have FK references to it. For existing tables with FK dependencies, explicitly use `nextval()` in INSERT:
```sql
-- When DEFAULT can't be added due to FK dependencies
INSERT INTO MyTable (ID, Name, ...) VALUES (nextval('seq_my_id'), ?, ...);
```

**Schema Migration Pattern** (safe table swap):
```python
# 1. Create sequence
conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_id START 1")

# 2. Create new table with proper schema
conn.execute("CREATE TABLE MyTable_new (...)")

# 3. Migrate data with JOINs for lookups
conn.execute("INSERT INTO MyTable_new (...) SELECT ... FROM MyTable old LEFT JOIN ...")

# 4. Verify count matches before swap
old_count = conn.execute("SELECT COUNT(*) FROM MyTable").fetchone()[0]
new_count = conn.execute("SELECT COUNT(*) FROM MyTable_new").fetchone()[0]
assert old_count == new_count

# 5. Swap tables
conn.execute("DROP TABLE MyTable")
conn.execute("ALTER TABLE MyTable_new RENAME TO MyTable")
```

**Division by Zero Protection** (for percentage calculations):
```sql
-- Wrong - can cause division by zero
ROUND(100.0 * coded_count / total_count, 1) AS coverage_pct

-- Correct - NULLIF returns NULL if divisor is 0, COALESCE converts to 0.0
COALESCE(
    ROUND(100.0 * coded_count / NULLIF(total_count, 0), 1),
    0.0
) AS coverage_pct
```

**🔴 Catalog Corruption from Interrupted Migrations**: If a migration is interrupted during `DROP TABLE X; ALTER TABLE X_new RENAME TO X`, DuckDB's internal constraint catalog can retain stale FK references to the non-existent `_new` table. **Fix**: Use `EXPORT DATABASE` + `IMPORT DATABASE` to completely rebuild the catalog:
```python
# Nuclear fix for corrupted catalog (stale FK references)
conn.execute(f"EXPORT DATABASE '{export_dir}' (FORMAT PARQUET)")
conn.close()
os.remove(db_path)  # Also remove .wal file if exists
conn = duckdb.connect(db_path)
conn.execute(f"IMPORT DATABASE '{export_dir}'")
```
**Warning**: Don't run EXPORT/IMPORT while other connections are active - causes "Can't open connection with different configuration" error. Admin Panel has "Full Catalog Rebuild" button for this.

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

Multi-user bill classification for democratic erosion research. **Full documentation**: [`src/ui/services/cap/CLAUDE.md`](src/ui/services/cap/CLAUDE.md)

**Quick Reference**:
- Roles: `admin` (full access) / `researcher` (annotate only)
- 🔴 Always use `cap_user_id` (int), not `cap_researcher_name` (string) for operations
- Tables: `UserResearchers`, `UserCAPTaxonomy`, `UserBillCAP`

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

## Streamlit Cloud Deployment

**GCS Credentials** (🔴 Critical): Streamlit Cloud's TOML editor breaks multi-line strings. Use base64 encoding:
```toml
[gcp_service_account]
credentials_base64 = "eyJ0eXBlIjoic2VydmljZV9hY2NvdW50Ii..."  # base64-encoded JSON
```

**Code reads it with**:
```python
if 'credentials_base64' in gcp_secrets:
    decoded = base64.b64decode(gcp_secrets['credentials_base64']).decode('utf-8')
    credentials = json.loads(decoded)
```

**Data Persistence**: Database files are gitignored. On Streamlit Cloud:
- Startup: Downloads `warehouse.duckdb` from GCS bucket
- After annotation: Auto-uploads database to GCS (see `_sync_to_cloud()` in renderers)
- Without GCS: Data lost on app reboot

**Upload local data to GCS**: `GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json" python upload_to_gcs.py`

**Sync Local ↔ Cloud Database**:

| Direction | Command |
|-----------|---------|
| Upload local → cloud | `GOOGLE_APPLICATION_CREDENTIALS="./iucc-international-dimensions-b86f1553b132.json" python upload_to_gcs.py` |
| Download cloud → local | `GOOGLE_APPLICATION_CREDENTIALS="./iucc-international-dimensions-b86f1553b132.json" python download_from_gcs.py` |

**🔴 Local vs Cloud are independent databases** - researchers/annotations added on Streamlit Cloud won't appear locally until you run `download_from_gcs.py`.

## Streamlit Patterns

**Lazy Loading for Performance** (Streamlit Cloud Free Tier):

The free tier has ~1GB RAM and shared CPU. The entire script re-runs on every interaction, so use section-based lazy loading:
```python
# Define sections with tab-like navigation
SECTIONS = ["📊 Data Explorer", "📈 Visualizations", "🏷️ CAP Annotation"]

# Store active section in session state
if "active_section" not in st.session_state:
    st.session_state.active_section = SECTIONS[0]

selected = st.radio("Navigate:", SECTIONS, horizontal=True, label_visibility="collapsed")
st.session_state.active_section = selected

# Only render the active section (skip 60-70% of code per interaction)
if selected == "📊 Data Explorer":
    # Store renderer in session_state to preserve internal caches
    if "data_renderer" not in st.session_state:
        st.session_state.data_renderer = DataRenderer(db_path)
    st.session_state.data_renderer.render()
elif selected == "📈 Visualizations":
    # ... same pattern
```

**Key principles:**
- Store renderers in `st.session_state` to preserve caches across reruns
- Use `@st.cache_data(ttl=3600)` for expensive computations
- Only instantiate components when their section is accessed

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

**Double-Click Button Prevention** (🔴 Critical):

`st.button()` and `st.form_submit_button()` automatically trigger reruns. Adding `st.rerun()` after them causes double-click issues.

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

**When st.rerun() IS needed**: Only after operations that don't naturally trigger reruns (e.g., after `st.cache_data.clear()` in a non-button context, or after programmatic state changes outside widget callbacks).

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

**Safe DataFrame Row Access** (for iterating over query results):
```python
# Wrong - fails if column doesn't exist
for _, row in df.iterrows():
    value = row["ColumnName"]  # KeyError if column missing

# Correct - use .get() with defaults and wrap in try/except
for _, row in df.iterrows():
    try:
        value = row.get("ColumnName", "default")
        other = row.get("OtherColumn", 0)
        # ... use values
    except Exception as e:
        logger.warning(f"Error processing row: {e}")
        continue
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

## Claude Code Automations

**Local config** (gitignored in `.claude/`):
- `.claude/settings.json` - Hooks: auto-format (black/isort), block .env edits
- `.claude/agents/code-reviewer.md` - Code review agent definition
- `.claude/skills/cap-annotate/` - CAP annotation utilities skill
- `.claude/reviews/` - Auto-generated code review results

**Post-commit code review**: `.git/hooks/post-commit` runs automatic code review after commits with Python changes. Results saved to `.claude/reviews/review_<commit>_<timestamp>.md`.

**MCP servers available**: context7 (docs), Linear (issues), Playwright (browser), Figma (design).

## Quick Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| Researchers added on cloud not visible locally | Local and cloud databases are independent | Run `download_from_gcs.py` with GCP credentials |
| `No module named 'bcrypt'` | bcrypt not in venv | `.venv/bin/pip install bcrypt` |
| `MalformedError: missing client_email` | GCS credentials TOML parsing failed | Use `credentials_base64` instead of multi-line private_key |
| `Table does not have column AssignedBy` | Old schema query | Use `ResearcherID` JOIN to `UserResearchers` |
| `NOT NULL constraint on ResearcherID` | Missing researcher_id in INSERT | Use `nextval('seq_researcher_id')` explicitly |
| `This event loop is already running` | asyncio.run() in Streamlit | Use thread isolation pattern (see Streamlit Patterns) |
| App sluggish on Streamlit Cloud | Free tier resource limits | Use lazy loading pattern (see Streamlit Patterns) |
| `Table with name *_new does not exist` | Interrupted migration left stale FK in catalog | Use Admin Panel → "Full Catalog Rebuild" or EXPORT/IMPORT DATABASE |
| `Can't open connection...different configuration` | Multiple connections with conflicting modes | Close all connections, reboot app, then retry |

## Test Status

453 passed, 26 skipped, 0 failures. Run fast tests before commits.

**CAP Tests** (144 total across `test_cap_services.py`, `test_cap_integration.py`, `test_cap_renderers.py`):
- Taxonomy service operations (5 tests)
- Repository CRUD with `researcher_id` (17 tests)
- Statistics with `COUNT(DISTINCT BillID)` (4 tests)
- Service facade delegation (4 tests)
- User service annotation counts (2 tests)
- Statistics edge cases: empty DB, division by zero, CSV export (3 tests)

Key behaviors tested:
- Multi-researcher same-bill annotation
- Upsert behavior (re-annotation updates, not duplicates)
- Researcher-specific uncoded bills queue
- `get_all_annotations_for_bill()` for inter-rater comparison
- Annotation count per bill (`AnnotationCount` column)
- `get_user_annotation_count()` uses `ResearcherID` (not legacy `AssignedBy`)
- Division by zero protection in coverage statistics
- CSV export with multi-annotator researcher info

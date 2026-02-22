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
- [Research Coding System](#research-coding-system) - Imported policy classifications
- [Streamlit Cloud Deployment](#streamlit-cloud-deployment) - GCS, secrets
- [Streamlit Patterns](#streamlit-patterns) - Performance, widgets, async
- [Quick Troubleshooting](#quick-troubleshooting) - Common errors
- [Security Patterns](#security-patterns) - SQL injection, validation, atomic writes
- [Documentation](#documentation) - Architecture, deployment, researcher guides
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
# Or with uv (faster): uv sync --all-extras

# ⚠️ PYTHONPATH: Most commands need PYTHONPATH="./src" (imports use bare module names like `from config.database import ...`)

# Fast unit tests (~10s, 576 tests)
PYTHONPATH="./src" pytest tests/ --ignore=tests/test_api_integration.py --ignore=tests/test_e2e.py --ignore=tests/test_data_pipeline_integration.py --ignore=tests/test_connection_leaks.py --tb=short -q

# Data refresh
PYTHONPATH="./src" python -m backend.fetch_table --all

# Launch app
streamlit run src/ui/data_refresh.py --server.port 8501

# Launch (alternative scripts)
python launch_knesset.py          # Full app launcher
python researcher_launcher.py     # Researcher-focused launcher
./start-knesset.sh                # Shell script launcher

# Diagnostics
python diagnose_db.py             # Database health check
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

**Data Flow**: Knesset OData API → DuckDB/Parquet → Processing → Streamlit UI (12 chart classes)

| Layer | Location | Purpose |
|-------|----------|---------|
| API | `src/api/` | Async OData client with circuit breaker |
| Core | `src/core/` | Dependency injection (`dependencies.py`) |
| Data | `src/data/` | Repository pattern with DI |
| Storage | `src/data/storage/` | Cloud storage, credential resolution |
| Backend | `src/backend/` | Connection manager, table configs, data fetch |
| Config | `src/config/` | Database, API, settings, table config |
| Utils | `src/utils/` | Validators, resolvers, exporters, layout helpers |
| UI | `src/ui/` | Component-based Streamlit |
| State | `src/ui/state/` | Session manager, typed contracts (`state_contracts.py`), state ops |
| Sidebar | `src/ui/sidebar/` | Sidebar components, refresh/query/explorer handlers |
| Renderers | `src/ui/renderers/` | Page facades with extracted `*_ops.py` (plots/, data_refresh/, cap/) |
| Charts | `src/ui/charts/` | BaseChart-derived with mixins (comparison/, network/) |
| Queries | `src/ui/queries/` | SQL templates, query packs (`packs/`), types, executor |

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

**Use `read_only=True` for visualizations**: Charts and read operations should always use `read_only=True` to improve concurrency and prevent accidental modifications:
```python
# Correct - for charts and queries
with get_db_connection(db_path, read_only=True, logger_obj=self.logger) as conn:
    df = safe_execute_query(conn, "SELECT * FROM ...")

# Only use read_only=False when actually writing
with get_db_connection(db_path, read_only=False, logger_obj=self.logger) as conn:
    conn.execute("INSERT INTO ...")
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

**K1-25 coalition data** in `data/faction_coalition_status_all_knessets.csv` (385 rows). K21-22 show "Unknown" (interim Knessets).
Original K25-only backup preserved in `data/faction_coalition_status.csv`.

Mid-term coalition changes stored in `DateJoinedCoalition`/`DateLeftCoalition` columns.
To update: Edit CSV or re-run `import_coalition_data.py` → reload via `load_faction_coalition_status()`.

**Faction Name Display** (🔴 Critical): Always prefer `NewFactionName` from `UserFactionCoalitionStatus`:
```sql
COALESCE(ufs.NewFactionName, f.Name, 'Unknown') AS FactionName
```
This pattern is used in ~15 files (charts, queries, network, exporters, UI filters). See `src/utils/faction_resolver.py:get_faction_name_field()`.

**Coalition Status Fallback**: Always use `'Unknown'` as the fallback — never `'Unmapped'` or empty string. Applied via `COALESCE(ufs.CoalitionStatus, 'Unknown')`.

## CAP Annotation System

Multi-user bill classification for democratic erosion research. **Full documentation**: [`src/ui/services/cap/CLAUDE.md`](src/ui/services/cap/CLAUDE.md)

**Quick Reference**:
- Roles: `admin` (full access) / `researcher` (annotate only)
- 🔴 Always use `cap_user_id` (int), not `cap_researcher_name` (string) for operations
- Tables: `UserResearchers`, `UserCAPTaxonomy`, `UserBillCAP`

**Password Requirements**:
- Minimum 8 characters
- At least one uppercase letter
- At least one lowercase letter
- At least one digit

**Rate Limiting**:
- 5 failed login attempts → 15-minute account lockout
- Lockout is per-username, tracked in session state

## Research Coding System

Imported policy classification data (MajorIL, MinorIL, CAP codes, Religion, Territories) from researcher-provided files.

**Tables**: `UserBillCoding`, `UserQueryCoding`, `UserAgendaCoding`

**Import service**: `src/utils/research_coding_importer.py`
- Bulk upsert via DuckDB `register()` + `INSERT ... ON CONFLICT`
- Case-insensitive column mapping (handles UPPERCASE, mixedCase, lowercase source files)
- Sentinel value cleaning (`-99` → NULL)
- Agenda matching: K19-20 by `id2` column, K23-24 by 3-tier title matching (exact → normalized → fuzzy)

**CLI import**: `import_research_coding.py` — bulk import with gap analysis output
```bash
PYTHONPATH="./src" python import_research_coding.py \
  --bills path/to/bills.xlsx \
  --queries path/to/queries.xlsx \
  --agendas path/to/agendas.xlsx
```

**UI**: "📥 Research Coding" tab in main app (`src/ui/renderers/research_coding_page.py`)

**Predefined queries**: All 3 queries include LEFT JOINs to coding tables, adding `Coding*` columns to exports.

**Known data gaps**: Knesset OData API has no queries for K10-K18 (1981-2013). See `data/gap_analysis/IMPORT_SUMMARY.md`.

## Plotly Chart Patterns

**Categorical axes with numeric values**: Plotly auto-skips tick labels and sorts lexicographically ("1", "10", "2") when y-values are string-typed numbers. Fix:
```python
# Sort numerically, then convert to string for categorical axis
topic_order = sorted(df["TopicCode"].unique())  # int sort
df["TopicCode"] = df["TopicCode"].astype(str)
fig.update_layout(yaxis=dict(type="category", dtick=1))
```

**New chart registration** requires 3 files: chart class method in `src/ui/charts/<category>.py` → `factory.py` available charts list → `plot_generators.py` legacy wrapper + `get_available_plots()` registry.

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

## Query Execution Pipeline

**`execute_query_with_filters()` returns 4-tuple**: `(df, sql, filters, params)` — callers must unpack all four.

**Params travel with SQL**: Filter `?` placeholders in stored SQL require `last_query_params` from session state for re-execution. `DatasetExporter` uses both to fetch full datasets. LIMIT/OFFSET are inlined as integers (safe, code-derived) so the exporter's regex can strip them.

## Key File References

| Category | Files |
|----------|-------|
| **Charts** | `src/ui/charts/comparison/`, `time_series.py`, `distribution.py`, `network/` |
| **Chart Base** | `src/ui/charts/base.py` (BaseChart, `@chart_error_handler`), `mixins/` |
| **Queries** | `src/ui/queries/predefined_queries.py` (facade: `get_all_query_names()`, `get_query_definition(name)`), `packs/` (bills, agenda, parliamentary, registry), `types.py`, `sql_templates.py`, `query_executor.py` (`_strip_table_alias()`) |
| **CAP Services** | `src/ui/services/cap/` — facades: `cap_service.py`, `user_service.py`, `repository.py`, `taxonomy.py`; ops: `user_service_*_ops.py`, `repository_*_ops.py`, `taxonomy_migration_ops.py` |
| **CAP Renderers** | `src/ui/renderers/cap/` (form_renderer.py, admin_renderer.py → `admin_maintenance_ops.py`, auth_handler.py, bill_queue_renderer.py) |
| **UI Renderers** | `src/ui/renderers/plots_page.py` → `plots/generation_ops.py`, `plots/selection_ops.py`; `data_refresh/page.py` → `data_refresh/query_results_ops.py`; `cap_annotation_page.py`, `research_coding_page.py` |
| **Dataset Export** | `src/ui/renderers/data_refresh/dataset_exporter.py` (full dataset download with param passthrough) |
| **Research Coding** | `src/utils/research_coding_importer.py`, `import_research_coding.py` |
| **Gov Bill Import** | `import_government_bills.py` — K10-20 + K23-24 government bill coding into UserBillCoding |
| **Coalition Import** | `import_coalition_data.py` — merge researcher CSVs → `data/faction_coalition_status_all_knessets.csv` |
| **Sidebar** | `src/ui/sidebar/components.py`, `data_refresh_handler.py`, `query_handler.py` |
| **Data Sync** | `src/data/services/storage_sync_service.py` (facade) → `storage_sync_metadata_ops.py`, `storage_sync_transfer_ops.py`, `storage_sync_startup_ops.py`, `sync_types.py`, `sync_data_refresh_service.py` |
| **Cloud Storage** | `src/data/storage/cloud_storage.py` (facade) → `cloud_storage_ops.py`, `credential_resolver.py` |
| **Data Refresh** | `src/data/services/data_refresh_service.py`, `src/api/odata_client.py` |
| **State** | `src/ui/state/session_manager.py`, `state_contracts.py`, `state_ops.py` |
| **Data** | `data/faction_coalition_status_all_knessets.csv`, `data/faction_coalition_status.csv` (K25 backup), `data/taxonomies/democratic_erosion_codebook.csv` |
| **Config** | `src/config/database.py`, `src/config/api.py`, `src/backend/tables.py` |
| **Connection** | `src/backend/connection_manager.py` (get_db_connection context manager) |
| **Performance** | `src/utils/performance_utils.py` (`optimize_dataframe_dtypes()`, `reduce_plotly_figure_size()`) |
| **CI** | `.github/workflows/ci.yml` — jobs: `quality`, `unit-tests`, `cloud-compat` (uv sync), `e2e-tests`, `summary` |
| **Launchers** | `launch_knesset.py`, `researcher_launcher.py`, `start-knesset.sh` |

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

**Upload local data to GCS**: `GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json" python upload_to_gcs.py --bucket knesset_bucket`

**🔴 GCS scripts must run WITHOUT `PYTHONPATH="./src"`** — `src/requests.py` shadows the `requests` package, breaking `google.auth.transport`.

**Sync Local ↔ Cloud Database**:

| Direction | Command |
|-----------|---------|
| Upload local → cloud | `GOOGLE_APPLICATION_CREDENTIALS="./iucc-international-dimensions-b86f1553b132.json" python upload_to_gcs.py --bucket knesset_bucket` |
| Download cloud → local | `GOOGLE_APPLICATION_CREDENTIALS="./iucc-international-dimensions-b86f1553b132.json" python download_from_gcs.py --bucket knesset_bucket` |

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
- Use `@st.cache_data(ttl=...)` for expensive computations (chart queries: 30min, annotation counts: 10min, filter options: 1hr)
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

**When st.rerun() IS needed**: Only after operations that don't naturally trigger reruns (e.g., after programmatic state changes outside widget callbacks).

**Targeted Cache Invalidation** (🔴 Important):

Don't use `st.cache_data.clear()` — it wipes ALL caches (charts, filters, queries). Use targeted invalidation:
```python
# Wrong - nuclear clear wipes unrelated chart/filter caches
st.cache_data.clear()

# Correct - clear only the affected cache
from ui.services.cap.repository_cache_ops import clear_annotation_counts_cache
clear_annotation_counts_cache()
```
Only `data_refresh_handler.py` retains nuclear `st.cache_data.clear()` for when underlying data actually changes.

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

## Documentation

| Document | Purpose |
|----------|---------|
| `ARCHITECTURE.md` | System architecture overview |
| `ARCHITECTURE_DIAGRAM.md` | Visual architecture diagrams |
| `DEPLOYMENT_GUIDE.md` | Deployment instructions |
| `DEPLOYMENT_OPTIONS.md` | Deployment platform comparison |
| `RESEARCHER_GUIDE.md` | Guide for researchers using CAP system |
| `PERFORMANCE_OPTIMIZATIONS.md` | Performance tuning details |
| `docs/CAP_ANNOTATION_GUIDE.md` | CAP annotation workflow guide |
| `docs/FULL_DATASET_DOWNLOAD.md` | Full dataset download instructions |

## Claude Code Automations

**Local config** (gitignored in `.claude/`):
- `.claude/settings.json` - Hooks: auto-format (black/isort), block .env edits
- `.claude/agents/code-reviewer.md` - Code review agent definition
- `.claude/skills/cap-annotate/` - CAP annotation utilities skill
- `.claude/reviews/` - Auto-generated code review results

**Post-commit code review**: `.git/hooks/post-commit` runs automatic code review after commits with Python changes. Results saved to `.claude/reviews/review_<commit>_<timestamp>.md`.

**MCP servers available**: context7 (docs), Linear (issues), Playwright (browser), Figma (design).

## Security Patterns

**SQL Injection Prevention** (🔴 Critical):

Always use parameterized queries, never string interpolation for user input:
```python
# Wrong - SQL injection vulnerable
query = f"SELECT * FROM users WHERE name = '{user_input}'"

# Correct - parameterized query
query = "SELECT * FROM users WHERE name = ?"
conn.execute(query, [user_input])
```

**Credential Validation**:

GCS credentials are validated on initialization (`cloud_storage.py`):
```python
REQUIRED_CREDENTIAL_FIELDS = {'type', 'project_id', 'private_key', 'client_email'}
# Missing fields raise ValueError with clear message
```

**Atomic File Writes** (for state files):

Use temp file + rename pattern to prevent corruption from crashes:
```python
import tempfile
from pathlib import Path

# Write to temp file first
temp_fd, temp_path = tempfile.mkstemp(dir=file.parent, prefix=".state_", suffix=".tmp")
try:
    with open(temp_fd, 'w') as f:
        json.dump(data, f)
    Path(temp_path).replace(file)  # Atomic rename
except Exception:
    Path(temp_path).unlink(missing_ok=True)
    raise
```

**Cloud Sync Locking**:

File locking prevents concurrent annotation uploads from overwriting each other (`form_renderer.py`):
```python
lock_file = db_path.with_suffix('.lock')
if lock_file.exists() and (time.time() - lock_file.stat().st_mtime) < 60:
    return  # Another sync in progress
```

**Connection Close Safety**:

Always wrap connection close in try/except to prevent masking original exceptions:
```python
finally:
    if conn:
        try:
            conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")
```

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
| `Account temporarily locked` | Too many failed login attempts | Wait 15 minutes or clear session state |
| `Password must contain...` | New password complexity requirements | Use 8+ chars with uppercase, lowercase, and digit |
| `IndexError: iloc[-1]` on empty DataFrame | API returned no data | Check filters, add `if df.empty:` guard |
| Widget key collision errors | Duplicate Streamlit widget keys | Use unique prefix like `f"filter_{id(self)}_..."` |
| `ModuleNotFoundError: No module named 'config'` | Missing PYTHONPATH | Prefix command with `PYTHONPATH="./src"` |
| `No module named 'requests.adapters'` | `src/requests.py` shadows `requests` package | Run GCS scripts **without** `PYTHONPATH="./src"` |
| Untracked `.xlsx` files at project root | Research coding source data | Don't commit — used by `import_research_coding.py`, not needed in repo |
| `Values were not provided for...prepared statement parameters` on full dataset download | Stored SQL has `?` placeholders but params weren't passed to exporter | Ensure `last_query_params` is stored in session state and passed through `DatasetExporter` methods |
| Chart code changes not visible after edit | `@st.cache_resource` caches chart generator instances across hot-reloads | Restart Streamlit server (`Ctrl+C` + relaunch) |

## Test Status

Run fast tests before commits.

**Known Failing Test**: `test_cli.py::test_refresh_specific_table` — mock assertion mismatch (pre-existing, not a regression).

**Cloud Compatibility Tests** (55 tests across 3 files):
- `test_cloud_compatibility.py`: 34 unit tests covering credential loading, storage ops, async patterns, database persistence, session state, resource constraints
- `test_cloud_integration.py`: 10 integration tests for deployment scenarios, secrets configuration, concurrent access
- `test_cloud_constraints.py`: 11 constraint tests catching: module shadowing, dep drift (requirements.txt vs pyproject.toml), SQL alias scope, unsafe asyncio.run(), unbound-in-finally
- Fixtures in `tests/fixtures/cloud_fixtures.py`: mocked secrets, GCS client, Streamlit/CLI contexts, session state
- See `tests/README_CLOUD_TESTS.md` for full documentation

**CAP Tests** (across `test_cap_services.py`, `test_cap_integration.py`, `test_cap_renderers.py`):
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
- Password complexity validation (uppercase, lowercase, digit)
- Rate limiting and account lockout

**Test Passwords**: Tests use passwords like `Password1`, `TestPass1` that meet complexity requirements (8+ chars, upper, lower, digit).

**Refactor Tests** (new in this refactor):
- `test_chart_refactor_completion.py`: Chart factory coverage, legacy wrapper delegation, date filter wiring
- `test_query_builder.py`: Query builder unit tests
- `tests/fixtures/common_fixtures.py`: Shared test fixtures extracted from conftest
- `tests/fixtures/runtime_fixtures.py`: Runtime-specific fixtures

**Research Coding Tests** (29 tests in `test_research_coding_importer.py`):
- Table creation and idempotency (2 tests)
- File reading: CSV, Excel, missing file, unsupported format (4 tests)
- Bill import: full import, updates, missing columns (3 tests)
- Query import: full import, sentinel value cleaning (2 tests)
- Agenda import: ID matching, title matching, unmatched items (3 tests)
- Text normalization: whitespace, punctuation, Hebrew, Unicode NFC (4 tests)
- Gap analysis: empty and populated states (3 tests)
- Statistics and clear operations (3 tests)
- Edge cases: empty files, duplicate IDs, mixed case columns, NaN values (5 tests)

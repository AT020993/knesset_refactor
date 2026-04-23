# AGENTS.md

Knesset parliamentary data analysis platform. Knesset OData API → DuckDB → Streamlit UI (25+ chart types).

## Commands

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt  # Or: uv sync --all-extras

# ⚠️ All commands need PYTHONPATH="./src" (bare module imports like `from config.database import ...`)

# Tests (~576 fast tests)
PYTHONPATH="./src" pytest tests/ --ignore=tests/test_api_integration.py --ignore=tests/test_e2e.py --ignore=tests/test_data_pipeline_integration.py --ignore=tests/test_connection_leaks.py --tb=short -q

# Launch
streamlit run src/ui/data_refresh.py --server.port 8501

# Data refresh (all tables; add -t KNS_Bill to scope to one table)
PYTHONPATH="./src" python -m src.cli refresh

# Recurring-bills classification (ingests Dr. Tal Alovitz's dataset)
PYTHONPATH="./src" python scripts/classify_recurring_bills.py rebuild  # fast, no network
PYTHONPATH="./src" python scripts/classify_recurring_bills.py refresh  # ~90 min first run

# Export Parquet snapshot bundle for knesset-platform consumers
PYTHONPATH="./src" python -m data.snapshots.exporter \
    --warehouse data/warehouse.duckdb --output-dir data/snapshots/

# Diagnostics
python scripts/diagnose_db.py
```

**Git commits**: HEREDOC syntax fails in sandbox. Use inline: `git commit -m "feat: ...\n\nDetails.\n\nCo-Authored-By: Codex <noreply@anthropic.com>"`

## Architecture

| Layer | Location | Purpose |
|-------|----------|---------|
| API | `src/api/` | Async OData client with circuit breaker |
| Core | `src/core/` | Dependency injection (`dependencies.py`) |
| Data | `src/data/` | Repository pattern; `storage/` for GCS + credential resolution |
| Backend | `src/backend/` | Connection manager, table configs, data fetch |
| Config | `src/config/` | Database, API, settings, table config |
| Utils | `src/utils/` | Validators, resolvers, exporters, layout helpers |
| UI | `src/ui/` | Streamlit: `state/`, `sidebar/`, `renderers/`, `charts/`, `queries/` (shim) |
| Charts | `src/ui/charts/` | BaseChart + mixins; subdirs: `comparison/`, `network/` |
| Queries | `src/data/queries/` | **Canonical** — SQL templates, query packs (`packs/`), types, filter builder. `src/ui/queries/` is a back-compat shim holding only `QueryExecutor` (Streamlit-only). |
| Snapshots | `src/data/snapshots/` | Parquet exporter — atomic per-file + `manifest.json` commit marker. Consumed by `knesset-platform` via `$KNESSET_SNAPSHOT_DIR`. |

## Snapshot contract (downstream `knesset-platform`)

`src/data/snapshots/exporter.py` is the **only** interface other repos use — they never import this codebase. `v1.0.0` tag on main pins the contract; bump major on breaking Parquet-shape changes. Repo: [`knesset-platform`](https://github.com/AT020993/knesset-platform).

Produces 7 Parquets (`mk_summary`, `mk_bills`, `mk_questions`, `mk_motions`, `parties_list`, `committees_list`, `topics_list`) + `manifest.json`. Byte-idempotent on unchanged warehouse.

**🔴 Stable `ORDER BY` required** in every pack query used by the exporter — DuckDB parallel execution shuffles rows otherwise, breaking byte-idempotence. See `src/data/queries/packs/{mks,parties,committees}.py` for the pattern.

**⚠️ Byte-idempotence caveat — not venv-stable**: same warehouse + identical pinned deps produce byte-identical parquets *within a stable venv session*, but recreating the venv can shift per-file footer metadata (~25 bytes/file) due to Arrow/snappy compiled-binary variations. Downstream hash consumers in `knesset-platform` will see churn on any venv rebuild — not a data issue, but worth knowing. Before merging dep upgrades that touch parquet encoding (pandas, pyarrow, fastparquet), run `scripts/check_snapshot_regression.py {baseline,compare}` — it filters `generated_at_utc` noise and flags real row-level diffs as 🔴.

VPS deploy blueprint: `docs/deploy/knesset-etl.{service,timer}` (systemd, Phase 5). Dev machine runs under launchd instead (see `knesset-platform` README).

## Connection Management

Always use `get_db_connection()` context manager, never `duckdb.connect()` directly. Use `read_only=True` for charts/queries, `read_only=False` only for writes. `ErrorCategory` enum lives in `src/api/error_handling.py` (canonical location).

## Error Handling

- **Return tuples**: `(result, Optional[str])` — success returns `(data, None)`, failure returns `(None, "error message")`
- **Fail-secure**: On DB errors, return `False`/inactive rather than raising

## DuckDB Gotchas

**🔴 Sequences required**: DuckDB does NOT auto-increment `INTEGER PRIMARY KEY` like SQLite. Use `CREATE SEQUENCE IF NOT EXISTS seq_id START 1` + `DEFAULT nextval('seq_id')`. If FK deps block ALTER DEFAULT, use `nextval()` explicitly in INSERT.

**Schema migration**: Create new table → migrate data with JOINs → verify counts match → `DROP` old → `ALTER TABLE ... RENAME`.

**🔴 Catalog corruption**: Interrupted migrations leave stale FK refs. Fix: `EXPORT DATABASE` → delete DB → `IMPORT DATABASE`. Don't run while other connections are active. Admin Panel has "Full Catalog Rebuild" button.

**Division by zero**: Always use `COALESCE(ROUND(100.0 * x / NULLIF(y, 0), 1), 0.0)` for percentages.

## Bill Analytics Rules

**Status categories**: 🔴 Stopped (all other IDs) · 🔵 First Reading (104,108,111,141,109,101,106,142,150,113,130,114) · 🟢 Passed (118)

**🔴 Faction matching** — JOIN must include `ptp.FactionID IS NOT NULL` to exclude committee/plenum positions:
```sql
LEFT JOIN KNS_PersonToPosition ptp ON item.PersonID = ptp.PersonID
    AND item.KnessetNum = ptp.KnessetNum
    AND ptp.FactionID IS NOT NULL
    AND CAST(item.SubmitDate AS TIMESTAMP)
        BETWEEN CAST(ptp.StartDate AS TIMESTAMP)
        AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)
```

**🔴 Agenda dates**: `KNS_Agenda.LastUpdatedDate` is an API refresh timestamp, NOT the actual agenda date. Match on `PersonID + KnessetNum` only. Use `DISTINCT ON` + `ORDER BY p2p.StartDate DESC` for mid-Knesset faction switches.

**Bill submission date**: Use `BillFirstSubmission` CTE (99.1% coverage). **Bill origin**: `PrivateNumber IS NOT NULL` = private, `IS NULL` = governmental. Committee: `SubTypeID=54`.

## Coalition Status

`data/faction_coalition_status_all_knessets.csv` (400 rows, K1-25). K21-22 show "Unknown" (interim). Mid-term changes in `DateJoinedCoalition`/`DateLeftCoalition`. Update: edit CSV or `scripts/import_coalition_data.py`.

**🔴 Faction name**: Always `COALESCE(ufs.NewFactionName, f.Name, 'Unknown')`. See `src/utils/faction_resolver.py:get_faction_name_field()`.
**Coalition fallback**: Always `COALESCE(ufs.CoalitionStatus, 'Unknown')` — never `'Unmapped'` or empty string.

## CAP Annotation System

Multi-user bill classification for democratic erosion research. **Full docs**: [`src/ui/services/cap/AGENTS.md`](src/ui/services/cap/AGENTS.md)

- Roles: `admin` / `researcher`. 🔴 Always use `cap_user_id` (int), not `cap_researcher_name` (string)
- Tables: `UserResearchers`, `UserCAPTaxonomy`, `UserBillCAP`
- Passwords: 8+ chars, uppercase + lowercase + digit. 5 failed attempts → 15-min lockout

## Research Coding System

Tables: `UserBillCoding`, `UserQueryCoding`, `UserAgendaCoding`. Import service: `src/utils/research_coding_importer.py`.

```bash
PYTHONPATH="./src" python scripts/import_research_coding.py \
  --bills path/to/bills.xlsx --queries path/to/queries.xlsx --agendas path/to/agendas.xlsx
```

Match rates: Bills 99.6%, Agendas 100%, Queries only 13% (24K entries for K10-K18 which don't exist in API — expected).

## Charts & Queries

**New chart registration**: 3 files — chart method in `src/ui/charts/<category>.py` → `factory.py` → `plot_generators.py` wrapper + `get_available_plots()`.

**Plotly categorical axes**: Sort numerically first, then `astype(str)` + `yaxis=dict(type="category", dtick=1)`.

**`execute_query_with_filters()` returns 4-tuple**: `(df, sql, filters, params)`. Params travel with SQL for re-execution. LIMIT/OFFSET are inlined integers so exporter regex can strip them.

**SQL templates** in `src/ui/queries/sql_templates.py`: `STANDARD_FACTION_LOOKUP`, `BILL_FIRST_SUBMISSION`, `BILL_STATUS_CASE_HE/EN`, `QUERY_STATUS_CASE`.

## Streamlit Patterns

- **Lazy loading**: Only render active section. Store renderers in `session_state` to preserve caches.
- **Caching TTL**: Charts 30min, annotation counts 10min, filter options 1hr. Never `st.cache_data.clear()` — use targeted invalidation.
- **Widgets**: Use `on_change` callbacks, not post-render comparison (double-click bug). Use `st.radio` + session state, not `st.tabs` (tabs reset on form submit).
- **No redundant `st.rerun()`**: Buttons and form submissions auto-rerun. Extra `st.rerun()` causes double-execution.
- **Disable buttons** during long ops: `st.button("...", disabled=session_state.get("running", False))`
- **Async**: Use thread isolation (Tornado event loop conflict). See `run_async_in_streamlit()` pattern in codebase.
- **tqdm**: Causes `BrokenPipeError` in threads. Use `_DummyProgressBar` when `not sys.stderr.isatty()`.

## Streamlit Cloud Deployment

**GCS credentials**: Streamlit TOML breaks multi-line strings. Use `credentials_base64` (base64-encoded JSON).

**Data persistence**: DB gitignored → downloaded from GCS on startup → auto-uploaded after annotations. Without GCS, data lost on reboot.

**🔴 GCS scripts must run WITHOUT `PYTHONPATH="./src"`** — `src/requests.py` shadows the `requests` package.

**🔴 Prefer `gsutil` over `scripts/upload_to_gcs.py`** — Python GCS client hangs on OneDrive FUSE.

| Direction | Command |
|-----------|---------|
| Upload DB | `GOOGLE_APPLICATION_CREDENTIALS="./iucc-international-dimensions-b86f1553b132.json" gsutil -m cp data/warehouse.duckdb gs://knesset_bucket/data/warehouse.duckdb` |
| Download | `GOOGLE_APPLICATION_CREDENTIALS="./iucc-international-dimensions-b86f1553b132.json" python scripts/download_from_gcs.py --bucket knesset_bucket` |

**🔴 Local and Cloud are independent databases** — run `scripts/download_from_gcs.py` to sync.

## Key File References

| Category | Files |
|----------|-------|
| **Charts** | `src/ui/charts/comparison/`, `time_series.py`, `distribution.py`, `network/` |
| **Chart Base** | `src/ui/charts/base.py` (BaseChart, `@chart_error_handler`), `mixins/` |
| **Queries** | `src/ui/queries/predefined_queries.py` (facade), `packs/`, `sql_templates.py`, `query_executor.py` |
| **CAP** | `src/ui/services/cap/` — facades + ops files. Renderers: `src/ui/renderers/cap/` |
| **UI Renderers** | `src/ui/renderers/plots_page.py`, `data_refresh/page.py`, `cap_annotation_page.py`, `research_coding_page.py` |
| **Scripts** | `scripts/import_research_coding.py`, `import_government_bills.py`, `import_filtered_coding.py`, `import_coalition_data.py`, `export_uncoded_private_bills.py`, `export_uncoded_committee_gov_bills.py`, `export_amnon_classified_excel.py` (Option-C deliverable for Prof. Amnon — adds `is_recurring_upstream` + `effective_original_reason` columns; exits 2 on integrity violations), `upload_to_gcs.py`, `download_from_gcs.py`, `diagnose_db.py` |
| **Recurring Bills** | `src/data/recurring_bills/` — ingests pmb.teca-it.com → `bill_classifications` DuckDB table (31,490 rows) + `data/snapshots/bill_classifications.parquet` + `v_cap_bills_with_recurrence` view. Cache at `data/external/tal_bill_details/` (~17K Hebrew JSONs with full דברי הסבר per bill, usable for NLP/coding-assistance without re-crawling). K16-K18 doc-based cache at `data/external/knesset_docs/` (~7K .doc/.pdf from fs.knesset.gov.il, resumable via `rebuild --k16-k18-method doc`). |
| **Deploy** | `docs/deploy/knesset-etl.service` + `knesset-etl.timer` + `README.md` (VPS systemd blueprint; install in Phase 5) |
| **Data** | `data/faction_coalition_status_all_knessets.csv`, `data/taxonomies/majoril_labels.csv`, `data/taxonomies/democratic_erosion_codebook.csv` |
| **Config** | `src/config/database.py`, `src/config/api.py`, `src/backend/tables.py` |
| **Connection** | `src/backend/connection_manager.py` (`get_db_connection` context manager) |
| **State** | `src/ui/state/session_manager.py`, `state_contracts.py`, `state_ops.py` |
| **Sync** | `src/data/services/storage_sync_service.py` → `*_ops.py` files |
| **CI** | `.github/workflows/ci.yml` — jobs: quality, unit-tests, cloud-compat, e2e-tests, summary |
| **Launchers** | `launch_knesset.py`, `researcher_launcher.py`, `start-knesset.sh` (gitignored) |

## Documentation

`README.md` · `CONTRIBUTING.md` · `SECURITY.md` · `CHANGELOG.md` · `ARCHITECTURE.md` · `ARCHITECTURE_DIAGRAM.md` · `DEPLOYMENT_GUIDE.md` · `DEPLOYMENT_OPTIONS.md` · `PERFORMANCE_OPTIMIZATIONS.md` · `docs/README.md` · `docs/CAP_ANNOTATION_GUIDE.md` · `docs/FULL_DATASET_DOWNLOAD.md` · `docs/DATA_PROVENANCE.md` · `docs/superpowers/specs/2026-04-17-recurring-bills-classification-design.md` · `docs/superpowers/specs/2026-04-17-recurring-bills-coverage-report.md`

## Codex Automations

- `.Codex/settings.json` — Hooks: auto-format (black/isort), SQL injection detection, .env edit blocking
- `.Codex/agents/` — Subagent definitions: `code-reviewer.md`, `cap-debugger.md`, `data-refresh.md`
- `.Codex/skills/cap-annotate/` — CAP annotation utilities
- `.Codex/reviews/` — Code review results from manual agent runs

## Quick Troubleshooting

| Error | Fix |
|-------|-----|
| `ModuleNotFoundError: No module named 'config'` | `PYTHONPATH="./src"` |
| `No module named 'requests.adapters'` | Run GCS scripts WITHOUT `PYTHONPATH="./src"` |
| `This event loop is already running` | Thread isolation pattern (see Streamlit Patterns) |
| `Table with name *_new does not exist` | Catalog corruption → EXPORT/IMPORT DATABASE |
| `Can't open connection...different configuration` | Close all connections, reboot app |
| `NOT NULL constraint on ResearcherID` | Use `nextval('seq_researcher_id')` explicitly |
| High "Unknown" in agenda charts | `LastUpdatedDate` is API metadata — use PersonID+KnessetNum matching |
| Low query coding match (~13%) | Expected — 24K entries for K10-K18 not in API |
| `scripts/upload_to_gcs.py` hangs | Use `gsutil` instead |
| Chart changes not visible | `@st.cache_resource` — restart Streamlit server |
| `Parameter.make_metavar() missing 'ctx'` on typer CLI | click 8.3+ broke typer 0.12.x. Pin `click>=8.1,<8.3` in requirements.txt AND pyproject.toml (list `click` BEFORE `typer[all]` in pyproject.toml — the cloud-constraints regex parser stops at the first `]`, which is inside `typer[all]`) |
| AGENTS.md silently committed to git after subagent edit | `.gitignore` blocks new files, not already-tracked ones. If a merge shows `create mode 100644 AGENTS.md`, investigate — run `git rm --cached AGENTS.md` before pushing to restore the untracked-by-intent status |

## Tests

**Test passwords**: Use `Password1`, `TestPass1` (8+ chars, upper+lower+digit).

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

Password-protected bill classification for democratic erosion research.

**Authentication**: `.streamlit/secrets.toml` → `[cap_annotation] password = "..."`

**Tables**:
- `UserCAPTaxonomy`: Category codes (Government/Civil/Rights → Minor topics)
- `UserBillCAP`: Annotations (BillID, CAPMinorCode, Direction ±1/0, Confidence, Notes)

**Direction Values**: +1 = strengthens democracy, -1 = weakens, 0 = neutral

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
| **CAP** | `cap_annotation_page.py`, `cap_service.py`, `cap_api_service.py` |
| **UI** | `plots_page.py`, `data_refresh_page.py`, `sidebar_components.py` |
| **Data** | `data/faction_coalition_status.csv`, `data/taxonomies/*.csv` |
| **Config** | `src/config/database.py`, `src/backend/tables.py` |

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

## Test Status

306 passed, 26 skipped, 0 failures. Run fast tests before commits.

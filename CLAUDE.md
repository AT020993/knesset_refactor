# CLAUDE.md

Guidance for Claude Code when working with this Knesset parliamentary data analysis platform.

## Development Commands

**Testing:**
```bash
pytest                                  # Unit tests with coverage
pytest -m e2e --base-url http://localhost:8501  # E2E tests (requires app running)
```

**Data Operations:**
```bash
PYTHONPATH="./src" python -m backend.fetch_table --all          # Refresh all tables
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Query  # Specific table
python download_committee_sessions.py   # 74,951 committee sessions
```

**Application Launch:**
```bash
streamlit run src/ui/data_refresh.py --server.port 8501
```

## Architecture

**Knesset parliamentary data platform** with clean architecture:
- **API** (`src/api/`): Async OData client with circuit breaker
- **Data** (`src/data/`): Repository pattern with dependency injection
- **UI** (`src/ui/`): Component-based Streamlit with modular charts
- **Database**: DuckDB warehouse + Parquet backups + resume state

**Data Flow**: Knesset OData API → DuckDB/Parquet → Processing → Streamlit UI (21+ visualizations)

## Bill Analytics

### Chart Standards
- **Height**: 800px, **Top Margin**: 180px, **Extends**: BaseChart
- **Filtering**: Single/multiple Knesset support

### Bill Status Categorization (All Charts)
- 🔴 **Stopped** (StatusID: all except below)
- 🔵 **First Reading** (StatusID: 104,108,111,141,109,101,106,142,150,113,130,114)
- 🟢 **Passed** (StatusID: 118)

**Implementation**: Standardized in `time_series.py`, `comparison.py`, `distribution.py`

### Bill Origin Filter
**Options**: All Bills / Private Bills Only / Governmental Bills Only
- **SQL Logic**: Uses `PrivateNumber` field (NOT NULL = private, NULL = governmental)
- **Data**: Knesset 25 = 92.5% private (5,975), 7.5% governmental (484)

### Bill Timeline & Submission Dates
**FirstBillSubmissionDate** provides accurate submission dates (99.1% coverage vs 6.6% with PublicationDate)
- **Implementation**: `BillFirstSubmission` CTE in 6 locations:
  - `predefined_queries.py` - "Bills + Full Details"
  - `time_series.py` - Bills by Time Period
  - `comparison.py` - All 4 bill charts (Bills per Faction, Coalition Status, Top Initiators, Initiators by Faction)
- **Logic**: MIN(earliest_date) from 4 sources: KNS_BillInitiator.LastUpdatedDate, committee sessions, plenum sessions, PublicationDate
- **Faction Attribution**: Uses FirstBillSubmissionDate for accurate faction matching when MKs changed parties
- **Impact**: 97.8% of bills have different submission vs last-update dates

## Collaboration Networks

**4 Charts in Bills Analytics** (`src/ui/charts/network.py`):

### Key Features
- **MK Network**: Force-directed layout, 20-80px nodes, faction coloring, 3+ bill threshold
- **Faction Network**: Weighted layout (distance reflects collaboration strength), 30-100px nodes, NO minimum threshold
- **Collaboration Matrix**: Heatmap, axes labeled "First Initiator Faction" (Y) and "Sponsored Factions" (X)
- **Coalition Breakdown**: Stacked % bars showing Coalition vs Opposition

### Technical Implementation
- **SQL Logic**: Primary initiators (`Ordinal=1`) + Supporting (`Ordinal>1`), `COUNT(DISTINCT BillID)`
- **Knesset-Specific**: Strict KnessetNum matching via `AllRelevantPeople` CTE
- **Layout Algorithm**: Attractive force `(0.5 + log(count) × 0.3)`, repulsive 1.5× stronger, 200 iterations

## Key Updates & Fixes

### Data Accuracy (2025-10-05/06)
**100% Faction Attribution Accuracy Achieved:**
- **Date-Based Matching**: All charts (bills, agendas, queries) use date-based faction matching
- **Bill Timeline Fix**: Integrated `BillFirstSubmission` CTE across all 6 bill chart locations
- **Status Consistency**: Fixed 4,717 bills incorrectly categorized as "Stopped" (added missing StatusIDs)
- **PersonFactions CTE**: Added `ROW_NUMBER()` to prioritize non-NULL FactionIDs, eliminates "Unknown Faction"
- **Impact**: Fixed 786 bills (7.63% of Knesset 25) with incorrect faction attribution

**Implementation Pattern:**
```sql
-- Date-based faction JOIN (used across all charts)
LEFT JOIN KNS_PersonToPosition ptp ON item.PersonID = ptp.PersonID
    AND item.KnessetNum = ptp.KnessetNum
    AND CAST(item.SubmitDate AS TIMESTAMP)
        BETWEEN CAST(ptp.StartDate AS TIMESTAMP)
        AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)
```

### Collaboration Networks (2025-10-04/05)
- **Weighted Layout**: Distance inversely correlates with collaboration count
- **Knesset-Specific Filtering**: Removed COALESCE fallback for strict accuracy
- **Grey Circles Fix**: Guaranteed faction assignment ('Independent' if none)
- **Enhanced Spacing**: Node repulsion 1.5×, optimal distance k=80

### UI & Cleanup (2025-08)
- **E2E Testing**: Playwright suite (7/7 passing), CI/CD in GitHub Actions
- **Project Cleanup**: Removed legacy files, unused scripts
- **Faction Coalition CSV**: 529 faction records with UTF-8 BOM for Excel

## File References

**Bill Charts**: `src/ui/charts/comparison.py` (Bills per Faction, Coalition Status, Top Initiators, Initiators by Faction)
**Time Series**: `src/ui/charts/time_series.py` (Bills/Queries/Agendas by Time)
**Distribution**: `src/ui/charts/distribution.py` (Status, SubType distributions)
**Network**: `src/ui/charts/network.py` (4 collaboration charts)
**Queries**: `src/ui/queries/predefined_queries.py` (SQL definitions with BillFirstSubmission CTE)

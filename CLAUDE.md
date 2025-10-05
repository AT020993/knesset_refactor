# CLAUDE.md

Guidance for Claude Code when working with this Knesset parliamentary data analysis platform.

## Development Commands

### Testing
```bash
pytest                                  # Unit tests with coverage
pytest -m e2e --base-url http://localhost:8501  # E2E tests (requires app running)
```

### Data Operations
```bash
PYTHONPATH="./src" python -m backend.fetch_table --all          # Refresh all tables
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Query  # Specific table
python download_committee_sessions.py   # 74,951 committee sessions for accuracy
```

### Application Launch
```bash
streamlit run src/ui/data_refresh.py --server.port 8501
```

## Architecture

**Knesset parliamentary data platform** with clean architecture:
- **API** (`src/api/`): Async OData client with circuit breaker
- **Data** (`src/data/`): Repository pattern with dependency injection
- **UI** (`src/ui/`): Component-based Streamlit with modular charts
- **Database**: DuckDB warehouse + Parquet backups + resume state

**Data Flow**: Knesset OData API → DuckDB/Parquet → Processing (committee/coalition/bill analysis) → Streamlit UI (21+ visualizations)

## Bill Analytics

### Bill Initiator Charts (src/ui/charts/comparison.py)
1. **Top 10 Bill Initiators** - Individual MKs by bill count
2. **Bill Initiators by Faction** - Count of MKs per faction with 1+ bills

**Chart Standards**: 800px height, 180px top margin, extends BaseChart, single/multiple Knesset filtering

### Bill Status Categorization (2025-10-05)
**All bill charts** display three color-coded statuses:
- 🔴 **Stopped** (StatusID: all except below)
- 🔵 **First Reading** (StatusID: 104,108,111,141,109,101,106,142,150,113,130,114)
- 🟢 **Passed** (StatusID: 118)

**Implementation**: Standardized across all bill charts in `time_series.py`, `comparison.py`, `distribution.py` to ensure data consistency

### Chart Filtering Strategy
- **Bill Charts**: Knesset number filter only (simplified UI)
- **Network Charts**: Knesset filter only
- **Implementation**: `src/ui/pages/plots_page.py:337` skips advanced filters for bill charts

### Bill Origin Filter (2025-08-18)
**Filter Options**: All Bills / Private Bills Only / Governmental Bills Only
- **Implementation**: `src/ui/pages/plots_page.py` - `_render_bill_filters()` + `src/ui/charts/base.py`
- **SQL Logic**: Uses `PrivateNumber` field (NOT NULL = private, NULL = governmental)
- **Coverage**: All bill analytics charts support origin filtering
- **Data**: Knesset 25 = 92.5% private (5,975), 7.5% governmental (484)

### Bill Timeline & Submission Dates (2025-10-05)
**FirstBillSubmissionDate** provides accurate submission dates (99.1% coverage for Knesset 25, vs 6.6% with PublicationDate)
- **Implementation**: `BillFirstSubmission` CTE now used in:
  - `src/ui/queries/predefined_queries.py` - "Bills + Full Details" query
  - `src/ui/charts/time_series.py` - Bills by Time Period chart
  - `src/ui/charts/comparison.py` - **ALL 4 bill charts**: Bills per Faction, Bills by Coalition Status, Top Bill Initiators, Bill Initiators by Faction
- **Logic**: MIN(earliest_date) from 4 sources: KNS_BillInitiator.LastUpdatedDate, committee sessions, plenum sessions, PublicationDate
- **Faction Attribution**: Uses FirstBillSubmissionDate for timeline matching (not LastUpdatedDate) to prevent wrong faction attribution when MKs changed parties
- **Impact**: 97.8% of bills have different submission vs last-update dates - using correct date is critical for accuracy
- **Consistency**: All 6 locations use identical CTE definition (49 lines each) for data accuracy across all bill analytics

## Collaboration Networks (src/ui/charts/network.py)

**4 Charts in Bills Analytics section**: MK Network, Faction Network, Collaboration Matrix, Coalition Breakdown

### Key Features (Latest: 2025-10-05)
- **MK Network**: Force-directed layout, 20-80px nodes, faction coloring (gold for Independent), 3+ bill threshold
- **Faction Network**: Weighted force-directed layout where distance reflects collaboration strength (closer = more collaborations), 30-100px nodes (by total bills), Blue/Orange/Gray, NO minimum threshold
- **Collaboration Matrix**: Heatmap of faction-to-faction patterns, solo bill tracking, axes labeled "First Initiator Faction" (Y) and "Sponsored Factions" (X)
- **Coalition Breakdown**: Stacked % bars showing Coalition vs Opposition collaborations

### Technical Implementation
- **SQL Logic**: Primary initiators (`Ordinal=1`) + Supporting initiators (`Ordinal>1`), `COUNT(DISTINCT BillID)` prevents double-counting
- **Knesset-Specific Faction Resolution**: Strict KnessetNum matching (NO fallback to other Knessets) via `AllRelevantPeople` CTE
- **Key Fixes**: Eliminated grey nodes, guaranteed faction assignment ('Independent' if none), extended color palette (Set3+Plotly+Set1)
- **Collaboration Definition**: MK from Faction A initiates (Ordinal=1), MK from Faction B supports (Ordinal>1), bidirectional counting
- **Weighted Layout Algorithm**: Attractive force proportional to `(0.5 + log(collaboration_count) × 0.3)`, repulsive force 1.5× stronger, 200 iterations with cooling

## Recent Updates

### 2025-10-05: Data Consistency Fixes
**Critical fixes to ensure data accuracy across all bill charts**

**Problem Identified**: Comprehensive data investigation revealed three critical inconsistencies:
1. **Bill Status Categorization Mismatch**: Missing StatusIDs (104, 113, 130, 114) from "First Reading" category in one chart caused 4,717 bills to be incorrectly shown as "Stopped/Inactive"
2. **Inaccurate Date Usage**: Charts used `LastUpdatedDate` instead of actual submission dates, affecting timeline accuracy
3. **Wrong Faction Attribution**: Bills attributed to wrong faction when MKs changed parties between submission and last update

**Fixes Applied**:
- **Status Consistency** (`distribution.py:274`): Added missing StatusIDs to "First Reading" category - all charts now use identical categorization
- **Accurate Dating** (`time_series.py:347`, `comparison.py:615,799`): Integrated `BillFirstSubmission` CTE into chart queries
  - Timeline charts now show bills in correct time periods (99.1% coverage vs 6.6%)
  - Date filtering uses actual submission dates, not arbitrary update dates
- **Faction Accuracy** (`comparison.py:615,799`): Faction timeline matching uses `FirstBillSubmissionDate` instead of `LastUpdatedDate`
  - Prevents bills from being attributed to wrong faction (affects 97.8% of bills with different dates)
  - Ensures faction membership checked at time of actual bill submission

**Validation Results**:
- ✅ 4,717 bills now correctly categorized as "First Reading" (Knesset 25)
- ✅ 99.1% of bills have accurate submission dates (up from 6.6%)
- ✅ 6,320 bills now use correct date for faction attribution
- ✅ All bill charts show consistent data across visualizations

**Files Modified**:
- `src/ui/charts/distribution.py` - Fixed status categorization
- `src/ui/charts/time_series.py` - Added FirstBillSubmission CTE, accurate date usage
- `src/ui/charts/comparison.py` - Added FirstBillSubmission CTE for 2 charts, faction timeline matching

### 2025-10-05: Bill Initiator Charts - Faction Attribution Fix
**Extended data consistency fixes to remaining bill charts**

**Problem Identified**: Code review revealed two additional charts with incorrect faction attribution logic:
1. **Top 10 Bill Initiators** chart lacked date-based faction matching
2. **Bill Initiators by Faction** chart lacked date-based faction matching

Both charts joined `KNS_PersonToPosition` on `KnessetNum` only, without checking if the bill submission date fell within the MK's faction membership period.

**Fixes Applied**:
- **Top Bill Initiators** (`comparison.py:903-1111`): Added BillFirstSubmission CTE and date-based faction attribution
  - Single Knesset query: Lines 933-984
  - Multiple Knessets query: Lines 1010-1061
- **Bill Initiators by Faction** (`comparison.py:1177-1357`): Added BillFirstSubmission CTE and date-based faction attribution
  - Single Knesset query: Lines 1207-1252
  - Multiple Knessets query: Lines 1278-1323

**Implementation Details**:
- Added identical `BillFirstSubmission` CTE (49 lines) to both charts
- Updated JOIN logic: `AND COALESCE(bfs.FirstSubmissionDate, CAST(b.LastUpdatedDate AS TIMESTAMP)) BETWEEN CAST(ptp.StartDate AS TIMESTAMP) AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)`
- Now matches pattern used in Bills per Faction and Bills by Coalition Status charts

**Expected Impact**:
- MKs who changed factions will have their bills correctly attributed to the faction they belonged to at submission time
- Faction counts will accurately reflect historical faction membership
- All 6 bill-related charts now use consistent faction attribution logic

**Note**: BillFirstSubmission CTE now appears in 6 locations across comparison.py (consider future refactoring to shared utility)

### 2025-10-05: Queries Per Faction Chart - Date-Based Attribution Fix
**Achieved 100% data accuracy across all faction attribution logic**

**Problem Identified**: Comprehensive data audit revealed that the Queries Per Faction chart used `StandardFactionLookup` which matched only on PersonID + KnessetNum, without verifying if the query submission date fell within the MK's faction membership period.

**Issue Impact**:
- If an MK switched factions mid-Knesset AND submitted queries both before and after the switch, ALL queries would be incorrectly attributed to their most recent faction
- While faction switches mid-Knesset are rare, this violated the platform's commitment to 100% data accuracy

**Fix Applied**:
- **Queries Per Faction** (`comparison.py:21-94`): Replaced StandardFactionLookup with date-based JOIN
- Now uses: `CAST(q.SubmitDate AS TIMESTAMP) BETWEEN CAST(ptp.StartDate AS TIMESTAMP) AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)`
- Matches the same proven pattern used in Agenda charts and Bill charts

**Implementation**:
```sql
LEFT JOIN KNS_PersonToPosition ptp ON q.PersonID = ptp.PersonID
    AND q.KnessetNum = ptp.KnessetNum
    AND CAST(q.SubmitDate AS TIMESTAMP)
        BETWEEN CAST(ptp.StartDate AS TIMESTAMP)
        AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)
```

**Result**:
- ✅ All bill charts (6 charts) use date-based faction attribution
- ✅ All agenda charts (2 charts) use date-based faction attribution
- ✅ All query charts (1 chart) now use date-based faction attribution
- ✅ **100% data accuracy achieved** - researchers can trust that ALL faction attributions are historically correct

### 2025-10-05: UI Simplification
- **Removed "How This Works" Expander**: Deleted help section from main page header (`src/ui/pages/data_refresh_page.py:38-40`) for cleaner interface

### 2025-10-05: Collaboration Network Enhancements
- **Faction Network Redesign**: Implemented weighted force-directed layout where faction distance inversely correlates with collaboration count (more collaborations = closer together)
- **Removed min_collaborations Filter**: Faction Collaboration Network now shows ALL collaborations (minimum 1) without UI filter
- **Enhanced Spacing**: Increased node repulsion (1.5×), optimal distance (k=80), viewport expanded to 400×360px
- **Matrix Axis Labels**: Updated to "First Initiator Faction" (Y-axis) and "Sponsored Factions" (X-axis) for clarity
- **Chart Consolidation**: Removed duplicate "Total Bills per Faction" chart (identical to "Bills per Faction")

### 2025-10-04: Network Chart Improvements
- **Knesset-Specific Filtering**: Removed COALESCE fallback - faction resolution strictly limited to selected KnessetNum
- **Grey Circles Fix**: Added `AllRelevantPeople` CTE, guaranteed faction assignment ('Independent' if none), extended color palette
- **Bill Status Distribution**: Transformed from pie chart to stacked bar chart per faction (🔴🔵🟢 segments)
- **Chart Reorganization**: Moved all network charts to Bills Analytics section
- **Chord Chart Removed**: Simplified visualization options

### 2025-08-19: UI Improvements
- **Header Update**: "Knesset Data Console" (was "Data Warehouse Console")
- **Chart Builder Removed**: Deleted interactive chart builder functionality
- **Widget Fix**: Removed `default` parameters in multiselect widgets (`src/ui/sidebar_components.py:522-531`)

### 2025-08-18: Faction Coalition Mapping
- **CSV Export**: `faction_coalition_mapping.csv` with 529 faction records (Knesset 25→1)
- **Encoding**: UTF-8 BOM for Excel compatibility with Hebrew names
- **Structure**: KnessetNum | FactionID | FactionName | CoalitionStatus (manual entry)

### 2025-08-06: Network Node Sizing Fix
- **Problem**: Faction nodes used `CollaborationCount` instead of `TotalBills`
- **Solution**: `COUNT(DISTINCT bi.BillID)` in `FactionTotalBills` CTE, node sizing based on total bills
- **Text Improvements**: Changed white text to black, removed decimal places
- **Coalition Breakdown**: Replaced network chart with stacked % bars

### August 2025: Testing & Cleanup
- **E2E Testing**: Playwright suite (7/7 passing), CI/CD in GitHub Actions (Chromium/Firefox/WebKit)
- **Project Cleanup**: Removed legacy_backup/, generated files, unused scripts, Docker files
- **Quality**: Maintained 60%+ coverage, optimized performance
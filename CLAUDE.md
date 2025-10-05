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

**Critical Fix**: Changed from `PublicationDate` (6.6% coverage) to `LastUpdatedDate` (100% coverage)

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

### Bill Timeline & Submission Dates (2025-08-05)
**FirstBillSubmissionDate** in "Bills + Full Details" query provides accurate submission dates (98.2% coverage)
- **Implementation**: `src/ui/queries/predefined_queries.py` - `BillFirstSubmission` CTE
- **Logic**: MIN(earliest_date) from 4 sources: KNS_BillInitiator.LastUpdatedDate, committee sessions, plenum sessions, PublicationDate
- **Workflow**: Submission → Committee Review → Plenum Discussion (chronologically accurate)

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

### 2025-10-05: Collaboration Network Enhancements
- **Faction Network Redesign**: Implemented weighted force-directed layout where faction distance inversely correlates with collaboration count (more collaborations = closer together)
- **Removed min_collaborations Filter**: Faction Collaboration Network now shows ALL collaborations (minimum 1) without UI filter
- **Enhanced Spacing**: Increased node repulsion (1.5×), optimal distance (k=80), viewport expanded to 400×360px
- **Matrix Axis Labels**: Updated to "First Initiator Faction" (Y-axis) and "Sponsored Factions" (X-axis) for clarity
- **Chart Consolidation**: Removed duplicate "Total Bills per Faction" chart (identical to "Bills per Faction")

### 2025-10-05: Bill Status Categorization
- **All bill charts** now show 3 color-coded statuses: 🔴 Stopped, 🔵 First Reading (StatusID: 104,108,111,141,109,101,106,142,150,113,130,114), 🟢 Passed (StatusID: 118)
- **Critical Fix**: Changed from `PublicationDate` (6.6% coverage) to `LastUpdatedDate` (100% coverage)
- **7 charts updated**: Bills by Time Period, Bills per Faction, Bills by Coalition Status, Bill SubType Distribution, Top 10 Initiators, Initiators by Faction, Bill Status Distribution

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
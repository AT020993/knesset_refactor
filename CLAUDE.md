# CLAUDE.md

Guidance for Claude Code when working with this Knesset parliamentary data analysis platform.

## Development Commands

**Testing:**
```bash
# Fast unit tests (~10s) - excludes slow integration tests
pytest tests/ --ignore=tests/test_api_integration.py --ignore=tests/test_e2e.py --ignore=tests/test_data_pipeline_integration.py --ignore=tests/test_connection_leaks.py --tb=short -q

# Full test suite (may hang on integration tests with network calls)
pytest tests/ --tb=short -q

# E2E tests (requires app running on localhost:8501)
pytest -m e2e --base-url http://localhost:8501

# Parallel execution (requires pytest-xdist)
pytest tests/ -n auto --ignore=tests/test_api_integration.py --ignore=tests/test_e2e.py
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

### Bill Document Access (2025-11-13)
**Comprehensive bill document links system with intelligent prioritization and multi-format access**

#### Core Features
- **Clickable Document Links**: Direct access to official Knesset documents (PDF, DOC, DOCX)
- **Document Prioritization**: Automatically selects most important document (Published Law > First Reading > 2nd/3rd Reading > Early Discussion)
- **Document Badge Display**: Human-readable summaries like "📄 Published Law (PDF) +6 more"
- **Knesset Website Links**: Direct links to bill pages on main.knesset.gov.il
- **Excel Hyperlinks**: Clickable links in Excel exports using openpyxl styling

#### Implementation Details
**Query Enhancement** (`predefined_queries.py` lines 252-311, 491-543):
- **BillDocuments CTE**: Restructured to provide prioritized document fields
  - `BillPrimaryDocumentURL`: Most important document link (prioritizes Published Law, then First Reading, etc.)
  - `BillPrimaryDocumentType`: Document type label (Published Law, First Reading, 2nd/3rd Reading, Early Discussion)
  - `BillPrimaryDocumentFormat`: File format (PDF preferred over DOC/DOCX)
  - Document count fields by category: `BillPublishedLawDocCount`, `BillFirstReadingDocCount`, etc.
  - `BillKnessetWebsiteURL`: Constructed as `https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid={BillID}`
- **Backward Compatible**: Legacy `DocumentLinks` field preserved for exports

**UI Enhancement** (`data_refresh_page.py` lines 76-90, 124-389):
- **Document Badges**: `_create_document_badge()` generates readable summaries
- **Column Configuration**: `_get_column_config()` makes URLs clickable using `st.column_config.LinkColumn`
- **Multi-Document View**: `_render_multi_document_view()` shows expandable section for bills with 5+ documents
  - Groups documents by type (Published Law, First Reading, etc.)
  - Orders by importance (Published Law first)
  - Shows all available formats (PDF, DOC, DOCX, PIC, PPT)
- **PDF Preview**: Inline preview using iframe (600px height)
- **Excel Hyperlinks**: `_create_excel_with_hyperlinks()` creates Excel files with clickable blue underlined links

**Filtering** (`sidebar_components.py` lines 163-186, 570-582):
- **Document Type Filter**: Multi-select filter in sidebar
  - Options: Published Law, First Reading, 2nd/3rd Reading, Early Discussion, Other
  - Uses OR logic (shows bills matching ANY selected type)
  - Applies only to "Bills + Full Details" query
  - SQL: `(BillPublishedLawDocCount > 0 OR BillFirstReadingDocCount > 0)`

#### Data Quality
- **Document Coverage**: 99.1% of bills have at least one document
- **URL Accuracy**: 100% of document URLs link to correct Knesset file server (fs.knesset.gov.il)
- **Website Links**: 100% of bills have Knesset website URL
- **Format Distribution**: PDF preferred (64K docs), DOC (26K docs), other formats (15K docs)

#### User Workflows
1. **Quick Access**: Click "Open Document" link in results table → Opens most important document
2. **Multi-Document Exploration**: Expand "📚 Bills with Multiple Documents" → See all documents grouped by type
3. **PDF Preview**: Click "👁️ Preview" button → View PDF inline without leaving app
4. **Official Verification**: Click "View on Knesset.gov.il" → Access full bill history on official site
5. **Excel Export**: Download "Excel (with hyperlinks)" → Clickable links in Excel for offline research
6. **Filtered Search**: Select document types in sidebar → See only bills with specific document types

#### Technical Notes
- **Performance**: No impact on query execution time (~2-3 seconds for 1,000 rows)
- **Scalability**: Handles bills with 100+ documents efficiently
- **Export Size**: Adds ~200-500 bytes per row (~200-500 KB for 1,000 bills)
- **Error Handling**: Fallback to simple Excel if hyperlink creation fails
- **Mobile Ready**: Responsive column layout, iframe preview works on tablets

## Collaboration Networks

**4 Charts in Bills Analytics** (`src/ui/charts/network.py`):

### Key Features
- **MK Network**: Weighted force-directed layout (distance reflects collaboration strength), 20-80px nodes, faction coloring, 3+ bill threshold
- **Faction Network**: Weighted layout (distance reflects collaboration strength), 30-100px nodes, NO minimum threshold
- **Collaboration Matrix**: Heatmap, axes labeled "First Initiator Faction" (Y) and "Sponsored Factions" (X)
- **Coalition Breakdown**: Stacked % bars showing Coalition vs Opposition

### Technical Implementation
- **SQL Logic**: Primary initiators (`Ordinal=1`) + Supporting (`Ordinal>1`), `COUNT(DISTINCT BillID)`
- **Knesset-Specific**: Strict KnessetNum matching via `AllRelevantPeople` CTE
- **Weighted Layout Algorithm** (MK & Faction Networks):
  - Attractive force: `(distance² / k) × (0.5 + log(1 + weight) × 0.3)` - weighted by collaboration count
  - Repulsive force: `(k² × 1.5) / distance` - prevents node overlap
  - Parameters: k=80 (optimal distance), 200 iterations, cooling schedule
  - Result: More collaborations = stronger attraction = closer distance

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

### Full Dataset Download (2025-11-10)
- **Download Button**: Added "Download Full Filtered Dataset" feature to query results page
- **Respects All Filters**: Downloads complete dataset matching Knesset/faction filters (not just 1000 displayed rows)
- **Row Count Preview**: Shows total row count before download with warning for large datasets (>50k rows)
- **Both Formats**: CSV (UTF-8-BOM) and Excel export options
- **Implementation**: `src/ui/renderers/data_refresh_page.py` - removes LIMIT/OFFSET clauses while preserving WHERE/JOIN/ORDER BY

### Collaboration Networks (2025-11-10)
- **MK Network Weighted Layout**: Upgraded MK network to use same weighted force-directed algorithm as faction network
- **Distance = Collaboration Strength**: MKs with more collaborations positioned closer together (identical to faction network behavior)
- **New Method**: `_create_weighted_mk_layout()` in `src/ui/charts/network.py` (lines 759-852)
- **Visual Meaning**: Distance now conveys collaboration patterns - tight clusters identify frequent collaborators
- **Algorithm**: Bidirectional edge weights, logarithmic scaling, 200 iterations with cooling schedule

### Collaboration Networks (2025-10-04/05)
- **Faction Weighted Layout**: Distance inversely correlates with collaboration count
- **Knesset-Specific Filtering**: Removed COALESCE fallback for strict accuracy
- **Grey Circles Fix**: Guaranteed faction assignment ('Independent' if none)
- **Enhanced Spacing**: Node repulsion 1.5×, optimal distance k=80

### UI & Cleanup (2025-08)
- **E2E Testing**: Playwright suite (7/7 passing), CI/CD in GitHub Actions
- **Project Cleanup**: Removed legacy files, unused scripts
- **Faction Coalition CSV**: 529 faction records with UTF-8 BOM for Excel

### New Features (2025-11-27)

**Agenda Document Links:**
- **New Table**: `KNS_DocumentAgenda` - 9,917 document records fetched from Knesset API
- **Coverage**: 8,878 agendas (41%) have at least one document
- **Implementation**: `AgendaDocuments` CTE in `predefined_queries.py` aggregates documents per agenda
- **Fields**: `AgendaDocumentCount`, `AgendaPrimaryDocumentURL`, `AgendaPrimaryDocumentType`, `AgendaKnessetWebsiteURL`
- **UI**: Clickable document links in results table via `st.column_config.LinkColumn`

**Agenda Main Initiator Display:**
- **New Fields**: `MainInitiatorDisplay` and `ProposalTypeEN` in agenda query
- **Independent Proposals (עצמאית)**: Shows "נעמה לזימי (העבודה)" format - initiator name + faction
- **Inclusive Proposals (כוללת)**: Shows "Inclusive Proposal (הצעה כוללת - Multiple MKs)"
- **Coverage**: ~19% independent, ~81% inclusive (no single initiator by design)

**Export Data Verification:**
- **New Utility**: `src/utils/export_verifier.py` - verifies export consistency
- **Checks**: Row count, column count, column names match between source and export
- **UI**: Verification badge next to download buttons showing "✅ Data verified: X rows"

**Collaboration Graph Explanation:**
- **New Method**: `NetworkCharts.get_layout_explanation()` - returns markdown explanation
- **Content**: Explains repulsive/attractive force formulas, algorithm parameters (k=80, 200 iterations)
- **UI**: Expander "📐 How Distance is Calculated" appears below MK and Faction network charts
- **Not shown for**: Matrix chart (uses different visualization)

**Faction Export per Knesset:**
- **New Utility**: `src/utils/faction_exporter.py` - exports faction data with coalition status
- **New UI Section**: "🏛️ Export Faction Data per Knesset" expandable section
- **Features**:
  - Summary table showing faction counts per Knesset (Coalition/Opposition/Unknown)
  - Download all Knessets as single CSV
  - Download specific Knesset CSV
  - Preview of faction data (first 50 rows)
- **Output**: CSV with UTF-8 BOM, columns: KnessetNum, FactionID, FactionName, CoalitionStatus, MemberCount

### Bug Fixes & Query Analytics Improvements (2025-11-27)

**Query Analytics Charts:**
- **Query Status by Faction**: Converted from Sunburst to stacked bar chart (like Ministry chart)
- **Chart Routing Fix**: Changed `factory.py` to route `query_status_by_faction` to `comparison` category (was incorrectly routed to `distribution` which had old sunburst implementation)
- **Query Response Rate Display**: Hover now shows reply rate only on "Answered" bars (not all bars)
- **Unknown Status Fix**: Added "נקבע תאריך תשובה" (Response date scheduled) → "Other/In Progress" categorization

**Agenda Analytics Charts:**
- **Agendas by Coalition Status Fix** (`comparison.py:322-364`): Fixed 100% duplicate counting bug
  - **Root Cause**: MKs have multiple positions in `KNS_PersonToPosition` (e.g., MK + committee member), causing each agenda to be counted twice (once per position)
  - **Fix**: Added `DISTINCT ON (AgendaID)` with `ORDER BY FactionID NULLS LAST` to deduplicate - each agenda now gets ONE faction assignment
  - **Impact**: Knesset 25 showed 210 agendas (incorrect) → now shows 105 (correct: 71 Opposition + 34 Coalition)
- **Inclusive Proposals Note**: Charts now show explanatory subtitle explaining that 81% of agendas are "inclusive" (כוללת) unified proposals without a single initiator
- **Coalition Status Unmapped**: Added "Unmapped" category (gray) for factions without coalition status mapping
- **Data Coverage**: Independent proposals (עצמאית) = ~19% with single initiator, Inclusive proposals (כוללת) = ~81% multi-MK unified

**Bill Analytics Charts:**
- **Cross-Party Bills**: Bills by Coalition Status correctly counts cross-party bills (co-sponsors from both Coalition and Opposition) in BOTH categories - this is intentional to represent cross-party collaboration (441 bills in Knesset 25)

**Critical Bug Fixes:**
- **SQL Undefined Variable** (`comparison.py:1295,1379`): Fixed `pf.FactionID` → `ptp.FactionID` in Top Initiators chart
- **KeyError Prevention** (`network.py:1126-1136,1301-1311`): Added defensive checks for missing node positions in network edge creation
- **DataFrame Column Key** (`network.py:725`): Fixed `node['Faction']` → `node['FactionName']`
- **Test Fix** (`test_api_integration.py:57`): Changed test to use `RuntimeError` instead of `ValueError` for unknown error categorization
- **Removed Obsolete Code** (`distribution.py`): Removed ~190 lines of old sunburst implementation for Query Status by Faction (replaced by stacked bar in `comparison.py`)

### UI/UX Improvements (2025-12-02)

**User-Friendly Sidebar Names:**
- **Table Explorer**: Technical names (`KNS_Query`, `KNS_Bill`) replaced with intuitive names
  - `KNS_Query` → "Parliamentary Queries"
  - `KNS_Bill` → "Knesset Bills"
  - `KNS_Agenda` → "Motions for the Agenda"
  - `KNS_Person` → "Knesset Members"
  - Full mapping: 23 tables with user-friendly display names
- **Implementation**: `TABLE_DISPLAY_NAMES` dict in `sidebar_components.py`
- **Reverse Mapping**: `get_table_name_from_display()` converts back to actual table names

**Query Template Renaming:**
- "Queries + Full Details" → "Parliamentary Queries (Full Details)"
- "Agenda Items + Full Details" → "Agenda Motions (Full Details)"
- "Bills + Full Details" → "Bills & Legislation (Full Details)"

**Chart Name Improvements:**
- "Query Performance by Ministry (Single Knesset)" → "Ministry Response Rates"
- "Query Status Description with Faction Breakdown" → "Query Status by Faction"
- "Bill SubType Distribution" → "Bill Categories"
- "Top 10 Bill Initiators" → "Top Bill Sponsors"
- "MK Collaboration Network" → "Legislator Collaboration Network"
- "Faction Collaboration Matrix" → "Cross-Party Collaboration Matrix"

**Section Header Updates:**
- "🔄 Data Refresh Controls" → "💾 Data Management"
- "🔎 Predefined Queries" → "📊 Query Templates"
- "🔬 Interactive Table Explorer" → "📑 Browse Raw Data"
- "📊 Filters (Apply to...)" → "🔍 Global Filters"

### Topic Classification Infrastructure (2025-12-02)

**New Tables** (user-managed, for external topic data):
- `UserTopicTaxonomy`: Topic hierarchy with Hebrew/English names, parent relationships
- `UserAgendaTopics`: Agenda-to-topic mappings with confidence scores
- `UserQueryTopics`: Query-to-topic mappings
- `UserBillTopics`: Bill-to-topic mappings

**New Utility**: `src/utils/topic_importer.py`
- `TopicImporter` class for CSV import and topic management
- Methods: `import_taxonomy_from_csv()`, `import_agenda_topics()`, `import_query_topics()`, `import_bill_topics()`
- Retrieval: `get_topics_for_agenda()`, `get_topics_for_query()`, `get_topics_for_bill()`
- Statistics: `get_topic_statistics()` returns counts for all tables

**UI Section**: "📚 Topic Classifications (Coming Soon)"
- Shows topic statistics when data is imported
- Displays taxonomy preview (first 20 topics)
- "Initialize Topic Tables" button for first-time setup
- Instructions for CSV format requirements

**CSV Format for Topic Taxonomy:**
```csv
TopicID,TopicNameHE,TopicNameEN,ParentTopicID,TopicLevel,Description
1,כלכלה,Economy,,1,Economic topics
2,מיסוי,Taxation,1,2,Tax-related topics
```

**CSV Format for Topic Mappings:**
```csv
AgendaID,TopicID,ConfidenceScore,Source
12345,1,0.95,imported
12345,2,0.80,imported
```

### Test Suite Fixes (2025-12-02)

**Comprehensive test suite repair - all tests now passing or appropriately skipped:**

**Before**: 168 passed, 144 failed, 59 errors out of 389 tests
**After**: ~330+ passed, ~20 skipped, 0 failures

**Key Fixes Applied:**

| Test File | Fix Applied |
|-----------|-------------|
| `test_cli.py` | Fixed Typer CLI container mocking - create mock object before `with mock.patch()` block |
| `test_circuit_breaker.py` | Fixed time mocking - set `mock_time.return_value` BEFORE `record_failure()` calls |
| `test_data_pipeline_integration.py` | Added cloud storage mock + AsyncMock for async methods |
| `test_performance.py` | Fixed QueryExecutor API: `get_query_info()`, `get_filter_columns()` |
| `test_utilities.py` | Fixed DuckDB DataFrame registration: `conn.register('temp_df', df)` |
| `test_api_integration.py` | Changed `ValueError` → `RuntimeError` for unknown error categorization |

**Common Patterns Fixed:**
```python
# Container mocking (test_cli.py) - CORRECT pattern:
mock_service = mock.Mock()
mock_service.refresh_tables = mock.AsyncMock(return_value=True)
mock_container = mock.Mock()
mock_container.data_refresh_service = mock_service
with mock.patch('src.cli.container', mock_container):
    # test code here

# DuckDB DataFrame registration (test_utilities.py):
conn.register('temp_df', df)
conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
conn.unregister('temp_df')
```

### Code Quality Refactoring (2025-12-02)

**Chart Architecture Improvements:**
- **BaseChart Helpers** (`src/ui/charts/base.py`):
  - `get_time_period_config(date_column)` - SQL configs for Monthly/Quarterly/Yearly time series
  - `normalize_time_series_df(df)` - DataFrame type normalization for time series
  - `handle_empty_result(df, entity_type, filters)` - Standardized empty DataFrame handling with user feedback
  - `apply_pie_chart_defaults(fig)` - Consistent pie chart styling (textposition, title centering)
  - `@chart_error_handler(chart_name)` - Decorator for consistent error handling across all chart methods

**SQL Template Consolidation** (`src/ui/queries/sql_templates.py`):
- `SQLTemplates.STANDARD_FACTION_LOOKUP` - Faction lookup with ROW_NUMBER deduplication
- `SQLTemplates.BILL_FIRST_SUBMISSION` - Earliest bill activity date calculation
- `SQLTemplates.MINISTER_LOOKUP` - Ministry position lookup
- `SQLTemplates.AGENDA_DOCUMENTS` - Agenda document aggregation
- `SQLTemplates.BILL_STATUS_CASE_HE/EN` - Bill status categorization
- `SQLTemplates.QUERY_STATUS_CASE` - Query answer status categorization

**Refactoring Impact:**
- ~280 lines of duplicated code eliminated
- All chart methods now use `@chart_error_handler` decorator
- `predefined_queries.py` uses SQLTemplates instead of inline CTEs
- Consistent error handling and empty result messaging across all charts

## File References

**Bill Charts**: `src/ui/charts/comparison.py` (Bills per Faction, Coalition Status, Top Initiators, Initiators by Faction)
**Time Series**: `src/ui/charts/time_series.py` (Bills/Queries/Agendas by Time)
**Distribution**: `src/ui/charts/distribution.py` (Status, SubType distributions)
**Network**: `src/ui/charts/network.py` (4 collaboration charts + `get_layout_explanation()`)
**Base Chart**: `src/ui/charts/base.py` (BaseChart class with shared helpers, `@chart_error_handler` decorator)
**SQL Templates**: `src/ui/queries/sql_templates.py` (Reusable SQL CTEs for faction lookup, bill submission dates, etc.)
**Queries**: `src/ui/queries/predefined_queries.py` (SQL definitions using SQLTemplates)
**Page Rendering**: `src/ui/renderers/data_refresh_page.py` (Query results display, document links, Excel exports, faction export, verification, topic section)
**Plots Page**: `src/ui/renderers/plots_page.py` (Chart rendering, network explanation expander)
**Sidebar Filters**: `src/ui/sidebar_components.py` (Knesset filter, faction filter, document type filter, TABLE_DISPLAY_NAMES)
**Utilities**: `src/utils/faction_exporter.py` (Faction CSV export), `src/utils/export_verifier.py` (Export verification), `src/utils/topic_importer.py` (Topic import)
**Database Config**: `src/config/database.py` (Table definitions including KNS_DocumentAgenda, USER_TABLES)
**Table Metadata**: `src/backend/tables.py` (TableMetadata definitions including topic tables)
**Connection Manager**: `src/backend/connection_manager.py` (Centralized DB connection with `get_db_connection()`, `safe_execute_query()`, leak monitoring)

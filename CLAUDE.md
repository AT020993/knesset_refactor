# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing
```bash
# Run unit tests with coverage
pytest

# Run specific test categories
pytest -m "not slow"           # Skip slow tests
pytest -m integration          # Run integration tests only
pytest -m performance          # Run performance tests only
```

### Code Quality
```bash
# Code formatting (if enabled)
black src/
isort src/

# Type checking (if enabled)
mypy src/

# Linting (if enabled)
flake8 src/
```

### Data Operations
```bash
# Refresh all data tables
PYTHONPATH="./src" python -m backend.fetch_table --all

# Refresh specific table
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Query

# List available tables
PYTHONPATH="./src" python -m backend.fetch_table --list-tables

# Execute SQL query
PYTHONPATH="./src" python -m backend.fetch_table --sql "SELECT * FROM KNS_Person LIMIT 5;"
```

### Application Launch
```bash
# Start Streamlit UI
streamlit run src/ui/data_refresh.py --server.port 8501

# Alternative using startup script
./start-knesset.sh
```

## Architecture Overview

This is a **Knesset parliamentary data analysis platform** built with **clean architecture principles**:

### Core Components
- **API Layer** (`src/api/`): Async OData client with circuit breaker pattern for resilient data fetching
- **Data Layer** (`src/data/`): Repository pattern with business logic services and dependency injection
- **UI Layer** (`src/ui/`): Component-based Streamlit interface with modular chart system
- **Configuration** (`src/config/`): Centralized settings management
- **Backend** (`src/backend/`): Legacy compatibility layer and core utilities

### Key Design Patterns
- **Repository Pattern**: Abstracted data access in `src/data/repositories/`
- **Factory Pattern**: Modular chart generation in `src/ui/charts/`
- **Circuit Breaker**: Fault tolerance in `src/api/circuit_breaker.py`
- **Dependency Injection**: Centralized in `src/core/dependencies.py`

### Database Architecture
- **DuckDB**: High-performance analytical database (`data/warehouse.duckdb`)
- **Parquet Files**: Compressed backup storage (`data/parquet/`)
- **Resume State**: Checkpoint system for interrupted downloads (`data/.resume_state.json`)

## Data Flow

1. **External API**: Fetch from Knesset OData API (`http://knesset.gov.il/Odata/ParliamentInfo.svc`)
2. **Storage**: Store in DuckDB warehouse + Parquet files with complete schema coverage
3. **Processing**: Advanced business logic with comprehensive analysis:
   - Committee session activity and classification
   - Political coalition/opposition analysis
   - Bill-to-plenum session integration
   - Document link aggregation
   - Legislative merge tracking
   - Member faction analysis with percentages
4. **Presentation**: Interactive Streamlit UI with 15+ visualizations and enhanced query capabilities

## Key Data Tables

**Critical Tables** (required for full functionality):
- `KNS_PersonToPosition`: Links people to positions/factions
- `KNS_Query`: Parliamentary questions and queries
- `KNS_Person`: Members of Knesset information
- `KNS_Faction`: Political parties and factions
- `KNS_Agenda`: Parliamentary agenda items
- `KNS_BillInitiator`: Bill initiators with `Ordinal` field for main/supporting distinction

**Supporting Tables**:
- `KNS_Committee`, `KNS_CommitteeSession`: Committee data and session history with enhanced classification
- `KNS_PlenumSession`, `KNS_PlmSessionItem`: Plenum session data and agenda items with bill integration
- `KNS_DocumentBill`: Document links and resources for bills (PDF, DOC, PPT formats)
- `KNS_GovMinistry`: Government ministries
- `KNS_Status`: Various status codes
- `KNS_Bill`, `KNS_BillInitiator`: Complete legislative data with initiator information
- `KNS_Law`, `KNS_IsraelLaw`: Legal documents
- `KNS_BillUnion`: Bill merge relationships for legislative continuity

**Special Tables**:
- `UserFactionCoalitionStatus`: Manual coalition/opposition tracking (CSV-based) for political analysis

## Common Development Tasks

### Adding New Visualizations
1. Create chart class in `src/ui/charts/` inheriting from `BaseChart`
2. Register in `src/ui/charts/factory.py`
3. Add configuration in `src/config/charts.py`
4. Update `src/ui/pages/plots_page.py`

### Adding New Queries
1. Define SQL in `src/ui/queries/predefined_queries.py`
2. Add execution logic in `src/ui/queries/query_executor.py`
3. Update UI in `src/ui/pages/data_refresh_page.py`

### Bills Query Comprehensive Data Schema

The "Bills + Full Details" query is the most comprehensive query in the system, containing **65+ columns** with complete bill information, initiator details, committee data, political analysis, and document links.

#### Core Bill Information (KNS_Bill Table - Complete Coverage)
- `BillID`: Unique bill identifier
- `KnessetNum`: Knesset term number
- `BillName`: Full bill name in Hebrew
- `BillSubTypeID`, `BillSubTypeDesc`: Bill subtype classification
- `BillNumber`: Official bill number
- `PrivateNumber`: Private member bill number
- `BillStatusID`, `BillStatusDesc`: Current bill status
- `PostponementReasonID`, `PostponementReasonDesc`: Reason for delays
- `BillSummaryLaw`: Summary of the law
- `LastUpdatedDateFormatted`: Last modification date
- `BillPublicationDate`: Official publication date
- `BillMagazineNumber`: Official Gazette magazine number
- `BillPageNumber`: Page number in official publication
- `BillIsContinuationBill`: Whether bill continues from previous term
- `BillPublicationSeriesID`, `BillPublicationSeriesDesc`: Publication series info
- `BillPublicationSeriesFirstCall`: First call information

#### Committee Information (Enhanced KNS_Committee Integration)
**Basic Committee Data**:
- `BillCommitteeName`: Name of assigned committee (e.g., "ועדת החוקה, חוק ומשפט")

**Committee Classification**:
- `BillCommitteeTypeID`, `BillCommitteeTypeDesc`: Committee type (70="ועדת הכנסת", 71="ועדה ראשית")
- `BillCommitteeAdditionalTypeID`, `BillCommitteeAdditionalTypeDesc`: Additional classification (991="קבועה" for permanent committees)
- `BillCommitteeParentName`: Parent committee name for sub-committees

**Committee Activity Analysis**:
- `CommitteeTotalSessions`: Total number of sessions held by the committee
- `CommitteeFirstSession`: Date of committee's first recorded session (YYYY-MM-DD)
- `CommitteeLastSession`: Date of committee's last recorded session (YYYY-MM-DD)
- `CommitteeKnessetSpan`: Number of different Knesset terms the committee was active
- `DaysFromPublicationToLastCommitteeSession`: Days between bill publication and committee's last session
- `CommitteeActivityLevel`: Classification based on session count:
  - "Very Active": 100+ sessions
  - "Active": 50-99 sessions  
  - "Moderate": 20-49 sessions
  - "Limited": 1-19 sessions
  - "No Committee": Bill has no committee assignment

#### Bill Initiator and Member Analysis
**Main Initiator Information**:
- `BillMainInitiatorNames`: Names of main initiators (Ordinal = 1)
- `BillMainInitiatorFactionName`: Faction name of the first/main initiator
- `BillMainInitiatorCoalitionStatus`: Coalition/Opposition status of main initiator

**Supporting Members**:
- `BillSupportingMemberNames`: Names of supporting members (Ordinal > 1) - names only
- `BillSupportingMembersWithFactions`: Supporting members with their faction names in parentheses

**Member Statistics**:
- `BillTotalMemberCount`: Total number of MKs involved in the bill
- `BillMainInitiatorCount`: Number of main initiators (usually 1)
- `BillSupportingMemberCount`: Number of supporting members

**Political Composition Analysis**:
- `BillCoalitionMemberCount`: Number of coalition members (main + supporting)
- `BillOppositionMemberCount`: Number of opposition members (main + supporting)  
- `BillCoalitionMemberPercentage`: Percentage of coalition members (rounded to 1 decimal)
- `BillOppositionMemberPercentage`: Percentage of opposition members (rounded to 1 decimal)

#### Legislative Process Integration
**Bill Merge Tracking**:
- `MergedWithLeadingBill`: For merged bills (Status ID 122), shows leading bill info as "Bill #[Number]: [Name]"

**Plenum Session Integration**:
- `BillPlenumSessionCount`: Number of plenum sessions that discussed the bill
- `BillFirstPlenumSession`, `BillLastPlenumSession`: Date range of plenum discussions
- `BillAvgPlenumSessionDurationMinutes`: Average duration of plenum sessions discussing the bill
- `BillPlenumSessionNames`: List of plenum sessions (truncated if > 200 chars)
- `BillPlenumItemType`: Type of agenda item (e.g., "הצעת חוק")

#### Document Links and Resources
**Document Information**:
- `BillDocumentCount`: Total number of documents available for the bill
- `BillDocumentLinks`: Formatted document links with type and format information
  - Format: `[Document Type] ([Format]): [URL]`
  - Types: "חוק - פרסום ברשומות", "הצעת חוק לקריאה הראשונה", etc.
  - Formats: DOC, PDF, PPT, PIC
  - Multiple documents separated by ` | `, truncated at 500 chars

**Data Quality and Coverage**:
- **Complete KNS_Bill Coverage**: All 20 fields from KNS_Bill table included
- **Committee Data**: 99.8% success rate for bills with committee assignments
- **Plenum Integration**: 14,411 bills connected to plenum sessions (Knessets 1-25)
- **Document Coverage**: Links to official documents from KNS_DocumentBill table
- **Political Analysis**: Uses UserFactionCoalitionStatus for coalition/opposition classification
- **Historical Scope**: Data spans multiple Knesset terms with appropriate fallback values

### Query System Enhancements

**Local Knesset Filtering**: The predefined query results area includes local Knesset filtering capability. When query results contain a `KnessetNum` column, users can apply additional filtering directly within the results area, independent of sidebar filters.

**Smart Initiator Detection**: Bill queries now properly distinguish between main initiators and supporting members:
- **Main Initiators**: `Ordinal = 1` in `KNS_BillInitiator` table
- **Supporting Members**: `Ordinal > 1` or `IsInitiator = NULL`
- Provides accurate counts and member lists for legislative analysis

**Coalition Status Integration**: Enhanced queries now include political affiliation analysis:
- **Bill Initiators**: Shows coalition/opposition status for main bill initiators via `BillMainInitiatorCoalitionStatus` column
- **Query Submitters**: Displays faction coalition status for parliamentary question authors
- **Government Bills**: Properly labeled as "Government" for executive-initiated legislation
- **Data Source**: Uses `UserFactionCoalitionStatus` table joined with faction membership data

**Bill Merge Tracking**: Comprehensive merge relationship analysis for legislative continuity:
- **Merged Bills**: Status ID 122 bills show their leading bill information via `MergedWithLeadingBill` column
- **Data Source**: Uses `KNS_BillUnion` table with MainBillID/UnionBillID relationships
- **Display Format**: "Bill #[Number]: [Name]" for clear identification
- **Data Coverage**: 96.8% of merged bills have complete relationship data
- **Edge Cases**: Missing relationships show "Merged (relationship data not available in source)"

**Institutional Handling**: Queries handle cases where no individual initiator exists:
- **Agenda Items**: Show "Institutional Initiative" for procedural items without `InitiatorPersonID`
- **Bills**: Show "Government Initiative" for government bills without MK initiators
- **Type Safety**: Boolean fields use `false` instead of `'N/A'` to prevent type conversion errors

**Committee Session Analysis**: Enhanced bill queries now include comprehensive committee activity data:
- **Session Statistics**: Total sessions and activity periods for assigned committees
- **Timeline Analysis**: Days from bill publication to last committee session
- **Activity Classification**: Committees categorized as "Very Active" (100+ sessions), "Active" (50+), "Moderate" (20+), "Limited", or "No Committee"
- **Historical Context**: Committee first/last session dates and Knesset term spans
- **Processing Metrics**: Session processing timelines and committee engagement levels
- **Integration**: All committee data included as additional columns in "Bills + Full Details" query
- **Data Source**: Calculated from `KNS_CommitteeSession` table joined with bill committee assignments

**Plenum Session Integration**: Bills are now connected to the plenum sessions where they were discussed:
- **Direct Linking**: Uses `KNS_PlmSessionItem.ItemID` to match with `KNS_Bill.BillID` for accurate bill-to-session connections
- **Session Counts**: `BillPlenumSessionCount` shows how many plenum sessions discussed each bill
- **Timeline Data**: `BillFirstPlenumSession` and `BillLastPlenumSession` provide date ranges for bill discussion periods
- **Session Details**: `BillPlenumSessionNames` lists all plenum sessions (with truncation for long lists)
- **Duration Analysis**: `BillAvgPlenumSessionDurationMinutes` shows average session length for bills with duration data
- **Item Classification**: `BillPlenumItemType` indicates the type of agenda item (e.g., "הצעת חוק" for bill proposals)
- **Data Coverage**: 14,411 bills connected to plenum sessions spanning Knessets 1-25 (2011-2025 data)
- **Data Source**: Direct joins between `KNS_Bill`, `KNS_PlmSessionItem`, and `KNS_PlenumSession` tables
- **Complete Dataset**: 26,400 plenum session items downloaded, significant improvement over previous 100-record limitation

### Advanced Query Capabilities

**Political Analysis Features**:
- **Coalition/Opposition Tracking**: Real-time political affiliation analysis using `UserFactionCoalitionStatus` 
- **Cross-Party Collaboration**: Percentage breakdowns of coalition vs. opposition participation
- **Faction Integration**: Complete faction information for all bill participants
- **Government vs. MK Bills**: Automatic detection and classification of bill origins

**Legislative Process Tracking**:
- **Bill Lifecycle**: From initial proposal through committee review to plenum discussion
- **Document Integration**: Direct links to official documents in multiple formats
- **Merge Analysis**: Complete tracking of bill consolidation and relationships
- **Timeline Analysis**: Processing duration calculations and committee engagement metrics

**Committee Intelligence**:
- **Activity Classification**: Automated committee activity level assessment
- **Historical Context**: Multi-term committee performance tracking  
- **Committee Hierarchy**: Parent-child committee relationships and classifications
- **Session Analysis**: Comprehensive committee session statistics and trends

**Data Export and Analysis**:
- **Comprehensive Coverage**: 65+ columns per bill with complete relational data
- **Filtered Results**: Local Knesset filtering within query results
- **Format Flexibility**: Support for various output formats and analysis tools
- **Research Ready**: Academic and policy research optimized data structure

### Database Schema Changes
1. Update table definitions in `src/config/database.py`
2. Modify repository layer in `src/data/repositories/`
3. Update fetch logic in `src/backend/fetch_table.py`

## Environment Setup

### Python Environment
- **Python 3.12+** required
- Virtual environment in `.venv/`
- Dependencies in `requirements.txt`

### Database
- DuckDB 1.2.2 with analytical extensions
- Parquet files for efficient storage
- Automatic connection management

### Development Tools
- pytest for testing with 60%+ coverage requirement
- Optional: black, flake8, isort, mypy for code quality
- Streamlit for UI development
- asyncio for concurrent operations

## Session State Management

The application uses **centralized session state** in `src/ui/state/session_manager.py`:
- Type-safe accessors and mutators
- Centralized state initialization
- Proper encapsulation of state changes

## Error Handling

**Layered approach**:
1. **API Layer**: Categorizes external API errors
2. **Circuit Breaker**: Prevents cascade failures
3. **Service Layer**: Business logic error handling
4. **UI Layer**: User-friendly error messages

## Testing Strategy

- **Unit Tests**: Component isolation with dependency injection
- **Integration Tests**: End-to-end flows with real database
- **Performance Tests**: Large dataset handling
- **Edge Case Tests**: Boundary conditions and error scenarios

## Key Performance Considerations

- **Connection Pooling**: Reuse database connections
- **Async Operations**: Non-blocking API calls
- **Lazy Loading**: Services instantiated when needed
- **Query Optimization**: Parameterized queries with indexing
- **Memory Management**: Proper resource cleanup

## Common Issues

1. **ModuleNotFoundError**: Always set `PYTHONPATH="./src"` for CLI commands
2. **Database Locks**: Connection leaks - restart application if needed
3. **Timestamp Errors**: Re-fetch problematic tables after upgrades
4. **Missing Data**: Some large tables may fail in bulk - fetch individually

## Migration and Legacy Notes

### Deprecated Features (Still Functional)
- `plot_generators.py`: Use `ui.charts.factory.ChartFactory` or `ui.services.chart_service.ChartService`
- `fetch_table.py`: Use new modular data services
- Direct `connect_db()`: Use `get_db_connection()` context manager

### Feature Flags
- `ENABLE_LEGACY_COMPATIBILITY = True`: Maintains backward compatibility
- Legacy imports show deprecation warnings but still work

### Current Migration Status
- ✅ Main UI refactoring complete (80% code reduction)
- 🔄 Chart system migration in progress
- ⏳ Legacy cleanup pending full testing

## Important Notes

- This is a **defensive security application** for comprehensive parliamentary data analysis
- All database operations are **read-only** by default with secure connection management
- SQL queries are **parameterized** to prevent injection attacks
- **Circuit breaker** prevents API abuse and handles failures gracefully
- **Clean architecture** enables testing, maintainability, and feature expansion
- **Modular design** supports incremental improvements and comprehensive data integration
- **Research-grade data quality** with complete schema coverage and relationship mapping
- **Political analysis capabilities** provide coalition/opposition insights and cross-party collaboration metrics
- **Legislative process intelligence** tracks bills from initiation through passage with complete document integration
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
2. **Storage**: Store in DuckDB warehouse + Parquet files
3. **Processing**: Business logic in service layer with committee session analysis
4. **Presentation**: Interactive Streamlit UI with 15+ visualizations and committee timeline insights

## Key Data Tables

**Critical Tables** (required for full functionality):
- `KNS_PersonToPosition`: Links people to positions/factions
- `KNS_Query`: Parliamentary questions and queries
- `KNS_Person`: Members of Knesset information
- `KNS_Faction`: Political parties and factions
- `KNS_Agenda`: Parliamentary agenda items
- `KNS_BillInitiator`: Bill initiators with `Ordinal` field for main/supporting distinction

**Supporting Tables**:
- `KNS_Committee`, `KNS_CommitteeSession`: Committee data and session history
- `KNS_PlenumSession`, `KNS_PlmSessionItem`: Plenum session data and agenda items
- `KNS_GovMinistry`: Government ministries
- `KNS_Status`: Various status codes
- `KNS_Bill`, `KNS_BillInitiator`: Legislative data with initiator information
- `KNS_Law`, `KNS_IsraelLaw`: Legal documents

**Special Tables**:
- `UserFactionCoalitionStatus`: Manual coalition/opposition tracking (CSV-based)

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

### Committee Session Data Fields

The "Bills + Full Details" query includes the following committee-related columns:

**Core Committee Information**:
- `BillCommitteeID`: The ID of the committee assigned to the bill
- `CommitteeName`: Name of the assigned committee (e.g., "ועדת החוקה, חוק ומשפט")

**Session Statistics**:
- `CommitteeTotalSessions`: Total number of sessions held by the committee

**Timeline Data**:
- `CommitteeFirstSession`: Date of the committee's first recorded session (YYYY-MM-DD format)
- `CommitteeLastSession`: Date of the committee's last recorded session (YYYY-MM-DD format)
- `CommitteeKnessetSpan`: Number of different Knesset terms the committee was active
- `DaysFromPublicationToLastCommitteeSession`: Days between bill publication and committee's last session

**Analysis Fields**:
- `CommitteeActivityLevel`: Classification based on session count:
  - "Very Active": 100+ sessions
  - "Active": 50-99 sessions  
  - "Moderate": 20-49 sessions
  - "Limited": 1-19 sessions
  - "No Committee": Bill has no committee assignment

**Calculation Methodology**:
- **Timeline Analysis**: Uses bill publication date from `KNS_Bill.PublicationDate` field to calculate processing timelines
- **Activity Classification**: Based on total session counts with realistic thresholds

**Data Quality Considerations**:
- Committee assignment coverage: 99.8% success rate for bills with committee assignments
- Historical data may be incomplete for older Knesset terms
- Edge cases with missing timeline data show appropriate fallback values (NULL or 0)

**Usage Notes**:
- Committee data shows historical activity patterns for the assigned committee
- Timeline analysis helps understand bill processing context
- Activity levels provide quick assessment of committee engagement
- NULL values indicate bills without committee assignments

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

- This is a **defensive security application** for parliamentary data analysis
- All database operations are **read-only** by default
- SQL queries are **parameterized** to prevent injection
- **Circuit breaker** prevents API abuse and handles failures gracefully
- **Clean architecture** enables testing and maintainability
- **Modular design** supports incremental improvements and maintenance
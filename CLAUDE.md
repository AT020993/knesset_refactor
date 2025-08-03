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

# Download complete committee session data (RECOMMENDED for full accuracy)
# This downloads 74,951 committee session items for accurate bill-to-session mapping
python download_committee_sessions.py

# Refresh committee data with historical coverage (requires KnessetNum filtering)
# Note: Default fetch only gets recent committees. For complete historical data,
# use manual KnessetNum filtering as implemented in the improved committee resolution.

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
4. **Presentation**: Interactive Streamlit UI with 18+ visualizations and enhanced query capabilities

## Bill Initiator Analysis System

### New Bill Analytics Charts (2025-01-03)

The platform now includes three complementary bill initiator charts that provide different perspectives on legislative activity:

#### 1. Top 10 Bill Initiators (`plot_top_bill_initiators`)
- **Purpose**: Shows individual MKs with the highest number of initiated bills
- **Data**: Individual MK names with their total bill count
- **Example**: ואליד אלהואשלה (275 bills), אחמד טיבי (233 bills)
- **Location**: `src/ui/charts/comparison.py:767`

#### 2. Bill Initiators by Faction (`plot_bill_initiators_by_faction`) 
- **Purpose**: Shows count of MKs per faction who initiated at least one bill
- **Data**: Faction names with count of unique MKs who are bill initiators
- **Example**: Likud (29 MKs), Yesh Atid (25 MKs)
- **Location**: `src/ui/charts/comparison.py:951`

#### 3. Total Bills per Faction (`plot_total_bills_per_faction`)
- **Purpose**: Shows total number of bills initiated by all MKs in each faction
- **Data**: Faction names with cumulative bill count from all members
- **Example**: Likud (1,180 total bills), Yesh Atid (984 total bills)
- **Location**: `src/ui/charts/comparison.py:1124`

### Technical Implementation

**Chart Architecture**:
- All charts extend `BaseChart` class with consistent error handling
- Support single/multiple Knesset filtering with appropriate SQL query structure
- Use faction filtering for drill-down analysis
- Implement proper sorting and data aggregation

**SQL Query Logic**:
- **Single Knesset**: Simplified queries without KnessetNum in SELECT/GROUP BY
- **Multiple Knessets**: Include KnessetNum for proper aggregation across terms
- **Main Initiator Filter**: `bi.Ordinal = 1` ensures only primary initiators counted
- **Faction Resolution**: LEFT JOIN with `KNS_PersonToPosition` and `KNS_Faction`

**UI Integration**:
- Registered in `ChartFactory` comparison charts list
- Legacy wrappers in `plot_generators.py` for backward compatibility
- Chart service methods in `chart_service.py` for clean API
- Available in "Bills Analytics" section of UI

### Chart Spacing Optimization

All bill initiator charts use enhanced spacing to prevent text label cutoff:
- **Height**: 800px (increased from default 600px)
- **Top Margin**: 180px (increased from default 100px)
- **Layout**: Optimized for displaying numbers above bars without truncation

### Data Relationships Explained

Understanding the three different metrics:

1. **Individual Bills** (275): One highly active MK's personal contribution
2. **MK Count** (29): Number of different people in a faction who initiated bills  
3. **Total Bills** (1,180): Sum of all bills from all MKs in that faction

**Mathematical Relationship**: 
- Total Bills per Faction = Sum of all individual MK bills in that faction
- MK Count per Faction = Count of unique MKs with at least 1 bill in that faction
- Average Bills per MK = Total Bills ÷ MK Count

## Memory Notes

### Chart Filtering Strategy (IMPLEMENTED)
- **Bill Charts**: All bill analytics charts use simplified filtering with only Knesset number filter
- **No Additional Filters**: Bill Type, Bill Status, and other advanced filters removed from bill charts
- **Implementation**: `src/ui/pages/plots_page.py:337` - `_render_advanced_filters()` skips all bill charts
- **User Preference**: Simplified interface reduces complexity and focuses on core Knesset selection

### Affected Charts
All charts containing "Bill" or "Bills" in the name now use simplified filtering:
- Bill Status Distribution
- Bills by Time Period  
- Bill SubType Distribution
- Bills per Faction
- Bills by Coalition Status
- Top 10 Bill Initiators
- Bill Initiators by Faction
- Total Bills per Faction

### Chart Spacing Standards
- **Height**: 800px for all bill charts (ensures adequate space for data labels)
- **Top Margin**: 180px (prevents number cutoff above bars)
- **Implementation**: Applied to all new bill initiator charts for consistent UX
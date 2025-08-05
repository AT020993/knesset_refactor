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
4. **Presentation**: Interactive Streamlit UI with 21+ visualizations including network analysis and enhanced query capabilities

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

### Network Charts
Network collaboration charts use Knesset filtering only (no additional filters):
- MK Collaboration Network
- Faction Collaboration Network
- Coalition/Opposition Network

### Chart Spacing Standards
- **Height**: 800px for all bill charts (ensures adequate space for data labels)
- **Top Margin**: 180px (prevents number cutoff above bars)
- **Implementation**: Applied to all new bill initiator charts for consistent UX

## Bill Timeline & Submission Date Analysis (2025-08-05)

### Enhanced Bill Queries with FirstBillSubmissionDate

The predefined "Bills + Full Details" query now includes a new `FirstBillSubmissionDate` column that provides accurate bill submission dates with proper chronological ordering.

#### Background & Problem
- **Original Issue**: The deprecated `KNS_BillHistoryInitiator` table contained outdated data (last updated 2015, data only until 2012)
- **Chronological Inconsistency**: Committee sessions were appearing before plenum discussions, creating logical inconsistencies
- **Data Quality**: Only 1.8% coverage with unreliable dates vs current needs

#### Solution: Multi-Source Date Resolution

**Implementation Location**: `src/ui/queries/predefined_queries.py` - `BillFirstSubmission` CTE

**Logic**: Find the **earliest activity date** across all bill-related sources:

```sql
BillFirstSubmission AS (
    SELECT 
        B.BillID,
        MIN(earliest_date) as FirstSubmissionDate
    FROM KNS_Bill B
    LEFT JOIN (
        -- 1. Initiator assignment dates (often the true submission)
        SELECT BI.BillID, MIN(CAST(BI.LastUpdatedDate AS TIMESTAMP)) as earliest_date
        FROM KNS_BillInitiator BI WHERE BI.LastUpdatedDate IS NOT NULL GROUP BY BI.BillID
        
        UNION ALL
        
        -- 2. Committee session dates
        SELECT csi.ItemID as BillID, MIN(CAST(cs.StartDate AS TIMESTAMP)) as earliest_date
        FROM KNS_CmtSessionItem csi JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
        WHERE csi.ItemID IS NOT NULL AND cs.StartDate IS NOT NULL GROUP BY csi.ItemID
        
        UNION ALL
        
        -- 3. Plenum session dates
        SELECT psi.ItemID as BillID, MIN(CAST(ps.StartDate AS TIMESTAMP)) as earliest_date
        FROM KNS_PlmSessionItem psi JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
        WHERE psi.ItemID IS NOT NULL AND ps.StartDate IS NOT NULL GROUP BY psi.ItemID
        
        UNION ALL
        
        -- 4. Publication dates
        SELECT B.BillID, CAST(B.PublicationDate AS TIMESTAMP) as earliest_date
        FROM KNS_Bill B WHERE B.PublicationDate IS NOT NULL
    ) all_dates ON B.BillID = all_dates.BillID
    WHERE all_dates.earliest_date IS NOT NULL
    GROUP BY B.BillID
)
```

#### Results & Validation

**✅ Chronological Accuracy**: Ensures proper timeline order
- Submission Date ≤ Committee Date ≤ Plenum Date

**✅ Excellent Coverage**: 98.2% of bills (57,171 out of 58,190)

**✅ Current Data**: Up to 2025 with real-time updates

**✅ Example Timeline** (Bill 2220461):
- **Submission**: 2024-07-09 (initiator assigned to bill)
- **Committee**: 2025-03-23 (first committee session)
- **Plenum**: 2025-06-03 (first formal discussion)

#### Israeli Legislative Process Understanding

The solution correctly models the Israeli Knesset legislative workflow:

1. **Bill Submission** (`FirstBillSubmissionDate`): When initiators are assigned and bill enters system
2. **Committee Review**: Preliminary examination and markup (can be months later)
3. **Plenum Discussion**: Formal parliamentary debate and voting

This multi-stage process explains why committee sessions can occur significantly before plenum discussions - it's the correct legislative sequence, not a data error.

#### Data Sources Priority

The algorithm prioritizes dates in this logical order:
1. **KNS_BillInitiator.LastUpdatedDate**: Most reliable indicator of true submission
2. **Committee/Plenum session dates**: When legislative activity begins
3. **PublicationDate**: Final fallback for published bills

This approach provides the most accurate representation of when bills actually entered the legislative process.

## Collaboration Network Analysis System

### Network/Connection Map Charts (2025-08-05)

The platform includes three interactive network visualization charts that analyze collaboration patterns in Israeli parliamentary bill legislation:

#### 1. MK Collaboration Network (`plot_mk_collaboration_network`)
- **Purpose**: Visualizes individual MK collaboration patterns through bill co-sponsorship
- **Data**: Shows which MKs collaborate most frequently as primary and supporting bill initiators
- **Visualization**: Interactive network with nodes (MKs) and edges (collaboration relationships)
- **Features**:
  - Node size reflects total bills initiated by each MK
  - Node colors group MKs by faction affiliation
  - Minimum collaboration threshold (configurable, default 3+ bills)
  - Circular network layout with hover details
- **Location**: `src/ui/charts/network.py:21`

#### 2. Faction Collaboration Network (`plot_faction_collaboration_network`)
- **Purpose**: Shows inter-faction collaboration patterns in bill initiation
- **Data**: Analyzes which factions work together across party lines on legislative initiatives
- **Visualization**: Network showing faction-to-faction collaboration relationships
- **Features**:
  - Nodes represent political factions
  - Edges show collaboration strength between factions
  - Minimum collaboration threshold (configurable, default 5+ bills)
  - Coalition/opposition status integration
- **Location**: `src/ui/charts/network.py:116`

#### 3. Coalition/Opposition Network (`plot_coalition_opposition_network`)
- **Purpose**: Analyzes cross-party cooperation between coalition and opposition members
- **Data**: Identifies bills where coalition and opposition MKs collaborate
- **Visualization**: Specialized network highlighting bi-partisan legislative cooperation
- **Features**:
  - Focuses specifically on coalition-opposition collaboration
  - Shows faction-level details in annotations
  - Minimum collaboration threshold (configurable, default 3+ bills)
  - Cross-party cooperation metrics
- **Location**: `src/ui/charts/network.py:191`

### Technical Implementation

**Network Chart Architecture**:
- Built on the modular chart system extending `BaseChart`
- Uses Plotly interactive network visualizations with circular layouts
- Implements robust Hebrew text handling for faction names
- Database queries analyze `KNS_BillInitiator` table with `Ordinal` field for proper collaboration detection

**SQL Analysis Logic**:
- **Primary Initiators**: `main.Ordinal = 1` (bill sponsors)
- **Supporting Initiators**: `supp.Ordinal > 1` (co-sponsors)
- **Collaboration Detection**: JOIN on same BillID between primary and supporting initiators
- **Faction Mapping**: LEFT JOIN with `KNS_PersonToPosition` and `KNS_Faction` for proper faction resolution
- **Coalition Status**: Integration with `UserFactionCoalitionStatus` for cross-party analysis

**Data Processing Features**:
- Configurable minimum collaboration thresholds
- Proper handling of Hebrew faction names and MK names
- Error handling and fallback visualizations
- Node sizing based on activity levels (total bills initiated)
- Color coding by faction or coalition status

**UI Integration**:
- Available in "Collaboration Networks" section of Streamlit interface
- Registered in `ChartFactory` under "network" category
- Service layer methods in `chart_service.py`
- Legacy compatibility wrappers in `plot_generators.py`

### Network Visualization Features

**Interactive Elements**:
- **Hover Information**: Shows collaboration counts, faction affiliations, and bill totals
- **Node Sizing**: Proportional to legislative activity (total bills initiated)
- **Color Grouping**: Visual distinction by faction (MK networks) or coalition status
- **Circular Layout**: Optimized for readability with proper spacing
- **Legend Support**: Faction-based legends for easy identification

**Error Handling**:
- Robust Hebrew text processing with safe string conversion
- Fallback visualizations when data processing fails
- Database connection error handling
- Empty data state management

### Use Cases

**Research Applications**:
- **Political Science**: Study cross-party collaboration patterns in parliamentary systems
- **Legislative Analysis**: Identify most collaborative MKs and factions
- **Coalition Dynamics**: Analyze bi-partisan cooperation trends
- **Faction Relationships**: Understand inter-faction legislative partnerships

**Data Insights**:
- Which MKs work together most frequently across party lines
- How often coalition and opposition collaborate on legislation
- Which factions have the strongest inter-party relationships
- Patterns of legislative cooperation by Knesset term
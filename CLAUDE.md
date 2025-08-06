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

## Collaboration Network Analysis System (2025-08-06 - Updated)

### Network/Connection Map Charts 

The platform includes three interactive visualization charts that analyze collaboration patterns in Israeli parliamentary bill legislation:

#### 1. MK Collaboration Network (`plot_mk_collaboration_network`)
- **Purpose**: Visualizes individual MK collaboration patterns through bill co-sponsorship
- **Data**: Shows which MKs collaborate most frequently as primary and supporting bill initiators
- **Visualization**: Interactive force-directed network with enhanced node sizing and visibility
- **Features**:
  - **Enhanced Node Sizing**: 20-80px nodes based on total bills initiated (dramatically improved visibility)
  - **Force-Directed Layout**: Dynamic positioning that clusters connected nodes for better readability
  - **Faction-Based Coloring**: Visual distinction by political faction with comprehensive legend
  - **Improved Text Rendering**: Black text on colored backgrounds for maximum readability
  - **Advanced Faction Mapping**: Multi-level resolution with fallback logic achieving 100% faction coverage
  - **Minimum collaboration threshold**: Configurable (default 3+ bills)
  - **Interactive Hover**: Detailed information including collaboration counts and bill totals
- **Technical Improvements**: Fixed gray node issues through enhanced SQL faction resolution
- **Location**: `src/ui/charts/network.py:21`

#### 2. Faction Collaboration Network (`plot_faction_collaboration_network`)
- **Purpose**: Shows inter-faction collaboration patterns with accurate bill counting
- **Data**: Analyzes which factions work together across party lines on legislative initiatives  
- **Visualization**: Network showing faction-to-faction collaboration relationships with proper node sizing
- **Key Fixes (2025-08-06)**:
  - **Accurate Bill Counting**: Uses `COUNT(DISTINCT bi.BillID)` to prevent double-counting when multiple MKs from same faction initiate the same bill
  - **Enhanced Node Sizing**: 30-100px nodes based on total bills per faction (not collaboration count)
  - **Coalition/Opposition Layout**: Clear visual separation between political alignments
  - **Comprehensive Hover Information**: Shows total bills, collaborations, and partner faction counts
- **Features**:
  - Node sizes reflect **total unique bills** initiated by each faction
  - Color coding by coalition status (Coalition=Blue, Opposition=Orange, Unknown=Gray)
  - Minimum collaboration threshold (configurable, default 5+ bills)  
  - Force-directed positioning for optimal readability
- **Data Quality**: Proper faction-to-person mapping ensuring accurate bill attribution
- **Location**: `src/ui/charts/network.py:112`

#### 3. Faction Coalition Breakdown (`plot_faction_coalition_breakdown`) 
- **Purpose**: Shows percentage breakdown of each faction's collaborations with Coalition vs Opposition partners
- **Data**: Analyzes collaboration patterns by political alignment rather than network topology
- **Visualization**: Horizontal stacked bar chart showing Coalition vs Opposition collaboration percentages
- **Features**:
  - **Stacked Percentage Bars**: Visual breakdown of collaboration distribution
  - **Clean Black Annotations**: "Total: X" numbers on right side in black text (no political color confusion)
  - **Whole Number Display**: Clean integers without decimal places (e.g., "Total: 45" not "Total: 45.0")
  - **Coalition Status Integration**: Blue bars for Coalition collaborations, Orange for Opposition
  - **Dynamic Height**: Chart scales based on number of factions
  - **Enhanced Spacing**: Proper title/legend positioning to prevent overlap
- **Replaces**: Previously removed coalition/opposition network chart (2025-08-06)
- **Use Case**: Understanding cross-party cooperation patterns by political alignment
- **Location**: `src/ui/charts/network.py:235`

### Technical Implementation (Updated 2025-08-06)

**Network Chart Architecture**:
- Built on the modular chart system extending `BaseChart`
- Uses Plotly interactive network visualizations with force-directed and coalition-based layouts
- Implements robust Hebrew text handling with safe string conversion
- Advanced SQL queries with multi-level faction resolution and double-counting prevention

**Enhanced SQL Analysis Logic**:
- **Primary Initiators**: `main.Ordinal = 1` (bill sponsors)
- **Supporting Initiators**: `supp.Ordinal > 1` (co-sponsors)
- **Collaboration Detection**: JOIN on same BillID between primary and supporting initiators
- **Advanced Faction Mapping**: Multi-level COALESCE queries with fallback logic:
  ```sql
  COALESCE(
      -- Try current Knesset first
      (SELECT f.Name FROM KNS_PersonToPosition ptp 
       JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
       WHERE ptp.PersonID = person.PersonID AND ptp.KnessetNum = current_knesset
       ORDER BY ptp.StartDate DESC LIMIT 1),
      -- Fallback to most recent faction
      (SELECT f.Name FROM KNS_PersonToPosition ptp
       JOIN KNS_Faction f ON ptp.FactionID = f.FactionID  
       WHERE ptp.PersonID = person.PersonID
       ORDER BY ptp.KnessetNum DESC, ptp.StartDate DESC LIMIT 1)
  )
  ```
- **Bill Counting with Double-Count Prevention**: 
  - Uses `COUNT(DISTINCT bi.BillID)` in `FactionTotalBills` CTE
  - Ensures accurate faction node sizing without duplicate counting
  - Proper bill-to-faction attribution through KnessetNum matching

**Data Processing Features**:
- **Collaboration Relationship Definition**: Minimum threshold filtering (5+ bills) to show only meaningful partnerships
- **Bidirectional Counting**: Separate tracking of Faction A→B and B→A relationships
- **Enhanced Error Handling**: Comprehensive Hebrew text processing with safe string conversion
- **Node Sizing Algorithms**: 
  - MK Network: 20-80px based on individual bill counts
  - Faction Network: 30-100px based on total faction bills (with double-count prevention)
- **Color Coding Systems**:
  - Faction-based coloring for MK networks (Set3 palette)
  - Coalition status coloring (Blue=Coalition, Orange=Opposition, Gray=Unknown)
  - Black text annotations for clean readability

**UI Integration**:
- Available in "Collaboration Networks" section of Streamlit interface
- Registered in `ChartFactory` under "network" category with updated chart types
- Service layer methods in `chart_service.py` 
- Legacy compatibility wrappers in `plot_generators.py`
- Chart factory registration updated to include `faction_coalition_breakdown`

### Network Visualization Features (Enhanced 2025-08-06)

**Interactive Elements**:
- **Enhanced Hover Information**: 
  - MK Networks: Shows MK name, faction, total bills, and collaboration counts
  - Faction Networks: Displays faction name, coalition status, total bills, collaborations, and partner faction counts
- **Proportional Node Sizing**: 
  - **Dramatically Improved**: 67-167% larger nodes for better visibility
  - MK nodes: 20-80px based on individual legislative activity
  - Faction nodes: 30-100px based on total unique bills (no double-counting)
- **Advanced Color Systems**: 
  - **Faction-based grouping** with comprehensive Set3 palette for MK networks
  - **Coalition status coloring** (Blue/Orange/Gray) for faction networks
  - **Black text rendering** on all backgrounds for maximum readability
- **Dynamic Layouts**: 
  - **Force-directed positioning** for MK networks with faction clustering
  - **Coalition/opposition separation** for faction networks
  - **900x900px charts** for optimal spacing and visibility

**Error Handling & Data Quality**:
- **100% Faction Coverage**: Advanced SQL resolution eliminated gray nodes
- **Double-Count Prevention**: Proper DISTINCT counting prevents faction bill inflation
- **Safe String Conversion**: Robust Hebrew text processing with error fallbacks
- **Database Connection Management**: Comprehensive error handling and fallback states
- **Empty Data Visualization**: Clear messaging when no collaboration data exists

### Collaboration Relationship Definition

**What Constitutes a "Collaboration"**:
1. **Single Bill Collaboration**: MK from Faction A initiates bill (Ordinal=1), MK from Faction B supports (Ordinal>1)
2. **Relationship Threshold**: Only partnerships with 5+ collaborative bills appear in network
3. **Bidirectional Counting**: Faction A→B and B→A counted as separate relationships
4. **Filtering Effect**: 103 relationships above threshold shown, 52 weak relationships (1-4 bills) hidden

**Metrics Explained**:
- **"20 Collaborations"** = 20 different faction-to-faction relationships involving that faction
- **"984 Total Bills"** = All bills where faction members were primary initiators
- **"11 Partner Factions"** = Number of different factions they collaborate with

### Use Cases & Research Applications

**Enhanced Political Analysis**:
- **Coalition Dynamics**: Analyze cross-party cooperation with accurate bill attribution
- **Legislative Productivity**: Compare faction activity levels with proper double-count prevention
- **Collaboration Patterns**: Identify sustained partnerships vs occasional cooperation
- **Political Alignment Analysis**: Understand coalition vs opposition collaboration percentages

**Data-Driven Insights**:
- Which MKs are most collaborative across party lines (accurate node sizing)
- How faction mergers affect collaboration patterns (e.g., הציונות הדתית transitions)
- Strength of coalition unity vs opposition cooperation
- Evolution of political partnerships across Knesset terms
- Impact of minimum collaboration thresholds on network visibility

**Technical Validation**:
- Faction bill counts: Likud (1,180 bills), Yesh Atid (984 bills) - verified accurate
- Collaboration relationship counts properly reflect bidirectional partnerships
- Coalition status integration shows political alignment effects on cooperation

## Recent Network Chart Improvements (2025-08-06)

### Issues Resolved

#### 1. **Faction Node Sizing Fix**
- **Problem**: Faction nodes showed incorrectly small sizes due to using collaboration count instead of total bills
- **Root Cause**: Node sizing was based on `CollaborationCount` (number of relationships) instead of `TotalBills` (actual legislative output)
- **Solution**: 
  - Updated `FactionTotalBills` CTE with `COUNT(DISTINCT bi.BillID)` to prevent double-counting
  - Modified node sizing logic to use `faction['TotalBills']` instead of `faction['CollaborationCount']`
  - Result: Faction nodes now properly reflect legislative activity (e.g., Likud shows 1,180 bills vs previous undercount)

#### 2. **Coalition/Opposition Chart Replacement**
- **Problem**: Original coalition/opposition network chart was inadequate for showing collaboration patterns
- **Solution**: Replaced with `Faction Coalition Breakdown` chart showing percentage split of collaborations
- **Features**: Horizontal stacked bars showing Coalition vs Opposition collaboration percentages per faction

#### 3. **Text Readability Improvements** 
- **Problem**: White text on colored backgrounds was invisible, political color coding caused confusion
- **Solutions**:
  - Changed all text colors from white to black for maximum contrast
  - Updated breakdown chart annotations to use black text instead of political colors
  - Converted decimal numbers to integers (removed ".0" suffixes)

#### 4. **Collaboration Count Clarification**
- **Issue**: User confusion about collaboration metrics (984 bills vs 20 collaborations vs 11 partners)
- **Documentation**: Added comprehensive explanation of bidirectional relationship counting and threshold filtering
- **Understanding**: "20 collaborations" = 20 different faction-pair relationships, not 20 bills

### Files Modified

1. **`src/ui/charts/network.py`**:
   - Lines 1002-1023: Updated faction bill counting logic
   - Lines 1068, 1088: Changed node sizing from CollaborationCount to TotalBills  
   - Lines 1105-1108: Enhanced hover text with both bill counts and collaboration info
   - Lines 1200-1207: Updated breakdown chart text to black color and integer formatting

2. **`src/ui/charts/factory.py`**: Updated chart registration to include `faction_coalition_breakdown`

3. **`src/ui/services/chart_service.py`**: Added service method for new breakdown chart

4. **`src/ui/plot_generators.py`**: Added legacy wrapper for breakdown chart compatibility
# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing
```bash
# Unit tests with coverage
pytest
pytest --cov=src --cov-report=term-missing

# Run specific test categories
pytest -m "not slow"           # Skip slow tests
pytest -m integration          # Run integration tests only
pytest -m performance          # Run performance tests only
pytest -m e2e                  # Run E2E tests only

# End-to-End testing (requires app running)
pip install -r requirements-dev.txt
playwright install --with-deps
streamlit run src/ui/data_refresh.py  # In separate terminal
pytest -m e2e --base-url http://localhost:8501
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

# Export faction coalition mapping CSV for manual status entry
# Creates faction_coalition_mapping.csv with all factions from Knesset 25-1
# File includes Hebrew faction names with proper Excel encoding (UTF-8 BOM)
python -c "
import sys
sys.path.insert(0, 'src')
import csv, duckdb
from pathlib import Path
from config.settings import Settings
with duckdb.connect(str(Settings.DEFAULT_DB_PATH), read_only=True) as con:
    result = con.execute('SELECT KnessetNum, FactionID, Name FROM KNS_Faction WHERE KnessetNum BETWEEN 1 AND 25 ORDER BY KnessetNum DESC, Name ASC').fetchall()
    with open('faction_coalition_mapping.csv', 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['KnessetNum', 'FactionID', 'FactionName', 'CoalitionStatus'])
        for row in result: writer.writerow([row[0], row[1], row[2], ''])
print('✅ faction_coalition_mapping.csv created with 529 faction records (Knesset 25→1)')
"
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

## Bill Origin Filter System (2025-08-18)

### Private vs Governmental Bill Filtering

The platform now includes comprehensive filtering capabilities to distinguish between private member bills and governmental bills across all bill visualizations.

#### Filter Options

**Bill Origin Filter** (`Bill Filters` section):
- **All Bills** (default): Shows both private and governmental bills
- **Private Bills Only**: Shows only bills initiated by MKs (PrivateNumber IS NOT NULL)
- **Governmental Bills Only**: Shows only bills initiated by government (PrivateNumber IS NULL)

#### Data Distribution

**Knesset 25 Example**:
- **Total Bills**: 6,459 (100%)
- **Private Bills**: 5,975 (92.5%) - Member-initiated legislation
- **Governmental Bills**: 484 (7.5%) - Government-initiated legislation

#### Implementation Details

**Technical Architecture**:
- **Filter Location**: `src/ui/pages/plots_page.py` - `_render_bill_filters()` method
- **Base Class Logic**: `src/ui/charts/base.py` - `_add_advanced_filters()` method  
- **SQL Condition**: Uses `PrivateNumber` field to distinguish bill origins
- **Chart Integration**: All bill charts updated to support `bill_origin_filter` parameter

**Filter Logic**:
```python
# Base class filter building
def _add_advanced_filters(self, filters: dict, prefix: str = '', **kwargs):
    bill_origin_filter = kwargs.get('bill_origin_filter', 'All Bills')
    if bill_origin_filter == 'Private Bills Only':
        filters['bill_origin_condition'] = f'{prefix}PrivateNumber IS NOT NULL'
    elif bill_origin_filter == 'Governmental Bills Only':
        filters['bill_origin_condition'] = f'{prefix}PrivateNumber IS NULL'
    else:
        filters['bill_origin_condition'] = '1=1'  # No filter
```

**Chart Coverage**:
All bill analytics charts now support origin filtering:
- Bill Status Distribution
- Bill SubType Distribution  
- Bills by Time Period
- Bills per Faction
- Bills by Coalition Status
- Top 10 Bill Initiators
- Bill Initiators by Faction
- Total Bills per Faction

#### User Interface

**Filter Access**:
1. Navigate to **"📈 Predefined Visualizations"**
2. Select **"Bills Analytics"** topic
3. Choose any bill chart from the dropdown
4. Select desired Knesset number
5. **"Bill Filters"** section appears with **"Bill Origin"** dropdown

**Filter Validation**:
- **Mathematical Accuracy**: Private Bills + Governmental Bills = Total Bills ✅
- **Real-time Updates**: Filter changes immediately update visualizations
- **State Persistence**: Filter selection maintained across chart switches

#### Research Applications

**Legislative Analysis**:
- **Member Initiative Patterns**: Analyze individual MK legislative productivity
- **Government vs Opposition**: Compare government vs private member bill success rates
- **Coalition Dynamics**: Study how coalition status affects private bill initiation
- **Historical Trends**: Track evolution of government vs private member legislation

**Data-Driven Insights**:
- Private member bills constitute ~92.5% of legislative proposals
- Government bills show higher passage rates (59.5% vs 2.4% for private bills)
- Coalition MKs initiate significantly more private bills than opposition
- Bill origin strongly correlates with legislative success probability

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

## Collaboration Network Analysis System (2025-10-04 - Updated)

### Network/Connection Map Charts

The platform includes four interactive visualization charts that analyze collaboration patterns in Israeli parliamentary bill legislation. These charts are integrated into the **Bills Analytics** section for streamlined bill-related analysis.

#### 1. MK Collaboration Network (`plot_mk_collaboration_network`)
- **Purpose**: Visualizes individual MK collaboration patterns through bill co-sponsorship
- **Data**: Shows which MKs collaborate most frequently as primary and supporting bill initiators
- **Visualization**: Interactive force-directed network with enhanced node sizing and visibility
- **Features**:
  - **Enhanced Node Sizing**: 20-80px nodes based on total bills initiated (dramatically improved visibility)
  - **Force-Directed Layout**: Dynamic positioning that clusters connected nodes for better readability
  - **Faction-Based Coloring**: Visual distinction by political faction with comprehensive legend (gold for Independent MKs)
  - **Improved Text Rendering**: Black text on colored backgrounds for maximum readability
  - **Knesset-Specific Faction Resolution** (2025-10-04): Only shows factions relevant to selected Knesset(s)
  - **Minimum collaboration threshold**: Configurable (default 3+ bills)
  - **Interactive Hover**: Detailed information including collaboration counts and bill totals
- **Key Fixes (2025-10-04)**:
  - **Eliminated Grey Circles**: Fixed NULL faction assignment through improved SQL query with `AllRelevantPeople` CTE
  - **Guaranteed Faction Assignment**: Every MK gets either their actual faction or 'Independent' - no NULL values
  - **Extended Color Palette**: Multiple Plotly color sets ensure sufficient unique colors for all factions
- **Location**: `src/ui/charts/network.py:22`

#### 2. Faction Collaboration Network (`plot_faction_collaboration_network`)
- **Purpose**: Shows inter-faction collaboration patterns with accurate bill counting
- **Data**: Analyzes which factions work together across party lines on legislative initiatives
- **Visualization**: Network showing faction-to-faction collaboration relationships with proper node sizing
- **Key Fixes**:
  - **Accurate Bill Counting** (2025-08-06): Uses `COUNT(DISTINCT bi.BillID)` to prevent double-counting when multiple MKs from same faction initiate the same bill
  - **Enhanced Node Sizing** (2025-08-06): 30-100px nodes based on total bills per faction (not collaboration count)
  - **Knesset-Specific Faction Resolution** (2025-10-04): Removed fallback logic that pulled factions from other Knessets
  - **Coalition/Opposition Layout**: Clear visual separation between political alignments
  - **Comprehensive Hover Information**: Shows total bills, collaborations, and partner faction counts
- **Features**:
  - Node sizes reflect **total unique bills** initiated by each faction
  - Color coding by coalition status (Coalition=Blue, Opposition=Orange, Unknown=Gray)
  - Minimum collaboration threshold (configurable, default 5+ bills)
  - Force-directed positioning for optimal readability
- **Data Quality**: Proper faction-to-person mapping ensuring accurate bill attribution, strictly limited to selected Knesset(s)
- **Location**: `src/ui/charts/network.py:137`

#### 3. Faction Collaboration Matrix (`plot_faction_collaboration_matrix`)
- **Purpose**: Heatmap showing collaboration intensity between all faction pairs
- **Data**: Matrix view of faction-to-faction collaboration patterns with solo bill tracking
- **Visualization**: Interactive heatmap with color intensity representing collaboration frequency
- **Key Fixes (2025-10-04)**:
  - **Knesset-Specific Faction Resolution**: Removed COALESCE fallback logic across all CTEs
  - **Accurate Faction Filtering**: Only shows factions active in selected Knesset(s)
- **Features**:
  - Enhanced matrix showing both collaborations and solo bills per faction
  - Minimum collaboration threshold (configurable, default 3+ bills)
  - Solo bill tracking option (show/hide)
  - Color-coded heatmap for visual pattern recognition
- **Location**: `src/ui/charts/network.py:1267`

#### 4. Faction Coalition Breakdown (`plot_faction_coalition_breakdown`)
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
- **Location**: `src/ui/charts/network.py:254`

### Technical Implementation (Updated 2025-10-04)

**Network Chart Architecture**:
- Built on the modular chart system extending `BaseChart`
- Uses Plotly interactive network visualizations with force-directed and coalition-based layouts
- Implements robust Hebrew text handling with safe string conversion
- Advanced SQL queries with Knesset-specific faction resolution and double-counting prevention

**Enhanced SQL Analysis Logic (2025-10-04)**:
- **Primary Initiators**: `main.Ordinal = 1` (bill sponsors)
- **Supporting Initiators**: `supp.Ordinal > 1` (co-sponsors)
- **Collaboration Detection**: JOIN on same BillID between primary and supporting initiators
- **Knesset-Specific Faction Mapping**: Strict single-query approach (NO fallback logic):
  ```sql
  -- MK Collaboration Network approach
  AllRelevantPeople AS (
      SELECT DISTINCT PersonID, KnessetNum
      FROM BillCollaborations
  ),
  MKFactionInKnesset AS (
      SELECT
          arp.PersonID,
          arp.KnessetNum,
          COALESCE(
              (SELECT f.Name
               FROM KNS_PersonToPosition ptp
               JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
               WHERE ptp.PersonID = arp.PersonID
                   AND ptp.KnessetNum = arp.KnessetNum
               ORDER BY ptp.StartDate DESC LIMIT 1),
              'Independent'
          ) as FactionName
      FROM AllRelevantPeople arp
  )
  ```
- **Key Change**: Removed COALESCE fallback that searched other Knessets - now strictly matches bill's KnessetNum
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
- Available in "Bills Analytics" section of Streamlit interface
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

## Recent Updates (August 2025)

### End-to-End Testing Implementation

A comprehensive E2E testing suite has been implemented using Playwright:

#### Test Coverage (7/7 tests passing ✅)
- **Main page loading and header verification**
- **Data refresh controls functionality**
- **Predefined queries section**
- **Sidebar navigation**
- **Error handling with invalid inputs**
- **Responsive design (mobile viewport)**
- **Page load performance**

#### CI/CD Integration
- **Automated Testing**: E2E tests run automatically in GitHub Actions
- **Browser Support**: Tests run on Chromium, Firefox, and WebKit
- **Quality Gates**: All tests must pass before merge
- **Screenshot Capture**: Automatic screenshot capture on test failures

### Project Cleanup & Maintenance

#### Files Removed
- **Legacy Backup Files**: `legacy_backup/` directory removed
- **Generated Files**: Coverage reports, log files, HTML reports
- **Unused Scripts**: Old fetch scripts superseded by modular backend
- **Outdated Documentation**: Refactoring guides, outdated setup docs
- **Docker Files**: Unused Docker configuration files

#### Files Preserved
- ✅ **All visualization and chart functionality**
- ✅ **Core application features**
- ✅ **Essential documentation** (README.md, CLAUDE.md, ARCHITECTURE.md)
- ✅ **Data processing capabilities**
- ✅ **Interactive UI components**

### Quality Improvements
- **Code Coverage**: Maintained 60%+ coverage requirement
- **Performance**: Application startup and response times optimized
- **Maintainability**: Cleaner project structure with focused files
- **Documentation**: Updated all documentation to reflect current state

### Faction Coalition Mapping System (2025-08-18)

#### Manual Coalition Status Management

The platform now supports external coalition status management through a CSV-based approach for manual political affiliation assignment:

**CSV Export Functionality**:
- **File**: `faction_coalition_mapping.csv` - Master reference for faction political alignments
- **Coverage**: Complete historical data from Knesset 1 to Knesset 25 (529 faction records)
- **Structure**: KnessetNum | FactionID | FactionName | CoalitionStatus (manual entry)
- **Encoding**: UTF-8 with BOM for proper Hebrew display in Excel
- **Sort Order**: Descending by Knesset (25→1) for current-first data entry

**Key Features**:
- **Excel Compatibility**: Hebrew faction names display correctly in Microsoft Excel
- **Historical Completeness**: Every faction from every Knesset term included
- **Manual Control**: Column D (CoalitionStatus) left empty for user political categorization
- **Easy Generation**: One-line command creates complete CSV export

**Usage Workflow**:
1. Run CSV export command to generate `faction_coalition_mapping.csv`
2. Open file in Excel (Hebrew names display properly)
3. Manually enter coalition status for each faction-Knesset combination
4. Use completed CSV as reference for political analysis queries

**Data Quality**:
- **Comprehensive Coverage**: No missing factions from any Knesset term
- **Accurate Metadata**: Correct FactionID and KnessetNum for database integration
- **User Control**: Manual entry ensures political accuracy over automated classification
- **Excel Integration**: Proper encoding eliminates character display issues

This system provides the foundation for accurate coalition vs opposition analysis while maintaining full user control over political categorization decisions.

## Recent Updates (2025-10-04)

### Collaboration Network Improvements and Chart Reorganization

#### Knesset-Specific Faction Filtering
- **Issue Resolved**: Collaboration charts were showing factions from other Knessets when a specific Knesset was selected
- **Root Cause**: SQL queries used COALESCE fallback logic that searched across all Knessets when faction not found in selected Knesset
- **Solution**: Removed all fallback logic - faction resolution now strictly limited to bill's KnessetNum
- **Impact**: All network charts (MK, Faction, Matrix) now show only factions active in selected Knesset(s)
- **Files Modified**: `src/ui/charts/network.py`

#### Grey Circles Fix in MK Collaboration Network
- **Issue Resolved**: Grey circles appeared for MKs without faction assignments in selected Knesset
- **Root Cause**: SQL GROUP BY clause wasn't properly handling NULL faction values despite COALESCE
- **Solution**:
  - Added `AllRelevantPeople` CTE to track collaborators with their Knesset numbers
  - Rewrote `MKFactionInKnesset` to guarantee every MK gets a faction assignment
  - Changed GROUP BY to use resolved faction name, eliminating NULL issues
  - Added gold color (#FFD700) for 'Independent' MKs
  - Extended color palette with multiple Plotly color sets
- **Result**: Every MK node now has proper coloring based on faction or shows as gold if independent
- **Files Modified**: `src/ui/charts/network.py:63-99, 884-894, 962-964`

#### Chord Chart Removal
- **Action**: Removed Faction Collaboration Chord chart from platform
- **Reason**: Simplified visualization options, focusing on more effective chart types
- **Charts Removed**: `plot_faction_collaboration_chord()` and `_create_faction_chord_chart()`
- **Files Modified**:
  - `src/ui/charts/network.py`
  - `src/ui/charts/factory.py`
  - `src/ui/plot_generators.py`
  - `src/ui/services/chart_service.py`

#### Chart Reorganization
- **Change**: Moved all collaboration network charts from standalone "Collaboration Networks" section to "Bills Analytics" section
- **Charts Affected**:
  - MK Collaboration Network
  - Faction Collaboration Network
  - Faction Collaboration Matrix
  - Faction Coalition Breakdown
- **Benefit**: Streamlined UI with all bill-related analysis in single location
- **Files Modified**: `src/ui/plot_generators.py:240-253`

#### Bill Status Distribution Chart Transformation
- **Previous**: Single pie chart showing overall bill status distribution
- **New**: Stacked bar chart showing bill status breakdown per faction
- **Visualization**: Each faction gets a bar divided into three colored segments:
  - 🔴 **Red (bottom)**: Bills that stopped (הופסק/לא פעיל)
  - 🔵 **Blue (middle)**: Bills that passed first reading (קריאה ראשונה)
  - 🟢 **Green (top)**: Bills that passed third reading/became law (התקבלה בקריאה שלישית)
- **Features**:
  - Factions ordered by total bill count (most active first)
  - Count labels inside each segment
  - Responsive height based on number of factions
  - Angled faction names for readability
  - Interactive hover showing faction, stage, and count
- **Use Case**: Compare legislative success rates across factions at a glance
- **Files Modified**: `src/ui/charts/distribution.py:230-359`

### Technical Improvements

**SQL Query Optimization**:
- Eliminated nested COALESCE queries that caused performance issues
- Simplified faction resolution logic with single-query approach
- Added `RelevantKnessets` CTE for better query organization
- Improved GROUP BY clauses to handle COALESCE expressions properly

**Color Palette Enhancement**:
- Extended from single Set3 palette to Set3 + Plotly + Set1
- Added special gold color for Independent MKs
- Changed fallback from grey (#808080) to purple (#9467BD)
- Ensures sufficient unique colors for all factions in any Knesset

**Data Quality**:
- 100% faction coverage - no NULL or missing values
- Accurate Knesset-specific data - no cross-Knesset contamination
- Proper bill-to-faction attribution through KnessetNum matching
- Guaranteed 'Independent' assignment when no faction found

## Recent Updates (2025-08-19)

### UI Improvements and Bug Fixes

#### Header Update
- **Main Console Title**: Updated from "🇮🇱 Knesset Data Warehouse Console" to "🇮🇱 Knesset Data Console"
- **Location**: `src/ui/pages/data_refresh_page.py:40`
- **Tests Updated**: All E2E tests in `tests/test_e2e.py` updated to match new header

#### Interactive Chart Builder Removal
- **Removed Components**: Completely removed Interactive Chart Builder functionality
- **Files Deleted**: 
  - `src/ui/chart_builder.py`
  - `src/ui/chart_builder_ui.py`
  - `src/ui/chart_renderer.py`
  - `tests/test_chart_builder_ui.py`
- **Main UI Updated**: Removed chart builder import and section from `src/ui/data_refresh.py`
- **Documentation Cleaned**: Removed all references from README.md

#### Widget Key Conflict Fix
- **Issue Resolved**: Fixed Streamlit widget key conflict for `ms_knesset_filter` and `ms_faction_filter`
- **Root Cause**: Widgets had both `default` parameter and `key` parameter referencing same session state
- **Solution**: Removed `default` parameters from multiselect widgets in `src/ui/sidebar_components.py:522-531`
- **Result**: Eliminated "widget was created with default value but also had its value set via Session State API" warning

#### Code Quality Standards
- **Testing Protocol**: After every code change, run all tests to ensure stability
- **Documentation Policy**: Update project documentation after successful code changes and test passes
- **Syntax Validation**: All modified files pass Python syntax compilation checks
- **Import Verification**: Core application modules import successfully without errors

### Technical Implementation Notes

**Session State Management**: The fix leverages Streamlit's automatic session state management through widget `key` parameters, eliminating the need for manual `default` value setting when the SessionManager already initializes these values.

**E2E Test Maintenance**: All end-to-end tests maintain synchronization with UI changes to ensure continuous integration reliability.

**Clean Architecture Preservation**: Removal of chart builder maintains the clean architecture principles while reducing complexity and focusing on core functionality.
# Bill Initiators Feature

This document explains the enhanced bill initiator functionality in the Knesset Data system.

## Overview

The system now provides intelligent bill initiator analysis in the "Bills + Full Details" query, with proper distinction between main initiators and supporting members. This enhancement provides accurate transparency about the legislative process.

## Changes Made

### 1. Database Schema Updates

Added `KNS_BillInitiator` table to the system:
- Updated `src/config/database.py` to include the table
- Added table metadata to `src/backend/tables.py`

### 2. Smart Initiator Detection

Enhanced the "Bills + Full Details" query in `src/ui/queries/predefined_queries.py` to:
- Join with `KNS_BillInitiator` table using `Ordinal` field for proper distinction
- Join with `KNS_Person` table to get initiator names
- **Smart Classification**: Uses `Ordinal = 1` to identify main initiators vs supporting members
- **Coalition Status Integration**: Shows political affiliation of main bill initiators
- **Bill Merge Tracking**: Displays leading bill information for merged bills (Status ID 122)
- Include the following enhanced columns:
  - `BillMainInitiatorNames` - Names of actual bill initiators (Ordinal = 1)
  - `BillMainInitiatorCoalitionStatus` - Coalition/Opposition/Government status of main initiator
  - `MergedWithLeadingBill` - Leading bill information for merged bills
  - `BillSupportingMemberNames` - Names of supporting members (Ordinal > 1 or IsInitiator = NULL)
  - `BillMainInitiatorCount` - Number of main initiators
  - `BillSupportingMemberCount` - Number of supporting members
  - `BillTotalMemberCount` - Total members involved

### 3. Institutional Handling

Added proper handling for government bills:
- Shows "Government Initiative" for bills without MK initiators
- Distinguishes between private member bills and government bills

### 3. Data Fetching Scripts

Created two scripts to fetch the required data:
- `fetch_bill_initiators.py` - Fetches only KNS_BillInitiator data
- `fetch_bill_data.py` - Comprehensive script that fetches all bill-related tables

## Usage

### Fetching Data

To populate the database with bill and initiator data:

```bash
# Option 1: Fetch all bill-related data (recommended)
python3 fetch_bill_data.py

# Option 2: Fetch only bill initiator data
python3 fetch_bill_initiators.py
```

### Viewing Bills with Initiators

1. Start the Streamlit application:
   ```bash
   streamlit run src/cli.py
   ```

2. Navigate to the "Pre-defined Data Queries" section

3. Select "Bills + Full Details" from the dropdown

4. The results will include initiator information for each bill

## Data Structure

The `KNS_BillInitiator` table creates a many-to-many relationship between bills and people:
- `BillID` - References KNS_Bill
- `PersonID` - References KNS_Person
- Multiple people can initiate a single bill
- The same person can initiate multiple bills

## Example Output

When running the "Bills + Full Details" query, you'll see columns like:
```
BillID | BillName | BillMainInitiatorNames | BillMainInitiatorCoalitionStatus | MergedWithLeadingBill | BillSupportingMemberNames | BillMainInitiatorCount | BillSupportingMemberCount
123    | Tax Law  | John Doe               | Coalition                       | NULL                  | Jane Smith, Bob Jones     | 1                      | 2
456    | Budget   | Government Initiative  | Government                      | NULL                  | None                      | 0                      | 0
789    | Old Bill | Mary Johnson           | Opposition                      | Bill #123: Tax Law    | None                      | 1                      | 0
```

## Key Improvements

### Before Enhancement
- All members listed as "initiators" without distinction
- No way to identify who actually started the bill
- Government bills showed empty results

### After Enhancement  
- **Main Initiators**: Clear identification of bill originators (Ordinal = 1)
- **Coalition Status**: Political affiliation analysis for main initiators
- **Bill Merge Tracking**: Shows leading bill for merged legislation (Status ID 122)
- **Supporting Members**: Separate list of MKs who joined later
- **Government Bills**: Properly labeled as "Government Initiative"
- **Accurate Counts**: Meaningful statistics for legislative analysis
- **Legislative Continuity**: Track bill progression through merge relationships
- **Plenum Session Integration**: Bills now connected to plenum sessions where they were discussed (14,411 bills with session data 2011-2025)
- **Interactive Visualizations**: Three new charts for comprehensive bill initiator analysis

## Bill Initiator Visualizations (Added 2025-01-03)

The system now includes three complementary interactive charts that provide different perspectives on bill initiation patterns:

### 1. Top 10 Bill Initiators
**Location**: Bills Analytics → "Top 10 Bill Initiators"
- **Purpose**: Shows individual MKs with the highest number of initiated bills
- **Data Shown**: MK names, bill counts, faction affiliation
- **Example Results**: ואליד אלהואשלה (275 bills), אחמד טיבי (233 bills)
- **Use Case**: Identify the most legislatively active individual MKs

### 2. Bill Initiators by Faction  
**Location**: Bills Analytics → "Bill Initiators by Faction"
- **Purpose**: Shows count of MKs per faction who initiated at least one bill
- **Data Shown**: Faction names with count of unique MKs who are bill initiators
- **Example Results**: Likud (29 MKs), Yesh Atid (25 MKs)
- **Use Case**: Understand legislative participation breadth within each faction

### 3. Total Bills per Faction
**Location**: Bills Analytics → "Total Bills per Faction"
- **Purpose**: Shows total number of bills initiated by all MKs in each faction combined
- **Data Shown**: Faction names with cumulative bill count from all members
- **Example Results**: Likud (1,180 total bills), Yesh Atid (984 total bills)
- **Use Case**: Measure overall legislative output by political faction

### Understanding the Relationships

These three metrics provide complementary insights:
- **Individual Activity**: One MK might initiate 275 bills personally
- **Faction Participation**: That MK's faction might have 29 different members who initiated bills
- **Total Output**: All 29 MKs combined might have initiated 1,180 bills total

**Mathematical Relationship**: 
- Average Bills per Active MK = Total Bills per Faction ÷ MK Count per Faction
- Example: Likud average = 1,180 ÷ 29 = ~41 bills per participating MK

### Chart Features
- **Simplified Filtering**: All bill charts use only Knesset number filter (no additional bill type/status filters)
- **Enhanced Spacing**: 800px height with 180px top margin to prevent number cutoff
- **Interactive Elements**: Hover details, faction information, proper sorting
- **Responsive Design**: Optimized for various screen sizes

## Troubleshooting

If you encounter issues:
1. Ensure all required tables are fetched (KNS_Bill, KNS_BillInitiator, KNS_Person)
2. Check that the database connection is working
3. Verify that the Knesset OData API is accessible

## API Information

The data is fetched from the Knesset OData API:
- Bills: `https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_Bill`
- Bill Initiators: `https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_BillInitiator`
- People: `https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_Person`
- Plenum Sessions: `https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_PlenumSession`
- Plenum Session Items: `https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_PlmSessionItem`
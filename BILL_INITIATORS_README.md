# Bill Initiators Feature

This document explains the new bill initiator functionality added to the Knesset Data system.

## Overview

The system now supports displaying bill initiator information (first and last names) in the "Bills + Full Details" query. This enhancement provides transparency about who initiated each legislative bill.

## Changes Made

### 1. Database Schema Updates

Added `KNS_BillInitiator` table to the system:
- Updated `src/config/database.py` to include the table
- Added table metadata to `src/backend/tables.py`

### 2. Query Enhancement

Modified the "Bills + Full Details" query in `src/ui/queries/predefined_queries.py` to:
- Join with `KNS_BillInitiator` table
- Join with `KNS_Person` table to get initiator names
- Include the following new columns:
  - `BillInitiatorNames` - Full names of all initiators (comma-separated)
  - `BillInitiatorFirstNames` - First names only (comma-separated)
  - `BillInitiatorLastNames` - Last names only (comma-separated)
  - `BillInitiatorCount` - Number of initiators for the bill

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
BillID | BillName | ... | BillInitiatorNames | BillInitiatorCount
123    | Tax Law  | ... | John Doe, Jane Smith | 2
```

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
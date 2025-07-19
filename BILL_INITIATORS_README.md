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
- Include the following enhanced columns:
  - `BillMainInitiatorNames` - Names of actual bill initiators (Ordinal = 1)
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
BillID | BillName | BillMainInitiatorNames | BillSupportingMemberNames | BillMainInitiatorCount | BillSupportingMemberCount
123    | Tax Law  | John Doe               | Jane Smith, Bob Jones     | 1                      | 2
456    | Budget   | Government Initiative  | None                      | 0                      | 0
```

## Key Improvements

### Before Enhancement
- All members listed as "initiators" without distinction
- No way to identify who actually started the bill
- Government bills showed empty results

### After Enhancement  
- **Main Initiators**: Clear identification of bill originators (Ordinal = 1)
- **Supporting Members**: Separate list of MKs who joined later
- **Government Bills**: Properly labeled as "Government Initiative"
- **Accurate Counts**: Meaningful statistics for legislative analysis

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
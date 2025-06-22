#!/usr/bin/env python3
"""Comprehensive script to fetch all bill-related data from Knesset OData API"""

import requests
import pandas as pd
import duckdb
import sys
from pathlib import Path

def fetch_odata_table(table_name, primary_key=None, limit=None):
    """Generic function to fetch data from a Knesset OData table"""
    print(f"\n📊 Fetching {table_name} data...")
    
    base_url = f"https://knesset.gov.il/Odata/ParliamentInfo.svc/{table_name}"
    all_records = []
    skip = 0
    top = 100  # API seems to limit to 100 records per request
    
    while True:
        if limit and skip >= limit:
            break
            
        url = f"{base_url}?$skip={skip}&$top={top}"
        print(f"  Fetching records {skip} to {skip + top}...")
        
        try:
            headers = {'Accept': 'application/json'}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            records = data.get('value', [])
            
            if not records:
                print("  No more records to fetch.")
                break
                
            all_records.extend(records)
            print(f"  Fetched {len(records)} records. Total: {len(all_records)}")
            
            skip += len(records)  # Use actual number of records fetched
            
            # Continue fetching if we got the maximum number of records
            if len(records) < top:
                print("  Reached end of records.")
                break
                
        except Exception as e:
            print(f"  ❌ Error: {e}")
            break
    
    if all_records:
        print(f"  ✅ Total {table_name} records fetched: {len(all_records)}")
    else:
        print(f"  ❌ No {table_name} data fetched!")
        
    return all_records

def main():
    print("🏛️ Knesset Bill Data Fetcher")
    print("=" * 50)
    
    db_path = Path("data/warehouse.duckdb")
    db_path.parent.mkdir(exist_ok=True)
    
    # Tables to fetch
    tables = [
        ("KNS_Person", "PersonID"),
        ("KNS_Bill", "BillID"),
        ("KNS_BillInitiator", "BillInitiatorID"),
        ("KNS_Status", "StatusID"),
        ("KNS_Committee", "CommitteeID"),
    ]
    
    con = duckdb.connect(str(db_path))
    success_count = 0
    
    for table_name, primary_key in tables:
        records = fetch_odata_table(table_name, primary_key)
        
        if records:
            df = pd.DataFrame(records)
            print(f"  Storing {len(df)} records in database...")
            
            # Drop and recreate table
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            
            # Verify
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  ✅ Stored {count} records in {table_name}")
            success_count += 1
        else:
            print(f"  ⚠️  Skipping {table_name} - no data")
    
    # Show sample bill with initiator
    if success_count > 0:
        print("\n📝 Sample Bills with Initiators:")
        try:
            sample = con.execute("""
                SELECT 
                    b.BillID, 
                    b.Name AS BillName,
                    p.FirstName || ' ' || p.LastName AS InitiatorName
                FROM KNS_Bill b
                LEFT JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
                LEFT JOIN KNS_Person p ON bi.PersonID = p.PersonID
                WHERE p.FirstName IS NOT NULL
                LIMIT 5
            """).fetchall()
            
            for bill in sample:
                print(f"  - Bill {bill[0]}: {bill[1][:50]}... - Initiated by: {bill[2]}")
        except Exception as e:
            print(f"  Could not show sample: {e}")
    
    con.close()
    
    print(f"\n✨ Summary: Successfully fetched {success_count}/{len(tables)} tables")
    
    if success_count == len(tables):
        print("🎉 All bill data fetched successfully!")
        print("You can now run the 'Bills + Full Details' query to see bills with initiator names.")
        return 0
    else:
        print("⚠️  Some tables failed to fetch. Check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
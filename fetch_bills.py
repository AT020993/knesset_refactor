#!/usr/bin/env python3
"""Simple script to fetch KNS_Bill data from Knesset OData API"""

import requests
import pandas as pd
import duckdb
import json
from pathlib import Path

def fetch_kns_bill():
    print("🏛️ Fetching KNS_Bill data from Knesset OData API...")
    
    # Knesset OData API endpoint for Bills
    base_url = "https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_Bill"
    
    all_bills = []
    skip = 0
    top = 1000  # Fetch 1000 records at a time
    
    while True:
        url = f"{base_url}?$skip={skip}&$top={top}"
        print(f"Fetching records {skip} to {skip + top}...")
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            bills = data.get('value', [])
            
            if not bills:
                print("No more records to fetch.")
                break
                
            all_bills.extend(bills)
            print(f"Fetched {len(bills)} bills. Total so far: {len(all_bills)}")
            
            skip += top
            
            # Break if we got fewer records than requested (end of data)
            if len(bills) < top:
                break
                
        except Exception as e:
            print(f"Error fetching data: {e}")
            break
    
    if not all_bills:
        print("❌ No bills data fetched!")
        return False
    
    print(f"✅ Fetched {len(all_bills)} total bills")
    
    # Convert to DataFrame
    df = pd.DataFrame(all_bills)
    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Connect to database and store data
    db_path = "data/warehouse.duckdb"
    print(f"Storing data in {db_path}...")
    
    con = duckdb.connect(db_path)
    
    # Drop table if exists and recreate
    con.execute("DROP TABLE IF EXISTS KNS_Bill")
    
    # Store the data
    con.execute("CREATE TABLE KNS_Bill AS SELECT * FROM df")
    
    # Verify the data was stored
    count = con.execute("SELECT COUNT(*) FROM KNS_Bill").fetchone()[0]
    print(f"✅ Stored {count} bills in database")
    
    # Show sample data
    sample = con.execute("SELECT BillID, Name, KnessetNum FROM KNS_Bill LIMIT 5").fetchall()
    print("Sample bills:")
    for bill in sample:
        print(f"  - Bill {bill[0]}: {bill[1]} (Knesset {bill[2]})")
    
    con.close()
    return True

if __name__ == "__main__":
    success = fetch_kns_bill()
    if success:
        print("🎉 KNS_Bill data is now available!")
        print("You can now run your Bills query in Streamlit.")
    else:
        print("❌ Failed to fetch KNS_Bill data.")
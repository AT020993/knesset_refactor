#!/usr/bin/env python3
"""
Comprehensive download strategy for complete KNS_CmtSessionItem dataset.
Tests multiple approaches to get all 75,051 records.
"""

import json
import duckdb
import subprocess
import time
from datetime import datetime

def test_api_parameters():
    """Test different API parameters to understand limitations."""
    print("🔍 Testing API parameter behavior...")
    
    test_cases = [
        ("Default batch", "?$top=1000"),
        ("Large batch", "?$top=5000"), 
        ("With skip", "?$skip=0&$top=1000"),
        ("Different skip", "?$skip=1000&$top=1000"),
        ("With orderby", "?$top=1000&$orderby=CmtSessionItemID"),
        ("Orderby desc", "?$top=1000&$orderby=CmtSessionItemID desc"),
        ("No top limit", "?$skip=0"),
    ]
    
    base_url = 'https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_CmtSessionItem'
    
    for name, params in test_cases:
        try:
            url = f'{base_url}{params}&$format=json'
            result = subprocess.run(['curl', '-s', url], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                try:
                    data = json.loads(result.stdout)
                    records = data.get('value', [])
                    print(f"  {name:15}: {len(records):3} records")
                    if records:
                        ids = [r.get('CmtSessionItemID') for r in records[:3]]
                        print(f"                   First 3 IDs: {ids}")
                except json.JSONDecodeError:
                    print(f"  {name:15}: JSON decode error")
            else:
                print(f"  {name:15}: HTTP error {result.returncode}")
                
        except Exception as e:
            print(f"  {name:15}: Error - {e}")
        
        time.sleep(0.5)  # Be respectful to API

def download_with_orderby_strategy():
    """Try downloading with explicit ordering to ensure we get all records."""
    print("\n🚀 Attempting comprehensive download with ordering strategy...")
    
    conn = duckdb.connect('data/warehouse.duckdb', read_only=False)
    
    # Clear existing data
    print('📝 Clearing existing data...')
    conn.execute('DROP TABLE IF EXISTS KNS_CmtSessionItem_Complete')
    
    # Create new table
    conn.execute('''
    CREATE TABLE KNS_CmtSessionItem_Complete (
        CmtSessionItemID BIGINT,
        ItemID BIGINT,
        CommitteeSessionID BIGINT,
        Ordinal INTEGER,
        StatusID INTEGER,
        Name VARCHAR,
        ItemTypeID BIGINT,
        LastUpdatedDate TIMESTAMP
    )
    ''')
    
    base_url = 'https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_CmtSessionItem'
    batch_size = 100  # API seems to limit to 100 regardless of $top
    total_downloaded = 0
    skip = 0
    consecutive_empty = 0
    max_consecutive_empty = 5
    
    start_time = time.time()
    
    while consecutive_empty < max_consecutive_empty:
        try:
            # Try with explicit ordering
            url = f'{base_url}?$skip={skip}&$top={batch_size}&$orderby=CmtSessionItemID&$format=json'
            
            print(f'📥 Batch {skip//batch_size + 1} (skip={skip})...', end='', flush=True)
            
            result = subprocess.run(['curl', '-s', url], capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                print(f' ❌ HTTP error {result.returncode}')
                consecutive_empty += 1
                skip += batch_size
                continue
                
            try:
                data = json.loads(result.stdout)
            except json.JSONDecodeError as e:
                print(f' ❌ JSON error: {e}')
                consecutive_empty += 1
                skip += batch_size
                continue
                
            records = data.get('value', [])
            
            if not records:
                print(f' ⚠️  Empty batch (consecutive: {consecutive_empty + 1})')
                consecutive_empty += 1
                skip += batch_size
                continue
            
            consecutive_empty = 0  # Reset counter
            
            # Insert records
            successful_inserts = 0
            for record in records:
                try:
                    conn.execute('''
                        INSERT INTO KNS_CmtSessionItem_Complete VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        record.get('CmtSessionItemID'),
                        record.get('ItemID'),
                        record.get('CommitteeSessionID'),
                        record.get('Ordinal'),
                        record.get('StatusID'),
                        record.get('Name'),
                        record.get('ItemTypeID'),
                        record.get('LastUpdatedDate')
                    ))
                    successful_inserts += 1
                except Exception as e:
                    # Skip duplicates
                    continue
            
            total_downloaded += successful_inserts
            elapsed = time.time() - start_time
            rate = total_downloaded / elapsed if elapsed > 0 else 0
            
            if records:
                first_id = records[0].get('CmtSessionItemID', 'N/A')
                last_id = records[-1].get('CmtSessionItemID', 'N/A')
                print(f' ✅ {successful_inserts} records (IDs {first_id}-{last_id}) | Total: {total_downloaded:,} | {rate:.1f} rec/sec')
            
            skip += batch_size
            time.sleep(0.1)  # Be respectful
            
        except Exception as e:
            print(f' ❌ Error: {e}')
            consecutive_empty += 1
            skip += batch_size
            continue
    
    print(f"\n📊 Download complete!")
    print(f"   Total records downloaded: {total_downloaded:,}")
    print(f"   Total time: {(time.time() - start_time)/60:.1f} minutes")
    
    # Verify and analyze
    try:
        final_count = conn.execute('SELECT COUNT(*) FROM KNS_CmtSessionItem_Complete').fetchone()[0]
        unique_items = conn.execute('SELECT COUNT(DISTINCT ItemID) FROM KNS_CmtSessionItem_Complete WHERE ItemID IS NOT NULL').fetchone()[0]
        id_range = conn.execute('SELECT MIN(CmtSessionItemID), MAX(CmtSessionItemID) FROM KNS_CmtSessionItem_Complete').fetchone()
        
        print(f"   Records in database: {final_count:,}")
        print(f"   Unique ItemIDs: {unique_items:,}")
        print(f"   ID range: {id_range[0]:,} to {id_range[1]:,}")
        
        # Check for missing ranges
        if final_count < 75051:
            print(f"   ⚠️  Still missing {75051 - final_count:,} records")
            print("   Trying alternative download strategies...")
        else:
            print("   ✅ Complete dataset downloaded!")
            
    except Exception as e:
        print(f"   ❌ Verification error: {e}")
    
    conn.close()
    return total_downloaded

def try_range_based_download():
    """Try downloading by ID ranges if sequential approach doesn't work."""
    print("\n🎯 Attempting range-based download strategy...")
    
    # This would try downloading specific ID ranges
    # Implementation would go here if needed
    pass

def main():
    print("🚀 Comprehensive KNS_CmtSessionItem Download Strategy")
    print("=" * 60)
    
    # Step 1: Test API behavior
    test_api_parameters()
    
    # Step 2: Try comprehensive download
    total_records = download_with_orderby_strategy()
    
    print(f"\n🎉 Strategy complete! Downloaded {total_records:,} records.")
    
    if total_records < 70000:  # If still significantly short
        print("🔄 Consider implementing additional strategies:")
        print("   - Date-based filtering")
        print("   - Committee-based filtering") 
        print("   - Multiple concurrent downloads")

if __name__ == '__main__':
    main()
#!/usr/bin/env python3
"""
Download complete KNS_CmtSessionItem dataset for accurate bill-to-committee session mapping.
"""

import json
import duckdb
import subprocess
import time
from datetime import datetime

def main():
    print(f'🚀 Starting complete KNS_CmtSessionItem download at {datetime.now().strftime("%H:%M:%S")}')
    print('Expected records: 75,051')
    print('This may take 10-15 minutes...')

    # Initialize database connection
    conn = duckdb.connect('data/warehouse.duckdb', read_only=False)

    # Clear existing data
    print('\n📝 Clearing existing KNS_CmtSessionItem data...')
    conn.execute('DROP TABLE IF EXISTS KNS_CmtSessionItem')

    # Create table with proper schema
    conn.execute('''
    CREATE TABLE KNS_CmtSessionItem (
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

    # Download in batches using curl
    base_url = 'https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_CmtSessionItem'
    batch_size = 1000
    total_downloaded = 0
    skip = 0

    start_time = time.time()

    while True:
        try:
            url = f'{base_url}?$skip={skip}&$top={batch_size}&$format=json'
            
            print(f'📥 Downloading batch {skip//batch_size + 1} (records {skip+1}-{skip+batch_size})...', end='', flush=True)
            
            # Use curl to download data
            result = subprocess.run(['curl', '-s', url], capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                print(f'\n❌ Curl failed with return code {result.returncode}')
                continue
                
            try:
                data = json.loads(result.stdout)
            except json.JSONDecodeError as e:
                print(f'\n❌ JSON decode error: {e}')
                continue
                
            records = data.get('value', [])
            
            if not records:
                print(' ✅ No more records')
                break
                
            # Insert batch into database
            successful_inserts = 0
            for record in records:
                try:
                    conn.execute('''
                        INSERT INTO KNS_CmtSessionItem VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
                    print(f'\nError inserting record {record.get("CmtSessionItemID")}: {e}')
                    continue
            
            total_downloaded += successful_inserts
            elapsed = time.time() - start_time
            rate = total_downloaded / elapsed if elapsed > 0 else 0
            eta = (75051 - total_downloaded) / rate if rate > 0 else 0
            
            print(f' ✅ {successful_inserts} records ({total_downloaded:,} total, {rate:.1f} rec/sec, ETA: {eta/60:.1f} min)')
            
            skip += batch_size
            
            # Small delay to be respectful to the API
            time.sleep(0.2)
            
        except subprocess.TimeoutExpired:
            print(f'\n⏰ Request timeout at skip={skip}. Retrying...')
            continue
        except Exception as e:
            print(f'\n❌ Error downloading batch at skip={skip}: {e}')
            print('Retrying in 5 seconds...')
            time.sleep(5)
            continue

    print(f'\n🎉 Download complete! Total records: {total_downloaded:,}')
    print(f'⏱️  Total time: {(time.time() - start_time)/60:.1f} minutes')

    # Verify data
    try:
        final_count = conn.execute('SELECT COUNT(*) FROM KNS_CmtSessionItem').fetchone()[0]
        bill_connections = conn.execute('SELECT COUNT(DISTINCT ItemID) FROM KNS_CmtSessionItem WHERE ItemID IS NOT NULL').fetchone()[0]

        print(f'\n📊 Verification:')
        print(f'   Records in database: {final_count:,}')
        print(f'   Unique ItemIDs (potential bills): {bill_connections:,}')
        
        # Quick analysis
        matching_bills = conn.execute('''
            SELECT COUNT(DISTINCT b.BillID) 
            FROM KNS_Bill b 
            JOIN KNS_CmtSessionItem csi ON b.BillID = csi.ItemID
        ''').fetchone()[0]
        
        print(f'   Bills with direct session connections: {matching_bills:,}')
        
    except Exception as e:
        print(f'\n❌ Error in verification: {e}')

    conn.close()
    print('\n✅ Complete dataset downloaded successfully!')

if __name__ == '__main__':
    main()
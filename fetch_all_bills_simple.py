#!/usr/bin/env python3
"""
Simple script to fetch ALL bills data from Knesset OData API using only built-in libraries.
This script will fetch all available bills, including recent ones.
"""

import urllib.request
import urllib.parse
import json
import sqlite3
import csv
from pathlib import Path
import sys
from datetime import datetime

def fetch_with_urllib(url, timeout=60):
    """Fetch JSON data using urllib (built-in library)."""
    try:
        # Create request with proper headers to mimic browser behavior
        req = urllib.request.Request(url)
        req.add_header('Accept', 'application/json, text/plain, */*')
        req.add_header('Accept-Language', 'he,en-US;q=0.9,en;q=0.8')
        req.add_header('Accept-Encoding', 'gzip, deflate')
        req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        req.add_header('Referer', 'http://knesset.gov.il/')
        req.add_header('Connection', 'keep-alive')
        
        with urllib.request.urlopen(req, timeout=timeout) as response:
            content = response.read().decode('utf-8')
            if not content.strip():
                print(f"Empty response from {url}")
                return None
            return json.loads(content)
    except json.JSONDecodeError as e:
        print(f"JSON decode error for {url}: {e}")
        return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def fetch_all_bills():
    """Fetch all bills from the Knesset OData API using cursor-based paging."""
    print("🏛️ Fetching ALL Bills from Knesset OData API...")
    print("=" * 60)
    
    base_url = "http://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_Bill()"
    all_bills = []
    last_bill_id = -1  # Start from -1 to get all records
    top = 100  # API limit
    
    while True:
        # Use cursor-based paging with BillID filter and ordering
        url = f"{base_url}?$format=json&$top={top}&$filter=BillID%20gt%20{last_bill_id}&$orderby=BillID%20asc"
        print(f"📥 Fetching records with BillID > {last_bill_id}...")
        
        data = fetch_with_urllib(url)
        if not data:
            print("❌ Failed to fetch data")
            break
            
        bills = data.get('value', [])
        if not bills:
            print("✅ No more records - reached end of data")
            break
            
        all_bills.extend(bills)
        print(f"   Fetched {len(bills)} bills. Total so far: {len(all_bills)}")
        
        # Update last_bill_id to the highest BillID from this batch
        last_bill_id = max(bill.get('BillID', last_bill_id) for bill in bills)
        
        # If we got fewer records than requested, we've reached the end
        if len(bills) < top:
            print("✅ Reached end of available data")
            break
    
    print(f"\n🎉 TOTAL BILLS FETCHED: {len(all_bills)}")
    return all_bills

def analyze_bills(bills):
    """Analyze the fetched bills data."""
    if not bills:
        return
        
    print("\n📊 BILL DATA ANALYSIS")
    print("=" * 30)
    
    # Count by Knesset
    knesset_counts = {}
    years_with_data = set()
    recent_bills = []
    
    for bill in bills:
        knesset_num = bill.get('KnessetNum')
        if knesset_num:
            knesset_counts[knesset_num] = knesset_counts.get(knesset_num, 0) + 1
        
        # Check publication dates
        pub_date = bill.get('PublicationDate')
        if pub_date:
            try:
                year = datetime.fromisoformat(pub_date.replace('Z', '+00:00')).year
                years_with_data.add(year)
                if year >= 2020:  # Recent bills
                    recent_bills.append((bill.get('BillID'), bill.get('Name', 'Unknown'), pub_date))
            except:
                pass
    
    print(f"📈 Bills by Knesset:")
    for knesset in sorted(knesset_counts.keys()):
        print(f"   Knesset {knesset}: {knesset_counts[knesset]} bills")
    
    if years_with_data:
        print(f"\n📅 Years with data: {min(years_with_data)} - {max(years_with_data)}")
    
    if recent_bills:
        print(f"\n🆕 Recent bills (2020+): {len(recent_bills)}")
        for bill_id, name, date in recent_bills[:5]:
            print(f"   - Bill {bill_id}: {name[:50]}... ({date[:10]})")
    else:
        print("\n⚠️  No recent bills found (2020+)")

def save_to_csv(bills, filename):
    """Save bills to CSV file."""
    if not bills:
        return False
        
    # Get all possible field names
    all_fields = set()
    for bill in bills:
        all_fields.update(bill.keys())
    
    all_fields = sorted(list(all_fields))
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=all_fields)
        writer.writeheader()
        for bill in bills:
            writer.writerow(bill)
    
    print(f"💾 Saved {len(bills)} bills to {filename}")
    return True

def save_to_duckdb(bills, db_path):
    """Save bills to DuckDB database."""
    try:
        import duckdb
        
        # Create data directory if needed
        Path(db_path).parent.mkdir(exist_ok=True)
        
        con = duckdb.connect(str(db_path))
        
        # Create table from data
        if bills:
            # Convert to a format DuckDB can handle
            import pandas as pd
            df = pd.DataFrame(bills)
            
            # Drop and recreate table
            con.execute("DROP TABLE IF EXISTS KNS_Bill")
            con.execute("CREATE TABLE KNS_Bill AS SELECT * FROM df")
            
            # Verify
            count = con.execute("SELECT COUNT(*) FROM KNS_Bill").fetchone()[0]
            print(f"💾 Saved {count} bills to DuckDB: {db_path}")
            
            # Show latest bills
            try:
                latest = con.execute("""
                    SELECT BillID, Name, KnessetNum, PublicationDate 
                    FROM KNS_Bill 
                    WHERE PublicationDate IS NOT NULL 
                    ORDER BY PublicationDate DESC 
                    LIMIT 5
                """).fetchall()
                
                print("🔍 Latest bills by publication date:")
                for bill in latest:
                    print(f"   - Bill {bill[0]} (K{bill[2]}): {bill[1][:40]}... ({bill[3][:10]})")
                    
            except Exception as e:
                print(f"Could not show latest bills: {e}")
        
        con.close()
        return True
        
    except ImportError:
        print("⚠️  DuckDB not available - skipping database save")
        return False
    except Exception as e:
        print(f"❌ Error saving to DuckDB: {e}")
        return False

def main():
    """Main function."""
    print("🇮🇱 KNESSET BILLS COMPREHENSIVE FETCHER")
    print("=" * 50)
    print("This will fetch ALL available bills from the Knesset API")
    print()
    
    # Fetch all bills
    bills = fetch_all_bills()
    
    if not bills:
        print("❌ No bills data fetched!")
        return 1
    
    # Analyze the data
    analyze_bills(bills)
    
    # Save to CSV
    csv_file = "data/all_knesset_bills.csv"
    Path("data").mkdir(exist_ok=True)
    save_to_csv(bills, csv_file)
    
    # Try to save to DuckDB
    db_file = "data/warehouse.duckdb"
    save_to_duckdb(bills, db_file)
    
    print(f"\n🎉 SUCCESS! Fetched {len(bills)} bills")
    print(f"📁 Data saved to:")
    print(f"   - CSV: {csv_file}")
    print(f"   - Database: {db_file}")
    print("\nYou can now check if you have more recent bills data!")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
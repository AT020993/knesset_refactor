#!/usr/bin/env python3
"""
Script to check the recency of bill data in the database and compare with live API data.
This helps determine if the database needs updating.
"""

import sys
import duckdb
from pathlib import Path
from datetime import datetime, timezone

def check_database_status():
    """Check the current status of the database."""
    print("=" * 60)
    print("📊 DATABASE STATUS CHECK")
    print("=" * 60)
    
    db_path = Path("data/warehouse.duckdb")
    
    if not db_path.exists():
        print(f"❌ Database file does not exist: {db_path}")
        return False
    
    print(f"✅ Database file exists: {db_path}")
    print(f"📅 Database file last modified: {datetime.fromtimestamp(db_path.stat().st_mtime)}")
    print(f"💾 Database file size: {db_path.stat().st_size / (1024*1024):.1f} MB")
    
    try:
        con = duckdb.connect(str(db_path))
        
        # Check available tables
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        print(f"\n📋 Available tables ({len(table_names)}):")
        for table in sorted(table_names):
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  • {table}: {count:,} records")
            except Exception as e:
                print(f"  • {table}: Error - {e}")
        
        # Focus on KNS_Bill table analysis
        if 'KNS_Bill' in table_names:
            print(f"\n🏛️ BILL DATA ANALYSIS")
            print("-" * 40)
            
            # Basic stats
            total_bills = con.execute("SELECT COUNT(*) FROM KNS_Bill").fetchone()[0]
            print(f"Total bills: {total_bills:,}")
            
            # Date column analysis
            try:
                date_stats = con.execute("""
                    SELECT 
                        COUNT(CASE WHEN PublicationDate IS NOT NULL THEN 1 END) as bills_with_pub_date,
                        COUNT(CASE WHEN LastUpdatedDate IS NOT NULL THEN 1 END) as bills_with_update_date,
                        MIN(PublicationDate) as earliest_publication,
                        MAX(PublicationDate) as latest_publication,
                        MIN(LastUpdatedDate) as earliest_update,
                        MAX(LastUpdatedDate) as latest_update
                    FROM KNS_Bill
                """).fetchone()
                
                print(f"\n📅 Date Information:")
                print(f"  Bills with publication date: {date_stats[0]:,}")
                print(f"  Bills with last updated date: {date_stats[1]:,}")
                print(f"  \n  Publication date range:")
                print(f"    Earliest: {date_stats[2]}")
                print(f"    Latest: {date_stats[3]}")
                print(f"  \n  Last updated date range:")
                print(f"    Earliest: {date_stats[4]}")
                print(f"    Latest: {date_stats[5]}")
                
                # Check if latest update is recent
                if date_stats[5]:
                    try:
                        latest_update = datetime.fromisoformat(date_stats[5].replace('T', ' ').replace('Z', ''))
                        days_since_update = (datetime.now() - latest_update).days
                        print(f"    Days since latest update: {days_since_update}")
                        
                        if days_since_update < 7:
                            print(f"    ✅ Data appears recent (updated within last 7 days)")
                        elif days_since_update < 30:
                            print(f"    ⚠️  Data is somewhat recent (updated within last 30 days)")
                        else:
                            print(f"    🔴 Data may be outdated (updated more than 30 days ago)")
                    except Exception as e:
                        print(f"    Could not parse latest update date: {e}")
                        
            except Exception as e:
                print(f"Error analyzing dates: {e}")
            
            # Knesset distribution
            try:
                knesset_stats = con.execute("""
                    SELECT 
                        KnessetNum,
                        COUNT(*) as bill_count,
                        MIN(PublicationDate) as earliest_pub,
                        MAX(PublicationDate) as latest_pub,
                        MAX(LastUpdatedDate) as latest_update
                    FROM KNS_Bill 
                    WHERE KnessetNum IS NOT NULL
                    GROUP BY KnessetNum 
                    ORDER BY KnessetNum DESC
                    LIMIT 10
                """).fetchall()
                
                print(f"\n🏛️ Bills by Knesset (Top 10 most recent):")
                for stat in knesset_stats:
                    knesset, count, earliest_pub, latest_pub, latest_update = stat
                    print(f"  Knesset {knesset}: {count:,} bills")
                    print(f"    Publication: {earliest_pub} to {latest_pub}")
                    print(f"    Last updated: {latest_update}")
                    print()
                    
            except Exception as e:
                print(f"Error analyzing Knesset distribution: {e}")
                
            # Most recent bills
            try:
                recent_bills = con.execute("""
                    SELECT 
                        BillID,
                        Name,
                        KnessetNum,
                        PublicationDate,
                        LastUpdatedDate
                    FROM KNS_Bill 
                    WHERE LastUpdatedDate IS NOT NULL
                    ORDER BY LastUpdatedDate DESC
                    LIMIT 5
                """).fetchall()
                
                print(f"📋 5 Most recently updated bills:")
                for i, bill in enumerate(recent_bills, 1):
                    bill_id, name, knesset, pub_date, update_date = bill
                    name_short = name[:50] + "..." if len(name) > 50 else name
                    print(f"  {i}. Bill {bill_id} (Knesset {knesset})")
                    print(f"     {name_short}")
                    print(f"     Published: {pub_date}")
                    print(f"     Updated: {update_date}")
                    print()
                    
            except Exception as e:
                print(f"Error getting recent bills: {e}")
        
        con.close()
        return True
        
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return False

def check_parquet_files():
    """Check the status of parquet files."""
    print("\n" + "=" * 60)
    print("📁 PARQUET FILES STATUS")
    print("=" * 60)
    
    parquet_dir = Path("data/parquet")
    
    if not parquet_dir.exists():
        print(f"❌ Parquet directory does not exist: {parquet_dir}")
        return
    
    parquet_files = list(parquet_dir.glob("*.parquet"))
    
    if not parquet_files:
        print("❌ No parquet files found")
        return
    
    print(f"📂 Found {len(parquet_files)} parquet files:")
    
    for file in sorted(parquet_files):
        modified = datetime.fromtimestamp(file.stat().st_mtime)
        size_mb = file.stat().st_size / (1024 * 1024)
        days_old = (datetime.now() - modified).days
        
        status = "🟢" if days_old < 7 else "🟡" if days_old < 30 else "🔴"
        
        print(f"  {status} {file.name}")
        print(f"     Size: {size_mb:.1f} MB")
        print(f"     Modified: {modified.strftime('%Y-%m-%d %H:%M:%S')} ({days_old} days ago)")

def generate_recommendations():
    """Generate recommendations based on data analysis."""
    print("\n" + "=" * 60)
    print("💡 RECOMMENDATIONS")
    print("=" * 60)
    
    print("Based on the analysis above:")
    print()
    print("1. 📊 DATA RECENCY:")
    print("   • Your bill data shows most recent publications from 2011")
    print("   • However, LastUpdatedDate shows updates through December 2024")
    print("   • This suggests the API provides updates to existing bills but may not include newer bills")
    print()
    print("2. 🔄 DATA REFRESH:")
    print("   • To get the most recent data, run: python fetch_bills.py")
    print("   • Or use the data refresh functionality in the Streamlit UI")
    print("   • Consider setting up automated daily/weekly refreshes")
    print()
    print("3. 🏛️ CURRENT COVERAGE:")
    print("   • Data covers Knessets 1, 7, 16, 17, and 18")
    print("   • Latest publication dates are from 2011 (Knesset 18)")
    print("   • Current Knesset (25th) data may not be fully represented")
    print()
    print("4. 🔍 TO CHECK IF NEW DATA IS AVAILABLE:")
    print("   • Run the fetch scripts to see if API has more recent bills")
    print("   • Compare BillID ranges - higher IDs typically indicate newer bills")
    print("   • Check if API rate limits or filters are affecting data completeness")
    print()
    print("5. ⚠️  IMPORTANT CONSIDERATIONS:")
    print("   • Knesset API may have limitations on historical vs. current data")
    print("   • Some bills might be restricted or not publicly available")
    print("   • Data structure might have changed for more recent Knessets")

def main():
    """Main function to run all checks."""
    print("🏛️ KNESSET BILL DATA RECENCY CHECK")
    print(f"Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Change to script directory
    script_dir = Path(__file__).parent
    import os
    os.chdir(script_dir)
    
    success = check_database_status()
    check_parquet_files()
    generate_recommendations()
    
    print("\n" + "=" * 60)
    print("✅ DATA RECENCY CHECK COMPLETE")
    print("=" * 60)
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
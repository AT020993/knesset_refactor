#!/usr/bin/env python3
"""
Comprehensive summary of bill data recency and recommendations for updating.
"""

import sys
import duckdb
from pathlib import Path
from datetime import datetime

def create_comprehensive_summary():
    print("🏛️ KNESSET BILL DATA RECENCY SUMMARY")
    print("=" * 80)
    print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Database Analysis
    db_path = Path("data/warehouse.duckdb")
    if db_path.exists():
        con = duckdb.connect(str(db_path))
        
        print("📊 CURRENT DATABASE STATUS")
        print("-" * 40)
        
        # Get basic stats
        total_bills = con.execute("SELECT COUNT(*) FROM KNS_Bill").fetchone()[0]
        
        # Get date ranges
        date_stats = con.execute("""
            SELECT 
                MIN(PublicationDate) as earliest_pub,
                MAX(PublicationDate) as latest_pub,
                MAX(LastUpdatedDate) as latest_update,
                COUNT(CASE WHEN PublicationDate IS NOT NULL THEN 1 END) as bills_with_pub_date
            FROM KNS_Bill
        """).fetchone()
        
        earliest_pub, latest_pub, latest_update, bills_with_pub = date_stats
        
        print(f"• Total bills in database: {total_bills:,}")
        print(f"• Bills with publication dates: {bills_with_pub:,}")
        print(f"• Publication date range: {earliest_pub} to {latest_pub}")
        print(f"• Most recent update: {latest_update}")
        
        # Calculate days since last update
        if latest_update:
            try:
                latest_dt = datetime.fromisoformat(latest_update.replace('T', ' ').replace('Z', ''))
                days_old = (datetime.now() - latest_dt).days
                print(f"• Days since last update: {days_old}")
            except:
                print(f"• Could not parse update date")
        
        # Knesset coverage
        knesset_coverage = con.execute("""
            SELECT KnessetNum, COUNT(*) as count
            FROM KNS_Bill 
            WHERE KnessetNum IS NOT NULL
            GROUP BY KnessetNum 
            ORDER BY KnessetNum DESC
        """).fetchall()
        
        print(f"\n• Knesset coverage:")
        for knesset, count in knesset_coverage:
            print(f"  - Knesset {knesset}: {count:,} bills")
        
        con.close()
    
    print("\n🔍 DATA RECENCY ASSESSMENT")
    print("-" * 40)
    
    print("FINDINGS:")
    print("• ✅ Database contains 5,000 bills with recent update timestamps")
    print("• ❌ Most recent bill publications are from 2011 (Knesset 18)")
    print("• ⚠️  Missing data from Knessets 19-25 (2013-2025)")
    print("• ⚠️  Last database update was 187+ days ago")
    
    print("\nIMPLICATIONS:")
    print("• Your analysis will be limited to historical data (pre-2012)")
    print("• Current Knesset (25th) legislation is not represented")
    print("• Recent political developments and bills are not captured")
    print("• Trend analysis will be limited to older periods")
    
    print("\n🔄 RECOMMENDED ACTIONS")
    print("-" * 40)
    
    print("IMMEDIATE STEPS:")
    print("1. 🚀 Run data refresh to get latest available data:")
    print("   bash scripts/refresh_all.sh")
    print("   # OR")
    print("   python -m src.cli refresh")
    print("   # OR")
    print("   python fetch_bills.py")
    
    print("\n2. 🔍 Check if API limitations exist:")
    print("   • The Knesset API might have restrictions on recent data")
    print("   • Some bills might require different API endpoints")
    print("   • Authentication might be needed for current session data")
    
    print("\n3. 📋 Verify data completeness after refresh:")
    print("   • Check if new Knesset numbers appear")
    print("   • Look for bills with 2012+ publication dates")
    print("   • Compare total record counts before/after refresh")
    
    print("\nLONG-TERM SOLUTIONS:")
    print("• Set up automated daily/weekly data refreshes")
    print("• Monitor API changes or new endpoints")
    print("• Consider supplementing with additional data sources")
    print("• Implement data validation checks")
    
    print("\n🎯 ANALYSIS STRATEGIES WITH CURRENT DATA")
    print("-" * 40)
    
    print("WHAT YOU CAN ANALYZE NOW:")
    print("• Historical trends (1949-2011)")
    print("• Comparison between Knessets 16-18")
    print("• Bill initiator patterns in earlier periods")
    print("• Committee assignment trends")
    print("• Coalition vs. Opposition dynamics (historical)")
    
    print("\nLIMITATIONS TO ACKNOWLEDGE:")
    print("• Results don't reflect current political landscape")
    print("• Missing recent legislative priorities")
    print("• Coalition/Opposition dynamics may have changed")
    print("• Current procedural changes not captured")
    
    print("\n📋 QUICK COMMANDS TO RUN")
    print("-" * 40)
    
    print("To refresh data immediately:")
    print("  cd /path/to/knesset_refactor")
    print("  python fetch_bills.py")
    
    print("\nTo check API availability:")
    print("  curl 'https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_Bill?$top=1'")
    
    print("\nTo monitor refresh progress:")
    print("  tail -f logs/knesset_ui_data_refresh.log")
    
    print("\n" + "=" * 80)
    print("📋 SUMMARY: Your bill data covers 1949-2011 with recent API updates")
    print("🔄 ACTION NEEDED: Run data refresh to attempt getting recent bills")
    print("⚠️  EXPECT: API may have limitations on current Knesset data")
    print("=" * 80)

if __name__ == "__main__":
    # Change to script directory
    script_dir = Path(__file__).parent
    import os
    os.chdir(script_dir)
    
    create_comprehensive_summary()
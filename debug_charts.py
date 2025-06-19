#!/usr/bin/env python3
"""Debug script to test chart functionality."""

import sys
from pathlib import Path

# Add src directory to path
current_dir = Path(__file__).parent
src_dir = current_dir / "src"
sys.path.insert(0, str(src_dir))

import logging
from utils.logger_setup import setup_logging
from ui.charts.factory import ChartFactory

# Setup logging
logger = setup_logging("debug_charts", console_output=True)

# Test chart factory
db_path = Path("data/warehouse.duckdb")
print(f"Database path: {db_path}")
print(f"Database exists: {db_path.exists()}")

if db_path.exists():
    try:
        factory = ChartFactory(db_path, logger)
        print("✅ ChartFactory created successfully")
        
        # Test available charts
        available_charts = factory.get_available_charts()
        print(f"Available charts: {available_charts}")
        
        # Test a simple chart
        print("\n🧪 Testing queries by time chart...")
        fig = factory.create_chart('time_series', 'queries_by_time', knesset_filter=[25])
        if fig:
            print("✅ Queries time series chart created successfully")
        else:
            print("❌ Queries time series chart failed")
        
        print("\n🧪 Testing agendas by time chart...")
        fig = factory.create_chart('time_series', 'agendas_by_time', knesset_filter=[25])
        if fig:
            print("✅ Agendas time series chart created successfully")
        else:
            print("❌ Agendas time series chart failed")
            
        print("\n🧪 Testing agenda classifications pie chart...")
        fig = factory.create_chart('distribution', 'agenda_classifications_pie', knesset_filter=[25])
        if fig:
            print("✅ Agenda classifications pie chart created successfully")
        else:
            print("❌ Agenda classifications pie chart failed")
            
    except Exception as e:
        print(f"❌ Error testing charts: {e}")
        import traceback
        traceback.print_exc()
else:
    print("❌ Database not found")
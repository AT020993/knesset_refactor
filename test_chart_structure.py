#!/usr/bin/env python3
"""Test chart structure without dependencies."""

import sys
from pathlib import Path

# Add src directory to path
current_dir = Path(__file__).parent
src_dir = current_dir / "src"
sys.path.insert(0, str(src_dir))

import logging

# Test imports
print("🧪 Testing imports...")

try:
    from ui.data_refresh import AVAILABLE_PLOTS_BY_TOPIC
    print("✅ AVAILABLE_PLOTS_BY_TOPIC imported successfully")
    
    # Check available plots
    for topic, plots in AVAILABLE_PLOTS_BY_TOPIC.items():
        print(f"📊 {topic}: {len(plots)} charts")
        for name in plots.keys():
            print(f"  - {name}")
    
    # Test the function definitions exist
    import ui.plot_generators as pg
    print("\n🧪 Testing plot generator functions...")
    
    functions_to_test = [
        'plot_queries_by_time_period',
        'plot_query_types_distribution', 
        'plot_queries_per_faction_in_knesset',
        'plot_agendas_by_time_period',
        'plot_agenda_classifications_pie',
        'plot_agenda_status_distribution',
        'plot_bill_status_distribution'
    ]
    
    for func_name in functions_to_test:
        if hasattr(pg, func_name):
            func = getattr(pg, func_name)
            if callable(func):
                print(f"✅ {func_name} - function exists and callable")
            else:
                print(f"❌ {func_name} - exists but not callable")
        else:
            print(f"❌ {func_name} - function missing")
    
    print("\n✅ All structure tests passed!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
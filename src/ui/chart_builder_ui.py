from __future__ import annotations

# Standard Library Imports
import logging
from pathlib import Path

# Third-Party Imports
import streamlit as st

# Local Application Imports
from .chart_builder import ChartBuilder
from .chart_renderer import ChartRenderer


def display_chart_builder(
    db_path: Path,
    max_rows_for_chart_builder: int,
    max_unique_values_for_facet: int,
    faction_display_map_global: dict,
    logger_obj: logging.Logger,
):
    """Renders the Interactive Chart Builder UI and logic."""
    
    st.header("📊 Interactive Chart Builder")
    if not db_path.exists():
        st.warning("Database not found. Chart Builder requires data. Please run a data refresh.")
        return
    
    # Initialize core components
    chart_builder = ChartBuilder(
        db_path, max_rows_for_chart_builder, max_unique_values_for_facet,
        faction_display_map_global, logger_obj
    )
    chart_renderer = ChartRenderer(chart_builder)
    
    # Initialize session state
    chart_builder.initialize_session_state()
    
    # 1. Table Selection
    selected_table = chart_renderer.render_table_selection()
    
    # Handle table selection changes
    if chart_builder.handle_table_selection(selected_table):
        st.rerun()
    
    # Only proceed if a valid table is selected
    if not st.session_state.get("builder_selected_table"):
        logger_obj.debug("Chart Builder: No valid table selected, so chart options are not rendered.")
        return
    
    logger_obj.info(f"Chart Builder: Rendering options for table: {st.session_state.builder_selected_table}")
    st.write(f"Selected Table: **{st.session_state.builder_selected_table}**")
    
    # 2. Chart Type Selection
    chart_renderer.render_chart_type_selection()
    
    # Handle chart type changes
    if chart_builder.handle_chart_type_change():
        st.rerun()
    
    # 3. Chart-Specific Filters
    chart_renderer.render_chart_specific_filters()
    
    # 4. Chart Aesthetics
    chart_renderer.render_chart_aesthetics()
    
    # 5. Advanced Layout Options
    chart_renderer.render_advanced_layout_options()
    
    # 6. Generate Chart Button
    if st.button("🚀 Generate Chart", key="btn_generate_custom_chart", type="primary"):
        chart_renderer.generate_chart()
    
    # 7. Display Generated Chart
    chart_renderer.render_generated_chart()
    
    # Update previous chart type for next run
    if "builder_chart_type" not in st.session_state:
        st.session_state.previous_builder_chart_type = None
    else:
        st.session_state.previous_builder_chart_type = st.session_state.builder_chart_type
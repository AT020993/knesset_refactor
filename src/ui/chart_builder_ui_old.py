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

    # Initialize session state for new options if they don't exist
    if "builder_title_font_size" not in st.session_state:
        st.session_state.builder_title_font_size = 20
    if "builder_title_font_family" not in st.session_state:
        st.session_state.builder_title_font_family = "Open Sans" # Default font
    if "builder_axis_label_font_size" not in st.session_state:
        st.session_state.builder_axis_label_font_size = 14
    if "builder_legend_orientation" not in st.session_state:
        st.session_state.builder_legend_orientation = "v" # Vertical
    if "builder_legend_x" not in st.session_state:
        st.session_state.builder_legend_x = 1.02 # Default x position (right of plot) - float
    if "builder_legend_y" not in st.session_state:
        st.session_state.builder_legend_y = 1.0 # Default y position (top) - FIXED TO FLOAT
    if "builder_color_palette" not in st.session_state:
        st.session_state.builder_color_palette = "Plotly" # Default palette
    if "builder_marker_opacity" not in st.session_state:
        st.session_state.builder_marker_opacity = 1.0


    # 1. Select Table
    db_tables_for_builder = [""] + ui_utils.get_db_table_list(db_path, logger_obj)

    current_builder_selected_table_value = st.session_state.get('builder_selected_table')
    table_select_default_index = 0
    if current_builder_selected_table_value and current_builder_selected_table_value in db_tables_for_builder:
        try:
            table_select_default_index = db_tables_for_builder.index(current_builder_selected_table_value)
        except ValueError:
            table_select_default_index = 0

    selectbox_output_table = st.selectbox(
        "1. Select Table to Visualize:",
        options=db_tables_for_builder,
        index=table_select_default_index,
        key="builder_table_select_widget",
    )

    if selectbox_output_table != st.session_state.get("builder_selected_table_previous_run"):
        logger_obj.info(f"Chart Builder: User selection for table changed from '{st.session_state.get('builder_selected_table_previous_run')}' to '{selectbox_output_table}'.")

        if selectbox_output_table and selectbox_output_table != "":
            st.session_state.builder_selected_table = selectbox_output_table
            logger_obj.info(f"Chart Builder: Valid table '{selectbox_output_table}' set. Updating columns and resetting axes.")
            all_cols, numeric_cols, cat_cols = ui_utils.get_table_columns(db_path, selectbox_output_table, logger_obj)
            st.session_state.builder_columns = [""] + all_cols
            st.session_state.builder_numeric_columns = [""] + numeric_cols
            st.session_state.builder_categorical_columns = [""] + cat_cols

            con = None
            try:
                con = ui_utils.connect_db(db_path, read_only=True, _logger_obj=logger_obj)
                temp_query = f'SELECT * FROM "{selectbox_output_table}"'
                temp_where_clauses = []

                sidebar_knesset_filter = st.session_state.get("ms_knesset_filter", [])
                sidebar_faction_filter_names = st.session_state.get("ms_faction_filter", [])

                actual_knesset_col = next((col for col in all_cols if col.lower() == "knessetnum"), None)
                actual_faction_col = next((col for col in all_cols if col.lower() == "factionid"), None)

                if actual_knesset_col and sidebar_knesset_filter:
                    temp_where_clauses.append(f'"{actual_knesset_col}" IN ({", ".join(map(str, sidebar_knesset_filter))})')
                if actual_faction_col and sidebar_faction_filter_names:
                    sidebar_faction_ids = [faction_display_map_global[name] for name in sidebar_faction_filter_names if name in faction_display_map_global]
                    if sidebar_faction_ids:
                        temp_where_clauses.append(f'"{actual_faction_col}" IN ({", ".join(map(str, sidebar_faction_ids))})')

                if temp_where_clauses:
                    temp_query += " WHERE " + " AND ".join(temp_where_clauses)
                temp_query += f" LIMIT {max_rows_for_chart_builder}"

                st.session_state.builder_data_for_cs_filters = ui_utils.safe_execute_query(con, temp_query, logger_obj)
                logger_obj.info(f"Fetched {len(st.session_state.builder_data_for_cs_filters)} rows for chart-specific filter population from table '{selectbox_output_table}' after global filters.")

            except Exception as e_filter_data:
                logger_obj.error(f"Error fetching data for chart-specific filter options: {e_filter_data}", exc_info=True)
                st.session_state.builder_data_for_cs_filters = pd.DataFrame()
            finally:
                if con:
                    con.close()
        else:
            logger_obj.info("Chart Builder: Placeholder selected for table. Resetting active table and dependent state.")
            st.session_state.builder_selected_table = None
            st.session_state.builder_columns = [""]
            st.session_state.builder_numeric_columns = [""]
            st.session_state.builder_categorical_columns = [""]
            st.session_state.builder_data_for_cs_filters = pd.DataFrame()

        # Reset all dependent selections
        st.session_state.builder_x_axis = None
        st.session_state.builder_y_axis = None
        st.session_state.builder_color = None
        st.session_state.builder_size = None
        st.session_state.builder_facet_row = None
        st.session_state.builder_facet_col = None
        st.session_state.builder_hover_name = None
        st.session_state.builder_names = None
        st.session_state.builder_values = None
        st.session_state.builder_generated_chart = None
        st.session_state.builder_knesset_filter_cs = []
        st.session_state.builder_faction_filter_cs = []

        st.session_state.builder_selected_table_previous_run = selectbox_output_table
        st.rerun()

    if st.session_state.get("builder_selected_table"):
        logger_obj.info(f"Chart Builder: Rendering options for table: {st.session_state.builder_selected_table}")
        st.write(f"Selected Table: **{st.session_state.builder_selected_table}**")

        # 2. Select Chart Type
        chart_types = ["bar", "line", "scatter", "pie", "histogram", "box"]
        current_chart_type = st.session_state.get("builder_chart_type", "bar")
        st.session_state.builder_chart_type = st.selectbox(
            "2. Select Chart Type:",
            options=chart_types,
            index=chart_types.index(current_chart_type) if current_chart_type in chart_types else 0,
            key="builder_chart_type_selector"
        )

        # --- Chart-Specific Filters UI ---
        st.markdown("##### 3. Apply Chart-Specific Filters (Optional):")
        cs_filter_data = st.session_state.get("builder_data_for_cs_filters", pd.DataFrame())

        if cs_filter_data.empty and st.session_state.get("builder_selected_table"):
             st.info(f"No data available for table '{st.session_state.builder_selected_table}' with the current global sidebar filters. Chart-specific filters cannot be populated.")

        if not cs_filter_data.empty and 'KnessetNum' in cs_filter_data.columns:
            # Sort Knesset numbers in descending order
            unique_knessets_in_data = sorted(cs_filter_data['KnessetNum'].dropna().unique().astype(int), reverse=True) # MODIFIED HERE
            if unique_knessets_in_data:
                st.session_state.builder_knesset_filter_cs = st.multiselect(
                    "Filter by Knesset Number(s) (Chart Specific):",
                    options=unique_knessets_in_data,
                    default=st.session_state.get("builder_knesset_filter_cs", []),
                    key="builder_knesset_filter_cs_widget"
                )
            else:
                st.caption("No Knesset numbers available in the filtered data for chart-specific Knesset filtering.")

        if not cs_filter_data.empty and 'FactionID' in cs_filter_data.columns:
            unique_faction_ids_in_data = cs_filter_data['FactionID'].dropna().unique()
            # Sort faction names alphabetically for the multiselect options
            faction_options_sorted = sorted(
                [
                    display_name
                    for display_name, f_id in faction_display_map_global.items()
                    if f_id in unique_faction_ids_in_data
                ]
            )
            if faction_options_sorted: # Check if there are any factions to show after filtering
                st.session_state.builder_faction_filter_cs = st.multiselect(
                    "Filter by Faction(s) (Chart Specific):",
                    options=faction_options_sorted,
                    default=st.session_state.get("builder_faction_filter_cs", []),
                    key="builder_faction_filter_cs_widget"
                )
            else:
                 st.caption("No factions available in the filtered data for chart-specific faction filtering.")
        # --- End Chart-Specific Filters UI ---

        if st.session_state.get("previous_builder_chart_type") != st.session_state.builder_chart_type:
            logger_obj.info(f"Chart type changed from {st.session_state.get('previous_builder_chart_type')} to {st.session_state.builder_chart_type}. Validating axes.")
            new_chart_type = st.session_state.builder_chart_type
            rerun_needed_for_chart_type_change = False
            y_axis_current = st.session_state.get("builder_y_axis")
            y_options_numeric = st.session_state.get("builder_numeric_columns", [])

            if new_chart_type in ["line", "scatter"]:
                if y_axis_current and y_axis_current not in y_options_numeric:
                    logger_obj.info(f"Resetting Y-axis ('{y_axis_current}') as it's not numeric and new chart type '{new_chart_type}' requires numeric Y.")
                    st.session_state.builder_y_axis = None
                    rerun_needed_for_chart_type_change = True

            if new_chart_type != "pie" and st.session_state.get("previous_builder_chart_type") == "pie":
                st.session_state.builder_names = None
                st.session_state.builder_values = None
                rerun_needed_for_chart_type_change = True

            st.session_state.previous_builder_chart_type = new_chart_type
            st.session_state.builder_generated_chart = None

            if rerun_needed_for_chart_type_change:
                st.rerun()

        # 4. Configure Chart Aesthetics
        st.markdown("##### 4. Configure Chart Aesthetics:")
        cols_c1, cols_c2 = st.columns(2)
        with cols_c1:
            def get_safe_index(options_list, current_value_key, default_value=None):
                val = st.session_state.get(current_value_key, default_value)
                try:
                    return options_list.index(val) if val and val in options_list else 0
                except ValueError: return 0

            x_axis_options = st.session_state.get("builder_columns", [""])
            st.session_state.builder_x_axis = st.selectbox("X-axis:", options=x_axis_options, index=get_safe_index(x_axis_options, "builder_x_axis"), key="cb_x_axis")

            if st.session_state.builder_chart_type not in ["pie", "histogram",]:
                y_axis_options_all = st.session_state.get("builder_columns", [""])
                y_axis_options_numeric = st.session_state.get("builder_numeric_columns", [""])
                current_y_options = y_axis_options_numeric if st.session_state.builder_chart_type not in ["bar", "box"] else y_axis_options_all
                st.session_state.builder_y_axis = st.selectbox("Y-axis:", options=current_y_options, index=get_safe_index(current_y_options, "builder_y_axis"), help="Select a numeric column for Y-axis (Bar and Box plots can also use categorical).", key="cb_y_axis")

            if st.session_state.builder_chart_type == "pie":
                pie_names_options = st.session_state.get("builder_categorical_columns", [""])
                st.session_state.builder_names = st.selectbox("Names (for Pie chart slices):", options=pie_names_options, index=get_safe_index(pie_names_options, "builder_names"), key="cb_pie_names")

                pie_values_options = st.session_state.get("builder_numeric_columns", [""])
                st.session_state.builder_values = st.selectbox("Values (for Pie chart sizes):", options=pie_values_options, index=get_safe_index(pie_values_options, "builder_values"), key="cb_pie_values")

            color_by_options = st.session_state.get("builder_columns", [""])
            st.session_state.builder_color = st.selectbox("Color by:", options=color_by_options, index=get_safe_index(color_by_options, "builder_color"), key="cb_color")

            if st.session_state.builder_chart_type in ["scatter"]:
                size_by_options = st.session_state.get("builder_numeric_columns", [""])
                st.session_state.builder_size = st.selectbox("Size by (for scatter):", options=size_by_options, index=get_safe_index(size_by_options, "builder_size"), key="cb_size")

        with cols_c2:
            facet_row_options = st.session_state.get("builder_columns", [""])
            st.session_state.builder_facet_row = st.selectbox("Facet Row by:", options=facet_row_options, index=get_safe_index(facet_row_options, "builder_facet_row"), key="cb_facet_row")

            facet_col_options = st.session_state.get("builder_columns", [""])
            st.session_state.builder_facet_col = st.selectbox("Facet Column by:", options=facet_col_options, index=get_safe_index(facet_col_options, "builder_facet_col"), key="cb_facet_col")

            hover_name_options = st.session_state.get("builder_columns", [""])
            st.session_state.builder_hover_name = st.selectbox("Hover Name:", options=hover_name_options, index=get_safe_index(hover_name_options, "builder_hover_name"), key="cb_hover_name")

            if st.session_state.builder_chart_type not in ["pie"]:
                st.session_state.builder_log_x = st.checkbox("Logarithmic X-axis", value=st.session_state.get("builder_log_x", False), key="cb_log_x")
                if st.session_state.builder_chart_type not in ["histogram"]:
                    st.session_state.builder_log_y = st.checkbox("Logarithmic Y-axis", value=st.session_state.get("builder_log_y", False), key="cb_log_y")

            if st.session_state.builder_chart_type == "bar":
                barmode_options = ["relative", "group", "overlay", "stack"]
                current_barmode = st.session_state.get("builder_barmode", "stack")
                st.session_state.builder_barmode = st.selectbox("Bar Mode:", options=barmode_options, index=barmode_options.index(current_barmode) if current_barmode in barmode_options else 0, key="cb_barmode")

        # 5. Advanced Layout Options
        st.markdown("##### 5. Advanced Layout Options:")
        adv_cols1, adv_cols2 = st.columns(2)
        with adv_cols1:
            st.session_state.builder_title_font_family = st.selectbox(
                "Title Font Family:", options=PLOTLY_FONT_FAMILIES,
                index=get_safe_index(PLOTLY_FONT_FAMILIES, "builder_title_font_family", "Open Sans"),
                key="cb_title_font_family"
            )
            st.session_state.builder_title_font_size = st.slider(
                "Title Font Size:", min_value=10, max_value=40,
                value=st.session_state.get("builder_title_font_size", 20),
                key="cb_title_font_size"
            )
            st.session_state.builder_axis_label_font_size = st.slider(
                "Axis Label Font Size:", min_value=8, max_value=30,
                value=st.session_state.get("builder_axis_label_font_size", 14),
                key="cb_axis_label_font_size"
            )
            st.session_state.builder_color_palette = st.selectbox(
                "Color Palette (Qualitative):", options=list(PLOTLY_COLOR_SCALES.keys()),
                index=get_safe_index(list(PLOTLY_COLOR_SCALES.keys()), "builder_color_palette", "Plotly"),
                key="cb_color_palette",
                help="Applied if 'Color by' is a categorical column."
            )

        with adv_cols2:
            st.session_state.builder_legend_orientation = st.selectbox(
                "Legend Orientation:", options=["v", "h"],
                index=["v", "h"].index(st.session_state.get("builder_legend_orientation", "v")),
                key="cb_legend_orientation"
            )
            st.session_state.builder_legend_x = st.number_input(
                "Legend X Position (0-1.5):", min_value=0.0, max_value=1.5, step=0.01,
                value=float(st.session_state.get("builder_legend_x", 1.02)),
                key="cb_legend_x"
            )
            st.session_state.builder_legend_y = st.number_input(
                "Legend Y Position (0-1.5):", min_value=0.0, max_value=1.5, step=0.01,
                value=float(st.session_state.get("builder_legend_y", 1.0)),
                key="cb_legend_y"
            )
            if st.session_state.builder_chart_type == "scatter":
                st.session_state.builder_marker_opacity = st.slider(
                    "Marker Opacity (for scatter):", min_value=0.1, max_value=1.0, step=0.1,
                    value=st.session_state.get("builder_marker_opacity", 1.0),
                    key="cb_marker_opacity"
                )


        if st.button("🚀 Generate Chart", key="btn_generate_custom_chart", type="primary"):
            logger_obj.info("--- 'Generate Chart' BUTTON CLICKED ---")

            selected_x = st.session_state.get('builder_x_axis') if st.session_state.get('builder_x_axis', "") != "" else None
            selected_y = st.session_state.get('builder_y_axis') if st.session_state.get('builder_y_axis', "") != "" else None
            selected_names = st.session_state.get('builder_names') if st.session_state.get('builder_names', "") != "" else None
            selected_values = st.session_state.get('builder_values') if st.session_state.get('builder_values', "") != "" else None
            selected_color = st.session_state.get('builder_color') if st.session_state.get('builder_color', "") != "" else None
            selected_size = st.session_state.get('builder_size') if st.session_state.get('builder_size', "") != "" else None
            selected_facet_row = st.session_state.get('builder_facet_row') if st.session_state.get('builder_facet_row', "") != "" else None
            selected_facet_col = st.session_state.get('builder_facet_col') if st.session_state.get('builder_facet_col', "") != "" else None
            selected_hover_name = st.session_state.get('builder_hover_name') if st.session_state.get('builder_hover_name', "") != "" else None

            knesset_filter_cs_selected = st.session_state.get("builder_knesset_filter_cs", [])
            faction_filter_cs_selected_names = st.session_state.get("builder_faction_filter_cs", [])

            faction_filter_cs_selected_ids = []
            if faction_filter_cs_selected_names:
                temp_cs_filter_data_for_map = st.session_state.get("builder_data_for_cs_filters", pd.DataFrame())
                if not temp_cs_filter_data_for_map.empty and 'FactionID' in temp_cs_filter_data_for_map.columns:
                    unique_faction_ids_in_cs_data = temp_cs_filter_data_for_map['FactionID'].dropna().unique()
                    temp_chart_specific_faction_display_map = {
                        display_name: f_id
                        for display_name, f_id in faction_display_map_global.items()
                        if f_id in unique_faction_ids_in_cs_data
                    }
                    faction_filter_cs_selected_ids = [temp_chart_specific_faction_display_map[name] for name in faction_filter_cs_selected_names if name in temp_chart_specific_faction_display_map]

            logger_obj.debug(f"Chart Builder Selections: X='{selected_x}', Y='{selected_y}', ChartType='{st.session_state.builder_chart_type}'")
            logger_obj.debug(f"Chart Specific Filters: Knessets={knesset_filter_cs_selected}, Factions IDs={faction_filter_cs_selected_ids}")

            valid_input = True
            active_table_for_chart = st.session_state.get("builder_selected_table")
            if not active_table_for_chart:
                st.error("Error: No table selected for chart generation."); valid_input = False
            elif st.session_state.builder_chart_type not in ["pie", "histogram", "box"] and (not selected_x or not selected_y):
                st.error("Please select valid X-axis and Y-axis columns for this chart type."); valid_input = False
            elif st.session_state.builder_chart_type in ["histogram", "box"] and not selected_x :
                if not (st.session_state.builder_chart_type == "box" and selected_x and selected_y):
                     st.error(f"Please select a valid X-axis for the {st.session_state.builder_chart_type} chart (and optionally Y for box)."); valid_input = False
            elif st.session_state.builder_chart_type == "pie" and (not selected_names or not selected_values):
                st.error("Please select valid 'Names' and 'Values' columns for the Pie chart."); valid_input = False

            if valid_input:
                logger_obj.info(f"Input validated for table '{active_table_for_chart}'. Proceeding to fetch data and generate chart.")
                try:
                    df_for_chart = st.session_state.get("builder_data_for_cs_filters", pd.DataFrame()).copy()
                    logger_obj.info(f"Starting with {len(df_for_chart)} rows for chart (data already globally filtered).")

                    if knesset_filter_cs_selected and 'KnessetNum' in df_for_chart.columns:
                        if not pd.api.types.is_integer_dtype(df_for_chart['KnessetNum']) and df_for_chart['KnessetNum'].notna().any():
                            try:
                                df_for_chart['KnessetNum'] = pd.to_numeric(df_for_chart['KnessetNum'], errors='coerce').fillna(-1).astype(int)
                            except Exception as e_conv:
                                logger_obj.warning(f"Could not convert KnessetNum to int for chart-specific filtering: {e_conv}")
                        df_for_chart = df_for_chart[df_for_chart['KnessetNum'].isin(knesset_filter_cs_selected)]
                        logger_obj.info(f"After chart-specific Knesset filter: {len(df_for_chart)} rows.")

                    if faction_filter_cs_selected_ids and 'FactionID' in df_for_chart.columns:
                        if not pd.api.types.is_integer_dtype(df_for_chart['FactionID']) and df_for_chart['FactionID'].notna().any():
                            try:
                                df_for_chart['FactionID'] = pd.to_numeric(df_for_chart['FactionID'], errors='coerce').fillna(-1).astype(int)
                            except Exception as e_conv:
                                 logger_obj.warning(f"Could not convert FactionID to int for chart-specific filtering: {e_conv}")
                        df_for_chart = df_for_chart[df_for_chart['FactionID'].isin(faction_filter_cs_selected_ids)]
                        logger_obj.info(f"After chart-specific Faction filter: {len(df_for_chart)} rows.")

                    if df_for_chart.empty:
                        st.warning("No data remains after applying all filters (global and chart-specific). Cannot generate chart.")
                        logger_obj.warning("DataFrame for chart is empty after all filters applied.")
                        st.session_state.builder_generated_chart = None
                    else:
                        chart_params = {"data_frame": df_for_chart,
                                        "title": f"{st.session_state.builder_chart_type.capitalize()} of {active_table_for_chart}"}

                        if selected_x: chart_params["x"] = selected_x
                        if selected_y and st.session_state.builder_chart_type not in ["histogram", "box"]: chart_params["y"] = selected_y
                        elif selected_y and st.session_state.builder_chart_type == "box": chart_params["y"] = selected_y

                        if st.session_state.builder_chart_type == "pie":
                            if selected_names: chart_params["names"] = selected_names
                            if selected_values: chart_params["values"] = selected_values
                        if selected_color: chart_params["color"] = selected_color
                        if selected_size and st.session_state.builder_chart_type == "scatter": chart_params["size"] = selected_size

                        facet_issue = False
                        if selected_facet_row:
                            if selected_facet_row in df_for_chart.columns:
                                unique_facet_rows = df_for_chart[selected_facet_row].nunique()
                                if unique_facet_rows > max_unique_values_for_facet:
                                    st.error(f"Cannot use '{selected_facet_row}' for Facet Row: Too many unique values ({unique_facet_rows}). Max allowed: {max_unique_values_for_facet}.")
                                    facet_issue = True
                                else:
                                    chart_params["facet_row"] = selected_facet_row

                        if selected_facet_col and not facet_issue:
                            if selected_facet_col in df_for_chart.columns:
                                unique_facet_cols = df_for_chart[selected_facet_col].nunique()
                                if unique_facet_cols > max_unique_values_for_facet:
                                    st.error(f"Cannot use '{selected_facet_col}' for Facet Column: Too many unique values ({unique_facet_cols}). Max allowed: {max_unique_values_for_facet}.")
                                    facet_issue = True
                                else:
                                    chart_params["facet_col"] = selected_facet_col

                        if facet_issue:
                            st.session_state.builder_generated_chart = None
                            logger_obj.warning("Chart generation halted due to facet cardinality issue.")
                        else:
                            if selected_hover_name: chart_params["hover_name"] = selected_hover_name
                            if st.session_state.builder_chart_type not in ["pie"]: chart_params["log_x"] = st.session_state.get("builder_log_x", False)
                            if st.session_state.builder_chart_type not in ["pie", "histogram"]: chart_params["log_y"] = st.session_state.get("builder_log_y", False)
                            if st.session_state.builder_chart_type == "bar" and st.session_state.get("builder_barmode"): chart_params["barmode"] = st.session_state.builder_barmode
                            
                            selected_palette_name = st.session_state.get("builder_color_palette", "Plotly")
                            chart_params["color_discrete_sequence"] = PLOTLY_COLOR_SCALES.get(selected_palette_name, px.colors.qualitative.Plotly)


                            logger_obj.info(f"Attempting to generate {st.session_state.builder_chart_type} chart with params: {chart_params}")

                            essential_missing = False
                            current_chart_type = st.session_state.builder_chart_type
                            if current_chart_type == "pie" and (not chart_params.get("names") or not chart_params.get("values")):
                                st.error("For Pie chart, 'Names' and 'Values' must be selected with valid columns."); essential_missing = True
                            elif current_chart_type == "histogram" and not chart_params.get("x"):
                                st.error(f"For {current_chart_type} chart, 'X-axis' must be selected."); essential_missing = True
                            elif current_chart_type == "box" and not chart_params.get("x") and not chart_params.get("y"): 
                                st.error(f"For Box chart, at least 'X-axis' or 'Y-axis' must be selected."); essential_missing = True
                            elif current_chart_type not in ["pie", "histogram", "box"] and (not chart_params.get("x") or not chart_params.get("y")):
                                st.error(f"For {current_chart_type} chart, 'X-axis' and 'Y-axis' must be selected with valid columns."); essential_missing = True


                            if essential_missing:
                                logger_obj.warning("Essential parameters missing for chart generation right before Plotly call.")
                                st.session_state.builder_generated_chart = None
                            else:
                                fig_builder = getattr(px, current_chart_type)(**chart_params)

                                fig_builder.update_layout(
                                    title_font_family=st.session_state.get("builder_title_font_family"),
                                    title_font_size=st.session_state.get("builder_title_font_size"),
                                    xaxis_title_font_size=st.session_state.get("builder_axis_label_font_size"),
                                    yaxis_title_font_size=st.session_state.get("builder_axis_label_font_size"),
                                    legend_orientation=st.session_state.get("builder_legend_orientation"),
                                    legend_x=float(st.session_state.get("builder_legend_x", 1.02)), 
                                    legend_y=float(st.session_state.get("builder_legend_y", 1.0)),  
                                    legend_font_size=st.session_state.get("builder_axis_label_font_size")
                                )
                                if st.session_state.builder_chart_type == "scatter" and "builder_marker_opacity" in st.session_state:
                                    fig_builder.update_traces(marker=dict(opacity=st.session_state.builder_marker_opacity))


                                st.session_state.builder_generated_chart = fig_builder
                                st.toast(f"Chart '{chart_params['title']}' generated!", icon="✅")
                except Exception as e:
                    logger_obj.error(f"Error generating custom chart: {e}", exc_info=True)
                    st.error(f"Could not generate chart: {e}")
                    st.code(f"Error details: {str(e)}\n\nTraceback:\n{ui_utils.format_exception_for_ui(sys.exc_info())}")
                    st.session_state.builder_generated_chart = None
            else:
                 logger_obj.warning("Input validation failed for chart generation (before data fetch).")

        if st.session_state.get("builder_generated_chart"):
            st.plotly_chart(st.session_state.builder_generated_chart, use_container_width=True)
    else:
        logger_obj.debug("Chart Builder: No valid table selected in st.session_state.builder_selected_table, so chart options are not rendered.")

    if "builder_chart_type" not in st.session_state:
        st.session_state.previous_builder_chart_type = None
    else:
        st.session_state.previous_builder_chart_type = st.session_state.builder_chart_type

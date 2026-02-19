"""Query results, filtering, pagination, and ad-hoc SQL operations."""

from __future__ import annotations

import io
import re
import sys
from typing import Any, Callable

import pandas as pd
import streamlit as st

import ui.ui_utils as ui_utils
from ui.state.session_manager import SessionStateManager
from utils.export_verifier import ExportVerifier


def render_query_results_display(renderer: Any) -> None:
    """Render predefined query results display including downloads and SQL panel."""
    query_name = SessionStateManager.get_executed_query_name()
    query_name_safe = query_name or "query_results"
    results_df = SessionStateManager.get_query_results_df()
    applied_filters = SessionStateManager.get_applied_filters_info_query()
    last_sql = SessionStateManager.get_last_executed_sql()

    subheader_text = f"Results for: **{query_name_safe}**"
    if applied_filters:
        filters_applied_text = "; ".join(applied_filters)
        if (
            filters_applied_text
            and filters_applied_text != "Knesset(s): All; Faction(s): All"
        ):
            subheader_text += f" (Active Filters: *{filters_applied_text}*)"
    st.markdown(subheader_text)

    if not results_df.empty and "KnessetNum" in results_df.columns:
        render_local_knesset_filter(renderer, results_df)

    display_df = results_df

    if not display_df.empty:
        if "BillPrimaryDocumentURL" in display_df.columns:
            display_df = renderer._document_handler.format_bill_document_links(display_df)

        formatted_df = ui_utils.format_dataframe_dates(display_df, _logger_obj=renderer.logger)
        column_config = renderer._document_handler.get_column_config(formatted_df)
        st.dataframe(
            formatted_df,
            use_container_width=True,
            height=400,
            column_config=column_config,
        )

        if "BillID" in display_df.columns and "BillDocumentCount" in display_df.columns:
            renderer._document_handler.render_multi_document_view(display_df)

        safe_name = re.sub(r"[^a-zA-Z0-9_\-]+", "_", query_name_safe)
        render_download_options(renderer, display_df, safe_name)
        last_params = SessionStateManager.get_last_query_params()
        renderer._dataset_exporter.render_full_dataset_download(
            last_sql, safe_name, last_params
        )
    else:
        st.info("The query returned no results with the current filters.")

    with st.expander("Show Executed SQL", expanded=False):
        st.code(last_sql if last_sql else "No SQL executed yet.", language="sql")


def render_download_options(renderer: Any, display_df: pd.DataFrame, safe_name: str) -> None:
    """Render CSV/Excel export and verification controls."""
    col_csv, col_excel, col_verify = st.columns([1, 1, 1])
    verifier = ExportVerifier(renderer.logger)

    with col_csv:
        csv_data = display_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇️ CSV",
            csv_data,
            f"{safe_name}_results.csv",
            "text/csv",
            key=f"csv_dl_{safe_name}",
        )

    with col_excel:
        excel_buffer = renderer._document_handler.create_excel_with_hyperlinks(display_df)
        st.download_button(
            "⬇️ Excel (with hyperlinks)",
            excel_buffer.getvalue(),
            f"{safe_name}_results.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"excel_dl_{safe_name}",
            help="Excel file with clickable document and website links",
        )

    with col_verify:
        csv_buffer = io.BytesIO(csv_data)
        verification = verifier.verify_csv_export(display_df, csv_buffer)
        if verification["is_valid"]:
            st.success(f"✅ Data verified: {verification['source_rows']} rows", icon="✅")
        else:
            st.warning(f"⚠️ {verification['details']}", icon="⚠️")


def apply_knesset_filter_callback(renderer: Any) -> None:
    """Apply local Knesset filter and rerun predefined query."""
    if st.session_state.temp_knesset_filter == "All Knessetes":
        st.session_state.ms_knesset_filter = []
    else:
        try:
            knesset_num = int(st.session_state.temp_knesset_filter.replace("Knesset ", ""))
            st.session_state.ms_knesset_filter = [knesset_num]
        except (ValueError, AttributeError) as exc:
            renderer.logger.error(
                "Invalid Knesset filter format: %s, error: %s",
                st.session_state.temp_knesset_filter,
                exc,
            )
            st.error("Invalid Knesset filter format")
            return

    st.session_state.query_page_number = 1
    st.session_state.query_page_offset = 0

    query_name = SessionStateManager.get_executed_query_name()
    if query_name:
        from ui.sidebar import _handle_run_query_button_click
        from ui.queries.predefined_queries import PREDEFINED_QUERIES

        _handle_run_query_button_click(
            exports_dict=PREDEFINED_QUERIES,
            db_path=renderer.db_path,
            connect_func=lambda read_only=True: ui_utils.connect_db(
                renderer.db_path,
                read_only,
                renderer.logger,
            ),
            ui_logger=renderer.logger,
            format_exc_func=ui_utils.format_exception_for_ui,
            faction_display_map=st.session_state.get("faction_display_map", {}),
        )


def render_local_knesset_filter(renderer: Any, results_df: pd.DataFrame) -> None:
    """Render Knesset filter widget and pagination controls for query results."""
    query_name = SessionStateManager.get_executed_query_name()
    query_type = get_query_type_from_name(query_name or "")

    available_knessetes = ui_utils.get_available_knessetes_for_query(
        renderer.db_path,
        query_type,
        _logger_obj=renderer.logger,
    )

    st.markdown("**Filter Results by Knesset:**")
    st.info(
        "💡 Select a Knesset and click 'Apply Filter' to re-run the query with up to "
        "1,000 rows from the selected Knesset.",
        icon="ℹ️",
    )

    col1, col2, col3 = st.columns([3, 1, 1])

    with col1:
        current_ms_filter = st.session_state.get("ms_knesset_filter", [])
        if current_ms_filter and len(current_ms_filter) == 1:
            current_value = f"Knesset {current_ms_filter[0]}"
        else:
            current_value = "All Knessetes"

        if "temp_knesset_filter" not in st.session_state:
            st.session_state.temp_knesset_filter = current_value

        knesset_options = ["All Knessetes"] + [
            f"Knesset {k}" for k in available_knessetes
        ]
        selected_filter = st.selectbox(
            "Select Knesset:",
            options=knesset_options,
            index=(
                knesset_options.index(st.session_state.temp_knesset_filter)
                if st.session_state.temp_knesset_filter in knesset_options
                else 0
            ),
            key="local_knesset_filter_widget",
            help="Select a Knesset and click 'Apply Filter' to re-run the query.",
        )
        st.session_state.temp_knesset_filter = selected_filter

    with col2:
        st.button(
            "🔄 Apply Filter",
            key="apply_knesset_filter_btn",
            on_click=apply_knesset_filter_callback,
            kwargs={"renderer": renderer},
            use_container_width=True,
        )

    with col3:
        st.metric("Rows", len(results_df))

    render_pagination_controls(renderer, results_df)


def render_pagination_controls(renderer: Any, results_df: pd.DataFrame) -> None:
    """Render paging controls for query results."""
    if "query_page_number" not in st.session_state:
        st.session_state.query_page_number = 1

    st.markdown("---")
    st.markdown("**Navigate Results:**")

    current_page = st.session_state.query_page_number
    rows_per_page = 1000
    start_row = (current_page - 1) * rows_per_page + 1
    end_row = start_row + len(results_df) - 1

    col1, col2, col3, col4 = st.columns([2, 1, 1, 2])

    with col1:
        st.info(f"📄 Showing rows {start_row:,}-{end_row:,} (Page {current_page})")

    with col2:
        if st.button(
            "◀ Previous",
            key="prev_page_btn",
            disabled=(current_page == 1),
            use_container_width=True,
        ):
            st.session_state.query_page_number = max(1, current_page - 1)
            st.session_state.query_page_offset = (
                st.session_state.query_page_number - 1
            ) * 1000
            rerun_query_with_pagination(renderer)

    with col3:
        has_more = len(results_df) == rows_per_page
        if st.button(
            "Next ▶",
            key="next_page_btn",
            disabled=not has_more,
            use_container_width=True,
        ):
            st.session_state.query_page_number = current_page + 1
            st.session_state.query_page_offset = (
                st.session_state.query_page_number - 1
            ) * 1000
            rerun_query_with_pagination(renderer)

    with col4:
        if current_page > 1 and st.button(
            "⏮ First Page",
            key="first_page_btn",
            use_container_width=True,
        ):
            st.session_state.query_page_number = 1
            st.session_state.query_page_offset = 0
            rerun_query_with_pagination(renderer)


def rerun_query_with_pagination(renderer: Any) -> None:
    """Re-execute current predefined query with updated page offset."""
    query_name = SessionStateManager.get_executed_query_name()
    if query_name:
        from ui.sidebar import _handle_run_query_button_click
        from ui.queries.predefined_queries import PREDEFINED_QUERIES

        _handle_run_query_button_click(
            exports_dict=PREDEFINED_QUERIES,
            db_path=renderer.db_path,
            connect_func=lambda read_only=True: ui_utils.connect_db(
                renderer.db_path,
                read_only,
                renderer.logger,
            ),
            ui_logger=renderer.logger,
            format_exc_func=ui_utils.format_exception_for_ui,
            faction_display_map=st.session_state.get("faction_display_map", {}),
        )


def get_query_type_from_name(query_name: str) -> str:
    """Infer query group from query name."""
    if not query_name:
        return "queries"

    query_name_lower = query_name.lower()
    if "bill" in query_name_lower:
        return "bills"
    if "agenda" in query_name_lower:
        return "agendas"
    return "queries"


def execute_ad_hoc_query(renderer: Any, sql_query: str, connect_func: Callable) -> None:
    """Execute ad-hoc SQL and render dataframe + CSV download."""
    con = None
    try:
        con = connect_func(read_only=True)
        adhoc_result_df = ui_utils.safe_execute_query(
            con,
            sql_query,
            renderer.logger,
        )

        formatted_adhoc_df = ui_utils.format_dataframe_dates(
            adhoc_result_df,
            _logger_obj=renderer.logger,
        )
        st.dataframe(formatted_adhoc_df, use_container_width=True)

        if not adhoc_result_df.empty:
            csv_data = adhoc_result_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "⬇️ CSV",
                csv_data,
                "adhoc_results.csv",
                "text/csv",
                key="adhoc_csv_dl",
            )
    except Exception as exc:
        renderer.logger.error(f"❌ Ad-hoc SQL Query Error: {exc}", exc_info=True)
        st.error(f"❌ SQL Query Error: {ui_utils.format_exception_for_ui(sys.exc_info())}")
    finally:
        if con:
            con.close()

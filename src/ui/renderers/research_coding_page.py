"""
Research Coding Data Import Page.

Provides a Streamlit interface for importing, analyzing, and managing
researcher-provided policy classification data (MajorIL, MinorIL, CAP codes, etc.)
for bills, parliamentary queries, and agenda motions.

Tabs:
1. Import Files — Upload and import coding data files
2. Gap Analysis — Coverage metrics and per-Knesset breakdown
3. Current Data — View, search, and manage imported coding data
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

from utils.research_coding_importer import ResearchCodingImporter, ImportResult


class ResearchCodingPageRenderer:
    """Renderer for the Research Coding import and analysis page."""

    ACCEPTED_TYPES = ["csv", "xlsx", "xls"]

    def __init__(self, db_path: Path, logger: Optional[logging.Logger] = None):
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)
        self.importer = ResearchCodingImporter(db_path, self.logger)

    def render(self) -> None:
        """Main render method — displays the Research Coding page."""
        st.header("Research Coding Data")
        st.caption(
            "Import and manage researcher-provided policy classification data "
            "(MajorIL/MinorIL, CAP codes, Religion, Territories) for bills, queries, and agendas."
        )

        # Use radio for tab-like navigation (persists across form submissions)
        tab_options = ["Import Files", "Gap Analysis", "Current Data"]
        if "rc_active_tab" not in st.session_state:
            st.session_state.rc_active_tab = tab_options[0]

        selected_tab = st.radio(
            "Section:",
            tab_options,
            horizontal=True,
            key="rc_tab_selector",
            label_visibility="collapsed",
        )
        st.session_state.rc_active_tab = selected_tab

        st.markdown("---")

        if selected_tab == "Import Files":
            self._render_import_tab()
        elif selected_tab == "Gap Analysis":
            self._render_gap_analysis_tab()
        elif selected_tab == "Current Data":
            self._render_current_data_tab()

    # --- Import Tab ---

    def _render_import_tab(self) -> None:
        """Render file upload and import controls."""
        st.subheader("Import Coding Files")

        for data_type, label, help_text in [
            ("bills", "Bills Coding File", "Excel/CSV with BILLID and coding columns (MAJORIL, MINORIL, MAJORCAP, etc.)"),
            ("queries", "Queries Coding File", "Excel/CSV with id and coding columns (majorIL, minorIL, CAP_Maj, etc.)"),
            ("agendas", "Agendas Coding File", "Excel/CSV with id2/Subject and coding columns. K19-20 match by id2, K23-24 by title."),
        ]:
            with st.expander(f"**{label}**", expanded=False):
                self._render_single_import(data_type, label, help_text)

    def _render_single_import(self, data_type: str, label: str, help_text: str) -> None:
        """Render upload + import for a single data type."""
        st.caption(help_text)
        uploaded = st.file_uploader(
            f"Upload {label}",
            type=self.ACCEPTED_TYPES,
            key=f"rc_upload_{data_type}",
            label_visibility="collapsed",
        )

        if uploaded is not None:
            # Save to temp and read preview
            import tempfile
            suffix = Path(uploaded.name).suffix
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(uploaded.getvalue())
                tmp_path = Path(tmp.name)

            df, error = self.importer.read_file(tmp_path)
            if error:
                st.error(f"Error reading file: {error}")
                return

            st.info(f"**{uploaded.name}**: {len(df):,} rows, {len(df.columns)} columns")

            # Show column names
            with st.expander("Column names", expanded=False):
                st.write(list(df.columns))

            # Show preview
            st.dataframe(df.head(5), use_container_width=True)

            # Import button
            if st.button(f"Import {data_type.title()}", key=f"rc_import_{data_type}"):
                with st.spinner(f"Importing {data_type}..."):
                    if data_type == "bills":
                        result = self.importer.import_bill_coding(tmp_path)
                    elif data_type == "queries":
                        result = self.importer.import_query_coding(tmp_path)
                    elif data_type == "agendas":
                        result = self.importer.import_agenda_coding(tmp_path)
                    else:
                        return

                self._display_import_result(result)

            # Clean up temp file
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

    def _display_import_result(self, result: ImportResult) -> None:
        """Display import results with metrics and optional download."""
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total in File", f"{result.total_rows_in_file:,}")
        col2.metric("Inserted", f"{result.rows_imported:,}")
        col3.metric("Updated", f"{result.rows_updated:,}")
        col4.metric("Skipped", f"{result.rows_skipped_no_match:,}")

        if result.match_method_counts:
            st.markdown("**Match method breakdown:**")
            method_df = pd.DataFrame([
                {"Method": k, "Count": v}
                for k, v in sorted(result.match_method_counts.items())
            ])
            st.dataframe(method_df, use_container_width=True, hide_index=True)

        if result.errors:
            with st.expander(f"Errors ({len(result.errors)})", expanded=False):
                for err in result.errors[:20]:
                    st.warning(err)
                if len(result.errors) > 20:
                    st.info(f"... and {len(result.errors) - 20} more errors")

        if result.unmatched_items is not None and not result.unmatched_items.empty:
            st.warning(f"{len(result.unmatched_items):,} items could not be matched")
            with st.expander("Unmatched items preview"):
                st.dataframe(result.unmatched_items.head(50), use_container_width=True)
            csv = result.unmatched_items.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "Download Unmatched Items CSV",
                csv,
                file_name=f"{result.data_type}_unmatched.csv",
                mime="text/csv",
                key=f"rc_dl_unmatched_{result.data_type}",
            )

        if not result.errors:
            st.success(
                f"Import complete: {result.rows_imported:,} inserted, "
                f"{result.rows_updated:,} updated"
            )

    # --- Gap Analysis Tab ---

    def _render_gap_analysis_tab(self) -> None:
        """Render coverage metrics and per-Knesset breakdown."""
        st.subheader("Gap Analysis")
        st.caption(
            "Compare coding coverage against dashboard data. "
            "Shows which items are coded, which are missing, and per-Knesset breakdown."
        )

        stats = self.importer.get_coding_statistics()
        has_any = any(v > 0 for v in stats.values())
        if not has_any:
            st.info("No coding data imported yet. Use the Import tab to upload files.")
            return

        for data_type, label in [("bills", "Bills"), ("queries", "Queries"), ("agendas", "Agendas")]:
            if stats.get(data_type, 0) == 0:
                continue

            with st.expander(f"**{label}** ({stats[data_type]:,} coded items)", expanded=True):
                gap = self.importer.generate_gap_analysis(data_type)
                if gap is None:
                    st.error(f"Could not generate gap analysis for {data_type}")
                    continue

                # Summary metrics
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("In Dashboard", f"{gap.total_in_dashboard:,}")
                c2.metric("Coded", f"{gap.total_coded:,}")
                c3.metric("Matched", f"{gap.coded_and_matched:,}")
                coverage = (
                    round(100.0 * gap.coded_and_matched / gap.total_in_dashboard, 1)
                    if gap.total_in_dashboard > 0
                    else 0.0
                )
                c4.metric("Coverage", f"{coverage}%")

                # Per-Knesset table
                if not gap.coverage_by_knesset.empty:
                    st.markdown("**Per-Knesset Coverage:**")
                    st.dataframe(gap.coverage_by_knesset, use_container_width=True, hide_index=True)

                    csv = gap.coverage_by_knesset.to_csv(index=False).encode("utf-8-sig")
                    st.download_button(
                        f"Download {label} Coverage CSV",
                        csv,
                        file_name=f"{data_type}_coverage.csv",
                        mime="text/csv",
                        key=f"rc_dl_coverage_{data_type}",
                    )

                # Coded but not in dashboard
                if not gap.coded_not_in_dashboard.empty:
                    st.markdown(
                        f"**{len(gap.coded_not_in_dashboard):,} coded items not found in dashboard**"
                    )
                    csv2 = gap.coded_not_in_dashboard.to_csv(index=False).encode("utf-8-sig")
                    st.download_button(
                        f"Download {label} Orphans CSV",
                        csv2,
                        file_name=f"{data_type}_orphans.csv",
                        mime="text/csv",
                        key=f"rc_dl_orphans_{data_type}",
                    )

                # Uncoded in dashboard
                if not gap.uncoded_in_dashboard.empty:
                    st.markdown("**Uncoded items per Knesset:**")
                    st.dataframe(gap.uncoded_in_dashboard, use_container_width=True, hide_index=True)

    # --- Current Data Tab ---

    def _render_current_data_tab(self) -> None:
        """Render view/search/manage for imported coding data."""
        st.subheader("Imported Coding Data")

        stats = self.importer.get_coding_statistics()
        has_any = any(v > 0 for v in stats.values())

        # Summary
        c1, c2, c3 = st.columns(3)
        c1.metric("Bills", f"{stats.get('bills', 0):,}")
        c2.metric("Queries", f"{stats.get('queries', 0):,}")
        c3.metric("Agendas", f"{stats.get('agendas', 0):,}")

        if not has_any:
            st.info("No coding data imported yet.")
            return

        # View data sections
        for data_type, table, pk, label in [
            ("bills", "UserBillCoding", "BillID", "Bills"),
            ("queries", "UserQueryCoding", "QueryID", "Queries"),
            ("agendas", "UserAgendaCoding", "AgendaID", "Agendas"),
        ]:
            if stats.get(data_type, 0) == 0:
                continue

            with st.expander(f"**{label} Coding Data** ({stats[data_type]:,} rows)", expanded=False):
                self._render_data_viewer(data_type, table, pk, label)

    def _render_data_viewer(self, data_type: str, table: str, pk: str, label: str) -> None:
        """Render a data viewer with search and clear controls."""
        from backend.connection_manager import get_db_connection, safe_execute_query

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                df = safe_execute_query(
                    conn,
                    f"SELECT * FROM {table} ORDER BY {pk} LIMIT 500",
                    self.logger,
                )
            if df is not None and not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)

                csv = df.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    f"Download {label} Coding CSV",
                    csv,
                    file_name=f"{data_type}_coding_data.csv",
                    mime="text/csv",
                    key=f"rc_dl_data_{data_type}",
                )
            else:
                st.info("No data found.")
        except Exception as e:
            st.error(f"Error loading data: {e}")

        # Clear button with confirmation
        st.markdown("---")
        confirm_key = f"rc_confirm_clear_{data_type}"
        if confirm_key not in st.session_state:
            st.session_state[confirm_key] = False

        if not st.session_state[confirm_key]:
            if st.button(f"Clear {label} Coding Data", key=f"rc_clear_{data_type}"):
                st.session_state[confirm_key] = True
        else:
            st.warning(f"Are you sure you want to delete ALL {label.lower()} coding data?")
            col1, col2 = st.columns(2)
            if col1.button("Yes, delete", key=f"rc_confirm_yes_{data_type}"):
                success, error = self.importer.clear_coding_data(data_type)
                if success:
                    st.success(f"{label} coding data cleared.")
                else:
                    st.error(f"Error: {error}")
                st.session_state[confirm_key] = False
            if col2.button("Cancel", key=f"rc_confirm_no_{data_type}"):
                st.session_state[confirm_key] = False

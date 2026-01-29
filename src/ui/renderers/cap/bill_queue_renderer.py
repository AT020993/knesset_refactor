"""
CAP Bill Queue Renderer

Handles rendering of the bill queue for annotation:
- Bill list with status badges
- Recent annotations panel
- Other researchers' annotations
- Bill details view
"""

import logging
from typing import Optional, Tuple

import streamlit as st
import pandas as pd

from ui.services.cap_service import CAPAnnotationService


class CAPBillQueueRenderer:
    """Renders the bill queue for CAP annotation."""

    def __init__(
        self,
        service: CAPAnnotationService,
        logger_obj: Optional[logging.Logger] = None,
    ):
        """
        Initialize bill queue renderer.

        Args:
            service: CAP annotation service
            logger_obj: Optional logger
        """
        self.service = service
        self.logger = logger_obj or logging.getLogger(__name__)

    def render_bill_queue(self, researcher_id: int) -> Tuple[Optional[int], str]:
        """
        Render the bills queue with search, recent annotations, and status badges.

        In multi-annotator mode, shows bills that the current researcher hasn't
        annotated yet, regardless of other researchers' annotations.

        Args:
            researcher_id: The current researcher's database ID

        Returns:
            Tuple of (bill_id, submission_date) or (None, "")
        """
        st.subheader("📋 Bills Queue")

        # Recently Annotated Panel (collapsible, at top for quick access)
        self._render_recent_annotations(researcher_id)

        # Search input (free-text search by Bill ID or Name)
        search_term = st.text_input(
            "🔍 Search by Bill ID or Name",
            placeholder="Enter ID or keywords...",
            key="cap_bill_search",
        )

        # Filters row
        col1, col2, col3 = st.columns([2, 2, 2])
        with col1:
            # Use session state for knesset filter (can be set from coverage dashboard)
            default_knesset = st.session_state.get("cap_filter_knesset", None)
            knesset_options = [None] + list(range(25, 0, -1))
            knesset_index = 0 if default_knesset is None else knesset_options.index(default_knesset)
            knesset_filter = st.selectbox(
                "Filter by Knesset",
                options=knesset_options,
                index=knesset_index,
                format_func=lambda x: "All" if x is None else f"Knesset {x}",
                key="cap_knesset_filter",
            )
            # Update session state
            st.session_state["cap_filter_knesset"] = knesset_filter

        with col2:
            limit = st.selectbox(
                "Results to Show", options=[25, 50, 100, 200], index=1,
                key="cap_limit_filter",
            )

        with col3:
            # Toggle to show annotated bills too
            show_annotated = st.checkbox(
                "Show my annotated bills too",
                value=False,
                key="cap_show_annotated",
            )

        # Get bills with status badges (filtered by researcher_id)
        bills = self.service.get_bills_with_status(
            knesset_num=knesset_filter,
            limit=limit,
            search_term=search_term if search_term else None,
            include_coded=show_annotated,
            researcher_id=researcher_id,
        )

        if bills.empty:
            if search_term:
                st.warning(f"No bills found matching '{search_term}'")
            else:
                st.success("🎉 No bills to code! All caught up!")
            return None, ""

        # Count coded vs uncoded (by this researcher)
        coded_count = bills["IsCoded"].sum() if "IsCoded" in bills.columns else 0
        uncoded_count = len(bills) - coded_count
        if show_annotated:
            st.info(f"Found {len(bills)} bills ({uncoded_count} uncoded by you, {coded_count} annotated by you)")
        else:
            st.info(f"Found {len(bills)} bills for you to code")

        # Selection with status badges
        selected_idx = st.selectbox(
            "Select bill to code",
            options=range(len(bills)),
            format_func=lambda i: self._format_bill_option(bills.iloc[i]),
            key="cap_bill_select",
        )

        if selected_idx is not None:
            selected_bill = bills.iloc[selected_idx]
            self._render_bill_details(selected_bill)

            # Show other researchers' annotations for this bill
            self._render_other_annotations(int(selected_bill["BillID"]), researcher_id)

            # Return bill ID and publication date
            pub_date = selected_bill.get("PublicationDate", "")
            return int(selected_bill["BillID"]), pub_date if pub_date else ""

        return None, ""

    def _render_recent_annotations(self, researcher_id: int):
        """Render collapsible panel of recently annotated bills by this researcher."""
        recent = self.service.get_recent_annotations(limit=5, researcher_id=researcher_id)
        if recent.empty:
            return

        with st.expander("📝 My Recent Annotations (last 5)", expanded=False):
            for _, row in recent.iterrows():
                try:
                    # Use .get() with defaults for safe column access
                    raw_bill_name = row.get("BillName", "Unknown")
                    bill_name = str(raw_bill_name)[:50] if raw_bill_name else "Unknown"
                    if raw_bill_name and len(str(raw_bill_name)) > 50:
                        bill_name += "..."
                    bill_id = row.get("BillID", "?")
                    minor_code = row.get("MinorCode", "?")
                    st.markdown(
                        f"📄 **{bill_id}** - {bill_name} "
                        f"[{minor_code}]"
                    )
                except Exception as e:
                    self.logger.warning(f"Error rendering recent annotation row: {e}")
                    continue

    def _render_other_annotations(self, bill_id: int, current_researcher_id: int):
        """
        Show annotations from other researchers for comparison.

        Allows researchers to see how others have coded the same bill,
        supporting inter-rater reliability discussions.
        """
        try:
            all_annotations = self.service.get_all_annotations_for_bill(bill_id)

            if all_annotations.empty:
                return

            # Filter out current researcher's annotation (with column existence check)
            if "ResearcherID" not in all_annotations.columns:
                self.logger.warning(f"ResearcherID column missing for bill {bill_id}")
                return

            other_annotations = all_annotations[
                all_annotations["ResearcherID"] != current_researcher_id
            ]

            if other_annotations.empty:
                return

            with st.expander(f"👥 Other Annotations ({len(other_annotations)})", expanded=False):
                st.caption("Compare with other researchers' annotations:")
                for _, ann in other_annotations.iterrows():
                    try:
                        researcher_name = ann.get("ResearcherName", "Unknown")
                        assigned_date = ann.get("AssignedDate", "N/A")
                        minor_topic = ann.get("MinorTopic_HE", "")
                        minor_code = ann.get("CAPMinorCode", "")

                        st.markdown(
                            f"**{researcher_name}** ({assigned_date}): "
                            f"{minor_topic} [{minor_code}]"
                        )
                        notes = ann.get("Notes")
                        if notes:
                            st.caption(f"📝 Notes: {notes}")
                        st.divider()
                    except Exception as row_error:
                        self.logger.warning(f"Error rendering annotation row: {row_error}")
                        continue
        except Exception as e:
            self.logger.error(f"Error rendering other annotations for bill {bill_id}: {e}")

    def _format_bill_option(self, bill: pd.Series) -> str:
        """Format bill option with status badge and annotation count."""
        is_coded = bill.get("IsCoded", 0)
        annotation_count = bill.get("AnnotationCount", 0)

        # Handle None, NaN, and empty bill names
        raw_name = bill.get("BillName")
        if raw_name is None or (isinstance(raw_name, float) and pd.isna(raw_name)) or raw_name == "":
            bill_name = "Unknown"
        else:
            bill_name = str(raw_name)[:60]
            if len(str(raw_name)) > 60:
                bill_name += "..."

        # Build annotation count badge for multi-annotator visibility
        count_badge = ""
        if annotation_count > 0:
            count_badge = f" 👥{annotation_count}"

        if is_coded:
            # Show ✅ with category code for bills annotated by this researcher
            minor_code = bill.get("MinorCode", "")
            return f"✅ [{minor_code}] {bill['BillID']} - {bill_name}{count_badge}"
        else:
            # Show ⭕ for uncoded bills (by this researcher)
            return f"⭕ {bill['BillID']} - {bill_name}{count_badge}"

    def _render_bill_details(self, bill: pd.Series):
        """Render bill details section with defensive null checks."""
        try:
            # Safely extract all fields with null/NaN protection
            bill_name = bill.get("BillName", "Unknown Bill")
            if pd.isna(bill_name) or bill_name == "":
                bill_name = "Unknown Bill"

            bill_id = bill.get("BillID", "?")
            if pd.isna(bill_id):
                bill_id = "?"

            knesset_num = bill.get("KnessetNum", "?")
            if pd.isna(knesset_num):
                knesset_num = "?"

            bill_type = bill.get("BillType", "Unknown")
            if pd.isna(bill_type) or bill_type == "":
                bill_type = "Unknown"

            status_desc = bill.get("StatusDesc", "Unknown")
            if pd.isna(status_desc) or status_desc == "":
                status_desc = "Unknown"

            pub_date = bill.get("PublicationDate", "N/A")
            if pd.isna(pub_date) or pub_date == "":
                pub_date = "N/A"

            bill_url = bill.get("BillURL")
            if pd.isna(bill_url) or bill_url == "":
                bill_url = None

            # Render with safe values
            st.markdown("---")
            st.markdown(f"### 📄 {bill_name}")

            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(f"**ID:** {bill_id}")
                st.markdown(f"**Knesset:** {knesset_num}")
            with col2:
                st.markdown(f"**Type:** {bill_type}")
                st.markdown(f"**Status:** {status_desc}")
            with col3:
                st.markdown(f"**Publication Date:** {pub_date}")

            # Link to Knesset website (only if URL is valid)
            if bill_url:
                st.markdown(
                    f'🔗 <a href="{bill_url}" target="_blank" rel="noopener noreferrer">View on Knesset Website</a>',
                    unsafe_allow_html=True,
                )
        except Exception as e:
            self.logger.warning(f"Error rendering bill details: {e}")
            st.warning("Could not display bill details")

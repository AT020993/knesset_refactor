"""
CAP Form Renderer

Handles all annotation form rendering: new annotations, edits, and API-sourced bills.
"""

import logging
from pathlib import Path
from typing import Optional, Callable

import streamlit as st
import pandas as pd

from ui.services.cap_service import CAPAnnotationService
from ui.services.cap_api_service import get_cap_api_service


class CAPFormRenderer:
    """Renders CAP annotation forms."""

    def __init__(
        self,
        service: CAPAnnotationService,
        logger_obj: Optional[logging.Logger] = None,
        on_annotation_saved: Optional[Callable] = None,
    ):
        """
        Initialize form renderer.

        Args:
            service: CAP annotation service
            logger_obj: Optional logger
            on_annotation_saved: Callback when annotation is saved (e.g., clear cache)
        """
        self.service = service
        self.logger = logger_obj or logging.getLogger(__name__)
        self._on_annotation_saved = on_annotation_saved

    def _notify_annotation_saved(self):
        """Notify that an annotation was saved."""
        # Set flag to reset coded bills filters so user sees their new annotation
        st.session_state["cap_annotation_just_saved"] = True
        if self._on_annotation_saved:
            self._on_annotation_saved()

    def render_bill_queue(self) -> Optional[tuple]:
        """
        Render the uncoded bills queue.

        Returns:
            Tuple of (bill_id, submission_date) or (None, None)
        """
        st.subheader("📋 Bills Queue")

        # Filters
        col1, col2 = st.columns(2)
        with col1:
            knesset_filter = st.selectbox(
                "Filter by Knesset",
                options=[None] + list(range(25, 0, -1)),
                format_func=lambda x: "All" if x is None else f"Knesset {x}",
            )
        with col2:
            limit = st.selectbox(
                "Results to Show", options=[25, 50, 100, 200], index=1
            )

        # Get uncoded bills
        uncoded_bills = self.service.get_uncoded_bills(
            knesset_num=knesset_filter, limit=limit
        )

        if uncoded_bills.empty:
            st.success("🎉 No bills to code! All caught up!")
            return None, None

        st.info(f"Found {len(uncoded_bills)} bills to code")

        # Selection
        selected_idx = st.selectbox(
            "Select bill to code",
            options=range(len(uncoded_bills)),
            format_func=lambda i: f"{uncoded_bills.iloc[i]['BillID']} - {uncoded_bills.iloc[i]['BillName'][:80]}...",
        )

        if selected_idx is not None:
            selected_bill = uncoded_bills.iloc[selected_idx]
            self._render_bill_details(selected_bill)

            # Return bill ID and publication date
            pub_date = selected_bill.get("PublicationDate", "")
            return int(selected_bill["BillID"]), pub_date if pub_date else ""

        return None, None

    def _render_bill_details(self, bill: pd.Series):
        """Render bill details section."""
        st.markdown("---")
        st.markdown(f"### 📄 {bill['BillName']}")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"**ID:** {bill['BillID']}")
            st.markdown(f"**Knesset:** {bill['KnessetNum']}")
        with col2:
            st.markdown(f"**Type:** {bill['BillType']}")
            st.markdown(f"**Status:** {bill['StatusDesc']}")
        with col3:
            st.markdown(f"**Publication Date:** {bill.get('PublicationDate', 'N/A')}")

        # Link to Knesset website
        if "BillURL" in bill:
            st.markdown(
                f'🔗 <a href="{bill["BillURL"]}" target="_blank" rel="noopener noreferrer">View on Knesset Website</a>',
                unsafe_allow_html=True,
            )

    def render_annotation_form(
        self, bill_id: int, researcher_name: str, submission_date: str = ""
    ) -> bool:
        """
        Render the annotation form.

        Args:
            bill_id: The bill ID to annotate
            researcher_name: Name of the researcher
            submission_date: Pre-filled submission date from database

        Returns:
            True if annotation saved successfully
        """
        st.subheader("✏️ Annotation Form")

        # Get taxonomy
        taxonomy = self.service.get_taxonomy()
        if taxonomy.empty:
            st.error("Error loading taxonomy")
            return False

        with st.form("annotation_form"):
            # Category selections
            selected_major, selected_minor = self._render_category_selectors()

            # Show description for selected category
            if selected_minor:
                self._show_category_description(selected_minor)

            # Direction selection
            direction = self._render_direction_selector()

            # Submission date info
            if direction in [1, -1]:
                if submission_date:
                    st.info(f"📅 **Submission Date:** {submission_date} (from database)")
                else:
                    st.warning("⚠️ Submission date not available in database")

            # Confidence and notes
            confidence = st.selectbox(
                "Confidence Level", options=["High", "Medium", "Low"], index=1
            )
            notes = st.text_area(
                "Notes", placeholder="Additional notes about the annotation..."
            )

            col1, col2 = st.columns(2)
            with col1:
                submitted = st.form_submit_button("💾 Save Annotation", type="primary")
            with col2:
                skip = st.form_submit_button("⏭️ Skip")

            if submitted:
                return self._handle_form_submission(
                    bill_id,
                    selected_major,
                    selected_minor,
                    direction,
                    researcher_name,
                    confidence,
                    notes,
                    "Database",
                    submission_date,
                )

            if skip:
                st.info("Skipping this bill")

        return False

    def _render_category_selectors(self, prefix: str = "") -> tuple:
        """Render major and minor category selectors."""
        # Major category selection
        major_categories = self.service.get_major_categories()
        major_options = {
            f"{cat['MajorCode']} - {cat['MajorTopic_HE']} ({cat['MajorTopic_EN']})": cat[
                "MajorCode"
            ]
            for cat in major_categories
        }

        selected_major_label = st.selectbox(
            "Major Category *",
            options=list(major_options.keys()),
            index=None,
            placeholder="Select a major category...",
            key=f"{prefix}major" if prefix else None,
        )
        selected_major = (
            major_options.get(selected_major_label) if selected_major_label else None
        )

        # Minor category selection (filtered by major)
        minor_categories = self.service.get_minor_categories(selected_major)
        minor_options = {
            f"{cat['MinorCode']} - {cat['MinorTopic_HE']} ({cat['MinorTopic_EN']})": cat[
                "MinorCode"
            ]
            for cat in minor_categories
        }

        selected_minor_label = st.selectbox(
            "Minor Category *",
            options=list(minor_options.keys()),
            index=None,
            placeholder="Select a minor category...",
            key=f"{prefix}minor" if prefix else None,
        )
        selected_minor = (
            minor_options.get(selected_minor_label) if selected_minor_label else None
        )

        return selected_major, selected_minor

    def _show_category_description(self, minor_code: int):
        """Show description for selected minor category."""
        minor_categories = self.service.get_minor_categories(None)
        cat_info = next(
            (c for c in minor_categories if c["MinorCode"] == minor_code), None
        )
        if cat_info and cat_info.get("Description_HE"):
            st.info(f"**Description:** {cat_info['Description_HE']}")
        if cat_info and cat_info.get("Examples_HE"):
            st.caption(f"**Examples:** {cat_info['Examples_HE']}")

    def _render_direction_selector(self, default_index: int = None, key: str = None) -> int:
        """Render direction radio selector."""
        return st.radio(
            "Direction *",
            options=[1, -1, 0],
            index=default_index,
            format_func=lambda x: {
                1: "+1 הרחבה/חיזוק (Strengthening)",
                -1: "-1 צמצום/פגיעה (Weakening)",
                0: "0 אחר (Other)",
            }[x],
            horizontal=True,
            key=key,
        )

    def _handle_form_submission(
        self,
        bill_id: int,
        selected_major: Optional[int],
        selected_minor: Optional[int],
        direction: int,
        researcher_name: str,
        confidence: str,
        notes: str,
        source: str,
        submission_date: str,
    ) -> bool:
        """Handle form submission logic."""
        # Validate required fields
        if not selected_major:
            st.error("❌ Please select a Major Category")
            return False
        if not selected_minor:
            st.error("❌ Please select a Minor Category")
            return False

        # Save annotation
        success = self.service.save_annotation(
            bill_id=bill_id,
            cap_minor_code=selected_minor,
            direction=direction,
            assigned_by=researcher_name,
            confidence=confidence,
            notes=notes,
            source=source,
            submission_date=submission_date,
        )

        if success:
            st.success("✅ Annotation saved successfully!")
            self._notify_annotation_saved()
            return True
        else:
            st.error("❌ Error saving annotation")
            return False

    def render_api_annotation_form(
        self, bill_id: int, researcher_name: str, submission_date: str = ""
    ) -> bool:
        """Render annotation form for API-fetched bill."""
        st.subheader("✏️ Annotation Form (from API)")

        taxonomy = self.service.get_taxonomy()
        if taxonomy.empty:
            st.error("Error loading taxonomy")
            return False

        with st.form("api_annotation_form"):
            # Category selections with API prefix
            selected_major, selected_minor = self._render_category_selectors(prefix="api_")

            # Direction
            direction = self._render_direction_selector(key="api_direction")

            # Submission date info
            if direction in [1, -1]:
                if submission_date:
                    st.info(f"📅 **Submission Date:** {submission_date} (from API)")
                else:
                    st.warning("⚠️ Submission date not available from API")

            # Confidence
            confidence = st.selectbox(
                "Confidence Level",
                options=["High", "Medium", "Low"],
                index=1,
                key="api_confidence",
            )

            # Notes
            notes = st.text_area(
                "Notes", placeholder="Additional notes...", key="api_notes"
            )

            submitted = st.form_submit_button("💾 Save Annotation", type="primary")

            if submitted:
                success = self._handle_form_submission(
                    bill_id,
                    selected_major,
                    selected_minor,
                    direction,
                    researcher_name,
                    confidence,
                    notes,
                    "API",
                    submission_date,
                )

                if success:
                    # Remove from fetched bills
                    if "api_fetched_bills" in st.session_state:
                        st.session_state.api_fetched_bills = st.session_state.api_fetched_bills[
                            st.session_state.api_fetched_bills["BillID"] != bill_id
                        ]
                    return True

        return False

    def render_api_fetch_section(self, researcher_name: str):
        """Render section for fetching bills from API."""
        st.subheader("🌐 Fetch Bills from API")

        st.info(
            "Fetch bills directly from the Knesset API. Useful for recent bills not yet in the local database."
        )

        col1, col2 = st.columns(2)
        with col1:
            api_knesset = st.selectbox(
                "Knesset",
                options=[25, 24, 23, 22, 21, 20],
                index=0,
                key="api_knesset",
            )
        with col2:
            api_limit = st.selectbox(
                "Results to Fetch",
                options=[25, 50, 100, 200],
                index=1,
                key="api_limit",
            )

        if st.button("🔄 Fetch Bills", key="fetch_api_bills"):
            self._fetch_api_bills(api_knesset, api_limit)

        # Display fetched bills
        if (
            "api_fetched_bills" in st.session_state
            and not st.session_state.api_fetched_bills.empty
        ):
            self._render_api_bills_list(researcher_name)

    def _fetch_api_bills(self, knesset_num: int, limit: int):
        """Fetch bills from API."""
        with st.spinner("Fetching from API..."):
            try:
                api_service = get_cap_api_service(self.logger)
                api_bills = api_service.fetch_recent_bills_sync(
                    knesset_num=knesset_num, limit=limit
                )

                if api_bills.empty:
                    st.warning("No bills found")
                    return

                # Filter to bills not in database and not coded
                new_bills = self.service.get_bills_not_in_database(api_bills, limit=limit)

                if new_bills.empty:
                    st.success("All bills are already in database or coded!")
                    return

                st.session_state.api_fetched_bills = new_bills
                st.success(f"Found {len(new_bills)} new bills")

            except Exception as e:
                st.error(f"Fetch error: {e}")
                self.logger.error(f"API fetch error: {e}", exc_info=True)

    def _render_api_bills_list(self, researcher_name: str):
        """Render list of API-fetched bills."""
        api_bills = st.session_state.api_fetched_bills

        st.markdown("---")
        st.markdown(f"### Bills from API ({len(api_bills)} results)")

        # Select bill to code
        selected_api_idx = st.selectbox(
            "Select bill to code",
            options=range(len(api_bills)),
            format_func=lambda i: f"{api_bills.iloc[i]['BillID']} - {str(api_bills.iloc[i].get('Name', 'Unknown'))[:80]}...",
            key="api_bill_select",
        )

        if selected_api_idx is not None:
            selected_bill = api_bills.iloc[selected_api_idx]

            # Show bill details
            st.markdown("---")
            st.markdown(f"### 📄 {selected_bill.get('Name', 'Unknown')}")

            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**ID:** {selected_bill['BillID']}")
                st.markdown(f"**Knesset:** {selected_bill.get('KnessetNum', 'N/A')}")
            with col2:
                st.markdown(f"**Type:** {selected_bill.get('SubTypeDesc', 'N/A')}")

            # Link to Knesset website
            bill_url = selected_bill.get(
                "BillURL",
                f"https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid={selected_bill['BillID']}",
            )
            st.markdown(
                f'🔗 <a href="{bill_url}" target="_blank" rel="noopener noreferrer">View on Knesset Website</a>',
                unsafe_allow_html=True,
            )

            # Annotation form for API bill
            st.markdown("---")
            api_pub_date = (
                str(selected_bill.get("PublicationDate", ""))[:10]
                if selected_bill.get("PublicationDate")
                else ""
            )
            self.render_api_annotation_form(
                int(selected_bill["BillID"]), researcher_name, api_pub_date
            )

"""
CAP Bill Annotation Page

This module provides a Streamlit interface for researchers to annotate
bills according to the Democratic Erosion codebook. 

Features:
- Password-protected access
- Bill queue showing uncoded bills
- Annotation form with CAP code selection
- Direction coding (+1/-1/0)
- Progress dashboard
- Export functionality
"""

import logging
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime

import streamlit as st
import pandas as pd

from ui.services.cap_service import CAPAnnotationService, get_cap_service
from ui.services.cap_api_service import CAPAPIService, get_cap_api_service


class CAPAnnotationPageRenderer:
    """Renderer for the CAP annotation page."""

    @staticmethod
    def _clear_query_cache():
        """Clear query-related caches when annotations change."""
        # Clear session state query results so predefined queries fetch fresh data
        if 'query_results_df' in st.session_state:
            del st.session_state['query_results_df']
        if 'show_query_results' in st.session_state:
            st.session_state['show_query_results'] = False
        # Clear Streamlit's data cache
        st.cache_data.clear()

    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        """Initialize the renderer."""
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)
        self._service: Optional[CAPAnnotationService] = None
    
    @property
    def service(self) -> CAPAnnotationService:
        """Get or create the CAP service."""
        if self._service is None:
            self._service = get_cap_service(self.db_path, self.logger)
        return self._service
    
    def _check_authentication(self) -> Tuple[bool, str]:
        """
        Check if the user is authenticated for CAP annotation.
        
        Returns:
            Tuple of (is_authenticated, researcher_name)
        """
        try:
            # Check if CAP annotation is enabled
            if not st.secrets.get("cap_annotation", {}).get("enabled", False):
                return False, ""
            
            # Check session state for authentication
            if st.session_state.get("cap_authenticated", False):
                return True, st.session_state.get("cap_researcher_name", "Unknown")
            
            return False, ""
            
        except Exception:
            # Secrets not configured
            return False, ""
    
    def _render_login_form(self) -> bool:
        """
        Render the login form.

        Returns:
            True if login successful, False otherwise
        """
        st.subheader("🔐 Login")

        st.info("This system is for authorized researchers only to annotate bills according to the Democratic Erosion codebook.")

        with st.form("cap_login_form"):
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login")

            if submitted:
                try:
                    correct_password = st.secrets.get("cap_annotation", {}).get("password", "")
                    researcher_name = st.secrets.get("cap_annotation", {}).get("researcher_name", "Researcher")

                    if password == correct_password and correct_password:
                        st.session_state.cap_authenticated = True
                        st.session_state.cap_researcher_name = researcher_name
                        st.session_state.cap_login_time = datetime.now()
                        st.success(f"Welcome, {researcher_name}!")
                        st.rerun()
                        return True
                    else:
                        st.error("Incorrect password")
                        return False

                except Exception as e:
                    st.error(f"Login error: {e}")
                    return False

        return False
    
    def _render_stats_dashboard(self):
        """Render the annotation statistics dashboard."""
        stats = self.service.get_annotation_stats()

        if not stats:
            st.warning("No annotation data found")
            return

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "Bills Coded",
                stats.get('total_coded', 0)
            )

        with col2:
            st.metric(
                "Total Bills",
                stats.get('total_bills', 0)
            )

        with col3:
            total = stats.get('total_bills', 1)
            coded = stats.get('total_coded', 0)
            pct = (coded / total * 100) if total > 0 else 0
            st.metric(
                "Coding Progress",
                f"{pct:.1f}%"
            )

        # By direction chart
        if stats.get('by_direction'):
            st.subheader("By Direction")
            direction_data = pd.DataFrame(stats['by_direction'])
            if not direction_data.empty:
                direction_data['label'] = direction_data['Direction'].map({
                    1: 'הרחבה/חיזוק (+1)',
                    -1: 'צמצום/פגיעה (-1)',
                    0: 'אחר (0)'
                })
                st.bar_chart(direction_data.set_index('label')['count'])

        # By major category
        if stats.get('by_major_category'):
            st.subheader("By Major Category")
            cat_data = pd.DataFrame(stats['by_major_category'])
            if not cat_data.empty:
                st.bar_chart(cat_data.set_index('MajorTopic_HE')['count'])
    
    def _render_bill_queue(self) -> Optional[int]:
        """
        Render the uncoded bills queue.

        Returns:
            Selected bill ID or None
        """
        st.subheader("📋 Bills Queue")

        # Filters
        col1, col2 = st.columns(2)
        with col1:
            knesset_filter = st.selectbox(
                "Filter by Knesset",
                options=[None] + list(range(25, 0, -1)),
                format_func=lambda x: "All" if x is None else f"Knesset {x}"
            )
        with col2:
            limit = st.selectbox(
                "Results to Show",
                options=[25, 50, 100, 200],
                index=1
            )

        # Get uncoded bills
        uncoded_bills = self.service.get_uncoded_bills(
            knesset_num=knesset_filter,
            limit=limit
        )

        if uncoded_bills.empty:
            st.success("🎉 No bills to code! All caught up!")
            return None

        st.info(f"Found {len(uncoded_bills)} bills to code")

        # Display bills table
        display_df = uncoded_bills[['BillID', 'KnessetNum', 'BillName', 'BillType', 'StatusDesc']].copy()
        display_df.columns = ['ID', 'Knesset', 'Bill Name', 'Type', 'Status']

        # Selection
        selected_idx = st.selectbox(
            "Select bill to code",
            options=range(len(uncoded_bills)),
            format_func=lambda i: f"{uncoded_bills.iloc[i]['BillID']} - {uncoded_bills.iloc[i]['BillName'][:80]}..."
        )

        if selected_idx is not None:
            selected_bill = uncoded_bills.iloc[selected_idx]

            # Show bill details
            st.markdown("---")
            st.markdown(f"### 📄 {selected_bill['BillName']}")

            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(f"**ID:** {selected_bill['BillID']}")
                st.markdown(f"**Knesset:** {selected_bill['KnessetNum']}")
            with col2:
                st.markdown(f"**Type:** {selected_bill['BillType']}")
                st.markdown(f"**Status:** {selected_bill['StatusDesc']}")
            with col3:
                st.markdown(f"**Publication Date:** {selected_bill.get('PublicationDate', 'N/A')}")

            # Link to Knesset website (opens in new tab)
            if 'BillURL' in selected_bill:
                st.markdown(f'🔗 <a href="{selected_bill["BillURL"]}" target="_blank" rel="noopener noreferrer">View on Knesset Website</a>', unsafe_allow_html=True)

            # Return bill ID and publication date
            pub_date = selected_bill.get('PublicationDate', '')
            return int(selected_bill['BillID']), pub_date if pub_date else ''

        return None, None

    def _render_annotation_form(self, bill_id: int, researcher_name: str, submission_date: str = "") -> bool:
        """
        Render the annotation form.

        Args:
            bill_id: The bill ID to annotate
            researcher_name: Name of the researcher
            submission_date: Pre-filled submission date from database (auto-populated)

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
            # Major category selection
            major_categories = self.service.get_major_categories()
            major_options = {
                f"{cat['MajorCode']} - {cat['MajorTopic_HE']} ({cat['MajorTopic_EN']})": cat['MajorCode']
                for cat in major_categories
            }

            selected_major_label = st.selectbox(
                "Major Category *",
                options=list(major_options.keys())
            )
            selected_major = major_options.get(selected_major_label)

            # Minor category selection (filtered by major)
            minor_categories = self.service.get_minor_categories(selected_major)
            minor_options = {
                f"{cat['MinorCode']} - {cat['MinorTopic_HE']} ({cat['MinorTopic_EN']})": cat['MinorCode']
                for cat in minor_categories
            }

            selected_minor_label = st.selectbox(
                "Minor Category *",
                options=list(minor_options.keys())
            )
            selected_minor = minor_options.get(selected_minor_label)

            # Show description for selected category (kept in Hebrew as per codebook)
            if selected_minor:
                cat_info = next((c for c in minor_categories if c['MinorCode'] == selected_minor), None)
                if cat_info and cat_info.get('Description_HE'):
                    st.info(f"**Description:** {cat_info['Description_HE']}")
                if cat_info and cat_info.get('Examples_HE'):
                    st.caption(f"**Examples:** {cat_info['Examples_HE']}")

            # Direction selection
            direction = st.radio(
                "Direction *",
                options=[1, -1, 0],
                format_func=lambda x: {
                    1: "+1 הרחבה/חיזוק (Strengthening)",
                    -1: "-1 צמצום/פגיעה (Weakening)",
                    0: "0 אחר (Other)"
                }[x],
                horizontal=True
            )

            # Submission date (auto-populated from database)
            if direction in [1, -1]:
                if submission_date:
                    st.info(f"📅 **Submission Date:** {submission_date} (from database)")
                else:
                    st.warning("⚠️ Submission date not available in database")

            # Confidence level
            confidence = st.selectbox(
                "Confidence Level",
                options=["High", "Medium", "Low"],
                index=1
            )

            # Notes
            notes = st.text_area(
                "Notes",
                placeholder="Additional notes about the annotation..."
            )

            col1, col2 = st.columns(2)
            with col1:
                submitted = st.form_submit_button("💾 Save Annotation", type="primary")
            with col2:
                skip = st.form_submit_button("⏭️ Skip")

            if submitted:
                # Save (submission_date is auto-populated from database)
                success = self.service.save_annotation(
                    bill_id=bill_id,
                    cap_minor_code=selected_minor,
                    direction=direction,
                    assigned_by=researcher_name,
                    confidence=confidence,
                    notes=notes,
                    source="Database",
                    submission_date=submission_date
                )

                if success:
                    st.success("✅ Annotation saved successfully!")
                    self._clear_query_cache()
                    return True
                else:
                    st.error("❌ Error saving annotation")
                    return False

            if skip:
                st.info("Skipping this bill")
                st.rerun()

        return False
    
    def _render_coded_bills_view(self):
        """Render view of already coded bills with edit capability."""
        st.subheader("📚 Coded Bills")

        # Filters
        col1, col2 = st.columns(2)
        with col1:
            knesset_filter = st.selectbox(
                "Filter by Knesset",
                options=[None] + list(range(25, 0, -1)),
                format_func=lambda x: "All" if x is None else f"Knesset {x}",
                key="coded_knesset_filter"
            )
        with col2:
            taxonomy = self.service.get_taxonomy()
            cap_options = {None: "All"}
            for _, row in taxonomy.iterrows():
                cap_options[row['MinorCode']] = f"{row['MinorCode']} - {row['MinorTopic_HE']}"

            cap_filter = st.selectbox(
                "Filter by CAP Code",
                options=list(cap_options.keys()),
                format_func=lambda x: cap_options.get(x, str(x))
            )

        coded_bills = self.service.get_coded_bills(
            knesset_num=knesset_filter,
            cap_code=cap_filter,
            limit=100
        )

        if coded_bills.empty:
            st.info("No coded bills found")
            return

        st.info(f"Found {len(coded_bills)} annotated bills")

        # Display
        display_cols = ['BillID', 'KnessetNum', 'BillName', 'CAPTopic_HE', 'Direction', 'AssignedDate']
        display_df = coded_bills[display_cols].copy()
        display_df.columns = ['ID', 'Knesset', 'Bill Name', 'Category', 'Direction', 'Annotation Date']
        display_df['Direction'] = display_df['Direction'].map({1: '+1', -1: '-1', 0: '0'})

        st.dataframe(display_df, use_container_width=True)

        # Edit section
        st.markdown("---")
        st.subheader("✏️ Edit Annotation")

        # Bill selection for editing
        edit_idx = st.selectbox(
            "Select bill to edit",
            options=range(len(coded_bills)),
            format_func=lambda i: f"{coded_bills.iloc[i]['BillID']} - {coded_bills.iloc[i]['BillName'][:60]}...",
            key="edit_bill_select"
        )

        if edit_idx is not None:
            selected_bill = coded_bills.iloc[edit_idx]
            bill_id = int(selected_bill['BillID'])

            # Show current annotation details
            st.markdown(f"**Current Annotation for Bill {bill_id}:**")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write(f"📁 Category: {selected_bill['CAPTopic_HE']}")
            with col2:
                dir_map = {1: '+1 (Strengthening)', -1: '-1 (Weakening)', 0: '0 (Other)'}
                st.write(f"↔️ Direction: {dir_map.get(selected_bill['Direction'], selected_bill['Direction'])}")
            with col3:
                st.write(f"📅 Date: {selected_bill['AssignedDate']}")

            # Get full annotation details
            current_annotation = self.service.get_annotation_by_bill_id(bill_id)

            # Render edit form
            if current_annotation:
                self._render_edit_form(bill_id, current_annotation)
            else:
                st.warning("Could not load annotation details for editing")

        # Export button
        st.markdown("---")
        if st.button("📥 Export to CSV"):
            export_path = Path("data/exports/cap_annotations_export.csv")
            export_path.parent.mkdir(parents=True, exist_ok=True)

            if self.service.export_annotations(export_path):
                st.success(f"File saved to: {export_path}")

                # Provide download
                with open(export_path, 'rb') as f:
                    st.download_button(
                        label="⬇️ Download File",
                        data=f,
                        file_name="cap_annotations.csv",
                        mime="text/csv"
                    )

    def _render_edit_form(self, bill_id: int, current_annotation: dict) -> bool:
        """
        Render the edit form for an existing annotation.

        Args:
            bill_id: The bill ID to edit
            current_annotation: Dictionary with current annotation values

        Returns:
            True if annotation updated successfully
        """
        # Get taxonomy
        taxonomy = self.service.get_taxonomy()
        if taxonomy.empty:
            st.error("Error loading taxonomy")
            return False

        with st.form(f"edit_annotation_form_{bill_id}"):
            # Major category selection
            major_categories = self.service.get_major_categories()
            major_options = {
                f"{cat['MajorCode']} - {cat['MajorTopic_HE']} ({cat['MajorTopic_EN']})": cat['MajorCode']
                for cat in major_categories
            }

            # Find current major category (CAPMinorCode is an integer like 100, 201, 301)
            current_minor = current_annotation.get('CAPMinorCode', 0)
            current_minor_str = str(current_minor)
            # Major code is the first digit (1=Government, 2=Civil, 3=Rights)
            current_major = int(current_minor_str[0]) if current_minor_str else 1

            # Find index of current major
            major_keys = list(major_options.keys())
            major_default_idx = 0
            for idx, key in enumerate(major_keys):
                if major_options[key] == current_major:
                    major_default_idx = idx
                    break

            selected_major_label = st.selectbox(
                "Major Category *",
                options=major_keys,
                index=major_default_idx,
                key=f"edit_major_{bill_id}"
            )
            selected_major = major_options.get(selected_major_label)

            # Minor category selection (filtered by major)
            minor_categories = self.service.get_minor_categories(selected_major)
            minor_options = {
                f"{cat['MinorCode']} - {cat['MinorTopic_HE']} ({cat['MinorTopic_EN']})": cat['MinorCode']
                for cat in minor_categories
            }

            # Find index of current minor
            minor_keys = list(minor_options.keys())
            minor_default_idx = 0
            for idx, key in enumerate(minor_keys):
                if minor_options[key] == current_minor:
                    minor_default_idx = idx
                    break

            selected_minor_label = st.selectbox(
                "Minor Category *",
                options=minor_keys,
                index=minor_default_idx,
                key=f"edit_minor_{bill_id}"
            )
            selected_minor = minor_options.get(selected_minor_label)

            # Direction selection
            current_direction = current_annotation.get('Direction', 0)
            direction_options = [1, -1, 0]
            direction_idx = direction_options.index(current_direction) if current_direction in direction_options else 2

            direction = st.radio(
                "Direction *",
                options=direction_options,
                index=direction_idx,
                format_func=lambda x: {
                    1: "+1 הרחבה/חיזוק (Strengthening)",
                    -1: "-1 צמצום/פגיעה (Weakening)",
                    0: "0 אחר (Other)"
                }[x],
                horizontal=True,
                key=f"edit_direction_{bill_id}"
            )

            # Confidence level
            current_confidence = current_annotation.get('Confidence', 'Medium')
            confidence_options = ["High", "Medium", "Low"]
            confidence_idx = confidence_options.index(current_confidence) if current_confidence in confidence_options else 1

            confidence = st.selectbox(
                "Confidence Level",
                options=confidence_options,
                index=confidence_idx,
                key=f"edit_confidence_{bill_id}"
            )

            # Notes
            current_notes = current_annotation.get('Notes', '') or ''
            notes = st.text_area(
                "Notes",
                value=current_notes,
                placeholder="Additional notes about the annotation...",
                key=f"edit_notes_{bill_id}"
            )

            col1, col2 = st.columns(2)
            with col1:
                submitted = st.form_submit_button("💾 Update Annotation", type="primary")
            with col2:
                delete = st.form_submit_button("🗑️ Delete Annotation", type="secondary")

            if submitted:
                researcher_name = st.session_state.get("cap_researcher_name", "Unknown")
                submission_date = current_annotation.get('SubmissionDate', '')

                success = self.service.save_annotation(
                    bill_id=bill_id,
                    cap_minor_code=selected_minor,
                    direction=direction,
                    assigned_by=researcher_name,
                    confidence=confidence,
                    notes=notes,
                    source=current_annotation.get('Source', 'Database'),
                    submission_date=submission_date
                )

                if success:
                    st.success("✅ Annotation updated successfully!")
                    self._clear_query_cache()
                    st.rerun()
                    return True
                else:
                    st.error("❌ Error updating annotation")
                    return False

            if delete:
                # Store in session state for confirmation
                st.session_state[f'confirm_delete_{bill_id}'] = True
                st.rerun()

        # Handle delete confirmation outside the form
        if st.session_state.get(f'confirm_delete_{bill_id}', False):
            st.warning(f"⚠️ Are you sure you want to delete the annotation for Bill {bill_id}?")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Yes, Delete", key=f"confirm_del_{bill_id}", type="primary"):
                    success = self.service.delete_annotation(bill_id)
                    if success:
                        st.success("🗑️ Annotation deleted successfully!")
                        self._clear_query_cache()
                        del st.session_state[f'confirm_delete_{bill_id}']
                        st.rerun()
                    else:
                        st.error("❌ Error deleting annotation")
            with col2:
                if st.button("Cancel", key=f"cancel_del_{bill_id}"):
                    del st.session_state[f'confirm_delete_{bill_id}']
                    st.rerun()

        return False

    def _render_api_fetch_section(self, researcher_name: str):
        """Render section for fetching bills from API."""
        st.subheader("🌐 Fetch Bills from API")

        st.info("Fetch bills directly from the Knesset API. Useful for recent bills not yet in the local database.")

        col1, col2 = st.columns(2)
        with col1:
            api_knesset = st.selectbox(
                "Knesset",
                options=[25, 24, 23, 22, 21, 20],
                index=0,
                key="api_knesset"
            )
        with col2:
            api_limit = st.selectbox(
                "Results to Fetch",
                options=[25, 50, 100, 200],
                index=1,
                key="api_limit"
            )

        if st.button("🔄 Fetch Bills", key="fetch_api_bills"):
            with st.spinner("Fetching from API..."):
                try:
                    api_service = get_cap_api_service(self.logger)
                    api_bills = api_service.fetch_recent_bills_sync(
                        knesset_num=api_knesset,
                        limit=api_limit
                    )

                    if api_bills.empty:
                        st.warning("No bills found")
                        return

                    # Filter to bills not in database and not coded
                    new_bills = self.service.get_bills_not_in_database(api_bills, limit=api_limit)

                    if new_bills.empty:
                        st.success("All bills are already in database or coded!")
                        return

                    st.session_state.api_fetched_bills = new_bills
                    st.success(f"Found {len(new_bills)} new bills")

                except Exception as e:
                    st.error(f"Fetch error: {e}")
                    self.logger.error(f"API fetch error: {e}", exc_info=True)

        # Display fetched bills
        if 'api_fetched_bills' in st.session_state and not st.session_state.api_fetched_bills.empty:
            api_bills = st.session_state.api_fetched_bills

            st.markdown("---")
            st.markdown(f"### Bills from API ({len(api_bills)} results)")

            # Select bill to code
            display_cols = ['BillID', 'KnessetNum', 'Name', 'SubTypeDesc']
            available_cols = [c for c in display_cols if c in api_bills.columns]

            selected_api_idx = st.selectbox(
                "Select bill to code",
                options=range(len(api_bills)),
                format_func=lambda i: f"{api_bills.iloc[i]['BillID']} - {str(api_bills.iloc[i].get('Name', 'Unknown'))[:80]}...",
                key="api_bill_select"
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

                # Link to Knesset website (opens in new tab)
                bill_url = selected_bill.get('BillURL',
                    f"https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid={selected_bill['BillID']}")
                st.markdown(f'🔗 <a href="{bill_url}" target="_blank" rel="noopener noreferrer">View on Knesset Website</a>', unsafe_allow_html=True)

                # Annotation form for API bill - get date from API data
                st.markdown("---")
                api_pub_date = str(selected_bill.get('PublicationDate', ''))[:10] if selected_bill.get('PublicationDate') else ''
                self._render_api_annotation_form(int(selected_bill['BillID']), researcher_name, api_pub_date)

    def _render_api_annotation_form(self, bill_id: int, researcher_name: str, submission_date: str = "") -> bool:
        """Render annotation form for API-fetched bill."""
        st.subheader("✏️ Annotation Form (from API)")

        taxonomy = self.service.get_taxonomy()
        if taxonomy.empty:
            st.error("Error loading taxonomy")
            return False

        with st.form("api_annotation_form"):
            # Major category
            major_categories = self.service.get_major_categories()
            major_options = {
                f"{cat['MajorCode']} - {cat['MajorTopic_HE']} ({cat['MajorTopic_EN']})": cat['MajorCode']
                for cat in major_categories
            }

            selected_major_label = st.selectbox(
                "Major Category *",
                options=list(major_options.keys()),
                key="api_major"
            )
            selected_major = major_options.get(selected_major_label)

            # Minor category
            minor_categories = self.service.get_minor_categories(selected_major)
            minor_options = {
                f"{cat['MinorCode']} - {cat['MinorTopic_HE']} ({cat['MinorTopic_EN']})": cat['MinorCode']
                for cat in minor_categories
            }

            selected_minor_label = st.selectbox(
                "Minor Category *",
                options=list(minor_options.keys()),
                key="api_minor"
            )
            selected_minor = minor_options.get(selected_minor_label)

            # Direction
            direction = st.radio(
                "Direction *",
                options=[1, -1, 0],
                format_func=lambda x: {
                    1: "+1 הרחבה/חיזוק (Strengthening)",
                    -1: "-1 צמצום/פגיעה (Weakening)",
                    0: "0 אחר (Other)"
                }[x],
                horizontal=True,
                key="api_direction"
            )

            # Submission date (auto-populated from API)
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
                key="api_confidence"
            )

            # Notes
            notes = st.text_area(
                "Notes",
                placeholder="Additional notes...",
                key="api_notes"
            )

            submitted = st.form_submit_button("💾 Save Annotation", type="primary")

            if submitted:
                # Save (submission_date is auto-populated from API)
                success = self.service.save_annotation(
                    bill_id=bill_id,
                    cap_minor_code=selected_minor,
                    direction=direction,
                    assigned_by=researcher_name,
                    confidence=confidence,
                    notes=notes,
                    source="API",  # Mark as API-sourced
                    submission_date=submission_date
                )

                if success:
                    st.success("✅ Annotation saved!")
                    self._clear_query_cache()
                    # Remove from fetched bills
                    if 'api_fetched_bills' in st.session_state:
                        st.session_state.api_fetched_bills = st.session_state.api_fetched_bills[
                            st.session_state.api_fetched_bills['BillID'] != bill_id
                        ]
                    return True
                else:
                    st.error("❌ Error saving")
                    return False

        return False

    def render_cap_annotation_section(self):
        """Main render method for the CAP annotation section."""
        st.header("🏛️ Democratic Bill Annotation")

        # Check if feature is enabled
        try:
            cap_enabled = st.secrets.get("cap_annotation", {}).get("enabled", False)
        except Exception:
            cap_enabled = False

        if not cap_enabled:
            st.warning("""
            **Annotation System Not Enabled**

            To enable, add the following to `.streamlit/secrets.toml`:
            ```toml
            [cap_annotation]
            enabled = true
            password = "your-password"
            researcher_name = "Dr. Your Name"
            ```
            """)
            return
        
        # Check authentication
        is_authenticated, researcher_name = self._check_authentication()
        
        if not is_authenticated:
            self._render_login_form()
            return
        
        # Show researcher info and logout
        col1, col2 = st.columns([3, 1])
        with col1:
            login_time = st.session_state.get("cap_login_time")
            if login_time:
                time_str = login_time.strftime("%H:%M")
                st.success(f"👤 Logged in as: {researcher_name} (since {time_str})")
            else:
                st.success(f"👤 Logged in as: {researcher_name}")
        with col2:
            if st.button("🚪 Logout"):
                st.session_state.cap_authenticated = False
                st.session_state.cap_researcher_name = ""
                st.session_state.cap_login_time = None
                st.rerun()

        # Initialize tables
        if "cap_tables_initialized" not in st.session_state:
            with st.spinner("Initializing annotation system..."):
                self.service.ensure_tables_exist()
                self.service.load_taxonomy_from_csv()
                st.session_state.cap_tables_initialized = True

        # Tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs([
            "📝 New Annotation",
            "🌐 Fetch from API",
            "📚 View Coded",
            "📊 Statistics"
        ])

        with tab1:
            # Bill queue and annotation form
            result = self._render_bill_queue()
            selected_bill_id, submission_date = result if result[0] else (None, None)

            if selected_bill_id:
                st.markdown("---")
                success = self._render_annotation_form(selected_bill_id, researcher_name, submission_date or "")
                if success:
                    # Clear and refresh
                    st.rerun()

        with tab2:
            self._render_api_fetch_section(researcher_name)

        with tab3:
            self._render_coded_bills_view()

        with tab4:
            self._render_stats_dashboard()


def render_cap_page(db_path: Path, logger_obj: Optional[logging.Logger] = None):
    """Convenience function to render the CAP annotation page."""
    renderer = CAPAnnotationPageRenderer(db_path, logger_obj)
    renderer.render_cap_annotation_section()

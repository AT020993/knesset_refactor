"""
CAP Coded Bills Renderer

Handles viewing and editing existing annotations.
"""

import logging
from pathlib import Path
from typing import Optional, Callable

import streamlit as st

from ui.services.cap_service import CAPAnnotationService


class CAPCodedBillsRenderer:
    """Renders coded bills view with edit capability."""

    def __init__(
        self,
        service: CAPAnnotationService,
        logger_obj: Optional[logging.Logger] = None,
        on_annotation_changed: Optional[Callable] = None,
    ):
        """
        Initialize renderer.

        Args:
            service: CAP annotation service
            logger_obj: Optional logger
            on_annotation_changed: Callback when annotation is changed/deleted
        """
        self.service = service
        self.logger = logger_obj or logging.getLogger(__name__)
        self._on_annotation_changed = on_annotation_changed

    def _notify_annotation_changed(self):
        """Notify that an annotation was changed."""
        if self._on_annotation_changed:
            self._on_annotation_changed()

    def _handle_confirm_delete(self, bill_id: int):
        """Callback for confirming annotation deletion."""
        success = self.service.delete_annotation(bill_id)
        if success:
            self._notify_annotation_changed()
        # Clear confirmation state
        if f"confirm_delete_{bill_id}" in st.session_state:
            del st.session_state[f"confirm_delete_{bill_id}"]
        # Store result for display after rerun
        st.session_state[f"delete_result_{bill_id}"] = success

    def _handle_cancel_delete(self, bill_id: int):
        """Callback for canceling annotation deletion."""
        if f"confirm_delete_{bill_id}" in st.session_state:
            del st.session_state[f"confirm_delete_{bill_id}"]

    def render_coded_bills_view(self):
        """Render view of already coded bills with edit capability."""
        st.subheader("📚 Coded Bills")

        # Filters
        knesset_filter, cap_filter = self._render_filters()

        coded_bills = self.service.get_coded_bills(
            knesset_num=knesset_filter, cap_code=cap_filter, limit=100
        )

        if coded_bills.empty:
            st.info("No coded bills found")
            return

        st.info(f"Found {len(coded_bills)} annotated bills")

        # Display
        self._render_bills_table(coded_bills)

        # Edit section
        st.markdown("---")
        st.subheader("✏️ Edit Annotation")

        self._render_edit_section(coded_bills)

        # Export button
        st.markdown("---")
        self._render_export_section()

    def _render_filters(self) -> tuple:
        """Render filter controls."""
        # Check if we need to reset filters (after a new annotation)
        if st.session_state.pop("cap_annotation_just_saved", False):
            # Reset filter keys to show all results (user will see their new annotation)
            if "coded_knesset_filter" in st.session_state:
                del st.session_state["coded_knesset_filter"]
            if "coded_cap_filter" in st.session_state:
                del st.session_state["coded_cap_filter"]
            st.info("✨ Filters reset to show your newly coded bill!")

        col1, col2 = st.columns(2)
        with col1:
            knesset_filter = st.selectbox(
                "Filter by Knesset",
                options=[None] + list(range(25, 0, -1)),
                format_func=lambda x: "All" if x is None else f"Knesset {x}",
                key="coded_knesset_filter",
            )
        with col2:
            taxonomy = self.service.get_taxonomy()
            cap_options = {None: "All"}
            for _, row in taxonomy.iterrows():
                cap_options[row["MinorCode"]] = (
                    f"{row['MinorCode']} - {row['MinorTopic_HE']}"
                )

            cap_filter = st.selectbox(
                "Filter by CAP Code",
                options=list(cap_options.keys()),
                format_func=lambda x: cap_options.get(x, str(x)),
                key="coded_cap_filter",
            )

        return knesset_filter, cap_filter

    def _render_bills_table(self, coded_bills):
        """Render the coded bills data table."""
        display_cols = [
            "BillID",
            "KnessetNum",
            "BillName",
            "CAPTopic_HE",
            "Direction",
            "AssignedDate",
        ]
        display_df = coded_bills[display_cols].copy()
        display_df.columns = [
            "ID",
            "Knesset",
            "Bill Name",
            "Category",
            "Direction",
            "Annotation Date",
        ]
        display_df["Direction"] = display_df["Direction"].map({1: "+1", -1: "-1", 0: "0"})

        st.dataframe(display_df, use_container_width=True)

    def _render_edit_section(self, coded_bills):
        """Render bill selection and edit form."""
        # Bill selection for editing
        edit_idx = st.selectbox(
            "Select bill to edit",
            options=range(len(coded_bills)),
            format_func=lambda i: f"{coded_bills.iloc[i]['BillID']} - {coded_bills.iloc[i]['BillName'][:60]}...",
            key="edit_bill_select",
        )

        if edit_idx is not None:
            selected_bill = coded_bills.iloc[edit_idx]
            bill_id = int(selected_bill["BillID"])

            # Show current annotation details
            st.markdown(f"**Current Annotation for Bill {bill_id}:**")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write(f"📁 Category: {selected_bill['CAPTopic_HE']}")
            with col2:
                dir_map = {
                    1: "+1 (Strengthening)",
                    -1: "-1 (Weakening)",
                    0: "0 (Other)",
                }
                st.write(
                    f"↔️ Direction: {dir_map.get(selected_bill['Direction'], selected_bill['Direction'])}"
                )
            with col3:
                st.write(f"📅 Date: {selected_bill['AssignedDate']}")

            # Get full annotation details
            current_annotation = self.service.get_annotation_by_bill_id(bill_id)

            # Render edit form
            if current_annotation:
                self._render_edit_form(bill_id, current_annotation)
            else:
                st.warning("Could not load annotation details for editing")

    def _render_edit_form(self, bill_id: int, current_annotation: dict) -> bool:
        """Render the edit form for an existing annotation."""
        # Get taxonomy
        taxonomy = self.service.get_taxonomy()
        if taxonomy.empty:
            st.error("Error loading taxonomy")
            return False

        with st.form(f"edit_annotation_form_{bill_id}"):
            # Major category selection
            major_categories = self.service.get_major_categories()
            major_options = {
                f"{cat['MajorCode']} - {cat['MajorTopic_HE']} ({cat['MajorTopic_EN']})": cat[
                    "MajorCode"
                ]
                for cat in major_categories
            }

            # Find current major category
            current_minor = current_annotation.get("CAPMinorCode", 0)
            current_minor_str = str(current_minor)
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
                key=f"edit_major_{bill_id}",
            )
            selected_major = major_options.get(selected_major_label)

            # Minor category selection
            minor_categories = self.service.get_minor_categories(selected_major)
            minor_options = {
                f"{cat['MinorCode']} - {cat['MinorTopic_HE']} ({cat['MinorTopic_EN']})": cat[
                    "MinorCode"
                ]
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
                key=f"edit_minor_{bill_id}",
            )
            selected_minor = minor_options.get(selected_minor_label)

            # Direction selection
            current_direction = current_annotation.get("Direction", 0)
            direction_options = [1, -1, 0]
            direction_idx = (
                direction_options.index(current_direction)
                if current_direction in direction_options
                else 2
            )

            direction = st.radio(
                "Direction *",
                options=direction_options,
                index=direction_idx,
                format_func=lambda x: {
                    1: "+1 הרחבה/חיזוק (Strengthening)",
                    -1: "-1 צמצום/פגיעה (Weakening)",
                    0: "0 אחר (Other)",
                }[x],
                horizontal=True,
                key=f"edit_direction_{bill_id}",
            )

            # Confidence level
            current_confidence = current_annotation.get("Confidence", "Medium")
            confidence_options = ["High", "Medium", "Low"]
            confidence_idx = (
                confidence_options.index(current_confidence)
                if current_confidence in confidence_options
                else 1
            )

            confidence = st.selectbox(
                "Confidence Level",
                options=confidence_options,
                index=confidence_idx,
                key=f"edit_confidence_{bill_id}",
            )

            # Notes
            current_notes = current_annotation.get("Notes", "") or ""
            notes = st.text_area(
                "Notes",
                value=current_notes,
                placeholder="Additional notes about the annotation...",
                key=f"edit_notes_{bill_id}",
            )

            col1, col2 = st.columns(2)
            with col1:
                submitted = st.form_submit_button(
                    "💾 Update Annotation", type="primary"
                )
            with col2:
                delete = st.form_submit_button("🗑️ Delete Annotation", type="secondary")

            if submitted:
                researcher_name = st.session_state.get("cap_researcher_name", "Unknown")
                submission_date = current_annotation.get("SubmissionDate", "")

                success = self.service.save_annotation(
                    bill_id=bill_id,
                    cap_minor_code=selected_minor,
                    direction=direction,
                    assigned_by=researcher_name,
                    confidence=confidence,
                    notes=notes,
                    source=current_annotation.get("Source", "Database"),
                    submission_date=submission_date,
                )

                if success:
                    st.success("✅ Annotation updated successfully!")
                    self._notify_annotation_changed()
                    return True
                else:
                    st.error("❌ Error updating annotation")
                    return False

            if delete:
                st.session_state[f"confirm_delete_{bill_id}"] = True

        # Handle delete result from callback
        delete_result = st.session_state.pop(f"delete_result_{bill_id}", None)
        if delete_result is True:
            st.success("🗑️ Annotation deleted successfully!")
        elif delete_result is False:
            st.error("❌ Error deleting annotation")

        # Handle delete confirmation outside the form
        if st.session_state.get(f"confirm_delete_{bill_id}", False):
            st.warning(
                f"⚠️ Are you sure you want to delete the annotation for Bill {bill_id}?"
            )
            col1, col2 = st.columns(2)
            with col1:
                st.button(
                    "Yes, Delete",
                    key=f"confirm_del_{bill_id}",
                    type="primary",
                    on_click=self._handle_confirm_delete,
                    args=(bill_id,),
                )
            with col2:
                st.button(
                    "Cancel",
                    key=f"cancel_del_{bill_id}",
                    on_click=self._handle_cancel_delete,
                    args=(bill_id,),
                )

        return False

    def _render_export_section(self):
        """Render export functionality."""
        if st.button("📥 Export to CSV"):
            export_path = Path("data/exports/cap_annotations_export.csv")
            export_path.parent.mkdir(parents=True, exist_ok=True)

            if self.service.export_annotations(export_path):
                st.success(f"File saved to: {export_path}")

                # Provide download
                with open(export_path, "rb") as f:
                    st.download_button(
                        label="⬇️ Download File",
                        data=f,
                        file_name="cap_annotations.csv",
                        mime="text/csv",
                    )

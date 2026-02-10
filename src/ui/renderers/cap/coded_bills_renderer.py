"""
CAP Coded Bills Renderer

Handles viewing and editing existing annotations.
Supports multi-annotator mode where multiple researchers can annotate the same bill.
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

    def _sync_to_cloud(self) -> bool:
        """Sync database to cloud storage after annotation change.

        Returns:
            True if sync succeeded or was skipped (not enabled),
            False if sync was attempted but failed.
        """
        try:
            from data.services.storage_sync_service import StorageSyncService

            sync_service = StorageSyncService(logger_obj=self.logger)
            if not sync_service.is_enabled():
                # Cloud storage not enabled - nothing to do
                return True

            if sync_service.gcs_manager is None:
                self.logger.warning("Cloud sync enabled but GCS manager is not initialized")
                return False

            # Upload just the database file
            from config.settings import Settings

            success = sync_service.gcs_manager.upload_file(
                Settings.DEFAULT_DB_PATH, "data/warehouse.duckdb"
            )
            if success:
                self.logger.info(
                    "Database synced to cloud storage after annotation change"
                )
                return True
            else:
                self.logger.warning("Failed to sync database to cloud storage")
                return False
        except Exception as e:
            # Don't fail the operation if cloud sync fails
            self.logger.warning(f"Cloud sync after annotation change failed: {e}")
            return False

    def _handle_confirm_delete(self, bill_id: int, researcher_id: int):
        """Callback for confirming annotation deletion."""
        # Delete only this researcher's annotation
        success = self.service.delete_annotation(bill_id, researcher_id)
        sync_success = True
        if success:
            self._notify_annotation_changed()
            sync_success = self._sync_to_cloud()
        # Clear confirmation state
        if f"confirm_delete_{bill_id}" in st.session_state:
            del st.session_state[f"confirm_delete_{bill_id}"]
        # Store result for display after rerun
        st.session_state[f"delete_result_{bill_id}"] = success
        st.session_state[f"delete_sync_result_{bill_id}"] = sync_success

    def _handle_cancel_delete(self, bill_id: int):
        """Callback for canceling annotation deletion."""
        if f"confirm_delete_{bill_id}" in st.session_state:
            del st.session_state[f"confirm_delete_{bill_id}"]

    def render_coded_bills_view(self, researcher_id: Optional[int] = None):
        """
        Render view of already coded bills with edit capability.

        Args:
            researcher_id: Filter to show only this researcher's annotations.
                          If None, shows all annotations (admin view).
        """
        st.subheader("📚 Coded Bills")

        # Filters
        knesset_filter, cap_filter, show_all = self._render_filters(researcher_id)

        # Determine researcher filter
        filter_researcher_id = None if show_all else researcher_id

        coded_bills = self.service.get_coded_bills(
            knesset_num=knesset_filter, cap_code=cap_filter, limit=100,
            researcher_id=filter_researcher_id
        )

        if coded_bills.empty:
            if filter_researcher_id:
                st.info("You haven't coded any bills yet")
            else:
                st.info("No coded bills found")
            return

        # Count info
        unique_bills = coded_bills["BillID"].nunique() if "BillID" in coded_bills.columns else len(coded_bills)
        if filter_researcher_id:
            st.info(f"Found {len(coded_bills)} of your annotations ({unique_bills} unique bills)")
        else:
            st.info(f"Found {len(coded_bills)} annotations ({unique_bills} unique bills)")

        # Display
        self._render_bills_table(coded_bills)

        # Edit section
        st.markdown("---")
        st.subheader("✏️ Edit Annotation")

        self._render_edit_section(coded_bills, researcher_id)

        # Export button
        st.markdown("---")
        self._render_export_section()

    def _render_filters(self, researcher_id: Optional[int] = None) -> tuple:
        """
        Render filter controls.

        Args:
            researcher_id: Current researcher's ID for filtering options

        Returns:
            Tuple of (knesset_filter, cap_filter, show_all_researchers)
        """
        # Check if we need to reset filters (after a new annotation)
        if st.session_state.pop("cap_annotation_just_saved", False):
            # Reset filter keys to show all results (user will see their new annotation)
            if "coded_knesset_filter" in st.session_state:
                del st.session_state["coded_knesset_filter"]
            if "coded_cap_filter" in st.session_state:
                del st.session_state["coded_cap_filter"]
            st.info("✨ Filters reset to show your newly coded bill!")

        col1, col2, col3 = st.columns([2, 2, 2])
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

        with col3:
            # Option to show all researchers' annotations
            show_all = st.checkbox(
                "Show all researchers",
                value=False,
                key="coded_show_all_researchers",
                help="Show annotations from all researchers, not just yours"
            )

        return knesset_filter, cap_filter, show_all

    def _render_bills_table(self, coded_bills):
        """Render the coded bills data table with annotation count."""
        # Build display columns (some may not exist in older schemas)
        display_cols = ["BillID", "KnessetNum", "BillName", "CAPTopic_HE"]
        col_names = ["ID", "Knesset", "Bill Name", "Category"]

        # Add researcher name if available
        if "AssignedBy" in coded_bills.columns:
            display_cols.append("AssignedBy")
            col_names.append("Researcher")

        # Add annotation count if available
        if "AnnotationCount" in coded_bills.columns:
            display_cols.append("AnnotationCount")
            col_names.append("👥 Count")

        display_cols.append("AssignedDate")
        col_names.append("Annotation Date")

        display_df = coded_bills[display_cols].copy()
        display_df.columns = col_names

        st.dataframe(display_df, use_container_width=True)

    def _render_edit_section(self, coded_bills, researcher_id: Optional[int] = None):
        """
        Render bill selection and edit form.

        Args:
            coded_bills: DataFrame of coded bills
            researcher_id: Current researcher's ID for edits
        """
        # Search by Bill ID
        search_bill_id = st.text_input(
            "🔍 Search by Bill ID",
            key="edit_search_bill_id",
            placeholder="Enter Bill ID to filter...",
        )

        # Filter coded_bills if search is provided
        filtered_bills = coded_bills
        if search_bill_id:
            search_bill_id = search_bill_id.strip()
            # Filter where BillID contains the search string
            filtered_bills = coded_bills[
                coded_bills["BillID"].astype(str).str.contains(search_bill_id, na=False)
            ]

            if filtered_bills.empty:
                st.warning(f"No annotations found for Bill ID containing: {search_bill_id}")
                return

        # Bill selection for editing
        edit_idx = st.selectbox(
            "Select annotation to edit",
            options=range(len(filtered_bills)),
            format_func=lambda i: self._format_edit_option(filtered_bills.iloc[i]),
            key="edit_bill_select",
        )

        if edit_idx is not None:
            selected_bill = filtered_bills.iloc[edit_idx]
            bill_id = int(selected_bill["BillID"])
            # Get researcher ID from the selected annotation (in case viewing all)
            # Prioritize the annotation's ResearcherID, then fall back to current user
            annotation_researcher_id_raw = selected_bill.get("ResearcherID")
            if annotation_researcher_id_raw is not None:
                annotation_researcher_id = int(annotation_researcher_id_raw)
            elif researcher_id is not None:
                annotation_researcher_id = int(researcher_id)
            else:
                st.error("❌ Unable to identify researcher. Please log in again.")
                self.logger.error("Both ResearcherID and researcher_id are None")
                return

            # Show current annotation details
            researcher_name = selected_bill.get("AssignedBy", "Unknown")
            st.markdown(f"**Current Annotation for Bill {bill_id}** (by {researcher_name}):")
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"📁 Category: {selected_bill['CAPTopic_HE']}")
            with col2:
                st.write(f"📅 Date: {selected_bill['AssignedDate']}")

            # Get full annotation details (for this researcher's annotation)
            current_annotation = self.service.get_annotation_by_bill_id(
                bill_id, annotation_researcher_id
            )

            # Render edit form
            if current_annotation:
                self._render_edit_form(bill_id, current_annotation, annotation_researcher_id)
            else:
                st.warning("Could not load annotation details for editing")

    def _format_edit_option(self, bill) -> str:
        """Format bill option for edit selection with researcher info."""
        bill_name = str(bill["BillName"])[:50] if bill.get("BillName") else "Unknown"
        if len(str(bill.get("BillName", ""))) > 50:
            bill_name += "..."

        researcher = bill.get("AssignedBy", "")
        ann_count = bill.get("AnnotationCount", 1)

        badge = f" 👥{ann_count}" if ann_count > 1 else ""
        researcher_badge = f" [{researcher}]" if researcher else ""

        return f"{bill['BillID']} - {bill_name}{researcher_badge}{badge}"

    def _render_edit_form(
        self, bill_id: int, current_annotation: dict, researcher_id: int
    ) -> bool:
        """
        Render the edit form for an existing annotation.

        Args:
            bill_id: The bill ID
            current_annotation: Current annotation data
            researcher_id: The researcher ID who owns this annotation
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
                if selected_minor is None:
                    st.error("❌ Please select a valid minor category")
                    return False
                submission_date = current_annotation.get("SubmissionDate", "")

                # Use researcher_id for the update
                success = self.service.save_annotation(
                    bill_id=bill_id,
                    cap_minor_code=selected_minor,
                    researcher_id=researcher_id,
                    confidence=confidence,
                    notes=notes,
                    source=current_annotation.get("Source", "Database"),
                    submission_date=submission_date,
                )

                if success:
                    st.success("✅ Annotation updated successfully!")
                    self._notify_annotation_changed()
                    sync_success = self._sync_to_cloud()
                    if not sync_success:
                        st.warning(
                            "⚠️ Annotation updated locally, but cloud sync failed. "
                            "Your change may not be visible to other researchers "
                            "until sync succeeds."
                        )
                    return True
                else:
                    st.error("❌ Error updating annotation")
                    return False

            if delete:
                st.session_state[f"confirm_delete_{bill_id}"] = True
                # Store researcher_id for the delete callback
                st.session_state[f"delete_researcher_{bill_id}"] = researcher_id

        # Handle delete result from callback
        delete_result = st.session_state.pop(f"delete_result_{bill_id}", None)
        delete_sync_result = st.session_state.pop(f"delete_sync_result_{bill_id}", None)
        if delete_result is True:
            st.success("🗑️ Annotation deleted successfully!")
            if delete_sync_result is False:
                st.warning(
                    "⚠️ Annotation deleted locally, but cloud sync failed. "
                    "Your change may not be visible to other researchers "
                    "until sync succeeds."
                )
        elif delete_result is False:
            st.error("❌ Error deleting annotation")

        # Handle delete confirmation outside the form
        if st.session_state.get(f"confirm_delete_{bill_id}", False):
            st.warning(
                f"⚠️ Are you sure you want to delete your annotation for Bill {bill_id}?"
            )
            col1, col2 = st.columns(2)
            with col1:
                delete_researcher_id = st.session_state.get(
                    f"delete_researcher_{bill_id}", researcher_id
                )
                st.button(
                    "Yes, Delete",
                    key=f"confirm_del_{bill_id}",
                    type="primary",
                    on_click=self._handle_confirm_delete,
                    args=(bill_id, delete_researcher_id),
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

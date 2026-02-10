"""
CAP Form Renderer

Handles all annotation form rendering: new annotations, edits, and API-sourced bills.
"""

import logging
from pathlib import Path
from typing import Optional, Callable, Tuple

import streamlit as st
import pandas as pd

from ui.services.cap_service import CAPAnnotationService
from ui.services.cap_api_service import get_cap_api_service
from ui.renderers.cap.bill_queue_renderer import CAPBillQueueRenderer
from ui.renderers.cap.pdf_viewer import CAPPDFViewer
from ui.renderers.cap.category_selector import CAPCategorySelector


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
        # Delegate rendering to specialized components
        self._bill_queue_renderer = CAPBillQueueRenderer(service, logger_obj)
        self._pdf_viewer = CAPPDFViewer(service, logger_obj)
        self._category_selector = CAPCategorySelector(service, logger_obj)

    def _notify_annotation_saved(self):
        """Notify that an annotation was saved."""
        # Set flag to reset coded bills filters so user sees their new annotation
        st.session_state["cap_annotation_just_saved"] = True
        # Clear category selections so next bill starts fresh
        self._category_selector.clear_session_state("db_")
        self._category_selector.clear_session_state("api_")
        if self._on_annotation_saved:
            self._on_annotation_saved()

    def _sync_to_cloud(self) -> bool:
        """Sync database to cloud storage after annotation save.

        Uses file locking to prevent race conditions when multiple researchers
        save annotations concurrently.

        Returns:
            True if sync succeeded or was skipped (not enabled or locked),
            False if sync was attempted but failed.
        """
        try:
            from data.services.storage_sync_service import StorageSyncService
            from config.settings import Settings
            import time

            sync_service = StorageSyncService(logger_obj=self.logger)
            if not sync_service.is_enabled():
                # Cloud storage not enabled - nothing to do
                return True
            if sync_service.gcs_manager is None:
                self.logger.warning("Cloud sync enabled but GCS manager is not initialized")
                return False

            # Use file locking to prevent concurrent syncs (race condition fix)
            lock_file = Settings.DEFAULT_DB_PATH.with_suffix('.lock')

            # Check if another sync is in progress
            if lock_file.exists():
                # Check if lock is stale (older than 60 seconds)
                try:
                    lock_age = time.time() - lock_file.stat().st_mtime
                    if lock_age < 60:
                        self.logger.warning("Another cloud sync in progress, skipping")
                        return True  # Don't fail the annotation, just skip sync
                    else:
                        # Stale lock, remove it
                        self.logger.info("Removing stale lock file")
                        lock_file.unlink(missing_ok=True)
                except Exception:
                    pass

            try:
                # Create lock file
                lock_file.touch()

                # Upload just the database file (fastest)
                success = sync_service.gcs_manager.upload_file(
                    Settings.DEFAULT_DB_PATH, "data/warehouse.duckdb"
                )
                if success:
                    self.logger.info("Database synced to cloud storage after annotation")
                    return True
                else:
                    self.logger.warning("Failed to sync database to cloud storage")
                    return False
            finally:
                # Always remove lock file
                lock_file.unlink(missing_ok=True)

        except Exception as e:
            # Don't fail the annotation save if cloud sync fails
            self.logger.warning(f"Cloud sync after annotation failed: {e}")
            return False

    def render_bill_queue(self, researcher_id: int) -> Tuple[Optional[int], str]:
        """
        Render the bills queue with search, recent annotations, and status badges.

        Delegates to CAPBillQueueRenderer for the main queue UI, then adds
        PDF document viewing on top.

        Args:
            researcher_id: The researcher's database ID (int from cap_user_id).
                IMPORTANT: Must be int, NOT cap_researcher_name string!

        Returns:
            Tuple of (bill_id, submission_date) or (None, "")
        """
        # Delegate to bill queue renderer for main queue UI
        bill_id, pub_date = self._bill_queue_renderer.render_bill_queue(researcher_id)

        # Add PDF viewer if a bill is selected
        if bill_id is not None:
            self._pdf_viewer.render_bill_documents(bill_id)

        return bill_id, pub_date

    def render_annotation_form(
        self, bill_id: int, researcher_id: int, submission_date: str = ""
    ) -> bool:
        """
        Render the annotation form.

        Args:
            bill_id: The bill ID to annotate
            researcher_id: The researcher's database ID (int from cap_user_id).
                IMPORTANT: Must be int, NOT cap_researcher_name string!
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

        # Category selections OUTSIDE the form for proper filtering behavior
        # When major category changes, minor options update immediately
        selected_major, selected_minor = self._category_selector.render_selectors(prefix="db_")

        # Show description for selected category (also outside form)
        if selected_minor:
            self._category_selector.show_category_description(selected_minor)

        with st.form("annotation_form"):
            # Submission date info
            if submission_date:
                st.info(f"📅 **Submission Date:** {submission_date}")

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
                    researcher_id,
                    confidence,
                    notes,
                    "Database",
                    submission_date,
                )

            if skip:
                st.info("Skipping this bill")

        return False

    def _validate_category_selection(
        self,
        major_code: Optional[int],
        minor_code: Optional[int],
    ) -> Tuple[bool, Optional[str]]:
        """Validate that selected categories are valid and compatible.

        Before saving an annotation, this method ensures:
        1. A major category is selected
        2. A minor category is selected
        3. The minor category actually belongs to the selected major category

        This prevents FK constraint errors and data integrity issues.

        Args:
            major_code: Selected major category code
            minor_code: Selected minor category code

        Returns:
            Tuple of (is_valid, error_message). If valid, error_message is None.
        """
        if major_code is None:
            return False, "Please select a Major Category"

        if minor_code is None:
            return False, "Please select a Minor Category"

        # Get valid minor codes for this major category
        valid_minors = self.service.get_minor_categories(major_code)
        valid_minor_codes = {m["MinorCode"] for m in valid_minors}

        if minor_code not in valid_minor_codes:
            return (
                False,
                f"Selected minor category does not belong to major category {major_code}",
            )

        return True, None

    def _handle_form_submission(
        self,
        bill_id: int,
        selected_major: Optional[int],
        selected_minor: Optional[int],
        researcher_id: int,
        confidence: str,
        notes: str,
        source: str,
        submission_date: str,
    ) -> bool:
        """Handle form submission logic."""
        # Validate category selection (includes None checks and parent/child match)
        is_valid, error = self._validate_category_selection(selected_major, selected_minor)
        if not is_valid:
            st.error(f"❌ {error}")
            return False
        assert selected_minor is not None

        # Save annotation using researcher_id (not display name)
        success = self.service.save_annotation(
            bill_id=bill_id,
            cap_minor_code=selected_minor,
            researcher_id=researcher_id,
            confidence=confidence,
            notes=notes,
            source=source,
            submission_date=submission_date,
        )

        if success:
            st.success("✅ Annotation saved successfully!")
            self._notify_annotation_saved()
            # Sync database to cloud storage so other researchers can see the annotation
            sync_success = self._sync_to_cloud()
            if not sync_success:
                st.warning(
                    "⚠️ Annotation saved locally, but cloud sync failed. "
                    "Your work is safe but may not be visible to other researchers "
                    "until sync succeeds."
                )
            return True
        else:
            st.error("❌ Error saving annotation")
            return False

    def render_api_annotation_form(
        self, bill_id: int, researcher_id: int, submission_date: str = ""
    ) -> bool:
        """
        Render annotation form for API-fetched bill.

        Args:
            bill_id: The bill ID to annotate
            researcher_id: The researcher's database ID (int from cap_user_id).
                IMPORTANT: Must be int, NOT cap_researcher_name string!
            submission_date: Pre-filled submission date from API
        """
        st.subheader("✏️ Annotation Form (from API)")

        taxonomy = self.service.get_taxonomy()
        if taxonomy.empty:
            st.error("Error loading taxonomy")
            return False

        # Category selections OUTSIDE the form for proper filtering behavior
        selected_major, selected_minor = self._category_selector.render_selectors(prefix="api_")

        # Show description for selected category (also outside form)
        if selected_minor:
            self._category_selector.show_category_description(selected_minor)

        with st.form("api_annotation_form"):
            # Submission date info
            if submission_date:
                st.info(f"📅 **Submission Date:** {submission_date}")

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
                    researcher_id,
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

    def render_api_fetch_section(self, researcher_id: int) -> None:
        """
        Render section for fetching bills from API.

        Args:
            researcher_id: The researcher's database ID (int from cap_user_id).
        """
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
            self._fetch_api_bills(api_knesset, api_limit, researcher_id)

        # Display fetched bills
        if (
            "api_fetched_bills" in st.session_state
            and not st.session_state.api_fetched_bills.empty
        ):
            self._render_api_bills_list(researcher_id)

    def _fetch_api_bills(self, knesset_num: int, limit: int, researcher_id: int) -> None:
        """
        Fetch bills from API.

        Args:
            knesset_num: The Knesset number to fetch bills from
            limit: Maximum number of bills to fetch
            researcher_id: The researcher's database ID (int from cap_user_id).
        """
        with st.spinner("Fetching from API..."):
            try:
                api_service = get_cap_api_service(self.logger)
                api_bills = api_service.fetch_recent_bills_sync(
                    knesset_num=knesset_num, limit=limit
                )

                if api_bills.empty:
                    st.session_state.api_fetched_bills = pd.DataFrame()  # Clear stale data
                    st.warning("No bills found")
                    return

                # Filter to bills not in database and not coded by this researcher
                new_bills = self.service.get_bills_not_in_database(
                    api_bills, limit=limit, researcher_id=researcher_id
                )

                if new_bills.empty:
                    st.session_state.api_fetched_bills = pd.DataFrame()  # Clear stale data
                    st.success("All bills are already in database or coded by you!")
                    return

                st.session_state.api_fetched_bills = new_bills
                st.success(f"Found {len(new_bills)} new bills for you to code")

            except Exception as e:
                st.session_state.api_fetched_bills = pd.DataFrame()  # Clear stale data
                st.error(f"Fetch error: {e}")
                self.logger.error(f"API fetch error: {e}", exc_info=True)

    def _render_api_bills_list(self, researcher_id: int) -> None:
        """
        Render list of API-fetched bills.

        Args:
            researcher_id: The researcher's database ID (int from cap_user_id).
        """
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
                int(selected_bill["BillID"]), researcher_id, api_pub_date
            )

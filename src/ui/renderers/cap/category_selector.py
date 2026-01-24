"""
CAP Category Selector Component

Handles hierarchical category selection for bill annotation:
- Major category selection with filtering
- Minor category selection (filtered by major)
- Session state management for persistence
- Popover tooltips for category descriptions
"""

import logging
from typing import Optional, Tuple, List, Dict, Any

import streamlit as st

from ui.services.cap_service import CAPAnnotationService


class CAPCategorySelector:
    """Renders hierarchical category selectors for CAP annotation."""

    def __init__(
        self,
        service: CAPAnnotationService,
        logger_obj: Optional[logging.Logger] = None,
    ):
        """
        Initialize category selector.

        Args:
            service: CAP annotation service (for taxonomy lookups)
            logger_obj: Optional logger
        """
        self.service = service
        self.logger = logger_obj or logging.getLogger(__name__)

    def init_session_state(self, prefix: str = ""):
        """
        Initialize session state for category selectors.

        Creates session state keys for tracking selected major/minor categories.
        Called automatically by render methods.

        Args:
            prefix: Prefix for session state keys (e.g., "db_" or "api_")
        """
        major_key = f"{prefix}cap_selected_major"
        minor_key = f"{prefix}cap_selected_minor"
        minor_label_key = f"{prefix}cap_selected_minor_label"

        if major_key not in st.session_state:
            st.session_state[major_key] = None
        if minor_key not in st.session_state:
            st.session_state[minor_key] = None
        if minor_label_key not in st.session_state:
            st.session_state[minor_label_key] = None

    def clear_session_state(self, prefix: str = ""):
        """
        Clear category session state for a given prefix.

        Called after saving an annotation to reset selections for the next bill.

        Args:
            prefix: Prefix for session state keys (e.g., "db_" or "api_")
        """
        keys_to_clear = [
            f"{prefix}cap_selected_major",
            f"{prefix}cap_selected_minor",
            f"{prefix}cap_selected_minor_label",
        ]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]

    def _on_major_category_change(self, prefix: str = ""):
        """
        Callback when major category changes - clears minor selection.

        This ensures the minor category is reset when the major category changes,
        preventing invalid combinations.
        """
        minor_key = f"{prefix}cap_selected_minor"
        minor_label_key = f"{prefix}cap_selected_minor_label"
        st.session_state[minor_key] = None
        st.session_state[minor_label_key] = None

    def render_selectors(self, prefix: str = "") -> Tuple[Optional[int], Optional[int]]:
        """
        Render major and minor category selectors OUTSIDE the form.

        This allows proper filtering of minor categories when major changes,
        using on_change callbacks that trigger immediate reruns.

        Includes popover tooltips (ℹ️) for category descriptions.

        Args:
            prefix: Prefix for session state keys (e.g., "db_" or "api_")

        Returns:
            Tuple of (selected_major_code, selected_minor_code)
        """
        self.init_session_state(prefix)

        major_key = f"{prefix}cap_selected_major"
        minor_key = f"{prefix}cap_selected_minor"
        minor_label_key = f"{prefix}cap_selected_minor_label"

        # Major category selection
        major_categories = self.service.get_major_categories()
        major_options = {
            f"{cat['MajorCode']} - {cat['MajorTopic_HE']} ({cat['MajorTopic_EN']})": cat[
                "MajorCode"
            ]
            for cat in major_categories
        }
        major_labels = list(major_options.keys())

        # Find current index based on session state
        current_major = st.session_state[major_key]
        current_major_idx = None
        if current_major is not None:
            for i, label in enumerate(major_labels):
                if major_options[label] == current_major:
                    current_major_idx = i
                    break

        # Layout: selectbox + info icon
        col_major, col_major_info = st.columns([10, 1])

        with col_major:
            selected_major_label = st.selectbox(
                "Major Category *",
                options=major_labels,
                index=current_major_idx,
                placeholder="Select a major category...",
                key=f"{prefix}major_selector",
                on_change=self._on_major_category_change,
                kwargs={"prefix": prefix},
            )

        with col_major_info:
            st.markdown("<div style='height: 28px'></div>", unsafe_allow_html=True)
            with st.popover("ℹ️", help="View all major categories"):
                st.markdown("### 📚 Major Categories Overview")
                st.markdown("---")
                for cat in major_categories:
                    st.markdown(
                        f"**{cat['MajorCode']}. {cat['MajorTopic_HE']}** "
                        f"({cat['MajorTopic_EN']})"
                    )

        # Update session state with selected major
        selected_major = major_options.get(selected_major_label) if selected_major_label else None
        st.session_state[major_key] = selected_major

        # Minor category selection (filtered by selected major)
        minor_categories = self.service.get_minor_categories(selected_major)
        minor_options = {
            f"{cat['MinorCode']} - {cat['MinorTopic_HE']} ({cat['MinorTopic_EN']})": cat[
                "MinorCode"
            ]
            for cat in minor_categories
        }
        minor_labels = list(minor_options.keys())

        # Find current index based on session state
        current_minor_label = st.session_state[minor_label_key]
        current_minor_idx = None
        if current_minor_label is not None and current_minor_label in minor_labels:
            current_minor_idx = minor_labels.index(current_minor_label)

        # Layout: selectbox + info icon
        col_minor, col_minor_info = st.columns([10, 1])

        with col_minor:
            selected_minor_label = st.selectbox(
                "Minor Category *",
                options=minor_labels,
                index=current_minor_idx,
                placeholder="Select a minor category...",
                key=f"{prefix}minor_selector",
            )

        with col_minor_info:
            st.markdown("<div style='height: 28px'></div>", unsafe_allow_html=True)
            self._render_minor_category_popover(selected_major, major_categories, minor_categories)

        # Update session state with selected minor
        selected_minor = minor_options.get(selected_minor_label) if selected_minor_label else None
        st.session_state[minor_key] = selected_minor
        st.session_state[minor_label_key] = selected_minor_label

        return selected_major, selected_minor

    def _render_minor_category_popover(
        self,
        selected_major: Optional[int],
        major_categories: List[Dict[str, Any]],
        minor_categories: List[Dict[str, Any]],
    ):
        """Render the popover with minor category descriptions."""
        with st.popover("ℹ️", help="View category descriptions"):
            if selected_major:
                # Get the major category info for the header
                major_info = next(
                    (c for c in major_categories if c["MajorCode"] == selected_major),
                    None
                )
                if major_info:
                    st.markdown(
                        f"### 📖 {major_info['MajorTopic_HE']} "
                        f"({major_info['MajorTopic_EN']})"
                    )
                st.markdown("---")

                # Show only minor categories for the selected major
                for cat in minor_categories:
                    # Skip general categories (codes ending in 00)
                    if cat["MinorCode"] % 100 == 0:
                        continue

                    # Show minor category with description
                    with st.expander(
                        f"📝 {cat['MinorCode']} - {cat['MinorTopic_HE']}", expanded=False
                    ):
                        if cat.get("Description_HE"):
                            st.markdown(f"**תיאור:** {cat['Description_HE']}")
                        if cat.get("Examples_HE"):
                            st.markdown(f"**דוגמאות:** {cat['Examples_HE']}")
                        if not cat.get("Description_HE") and not cat.get("Examples_HE"):
                            st.caption("No description available")
            else:
                st.info("Select a major category first to see descriptions")

    def show_category_description(self, minor_code: int):
        """
        Show description for selected minor category.

        Displays the Hebrew description and examples in info/caption boxes.

        Args:
            minor_code: The selected minor category code
        """
        minor_categories = self.service.get_minor_categories(None)
        cat_info = next(
            (c for c in minor_categories if c["MinorCode"] == minor_code), None
        )
        if cat_info and cat_info.get("Description_HE"):
            st.info(f"**Description:** {cat_info['Description_HE']}")
        if cat_info and cat_info.get("Examples_HE"):
            st.caption(f"**Examples:** {cat_info['Examples_HE']}")

    def render_selectors_deprecated(self, prefix: str = "") -> Tuple[Optional[int], Optional[int]]:
        """
        Render major and minor category selectors.

        DEPRECATED: Use render_selectors() instead for proper filtering behavior.

        This method is kept for backward compatibility but may not filter
        correctly inside st.form() due to Streamlit's form behavior.
        """
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

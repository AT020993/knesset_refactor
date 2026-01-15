"""
CAP Statistics Dashboard Renderer

Renders annotation statistics and progress dashboard.
"""

import streamlit as st
import pandas as pd

from ui.services.cap_service import CAPAnnotationService


class CAPStatsRenderer:
    """Renders CAP annotation statistics dashboard."""

    def __init__(self, service: CAPAnnotationService):
        """Initialize with CAP service."""
        self.service = service

    def render_stats_dashboard(self):
        """Render the annotation statistics dashboard."""
        stats = self.service.get_annotation_stats()

        if not stats:
            st.warning("No annotation data found")
            return

        self._render_summary_metrics(stats)
        self._render_direction_chart(stats)
        self._render_category_chart(stats)

    def _render_summary_metrics(self, stats: dict):
        """Render summary metric cards."""
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Bills Coded", stats.get("total_coded", 0))

        with col2:
            st.metric("Total Bills", stats.get("total_bills", 0))

        with col3:
            total = stats.get("total_bills", 1)
            coded = stats.get("total_coded", 0)
            pct = (coded / total * 100) if total > 0 else 0
            st.metric("Coding Progress", f"{pct:.1f}%")

    def _render_direction_chart(self, stats: dict):
        """Render direction distribution chart."""
        if stats.get("by_direction"):
            st.subheader("By Direction")
            direction_data = pd.DataFrame(stats["by_direction"])
            if not direction_data.empty:
                direction_data["label"] = direction_data["Direction"].map(
                    {
                        1: "הרחבה/חיזוק (+1)",
                        -1: "צמצום/פגיעה (-1)",
                        0: "אחר (0)",
                    }
                )
                st.bar_chart(direction_data.set_index("label")["count"])

    def _render_category_chart(self, stats: dict):
        """Render category distribution chart."""
        if stats.get("by_major_category"):
            st.subheader("By Major Category")
            cat_data = pd.DataFrame(stats["by_major_category"])
            if not cat_data.empty:
                st.bar_chart(cat_data.set_index("MajorTopic_HE")["count"])

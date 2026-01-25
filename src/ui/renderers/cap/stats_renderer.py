"""
CAP Statistics Dashboard Renderer

Renders annotation statistics and progress dashboard.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from ui.services.cap_service import CAPAnnotationService
from config.charts import ChartConfig


class CAPStatsRenderer:
    """Renders CAP annotation statistics dashboard."""

    def __init__(self, service: CAPAnnotationService):
        """Initialize with CAP service."""
        self.service = service

    @staticmethod
    def _format_percentage(value: float) -> str:
        """
        Format a decimal value as a percentage string.

        Args:
            value: Decimal value (e.g., 0.5 for 50%)

        Returns:
            Formatted percentage string (e.g., "50.0%")
        """
        return f"{value * 100:.1f}%"

    def _get_summary_metrics(self, stats: dict) -> dict:
        """
        Extract summary metrics from annotation stats.

        Args:
            stats: Dictionary from service.get_annotation_stats()

        Returns:
            Dictionary with:
            - total_coded: Number of bills coded
            - total_bills: Total number of bills
            - progress_pct: Coding progress as decimal (0.0-1.0)
            - progress_str: Coding progress as formatted percentage string
        """
        total_bills = stats.get("total_bills", 0)
        total_coded = stats.get("total_coded", 0)

        # Avoid division by zero
        if total_bills > 0:
            progress_pct = total_coded / total_bills
        else:
            progress_pct = 0.0

        return {
            "total_coded": total_coded,
            "total_bills": total_bills,
            "progress_pct": progress_pct,
            "progress_str": self._format_percentage(progress_pct),
        }

    def render_stats_dashboard(self):
        """Render the annotation statistics dashboard."""
        stats = self.service.get_annotation_stats()

        if not stats:
            st.warning("No annotation data found")
            return

        self._render_summary_metrics(stats)

        # Render charts side by side: Major Category (left) | Direction (right)
        # Using spacer columns to make charts narrower and centered
        spacer_left, col_left, col_right, spacer_right = st.columns([0.5, 2, 2, 0.5])

        with col_left:
            self._render_category_chart(stats)

        with col_right:
            self._render_direction_chart(stats)

        # Coverage breakdown by Knesset
        st.markdown("---")
        self._render_coverage_breakdown()

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
        """Render direction distribution chart with semantic colors."""
        if stats.get("by_direction"):
            st.subheader("By Direction")
            direction_data = pd.DataFrame(stats["by_direction"])
            if not direction_data.empty:
                # Map direction values to labels and semantic colors
                direction_labels = {
                    1: "הרחבה/חיזוק (+1)",
                    -1: "צמצום/פגיעה (-1)",
                    0: "אחר (0)",
                }
                # Semantic colors: green=strengthens, red=weakens, gray=neutral
                direction_colors = {
                    1: "#00CC96",   # Green - strengthens democracy
                    -1: "#EF553B",  # Red - weakens democracy
                    0: "#7f7f7f",   # Gray - neutral/other
                }

                direction_data["label"] = direction_data["Direction"].map(direction_labels)
                direction_data["color"] = direction_data["Direction"].map(direction_colors)

                # Sort by direction value for consistent order: +1, 0, -1
                direction_data = direction_data.sort_values("Direction", ascending=False)

                fig = go.Figure(go.Bar(
                    x=direction_data["count"],
                    y=direction_data["label"],
                    orientation="h",
                    marker_color=direction_data["color"],
                    text=direction_data["count"],
                    textposition="auto",
                ))

                fig.update_layout(
                    xaxis_title="Count",
                    yaxis_title="",
                    showlegend=False,
                    height=250,
                    margin=dict(l=20, r=20, t=20, b=40),
                )

                st.plotly_chart(fig, use_container_width=True)

    def _render_category_chart(self, stats: dict):
        """Render category distribution chart with distinct colors."""
        if stats.get("by_major_category"):
            st.subheader("By Major Category")
            cat_data = pd.DataFrame(stats["by_major_category"])
            if not cat_data.empty:
                # Sort by count descending for better readability
                cat_data = cat_data.sort_values("count", ascending=True)

                # Assign distinct colors from the Knesset color sequence
                color_sequence = ChartConfig.KNESSET_COLOR_SEQUENCE
                num_categories = len(cat_data)
                colors = [color_sequence[i % len(color_sequence)] for i in range(num_categories)]

                fig = go.Figure(go.Bar(
                    x=cat_data["count"],
                    y=cat_data["MajorTopic_HE"],
                    orientation="h",
                    marker_color=colors,
                    text=cat_data["count"],
                    textposition="auto",
                ))

                fig.update_layout(
                    xaxis_title="Count",
                    yaxis_title="",
                    showlegend=False,
                    height=max(250, num_categories * 35),  # Dynamic height based on categories
                    margin=dict(l=20, r=20, t=20, b=40),
                )

                st.plotly_chart(fig, use_container_width=True)

    def _render_coverage_breakdown(self):
        """Render per-Knesset coverage with progress bars."""
        coverage = self.service.get_coverage_stats()
        if not coverage or not coverage.get("by_knesset"):
            return

        st.subheader("📊 Coverage by Knesset")

        for row in coverage["by_knesset"]:
            knesset = row["KnessetNum"]
            total = row["total_bills"]
            coded = row["coded_bills"]
            pct = row["coverage_pct"] or 0

            col1, col2, col3 = st.columns([1, 4, 1])
            with col1:
                st.markdown(f"**K{knesset}**")
            with col2:
                # Streamlit progress bar (0.0 to 1.0)
                st.progress(pct / 100.0, text=f"{coded}/{total} ({pct:.1f}%)")
            with col3:
                if st.button(f"View", key=f"coverage_k{knesset}"):
                    # Set filter to this Knesset and navigate to annotation tab
                    st.session_state["cap_filter_knesset"] = knesset
                    st.session_state["cap_active_tab"] = "📝 New Annotation"

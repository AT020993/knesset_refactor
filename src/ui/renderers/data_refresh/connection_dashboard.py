"""
Connection dashboard component for Streamlit UI.

This module provides a Streamlit dashboard for visualizing database connection
health and monitoring status.
"""

import streamlit as st
from typing import Dict, Any


def render_connection_dashboard() -> None:
    """
    Render a Streamlit dashboard component for monitoring database connections.

    This dashboard shows:
    - Connection health status (healthy/attention/warning/critical)
    - Active connection count
    - Connections grouped by database
    - Long-running connections that may indicate leaks
    """
    # Import here to avoid circular imports
    from backend.connection_diagnostics import monitor_connection_health
    from backend.connection_manager import log_connection_leaks

    st.subheader("🔌 Database Connection Monitor")

    health_info = monitor_connection_health()

    # Health status indicator
    status_colors = {
        "healthy": "🟢",
        "attention": "🟡",
        "warning": "🟠",
        "critical": "🔴",
    }

    status_icon = status_colors.get(health_info["health_status"], "⚪")
    st.write(f"**Status:** {status_icon} {health_info['health_status'].title()}")

    # Metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Active Connections", health_info["total_active"])

    with col2:
        st.metric("Databases Connected", len(health_info["connections_by_db"]))

    with col3:
        st.metric("Long-Running", len(health_info["long_running_connections"]))

    # Details in expandable sections
    if health_info["total_active"] > 0:
        with st.expander("📊 Connection Details"):
            _render_connection_details(health_info)

    # Actions
    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔄 Refresh Stats"):
            st.rerun()

    with col2:
        if st.button("📋 Log Connection Details"):
            log_connection_leaks()
            st.success("Connection details logged to application logs")


def _render_connection_details(health_info: Dict[str, Any]) -> None:
    """Render connection detail tables in expandable section."""
    # Connections by database
    if health_info["connections_by_db"]:
        st.write("**Connections by Database:**")
        for db_path, count in health_info["connections_by_db"].items():
            st.write(f"- `{db_path}`: {count} connection(s)")

    # Connections by thread
    if health_info["connections_by_thread"]:
        st.write("**Connections by Thread:**")
        for thread_id, count in health_info["connections_by_thread"].items():
            st.write(f"- Thread `{thread_id}`: {count} connection(s)")

    # Long-running connections
    if health_info["long_running_connections"]:
        st.warning("**Long-Running Connections (>5 min):**")
        for conn in health_info["long_running_connections"]:
            age_min = conn["age_seconds"] / 60
            st.write(
                f"- Connection `{conn['conn_id']}` to `{conn['db_path']}`: {age_min:.1f} minutes"
            )

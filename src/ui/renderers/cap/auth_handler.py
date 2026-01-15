"""
CAP Authentication Handler

Handles authentication logic for the CAP annotation system.
"""

from datetime import datetime
from typing import Tuple

import streamlit as st


class CAPAuthHandler:
    """Handles CAP annotation authentication."""

    @staticmethod
    def check_authentication() -> Tuple[bool, str]:
        """
        Check if the user is authenticated for CAP annotation.

        Returns:
            Tuple of (is_authenticated, researcher_name)
        """
        try:
            # Check if CAP annotation is enabled
            if not st.secrets["cap_annotation"]["enabled"]:
                return False, ""

            # Check session state for authentication
            if st.session_state.get("cap_authenticated", False):
                return True, st.session_state.get("cap_researcher_name", "Unknown")

            return False, ""

        except (KeyError, FileNotFoundError, AttributeError):
            # Secrets not configured
            return False, ""

    @staticmethod
    def render_login_form() -> bool:
        """
        Render the login form.

        Returns:
            True if login successful, False otherwise
        """
        st.subheader("🔐 Login")

        with st.form("cap_login_form"):
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login")

            if submitted:
                try:
                    correct_password = st.secrets["cap_annotation"]["password"]
                    researcher_name = st.secrets["cap_annotation"].get(
                        "researcher_name", "Researcher"
                    )

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

    @staticmethod
    def render_user_info(researcher_name: str):
        """Render user info and logout button."""
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

    @staticmethod
    def is_feature_enabled() -> bool:
        """Check if CAP annotation feature is enabled."""
        try:
            return st.secrets["cap_annotation"]["enabled"]
        except (KeyError, FileNotFoundError, AttributeError):
            return False

    @staticmethod
    def render_disabled_message():
        """Render message when feature is disabled."""
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

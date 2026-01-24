"""
CAP Authentication Handler

Handles multi-user authentication logic for the CAP annotation system.
Supports role-based access control with 'admin' and 'researcher' roles.
"""

from datetime import datetime
from pathlib import Path
from typing import Tuple, Optional
import logging

import streamlit as st

from ui.services.cap.user_service import CAPUserService, get_user_service


def _get_cap_secrets() -> dict:
    """
    Get CAP annotation secrets from Streamlit secrets.

    Expected format in secrets.toml:
        [cap_annotation]
        enabled = true
        bootstrap_admin_username = "admin"
        bootstrap_admin_display_name = "Administrator"
        bootstrap_admin_password = "your-password"

    Returns:
        Dictionary with CAP secrets, or empty dict if not found
    """
    try:
        # Debug: Show all available secret keys (not values!)
        all_keys = list(st.secrets.keys()) if hasattr(st.secrets, 'keys') else []
        logging.getLogger(__name__).info(f"Available secret sections: {all_keys}")

        # Get cap_annotation section
        cap_secrets = st.secrets.get("cap_annotation", {})

        if cap_secrets:
            # Debug: Show which keys are present (not values!)
            cap_keys = list(cap_secrets.keys()) if hasattr(cap_secrets, 'keys') else []
            logging.getLogger(__name__).info(f"CAP secrets keys found: {cap_keys}")
            return dict(cap_secrets)

        logging.getLogger(__name__).warning("cap_annotation section not found in secrets")
        return {}
    except Exception as e:
        logging.getLogger(__name__).error(f"Error reading CAP secrets: {e}")
        return {}


class CAPAuthHandler:
    """Handles CAP annotation authentication with multi-user support."""

    def __init__(self, db_path: Optional[Path] = None, logger_obj: Optional[logging.Logger] = None):
        """
        Initialize the auth handler.

        Args:
            db_path: Path to database (uses session state default if not provided)
            logger_obj: Optional logger instance
        """
        self._db_path = db_path
        self._logger = logger_obj or logging.getLogger(__name__)
        self._user_service: Optional[CAPUserService] = None

    @property
    def user_service(self) -> Optional[CAPUserService]:
        """Get or create the user service."""
        if self._user_service is None and self._db_path is not None:
            self._user_service = get_user_service(self._db_path, self._logger)
        return self._user_service

    @staticmethod
    def check_authentication() -> Tuple[bool, str]:
        """
        Check if the user is authenticated for CAP annotation.

        Returns:
            Tuple of (is_authenticated, researcher_name)
        """
        try:
            # Check if CAP annotation is enabled
            if not _get_cap_secrets().get("enabled", False):
                return False, ""

            # Check session state for authentication
            if st.session_state.get("cap_authenticated", False):
                return True, st.session_state.get("cap_researcher_name", "Unknown")

            return False, ""

        except (KeyError, FileNotFoundError, AttributeError):
            # Secrets not configured
            return False, ""

    @staticmethod
    def get_current_user_role() -> str:
        """Get the current user's role from session state."""
        return st.session_state.get("cap_user_role", "researcher")

    @staticmethod
    def get_current_user_id() -> Optional[int]:
        """Get the current user's ID from session state."""
        return st.session_state.get("cap_user_id")

    @staticmethod
    def is_admin() -> bool:
        """Check if current user is an admin."""
        return st.session_state.get("cap_user_role") == CAPUserService.ROLE_ADMIN

    def render_login_form(self) -> bool:
        """
        Render the multi-user login form with researcher dropdown.

        Returns:
            True if login successful, False otherwise
        """
        st.subheader("🔐 Login")

        # Ensure user service is available
        if self.user_service is None:
            st.error("Authentication service not available")
            return False

        # Bootstrap admin if needed (first run)
        self.user_service.bootstrap_admin_from_secrets()

        # Get active researchers for dropdown
        researchers = self.user_service.get_active_researchers()

        if not researchers:
            st.warning("""
            **No researchers configured**

            The system needs at least one user account.
            Please contact your administrator or check the bootstrap configuration.
            """)
            return False

        with st.form("cap_login_form"):
            # Researcher selection dropdown
            researcher_options = {r["display_name"]: r["username"] for r in researchers}
            selected_display = st.selectbox(
                "Select Researcher",
                options=list(researcher_options.keys()),
                help="Select your researcher account",
            )

            # Password input
            password = st.text_input("Password", type="password")

            submitted = st.form_submit_button("Login", use_container_width=True)

            if submitted:
                if not selected_display or not password:
                    st.error("Please select a researcher and enter your password")
                    return False

                username = researcher_options[selected_display]
                user = self.user_service.authenticate(username, password)

                if user:
                    # Set session state
                    st.session_state.cap_authenticated = True
                    st.session_state.cap_researcher_name = user["display_name"]
                    st.session_state.cap_user_id = user["id"]
                    st.session_state.cap_user_role = user["role"]
                    st.session_state.cap_username = user["username"]
                    st.session_state.cap_login_time = datetime.now()

                    st.success(f"Welcome, {user['display_name']}!")
                    return True
                else:
                    st.error("Incorrect password")
                    return False

        return False

    @staticmethod
    def render_user_info(researcher_name: str):
        """Render user info and logout button."""
        col1, col2 = st.columns([3, 1])
        with col1:
            login_time = st.session_state.get("cap_login_time")
            role = st.session_state.get("cap_user_role", "researcher")
            role_badge = "👑" if role == "admin" else "👤"

            if login_time:
                time_str = login_time.strftime("%H:%M")
                st.success(f"{role_badge} Logged in as: {researcher_name} (since {time_str})")
            else:
                st.success(f"{role_badge} Logged in as: {researcher_name}")
        with col2:
            if st.button("🚪 Logout"):
                CAPAuthHandler.logout()

    @staticmethod
    def logout():
        """Clear all authentication session state."""
        st.session_state.cap_authenticated = False
        st.session_state.cap_researcher_name = ""
        st.session_state.cap_user_id = None
        st.session_state.cap_user_role = ""
        st.session_state.cap_username = ""
        st.session_state.cap_login_time = None

    @staticmethod
    def is_feature_enabled() -> bool:
        """Check if CAP annotation feature is enabled."""
        return _get_cap_secrets().get("enabled", False)

    @staticmethod
    def render_disabled_message():
        """Render message when feature is disabled."""
        st.warning("""
        **Annotation System Not Enabled**

        To enable, add the following to `.streamlit/secrets.toml`:
        ```toml
        [cap_annotation]
        enabled = true
        bootstrap_admin_username = "admin"
        bootstrap_admin_display_name = "Administrator"
        bootstrap_admin_password = "your-secure-password"
        ```
        """)

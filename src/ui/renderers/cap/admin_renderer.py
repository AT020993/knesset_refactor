"""
CAP Admin Renderer

Provides the admin panel for managing researcher accounts.
Only accessible to users with the 'admin' role.

Features:
- View all researchers with status
- Add new researchers
- Edit display name
- Reset password
- Change role
- Deactivate/Reactivate (soft delete)
- Permanently delete (hard delete, only if no annotations)
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

from ui.services.cap.user_service import CAPUserService, get_user_service


class CAPAdminRenderer:
    """Renders the admin panel for researcher management."""

    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        """Initialize the admin renderer."""
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)
        self._user_service: Optional[CAPUserService] = None

    @property
    def user_service(self) -> CAPUserService:
        """Get or create the user service."""
        if self._user_service is None:
            self._user_service = get_user_service(self.db_path, self.logger)
        return self._user_service

    def render_admin_panel(self):
        """Render the complete admin panel."""
        st.subheader("👥 Researcher Management")

        # Current researchers table
        self._render_users_table()

        st.markdown("---")

        # Add new researcher form
        self._render_add_user_form()

        # Render dialogs if triggered
        self._render_edit_dialog()
        self._render_password_reset_dialog()
        self._render_delete_confirmation_dialog()

    def _render_users_table(self):
        """Render the table of all users with action buttons."""
        users_df = self.user_service.get_all_users()

        if users_df.empty:
            st.info("No researchers found. Add your first researcher below.")
            return

        st.markdown("#### Current Researchers")

        # Prepare display data
        display_df = users_df.copy()

        # Format columns for display
        display_df["Status"] = display_df["IsActive"].apply(
            lambda x: "✅ Active" if x else "❌ Inactive"
        )
        display_df["Role Display"] = display_df["Role"].apply(
            lambda x: "👑 Admin" if x == "admin" else "📝 Researcher"
        )
        display_df["Last Login"] = display_df["LastLoginAt"].apply(
            lambda x: x.strftime("%Y-%m-%d %H:%M") if pd.notna(x) else "Never"
        )
        display_df["Created"] = display_df["CreatedAt"].apply(
            lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else "Unknown"
        )

        # Select columns for display
        display_columns = ["DisplayName", "Username", "Role Display", "Status", "Last Login", "Created"]
        st.dataframe(
            display_df[display_columns],
            column_config={
                "DisplayName": st.column_config.TextColumn("Name", width="medium"),
                "Username": st.column_config.TextColumn("Username", width="small"),
                "Role Display": st.column_config.TextColumn("Role", width="small"),
                "Status": st.column_config.TextColumn("Status", width="small"),
                "Last Login": st.column_config.TextColumn("Last Login", width="medium"),
                "Created": st.column_config.TextColumn("Created", width="small"),
            },
            use_container_width=True,
            hide_index=True,
        )

        # Action section
        st.markdown("#### Actions")

        # Get current user ID to prevent self-operations
        current_user_id = st.session_state.get("cap_user_id")

        # User selector
        user_options = {
            f"{row['DisplayName']} ({row['Username']})": row['ResearcherID']
            for _, row in users_df.iterrows()
        }
        selected_user = st.selectbox(
            "Select user for action",
            options=list(user_options.keys()),
            key="admin_user_select",
        )
        selected_user_id = user_options.get(selected_user)

        # Get selected user info
        selected_user_info = None
        if selected_user_id:
            matching = users_df[users_df["ResearcherID"] == selected_user_id]
            if not matching.empty:
                selected_user_info = matching.iloc[0]

        is_self = selected_user_id == current_user_id
        is_active = selected_user_info["IsActive"] if selected_user_info is not None else True

        # Track if an action is in progress (prevents double-clicks)
        is_action_running = st.session_state.get("admin_action_running", False)

        # Action buttons in columns
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if st.button(
                "✏️ Edit Name",
                key="btn_edit",
                use_container_width=True,
                disabled=is_action_running,
            ):
                if selected_user_id:
                    st.session_state.admin_edit_user_id = selected_user_id
                    st.session_state.admin_show_edit_dialog = True

        with col2:
            if st.button(
                "🔑 Reset Password",
                key="btn_reset_pwd",
                use_container_width=True,
                disabled=is_action_running,
            ):
                if selected_user_id:
                    st.session_state.admin_reset_user_id = selected_user_id
                    st.session_state.admin_show_reset_dialog = True

        with col3:
            if is_active:
                disabled = is_self or is_action_running
                help_text = "Cannot deactivate yourself" if is_self else "Deactivate this user"
                if st.button(
                    "🚫 Deactivate",
                    key="btn_deactivate",
                    disabled=disabled,
                    help=help_text,
                    use_container_width=True,
                ):
                    if selected_user_id and not is_self:
                        st.session_state.admin_action_running = True
                        if self.user_service.delete_user(selected_user_id):
                            st.success("User deactivated successfully")
                        else:
                            st.error("Failed to deactivate user")
                        st.session_state.admin_action_running = False
            else:
                if st.button(
                    "✅ Reactivate",
                    key="btn_reactivate",
                    use_container_width=True,
                    disabled=is_action_running,
                ):
                    if selected_user_id:
                        st.session_state.admin_action_running = True
                        if self.user_service.reactivate_user(selected_user_id):
                            st.success("User reactivated successfully")
                        else:
                            st.error("Failed to reactivate user")
                        st.session_state.admin_action_running = False

        with col4:
            disabled = is_self or is_action_running
            help_text = "Cannot delete yourself" if is_self else "Permanently delete this user"
            if st.button(
                "🗑️ Delete",
                key="btn_delete",
                disabled=disabled,
                help=help_text,
                type="secondary",
                use_container_width=True,
            ):
                if selected_user_id and not is_self:
                    st.session_state.admin_delete_user_id = selected_user_id
                    st.session_state.admin_show_delete_dialog = True

        # Role change section
        if selected_user_info is not None and not is_self:
            with st.expander("🔄 Change Role", expanded=False):
                current_role = selected_user_info["Role"]
                new_role = st.radio(
                    "Select new role",
                    options=["researcher", "admin"],
                    index=0 if current_role == "researcher" else 1,
                    format_func=lambda x: "📝 Researcher" if x == "researcher" else "👑 Admin",
                    horizontal=True,
                    key="admin_role_change",
                )
                if new_role != current_role:
                    if st.button("Update Role", key="btn_update_role"):
                        if self.user_service.update_role(selected_user_id, new_role):
                            st.success(f"Role updated to {new_role}")
                        else:
                            st.error("Failed to update role")

    def _render_add_user_form(self):
        """Render the form for adding a new researcher."""
        with st.expander("➕ Add New Researcher", expanded=False):
            with st.form("add_researcher_form", clear_on_submit=True):
                col1, col2 = st.columns(2)

                with col1:
                    username = st.text_input(
                        "Username",
                        help="Unique login identifier (lowercase, no spaces)",
                        placeholder="jsmith",
                    )
                    password = st.text_input(
                        "Password",
                        type="password",
                        help="Minimum 6 characters",
                    )

                with col2:
                    display_name = st.text_input(
                        "Display Name",
                        help="Full name shown in the UI",
                        placeholder="Dr. John Smith",
                    )
                    confirm_password = st.text_input(
                        "Confirm Password",
                        type="password",
                    )

                role = st.selectbox(
                    "Role",
                    options=["researcher", "admin"],
                    format_func=lambda x: "📝 Researcher" if x == "researcher" else "👑 Admin",
                    help="Admins can manage other users",
                )

                submitted = st.form_submit_button("Add Researcher", use_container_width=True)

                if submitted:
                    errors = []

                    if not username or not username.strip():
                        errors.append("Username is required")
                    elif " " in username:
                        errors.append("Username cannot contain spaces")
                    elif self.user_service.user_exists(username.strip().lower()):
                        errors.append("Username already exists")

                    if not display_name or not display_name.strip():
                        errors.append("Display name is required")

                    if not password:
                        errors.append("Password is required")
                    elif len(password) < 6:
                        errors.append("Password must be at least 6 characters")
                    elif password != confirm_password:
                        errors.append("Passwords do not match")

                    if errors:
                        for error in errors:
                            st.error(error)
                    else:
                        created_by = st.session_state.get("cap_researcher_name", "Unknown")

                        success = self.user_service.create_user(
                            username=username.strip().lower(),
                            display_name=display_name.strip(),
                            password=password,
                            role=role,
                            created_by=created_by,
                        )

                        if success:
                            st.success(f"Researcher '{display_name}' created successfully!")
                        else:
                            st.error("Failed to create researcher. Please check the logs.")

    def _render_edit_dialog(self):
        """Render the edit display name dialog if triggered."""
        if not st.session_state.get("admin_show_edit_dialog", False):
            return

        user_id = st.session_state.get("admin_edit_user_id")
        if not user_id:
            st.session_state.admin_show_edit_dialog = False
            return

        user = self.user_service.get_user_by_id(user_id)
        if not user:
            st.error("User not found")
            st.session_state.admin_show_edit_dialog = False
            return

        st.markdown("---")
        st.markdown(f"#### ✏️ Edit User: {user['username']}")

        with st.form("edit_user_form"):
            new_display_name = st.text_input(
                "Display Name",
                value=user["display_name"],
                help="Full name shown in the UI",
            )

            col1, col2 = st.columns(2)

            with col1:
                if st.form_submit_button("Save Changes", use_container_width=True):
                    if not new_display_name or not new_display_name.strip():
                        st.error("Display name cannot be empty")
                    elif new_display_name.strip() == user["display_name"]:
                        st.info("No changes made")
                    else:
                        if self.user_service.update_display_name(user_id, new_display_name):
                            st.success(f"Display name updated to '{new_display_name.strip()}'")
                            st.session_state.admin_show_edit_dialog = False
                            st.session_state.admin_edit_user_id = None
                        else:
                            st.error("Failed to update display name")

            with col2:
                if st.form_submit_button("Cancel", use_container_width=True):
                    st.session_state.admin_show_edit_dialog = False
                    st.session_state.admin_edit_user_id = None

    def _render_password_reset_dialog(self):
        """Render the password reset dialog if triggered."""
        if not st.session_state.get("admin_show_reset_dialog", False):
            return

        user_id = st.session_state.get("admin_reset_user_id")
        if not user_id:
            st.session_state.admin_show_reset_dialog = False
            return

        user = self.user_service.get_user_by_id(user_id)
        if not user:
            st.error("User not found")
            st.session_state.admin_show_reset_dialog = False
            return

        st.markdown("---")
        st.markdown(f"#### 🔑 Reset Password for {user['display_name']}")

        with st.form("reset_password_form"):
            new_password = st.text_input(
                "New Password",
                type="password",
                help="Minimum 6 characters",
            )
            confirm_new_password = st.text_input(
                "Confirm New Password",
                type="password",
            )

            col1, col2 = st.columns(2)

            with col1:
                if st.form_submit_button("Reset Password", use_container_width=True):
                    if not new_password:
                        st.error("Password is required")
                    elif len(new_password) < 6:
                        st.error("Password must be at least 6 characters")
                    elif new_password != confirm_new_password:
                        st.error("Passwords do not match")
                    else:
                        if self.user_service.reset_password(user_id, new_password):
                            st.success(f"Password reset for {user['display_name']}")
                            st.session_state.admin_show_reset_dialog = False
                            st.session_state.admin_reset_user_id = None
                        else:
                            st.error("Failed to reset password")

            with col2:
                if st.form_submit_button("Cancel", use_container_width=True):
                    st.session_state.admin_show_reset_dialog = False
                    st.session_state.admin_reset_user_id = None

    def _render_delete_confirmation_dialog(self):
        """Render the delete confirmation dialog if triggered."""
        if not st.session_state.get("admin_show_delete_dialog", False):
            return

        user_id = st.session_state.get("admin_delete_user_id")
        if not user_id:
            st.session_state.admin_show_delete_dialog = False
            return

        user = self.user_service.get_user_by_id(user_id)
        if not user:
            st.error("User not found")
            st.session_state.admin_show_delete_dialog = False
            return

        # Check if user has annotations
        annotation_count = self.user_service.get_user_annotation_count(user_id)

        st.markdown("---")
        st.markdown(f"#### 🗑️ Delete User: {user['display_name']}")

        if annotation_count > 0:
            st.warning(f"""
            ⚠️ **Cannot permanently delete this user**

            {user['display_name']} has **{annotation_count} annotations** in the system.
            To preserve data integrity, you can only **deactivate** this user.

            Deactivating will:
            - Prevent them from logging in
            - Keep their annotations intact
            - Allow reactivation later if needed
            """)

            col1, col2 = st.columns(2)
            with col1:
                if st.button("🚫 Deactivate Instead", key="btn_deactivate_instead", use_container_width=True):
                    if self.user_service.delete_user(user_id):
                        st.success("User deactivated successfully")
                        st.session_state.admin_show_delete_dialog = False
                        st.session_state.admin_delete_user_id = None
                    else:
                        st.error("Failed to deactivate user")
            with col2:
                if st.button("Cancel", key="btn_cancel_delete", use_container_width=True):
                    st.session_state.admin_show_delete_dialog = False
                    st.session_state.admin_delete_user_id = None
        else:
            st.error(f"""
            ⚠️ **This action is permanent and cannot be undone!**

            Are you sure you want to permanently delete **{user['display_name']}** ({user['username']})?

            This will completely remove the user from the system.
            """)

            # Confirmation checkbox
            confirm = st.checkbox(
                f"I understand this will permanently delete {user['display_name']}",
                key="confirm_delete_checkbox",
            )

            col1, col2 = st.columns(2)
            with col1:
                if st.button(
                    "🗑️ Delete Permanently",
                    key="btn_confirm_delete",
                    disabled=not confirm,
                    type="primary",
                    use_container_width=True,
                ):
                    if self.user_service.hard_delete_user(user_id):
                        st.success(f"User '{user['display_name']}' permanently deleted")
                        st.session_state.admin_show_delete_dialog = False
                        st.session_state.admin_delete_user_id = None
                    else:
                        st.error("Failed to delete user")
            with col2:
                if st.button("Cancel", key="btn_cancel_hard_delete", use_container_width=True):
                    st.session_state.admin_show_delete_dialog = False
                    st.session_state.admin_delete_user_id = None


def render_admin_panel(db_path: Path, logger_obj: Optional[logging.Logger] = None):
    """Convenience function to render the admin panel."""
    renderer = CAPAdminRenderer(db_path, logger_obj)
    renderer.render_admin_panel()

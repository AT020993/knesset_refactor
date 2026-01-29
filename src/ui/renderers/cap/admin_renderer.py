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

    # Valid roles for the CAP system
    VALID_ROLES = {"admin", "researcher"}

    @property
    def user_service(self) -> CAPUserService:
        """Get or create the user service."""
        if self._user_service is None:
            self._user_service = get_user_service(self.db_path, self.logger)
        return self._user_service

    def _get_user_list(self) -> pd.DataFrame:
        """
        Get list of all users from the user service.

        Returns:
            DataFrame with user information
        """
        return self.user_service.get_all_users()

    def _can_delete_user(self, user_id: int, current_user_id: Optional[int] = None) -> tuple[bool, str]:
        """
        Check if a user can be deleted.

        Rules:
        - Cannot delete yourself
        - Users with annotations can only be soft-deleted (deactivated), not hard-deleted

        Args:
            user_id: ID of user to delete
            current_user_id: ID of currently logged-in user (if None, uses session state)

        Returns:
            Tuple of (can_delete, reason).
            - (True, "") if deletion is allowed
            - (False, "reason") if deletion is not allowed
        """
        if current_user_id is None:
            current_user_id = st.session_state.get("cap_user_id")

        # Cannot delete yourself
        if user_id == current_user_id:
            return False, "Cannot delete your own account"

        # Check if user has annotations
        annotation_count = self.user_service.get_user_annotation_count(user_id)
        if annotation_count > 0:
            return False, f"User has {annotation_count} annotations - can only deactivate, not permanently delete"

        return True, ""

    def _validate_role_change(self, new_role: str) -> tuple[bool, Optional[str]]:
        """
        Validate that a role is valid.

        Args:
            new_role: Role to validate

        Returns:
            Tuple of (is_valid, error_message).
            - (True, None) if role is valid
            - (False, "error message") if role is invalid
        """
        if new_role not in self.VALID_ROLES:
            return False, f"Invalid role: '{new_role}'. Must be one of: {', '.join(sorted(self.VALID_ROLES))}"
        return True, None

    def render_admin_panel(self):
        """Render the complete admin panel."""
        st.subheader("👥 Researcher Management")

        # Debug: Clear any stale delete dialog state if there was an error
        if st.session_state.get("admin_delete_had_error"):
            st.session_state.admin_show_delete_dialog = False
            st.session_state.admin_delete_had_error = False

        # Current researchers table
        self._render_users_table()

        st.markdown("---")

        # Add new researcher form
        self._render_add_user_form()

        # Render dialogs if triggered
        self._render_edit_dialog()
        self._render_password_reset_dialog()
        self._render_delete_confirmation_dialog()

        # Database maintenance section
        st.markdown("---")
        self._render_database_maintenance()

    def _render_users_table(self):
        """Render the table of all users with action buttons."""
        try:
            users_df = self.user_service.get_all_users()
        except Exception as e:
            import traceback
            st.error(f"Error loading users: {e}")
            st.code(traceback.format_exc())
            return

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
                    # Password confirmation check (UI-only validation)
                    if password and password != confirm_password:
                        st.error("Passwords do not match")
                    else:
                        created_by = st.session_state.get("cap_researcher_name", "Unknown")

                        # Use create_user_with_validation for all other validation
                        user_id, error = self.user_service.create_user_with_validation(
                            username=username,
                            display_name=display_name,
                            password=password,
                            role=role,
                            created_by=created_by,
                        )

                        if error:
                            st.error(f"❌ {error}")
                        else:
                            st.success(f"✅ User '{display_name.strip()}' created successfully!")

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
        try:
            annotation_count = self.user_service.get_user_annotation_count(user_id)
        except Exception as e:
            import traceback
            st.error(f"Error checking annotations: {e}")
            st.code(traceback.format_exc())
            st.info("Try running Database Repair first (scroll down to Database Maintenance)")
            # Set flag to clear dialog on next render
            st.session_state.admin_delete_had_error = True
            st.session_state.admin_show_delete_dialog = False
            return

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
                    try:
                        self.logger.info(f"Attempting to delete user {user_id}")
                        if self.user_service.hard_delete_user(user_id):
                            st.success(f"User '{user['display_name']}' permanently deleted")
                            st.session_state.admin_show_delete_dialog = False
                            st.session_state.admin_delete_user_id = None
                        else:
                            st.error("Failed to delete user. Check logs for details.")
                            st.info("Try running Database Repair first, then Sync to Cloud.")
                    except Exception as e:
                        import traceback
                        st.error(f"Delete error: {e}")
                        st.code(traceback.format_exc())
            with col2:
                if st.button("Cancel", key="btn_cancel_hard_delete", use_container_width=True):
                    st.session_state.admin_show_delete_dialog = False
                    st.session_state.admin_delete_user_id = None

    def _render_database_maintenance(self):
        """Render database maintenance tools for admins."""
        with st.expander("🔧 Database Maintenance", expanded=False):
            st.caption(
                "Use these tools to fix database issues. "
                "Only use if you're experiencing errors."
            )
            st.caption("_Code version: 2026-01-25-v6_")

            col1, col2 = st.columns(2)

            with col1:
                if st.button("🩺 Run Database Repair", key="btn_db_repair"):
                    self._run_database_repair()
                    self._clear_admin_caches()
                    st.info("🔄 Services reset. Please try your operation again.")

            with col2:
                if st.button("🔄 Full Catalog Rebuild", key="btn_catalog_rebuild"):
                    st.warning(
                        "⚠️ This completely rebuilds the database catalog using EXPORT/IMPORT. "
                        "Use this if regular repair doesn't fix the issue."
                    )
                    self._run_full_catalog_rebuild()
                    self._clear_admin_caches()

    def _clear_admin_caches(self):
        """Clear admin caches while preserving authentication."""
        # Clear cached services to force fresh connections
        self._user_service = None

        # Authentication keys to KEEP
        auth_keys = {
            'cap_authenticated', 'cap_user_id', 'cap_user_role',
            'cap_username', 'cap_researcher_name', 'cap_login_time'
        }
        keys_to_clear = [k for k in list(st.session_state.keys())
                       if k.startswith('admin_') or
                       (k.startswith('cap_') and k not in auth_keys)]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]

    def _run_full_catalog_rebuild(self):
        """
        Completely rebuild database catalog using EXPORT/IMPORT.

        This is the nuclear option for fixing corrupted FK references that persist
        in DuckDB's internal catalog even after rebuilding individual tables.
        """
        import duckdb
        import shutil
        import tempfile
        import os

        st.info("Starting full catalog rebuild... This may take a moment.")
        db_path_str = str(self.db_path)

        # Create temp directory for export
        export_dir = tempfile.mkdtemp(prefix="duckdb_export_")
        backup_path = db_path_str + ".backup"

        try:
            # Step 1: Export the entire database
            st.write("📤 Exporting database...")
            conn = duckdb.connect(db_path_str, read_only=False)
            try:
                conn.execute(f"EXPORT DATABASE '{export_dir}' (FORMAT PARQUET)")
                st.write("✅ Export completed")
            finally:
                conn.close()

            # Step 2: Backup original database file
            st.write("💾 Backing up original database...")
            shutil.copy2(db_path_str, backup_path)

            # Step 3: Delete original database to get clean slate
            st.write("🗑️ Removing original database...")
            os.remove(db_path_str)

            # Also remove WAL file if exists
            wal_path = db_path_str + ".wal"
            if os.path.exists(wal_path):
                os.remove(wal_path)

            # Step 4: Create fresh database and import
            st.write("📥 Creating fresh database and importing...")
            conn = duckdb.connect(db_path_str, read_only=False)
            try:
                conn.execute(f"IMPORT DATABASE '{export_dir}'")
                conn.execute("CHECKPOINT")
                st.write("✅ Import completed")

                # Verify tables
                tables = conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                ).fetchall()
                st.write(f"✅ Verified {len(tables)} tables imported")

                # Test query
                try:
                    count = conn.execute(
                        "SELECT COUNT(*) FROM UserBillCAP WHERE ResearcherID = 999"
                    ).fetchone()[0]
                    st.write(f"✅ Test query succeeded (count={count})")
                except Exception as e:
                    st.error(f"❌ Test query failed: {e}")

                # Remove backup since we succeeded
                if os.path.exists(backup_path):
                    os.remove(backup_path)

                st.success(
                    "✅ **Full catalog rebuild complete!** "
                    "The database now has a clean catalog. Try your operation again."
                )

                # Offer to sync
                st.markdown("---")
                if st.button("☁️ Sync Rebuilt DB to Cloud", key="btn_sync_rebuilt_to_cloud"):
                    self._sync_repaired_db_to_cloud()

            except Exception as import_e:
                st.error(f"❌ Import failed: {import_e}")
                conn.close()
                conn = None

                # Restore from backup
                if os.path.exists(backup_path):
                    st.write("⏮️ Restoring from backup...")
                    if os.path.exists(db_path_str):
                        os.remove(db_path_str)
                    shutil.move(backup_path, db_path_str)
                    st.warning("Database restored from backup.")

                raise

            finally:
                if conn:
                    conn.close()

        except Exception as e:
            import traceback
            st.error(f"❌ Full catalog rebuild failed: {e}")
            st.code(traceback.format_exc())
            self.logger.error(f"Full catalog rebuild error: {e}", exc_info=True)

        finally:
            # Clean up export directory
            if os.path.exists(export_dir):
                shutil.rmtree(export_dir, ignore_errors=True)

    def _run_database_repair(self):
        """Run database repair operations to fix migration artifacts."""
        import duckdb

        st.info("Running comprehensive database repair...")
        issues_found = []
        fixes_applied = []

        try:
            # Use raw duckdb connection to avoid any wrapper issues
            conn = duckdb.connect(str(self.db_path), read_only=False)

            try:
                # 1. List ALL tables to understand database state
                all_tables = conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                ).fetchall()
                fixes_applied.append(f"Found {len(all_tables)} tables: {[t[0] for t in all_tables]}")

                # 2. Find and drop any *_new migration artifact tables
                for (table_name,) in all_tables:
                    if table_name.endswith('_new'):
                        issues_found.append(f"Found migration artifact table: {table_name}")
                        try:
                            conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                            fixes_applied.append(f"Dropped table: {table_name}")
                        except Exception as e:
                            fixes_applied.append(f"Failed to drop {table_name}: {e}")

                # 3. Check for any views
                try:
                    views = conn.execute(
                        "SELECT table_name FROM information_schema.views WHERE table_schema = 'main'"
                    ).fetchall()
                    if views:
                        fixes_applied.append(f"Found views: {[v[0] for v in views]}")
                        for (view_name,) in views:
                            try:
                                view_def = conn.execute(
                                    f"SELECT view_definition FROM information_schema.views WHERE table_name = '{view_name}'"
                                ).fetchone()
                                if view_def and "_new" in str(view_def[0]):
                                    issues_found.append(f"View {view_name} references _new table!")
                                    conn.execute(f'DROP VIEW IF EXISTS "{view_name}"')
                                    fixes_applied.append(f"Dropped problematic view: {view_name}")
                            except Exception as e:
                                fixes_applied.append(f"Error checking view {view_name}: {e}")
                    else:
                        fixes_applied.append("No views found")
                except Exception as e:
                    fixes_applied.append(f"Could not check views: {e}")

                # 4. Check DuckDB internal tables/dependencies
                try:
                    deps = conn.execute("SELECT * FROM duckdb_dependencies()").fetchall()
                    new_deps = [d for d in deps if "_new" in str(d)]
                    if new_deps:
                        issues_found.append(f"Found dependencies with _new: {new_deps}")
                except Exception as e:
                    fixes_applied.append(f"Could not check dependencies: {e}")

                # 5. Check sequences
                try:
                    seqs = conn.execute("SELECT sequence_name FROM duckdb_sequences()").fetchall()
                    fixes_applied.append(f"Sequences: {[s[0] for s in seqs]}")
                except Exception as e:
                    fixes_applied.append(f"Could not list sequences: {e}")

                # 6. Force drop UserBillCAP_new
                try:
                    conn.execute("DROP TABLE IF EXISTS UserBillCAP_new CASCADE")
                    fixes_applied.append("Executed DROP TABLE IF EXISTS UserBillCAP_new CASCADE")
                except Exception as e:
                    fixes_applied.append(f"DROP UserBillCAP_new: {e}")

                # 7. Check UserBillCAP table structure
                try:
                    cols = conn.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'UserBillCAP' ORDER BY ordinal_position"
                    ).fetchall()
                    fixes_applied.append(f"UserBillCAP columns: {[c[0] for c in cols]}")
                except Exception as e:
                    issues_found.append(f"Could not read UserBillCAP structure: {e}")

                # 8. Check constraints on UserBillCAP
                try:
                    constraints = conn.execute(
                        "SELECT constraint_name, constraint_type "
                        "FROM information_schema.table_constraints "
                        "WHERE table_name = 'UserBillCAP'"
                    ).fetchall()
                    fixes_applied.append(f"UserBillCAP constraints: {constraints}")
                except Exception as e:
                    fixes_applied.append(f"Could not check constraints: {e}")

                # 9. CHECKPOINT and VACUUM
                try:
                    conn.execute("CHECKPOINT")
                    conn.execute("VACUUM")
                    fixes_applied.append("CHECKPOINT and VACUUM completed")
                except Exception as e:
                    fixes_applied.append(f"CHECKPOINT/VACUUM: {e}")

                # 10. Test the exact query that fails
                try:
                    count = conn.execute(
                        "SELECT COUNT(*) FROM UserBillCAP WHERE ResearcherID = 999"
                    ).fetchone()[0]
                    fixes_applied.append(f"✅ Test query succeeded (count={count})")
                except Exception as e:
                    issues_found.append(f"❌ Test query FAILED: {e}")

                # 11. Check for any triggers
                try:
                    triggers = conn.execute(
                        "SELECT * FROM duckdb_constraints() WHERE constraint_type = 'TRIGGER'"
                    ).fetchall()
                    if triggers:
                        fixes_applied.append(f"Triggers found: {triggers}")
                    else:
                        fixes_applied.append("No triggers found")
                except Exception as e:
                    fixes_applied.append(f"Could not check triggers: {e}")

                # 12. List all objects in catalog that might reference UserBillCAP
                try:
                    all_objects = conn.execute("""
                        SELECT table_name, table_type
                        FROM information_schema.tables
                        WHERE table_name LIKE '%UserBillCAP%' OR table_name LIKE '%userbillcap%'
                    """).fetchall()
                    fixes_applied.append(f"Objects matching UserBillCAP: {all_objects}")
                except Exception as e:
                    fixes_applied.append(f"Could not list UserBillCAP objects: {e}")

                # 13. Force full checkpoint to flush all changes to disk
                try:
                    conn.execute("FORCE CHECKPOINT")
                    fixes_applied.append("FORCE CHECKPOINT completed")
                except Exception as e:
                    fixes_applied.append(f"FORCE CHECKPOINT: {e}")

                # 14. NUCLEAR OPTION: Rebuild UserBillCAP table to force catalog rebuild
                # This exports data, drops table, recreates with clean schema, reimports
                try:
                    # Check if table exists
                    table_exists = conn.execute(
                        "SELECT 1 FROM information_schema.tables WHERE table_name = 'UserBillCAP'"
                    ).fetchone()

                    if table_exists:
                        # Check if table has data
                        row_count = conn.execute("SELECT COUNT(*) FROM UserBillCAP").fetchone()[0]
                        has_data = row_count > 0

                        if has_data:
                            # Export to temp table
                            conn.execute("CREATE TABLE UserBillCAP_backup AS SELECT * FROM UserBillCAP")
                            fixes_applied.append(f"Backed up UserBillCAP data ({row_count} rows)")

                        # Drop and recreate with explicit schema (forces catalog rebuild)
                        conn.execute("DROP TABLE IF EXISTS UserBillCAP CASCADE")

                        # Ensure sequence exists
                        conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_annotation_id START 1")

                        # Recreate table with proper schema (without Direction column)
                        conn.execute("""
                            CREATE TABLE UserBillCAP (
                                AnnotationID INTEGER PRIMARY KEY DEFAULT nextval('seq_annotation_id'),
                                BillID INTEGER NOT NULL,
                                ResearcherID INTEGER NOT NULL,
                                CAPMinorCode INTEGER NOT NULL,
                                AssignedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                Confidence VARCHAR DEFAULT 'Medium',
                                Notes VARCHAR,
                                Source VARCHAR DEFAULT 'Database',
                                SubmissionDate VARCHAR,
                                UNIQUE(BillID, ResearcherID)
                            )
                        """)

                        if has_data:
                            # Restore data
                            conn.execute("""
                                INSERT INTO UserBillCAP
                                (AnnotationID, BillID, ResearcherID, CAPMinorCode,
                                 AssignedDate, Confidence, Notes, Source, SubmissionDate)
                                SELECT AnnotationID, BillID, ResearcherID, CAPMinorCode,
                                       AssignedDate, Confidence, Notes, Source, SubmissionDate
                                FROM UserBillCAP_backup
                            """)
                            conn.execute("DROP TABLE UserBillCAP_backup")
                            fixes_applied.append(f"✅ Rebuilt UserBillCAP table with {row_count} rows restored")
                        else:
                            fixes_applied.append("✅ Rebuilt empty UserBillCAP table")

                        # Final checkpoint after rebuild
                        conn.execute("FORCE CHECKPOINT")
                        fixes_applied.append("Final CHECKPOINT after rebuild")
                    else:
                        fixes_applied.append("UserBillCAP table does not exist - nothing to rebuild")

                except Exception as e:
                    import traceback
                    fixes_applied.append(f"❌ Table rebuild failed: {e}")
                    fixes_applied.append(f"Traceback: {traceback.format_exc()[:500]}")

                # 15. Final test query after rebuild
                try:
                    count = conn.execute(
                        "SELECT COUNT(*) FROM UserBillCAP WHERE ResearcherID = 999"
                    ).fetchone()[0]
                    fixes_applied.append(f"✅ Final test query after rebuild succeeded (count={count})")
                except Exception as e:
                    issues_found.append(f"❌ Final test query FAILED after rebuild: {e}")

            finally:
                conn.close()

            # Report results
            if issues_found:
                st.warning("**Issues found:**")
                for issue in issues_found:
                    st.write(f"- {issue}")
            else:
                st.success("**No issues found!**")

            st.info("**Diagnostic info:**")
            for fix in fixes_applied:
                st.write(f"- {fix}")

            st.info("Please try your operation again.")

            # Offer to sync repaired database back to GCS
            st.markdown("---")
            st.markdown("**Sync repaired database to cloud?**")
            st.caption(
                "If you're on Streamlit Cloud, sync the repaired database to GCS "
                "so the fix persists across reboots."
            )
            if st.button("☁️ Sync to Cloud", key="btn_sync_repair_to_cloud"):
                self._sync_repaired_db_to_cloud()

        except Exception as e:
            import traceback
            st.error(f"Database repair failed: {e}")
            st.code(traceback.format_exc())
            self.logger.error(f"Database repair error: {e}", exc_info=True)

    def _sync_repaired_db_to_cloud(self):
        """Upload the repaired database to GCS."""
        try:
            from data.services.storage_sync_service import StorageSyncService
            from config.settings import Settings

            sync_service = StorageSyncService(logger_obj=self.logger)
            if not sync_service.is_enabled():
                st.warning("Cloud storage is not enabled. No sync needed for local development.")
                return

            with st.spinner("Uploading repaired database to cloud..."):
                success = sync_service.gcs_manager.upload_file(
                    Settings.DEFAULT_DB_PATH, "data/warehouse.duckdb"
                )

            if success:
                st.success("✅ Repaired database synced to cloud! The fix will persist across reboots.")
            else:
                st.error("❌ Failed to sync to cloud. Check logs for details.")

        except Exception as e:
            st.error(f"Cloud sync failed: {e}")
            self.logger.error(f"Cloud sync error: {e}", exc_info=True)


def render_admin_panel(db_path: Path, logger_obj: Optional[logging.Logger] = None):
    """Convenience function to render the admin panel."""
    renderer = CAPAdminRenderer(db_path, logger_obj)
    renderer.render_admin_panel()

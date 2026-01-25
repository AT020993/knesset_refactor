"""
CAP User Service

Handles researcher authentication and user management for the CAP annotation system.
Uses bcrypt for secure password hashing with proper salt handling.

Roles:
- 'admin': Can manage other users (create, delete, reset passwords)
- 'researcher': Can annotate bills but cannot manage users
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

import bcrypt
import pandas as pd
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query


class CAPUserService:
    """Service for managing CAP researcher authentication and user accounts."""

    ROLE_ADMIN = "admin"
    ROLE_RESEARCHER = "researcher"

    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        """Initialize the user service."""
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)
        self._table_ensured = False

    def ensure_table_exists(self) -> bool:
        """
        Ensure the UserResearchers table exists with proper sequence for ID generation.

        This is called automatically before any database queries to handle
        the case where the table hasn't been created yet.

        Note: Uses a DuckDB sequence for thread-safe ID generation instead of
        MAX()+1, which prevents race conditions when multiple admins create
        users simultaneously.

        Returns:
            True if table exists or was created, False on error
        """
        if self._table_ensured:
            return True

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                # Check if table already exists (for migration handling)
                table_exists = conn.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = 'UserResearchers'"
                ).fetchone()

                if table_exists:
                    # Table exists - ensure sequence exists and is set correctly
                    seq_exists = conn.execute(
                        "SELECT 1 FROM duckdb_sequences() WHERE sequence_name = 'seq_researcher_id'"
                    ).fetchone()

                    if not seq_exists:
                        # Migration: create sequence starting after max existing ID
                        max_id = conn.execute(
                            "SELECT COALESCE(MAX(ResearcherID), 0) FROM UserResearchers"
                        ).fetchone()[0]
                        conn.execute(f"CREATE SEQUENCE seq_researcher_id START {max_id + 1}")
                        self.logger.info(
                            f"Created seq_researcher_id starting at {max_id + 1} for existing table"
                        )
                    # Note: We don't try to ALTER TABLE to add DEFAULT because DuckDB
                    # doesn't allow altering tables with FK dependencies. Instead,
                    # create_user() explicitly uses nextval('seq_researcher_id').
                else:
                    # New installation - create sequence starting at 1
                    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_researcher_id START 1")

                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS UserResearchers (
                            ResearcherID INTEGER PRIMARY KEY DEFAULT nextval('seq_researcher_id'),
                            Username VARCHAR NOT NULL UNIQUE,
                            DisplayName VARCHAR NOT NULL,
                            PasswordHash VARCHAR NOT NULL,
                            Role VARCHAR NOT NULL DEFAULT 'researcher',
                            IsActive BOOLEAN NOT NULL DEFAULT TRUE,
                            CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            LastLoginAt TIMESTAMP,
                            CreatedBy VARCHAR
                        )
                    """)

                self._table_ensured = True
                self.logger.debug("UserResearchers table ensured with sequence")
                return True

        except Exception as e:
            self.logger.error(f"Error ensuring UserResearchers table: {e}", exc_info=True)
            return False

    # --- Password Hashing ---

    @staticmethod
    def hash_password(password: str) -> str:
        """
        Hash a password using bcrypt with cost factor 12.

        Args:
            password: Plain text password

        Returns:
            Hashed password string
        """
        return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")

    @staticmethod
    def verify_password(password: str, password_hash: str) -> bool:
        """
        Verify a password against its hash.

        Args:
            password: Plain text password to verify
            password_hash: Stored bcrypt hash

        Returns:
            True if password matches, False otherwise
        """
        try:
            return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
        except Exception:
            return False

    # --- Authentication ---

    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticate a user by username and password.

        Args:
            username: User's username
            password: Plain text password

        Returns:
            User dict with id, username, display_name, role if successful, None otherwise
        """
        self.ensure_table_exists()
        user_data = None
        researcher_id = None

        try:
            # First: verify credentials with read-only connection
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = conn.execute(
                    """
                    SELECT ResearcherID, Username, DisplayName, PasswordHash, Role
                    FROM UserResearchers
                    WHERE Username = ? AND IsActive = TRUE
                    """,
                    [username],
                ).fetchone()

                if result and self.verify_password(password, result[3]):
                    researcher_id = result[0]
                    user_data = {
                        "id": result[0],
                        "username": result[1],
                        "display_name": result[2],
                        "role": result[4],
                    }

            # Second: update last login AFTER read connection is closed
            # This avoids DuckDB's "different configuration" error
            if researcher_id is not None:
                self._update_last_login(researcher_id)

            return user_data

        except Exception as e:
            self.logger.error(f"Authentication error: {e}", exc_info=True)
            return None

    def _update_last_login(self, researcher_id: int) -> None:
        """Update the last login timestamp for a user."""
        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                conn.execute(
                    """
                    UPDATE UserResearchers
                    SET LastLoginAt = ?
                    WHERE ResearcherID = ?
                    """,
                    [datetime.now(), researcher_id],
                )
        except Exception as e:
            self.logger.warning(f"Failed to update last login: {e}")

    # --- User Management ---

    def get_active_researchers(self) -> List[Dict[str, Any]]:
        """
        Get list of active researchers for login dropdown.

        Returns:
            List of dicts with username and display_name
        """
        self.ensure_table_exists()
        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = conn.execute(
                    """
                    SELECT Username, DisplayName
                    FROM UserResearchers
                    WHERE IsActive = TRUE
                    ORDER BY DisplayName
                    """
                ).fetchall()

                return [
                    {"username": row[0], "display_name": row[1]}
                    for row in result
                ]

        except Exception as e:
            self.logger.error(f"Error getting active researchers: {e}", exc_info=True)
            return []

    def get_all_users(self) -> pd.DataFrame:
        """
        Get all users (including inactive) for admin view.

        Returns:
            DataFrame with all user information (excluding password hash)
        """
        self.ensure_table_exists()
        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = safe_execute_query(
                    conn,
                    """
                    SELECT
                        ResearcherID,
                        Username,
                        DisplayName,
                        Role,
                        IsActive,
                        CreatedAt,
                        LastLoginAt,
                        CreatedBy
                    FROM UserResearchers
                    ORDER BY IsActive DESC, DisplayName
                    """,
                    self.logger,
                )
                return result if result is not None else pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error getting all users: {e}", exc_info=True)
            return pd.DataFrame()

    def get_user_by_id(self, researcher_id: int) -> Optional[Dict[str, Any]]:
        """Get a user by their ID."""
        self.ensure_table_exists()
        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = conn.execute(
                    """
                    SELECT ResearcherID, Username, DisplayName, Role, IsActive,
                           CreatedAt, LastLoginAt, CreatedBy
                    FROM UserResearchers
                    WHERE ResearcherID = ?
                    """,
                    [researcher_id],
                ).fetchone()

                if result:
                    return {
                        "id": result[0],
                        "username": result[1],
                        "display_name": result[2],
                        "role": result[3],
                        "is_active": result[4],
                        "created_at": result[5],
                        "last_login_at": result[6],
                        "created_by": result[7],
                    }
                return None

        except Exception as e:
            self.logger.error(f"Error getting user by ID: {e}", exc_info=True)
            return None

    def create_user(
        self,
        username: str,
        display_name: str,
        password: str,
        role: str = ROLE_RESEARCHER,
        created_by: Optional[str] = None,
    ) -> bool:
        """
        Create a new user account.

        Args:
            username: Unique username for login
            display_name: Display name shown in UI
            password: Plain text password (will be hashed)
            role: 'admin' or 'researcher'
            created_by: Username of admin who created this account

        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate inputs
            if not username or not display_name or not password:
                self.logger.error("Missing required fields for user creation")
                return False

            if role not in [self.ROLE_ADMIN, self.ROLE_RESEARCHER]:
                self.logger.error(f"Invalid role: {role}")
                return False

            if len(password) < 6:
                self.logger.error("Password too short (minimum 6 characters)")
                return False

            password_hash = self.hash_password(password)

            self.ensure_table_exists()
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                # Explicitly use nextval() for ResearcherID since we can't ALTER existing
                # tables with FK dependencies to add DEFAULT clause
                conn.execute(
                    """
                    INSERT INTO UserResearchers
                    (ResearcherID, Username, DisplayName, PasswordHash, Role, IsActive, CreatedAt, CreatedBy)
                    VALUES (nextval('seq_researcher_id'), ?, ?, ?, ?, TRUE, ?, ?)
                    """,
                    [username, display_name, password_hash, role, datetime.now(), created_by],
                )
                self.logger.info(f"Created user: {username} with role: {role}")
                return True

        except Exception as e:
            if "UNIQUE constraint" in str(e):
                self.logger.error(f"Username already exists: {username}")
            else:
                self.logger.error(f"Error creating user: {e}", exc_info=True)
            return False

    def delete_user(self, researcher_id: int) -> bool:
        """
        Soft delete a user (sets IsActive=FALSE).

        This preserves the audit trail - annotations keep the original AssignedBy value.

        Args:
            researcher_id: ID of user to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                conn.execute(
                    """
                    UPDATE UserResearchers
                    SET IsActive = FALSE
                    WHERE ResearcherID = ?
                    """,
                    [researcher_id],
                )
                self.logger.info(f"Soft deleted user ID: {researcher_id}")
                return True

        except Exception as e:
            self.logger.error(f"Error deleting user: {e}", exc_info=True)
            return False

    def reactivate_user(self, researcher_id: int) -> bool:
        """
        Reactivate a soft-deleted user.

        Args:
            researcher_id: ID of user to reactivate

        Returns:
            True if successful, False otherwise
        """
        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                conn.execute(
                    """
                    UPDATE UserResearchers
                    SET IsActive = TRUE
                    WHERE ResearcherID = ?
                    """,
                    [researcher_id],
                )
                self.logger.info(f"Reactivated user ID: {researcher_id}")
                return True

        except Exception as e:
            self.logger.error(f"Error reactivating user: {e}", exc_info=True)
            return False

    def reset_password(self, researcher_id: int, new_password: str) -> bool:
        """
        Reset a user's password.

        Args:
            researcher_id: ID of user
            new_password: New plain text password (will be hashed)

        Returns:
            True if successful, False otherwise
        """
        try:
            if len(new_password) < 6:
                self.logger.error("Password too short (minimum 6 characters)")
                return False

            password_hash = self.hash_password(new_password)

            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                conn.execute(
                    """
                    UPDATE UserResearchers
                    SET PasswordHash = ?
                    WHERE ResearcherID = ?
                    """,
                    [password_hash, researcher_id],
                )
                self.logger.info(f"Reset password for user ID: {researcher_id}")
                return True

        except Exception as e:
            self.logger.error(f"Error resetting password: {e}", exc_info=True)
            return False

    def update_role(self, researcher_id: int, new_role: str) -> bool:
        """
        Update a user's role.

        Args:
            researcher_id: ID of user
            new_role: New role ('admin' or 'researcher')

        Returns:
            True if successful, False otherwise
        """
        try:
            if new_role not in [self.ROLE_ADMIN, self.ROLE_RESEARCHER]:
                self.logger.error(f"Invalid role: {new_role}")
                return False

            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                conn.execute(
                    """
                    UPDATE UserResearchers
                    SET Role = ?
                    WHERE ResearcherID = ?
                    """,
                    [new_role, researcher_id],
                )
                self.logger.info(f"Updated role for user ID {researcher_id} to: {new_role}")
                return True

        except Exception as e:
            self.logger.error(f"Error updating role: {e}", exc_info=True)
            return False

    def update_display_name(self, researcher_id: int, new_display_name: str) -> bool:
        """
        Update a user's display name.

        Args:
            researcher_id: ID of user
            new_display_name: New display name

        Returns:
            True if successful, False otherwise
        """
        self.ensure_table_exists()
        try:
            if not new_display_name or not new_display_name.strip():
                self.logger.error("Display name cannot be empty")
                return False

            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                conn.execute(
                    """
                    UPDATE UserResearchers
                    SET DisplayName = ?
                    WHERE ResearcherID = ?
                    """,
                    [new_display_name.strip(), researcher_id],
                )
                self.logger.info(f"Updated display name for user ID {researcher_id}")
                return True

        except Exception as e:
            self.logger.error(f"Error updating display name: {e}", exc_info=True)
            return False

    def hard_delete_user(self, researcher_id: int) -> bool:
        """
        Permanently delete a user from the database.

        WARNING: This is irreversible! Use delete_user() for soft delete instead.
        Note: This will fail if the user has annotations to preserve data integrity.

        Args:
            researcher_id: ID of user to permanently delete

        Returns:
            True if successful, False otherwise
        """
        self.ensure_table_exists()

        # Silent notifier - we handle errors ourselves
        def _silent_notify(msg: str, level: str) -> None:
            self.logger.warning(f"Suppressed UI notification ({level}): {msg}")

        try:
            # Check annotation count first
            annotation_count = self.get_user_annotation_count(researcher_id)
            if annotation_count > 0:
                self.logger.warning(
                    f"Cannot hard delete user ID {researcher_id}: "
                    f"has {annotation_count} annotations. Use soft delete instead."
                )
                return False

            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger, ui_notify=_silent_notify
            ) as conn:
                conn.execute(
                    "DELETE FROM UserResearchers WHERE ResearcherID = ?",
                    [researcher_id],
                )
                self.logger.info(f"Permanently deleted user ID: {researcher_id}")
                return True

        except Exception as e:
            self.logger.error(f"Error hard deleting user: {e}", exc_info=True)
            return False

    def get_user_annotation_count(self, researcher_id: int) -> int:
        """Get the number of annotations made by a user."""
        import traceback

        self.logger.info(f"get_user_annotation_count called for researcher_id={researcher_id}")
        self.ensure_table_exists()

        # Silent notifier - we handle errors ourselves, don't display to user
        def _silent_notify(msg: str, level: str) -> None:
            self.logger.warning(f"Suppressed UI notification ({level}): {msg}")

        try:
            # Use read_only=False to ensure we see current catalog state
            # DuckDB's MVCC means read-only connections may see stale snapshots
            # that reference non-existent migration artifact tables
            # Use silent notifier to prevent error dialogs - we handle errors below
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger, ui_notify=_silent_notify
            ) as conn:
                self.logger.info("Checking if UserBillCAP table exists...")
                # Check if UserBillCAP table exists
                table_check = conn.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = 'UserBillCAP'"
                ).fetchone()

                if not table_check:
                    # No annotations table means no annotations
                    self.logger.info("UserBillCAP table does not exist, returning 0")
                    return 0

                self.logger.info("Querying annotation count...")
                result = conn.execute(
                    """
                    SELECT COUNT(*) FROM UserBillCAP
                    WHERE ResearcherID = ?
                    """,
                    [researcher_id],
                ).fetchone()
                self.logger.info(f"Query succeeded, count={result[0] if result else 0}")
                return result[0] if result else 0

        except Exception as e:
            error_str = str(e)
            # Log full traceback to understand where the error comes from
            self.logger.error(
                f"Error in get_user_annotation_count: {error_str}\n"
                f"Full traceback:\n{traceback.format_exc()}"
            )
            # If table doesn't exist, return 0
            if "does not exist" in error_str:
                # Special handling for migration artifact errors
                if "UserBillCAP_new" in error_str:
                    self.logger.error(
                        "CRITICAL: UserBillCAP_new reference detected! "
                        "This should not happen. Check database state."
                    )
                return 0
            return 0

    def user_exists(self, username: str) -> bool:
        """
        Check if a username already exists.

        Args:
            username: Username to check

        Returns:
            True if user exists or on error (fail secure), False if user doesn't exist
        """
        self.ensure_table_exists()
        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = conn.execute(
                    "SELECT 1 FROM UserResearchers WHERE Username = ?",
                    [username],
                ).fetchone()
                return result is not None

        except Exception as e:
            self.logger.error(f"Error checking user existence: {e}", exc_info=True)
            # Fail secure: if we can't check, assume user exists to prevent duplicates
            return True

    @staticmethod
    def validate_username(username: str) -> Optional[str]:
        """
        Validate username format.

        Args:
            username: Username to validate

        Returns:
            Error message if invalid, None if valid
        """
        import re

        if not username:
            return "Username is required"

        username = username.strip()

        if len(username) < 3:
            return "Username must be at least 3 characters"

        # Only allow alphanumeric and underscore
        if not re.match(r"^[a-zA-Z0-9_]+$", username):
            return "Username can only contain letters, numbers, and underscores"

        return None

    def create_user_with_validation(
        self,
        username: str,
        display_name: str,
        password: str,
        role: str,
        created_by: Optional[str] = None,
    ) -> tuple[Optional[int], Optional[str]]:
        """
        Create user with pre-validation checks.

        This method performs all validations before attempting database insertion,
        providing clear error messages instead of relying on DB constraint errors.

        Args:
            username: Unique username for login (alphanumeric + underscore, min 3 chars)
            display_name: Display name shown in UI (non-empty after strip)
            password: Plain text password (min 6 chars, will be hashed)
            role: 'admin' or 'researcher'
            created_by: Username of admin who created this account

        Returns:
            Tuple of (user_id, error_message).
            - On success: (user_id, None)
            - On error: (None, "Error description")
        """
        # Validate username format
        username_error = self.validate_username(username)
        if username_error:
            return None, username_error

        # Normalize username
        username = username.strip().lower()

        # Validate password
        if not password:
            return None, "Password is required"
        if len(password) < 6:
            return None, "Password must be at least 6 characters"

        # Validate display name
        if not display_name or not display_name.strip():
            return None, "Display name is required"

        # Validate role
        if role not in [self.ROLE_ADMIN, self.ROLE_RESEARCHER]:
            return None, f"Role must be '{self.ROLE_ADMIN}' or '{self.ROLE_RESEARCHER}'"

        # Check if username already exists (before attempting insert)
        if self.user_exists(username):
            return None, f"Username '{username}' already exists"

        # All validations passed - create the user
        password_hash = self.hash_password(password)

        try:
            self.ensure_table_exists()
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                # Insert user and return the new ID
                conn.execute(
                    """
                    INSERT INTO UserResearchers
                    (ResearcherID, Username, DisplayName, PasswordHash, Role, IsActive, CreatedAt, CreatedBy)
                    VALUES (nextval('seq_researcher_id'), ?, ?, ?, ?, TRUE, ?, ?)
                    """,
                    [username, display_name.strip(), password_hash, role, datetime.now(), created_by],
                )

                # Get the ID of the newly created user
                result = conn.execute(
                    "SELECT ResearcherID FROM UserResearchers WHERE Username = ?",
                    [username],
                ).fetchone()

                if result:
                    user_id = result[0]
                    self.logger.info(f"Created user: {username} with role: {role}")
                    return user_id, None
                else:
                    return None, "User created but ID could not be retrieved"

        except Exception as e:
            if "UNIQUE constraint" in str(e):
                # Race condition: another process created the user between our check and insert
                return None, f"Username '{username}' already exists"
            self.logger.error(f"Error creating user: {e}", exc_info=True)
            return None, f"Failed to create user: {str(e)}"

    def is_user_active(self, user_id: int) -> bool:
        """
        Check if a user is currently active.

        This is used to validate that a logged-in user hasn't been deactivated
        by an admin since their last login. If a user is deactivated, they
        should be logged out on their next request.

        Args:
            user_id: The user's database ID (ResearcherID)

        Returns:
            True if user exists and is active, False otherwise.
            Returns False on database errors (fail secure).
        """
        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = conn.execute(
                    """
                    SELECT IsActive FROM UserResearchers
                    WHERE ResearcherID = ?
                    """,
                    [user_id],
                ).fetchone()

                if result is None:
                    # User doesn't exist
                    return False

                return bool(result[0])

        except Exception as e:
            # Fail secure - if we can't check, assume inactive
            self.logger.error(f"Error checking user active status: {e}", exc_info=True)
            return False

    def get_user_count(self) -> int:
        """Get count of all users (for bootstrap check)."""
        self.ensure_table_exists()
        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                result = conn.execute(
                    "SELECT COUNT(*) FROM UserResearchers"
                ).fetchone()
                return result[0] if result else 0

        except Exception as e:
            self.logger.warning(f"Error getting user count: {e}")
            return 0

    def bootstrap_admin_from_secrets(self) -> bool:
        """
        Bootstrap an admin user from secrets.toml if no users exist.

        This is only called on first run when UserResearchers table is empty.
        After bootstrap, admin should change their password and create other users.

        Returns:
            True if admin was created or users already exist, False on error
        """
        try:
            # Check if users already exist
            if self.get_user_count() > 0:
                return True  # Already bootstrapped

            # Get bootstrap config from secrets (handle multiple formats)
            try:
                # Try standard section format first: [cap_annotation]
                cap_secrets = st.secrets.get("cap_annotation", {})

                # If empty, try dotted key format: cap_annotation.enabled
                if not cap_secrets:
                    # Check if secrets has dotted keys directly
                    all_secrets = dict(st.secrets)
                    cap_secrets = {
                        k.replace("cap_annotation.", ""): v
                        for k, v in all_secrets.items()
                        if k.startswith("cap_annotation.")
                    }

                username = cap_secrets.get("bootstrap_admin_username", "admin")
                display_name = cap_secrets.get("bootstrap_admin_display_name", "Administrator")
                password = cap_secrets.get("bootstrap_admin_password")

                # Fall back to legacy password field
                if not password:
                    password = cap_secrets.get("password")

                if not password:
                    self.logger.warning("No bootstrap password configured - skipping admin creation")
                    self.logger.debug(f"Available secrets keys: {list(st.secrets.keys())}")
                    return False

            except Exception as e:
                self.logger.warning(f"Could not read secrets for bootstrap: {e}")
                return False

            # Create the bootstrap admin
            success = self.create_user(
                username=username,
                display_name=display_name,
                password=password,
                role=self.ROLE_ADMIN,
                created_by="System Bootstrap",
            )

            if success:
                self.logger.info(f"Bootstrap admin '{username}' created successfully")
            return success

        except Exception as e:
            self.logger.error(f"Error bootstrapping admin: {e}", exc_info=True)
            return False


def get_user_service(
    db_path: Path, logger_obj: Optional[logging.Logger] = None
) -> CAPUserService:
    """Factory function to get a CAP user service instance."""
    return CAPUserService(db_path, logger_obj)

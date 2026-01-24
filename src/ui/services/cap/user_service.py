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
        Ensure the UserResearchers table exists.

        This is called automatically before any database queries to handle
        the case where the table hasn't been created yet.

        Returns:
            True if table exists or was created, False on error
        """
        if self._table_ensured:
            return True

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS UserResearchers (
                        ResearcherID INTEGER PRIMARY KEY,
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
                self.logger.debug("UserResearchers table ensured")
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
        try:
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
                    # Update last login time
                    self._update_last_login(result[0])
                    return {
                        "id": result[0],
                        "username": result[1],
                        "display_name": result[2],
                        "role": result[4],
                    }

                return None

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
                # DuckDB doesn't auto-increment INTEGER PRIMARY KEY, so we compute next ID
                conn.execute(
                    """
                    INSERT INTO UserResearchers
                    (ResearcherID, Username, DisplayName, PasswordHash, Role, IsActive, CreatedAt, CreatedBy)
                    VALUES (
                        (SELECT COALESCE(MAX(ResearcherID), 0) + 1 FROM UserResearchers),
                        ?, ?, ?, ?, TRUE, ?, ?
                    )
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
                self.db_path, read_only=False, logger_obj=self.logger
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
        self.ensure_table_exists()
        try:
            with get_db_connection(
                self.db_path, read_only=True, logger_obj=self.logger
            ) as conn:
                # Check if UserBillCAP table exists
                table_check = conn.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = 'UserBillCAP'"
                ).fetchone()

                if not table_check:
                    # No annotations table means no annotations
                    return 0

                result = conn.execute(
                    """
                    SELECT COUNT(*) FROM UserBillCAP
                    WHERE ResearcherID = ?
                    """,
                    [researcher_id],
                ).fetchone()
                return result[0] if result else 0

        except Exception as e:
            # If table doesn't exist, return 0
            if "does not exist" in str(e):
                return 0
            self.logger.error(f"Error getting annotation count: {e}", exc_info=True)
            return 0

    def user_exists(self, username: str) -> bool:
        """Check if a username already exists."""
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

            # Get bootstrap config from secrets
            try:
                cap_secrets = st.secrets.get("cap_annotation", {})
                username = cap_secrets.get("bootstrap_admin_username", "admin")
                display_name = cap_secrets.get("bootstrap_admin_display_name", "Administrator")
                password = cap_secrets.get("bootstrap_admin_password")

                # Fall back to legacy password field
                if not password:
                    password = cap_secrets.get("password")

                if not password:
                    self.logger.warning("No bootstrap password configured - skipping admin creation")
                    return False

            except Exception:
                self.logger.warning("Could not read secrets for bootstrap")
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

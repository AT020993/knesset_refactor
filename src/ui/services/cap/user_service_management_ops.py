"""User account management operations for CAPUserService."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import pandas as pd
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query
from . import user_service_auth_ops as auth_ops


def get_active_researchers(service: Any) -> list[dict[str, Any]]:
    """Get active users for login dropdown."""
    service.ensure_table_exists()
    try:
        with get_db_connection(
            service.db_path, read_only=True, logger_obj=service.logger
        ) as conn:
            result = conn.execute(
                """
                SELECT Username, DisplayName
                FROM UserResearchers
                WHERE IsActive = TRUE
                ORDER BY DisplayName
                """
            ).fetchall()
            return [{"username": row[0], "display_name": row[1]} for row in result]
    except Exception as exc:
        service.logger.error(f"Error getting active researchers: {exc}", exc_info=True)
        return []


def get_all_users(service: Any) -> pd.DataFrame:
    """Get all users for admin view."""
    service.ensure_table_exists()
    try:
        with get_db_connection(
            service.db_path, read_only=True, logger_obj=service.logger
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
                service.logger,
            )
            return result if result is not None else pd.DataFrame()
    except Exception as exc:
        service.logger.error(f"Error getting all users: {exc}", exc_info=True)
        return pd.DataFrame()


def get_user_by_id(service: Any, researcher_id: int) -> Optional[dict[str, Any]]:
    """Get user record by ID."""
    service.ensure_table_exists()
    try:
        with get_db_connection(
            service.db_path, read_only=True, logger_obj=service.logger
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

            if not result:
                return None
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
    except Exception as exc:
        service.logger.error(f"Error getting user by ID: {exc}", exc_info=True)
        return None


def create_user(
    service: Any,
    username: str,
    display_name: str,
    password: str,
    role: str,
    created_by: Optional[str],
) -> bool:
    """Create a new user."""
    try:
        if not username or not display_name or not password:
            service.logger.error("Missing required fields for user creation")
            return False

        if role not in [service.ROLE_ADMIN, service.ROLE_RESEARCHER]:
            service.logger.error(f"Invalid role: {role}")
            return False

        password_error = auth_ops.validate_password_strength(
            password, service.MIN_PASSWORD_LENGTH
        )
        if password_error:
            service.logger.error(f"Password validation failed: {password_error}")
            return False

        password_hash = auth_ops.hash_password(password)

        service.ensure_table_exists()
        with get_db_connection(
            service.db_path, read_only=False, logger_obj=service.logger
        ) as conn:
            conn.execute(
                """
                INSERT INTO UserResearchers
                (ResearcherID, Username, DisplayName, PasswordHash, Role, IsActive, CreatedAt, CreatedBy)
                VALUES (nextval('seq_researcher_id'), ?, ?, ?, ?, TRUE, ?, ?)
                """,
                [username, display_name, password_hash, role, datetime.now(), created_by],
            )
            service.logger.info(f"Created user: {username} with role: {role}")
            return True
    except Exception as exc:
        if "UNIQUE constraint" in str(exc):
            service.logger.error(f"Username already exists: {username}")
        else:
            service.logger.error(f"Error creating user: {exc}", exc_info=True)
        return False


def delete_user(service: Any, researcher_id: int) -> bool:
    """Soft-delete user (mark inactive)."""
    try:
        with get_db_connection(
            service.db_path, read_only=False, logger_obj=service.logger
        ) as conn:
            conn.execute(
                """
                UPDATE UserResearchers
                SET IsActive = FALSE
                WHERE ResearcherID = ?
                """,
                [researcher_id],
            )
            service.logger.info(f"Soft deleted user ID: {researcher_id}")
            return True
    except Exception as exc:
        service.logger.error(f"Error deleting user: {exc}", exc_info=True)
        return False


def reactivate_user(service: Any, researcher_id: int) -> bool:
    """Reactivate user."""
    try:
        with get_db_connection(
            service.db_path, read_only=False, logger_obj=service.logger
        ) as conn:
            conn.execute(
                """
                UPDATE UserResearchers
                SET IsActive = TRUE
                WHERE ResearcherID = ?
                """,
                [researcher_id],
            )
            service.logger.info(f"Reactivated user ID: {researcher_id}")
            return True
    except Exception as exc:
        service.logger.error(f"Error reactivating user: {exc}", exc_info=True)
        return False


def reset_password(service: Any, researcher_id: int, new_password: str) -> bool:
    """Reset user password."""
    try:
        password_error = auth_ops.validate_password_strength(
            new_password, service.MIN_PASSWORD_LENGTH
        )
        if password_error:
            service.logger.error(f"Password validation failed: {password_error}")
            return False

        password_hash = auth_ops.hash_password(new_password)

        with get_db_connection(
            service.db_path, read_only=False, logger_obj=service.logger
        ) as conn:
            conn.execute(
                """
                UPDATE UserResearchers
                SET PasswordHash = ?
                WHERE ResearcherID = ?
                """,
                [password_hash, researcher_id],
            )
            service.logger.info(f"Reset password for user ID: {researcher_id}")
            return True
    except Exception as exc:
        service.logger.error(f"Error resetting password: {exc}", exc_info=True)
        return False


def update_role(service: Any, researcher_id: int, new_role: str) -> bool:
    """Update user role."""
    try:
        if new_role not in [service.ROLE_ADMIN, service.ROLE_RESEARCHER]:
            service.logger.error(f"Invalid role: {new_role}")
            return False

        with get_db_connection(
            service.db_path, read_only=False, logger_obj=service.logger
        ) as conn:
            conn.execute(
                """
                UPDATE UserResearchers
                SET Role = ?
                WHERE ResearcherID = ?
                """,
                [new_role, researcher_id],
            )
            service.logger.info(
                f"Updated role for user ID {researcher_id} to: {new_role}"
            )
            return True
    except Exception as exc:
        service.logger.error(f"Error updating role: {exc}", exc_info=True)
        return False


def update_display_name(service: Any, researcher_id: int, new_display_name: str) -> bool:
    """Update display name."""
    service.ensure_table_exists()
    try:
        if not new_display_name or not new_display_name.strip():
            service.logger.error("Display name cannot be empty")
            return False

        with get_db_connection(
            service.db_path, read_only=False, logger_obj=service.logger
        ) as conn:
            conn.execute(
                """
                UPDATE UserResearchers
                SET DisplayName = ?
                WHERE ResearcherID = ?
                """,
                [new_display_name.strip(), researcher_id],
            )
            service.logger.info(f"Updated display name for user ID {researcher_id}")
            return True
    except Exception as exc:
        service.logger.error(f"Error updating display name: {exc}", exc_info=True)
        return False


def user_exists(service: Any, username: str) -> bool:
    """Check if username exists."""
    service.ensure_table_exists()
    try:
        with get_db_connection(
            service.db_path, read_only=True, logger_obj=service.logger
        ) as conn:
            result = conn.execute(
                "SELECT 1 FROM UserResearchers WHERE Username = ?",
                [username],
            ).fetchone()
            return result is not None
    except Exception as exc:
        service.logger.error(f"Error checking user existence: {exc}", exc_info=True)
        return True


def validate_username(username: str) -> Optional[str]:
    """Validate username format."""
    import re

    if not username:
        return "Username is required"

    username = username.strip()
    if len(username) < 3:
        return "Username must be at least 3 characters"

    if not re.match(r"^[a-zA-Z0-9_]+$", username):
        return "Username can only contain letters, numbers, and underscores"
    return None


def create_user_with_validation(
    service: Any,
    username: str,
    display_name: str,
    password: str,
    role: str,
    created_by: Optional[str],
) -> tuple[Optional[int], Optional[str]]:
    """Create user with pre-validation and explicit errors."""
    username_error = validate_username(username)
    if username_error:
        return None, username_error

    username = username.strip().lower()

    password_error = auth_ops.validate_password_strength(
        password, service.MIN_PASSWORD_LENGTH
    )
    if password_error:
        return None, password_error

    if not display_name or not display_name.strip():
        return None, "Display name is required"

    if role not in [service.ROLE_ADMIN, service.ROLE_RESEARCHER]:
        return None, f"Role must be '{service.ROLE_ADMIN}' or '{service.ROLE_RESEARCHER}'"

    if user_exists(service, username):
        return None, f"Username '{username}' already exists"

    password_hash = auth_ops.hash_password(password)

    try:
        service.ensure_table_exists()
        with get_db_connection(
            service.db_path, read_only=False, logger_obj=service.logger
        ) as conn:
            conn.execute(
                """
                INSERT INTO UserResearchers
                (ResearcherID, Username, DisplayName, PasswordHash, Role, IsActive, CreatedAt, CreatedBy)
                VALUES (nextval('seq_researcher_id'), ?, ?, ?, ?, TRUE, ?, ?)
                """,
                [
                    username,
                    display_name.strip(),
                    password_hash,
                    role,
                    datetime.now(),
                    created_by,
                ],
            )

            result = conn.execute(
                "SELECT ResearcherID FROM UserResearchers WHERE Username = ?",
                [username],
            ).fetchone()

            if result:
                user_id = result[0]
                service.logger.info(f"Created user: {username} with role: {role}")
                return user_id, None
            return None, "User created but ID could not be retrieved"
    except Exception as exc:
        if "UNIQUE constraint" in str(exc):
            return None, f"Username '{username}' already exists"
        service.logger.error(f"Error creating user: {exc}", exc_info=True)
        return None, f"Failed to create user: {str(exc)}"


def is_user_active(service: Any, user_id: int) -> bool:
    """Check if user is active."""
    try:
        with get_db_connection(
            service.db_path, read_only=True, logger_obj=service.logger
        ) as conn:
            result = conn.execute(
                """
                SELECT IsActive FROM UserResearchers
                WHERE ResearcherID = ?
                """,
                [user_id],
            ).fetchone()

            if result is None:
                return False
            return bool(result[0])
    except Exception as exc:
        service.logger.error(f"Error checking user active status: {exc}", exc_info=True)
        return False


def get_user_count(service: Any) -> int:
    """Count users in table."""
    service.ensure_table_exists()
    try:
        with get_db_connection(
            service.db_path, read_only=True, logger_obj=service.logger
        ) as conn:
            result = conn.execute("SELECT COUNT(*) FROM UserResearchers").fetchone()
            return result[0] if result else 0
    except Exception as exc:
        service.logger.warning(f"Error getting user count: {exc}")
        return 0


def bootstrap_admin_from_secrets(service: Any) -> bool:
    """Bootstrap admin from Streamlit secrets on first run."""
    try:
        if get_user_count(service) > 0:
            return True

        try:
            cap_secrets = st.secrets.get("cap_annotation", {})
            if not cap_secrets:
                all_secrets = dict(st.secrets)
                cap_secrets = {
                    k.replace("cap_annotation.", ""): v
                    for k, v in all_secrets.items()
                    if k.startswith("cap_annotation.")
                }

            username = cap_secrets.get("bootstrap_admin_username", "admin")
            display_name = cap_secrets.get(
                "bootstrap_admin_display_name", "Administrator"
            )
            password = cap_secrets.get("bootstrap_admin_password")
            if not password:
                password = cap_secrets.get("password")

            if not password:
                service.logger.warning(
                    "No bootstrap password configured - skipping admin creation"
                )
                service.logger.debug(f"Available secrets keys: {list(st.secrets.keys())}")
                return False
        except Exception as exc:
            service.logger.warning(f"Could not read secrets for bootstrap: {exc}")
            return False

        success = create_user(
            service=service,
            username=username,
            display_name=display_name,
            password=password,
            role=service.ROLE_ADMIN,
            created_by="System Bootstrap",
        )
        if success:
            service.logger.info(f"Bootstrap admin '{username}' created successfully")
        return success
    except Exception as exc:
        service.logger.error(f"Error bootstrapping admin: {exc}", exc_info=True)
        return False

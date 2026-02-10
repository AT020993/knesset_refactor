"""
CAP User Service facade.

This module keeps the public CAPUserService API stable while delegating
implementation details to focused operation modules.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from backend.connection_manager import get_db_connection
from . import user_service_auth_ops as auth_ops
from . import user_service_catalog_ops as catalog_ops
from . import user_service_management_ops as management_ops


class CAPUserService:
    """Service for managing CAP researcher authentication and user accounts."""

    ROLE_ADMIN = "admin"
    ROLE_RESEARCHER = "researcher"

    MIN_PASSWORD_LENGTH = 8
    PASSWORD_REQUIREMENTS = (
        "Password must be at least 8 characters and contain: "
        "uppercase letter, lowercase letter, and a digit"
    )

    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)
        self._table_ensured = False

    def ensure_table_exists(self) -> bool:
        """Ensure UserResearchers table and sequence exist."""
        if self._table_ensured:
            return True

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as conn:
                table_exists = conn.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = 'UserResearchers'"
                ).fetchone()

                if table_exists:
                    seq_exists = conn.execute(
                        "SELECT 1 FROM duckdb_sequences() WHERE sequence_name = 'seq_researcher_id'"
                    ).fetchone()

                    if not seq_exists:
                        row = conn.execute(
                            "SELECT COALESCE(MAX(ResearcherID), 0) FROM UserResearchers"
                        ).fetchone()
                        max_id = int(row[0]) if row else 0
                        conn.execute(
                            f"CREATE SEQUENCE seq_researcher_id START {max_id + 1}"
                        )
                        self.logger.info(
                            f"Created seq_researcher_id starting at {max_id + 1} for existing table"
                        )
                else:
                    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_researcher_id START 1")
                    conn.execute(
                        """
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
                        """
                    )

                self._table_ensured = True
                self.logger.debug("UserResearchers table ensured with sequence")
                return True
        except Exception as exc:
            self.logger.error(
                f"Error ensuring UserResearchers table: {exc}", exc_info=True
            )
            return False

    @staticmethod
    def hash_password(password: str) -> str:
        return auth_ops.hash_password(password)

    @staticmethod
    def verify_password(password: str, password_hash: str) -> bool:
        return auth_ops.verify_password(password, password_hash)

    @classmethod
    def validate_password_strength(cls, password: str) -> Optional[str]:
        return auth_ops.validate_password_strength(password, cls.MIN_PASSWORD_LENGTH)

    def authenticate(self, username: str, password: str) -> Optional[dict[str, Any]]:
        return auth_ops.authenticate(self, username, password)

    def _update_last_login(self, researcher_id: int) -> None:
        auth_ops.update_last_login(self, researcher_id)

    def get_active_researchers(self) -> list[dict[str, Any]]:
        return management_ops.get_active_researchers(self)

    def get_all_users(self) -> pd.DataFrame:
        return management_ops.get_all_users(self)

    def get_user_by_id(self, researcher_id: int) -> Optional[dict[str, Any]]:
        return management_ops.get_user_by_id(self, researcher_id)

    def create_user(
        self,
        username: str,
        display_name: str,
        password: str,
        role: str = ROLE_RESEARCHER,
        created_by: Optional[str] = None,
    ) -> bool:
        return management_ops.create_user(
            self,
            username=username,
            display_name=display_name,
            password=password,
            role=role,
            created_by=created_by,
        )

    def delete_user(self, researcher_id: int) -> bool:
        return management_ops.delete_user(self, researcher_id)

    def reactivate_user(self, researcher_id: int) -> bool:
        return management_ops.reactivate_user(self, researcher_id)

    def reset_password(self, researcher_id: int, new_password: str) -> bool:
        return management_ops.reset_password(self, researcher_id, new_password)

    def update_role(self, researcher_id: int, new_role: str) -> bool:
        return management_ops.update_role(self, researcher_id, new_role)

    def update_display_name(self, researcher_id: int, new_display_name: str) -> bool:
        return management_ops.update_display_name(self, researcher_id, new_display_name)

    def hard_delete_user(self, researcher_id: int) -> bool:
        return catalog_ops.hard_delete_user(self, researcher_id)

    def _rebuild_database_catalog(self, researcher_id_to_delete: int) -> bool:
        return catalog_ops.rebuild_database_catalog(self, researcher_id_to_delete)

    def get_user_annotation_count(self, researcher_id: int) -> int:
        return catalog_ops.get_user_annotation_count(self, researcher_id)

    def user_exists(self, username: str) -> bool:
        return management_ops.user_exists(self, username)

    @staticmethod
    def validate_username(username: str) -> Optional[str]:
        return management_ops.validate_username(username)

    def create_user_with_validation(
        self,
        username: str,
        display_name: str,
        password: str,
        role: str,
        created_by: Optional[str] = None,
    ) -> tuple[Optional[int], Optional[str]]:
        return management_ops.create_user_with_validation(
            self,
            username=username,
            display_name=display_name,
            password=password,
            role=role,
            created_by=created_by,
        )

    def is_user_active(self, user_id: int) -> bool:
        return management_ops.is_user_active(self, user_id)

    def get_user_count(self) -> int:
        return management_ops.get_user_count(self)

    def bootstrap_admin_from_secrets(self) -> bool:
        return management_ops.bootstrap_admin_from_secrets(self)


def get_user_service(
    db_path: Path, logger_obj: Optional[logging.Logger] = None
) -> CAPUserService:
    """Factory function to get a CAP user service instance."""
    return CAPUserService(db_path, logger_obj)

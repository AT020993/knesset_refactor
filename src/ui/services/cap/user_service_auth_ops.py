"""Authentication and password operations for CAPUserService."""

from __future__ import annotations

import base64
from datetime import datetime
import hashlib
import hmac
import secrets
from typing import Any, Optional

try:
    import bcrypt  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - fallback path depends on env
    bcrypt = None

from backend.connection_manager import get_db_connection

_FALLBACK_PREFIX = "$2b$12$pbkdf2$"
_FALLBACK_ITERATIONS = 200_000
_FALLBACK_SALT_BYTES = 16
_FALLBACK_DKLEN = 32


def _b64_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64_decode(encoded: str) -> bytes:
    padding = "=" * ((4 - len(encoded) % 4) % 4)
    return base64.urlsafe_b64decode(encoded + padding)


def _hash_password_fallback(password: str) -> str:
    """Fallback hasher when optional bcrypt dependency is unavailable."""
    salt = secrets.token_bytes(_FALLBACK_SALT_BYTES)
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        _FALLBACK_ITERATIONS,
        dklen=_FALLBACK_DKLEN,
    )
    return f"{_FALLBACK_PREFIX}{_b64_encode(salt)}${_b64_encode(derived)}"


def _verify_password_fallback(password: str, password_hash: str) -> bool:
    if not password_hash.startswith(_FALLBACK_PREFIX):
        return False

    payload = password_hash[len(_FALLBACK_PREFIX) :]
    parts = payload.split("$", maxsplit=1)
    if len(parts) != 2:
        return False

    try:
        salt = _b64_decode(parts[0])
        expected = _b64_decode(parts[1])
    except Exception:
        return False

    actual = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        _FALLBACK_ITERATIONS,
        dklen=len(expected),
    )
    return hmac.compare_digest(actual, expected)


def hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    if bcrypt is not None:
        hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12))
        if isinstance(hashed, (bytes, bytearray)):
            return bytes(hashed).decode("utf-8")
        return str(hashed)
    return _hash_password_fallback(password)


def verify_password(password: str, password_hash: str) -> bool:
    """Verify password against hash."""
    if _verify_password_fallback(password, password_hash):
        return True
    if bcrypt is None:
        return False

    try:
        return bool(
            bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
        )
    except Exception:
        return False


def validate_password_strength(password: str, min_length: int) -> Optional[str]:
    """Validate password meets policy requirements."""
    if not password:
        return "Password is required"

    if len(password) < min_length:
        return f"Password must be at least {min_length} characters"

    has_upper = any(c.isupper() for c in password)
    has_lower = any(c.islower() for c in password)
    has_digit = any(c.isdigit() for c in password)

    if not has_upper:
        return "Password must contain at least one uppercase letter"
    if not has_lower:
        return "Password must contain at least one lowercase letter"
    if not has_digit:
        return "Password must contain at least one digit"

    return None


def authenticate(service: Any, username: str, password: str) -> Optional[dict[str, Any]]:
    """Authenticate a user and update last login timestamp."""
    service.ensure_table_exists()
    user_data: Optional[dict[str, Any]] = None
    researcher_id: Optional[int] = None

    try:
        with get_db_connection(
            service.db_path, read_only=True, logger_obj=service.logger
        ) as conn:
            result = conn.execute(
                """
                SELECT ResearcherID, Username, DisplayName, PasswordHash, Role
                FROM UserResearchers
                WHERE Username = ? AND IsActive = TRUE
                """,
                [username],
            ).fetchone()

            if result and verify_password(password, result[3]):
                researcher_id = result[0]
                user_data = {
                    "id": result[0],
                    "username": result[1],
                    "display_name": result[2],
                    "role": result[4],
                }

        if researcher_id is not None:
            update_last_login(service, researcher_id)

        return user_data
    except Exception as exc:
        service.logger.error(f"Authentication error: {exc}", exc_info=True)
        return None


def update_last_login(service: Any, researcher_id: int) -> None:
    """Update user login timestamp."""
    try:
        with get_db_connection(
            service.db_path, read_only=False, logger_obj=service.logger
        ) as conn:
            conn.execute(
                """
                UPDATE UserResearchers
                SET LastLoginAt = ?
                WHERE ResearcherID = ?
                """,
                [datetime.now(), researcher_id],
            )
    except Exception as exc:
        service.logger.warning(f"Failed to update last login: {exc}")

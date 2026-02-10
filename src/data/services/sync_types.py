"""Typed contracts for local/cloud synchronization flows."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SyncDirection(str, Enum):
    """Direction selected for a sync operation."""

    LOCAL_TO_CLOUD = "local_to_cloud"
    CLOUD_TO_LOCAL = "cloud_to_local"
    NONE = "none"


@dataclass(frozen=True)
class SyncMetadata:
    """Metadata observed while deciding sync behavior."""

    local_exists: bool
    cloud_exists: bool
    freshness_state: str


@dataclass(frozen=True)
class SyncReport:
    """Result report for a completed sync operation."""

    success: bool
    direction: SyncDirection
    message: str
    details: dict[str, Any] = field(default_factory=dict)

"""Manifest dataclass and atomic read/write for the snapshot directory.

The manifest is the cross-file consistency contract: readers observe only
``manifest.json`` that names existing, complete Parquet files, because the
exporter writes all Parquet files first and only then replaces the manifest
with a fresh version.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

MANIFEST_VERSION = 1


@dataclass(frozen=True)
class SnapshotEntry:
    """Per-file entry inside the manifest."""

    rows: int
    sha256: str
    bytes: int


@dataclass(frozen=True)
class Manifest:
    """Top-level manifest for a snapshot directory."""

    version: int
    generated_at_utc: str
    warehouse_mtime_utc: str
    snapshots: dict[str, SnapshotEntry] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generated_at_utc": self.generated_at_utc,
            "warehouse_mtime_utc": self.warehouse_mtime_utc,
            "snapshots": {name: asdict(entry) for name, entry in self.snapshots.items()},
        }


def write_manifest(path: Path, manifest: Manifest) -> None:
    """Write ``manifest.json`` atomically (``<path>.new`` → rename)."""
    tmp = path.with_suffix(path.suffix + ".new")
    tmp.write_text(json.dumps(manifest.to_json(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def read_manifest(path: Path) -> Manifest:
    """Load a manifest from disk. Callers can rely on the returned snapshot
    entries pointing at existing files because of the atomic write protocol."""
    data = json.loads(path.read_text(encoding="utf-8"))
    return Manifest(
        version=int(data["version"]),
        generated_at_utc=str(data["generated_at_utc"]),
        warehouse_mtime_utc=str(data["warehouse_mtime_utc"]),
        snapshots={
            name: SnapshotEntry(rows=int(e["rows"]), sha256=str(e["sha256"]), bytes=int(e["bytes"]))
            for name, e in data.get("snapshots", {}).items()
        },
    )

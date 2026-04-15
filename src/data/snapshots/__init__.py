"""Pre-shaped Parquet snapshots of the warehouse.

This package is the **contract layer** between the ETL/warehouse side of
knesset_refactor and downstream consumers (FastAPI, analytics, exporters):
callers never import the warehouse or run SQL — they read Parquet files
described by a sibling ``manifest.json``.

Atomic write protocol:
    Every snapshot file is written as ``<name>.parquet.new`` and then
    POSIX-renamed to ``<name>.parquet``. ``manifest.json`` is written
    last via the same protocol; it is the commit marker that pins all
    the individual snapshots to a single warehouse read.
"""

from .manifest import Manifest, SnapshotEntry, read_manifest, write_manifest

__all__ = ["Manifest", "SnapshotEntry", "read_manifest", "write_manifest"]

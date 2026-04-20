#!/usr/bin/env python
"""Byte-level snapshot regression check for dependency upgrades.

Use this before merging Dependabot PRs that bump pandas / pyarrow /
fastparquet — any library that affects parquet encoding. The snapshot
bundle is a v2.0.0-pinned contract consumed by ``knesset-platform``,
and its byte-idempotence guarantee (same warehouse → same parquets)
must be preserved across dep upgrades.

Workflow
========

    # 1) Take a baseline on CURRENT deps
    scripts/check_snapshot_regression.py baseline

    # 2) Upgrade the dep under test
    pip install pandas==3.0.2        # or whatever PR proposes

    # 3) Compare against baseline
    scripts/check_snapshot_regression.py compare

    # 4) Restore
    pip install -r requirements.txt

Output
======

For each of the 7 snapshot parquets + manifest.json, reports:
  - BYTE-IDENTICAL → safe upgrade
  - ROWS-IDENTICAL but bytes differ → metadata-only change (probably OK)
  - ROWS DIFFER → BLOCK merge, the dep change is logically observable

Baseline is stored at ``/tmp/knesset_snapshot_baseline/``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_BASELINE_DIR = Path("/tmp/knesset_snapshot_baseline")
_CANDIDATE_DIR = Path("/tmp/knesset_snapshot_candidate")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _run_exporter(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"  Exporting snapshot bundle → {output_dir}")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "data.snapshots.exporter",
            "--warehouse",
            str(_REPO_ROOT / "data" / "warehouse.duckdb"),
            "--output-dir",
            str(output_dir),
        ],
        cwd=_REPO_ROOT,
        env={"PYTHONPATH": str(_REPO_ROOT / "src"), "PATH": subprocess.os.environ["PATH"]},
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  EXPORTER FAILED (exit {result.returncode}):\n{result.stderr}", file=sys.stderr)
        raise SystemExit(2)


def _row_count(path: Path) -> int | None:
    if path.suffix != ".parquet":
        return None
    try:
        import pyarrow.parquet as pq

        return pq.read_metadata(str(path)).num_rows
    except Exception as exc:  # noqa: BLE001
        print(f"  Could not read row count for {path.name}: {exc}", file=sys.stderr)
        return None


def _rows_equal(a: Path, b: Path) -> bool:
    """Compare parquet row-level contents via pandas."""
    import pandas as pd

    df_a = pd.read_parquet(a)
    df_b = pd.read_parquet(b)
    if df_a.shape != df_b.shape:
        return False
    try:
        return df_a.equals(df_b)
    except Exception:
        return False


def baseline() -> int:
    if _BASELINE_DIR.exists():
        shutil.rmtree(_BASELINE_DIR)
    _run_exporter(_BASELINE_DIR)
    files = sorted(_BASELINE_DIR.glob("*"))
    print(f"\nBaseline captured: {len(files)} files in {_BASELINE_DIR}")
    for f in files:
        print(f"  {f.name:40s} sha256={_sha256(f)[:16]}…  {f.stat().st_size:>12,} bytes")
    print("\nNext: upgrade the dep, then run `scripts/check_snapshot_regression.py compare`")
    return 0


def compare() -> int:
    if not _BASELINE_DIR.exists():
        print(f"No baseline at {_BASELINE_DIR}. Run `baseline` first.", file=sys.stderr)
        return 1
    if _CANDIDATE_DIR.exists():
        shutil.rmtree(_CANDIDATE_DIR)
    _run_exporter(_CANDIDATE_DIR)

    baseline_files = {f.name: f for f in _BASELINE_DIR.iterdir()}
    candidate_files = {f.name: f for f in _CANDIDATE_DIR.iterdir()}
    all_names = sorted(set(baseline_files) | set(candidate_files))

    rows = []
    any_row_diff = False
    any_byte_diff = False
    for name in all_names:
        b = baseline_files.get(name)
        c = candidate_files.get(name)
        if b is None:
            rows.append((name, "NEW FILE (only in candidate)", ""))
            continue
        if c is None:
            rows.append((name, "MISSING (only in baseline)", ""))
            continue
        if _sha256(b) == _sha256(c):
            rows.append((name, "BYTE-IDENTICAL", ""))
            continue
        # Bytes differ. For parquet: compare row counts + row contents.
        # (any_byte_diff is only set below when the diff is a real concern —
        # a manifest.json whose only difference is generated_at_utc does not
        # count as byte-drift for the upgrade-safety verdict.)
        if name.endswith(".parquet"):
            any_byte_diff = True
            br, cr = _row_count(b), _row_count(c)
            rows_ok = br == cr and _rows_equal(b, c)
            if rows_ok:
                rows.append((name, "METADATA-ONLY (rows identical)", f"{br} rows"))
            else:
                any_row_diff = True
                rows.append((name, "ROWS DIFFER", f"baseline={br} candidate={cr}"))
        else:
            # manifest.json: show a diff summary, ignoring known-volatile fields
            # (``generated_at_utc`` is a wall-clock stamp that changes every run).
            volatile = {"generated_at_utc"}
            try:
                bj = json.loads(b.read_text())
                cj = json.loads(c.read_text())
                bj_stable = {k: v for k, v in bj.items() if k not in volatile}
                cj_stable = {k: v for k, v in cj.items() if k not in volatile}
                added = set(cj_stable) - set(bj_stable)
                removed = set(bj_stable) - set(cj_stable)
                changed = {k for k in set(bj_stable) & set(cj_stable)
                           if bj_stable[k] != cj_stable[k]}
                if not (added or removed or changed):
                    rows.append((name, "STABLE-FIELDS-IDENTICAL",
                                 "(only generated_at_utc differs)"))
                else:
                    any_byte_diff = True
                    any_row_diff = True  # real content change in manifest → block
                    rows.append((name, "MANIFEST CONTENT DIFFERS",
                                 f"+{len(added)} -{len(removed)} ~{len(changed)}"))
            except Exception:
                any_byte_diff = True
                rows.append((name, "BYTES DIFFER (not JSON)", ""))

    print("\n" + "=" * 78)
    print(f"{'File':<42s} {'Status':<32s} {'Detail'}")
    print("=" * 78)
    for name, status, detail in rows:
        print(f"{name:<42s} {status:<32s} {detail}")

    print("\n" + "=" * 78)
    if any_row_diff:
        print("🔴 ROW-LEVEL DIFFERENCES DETECTED — do NOT merge the dep upgrade.")
        return 2
    if any_byte_diff:
        print("🟡 Byte differences but rows match — metadata-only change.")
        print("   Check whether downstream consumers tolerate this.")
        print("   (The v2.0.0 snapshot contract says BYTE-IDEMPOTENT. If consumers")
        print("   hash the files, they'll see churn even when data is unchanged.)")
        return 1
    print("🟢 All files BYTE-IDENTICAL — dep upgrade is safe for the snapshot contract.")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=["baseline", "compare"])
    args = p.parse_args()
    if args.action == "baseline":
        return baseline()
    return compare()


if __name__ == "__main__":
    raise SystemExit(main())

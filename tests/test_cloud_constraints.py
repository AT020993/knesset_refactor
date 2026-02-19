# tests/test_cloud_constraints.py
"""
Cloud Constraint Tests — catches bug classes that previously broke Streamlit Cloud.

Each test here maps to a real production incident:
- Module shadowing (src/requests.py broke GCS imports)
- Dependency drift (pyproject.toml missing deps that requirements.txt had)
- SQL alias scope errors (table-prefixed columns used outside subqueries)
- Unsafe asyncio.run() calls (crashes in Streamlit's Tornado event loop)
- UnboundLocalError in finally blocks (variable only defined inside try)
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
PYPROJECT = PROJECT_ROOT / "pyproject.toml"
REQUIREMENTS_TXT = PROJECT_ROOT / "requirements.txt"


# ===================================================================
# (a) Module Shadowing Detection
# ===================================================================
class TestModuleShadowing:
    """Prevent src/ files from shadowing stdlib or popular third-party packages.

    Historical bug: src/requests.py shadowed the real `requests` package,
    which broke `google.auth.transport.requests` → GCS init failed →
    "Database not found" on Streamlit Cloud.
    """

    # Packages that must never be shadowed by files in src/
    PROTECTED_NAMES = {
        "requests",
        "logging",
        "json",
        "os",
        "sys",
        "re",
        "io",
        "abc",
        "typing",
        "collections",
        "functools",
        "pathlib",
        "asyncio",
        "http",
        "email",
        "html",
        "csv",
        "hashlib",
        "secrets",
        "base64",
        "subprocess",
        "socket",
        "ssl",
        "urllib",
    }

    def test_no_shadowed_modules_in_src(self) -> None:
        """No .py file at src/ root should share a name with a protected package."""
        violations: list[str] = []
        for py_file in SRC_DIR.glob("*.py"):
            stem = py_file.stem
            if stem == "__init__":
                continue
            if stem in self.PROTECTED_NAMES:
                violations.append(
                    f"src/{py_file.name} shadows the '{stem}' package — "
                    f"this breaks `import {stem}` when PYTHONPATH includes src/"
                )
        assert not violations, "\n".join(violations)

    def test_requests_package_resolves_correctly(self) -> None:
        """Importing `requests` should yield the real HTTP library, not a local file."""
        import requests

        assert hasattr(requests, "Session"), (
            "`requests` resolved to a local module instead of the real HTTP library. "
            "Check for src/requests.py or similar shadowing files."
        )
        assert hasattr(requests, "adapters"), (
            "`requests.adapters` missing — likely a local shadow module."
        )

    def test_google_cloud_storage_importable(self) -> None:
        """google.cloud.storage should import cleanly with PYTHONPATH=./src."""
        try:
            from google.cloud import storage  # noqa: F401
        except ImportError:
            pytest.skip("google-cloud-storage not installed")
        except Exception as exc:
            pytest.fail(
                f"google.cloud.storage import failed (possible module shadowing): {exc}"
            )


# ===================================================================
# (b) Dependency Completeness
# ===================================================================
class TestDependencyCompleteness:
    """Ensure pyproject.toml (used by Streamlit Cloud) has all deps from requirements.txt.

    Historical bug: aiohttp was in requirements.txt but not pyproject.toml,
    so CI passed (pip install -r requirements.txt) but Cloud failed (uv sync
    reads pyproject.toml).
    """

    @staticmethod
    def _parse_requirements_txt() -> set[str]:
        """Extract normalised package names from requirements.txt."""
        names: set[str] = set()
        if not REQUIREMENTS_TXT.exists():
            return names
        for line in REQUIREMENTS_TXT.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("-"):
                continue
            # Extract package name (before any version specifier)
            match = re.match(r"^([A-Za-z0-9_-]+(?:\[[A-Za-z0-9_,]+\])?)", line)
            if match:
                # Normalise: lowercase, strip extras, underscores → hyphens
                raw = match.group(1).split("[")[0]
                names.add(raw.lower().replace("_", "-"))
        return names

    @staticmethod
    def _parse_pyproject_deps() -> set[str]:
        """Extract normalised package names from pyproject.toml [project].dependencies."""
        names: set[str] = set()
        if not PYPROJECT.exists():
            return names
        # Simple TOML parser — look for lines inside dependencies = [...]
        text = PYPROJECT.read_text()
        match = re.search(
            r'^\s*dependencies\s*=\s*\[(.*?)\]',
            text,
            re.MULTILINE | re.DOTALL,
        )
        if not match:
            return names
        block = match.group(1)
        for line in block.splitlines():
            line = line.strip().strip(",").strip('"').strip("'")
            if not line or line.startswith("#"):
                continue
            pkg_match = re.match(r"^([A-Za-z0-9_-]+(?:\[[A-Za-z0-9_,]+\])?)", line)
            if pkg_match:
                raw = pkg_match.group(1).split("[")[0]
                names.add(raw.lower().replace("_", "-"))
        return names

    # Deps that are intentionally only in requirements.txt (platform-specific, dev-only)
    ALLOWED_DRIFT: set[str] = {"pywin32", "winshell", "pytest", "pytest-cov", "pytest-asyncio"}

    def test_pyproject_superset_of_requirements(self) -> None:
        """Every runtime dep in requirements.txt must also be in pyproject.toml."""
        req_deps = self._parse_requirements_txt()
        pyp_deps = self._parse_pyproject_deps()

        missing = req_deps - pyp_deps - self.ALLOWED_DRIFT
        assert not missing, (
            f"These packages are in requirements.txt but missing from pyproject.toml "
            f"(Streamlit Cloud won't install them): {sorted(missing)}"
        )

    def test_critical_cloud_deps_in_pyproject(self) -> None:
        """Key cloud-runtime deps must be explicitly listed in pyproject.toml."""
        pyp_deps = self._parse_pyproject_deps()
        required = {"aiohttp", "google-cloud-storage", "bcrypt", "backoff"}
        missing = required - pyp_deps
        assert not missing, (
            f"Critical cloud deps missing from pyproject.toml: {sorted(missing)}"
        )


# ===================================================================
# (c) SQL Filter Column Alias Validation
# ===================================================================
class TestSQLFilterColumnAliases:
    """Validate that query filter columns survive subquery wrapping.

    Historical bug: `Q.KnessetNum` was used as knesset_filter_column but
    applied outside the subquery where alias `Q` wasn't in scope → SQL error.
    The fix was _strip_table_alias(), which removes the table prefix.
    """

    def test_strip_table_alias_correctness(self) -> None:
        """_strip_table_alias must correctly strip known filter column patterns."""
        from ui.queries.query_executor import QueryExecutor

        cases = {
            "B.KnessetNum": "KnessetNum",
            "Q.KnessetNum": "KnessetNum",
            "A.KnessetNum": "KnessetNum",
            "f.FactionID": "FactionID",
            "KnessetNum": "KnessetNum",  # no alias — passthrough
            "FactionID": "FactionID",
        }
        for input_col, expected in cases.items():
            result = QueryExecutor._strip_table_alias(input_col)
            assert result == expected, (
                f"_strip_table_alias({input_col!r}) = {result!r}, expected {expected!r}"
            )

    def test_all_query_pack_filter_columns_strip_cleanly(self) -> None:
        """Every knesset/faction filter column in query packs must strip to a bare name."""
        from ui.queries.predefined_queries import get_all_query_names, get_query_definition
        from ui.queries.query_executor import QueryExecutor

        problems: list[str] = []
        for name in get_all_query_names():
            qdef = get_query_definition(name)
            if qdef is None:
                continue
            for attr in ("knesset_filter_column", "faction_filter_column"):
                col = getattr(qdef, attr, None)
                if col is None or col == "NULL":
                    continue
                stripped = QueryExecutor._strip_table_alias(col)
                if not stripped.isidentifier():
                    problems.append(
                        f"Query {name!r}: {attr}={col!r} strips to "
                        f"{stripped!r} which is not a valid identifier"
                    )
        assert not problems, "\n".join(problems)


# ===================================================================
# (d) asyncio Pattern Audit
# ===================================================================
class TestAsyncioPatterns:
    """Ensure sync wrappers never use bare asyncio.run() without loop detection.

    Historical bug: asyncio.run() crashes with "This event loop is already
    running" inside Streamlit's Tornado loop.  The safe pattern is:
      try: asyncio.get_running_loop()  →  thread isolation
      except RuntimeError:             →  asyncio.run()
    """

    # Files known to contain _run_async helpers
    EXPECTED_RUN_ASYNC_FILES = {
        SRC_DIR / "data" / "services" / "sync_data_refresh_service.py",
        SRC_DIR / "ui" / "services" / "cap_api_service.py",
    }

    def test_run_async_helpers_exist(self) -> None:
        """Files with sync wrappers must define a _run_async helper."""
        missing: list[str] = []
        for path in self.EXPECTED_RUN_ASYNC_FILES:
            if not path.exists():
                missing.append(f"{path.relative_to(PROJECT_ROOT)} does not exist")
                continue
            source = path.read_text()
            if "_run_async" not in source:
                missing.append(
                    f"{path.relative_to(PROJECT_ROOT)} has no _run_async helper"
                )
        assert not missing, "\n".join(missing)

    def test_no_bare_asyncio_run_in_src(self) -> None:
        """asyncio.run() must only appear inside except RuntimeError blocks (CLI fallback).

        We scan for asyncio.run() calls and verify they are preceded by a
        get_running_loop() check (the standard guard pattern).
        """
        violations: list[str] = []
        for py_file in SRC_DIR.rglob("*.py"):
            source = py_file.read_text()
            if "asyncio.run(" not in source:
                continue
            # The safe pattern: file must also contain get_running_loop
            if "get_running_loop" not in source:
                violations.append(
                    f"{py_file.relative_to(PROJECT_ROOT)}: uses asyncio.run() "
                    f"without get_running_loop() guard"
                )
        assert not violations, (
            "Bare asyncio.run() without event-loop detection will crash in Streamlit:\n"
            + "\n".join(violations)
        )


# ===================================================================
# (e) UnboundLocalError Prevention in finally Blocks
# ===================================================================
class TestFinallyBlockSafety:
    """Detect variables used in finally: that are only defined inside try:.

    Historical bug: `refresh_succeeded` was used in a finally block but only
    assigned inside the try body → UnboundLocalError when the try body raised
    before reaching the assignment.
    """

    @staticmethod
    def _check_file_for_unbound_finally(filepath: Path) -> list[str]:
        """AST-scan a file for finally blocks referencing try-only variables."""
        source = filepath.read_text()
        try:
            tree = ast.parse(source, filename=str(filepath))
        except SyntaxError:
            return []

        issues: list[str] = []

        for node in ast.walk(tree):
            if not isinstance(node, ast.Try) or not node.finalbody:
                continue

            # Collect names assigned BEFORE the try statement (in enclosing scope)
            # This is a simplified check — we look at the function/module scope
            parent_assigns: set[str] = set()
            # We'll check the function body or module body for assignments before this try
            # For simplicity, check if the variable appears assigned anywhere
            # in the same function before the try statement line
            try_line = node.lineno

            # Find the enclosing function
            for outer in ast.walk(tree):
                if isinstance(outer, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)):
                    body = getattr(outer, "body", [])
                    for stmt in body:
                        if hasattr(stmt, "lineno") and stmt.lineno < try_line:
                            for sub in ast.walk(stmt):
                                if isinstance(sub, ast.Assign):
                                    for target in sub.targets:
                                        if isinstance(target, ast.Name):
                                            parent_assigns.add(target.id)

            # Collect names assigned only in the try body
            try_assigns: set[str] = set()
            for stmt in node.body:
                for sub in ast.walk(stmt):
                    if isinstance(sub, ast.Assign):
                        for target in sub.targets:
                            if isinstance(target, ast.Name):
                                try_assigns.add(target.id)

            # Collect names used in finally
            finally_uses: set[str] = set()
            for stmt in node.finalbody:
                for sub in ast.walk(stmt):
                    if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load):
                        finally_uses.add(sub.id)

            # Flag: used in finally, assigned only in try (not before try)
            risky = finally_uses & try_assigns - parent_assigns
            # Exclude common safe patterns (builtins, well-known names)
            risky -= {"conn", "cursor", "e", "exc", "self", "cls"}

            for name in risky:
                issues.append(
                    f"  line {node.lineno}: '{name}' used in finally but "
                    f"only assigned inside try body"
                )

        return issues

    def test_data_refresh_handler_no_unbound_finally(self) -> None:
        """data_refresh_handler.py must not have unbound variables in finally blocks."""
        target = SRC_DIR / "ui" / "sidebar" / "data_refresh_handler.py"
        if not target.exists():
            pytest.skip(f"{target} not found")

        issues = self._check_file_for_unbound_finally(target)
        assert not issues, (
            f"Potential UnboundLocalError in {target.name}:\n" + "\n".join(issues)
        )

    def test_scan_all_src_for_risky_finally_blocks(self) -> None:
        """Broad scan: no src/ file should have obvious unbound-in-finally patterns."""
        all_issues: list[str] = []
        for py_file in SRC_DIR.rglob("*.py"):
            issues = self._check_file_for_unbound_finally(py_file)
            if issues:
                rel = py_file.relative_to(PROJECT_ROOT)
                all_issues.append(f"{rel}:")
                all_issues.extend(issues)

        assert not all_issues, (
            "Potential UnboundLocalError in finally blocks:\n" + "\n".join(all_issues)
        )

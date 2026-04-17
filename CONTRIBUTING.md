# Contributing

Thanks for considering a contribution. This repo is a research platform for analysing Israeli parliamentary (Knesset) data — improvements to data quality, queries, visualizations, tests, or documentation are all welcome.

## Getting set up

```bash
git clone https://github.com/AT020993/knesset_refactor.git
cd knesset_refactor
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

All commands require `PYTHONPATH="./src"` (the codebase uses bare module imports like `from config.database import …`).

## Branch and commit conventions

- Branches: `feature/<short-name>`, `fix/<short-name>`, `refactor/<short-name>`, `docs/<short-name>`.
- Commits: imperative mood, explain *why* not *what* (the diff already shows *what*). Short scoped subject line (≤72 chars) plus a short body when the change is non-obvious.
- One logical change per commit. Prefer several small commits over one large one.

## Running tests before you push

```bash
# Fast suite (~576 tests, seconds)
PYTHONPATH="./src" pytest tests/ \
  --ignore=tests/test_api_integration.py \
  --ignore=tests/test_e2e.py \
  --ignore=tests/test_data_pipeline_integration.py \
  --ignore=tests/test_connection_leaks.py \
  --tb=short -q
```

Do not push failing tests. If a test is knowingly broken, open an issue first.

## Code style

CI runs `ruff`, `black`, `isort`, and `mypy` (strict mode on a curated set of modules). Run them locally before pushing:

```bash
ruff check src tests
black --check src tests
isort --check-only src tests
mypy src/data/queries src/ui/queries   # strict-mode modules
```

Fix formatting issues with `black src tests && isort src tests`.

## Pull requests

1. Open an issue first if the change is non-trivial — it saves back-and-forth on scope.
2. Rebase onto the latest `main` before opening the PR.
3. Fill in the PR template: summary, test plan, screenshots for UI changes.
4. Add an entry to `CHANGELOG.md` under `## [Unreleased]` in the appropriate category (Added / Changed / Fixed / Removed / Deprecated / Security).
5. Keep PRs focused. If you discover tangential cleanup, open a follow-up PR.

## Where to look for project-specific gotchas

`CLAUDE.md` at the repo root documents the non-obvious rules — DuckDB sequence patterns, faction-matching SQL, bill status categories, the Parquet snapshot contract, and the Streamlit Cloud `PYTHONPATH` quirk. Skim it before touching SQL, the snapshot exporter, or the CAP annotation system.

Additional references:
- [`ARCHITECTURE.md`](ARCHITECTURE.md) — layer responsibilities and data flow.
- [`docs/README.md`](docs/README.md) — index of all docs.
- [`docs/DATA_PROVENANCE.md`](docs/DATA_PROVENANCE.md) — where the data comes from and how it is refreshed.

## Reporting security issues

See [`SECURITY.md`](SECURITY.md). Do **not** open a public issue for vulnerabilities.

## Code of conduct

Be constructive, assume good faith, and stay focused on the work. Disrespectful behaviour, harassment, or personal attacks are not acceptable in issues, PRs, or discussions.

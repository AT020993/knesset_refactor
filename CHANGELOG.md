# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- `CONTRIBUTING.md`, `SECURITY.md`, `CHANGELOG.md`, `docs/README.md`, `docs/DATA_PROVENANCE.md` — research-repo scaffolding.
- `.github/PULL_REQUEST_TEMPLATE.md`, `.github/dependabot.yml`, `.github/ISSUE_TEMPLATE/` — GitHub conventions.
- `.editorconfig`, `.gitattributes` — cross-platform consistency.
- Enriched `pyproject.toml` metadata (`description`, `authors`, `license`, `keywords`, `classifiers`, `project.urls`).
- GitHub repo description, topics, and homepage URL; GitHub Private Vulnerability Reporting enabled.

### Changed
- Renamed default branch from `master` to `main`.
- `README.md` Support section pruned of link rot; CLI invocations aligned on `python -m src.cli`.
- `ARCHITECTURE.md` — removed stale "60%+ coverage requirement" and "7/7 E2E tests" claims.
- `DEPLOYMENT_OPTIONS.md` — replaced dated "October 2025" stamp with a pricing disclaimer.
- Bumped package version from `0.1.0` to `1.0.0` to match the existing `v1.0.0` git tag.

### Removed
- `RESEARCHER_GUIDE.md` — described a defunct local-desktop-launcher workflow that conflicted with the Streamlit-Cloud-first story in README.
- `topics_list.parquet` from the snapshot bundle. The query produced only 3 rows (limited by `UserCAPTaxonomy` population) and had no downstream consumer — the knesset-platform API aggregates topic data directly from `mk_bills.parquet`, and CAP topic titles live in the site's client-side Hebrew dictionary. The `data.queries.packs.topics` module is retained for future use; only the exporter registration was dropped.

### Fixed
- `docs/FULL_DATASET_DOWNLOAD.md` referenced a non-existent `src/ui/pages/data_refresh_page.py`; corrected to `src/ui/renderers/data_refresh/page.py`.
- `CLAUDE.md` and `DEPLOYMENT_GUIDE.md` still referenced the `master` branch after the rename.

## [1.0.0] — 2026-02-11

First tagged release. The `v1.0.0` tag pins the Parquet snapshot contract consumed by downstream repos — see [`src/data/snapshots/exporter.py`](src/data/snapshots/exporter.py) and the snapshot contract section in [`CLAUDE.md`](CLAUDE.md). Byte-idempotent on an unchanged warehouse; breaking Parquet-shape changes bump the major version.

[Unreleased]: https://github.com/AT020993/knesset_refactor/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/AT020993/knesset_refactor/releases/tag/v1.0.0

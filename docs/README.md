# Documentation index

This directory and the repo root hold all user- and developer-facing documentation. Start with the [top-level README](../README.md) for the product overview and install path.

## Start here

- [`../README.md`](../README.md) — what the project is, install, run.
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — branching, tests, PR expectations.
- [`../SECURITY.md`](../SECURITY.md) — how to report vulnerabilities.
- [`../CHANGELOG.md`](../CHANGELOG.md) — release history.

## Architecture

- [`../ARCHITECTURE.md`](../ARCHITECTURE.md) — layers, responsibilities, patterns, data flow.
- [`../ARCHITECTURE_DIAGRAM.md`](../ARCHITECTURE_DIAGRAM.md) — visual representation of the module structure.
- [`superpowers/specs/`](superpowers/specs/) — historical design specs (e.g. recurring-bills classification).

## Operations

- [`../DEPLOYMENT_GUIDE.md`](../DEPLOYMENT_GUIDE.md) — step-by-step Streamlit Cloud deployment.
- [`../DEPLOYMENT_OPTIONS.md`](../DEPLOYMENT_OPTIONS.md) — comparison of hosting options.
- [`deploy/`](deploy/) — VPS systemd blueprints (`knesset-etl.service`, `knesset-etl.timer`).

## Research

- [`CAP_ANNOTATION_GUIDE.md`](CAP_ANNOTATION_GUIDE.md) — CAP bill-classification annotation workflow.
- [`classification_process.md`](classification_process.md) — recurring-bills classification method and the sendable Prof. Amnon workbook contract.
- [`QUERY_EXPORTS.md`](QUERY_EXPORTS.md) — generating and validating the sendable all-parliamentary-queries CSV.
- [`FULL_DATASET_DOWNLOAD.md`](FULL_DATASET_DOWNLOAD.md) — exporting a full filtered query result.
- [`../PERFORMANCE_OPTIMIZATIONS.md`](../PERFORMANCE_OPTIMIZATIONS.md) — caching strategies and bottleneck notes.
- [`DATA_PROVENANCE.md`](DATA_PROVENANCE.md) — where the data comes from, how it is refreshed, licensing notes.

## Reference

- [`KnessetOdataManual.pdf`](KnessetOdataManual.pdf) — the upstream Knesset OData API reference, for schema-level questions about source tables.

## Developer memory

- [`../CLAUDE.md`](../CLAUDE.md) — non-obvious rules the AI assistant and maintainers rely on (DuckDB sequence patterns, faction-matching SQL, the Parquet snapshot contract, Streamlit `PYTHONPATH` quirk). Gitignored by intent but present in every clone as authoritative internal guidance.

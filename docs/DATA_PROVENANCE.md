# Data provenance

This document describes where the data that flows through `knesset_refactor` comes from, how current it is, and how downstream consumers should think about stability.

## Upstream source

The canonical source is the **Knesset OData API** operated by the Israeli parliament:

- Base URL: `http://knesset.gov.il/Odata/ParliamentInfo.svc`
- Reference manual: [`KnessetOdataManual.pdf`](KnessetOdataManual.pdf)

All parliamentary data (MKs, factions, committees, bills, queries, agendas, voting records) originates here. This repo does not scrape the Knesset website and does not mix in any third-party data into the core tables.

### Supplementary datasets

- **Faction coalition status**: [`data/faction_coalition_status_all_knessets.csv`](../data/faction_coalition_status_all_knessets.csv) — hand-curated per-faction coalition/opposition labels across Knessets 1–25 (K21 and K22 marked "Unknown" — interim). See the Coalition Status section of [`../CLAUDE.md`](../CLAUDE.md) for the update protocol.
- **CAP taxonomy + democratic-erosion codebook**: [`data/taxonomies/`](../data/taxonomies/) — research codebooks used by the CAP annotation system.
- **Recurring-bills classification**: cached from `pmb.teca-it.com` (Dr. Tal Alovitz's dataset) into a local DuckDB table plus a Parquet snapshot (see `src/data/recurring_bills/`). The Prof. Amnon sendable workbook is generated from the local warehouse/doc scan by [`scripts/export_all_bills_classified.py`](../scripts/export_all_bills_classified.py) at `data/snapshots/All_Private_Bills_K1_K25_classified.xlsx`; see [`classification_process.md`](classification_process.md) for its column contract and validation caveats.

## Licensing

### Code

The code in this repository is licensed under the MIT License — see [`../LICENSE`](../LICENSE).

### Data

Parliamentary data published by the Knesset OData API is public-record government data. This repo treats it as such, but does not itself relicense it — consumers should treat upstream data according to Israeli government open-data policy. If you are aware of a more specific licence statement from the Knesset IT operators, please open an issue so this document can be updated.

Hand-curated supplementary CSVs in `data/` (coalition status, taxonomies) are released under the same MIT terms as the code unless a more restrictive note accompanies the file.

## Refresh cadence

Data is pulled on-demand by contributors and deployed instances. There is no centrally operated refresh cron in this repo — see [`docs/deploy/knesset-etl.service`](deploy/knesset-etl.service) for the VPS systemd blueprint used in production deployments.

Typical local full-refresh time: 15–30 minutes. Bill-focused sub-refresh: 5–10 minutes.

## Handling schema changes

The upstream API evolves. When a field is added or renamed upstream:

1. Refresh fails loudly — the pipeline validates expected columns.
2. Fix the table config in `src/backend/tables.py` and/or the API config in `src/config/api.py`.
3. Update any query pack in `src/data/queries/packs/` that used the changed column.
4. Add an entry to [`../CHANGELOG.md`](../CHANGELOG.md) under `Changed`.

## Downstream consumer contract

Other repos — notably [`knesset-platform`](https://github.com/AT020993/knesset-platform) — consume **only** the Parquet snapshot bundle produced by [`src/data/snapshots/exporter.py`](../src/data/snapshots/exporter.py). They never import this codebase directly.

The contract:

- Six Parquet files (`mk_summary`, `mk_bills`, `mk_questions`, `mk_motions`, `parties_list`, `committees_list`) plus a `manifest.json` commit marker.
- **Byte-idempotent** on an unchanged warehouse — stable `ORDER BY` required in every pack query used by the exporter (DuckDB parallel execution shuffles rows otherwise).
- Pinned by the `v1.0.0` git tag. A breaking Parquet-shape change bumps the major version.
- `manifest.json` contains SHA256 checksums for every emitted file — downstream consumers can verify integrity without re-downloading the warehouse.

If you are writing a new downstream consumer, depend on the snapshot bundle via `$KNESSET_SNAPSHOT_DIR`; do not import Python code from this repo.

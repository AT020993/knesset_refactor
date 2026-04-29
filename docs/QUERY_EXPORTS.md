# Parliamentary query exports

This document describes the sendable all-query CSV used for research sharing.

## Current sendable export

Generate the simplified all-columns query export with:

```bash
PYTHONPATH="./src" python scripts/export_parliamentary_queries_sendable.py \
  --coded-query-xlsx parliamentary_queries_coded_KN17_24_Feb2026.xlsx \
  --output data/exports/parliamentary_queries_all_columns_simplified_2026-04-29.csv
```

The script also writes a sibling summary file:

```text
data/exports/parliamentary_queries_all_columns_simplified_2026-04-29_summary.csv
```

`data/exports/` is gitignored by design, so generated CSVs are local/shareable artifacts rather than committed source files.

## Sources

The export uses only project-approved local sources:

- `data/warehouse.duckdb`, populated from the official Knesset OData API.
- `data/faction_coalition_status_all_knessets.csv`, the hand-curated project coalition/opposition mapping by `KnessetNum` and `FactionID`.
- `parliamentary_queries_coded_KN17_24_Feb2026.xlsx`, when present locally, for researcher-collected K17-K24 query coding and query-level coalition labels.

Do not fill missing query coalition/opposition values from external datasets. If OData and local project files do not identify the query submitter or coalition status, keep `CoalitionStatus = Unknown`.

## Column policy

The sendable CSV has one row per `QueryID` and one primary column for each business field. It keeps provenance columns where needed:

- `CoalitionStatus` is the main sendable coalition/opposition value.
- `CollectedCoalitionStatus` preserves the local researcher workbook value when available.
- `WarehouseCoalitionStatus` preserves the OData plus faction-table join value when available.
- `CoalitionStatusDisagreement` is `True` when the collected workbook and warehouse join disagree; `CoalitionStatus` uses the collected workbook value in that case.
- `CoalitionCaveat` explains rows that remain `Unknown` or have coalition-source disagreement.
- `DuplicateCodingCaveat`, `DuplicateConflictColumns`, and `DuplicateRowsSummary` document duplicate rows found in the local researcher workbook while keeping the export one-row-per-query.

The script validates before writing:

- no duplicate column headers
- no duplicate `QueryID` rows
- no missing `QueryID`, `KnessetNum`, `QueryName`, `CoalitionStatus`, or `RecordSource`
- `CoalitionStatus` limited to `Coalition`, `Opposition`, or `Unknown`

## Known caveats

Many historical K1-K9 `KNS_Query` rows in OData use `PersonID = 30299` (`אין נתונים`) and do not contain the real submitting MK. The project has historical coalition/opposition status by faction, but without a reliable query submitter/faction link those rows must remain `Unknown`.

K21 and K22 remain `Unknown` in the project coalition table because they were interim Knessets. Do not override those values from external datasets unless the project source policy changes.

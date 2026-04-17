# Recurring Bills Classification — Design Spec

**Date:** 2026-04-17
**Author:** Amir Tahori (with Claude)
**Requester:** Prof. Amnon (via email, 2026-04-17)
**Status:** Approved design — pending implementation plan

---

## 1. Goal

Every private member bill in the Knesset warehouse must be labelled `is_original` (a new proposal) or *recurring* (a reprise of an earlier bill). For recurring bills we also record `original_bill_id`, pointing to the "patient-zero" ancestor. The primary consumer is the CAP coding workflow: Prof. Amnon's team will only code originals, treating reprises as already covered by their ancestor's classification.

The feature must:

1. Label every bill covered by Dr. Tal Alovitz's classifier (K19–K25, 24,225 bills).
2. Label bills outside Tal's coverage using a simpler name-based fallback (K16–K18, ~7,265 bills from Prof. Amnon's Excel).
3. Make the labels available both for Streamlit queries (via DuckDB) and for the downstream `knesset-platform` consumer (via a Parquet snapshot).
4. Preserve Tal's richer categorical flags (`cross_term`, `within_term_dup`, `self_resubmission`) for future analyses even though the primary consumer only uses `is_original`.

Out of scope (explicitly):

- Downloading bill documents from `fs.knesset.gov.il` — obviated by Tal's API, which returns pre-extracted document text.
- Parsing Hebrew explanatory notes with regex patterns — Tal has already done this.
- Mutating `UserBillCAP` directly; we add a view instead.
- Extending Tal's methodology to K16–K18 — that would re-introduce the document-download dependency we just avoided. Deferred.

## 2. Upstream discovery (what we learned)

Before settling on this design we inspected three data sources:

| Source | Shape | What it gives us |
|---|---|---|
| Prof. Amnon's Excel `Private.Bills.Final.091123.xlsx` | 15,761 rows × 6 cols, K16–K25 | Bill IDs, titles, first-document URL. No classification. |
| Knesset OData warehouse `KNS_Bill` | 51,673 private bills, K1–K25 | Titles, dates, initiator joins. No document bodies. |
| Tal Alovitz's API at `pmb.teca-it.com` | 24,225 classified bills, K19–K25 | Per-bill `patient_zero_bill_id`, predecessors, Hebrew explanation text. |

Key discoveries during API probing (via Chrome DevTools MCP):

- `GET /api/export/bills.csv` — bulk dump with `is_original`, `is_cross_term`, `is_within_term_dup`, `is_self_resubmission`, `category`. **No** link-back.
- `GET /api/bill/{id}` — per-bill detail with `patient_zero_bill_id`, `predecessor_bill_ids`, `family_size`, `explanation_text`, `header_text`, `operative_text`. This is the authoritative link-back. No OpenAPI docs; no bulk families endpoint (`/api/bill/families` etc. return 422 because the route is `/api/bill/{int}`).
- Tal's `bill_id` matches the warehouse `KNS_Bill.BillID` (verified: 8,479 of 8,496 Excel BillIDs in range K19–K25 overlap with Tal; 17 orphans). Clean join key.

## 3. Architecture

```
src/data/recurring_bills/
├── __init__.py
├── fetch_tal.py      ← bulk CSV + per-bill-detail HTTP client with on-disk cache
├── normalize.py      ← Hebrew name normalizer for the K16–K18 fallback only
├── classify.py       ← merge Tal + K16–K18 fallback into a single DataFrame
├── report.py         ← coverage statistics, disagreement audit
└── pipeline.py       ← orchestrator: fetch → classify → write DuckDB + Parquet

scripts/classify_recurring_bills.py   ← thin CLI wrapper around pipeline

tests/test_recurring_bills_normalize.py
tests/test_recurring_bills_classify.py
tests/test_recurring_bills_fetch_tal.py   (offline, fixture-based)
```

This mirrors the existing `src/data/snapshots/` module (exporter + manifest), the `scripts/import_*.py` pattern for one-off pipelines, and the flat `tests/` layout. It does **not** follow the professor's suggested `knesset_refactor/recurring_bills/` path — that path does not exist in this codebase.

## 4. Data flow

```
┌──────────────────────────────────────────────────────┐
│  pmb.teca-it.com                                     │
│                                                      │
│    /api/export/bills.csv   ──► tal_alovitz_bills.csv │
│                                                      │
│    /api/bill/{id}          ──► tal_bill_details/     │
│       (~16,738 recurring bills)    <BillID>.json     │
└──────────────────────────────────────────────────────┘
                            │
                            ▼
            data/external/   (cached on disk; git-ignored)
                            │
                            ▼
              ┌─────────────────────────────┐
              │ src/data/recurring_bills/   │
              │                             │
   ┌──────────┤  (a) ingest Tal CSV         │
   │          │                             │
   │          │  (b) read per-bill JSON     │
   │          │      cache  → join          │
   │          │                             │
   │          │  (c) K16–K18 fallback:      │
   │          │      group Excel by         │
   │          │      normalized name        │
   │          │                             │
   │          │  (d) merge into single DF   │
   │          └──────────────┬──────────────┘
   │                         │
   │                         ▼
   │               ┌───────────────────┐
   │               │ bill_classifications│   (DuckDB table)
   │               │  ─ primary store  │
   │               └───────────────────┘
   │                         │
   │                         ▼
   │        ┌────────────────────────────────────┐
   │        │ data/snapshots/                    │
   │        │   bill_classifications.parquet      │   (additive snapshot)
   │        └────────────────────────────────────┘
   │                         │
   │                         ▼
   │               data/recurring_bills/
   │                 coverage_report.md           (markdown stats)
   │
   ▼
KNS_Bill ( warehouse)  ──► join BillID for name/dates; used by fallback & report
```

## 5. Output schema

Primary artefact — DuckDB table `bill_classifications`:

```sql
CREATE TABLE bill_classifications (
    BillID                   INTEGER PRIMARY KEY,
    KnessetNum               INTEGER NOT NULL,
    Name                     VARCHAR NOT NULL,

    -- Primary binary answer consumed by CAP workflow
    is_original              BOOLEAN NOT NULL,
    original_bill_id         INTEGER NOT NULL,   -- equals BillID when is_original

    -- Tal's richer classification (NULL when classification_source != 'tal_alovitz')
    tal_category             VARCHAR,            -- 'new' | 'cross' | 'within'
    is_cross_term            BOOLEAN,
    is_within_term_dup       BOOLEAN,
    is_self_resubmission     BOOLEAN,
    family_size              INTEGER,
    predecessor_bill_ids     INTEGER[],          -- multiple direct predecessors possible

    -- Provenance
    classification_source    VARCHAR NOT NULL,   -- 'tal_alovitz' | 'name_fallback_k16_k18'
    tal_fetched_at           TIMESTAMP,
    last_updated             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Notes:

- `original_bill_id` is NOT NULL and equals `BillID` for originals. This simplifies downstream joins (no null handling required).
- `predecessor_bill_ids` is a DuckDB list column. For `tal_alovitz`-sourced rows, this is Tal's `predecessor_bill_ids` field verbatim. For `name_fallback_k16_k18` rows, it is a single-element list `[original_bill_id]`. For originals, it is `[]`.
- `tal_category`, `is_cross_term`, `is_within_term_dup`, `is_self_resubmission`, `family_size`, `tal_fetched_at` are NULL for K16–K18 fallback rows.

Secondary artefact — Parquet snapshot `data/snapshots/bill_classifications.parquet`:

- Same columns as the DuckDB table.
- Written via the existing atomic `.new → os.replace` pattern (see `src/data/snapshots/exporter.py`).
- Stable `ORDER BY BillID` is required for byte-idempotence per CLAUDE.md.
- Additive to the existing snapshot contract (`knesset-platform` v1.0.0 tag is unaffected). Downstream consumers opt-in.

Tertiary artefact — `data/recurring_bills/coverage_report.md`:

- Summary table: classifications per source, per KnessetNum.
- Link-back resolution rate: % of recurring bills that got an `original_bill_id`.
- Disagreement audit: bills where Tal says "original" but name-matching would have called them recurring (and vice versa).
- Sample groups: 10 largest Tal-reported families with their titles, for eyeball validation.

## 6. CAP integration

Non-invasive — add a view to the warehouse, do not mutate `UserBillCAP`:

```sql
CREATE OR REPLACE VIEW v_cap_bills_with_recurrence AS
SELECT
    ubc.*,
    bc.is_original,
    bc.original_bill_id,
    bc.tal_category,
    bc.classification_source
FROM UserBillCAP ubc
LEFT JOIN bill_classifications bc USING (BillID);
```

Prof. Amnon's team filters `WHERE is_original` when picking bills to code. The `LEFT JOIN` preserves bills outside our classification scope (e.g., K1–K15 government bills) with NULL in the new columns.

A follow-up (out of scope for this spec) wires `is_original` into the `mk_bills.parquet` snapshot bundle so `knesset-platform` gets it for free.

## 7. Fetch politeness & caching

Tal's FastAPI server runs on uvicorn — lightweight but personally operated. We are a polite crawler:

- **User-Agent**: `knesset-refactor-research-bot/1.0 (contact: amirgo12@gmail.com)`
- **Delay**: 0.3 s between requests (configurable via `--delay`). At 0.3 s × 16,738 bills ≈ 1.4 h first run.
- **Cache**: each response written to `data/external/tal_bill_details/<BillID>.json`. Directory is git-ignored.
- **Resume**: skip fetch if cache file exists and `--force-refresh` is not set.
- **Retry**: 3× with exponential backoff (1 s, 3 s, 9 s) on 5xx or connection errors.
- **Bulk CSV refresh**: `If-None-Match` / `If-Modified-Since` headers — only re-download if Tal updated.
- **Originals**: skip per-bill fetch entirely. An `is_original` bill in Tal's CSV has `patient_zero = self` by definition; fetching it gives no new information.

## 8. K16–K18 fallback

For the 7,265 private bills in K16–K18 from Prof. Amnon's Excel (which Tal does not classify):

1. Normalize each `Name` by stripping the Hebrew year-suffix tail (e.g., `, התשע״ג-2013`) and collapsing whitespace.
2. Group by normalized name across K16–K18 only.
3. In each group, the row with the lowest `KnessetNum` is the original (tie-breaker: lowest `BillID`); others are reprises whose `original_bill_id` points to that earliest BillID.
4. Set `classification_source='name_fallback_k16_k18'`. Tal's extra flags remain NULL.

This is intentionally simple. A diagnostic pass on the Excel shows strict exact-match alone gives only ~1.9 % recurrence coverage, but year-stripped matching lifts this meaningfully without the precision collapse of fully-normalized matching (which conflated unrelated amendments to the same parent law).

**Known limitation of the fallback**: a K16 bill that is actually a reprise of a pre-K16 bill will be mislabelled as `is_original=TRUE` because the Excel window starts at K16. We accept this because (a) the Excel is Amnon's analytical frame, and (b) recovering the true ancestor would require the same document-downloading work we deliberately deferred in §1.

## 9. Error handling

- **Tal API 404 for a bill in the bulk CSV**: log a warning, record `classification_source='tal_alovitz'` with `patient_zero_bill_id = bill_id` (treat as unresolved original). Do not fail the pipeline.
- **Tal API 5xx after retries**: skip this bill for now; CLI exits with status 1 so CI or operator notices, but the partial run is still committed to DuckDB/Parquet.
- **BillID in Tal's data not in warehouse `KNS_Bill`**: include the row anyway with warehouse-sourced fields NULL. Log the orphan count.
- **DuckDB lock contention**: write Parquet first (the durable artefact), then DuckDB. If DuckDB fails, the Parquet still exists for downstream consumers.
- **Fail-secure**: all exceptions are logged; the pipeline continues to the next bill/step where possible.

## 10. Testing

All tests run offline. No real HTTP in the default suite.

- `test_normalize.py` — ≈ 20 asserts: Hebrew year suffixes (`, התשפ״ג-2023`), whitespace collapsing, nested parentheses, Unicode dashes. Pure functions, no I/O.
- `test_classify.py` — build a 15-row in-memory DataFrame with three recurring groups across K16–K18, assert earliest-KnessetNum picks and correct `original_bill_id` assignments.
- `test_fetch_tal.py` — fixture CSV + fixture JSON in `tests/fixtures/recurring_bills/`; use `requests-mock` to simulate the API. Verify cache write + cache hit path + retry on 5xx.
- Integration (gated behind `pytest -m integration`) — run the full pipeline end-to-end against a ~100-bill fixture warehouse, assert the generated Parquet matches a golden file byte-for-byte (idempotence check).

## 11. CLI

```bash
# Full first-time run: fetch bulk + per-bill for recurring, build classifications
PYTHONPATH="./src" python -m data.recurring_bills.pipeline refresh

# Rebuild from cache (no network); useful if the classification logic changes
PYTHONPATH="./src" python -m data.recurring_bills.pipeline rebuild

# Print stats against the current warehouse without re-running
PYTHONPATH="./src" python -m data.recurring_bills.pipeline report
```

Flags common to all modes:

- `--warehouse PATH` (default `data/warehouse.duckdb`)
- `--excel PATH` (default `data/Private.Bills.Final.091123.xlsx`)
- `--cache-dir PATH` (default `data/external/`)
- `--output-parquet PATH` (default `data/snapshots/bill_classifications.parquet`)
- `--report PATH` (default `data/recurring_bills/coverage_report.md`)

`refresh`-specific flags:

- `--delay SECONDS` (default 0.3)
- `--force-refresh` — ignore cache, re-fetch everything
- `--only-recurring` (default on) — skip per-bill fetch for `is_original=1` bills

## 12. Prerequisite setup

Before the first run, copy Prof. Amnon's Excel into the project's data folder:

```bash
cp ~/Downloads/Private.Bills.Final.091123.xlsx data/
```

The file is tracked in the design but not checked in to git (it is a ~1.2 MB raw dataset; the repo policy in `.gitignore` keeps raw data outside version control).

## 13. Runtime budget

| Step | First run | Cached rerun |
|---|---|---|
| Bulk CSV download | ~2 s | ~0 s (ETag 304) |
| Per-bill detail crawl (~16,738 × 0.3 s) | ~84 min | ~0 s (all cached) |
| Classification + merge | ~10 s | ~10 s |
| Write DuckDB + Parquet | ~5 s | ~5 s |
| Coverage report | ~5 s | ~5 s |
| **Total** | **~90 min** | **~25 s** |

## 14. Open questions deferred to the plan stage

- Does `predecessor_bill_ids` ever exceed length 1 in Tal's data? If yes, is our `INTEGER[]` column the right representation, or should we also add a scalar `primary_predecessor_bill_id`? (Check during implementation; adjust schema if the evidence warrants it.)
- Does Tal's `bill_id` ever refer to a government bill (`PrivateNumber IS NULL` in our warehouse)? If yes, we widen the CAP view's join condition. (Verify with a SQL spot-check during implementation.)
- Should the K16–K18 fallback also use the warehouse's `KNS_Bill` (51K rows including non-private) instead of Prof. Amnon's Excel subset (7K private only)? The Excel is narrower and matches Amnon's analytical frame. Default: use Excel. Revisit if Amnon asks for wider coverage.

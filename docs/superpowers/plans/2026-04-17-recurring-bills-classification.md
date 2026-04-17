# Recurring Bills Classification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a warehouse table + Parquet snapshot that marks every private member bill as `is_original` or recurring, with a link-back to its patient-zero ancestor, by ingesting Dr. Tal Alovitz's classifier (`pmb.teca-it.com`) and filling the K16–K18 gap with name-matching against Prof. Amnon's Excel.

**Architecture:** Small pure-Python modules under `src/data/recurring_bills/`, each with a single responsibility (name normalization, HTTP fetch+cache, classification merge, coverage report, pipeline orchestration). TDD: every module lands as test → implementation → commit. Output is written to DuckDB table `bill_classifications` and to `data/snapshots/bill_classifications.parquet` via the atomic-rename idempotence pattern already used by `src/data/snapshots/exporter.py`.

**Tech Stack:** Python 3.12, `pandas` 2.2, `duckdb` 1.2, `pyarrow` 19, `openpyxl` 3.1 (all present in `requirements.txt`). `requests` 2.32 (available in `.venv`; will be added to `requirements.txt` in Task 0). Tests use `pytest` + `unittest.mock.patch` (no new test deps).

---

## Spec reference

Full design: [docs/superpowers/specs/2026-04-17-recurring-bills-classification-design.md](../specs/2026-04-17-recurring-bills-classification-design.md).

## File structure

| Path | Purpose |
|------|---------|
| `src/data/recurring_bills/__init__.py` | Empty package marker |
| `src/data/recurring_bills/normalize.py` | Pure Hebrew-name normalizer used by K16–K18 fallback |
| `src/data/recurring_bills/fetch_tal.py` | HTTP client for `pmb.teca-it.com` with on-disk cache + retry |
| `src/data/recurring_bills/classify.py` | Merge Tal data + K16–K18 fallback into a single DataFrame |
| `src/data/recurring_bills/storage.py` | Write DataFrame to DuckDB table + Parquet snapshot (atomic) |
| `src/data/recurring_bills/report.py` | Build markdown coverage report from classified DataFrame |
| `src/data/recurring_bills/pipeline.py` | Orchestrator — `refresh` / `rebuild` / `report` entry points |
| `scripts/classify_recurring_bills.py` | Thin `argparse` CLI wrapper around `pipeline` |
| `tests/test_recurring_bills_normalize.py` | Unit tests for `normalize.py` |
| `tests/test_recurring_bills_fetch_tal.py` | Unit tests for `fetch_tal.py` (HTTP mocked) |
| `tests/test_recurring_bills_classify.py` | Unit tests for `classify.py` + `storage.py` |
| `tests/test_recurring_bills_report.py` | Unit tests for `report.py` |
| `tests/fixtures/recurring_bills/tal_bulk_sample.csv` | 6-row fixture mirroring Tal's bulk CSV shape |
| `tests/fixtures/recurring_bills/tal_detail_<id>.json` | 3 fixture detail JSONs |
| `tests/fixtures/recurring_bills/excel_sample.xlsx` | 8-row fixture for K16–K18 fallback |
| `.gitignore` | Append `data/external/` and `data/recurring_bills/` entries |
| `requirements.txt` | Append `requests==2.32.5` |
| `CLAUDE.md` | Append a one-line pointer to the new CLI under "Commands" |

One additional follow-up file lands in the warehouse at runtime (not in git):
- `data/warehouse.duckdb` — new table `bill_classifications` + new view `v_cap_bills_with_recurrence`

---

## Task 0: Project setup — deps, gitignore, fixtures scaffold

**Files:**
- Create: `src/data/recurring_bills/__init__.py`
- Create: `tests/fixtures/recurring_bills/tal_bulk_sample.csv`
- Create: `tests/fixtures/recurring_bills/tal_detail_477120.json`
- Create: `tests/fixtures/recurring_bills/tal_detail_477137.json`
- Create: `tests/fixtures/recurring_bills/tal_detail_477119.json`
- Create: `tests/fixtures/recurring_bills/excel_sample.xlsx` (via Python script)
- Modify: `.gitignore` (append two lines)
- Modify: `requirements.txt` (append `requests==2.32.5`)
- Copy: `~/Downloads/Private.Bills.Final.091123.xlsx` → `data/Private.Bills.Final.091123.xlsx`

- [ ] **Step 1: Create the package marker**

```bash
mkdir -p src/data/recurring_bills
printf '"""Recurring-bills classification package."""\n' > src/data/recurring_bills/__init__.py
```

- [ ] **Step 2: Copy the raw Excel into the project**

```bash
cp "/Users/amir/Downloads/Private.Bills.Final.091123.xlsx" data/Private.Bills.Final.091123.xlsx
ls -la data/Private.Bills.Final.091123.xlsx
```

Expected output: a ~1.2 MB file listing.

- [ ] **Step 3: Append gitignore entries**

Add to `.gitignore` after the existing `data/` block:

```
!/data/taxonomies/
!/data/faction_coalition_status*.csv
# Recurring-bills: keep cache + report dirs out of git
# (covered by /data/* blanket, but documented here for clarity)
```

Verify the cache dir will be ignored:
```bash
mkdir -p data/external/tal_bill_details data/recurring_bills
git check-ignore -v data/external/tal_bill_details/foo.json
git check-ignore -v data/recurring_bills/coverage_report.md
```

Expected: both paths report `.gitignore:2:/data/*` as the ignoring rule.

- [ ] **Step 4: Add `requests` to requirements**

```bash
echo "requests==2.32.5                  # HTTP client for Tal Alovitz API ingestion" >> requirements.txt
pip install requests==2.32.5
python -c "import requests; print(requests.__version__)"
```

Expected: `2.32.5`.

- [ ] **Step 5: Create fixture CSV (mirrors Tal's bulk export shape)**

```bash
cat > tests/fixtures/recurring_bills/tal_bulk_sample.csv <<'CSV'
bill_id,knesset_url,bill_name,lead_initiator,lead_faction,knesset_num,submission_date,category,is_cross_term,is_within_term_dup,is_self_resubmission,is_original
477119,https://example/477119,"חוק התפזרות הכנסת התשע-עשרה, התשע""ה-2014",זהבה גלאון,מרצ,19,2013-03-13T00:00:00,new,0,0,0,1
477120,https://example/477120,"הצעת חוק העונשין (תיקון), התשע""ג-2013",יצחק הרצוג,העבודה,19,2013-03-13T00:00:00,cross,1,0,0,0
477137,https://example/477137,"הצעת חוק התפזרות הכנסת התשע-עשרה, התשע""ג-2013",יצחק הרצוג,העבודה,19,2013-03-13T00:00:00,within,0,1,0,0
500001,https://example/500001,"הצעת חוק חינוך חובה",דוד לוי,ליכוד,20,2015-05-05T00:00:00,new,0,0,0,1
500002,https://example/500002,"הצעת חוק חינוך חובה",חיים רמון,קדימה,21,2019-05-05T00:00:00,cross,1,0,0,0
500003,https://example/500003,"הצעת חוק חינוך חובה",חיים רמון,קדימה,22,2020-05-05T00:00:00,cross,1,0,1,0
CSV
```

- [ ] **Step 6: Create 3 fixture detail JSONs**

```bash
cat > tests/fixtures/recurring_bills/tal_detail_477119.json <<'JSON'
{"bill_id": 477119, "citation_id": "פ/3/19", "title": "חוק התפזרות הכנסת התשע-עשרה", "name_core": "חוק התפזרות הכנסת התשע-עשרה", "knesset_num": 19, "submission_date": "2013-03-13 00:00:00", "is_root": true, "family_size": 2, "patient_zero_bill_id": 477119, "predecessor_bill_ids": [], "category": "new"}
JSON

cat > tests/fixtures/recurring_bills/tal_detail_477120.json <<'JSON'
{"bill_id": 477120, "citation_id": "פ/4/19", "title": "הצעת חוק העונשין", "name_core": "חוק העונשין", "knesset_num": 19, "submission_date": "2013-03-13 00:00:00", "is_root": true, "family_size": 1, "patient_zero_bill_id": 477120, "predecessor_bill_ids": [], "category": "cross"}
JSON

cat > tests/fixtures/recurring_bills/tal_detail_477137.json <<'JSON'
{"bill_id": 477137, "citation_id": "פ/5/19", "title": "הצעת חוק התפזרות הכנסת התשע-עשרה", "name_core": "חוק התפזרות הכנסת התשע-עשרה", "knesset_num": 19, "submission_date": "2013-03-13 00:00:00", "is_root": false, "family_size": 2, "patient_zero_bill_id": 477119, "predecessor_bill_ids": [477119], "category": "within"}
JSON
```

- [ ] **Step 7: Create fixture Excel (K16–K18 fallback testing)**

Run this one-off script (not checked in anywhere else):

```bash
python -c "
import pandas as pd
df = pd.DataFrame([
    # Two groups of bills sharing a normalized name across K16-K18
    {'KnessetNum': 16, 'Name': 'הצעת חוק חינוך חובה, התשס\"ג-2003', 'BillID': 10001, 'PrivateNumber': 101, 'documents': 'https://x/a.doc', 'factions_list': '{}'},
    {'KnessetNum': 17, 'Name': 'הצעת חוק חינוך חובה, התשס\"ז-2006', 'BillID': 10002, 'PrivateNumber': 201, 'documents': 'https://x/b.doc', 'factions_list': '{}'},
    {'KnessetNum': 18, 'Name': 'הצעת חוק חינוך חובה, התשע\"א-2011', 'BillID': 10003, 'PrivateNumber': 301, 'documents': 'https://x/c.doc', 'factions_list': '{}'},
    # Group present only in one Knesset (no reprise)
    {'KnessetNum': 16, 'Name': 'הצעת חוק ביטוח הלאומי, התשס\"ג-2003', 'BillID': 10004, 'PrivateNumber': 102, 'documents': 'https://x/d.doc', 'factions_list': '{}'},
    # A second recurring group in K17+K18 only
    {'KnessetNum': 17, 'Name': 'הצעת חוק שעות עבודה ומנוחה, התשס\"ז-2006', 'BillID': 10005, 'PrivateNumber': 202, 'documents': 'https://x/e.doc', 'factions_list': '{}'},
    {'KnessetNum': 18, 'Name': 'הצעת חוק שעות עבודה ומנוחה, התשע\"א-2011', 'BillID': 10006, 'PrivateNumber': 302, 'documents': 'https://x/f.doc', 'factions_list': '{}'},
    # Same-Knesset duplicates (tie-breaker test: lowest BillID wins)
    {'KnessetNum': 18, 'Name': 'הצעת חוק זכויות האזרח, התשע\"א-2011', 'BillID': 10008, 'PrivateNumber': 304, 'documents': 'https://x/h.doc', 'factions_list': '{}'},
    {'KnessetNum': 18, 'Name': 'הצעת חוק זכויות האזרח, התשע\"א-2011', 'BillID': 10007, 'PrivateNumber': 303, 'documents': 'https://x/g.doc', 'factions_list': '{}'},
])
df.to_excel('tests/fixtures/recurring_bills/excel_sample.xlsx', index=False)
print('wrote', len(df), 'rows')
"
```

Expected: `wrote 8 rows`.

- [ ] **Step 8: Commit**

```bash
git add src/data/recurring_bills/__init__.py tests/fixtures/recurring_bills/ requirements.txt .gitignore
git commit -m "chore: scaffold recurring_bills module + fixtures + deps

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 1: `normalize.py` — strip_year_suffix

**Files:**
- Create: `src/data/recurring_bills/normalize.py`
- Create: `tests/test_recurring_bills_normalize.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_recurring_bills_normalize.py`:

```python
"""Unit tests for src/data/recurring_bills/normalize.py."""

import pytest

from data.recurring_bills.normalize import strip_year_suffix


class TestStripYearSuffix:
    def test_strips_standard_hebrew_year_tail(self):
        assert strip_year_suffix('הצעת חוק חינוך חובה, התשס"ג-2003') == 'הצעת חוק חינוך חובה'

    def test_strips_year_tail_with_unicode_gershayim(self):
        # Real Hebrew text uses \u05F4 gershayim, not ASCII "
        assert strip_year_suffix('הצעת חוק העונשין, התשע\u05F4ג-2013') == 'הצעת חוק העונשין'

    def test_strips_year_tail_with_en_dash(self):
        # Some bills use EN DASH (\u2013) instead of ASCII -
        assert strip_year_suffix('חוק התפזרות, התשע"ה\u20132014') == 'חוק התפזרות'

    def test_leaves_string_without_year_tail_alone(self):
        assert strip_year_suffix('הצעת חוק חינוך חובה') == 'הצעת חוק חינוך חובה'

    def test_collapses_trailing_whitespace(self):
        assert strip_year_suffix('הצעת חוק חינוך חובה  ') == 'הצעת חוק חינוך חובה'

    def test_empty_string_returns_empty(self):
        assert strip_year_suffix('') == ''

    def test_only_year_tail_returns_empty(self):
        assert strip_year_suffix(', התשס"ג-2003') == ''

    def test_preserves_internal_whitespace(self):
        assert strip_year_suffix('חוק עבודה ומנוחה, התשס"ג-2003') == 'חוק עבודה ומנוחה'
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_normalize.py -v
```

Expected: `ModuleNotFoundError: No module named 'data.recurring_bills.normalize'` OR `ImportError`.

- [ ] **Step 3: Write minimal implementation**

Create `src/data/recurring_bills/normalize.py`:

```python
"""Hebrew name normalization for the K16-K18 name-match fallback."""

from __future__ import annotations

import re

_YEAR_SUFFIX_RE = re.compile(
    r",\s*הת\S+[\-\u2013]\d{4}\s*$",
)


def strip_year_suffix(name: str) -> str:
    """Strip the trailing Hebrew year suffix and trailing whitespace.

    Matches suffixes shaped like ``, התשס"ג-2003`` or ``, התשע״ג\u20132013``.
    Returns the input unchanged if no suffix is present.
    """
    if not name:
        return name
    return _YEAR_SUFFIX_RE.sub("", name).rstrip()
```

- [ ] **Step 4: Run test to verify it passes**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_normalize.py -v
```

Expected: all 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/data/recurring_bills/normalize.py tests/test_recurring_bills_normalize.py
git commit -m "feat(recurring_bills): Hebrew year-suffix stripper

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `normalize.py` — normalize_name (full normalizer)

**Files:**
- Modify: `src/data/recurring_bills/normalize.py` (add `normalize_name` function)
- Modify: `tests/test_recurring_bills_normalize.py` (add `TestNormalizeName` class)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_recurring_bills_normalize.py`:

```python
from data.recurring_bills.normalize import normalize_name


class TestNormalizeName:
    def test_combines_year_strip_and_whitespace_collapse(self):
        assert normalize_name('הצעת חוק חינוך חובה,  התשס"ג-2003') == 'הצעת חוק חינוך חובה'

    def test_collapses_multiple_internal_spaces(self):
        assert normalize_name('חוק   חינוך    חובה') == 'חוק חינוך חובה'

    def test_strips_leading_whitespace(self):
        assert normalize_name('   חוק חינוך חובה') == 'חוק חינוך חובה'

    def test_handles_nan_or_none_like_input(self):
        assert normalize_name(None) == ''
        assert normalize_name('') == ''

    def test_idempotent(self):
        once = normalize_name('הצעת חוק העונשין, התשע"ג-2013')
        twice = normalize_name(once)
        assert once == twice
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_normalize.py::TestNormalizeName -v
```

Expected: `ImportError: cannot import name 'normalize_name'`.

- [ ] **Step 3: Implement `normalize_name`**

Append to `src/data/recurring_bills/normalize.py`:

```python
_WHITESPACE_RE = re.compile(r"\s+")


def normalize_name(name: str | None) -> str:
    """Return a canonical form of a bill name for grouping.

    Applies (in order):
    1. Null/empty guard -> empty string
    2. Strip Hebrew year suffix (see :func:`strip_year_suffix`)
    3. Collapse any run of whitespace to a single space
    4. Strip leading/trailing whitespace
    """
    if not name:
        return ""
    stripped = strip_year_suffix(name)
    collapsed = _WHITESPACE_RE.sub(" ", stripped)
    return collapsed.strip()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_normalize.py -v
```

Expected: all 13 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/data/recurring_bills/normalize.py tests/test_recurring_bills_normalize.py
git commit -m "feat(recurring_bills): add normalize_name composing year-strip + whitespace

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `fetch_tal.py` — download_bulk_csv

**Files:**
- Create: `src/data/recurring_bills/fetch_tal.py`
- Create: `tests/test_recurring_bills_fetch_tal.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_recurring_bills_fetch_tal.py`:

```python
"""Unit tests for src/data/recurring_bills/fetch_tal.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from data.recurring_bills.fetch_tal import download_bulk_csv


FIXTURE_CSV = Path(__file__).parent / "fixtures" / "recurring_bills" / "tal_bulk_sample.csv"


class TestDownloadBulkCsv:
    def _mock_response(self, body: bytes, status: int = 200, headers: dict | None = None):
        resp = MagicMock()
        resp.status_code = status
        resp.content = body
        resp.iter_content = lambda chunk_size: [body]
        resp.headers = headers or {}
        resp.raise_for_status = MagicMock()
        return resp

    def test_writes_csv_to_disk(self, tmp_path: Path):
        body = FIXTURE_CSV.read_bytes()
        out = tmp_path / "tal_alovitz_bills.csv"

        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(body)
            path = download_bulk_csv(out)

        assert path == out
        assert out.exists()
        assert out.read_bytes() == body

    def test_sends_research_bot_user_agent(self, tmp_path: Path):
        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(b"bill_id\n1\n")
            download_bulk_csv(tmp_path / "x.csv")

        _, kwargs = mock_get.call_args
        ua = kwargs["headers"]["User-Agent"]
        assert "knesset-refactor-research-bot" in ua

    def test_etag_roundtrip_skips_download_on_304(self, tmp_path: Path):
        out = tmp_path / "tal_alovitz_bills.csv"
        etag_file = tmp_path / "tal_alovitz_bills.csv.etag"
        out.write_bytes(b"old content")
        etag_file.write_text('"abc123"')

        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(b"", status=304)
            path = download_bulk_csv(out)

        assert path == out
        assert out.read_bytes() == b"old content"  # untouched
        _, kwargs = mock_get.call_args
        assert kwargs["headers"]["If-None-Match"] == '"abc123"'

    def test_etag_roundtrip_writes_new_etag_on_200(self, tmp_path: Path):
        out = tmp_path / "tal_alovitz_bills.csv"
        etag_file = tmp_path / "tal_alovitz_bills.csv.etag"

        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(
                b"bill_id\n1\n",
                headers={"ETag": '"xyz789"'},
            )
            download_bulk_csv(out)

        assert etag_file.read_text() == '"xyz789"'
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_fetch_tal.py::TestDownloadBulkCsv -v
```

Expected: `ImportError: No module named 'data.recurring_bills.fetch_tal'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/data/recurring_bills/fetch_tal.py`:

```python
"""HTTP client for Dr. Tal Alovitz's classifier at ``pmb.teca-it.com``.

Two endpoints are used:

* ``GET /api/export/bills.csv`` — bulk summary download (with ETag caching)
* ``GET /api/bill/{id}``        — per-bill detail with patient-zero link-back
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://pmb.teca-it.com"
USER_AGENT = "knesset-refactor-research-bot/1.0 (contact: amirgo12@gmail.com)"
DEFAULT_TIMEOUT_S = 30


def download_bulk_csv(output_path: Path) -> Path:
    """Download Tal's bulk CSV, honouring local ETag for cheap refresh.

    On HTTP 304 (Not Modified) the existing file is left untouched.
    On 200 the new body replaces the file and the ETag is persisted alongside.
    Returns ``output_path`` either way.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    etag_path = output_path.with_suffix(output_path.suffix + ".etag")

    headers = {"User-Agent": USER_AGENT}
    if etag_path.exists():
        headers["If-None-Match"] = etag_path.read_text().strip()

    url = f"{BASE_URL}/api/export/bills.csv"
    resp = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT_S)

    if resp.status_code == 304:
        log.info("Bulk CSV unchanged (ETag match)")
        return output_path

    resp.raise_for_status()
    output_path.write_bytes(resp.content)
    new_etag = resp.headers.get("ETag")
    if new_etag:
        etag_path.write_text(new_etag)
    log.info("Downloaded bulk CSV: %d bytes -> %s", len(resp.content), output_path)
    return output_path
```

- [ ] **Step 4: Run test to verify it passes**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_fetch_tal.py::TestDownloadBulkCsv -v
```

Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/data/recurring_bills/fetch_tal.py tests/test_recurring_bills_fetch_tal.py
git commit -m "feat(recurring_bills): bulk CSV downloader with ETag cache

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `fetch_tal.py` — fetch_bill_detail with on-disk cache

**Files:**
- Modify: `src/data/recurring_bills/fetch_tal.py` (add `fetch_bill_detail` + `fetch_many_details`)
- Modify: `tests/test_recurring_bills_fetch_tal.py` (add `TestFetchBillDetail`)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_recurring_bills_fetch_tal.py`:

```python
import json
from data.recurring_bills.fetch_tal import fetch_bill_detail, fetch_many_details


class TestFetchBillDetail:
    def _mock_json(self, payload: dict, status: int = 200):
        resp = MagicMock()
        resp.status_code = status
        resp.json = MagicMock(return_value=payload)
        resp.raise_for_status = MagicMock()
        return resp

    def test_writes_json_to_cache_on_miss(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        payload = {"bill_id": 477120, "patient_zero_bill_id": 477120, "category": "cross"}

        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            mock_get.return_value = self._mock_json(payload)
            path = fetch_bill_detail(477120, cache_dir)

        assert path == cache_dir / "477120.json"
        assert json.loads(path.read_text()) == payload
        assert mock_get.call_count == 1

    def test_uses_cache_on_hit_and_skips_http(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        cached = cache_dir / "477120.json"
        cached.write_text('{"bill_id": 477120, "cached": true}')

        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            path = fetch_bill_detail(477120, cache_dir)

        assert mock_get.call_count == 0
        assert json.loads(path.read_text())["cached"] is True

    def test_force_refresh_bypasses_cache(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        (cache_dir / "477120.json").write_text('{"cached": true}')

        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            mock_get.return_value = self._mock_json({"bill_id": 477120, "cached": False})
            fetch_bill_detail(477120, cache_dir, force_refresh=True)

        assert mock_get.call_count == 1


class TestFetchManyDetails:
    def test_respects_delay_and_caches_all(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        bill_ids = [477119, 477120, 477137]

        def side_effect(url, **kwargs):
            bid = int(url.rsplit("/", 1)[-1])
            resp = MagicMock()
            resp.status_code = 200
            resp.json = MagicMock(return_value={"bill_id": bid})
            resp.raise_for_status = MagicMock()
            return resp

        with patch("data.recurring_bills.fetch_tal.requests.get", side_effect=side_effect):
            with patch("data.recurring_bills.fetch_tal.time.sleep") as mock_sleep:
                paths = fetch_many_details(bill_ids, cache_dir, delay_s=0.5)

        assert len(paths) == 3
        # Delay is called before each HTTP fetch (3 times)
        assert mock_sleep.call_count == 3
        for call in mock_sleep.call_args_list:
            assert call.args == (0.5,)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_fetch_tal.py::TestFetchBillDetail tests/test_recurring_bills_fetch_tal.py::TestFetchManyDetails -v
```

Expected: `ImportError: cannot import name 'fetch_bill_detail' from 'data.recurring_bills.fetch_tal'`.

- [ ] **Step 3: Implement `fetch_bill_detail` + `fetch_many_details`**

Append to `src/data/recurring_bills/fetch_tal.py`:

```python
def fetch_bill_detail(
    bill_id: int,
    cache_dir: Path,
    *,
    force_refresh: bool = False,
) -> Path:
    """Fetch per-bill detail from ``/api/bill/{id}``; cache to disk.

    Cache key: ``<cache_dir>/<bill_id>.json``. Directory is created on demand.
    When ``force_refresh`` is False (default) and the cache file exists,
    the HTTP call is skipped.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    out = cache_dir / f"{bill_id}.json"

    if out.exists() and not force_refresh:
        return out

    url = f"{BASE_URL}/api/bill/{bill_id}"
    resp = requests.get(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=DEFAULT_TIMEOUT_S,
    )
    resp.raise_for_status()
    out.write_text(json.dumps(resp.json(), ensure_ascii=False))
    return out


def fetch_many_details(
    bill_ids: list[int],
    cache_dir: Path,
    *,
    delay_s: float = 0.3,
    force_refresh: bool = False,
) -> list[Path]:
    """Fetch per-bill detail for many bills, sleeping ``delay_s`` between calls.

    Cache hits do NOT count against the politeness budget (no sleep).
    Returns the list of cache paths in input order.
    """
    cache_dir = Path(cache_dir)
    out_paths: list[Path] = []
    for bid in bill_ids:
        cache_hit = (cache_dir / f"{bid}.json").exists() and not force_refresh
        if not cache_hit:
            time.sleep(delay_s)
        out_paths.append(fetch_bill_detail(bid, cache_dir, force_refresh=force_refresh))
    return out_paths
```

Also add the import at the top of `fetch_tal.py` if not already present:

```python
import json
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_fetch_tal.py -v
```

Expected: all 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/data/recurring_bills/fetch_tal.py tests/test_recurring_bills_fetch_tal.py
git commit -m "feat(recurring_bills): per-bill detail fetcher with disk cache

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: `fetch_tal.py` — retry on 5xx

**Files:**
- Modify: `src/data/recurring_bills/fetch_tal.py` (wrap HTTP calls with retry)
- Modify: `tests/test_recurring_bills_fetch_tal.py` (add retry tests)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_recurring_bills_fetch_tal.py`:

```python
class TestRetry:
    def test_retries_on_500_then_succeeds(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        # Build a sequence: 500, 500, 200
        responses_ = [
            MagicMock(status_code=500, raise_for_status=MagicMock(side_effect=requests.HTTPError("500"))),
            MagicMock(status_code=500, raise_for_status=MagicMock(side_effect=requests.HTTPError("500"))),
        ]
        ok = MagicMock(status_code=200)
        ok.json = MagicMock(return_value={"bill_id": 477120})
        ok.raise_for_status = MagicMock()
        responses_.append(ok)

        with patch("data.recurring_bills.fetch_tal.requests.get", side_effect=responses_):
            with patch("data.recurring_bills.fetch_tal.time.sleep"):  # skip backoff delays
                path = fetch_bill_detail(477120, cache_dir)

        assert path.exists()

    def test_gives_up_after_three_failures(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"

        failing = MagicMock(status_code=500)
        failing.raise_for_status = MagicMock(side_effect=requests.HTTPError("500"))

        with patch("data.recurring_bills.fetch_tal.requests.get", return_value=failing):
            with patch("data.recurring_bills.fetch_tal.time.sleep"):
                with pytest.raises(requests.HTTPError):
                    fetch_bill_detail(477120, cache_dir)
```

Also add at top of `tests/test_recurring_bills_fetch_tal.py`:

```python
import requests  # noqa: F401 — imported so we can reference requests.HTTPError
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_fetch_tal.py::TestRetry -v
```

Expected: first test fails (no retry behaviour wired up yet).

- [ ] **Step 3: Add retry wrapper**

Insert into `src/data/recurring_bills/fetch_tal.py` (just below the constants, before `download_bulk_csv`):

```python
_RETRY_BACKOFFS_S = (1, 3, 9)


def _get_with_retry(url: str, *, headers: dict, timeout: int = DEFAULT_TIMEOUT_S) -> requests.Response:
    """GET with up to 3 attempts on connection errors / 5xx, exponential backoff.

    304 and 4xx are returned without retrying (not transient failures).
    """
    last_exc: Exception | None = None
    for attempt, backoff in enumerate((0,) + _RETRY_BACKOFFS_S[:-1]):
        if backoff:
            time.sleep(backoff)
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code < 500:
                return resp
            resp.raise_for_status()  # raises on 5xx
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            last_exc = exc
            log.warning("Attempt %d for %s failed: %s", attempt + 1, url, exc)
    assert last_exc is not None
    raise last_exc
```

Then replace the `requests.get(...)` calls in `download_bulk_csv` and `fetch_bill_detail` with `_get_with_retry(url, headers=headers)` (keep the existing headers dicts).

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_fetch_tal.py -v
```

Expected: all 10 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/data/recurring_bills/fetch_tal.py tests/test_recurring_bills_fetch_tal.py
git commit -m "feat(recurring_bills): 3x retry with exponential backoff on 5xx

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: `classify.py` — load Tal bulk + details into DataFrame

**Files:**
- Create: `src/data/recurring_bills/classify.py`
- Create: `tests/test_recurring_bills_classify.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_recurring_bills_classify.py`:

```python
"""Unit tests for src/data/recurring_bills/classify.py."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from data.recurring_bills.classify import build_tal_classifications


FIXTURES = Path(__file__).parent / "fixtures" / "recurring_bills"


class TestBuildTalClassifications:
    def test_columns_and_source(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        for bid in (477119, 477120, 477137):
            src = FIXTURES / f"tal_detail_{bid}.json"
            (cache_dir / f"{bid}.json").write_text(src.read_text())

        df = build_tal_classifications(
            bulk_csv=FIXTURES / "tal_bulk_sample.csv",
            cache_dir=cache_dir,
        )

        expected_cols = {
            "BillID", "KnessetNum", "Name",
            "is_original", "original_bill_id",
            "tal_category", "is_cross_term", "is_within_term_dup", "is_self_resubmission",
            "family_size", "predecessor_bill_ids",
            "classification_source", "tal_fetched_at",
        }
        assert expected_cols.issubset(df.columns)
        assert (df["classification_source"] == "tal_alovitz").all()

    def test_original_bill_id_from_patient_zero(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        for bid in (477119, 477120, 477137):
            src = FIXTURES / f"tal_detail_{bid}.json"
            (cache_dir / f"{bid}.json").write_text(src.read_text())

        df = build_tal_classifications(
            bulk_csv=FIXTURES / "tal_bulk_sample.csv",
            cache_dir=cache_dir,
        )

        row_new = df.loc[df["BillID"] == 477119].iloc[0]
        row_reprise = df.loc[df["BillID"] == 477137].iloc[0]

        assert row_new["is_original"] is True or row_new["is_original"] == 1
        assert row_new["original_bill_id"] == 477119

        assert row_reprise["is_original"] is False or row_reprise["is_original"] == 0
        assert row_reprise["original_bill_id"] == 477119

    def test_missing_detail_json_defaults_to_self(self, tmp_path: Path):
        """Bill in bulk CSV but no detail fetched — fall back to self-reference."""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        # Only fetch 477119, skip the others
        (cache_dir / "477119.json").write_text((FIXTURES / "tal_detail_477119.json").read_text())

        df = build_tal_classifications(
            bulk_csv=FIXTURES / "tal_bulk_sample.csv",
            cache_dir=cache_dir,
        )

        row_no_detail = df.loc[df["BillID"] == 477120].iloc[0]
        assert row_no_detail["original_bill_id"] == 477120  # self — no patient_zero known
        assert pd.isna(row_no_detail["family_size"])
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_classify.py::TestBuildTalClassifications -v
```

Expected: `ImportError: No module named 'data.recurring_bills.classify'`.

- [ ] **Step 3: Write `build_tal_classifications`**

Create `src/data/recurring_bills/classify.py`:

```python
"""Merge Tal Alovitz's classifications + K16-K18 fallback into one DataFrame."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)


def build_tal_classifications(
    *,
    bulk_csv: Path,
    cache_dir: Path,
) -> pd.DataFrame:
    """Build the tal_alovitz slice of the classifications table.

    Reads the bulk CSV for every bill's summary fields, then overlays each
    bill's cached detail JSON for ``patient_zero_bill_id``, ``predecessor_bill_ids``,
    and ``family_size``. Bills with no cached detail fall back to
    ``original_bill_id = bill_id`` (self) and NULL family info.
    """
    bulk = pd.read_csv(bulk_csv)
    bulk = bulk.rename(columns={"bill_id": "BillID", "knesset_num": "KnessetNum", "bill_name": "Name"})

    detail_rows: dict[int, dict] = {}
    cache_dir = Path(cache_dir)
    if cache_dir.exists():
        for path in cache_dir.glob("*.json"):
            try:
                payload = json.loads(path.read_text())
                detail_rows[int(payload["bill_id"])] = payload
            except (json.JSONDecodeError, KeyError) as exc:
                log.warning("Skipping malformed detail cache %s: %s", path, exc)

    def _patient_zero(row: pd.Series) -> int:
        d = detail_rows.get(int(row["BillID"]))
        if d and d.get("patient_zero_bill_id") is not None:
            return int(d["patient_zero_bill_id"])
        return int(row["BillID"])

    def _predecessors(row: pd.Series) -> list[int]:
        d = detail_rows.get(int(row["BillID"]))
        if d and d.get("predecessor_bill_ids"):
            return [int(x) for x in d["predecessor_bill_ids"]]
        return []

    def _family_size(row: pd.Series):
        d = detail_rows.get(int(row["BillID"]))
        return int(d["family_size"]) if d and d.get("family_size") is not None else None

    bulk["is_original"] = bulk["is_original"].astype(bool)
    bulk["is_cross_term"] = bulk["is_cross_term"].astype(bool)
    bulk["is_within_term_dup"] = bulk["is_within_term_dup"].astype(bool)
    bulk["is_self_resubmission"] = bulk["is_self_resubmission"].astype(bool)
    bulk["original_bill_id"] = bulk.apply(_patient_zero, axis=1)
    bulk["predecessor_bill_ids"] = bulk.apply(_predecessors, axis=1)
    bulk["family_size"] = bulk.apply(_family_size, axis=1)
    bulk["tal_category"] = bulk["category"]
    bulk["classification_source"] = "tal_alovitz"
    bulk["tal_fetched_at"] = datetime.now(timezone.utc)

    keep = [
        "BillID", "KnessetNum", "Name",
        "is_original", "original_bill_id",
        "tal_category", "is_cross_term", "is_within_term_dup", "is_self_resubmission",
        "family_size", "predecessor_bill_ids",
        "classification_source", "tal_fetched_at",
    ]
    return bulk[keep].copy()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_classify.py -v
```

Expected: all 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/data/recurring_bills/classify.py tests/test_recurring_bills_classify.py
git commit -m "feat(recurring_bills): build_tal_classifications merges bulk + details

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: `classify.py` — K16-K18 fallback

**Files:**
- Modify: `src/data/recurring_bills/classify.py` (add `build_k16_k18_fallback`)
- Modify: `tests/test_recurring_bills_classify.py` (add `TestFallback`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_recurring_bills_classify.py`:

```python
from data.recurring_bills.classify import build_k16_k18_fallback


class TestK16K18Fallback:
    def test_earliest_knesset_is_original(self):
        df = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")

        # Group 1: BillID 10001 (K16), 10002 (K17), 10003 (K18) — chinuch-chova
        row_earliest = df.loc[df["BillID"] == 10001].iloc[0]
        row_mid = df.loc[df["BillID"] == 10002].iloc[0]
        row_last = df.loc[df["BillID"] == 10003].iloc[0]

        assert row_earliest["is_original"] == True
        assert row_earliest["original_bill_id"] == 10001
        assert row_mid["is_original"] == False
        assert row_mid["original_bill_id"] == 10001
        assert row_last["is_original"] == False
        assert row_last["original_bill_id"] == 10001

    def test_singleton_group_is_original(self):
        df = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")
        row = df.loc[df["BillID"] == 10004].iloc[0]  # bituach le'umi — singleton
        assert row["is_original"] == True
        assert row["original_bill_id"] == 10004

    def test_same_knesset_tie_breaker_lowest_billid(self):
        df = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")
        # Both in K18, same name: 10007 (lower) and 10008
        row_lower = df.loc[df["BillID"] == 10007].iloc[0]
        row_higher = df.loc[df["BillID"] == 10008].iloc[0]
        assert row_lower["is_original"] == True
        assert row_higher["is_original"] == False
        assert row_higher["original_bill_id"] == 10007

    def test_source_is_name_fallback(self):
        df = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")
        assert (df["classification_source"] == "name_fallback_k16_k18").all()

    def test_tal_specific_columns_are_null(self):
        df = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")
        for col in ["tal_category", "is_cross_term", "is_within_term_dup",
                    "is_self_resubmission", "family_size", "tal_fetched_at"]:
            assert df[col].isna().all()

    def test_predecessor_list_contains_original_for_reprises(self):
        df = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")
        row_mid = df.loc[df["BillID"] == 10002].iloc[0]
        assert row_mid["predecessor_bill_ids"] == [10001]

        row_earliest = df.loc[df["BillID"] == 10001].iloc[0]
        assert row_earliest["predecessor_bill_ids"] == []
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_classify.py::TestK16K18Fallback -v
```

Expected: `ImportError: cannot import name 'build_k16_k18_fallback'`.

- [ ] **Step 3: Implement `build_k16_k18_fallback`**

Append to `src/data/recurring_bills/classify.py`:

```python
from data.recurring_bills.normalize import normalize_name


def build_k16_k18_fallback(excel_path: Path) -> pd.DataFrame:
    """Name-based fallback classification for K16-K18 bills from Amnon's Excel.

    Groups rows by ``normalize_name(Name)`` restricted to K16-K18. Within each
    group the earliest ``KnessetNum`` is marked original (ties broken by
    lowest ``BillID``); all others are reprises with ``original_bill_id``
    pointing to that earliest row.
    """
    xl = pd.read_excel(excel_path)
    xl = xl.loc[xl["KnessetNum"].between(16, 18)].copy()
    xl["_norm"] = xl["Name"].apply(normalize_name)

    # Sort so groupby picks earliest KnessetNum, then lowest BillID
    xl = xl.sort_values(["_norm", "KnessetNum", "BillID"], kind="stable")

    # First row in each normalized-name group is the original
    originals = xl.drop_duplicates("_norm", keep="first")[["_norm", "BillID"]].rename(
        columns={"BillID": "original_bill_id"}
    )
    xl = xl.merge(originals, on="_norm", how="left")
    xl["is_original"] = xl["BillID"] == xl["original_bill_id"]

    xl["predecessor_bill_ids"] = xl.apply(
        lambda r: [] if r["is_original"] else [int(r["original_bill_id"])],
        axis=1,
    )
    xl["classification_source"] = "name_fallback_k16_k18"
    xl["tal_category"] = None
    xl["is_cross_term"] = None
    xl["is_within_term_dup"] = None
    xl["is_self_resubmission"] = None
    xl["family_size"] = None
    xl["tal_fetched_at"] = None

    keep = [
        "BillID", "KnessetNum", "Name",
        "is_original", "original_bill_id",
        "tal_category", "is_cross_term", "is_within_term_dup", "is_self_resubmission",
        "family_size", "predecessor_bill_ids",
        "classification_source", "tal_fetched_at",
    ]
    return xl[keep].reset_index(drop=True)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_classify.py -v
```

Expected: all 9 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/data/recurring_bills/classify.py tests/test_recurring_bills_classify.py
git commit -m "feat(recurring_bills): K16-K18 name-match fallback classifier

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: `classify.py` — merge_all

**Files:**
- Modify: `src/data/recurring_bills/classify.py` (add `merge_all`)
- Modify: `tests/test_recurring_bills_classify.py` (add `TestMergeAll`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_recurring_bills_classify.py`:

```python
from data.recurring_bills.classify import merge_all


class TestMergeAll:
    def test_union_preserves_both_sources(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        for bid in (477119, 477120, 477137):
            (cache_dir / f"{bid}.json").write_text((FIXTURES / f"tal_detail_{bid}.json").read_text())

        tal = build_tal_classifications(
            bulk_csv=FIXTURES / "tal_bulk_sample.csv",
            cache_dir=cache_dir,
        )
        fb = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")

        merged = merge_all(tal=tal, fallback=fb)

        assert len(merged) == len(tal) + len(fb)
        assert set(merged["classification_source"].unique()) == {"tal_alovitz", "name_fallback_k16_k18"}

    def test_dedup_prefers_tal_over_fallback_on_collision(self, tmp_path: Path):
        """If the same BillID appears in both frames (shouldn't happen in real life,
        since Tal is K19-K25 and fallback is K16-K18), prefer Tal."""
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        (cache_dir / "10001.json").write_text(json.dumps({
            "bill_id": 10001, "patient_zero_bill_id": 99999,
            "family_size": 3, "predecessor_bill_ids": [99999], "category": "cross",
        }))
        tal = pd.DataFrame([{
            "BillID": 10001, "KnessetNum": 19, "Name": "x",
            "is_original": False, "original_bill_id": 99999,
            "tal_category": "cross", "is_cross_term": True, "is_within_term_dup": False,
            "is_self_resubmission": False, "family_size": 3, "predecessor_bill_ids": [99999],
            "classification_source": "tal_alovitz", "tal_fetched_at": pd.Timestamp.utcnow(),
        }])
        fb = build_k16_k18_fallback(FIXTURES / "excel_sample.xlsx")
        # Manually force a collision: rewrite fallback row BillID to match Tal's
        fb.loc[0, "BillID"] = 10001

        merged = merge_all(tal=tal, fallback=fb)
        collision = merged.loc[merged["BillID"] == 10001].iloc[0]
        assert collision["classification_source"] == "tal_alovitz"
        assert collision["original_bill_id"] == 99999
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_classify.py::TestMergeAll -v
```

Expected: `ImportError: cannot import name 'merge_all'`.

- [ ] **Step 3: Implement `merge_all`**

Append to `src/data/recurring_bills/classify.py`:

```python
def merge_all(*, tal: pd.DataFrame, fallback: pd.DataFrame) -> pd.DataFrame:
    """Union the Tal and K16-K18 fallback DataFrames.

    On BillID collisions (rare — Tal and fallback are supposed to be disjoint),
    Tal's row wins. Returns a stable-sorted DataFrame (by BillID) ready for
    writing.
    """
    fallback_filtered = fallback.loc[~fallback["BillID"].isin(set(tal["BillID"]))]
    combined = pd.concat([tal, fallback_filtered], ignore_index=True)
    return combined.sort_values("BillID", kind="stable").reset_index(drop=True)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_classify.py -v
```

Expected: all 11 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/data/recurring_bills/classify.py tests/test_recurring_bills_classify.py
git commit -m "feat(recurring_bills): merge_all unions Tal + fallback, Tal-wins on collision

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: `storage.py` — write DuckDB table + Parquet snapshot

**Files:**
- Create: `src/data/recurring_bills/storage.py`
- Modify: `tests/test_recurring_bills_classify.py` (add `TestStorage`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_recurring_bills_classify.py`:

```python
import duckdb

from data.recurring_bills.storage import write_duckdb_table, write_parquet_snapshot


class TestStorage:
    def _fixture_df(self) -> pd.DataFrame:
        return pd.DataFrame([
            {"BillID": 1, "KnessetNum": 20, "Name": "a", "is_original": True,
             "original_bill_id": 1, "tal_category": "new", "is_cross_term": False,
             "is_within_term_dup": False, "is_self_resubmission": False,
             "family_size": 1, "predecessor_bill_ids": [],
             "classification_source": "tal_alovitz",
             "tal_fetched_at": pd.Timestamp("2026-04-17", tz="UTC")},
            {"BillID": 2, "KnessetNum": 21, "Name": "b", "is_original": False,
             "original_bill_id": 1, "tal_category": "cross", "is_cross_term": True,
             "is_within_term_dup": False, "is_self_resubmission": False,
             "family_size": 2, "predecessor_bill_ids": [1],
             "classification_source": "tal_alovitz",
             "tal_fetched_at": pd.Timestamp("2026-04-17", tz="UTC")},
        ])

    def test_write_duckdb_creates_table_and_rows(self, tmp_path: Path):
        db = tmp_path / "w.duckdb"
        df = self._fixture_df()
        write_duckdb_table(df, db_path=db)

        con = duckdb.connect(str(db), read_only=True)
        count = con.execute("SELECT COUNT(*) FROM bill_classifications").fetchone()[0]
        con.close()
        assert count == 2

    def test_write_duckdb_replaces_on_rerun(self, tmp_path: Path):
        db = tmp_path / "w.duckdb"
        write_duckdb_table(self._fixture_df(), db_path=db)
        write_duckdb_table(self._fixture_df().head(1), db_path=db)  # smaller

        con = duckdb.connect(str(db), read_only=True)
        count = con.execute("SELECT COUNT(*) FROM bill_classifications").fetchone()[0]
        con.close()
        assert count == 1

    def test_write_parquet_atomic_and_idempotent(self, tmp_path: Path):
        out = tmp_path / "bill_classifications.parquet"
        df = self._fixture_df()

        write_parquet_snapshot(df, out)
        first_bytes = out.read_bytes()

        write_parquet_snapshot(df, out)
        assert out.read_bytes() == first_bytes  # byte-idempotent
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_classify.py::TestStorage -v
```

Expected: `ImportError: No module named 'data.recurring_bills.storage'`.

- [ ] **Step 3: Implement `storage.py`**

Create `src/data/recurring_bills/storage.py`:

```python
"""DuckDB table writer + atomic Parquet snapshot writer."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

TABLE_NAME = "bill_classifications"


def write_duckdb_table(df: pd.DataFrame, *, db_path: Path) -> None:
    """Replace the ``bill_classifications`` table with ``df`` contents.

    Uses DuckDB's DataFrame registration to avoid row-by-row inserts.
    Table is dropped and recreated each call (cheap — ~25K rows).
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    df_stable = df.sort_values("BillID", kind="stable").reset_index(drop=True)

    con = duckdb.connect(str(db_path), read_only=False)
    try:
        con.register("df_in", df_stable)
        con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM df_in ORDER BY BillID")
        con.unregister("df_in")
    finally:
        con.close()
    log.info("Wrote %d rows to DuckDB table %s", len(df_stable), TABLE_NAME)


def write_parquet_snapshot(df: pd.DataFrame, output_path: Path) -> None:
    """Write DataFrame to Parquet with atomic rename + stable ORDER BY.

    Replicates the idempotence pattern from src/data/snapshots/exporter.py:
    write to ``<path>.new`` then ``os.replace`` into place.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df_stable = df.sort_values("BillID", kind="stable").reset_index(drop=True)
    table = pa.Table.from_pandas(df_stable, preserve_index=False)

    tmp = output_path.with_suffix(output_path.suffix + ".new")
    pq.write_table(table, tmp, compression="zstd", use_dictionary=True)
    os.replace(tmp, output_path)
    log.info("Wrote Parquet snapshot: %s (%d rows)", output_path, len(df_stable))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_classify.py -v
```

Expected: all 14 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/data/recurring_bills/storage.py tests/test_recurring_bills_classify.py
git commit -m "feat(recurring_bills): DuckDB + atomic Parquet writers

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: `report.py` — coverage statistics + markdown render

**Files:**
- Create: `src/data/recurring_bills/report.py`
- Create: `tests/test_recurring_bills_report.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_recurring_bills_report.py`:

```python
"""Unit tests for src/data/recurring_bills/report.py."""

from __future__ import annotations

import pandas as pd

from data.recurring_bills.report import compute_stats, render_markdown


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame([
        {"BillID": 1, "KnessetNum": 17, "Name": "a", "is_original": True,
         "original_bill_id": 1, "classification_source": "name_fallback_k16_k18",
         "predecessor_bill_ids": []},
        {"BillID": 2, "KnessetNum": 18, "Name": "a", "is_original": False,
         "original_bill_id": 1, "classification_source": "name_fallback_k16_k18",
         "predecessor_bill_ids": [1]},
        {"BillID": 3, "KnessetNum": 20, "Name": "b", "is_original": True,
         "original_bill_id": 3, "classification_source": "tal_alovitz",
         "predecessor_bill_ids": []},
        {"BillID": 4, "KnessetNum": 21, "Name": "b", "is_original": False,
         "original_bill_id": 3, "classification_source": "tal_alovitz",
         "predecessor_bill_ids": [3]},
    ])


class TestComputeStats:
    def test_total_counts(self):
        stats = compute_stats(_sample_df())
        assert stats["total"] == 4
        assert stats["by_source"]["tal_alovitz"] == 2
        assert stats["by_source"]["name_fallback_k16_k18"] == 2

    def test_original_vs_recurring_split(self):
        stats = compute_stats(_sample_df())
        assert stats["originals"] == 2
        assert stats["recurring"] == 2

    def test_per_knesset_breakdown(self):
        stats = compute_stats(_sample_df())
        assert stats["by_knesset"][17]["total"] == 1
        assert stats["by_knesset"][20]["originals"] == 1


class TestRenderMarkdown:
    def test_includes_summary_headers(self):
        md = render_markdown(compute_stats(_sample_df()))
        assert "# Recurring Bills Classification Coverage" in md
        assert "## Summary" in md
        assert "## By Knesset" in md
        assert "## By Source" in md

    def test_renders_counts_in_body(self):
        md = render_markdown(compute_stats(_sample_df()))
        assert "Total bills classified | 4" in md or "| 4 |" in md
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_report.py -v
```

Expected: `ImportError: No module named 'data.recurring_bills.report'`.

- [ ] **Step 3: Implement `report.py`**

Create `src/data/recurring_bills/report.py`:

```python
"""Build a markdown coverage report from the classified DataFrame."""

from __future__ import annotations

from io import StringIO

import pandas as pd


def compute_stats(df: pd.DataFrame) -> dict:
    """Compute summary statistics for the coverage report.

    Returns a dict shaped::

        {
          "total": int,
          "originals": int,
          "recurring": int,
          "by_source": {source: count},
          "by_knesset": {knesset_num: {"total": int, "originals": int, "recurring": int}},
        }
    """
    return {
        "total": int(len(df)),
        "originals": int(df["is_original"].sum()),
        "recurring": int((~df["is_original"]).sum()),
        "by_source": df["classification_source"].value_counts().to_dict(),
        "by_knesset": {
            int(kn): {
                "total": int(len(group)),
                "originals": int(group["is_original"].sum()),
                "recurring": int((~group["is_original"]).sum()),
            }
            for kn, group in df.groupby("KnessetNum")
        },
    }


def render_markdown(stats: dict) -> str:
    """Render stats dict to a human-readable markdown report."""
    buf = StringIO()
    buf.write("# Recurring Bills Classification Coverage\n\n")
    buf.write("## Summary\n\n")
    buf.write("| Metric | Value |\n")
    buf.write("|---|---|\n")
    buf.write(f"| Total bills classified | {stats['total']} |\n")
    buf.write(f"| Originals | {stats['originals']} |\n")
    buf.write(f"| Recurring | {stats['recurring']} |\n")
    if stats["total"]:
        pct = 100 * stats["recurring"] / stats["total"]
        buf.write(f"| Recurring % | {pct:.1f} |\n")
    buf.write("\n")

    buf.write("## By Source\n\n| Source | Count |\n|---|---|\n")
    for source, count in sorted(stats["by_source"].items()):
        buf.write(f"| {source} | {count} |\n")
    buf.write("\n")

    buf.write("## By Knesset\n\n| Knesset | Total | Originals | Recurring |\n|---|---|---|---|\n")
    for kn in sorted(stats["by_knesset"]):
        row = stats["by_knesset"][kn]
        buf.write(f"| K{kn} | {row['total']} | {row['originals']} | {row['recurring']} |\n")
    buf.write("\n")
    return buf.getvalue()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_report.py -v
```

Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/data/recurring_bills/report.py tests/test_recurring_bills_report.py
git commit -m "feat(recurring_bills): compute_stats + render_markdown coverage report

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: `pipeline.py` — orchestrator

**Files:**
- Create: `src/data/recurring_bills/pipeline.py`
- Modify: `tests/test_recurring_bills_classify.py` (add `TestPipeline` with mocked fetches)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_recurring_bills_classify.py`:

```python
from unittest.mock import patch

from data.recurring_bills.pipeline import run_pipeline


class TestPipeline:
    def test_rebuild_mode_skips_network_and_builds_outputs(self, tmp_path: Path):
        # Stage fixtures into a working data/ tree
        work = tmp_path
        (work / "external" / "tal_bill_details").mkdir(parents=True)
        for bid in (477119, 477120, 477137):
            (work / "external" / "tal_bill_details" / f"{bid}.json").write_text(
                (FIXTURES / f"tal_detail_{bid}.json").read_text()
            )
        (work / "external" / "tal_alovitz_bills.csv").write_bytes(
            (FIXTURES / "tal_bulk_sample.csv").read_bytes()
        )

        out_parquet = work / "snapshots" / "bill_classifications.parquet"
        out_report = work / "recurring_bills" / "coverage_report.md"
        db = work / "warehouse.duckdb"

        result = run_pipeline(
            mode="rebuild",
            excel_path=FIXTURES / "excel_sample.xlsx",
            cache_dir=work / "external" / "tal_bill_details",
            bulk_csv=work / "external" / "tal_alovitz_bills.csv",
            db_path=db,
            parquet_path=out_parquet,
            report_path=out_report,
        )

        assert result["total"] >= 10  # 6 from Tal fixture + K16-K18 fallback
        assert out_parquet.exists()
        assert out_report.exists()
        assert "# Recurring Bills Classification Coverage" in out_report.read_text()

    def test_refresh_mode_triggers_fetch(self, tmp_path: Path):
        """refresh mode should call download_bulk_csv + fetch_many_details."""
        with patch("data.recurring_bills.pipeline.download_bulk_csv") as mock_bulk, \
             patch("data.recurring_bills.pipeline.fetch_many_details") as mock_many:
            mock_bulk.return_value = FIXTURES / "tal_bulk_sample.csv"
            mock_many.return_value = []

            # Stage cache_dir fixtures so classify still works
            cache_dir = tmp_path / "cache"
            cache_dir.mkdir()
            for bid in (477119, 477120, 477137):
                (cache_dir / f"{bid}.json").write_text(
                    (FIXTURES / f"tal_detail_{bid}.json").read_text()
                )

            run_pipeline(
                mode="refresh",
                excel_path=FIXTURES / "excel_sample.xlsx",
                cache_dir=cache_dir,
                bulk_csv=tmp_path / "bulk.csv",
                db_path=tmp_path / "w.duckdb",
                parquet_path=tmp_path / "snap.parquet",
                report_path=tmp_path / "report.md",
                delay_s=0,
            )

        assert mock_bulk.call_count == 1
        assert mock_many.call_count == 1
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_classify.py::TestPipeline -v
```

Expected: `ImportError: No module named 'data.recurring_bills.pipeline'`.

- [ ] **Step 3: Implement `pipeline.py`**

Create `src/data/recurring_bills/pipeline.py`:

```python
"""Orchestrator: fetch -> classify -> write outputs."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from data.recurring_bills.classify import (
    build_k16_k18_fallback,
    build_tal_classifications,
    merge_all,
)
from data.recurring_bills.fetch_tal import download_bulk_csv, fetch_many_details
from data.recurring_bills.report import compute_stats, render_markdown
from data.recurring_bills.storage import write_duckdb_table, write_parquet_snapshot

log = logging.getLogger(__name__)


def _bills_needing_detail(bulk_csv: Path) -> list[int]:
    """Return bill_ids from the bulk CSV with is_original = 0 (need patient_zero lookup)."""
    df = pd.read_csv(bulk_csv, usecols=["bill_id", "is_original"])
    return df.loc[df["is_original"] == 0, "bill_id"].astype(int).tolist()


def run_pipeline(
    *,
    mode: str,
    excel_path: Path,
    cache_dir: Path,
    bulk_csv: Path,
    db_path: Path,
    parquet_path: Path,
    report_path: Path,
    delay_s: float = 0.3,
    force_refresh: bool = False,
) -> dict:
    """Top-level orchestrator.

    Modes:
    - ``refresh``  : pull latest bulk CSV + fetch any missing detail, then classify + write
    - ``rebuild``  : classify from existing cache, write outputs (no network)
    - ``report``   : classify + write only the coverage report (skip DuckDB + Parquet)
    """
    assert mode in {"refresh", "rebuild", "report"}

    if mode == "refresh":
        download_bulk_csv(bulk_csv)
        needing = _bills_needing_detail(bulk_csv)
        log.info("Fetching detail for %d recurring bills (delay=%.1fs)", len(needing), delay_s)
        fetch_many_details(needing, cache_dir, delay_s=delay_s, force_refresh=force_refresh)

    tal = build_tal_classifications(bulk_csv=bulk_csv, cache_dir=cache_dir)
    fb = build_k16_k18_fallback(excel_path)
    df = merge_all(tal=tal, fallback=fb)

    stats = compute_stats(df)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_markdown(stats))

    if mode != "report":
        write_parquet_snapshot(df, parquet_path)
        write_duckdb_table(df, db_path=db_path)

    log.info("Pipeline %s complete: %d bills", mode, stats["total"])
    return stats
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_classify.py -v
```

Expected: all 16 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/data/recurring_bills/pipeline.py tests/test_recurring_bills_classify.py
git commit -m "feat(recurring_bills): pipeline orchestrator with refresh/rebuild/report modes

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: `scripts/classify_recurring_bills.py` — CLI wrapper

**Files:**
- Create: `scripts/classify_recurring_bills.py`

- [ ] **Step 1: Write the CLI**

Create `scripts/classify_recurring_bills.py`:

```python
#!/usr/bin/env python
"""CLI wrapper for src/data/recurring_bills/pipeline.py.

Run with:
    PYTHONPATH="./src" python scripts/classify_recurring_bills.py <mode> [options]

Modes:
    refresh   Pull latest from pmb.teca-it.com, rebuild outputs.
    rebuild   Rebuild outputs from existing cache (no network).
    report    Recompute coverage_report.md only.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure src/ is on the path if the user forgot PYTHONPATH
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from data.recurring_bills.pipeline import run_pipeline  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["refresh", "rebuild", "report"])
    parser.add_argument("--excel", type=Path,
                        default=_REPO_ROOT / "data" / "Private.Bills.Final.091123.xlsx")
    parser.add_argument("--cache-dir", type=Path,
                        default=_REPO_ROOT / "data" / "external" / "tal_bill_details")
    parser.add_argument("--bulk-csv", type=Path,
                        default=_REPO_ROOT / "data" / "external" / "tal_alovitz_bills.csv")
    parser.add_argument("--db", type=Path,
                        default=_REPO_ROOT / "data" / "warehouse.duckdb")
    parser.add_argument("--parquet", type=Path,
                        default=_REPO_ROOT / "data" / "snapshots" / "bill_classifications.parquet")
    parser.add_argument("--report", type=Path,
                        default=_REPO_ROOT / "data" / "recurring_bills" / "coverage_report.md")
    parser.add_argument("--delay", type=float, default=0.3,
                        help="Seconds between HTTP requests (refresh mode only)")
    parser.add_argument("--force-refresh", action="store_true",
                        help="Ignore detail cache, re-fetch every bill")
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    stats = run_pipeline(
        mode=args.mode,
        excel_path=args.excel,
        cache_dir=args.cache_dir,
        bulk_csv=args.bulk_csv,
        db_path=args.db,
        parquet_path=args.parquet,
        report_path=args.report,
        delay_s=args.delay,
        force_refresh=args.force_refresh,
    )

    print(f"\nClassified {stats['total']} bills:")
    print(f"  Originals: {stats['originals']}")
    print(f"  Recurring: {stats['recurring']}")
    print(f"  Coverage report: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Smoke test the CLI invocation (dry path — rebuild mode with empty cache)**

```bash
chmod +x scripts/classify_recurring_bills.py
PYTHONPATH="./src" python scripts/classify_recurring_bills.py --help
```

Expected: help text prints with all 3 modes listed.

- [ ] **Step 3: Commit**

```bash
git add scripts/classify_recurring_bills.py
git commit -m "feat(recurring_bills): argparse CLI wrapper at scripts/classify_recurring_bills.py

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 13: CAP integration view

**Files:**
- Create: `src/data/recurring_bills/cap_view.py`
- Modify: `tests/test_recurring_bills_classify.py` (add `TestCapView`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_recurring_bills_classify.py`:

```python
from data.recurring_bills.cap_view import create_cap_view


class TestCapView:
    def test_creates_view_with_expected_columns(self, tmp_path: Path):
        db = tmp_path / "w.duckdb"
        con = duckdb.connect(str(db), read_only=False)
        con.execute("CREATE TABLE UserBillCAP (BillID INTEGER, CAPMinorCode VARCHAR)")
        con.execute("INSERT INTO UserBillCAP VALUES (1, '100'), (2, '200')")
        con.execute("""
            CREATE TABLE bill_classifications (
                BillID INTEGER PRIMARY KEY, KnessetNum INTEGER, Name VARCHAR,
                is_original BOOLEAN, original_bill_id INTEGER, tal_category VARCHAR,
                classification_source VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO bill_classifications VALUES
            (1, 20, 'a', TRUE, 1, 'new', 'tal_alovitz'),
            (2, 21, 'a', FALSE, 1, 'cross', 'tal_alovitz')
        """)
        con.close()

        create_cap_view(db_path=db)

        con = duckdb.connect(str(db), read_only=True)
        rows = con.execute("""
            SELECT BillID, CAPMinorCode, is_original, original_bill_id
            FROM v_cap_bills_with_recurrence ORDER BY BillID
        """).fetchall()
        con.close()

        assert rows == [(1, '100', True, 1), (2, '200', False, 1)]

    def test_view_handles_cap_bills_without_classification(self, tmp_path: Path):
        """Bills in UserBillCAP that aren't in bill_classifications get NULL."""
        db = tmp_path / "w.duckdb"
        con = duckdb.connect(str(db), read_only=False)
        con.execute("CREATE TABLE UserBillCAP (BillID INTEGER, CAPMinorCode VARCHAR)")
        con.execute("INSERT INTO UserBillCAP VALUES (99, 'xxx')")
        con.execute("""
            CREATE TABLE bill_classifications (
                BillID INTEGER PRIMARY KEY, KnessetNum INTEGER, Name VARCHAR,
                is_original BOOLEAN, original_bill_id INTEGER, tal_category VARCHAR,
                classification_source VARCHAR
            )
        """)
        con.close()

        create_cap_view(db_path=db)

        con = duckdb.connect(str(db), read_only=True)
        row = con.execute(
            "SELECT is_original FROM v_cap_bills_with_recurrence WHERE BillID = 99"
        ).fetchone()
        con.close()
        assert row == (None,)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_classify.py::TestCapView -v
```

Expected: `ImportError: No module named 'data.recurring_bills.cap_view'`.

- [ ] **Step 3: Implement `cap_view.py`**

Create `src/data/recurring_bills/cap_view.py`:

```python
"""CAP integration: expose bill_classifications alongside UserBillCAP via a view."""

from __future__ import annotations

from pathlib import Path

import duckdb


VIEW_SQL = """
CREATE OR REPLACE VIEW v_cap_bills_with_recurrence AS
SELECT
    ubc.*,
    bc.is_original,
    bc.original_bill_id,
    bc.tal_category,
    bc.classification_source
FROM UserBillCAP ubc
LEFT JOIN bill_classifications bc USING (BillID)
"""


def create_cap_view(*, db_path: Path) -> None:
    """Create or replace ``v_cap_bills_with_recurrence`` in the warehouse.

    Expects ``UserBillCAP`` and ``bill_classifications`` tables to exist.
    """
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        con.execute(VIEW_SQL)
    finally:
        con.close()
```

- [ ] **Step 4: Wire `create_cap_view` into the pipeline**

Add to `src/data/recurring_bills/pipeline.py` after `write_duckdb_table` call:

```python
from data.recurring_bills.cap_view import create_cap_view
```

Add at module top. Then in `run_pipeline`, after `write_duckdb_table(df, db_path=db_path)`:

```python
        try:
            create_cap_view(db_path=db_path)
        except duckdb.CatalogException:
            log.warning("UserBillCAP not present in warehouse; skipping CAP view creation")
```

Add at top of `pipeline.py`:

```python
import duckdb
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
PYTHONPATH="./src" pytest tests/test_recurring_bills_classify.py -v
```

Expected: all 18 tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/data/recurring_bills/cap_view.py src/data/recurring_bills/pipeline.py tests/test_recurring_bills_classify.py
git commit -m "feat(recurring_bills): create v_cap_bills_with_recurrence view

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 14: Full suite run + CLAUDE.md pointer

**Files:**
- Modify: `CLAUDE.md` (append 2 lines under "Commands")

- [ ] **Step 1: Run the full fast test suite**

```bash
PYTHONPATH="./src" pytest tests/ --ignore=tests/test_api_integration.py --ignore=tests/test_e2e.py --ignore=tests/test_data_pipeline_integration.py --ignore=tests/test_connection_leaks.py --tb=short -q
```

Expected: all existing tests + our 18 new tests pass. Watch for any regressions caused by our changes.

- [ ] **Step 2: Update CLAUDE.md**

In `CLAUDE.md`, under the `## Commands` section (after the existing `# Data refresh` block), append:

```markdown

# Recurring-bills classification (ingests Dr. Tal Alovitz's dataset)
PYTHONPATH="./src" python scripts/classify_recurring_bills.py rebuild  # fast, no network
PYTHONPATH="./src" python scripts/classify_recurring_bills.py refresh  # ~90 min first run
```

Also append a row to the **Key File References → Scripts** table:

```markdown
| **Recurring Bills** | `src/data/recurring_bills/` (ingests pmb.teca-it.com; outputs `bill_classifications` table + `data/snapshots/bill_classifications.parquet`) |
```

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add recurring_bills pipeline to CLAUDE.md commands + references

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 15: First real end-to-end run (manual validation)

**Files:** None to create. This is an integration smoke test.

- [ ] **Step 1: Kick off the long-running refresh**

```bash
PYTHONPATH="./src" python scripts/classify_recurring_bills.py refresh --log-level INFO 2>&1 | tee /tmp/recurring_bills_refresh.log
```

Expected: runs for ~90 minutes. Periodically prints `Fetching detail for 16738 recurring bills (delay=0.3s)` and progress. If a 5xx rash happens, retries log warnings.

- [ ] **Step 2: Validate outputs**

```bash
# Row count sanity
PYTHONPATH="./src" python -c "
import duckdb
con = duckdb.connect('data/warehouse.duckdb', read_only=True)
print('Total:', con.execute('SELECT COUNT(*) FROM bill_classifications').fetchone()[0])
print('By source:')
for row in con.execute('SELECT classification_source, COUNT(*) FROM bill_classifications GROUP BY 1').fetchall():
    print(' ', row)
print('Originals vs recurring:')
for row in con.execute('SELECT is_original, COUNT(*) FROM bill_classifications GROUP BY 1').fetchall():
    print(' ', row)
print('CAP view sanity:')
print(con.execute('SELECT COUNT(*) FROM v_cap_bills_with_recurrence WHERE is_original').fetchone())
"
```

Expected shape: total ≈ 24,225 (Tal) + 7,265 (fallback) = ~31,490 rows; Tal source ≈ 24,225; fallback ≈ 7,265; originals + recurring sum to total. CAP view returns a non-zero count.

- [ ] **Step 3: Eyeball the coverage report**

```bash
cat data/recurring_bills/coverage_report.md
```

Expected: markdown with Summary / By Source / By Knesset sections. Numbers match step 2.

- [ ] **Step 4: Verify Parquet idempotence**

```bash
# Rerun rebuild mode — no network, should produce byte-identical Parquet
md5 data/snapshots/bill_classifications.parquet
PYTHONPATH="./src" python scripts/classify_recurring_bills.py rebuild
md5 data/snapshots/bill_classifications.parquet
```

Expected: both MD5 sums match.

- [ ] **Step 5: Commit the coverage report**

```bash
# Coverage report is under data/recurring_bills/ which is gitignored,
# so we copy it somewhere trackable for the spec-review trail:
cp data/recurring_bills/coverage_report.md docs/superpowers/specs/2026-04-17-recurring-bills-coverage-report.md
git add docs/superpowers/specs/2026-04-17-recurring-bills-coverage-report.md
git commit -m "docs: first recurring-bills run — coverage report

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Self-review notes (documented for the engineer)

**Spec coverage check:**

- §1 Goal — Tasks 6+7+8 produce `is_original` and `original_bill_id`
- §3 Architecture — Tasks 1-13 create all listed files
- §4 Data flow — Tasks 3-11 implement each arrow
- §5 Output schema — Task 9 creates table + Parquet; types checked by Task 9 tests
- §6 CAP integration — Task 13 creates the view
- §7 Fetch politeness — Task 4+5 cover UA/delay/cache/retry
- §8 K16-K18 fallback — Task 7 builds it; Task 8 merges
- §9 Error handling — Task 5 retries 5xx; Task 6 handles missing details; Task 13 handles missing UserBillCAP
- §10 Testing — Tasks 1-13 each include tests (no placeholders)
- §11 CLI — Task 12 builds the argparse wrapper with all flags from the spec
- §12 Prerequisite setup — Task 0 copies the Excel
- §13 Runtime budget — Task 15 validates end-to-end timing

**Type consistency:**

- DataFrame columns consistently named: `BillID` (int), `KnessetNum` (int), `Name` (str), `is_original` (bool), `original_bill_id` (int NOT NULL), `predecessor_bill_ids` (list[int]), `classification_source` (str), `tal_fetched_at` (datetime|None).
- `build_tal_classifications`, `build_k16_k18_fallback`, `merge_all` all emit the same 13-column schema (verified in Tasks 6, 7, 8 by fixture assertions).

**Known plan-stage decisions** (carried forward from spec §14):

- Multiple predecessors possible → `predecessor_bill_ids INTEGER[]` in schema. If real data shows always-length-1, a scalar column can be added later.
- `BillID` collisions between `cap_bills` and `bill_classifications` verified by spec-analyst as clean; view uses `LEFT JOIN USING (BillID)` to tolerate drift.

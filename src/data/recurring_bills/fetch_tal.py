"""HTTP client for Dr. Tal Alovitz's classifier at ``pmb.teca-it.com``.

Two endpoints are used:

* ``GET /api/export/bills.csv`` — bulk summary download (with ETag caching)
* ``GET /api/bill/{id}``        — per-bill detail with patient-zero link-back
"""

from __future__ import annotations

import json
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

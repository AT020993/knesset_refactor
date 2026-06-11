"""Client for the live Knesset MK site backend (``WebSiteApi/knessetapi/MKs``).

Three calls:
  * OData ``KNS_MkSiteCode`` → ``{PersonID: SiteId}`` (the site uses ``SiteId``,
    not ``PersonID``; the ParliamentInfo feed caps pages at 100 rows so we
    page explicitly).
  * ``GET GetMkDetailsContent?mkId={SiteId}`` → CV dict (``DateOfBirth``,
    ``PlaceOfBirth``, ``Education``, ``MilitaryService``, ``Languages``, …).
  * ``GET GetMkPositions?mkId={SiteId}`` → list of per-Knesset blocks, each with
    a ``Committee`` list (``CommitteeName``, ``Name`` role, ``FromDate``/``ToDate``).

Like the votes client this is an undocumented site backend, so we send
browser-like headers and retry politely on the server's rate-limit codes.
"""

from __future__ import annotations

import logging
import time
from typing import Any, cast

import requests

_BASE_WEB = "https://knesset.gov.il/WebSiteApi/knessetapi/MKs/"
_BASE_ODATA = "https://knesset.gov.il/Odata/ParliamentInfo.svc/"
_ODATA_PAGE = 100  # ParliamentInfo caps page size at 100 regardless of $top.

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://main.knesset.gov.il/mk/apps/mklobby/main/current-knesset-mks/",
}

log = logging.getLogger("data.mk_details.mk_details_client")


class MkDetailsClient:
    # Same non-standard rate-limit codes the votes backend emits.
    _RETRYABLE = frozenset({429, 481, 503})

    def __init__(
        self, *, timeout: int = 60, max_retries: int = 6, throttle_s: float = 0.0
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.throttle_s = throttle_s
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)

    def _get(self, url: str) -> requests.Response:
        last: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                resp = self._session.get(url, timeout=self.timeout)
                if resp.status_code < 500 and resp.status_code not in self._RETRYABLE:
                    return resp
                last = RuntimeError(f"HTTP {resp.status_code}")
            except (requests.ConnectionError, requests.Timeout) as exc:
                last = exc
            time.sleep(min(1.5 * (2**attempt), 20))
        raise RuntimeError(f"GET {url} failed after {self.max_retries} tries: {last}")

    def site_code_map(self) -> dict[int, int]:
        """``{PersonID: SiteId}`` for every MK, paged 100 rows at a time."""
        mapping: dict[int, int] = {}
        skip = 0
        while True:
            url = (
                f"{_BASE_ODATA}KNS_MkSiteCode?$format=json"
                f"&$skip={skip}&$top={_ODATA_PAGE}"
            )
            rows = self._get(url).json().get("value", []) or []
            if not rows:
                break
            for r in rows:
                kns_id, site_id = r.get("KnsID"), r.get("SiteId")
                if kns_id is not None and site_id is not None:
                    mapping[int(kns_id)] = int(site_id)
            skip += _ODATA_PAGE
        log.info("site-code map: %d MKs", len(mapping))
        return mapping

    def cv(self, site_id: int) -> dict[str, Any] | None:
        """CV payload for one MK; ``None`` if the site has no record."""
        resp = self._get(
            f"{_BASE_WEB}GetMkDetailsContent?mkId={site_id}&languageKey=he"
        )
        resp.raise_for_status()
        data = resp.json()
        return cast("dict[str, Any]", data) if isinstance(data, dict) else None

    def positions(self, site_id: int) -> list[dict[str, Any]]:
        """Per-Knesset position blocks (factions, committees, tenure) for one MK."""
        resp = self._get(f"{_BASE_WEB}GetMkPositions?mkId={site_id}&languageKey=he")
        resp.raise_for_status()
        data = resp.json()
        return cast("list[dict[str, Any]]", data) if isinstance(data, list) else []

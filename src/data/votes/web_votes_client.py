"""Client for the live Knesset plenum-votes API (``WebSiteApi/knessetapi/Votes``).

Two endpoints:
  * ``POST GetVotesHeaders`` body ``{}`` → all vote headers (PageSize is ignored;
    returns the full set, ~12 MB). Fields per row: ``VoteId, VoteDate,
    VoteDateStr, VoteType, ItemTitle, KnessetId, SessionId``.
  * ``GET  GetVoteDetails/{voteId}`` → ``{VoteHeader, VoteCounters, VoteDetails,
    …}``. ``VoteDetails`` is the per-MK list (``MkName, FactionName,
    VoteResultId, Title``) — populated for electronic votes; empty for
    show-of-hands.

This is an undocumented site backend, so we send browser-like headers and
retry politely.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, cast

import requests

_BASE = "https://knesset.gov.il/WebSiteApi/knessetapi/Votes/"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Referer": "https://main.knesset.gov.il/Activity/plenum/Votes/Pages/default.aspx",
}

# VoteResultId → canonical position. We key off the numeric id but the Hebrew
# Title is the source of truth if a new id appears (logged by the ingester).
RESULT_ID_TO_POSITION: dict[int, str] = {
    7: "for",  # בעד
    8: "against",  # נגד
    9: "abstain",  # נמנע
    6: "present",  # נוכח (present, did not vote for/against)
}

log = logging.getLogger("data.votes.web_votes_client")


class WebVotesClient:
    def __init__(
        self, *, timeout: int = 60, max_retries: int = 6, throttle_s: float = 0.0
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.throttle_s = throttle_s
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)

    # The Knesset server throttles bursts with HTTP 481 (non-standard) and may
    # also return 429/503. Treat these as transient and back off, rather than
    # dropping the vote.
    _RETRYABLE = frozenset({429, 481, 503})

    def _request(self, method: str, path: str, **kw: Any) -> requests.Response:
        last: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                resp = self._session.request(
                    method, _BASE + path, timeout=self.timeout, **kw
                )
                if resp.status_code < 500 and resp.status_code not in self._RETRYABLE:
                    return resp
                last = RuntimeError(f"HTTP {resp.status_code}")
            except (requests.ConnectionError, requests.Timeout) as exc:
                last = exc
            # Exponential backoff with a floor — rate-limit responses need real
            # breathing room, not millisecond retries.
            time.sleep(min(1.5 * (2**attempt), 20))
        raise RuntimeError(
            f"{method} {path} failed after {self.max_retries} tries: {last}"
        )

    def get_headers(self) -> list[dict[str, Any]]:
        """All vote headers across every Knesset (caller filters by KnessetId)."""
        resp = self._request("POST", "GetVotesHeaders", data="{}")
        resp.raise_for_status()
        return resp.json().get("Table", []) or []

    def get_vote_details(self, vote_id: int) -> dict[str, Any]:
        """Header + counters + per-MK ``VoteDetails`` for one vote."""
        resp = self._request("GET", f"GetVoteDetails/{vote_id}")
        resp.raise_for_status()
        return cast("dict[str, Any]", resp.json())

    def fetch_details_concurrent(
        self,
        vote_ids: list[int],
        *,
        max_workers: int = 6,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> dict[int, dict[str, Any]]:
        """Fetch ``GetVoteDetails`` for many votes with bounded concurrency.

        Failures are logged and skipped (the vote simply isn't ingested this run;
        a later run retries it since it's still missing from the warehouse).
        """
        out: dict[int, dict[str, Any]] = {}
        total = len(vote_ids)
        done = 0
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self.get_vote_details, vid): vid for vid in vote_ids}
            for fut in as_completed(futures):
                vid = futures[fut]
                done += 1
                try:
                    out[vid] = fut.result()
                except Exception as exc:  # noqa: BLE001 — log and continue
                    log.warning("vote %s details failed: %s", vid, exc)
                if on_progress and done % 100 == 0:
                    on_progress(done, total)
                if self.throttle_s:
                    time.sleep(self.throttle_s)
        return out

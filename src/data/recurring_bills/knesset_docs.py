"""Download + parse Knesset bill documents directly from fs.knesset.gov.il.

Used to extend recurring-bills classification to K16-K18 (which Tal Alovitz's
API at pmb.teca-it.com does not cover). Replicates Tal's method:

1. Download the bill's first document URL from ``fs.knesset.gov.il``.
2. Extract text — ``textutil`` (macOS built-in) for .doc/.docx; ``pypdf`` for .pdf.
3. Detect recurrence phrases in the explanatory notes.
4. Resolve local ``פ/NNN`` references back to BillIDs via ``KNS_Bill.PrivateNumber``.
"""

from __future__ import annotations

from datetime import date
import logging
import re
import subprocess
import time
from pathlib import Path

import requests
from data.recurring_bills.normalize import normalize_name

log = logging.getLogger(__name__)

# Reuses the politeness defaults from fetch_tal
USER_AGENT = "knesset-refactor-research-bot/1.0 (contact: amirgo12@gmail.com)"
DEFAULT_TIMEOUT_S = 30
TEXTUTIL_TIMEOUT_S = 30

_RECURRENCE_PATTERNS = [
    {
        "pattern": re.compile(r"הצעות\s+חוק\s+דומות(?:\s+בעיקרן)?"),
        "recurrence_type": "similar",
    },
    {
        "pattern": re.compile(r"הצעת\s+חוק\s+דומה"),
        "recurrence_type": "similar",
    },
    {
        "pattern": re.compile(r"בהמשך\s+להצעת\s+חוק"),
        "recurrence_type": "similar",
    },
    {
        "pattern": re.compile(r"הצעת\s+חוק\s+זהה"),
        "recurrence_type": "identical",
    },
    {
        "pattern": re.compile(r"הצעות\s+חוק\s+זהות"),
        "recurrence_type": "identical",
    },
    {
        "pattern": re.compile(r"הצעה\s+זהה"),
        "recurrence_type": "identical",
    },
    {
        "pattern": re.compile(r"חוזרת\s+ומוגש"),
        "recurrence_type": "identical",
    },
    {
        "pattern": re.compile(r"הוגש[הה]?\s+בכנסת\s+ה[־\-]?\s*\S+"),
        "recurrence_type": "identical",
    },
    {
        "pattern": re.compile(
            r"הונחה\s+על\s+שולחן\s+הכנסת\s+ה(?:שש|שבע|שמונה|תשע|עשרים)[\-\s]*[\u0590-\u05FF]*"
        ),
        "recurrence_type": "identical",
    },
    {
        "pattern": re.compile(r"ומספרה\s+פ\s*/\s*\d+\s*/\s*\d+"),
        "recurrence_type": "identical",
    },
]

_PATTERN_PRIVATE_REF = re.compile(r"[פק]\s*/\s*(\d{2,5})(?:\s*/\s*(\d{1,2}))?")

_HEBREW_KNESSET_NUMS = {
    "הראשונה": 1,
    "השנייה": 2,
    "השניה": 2,
    "השלישית": 3,
    "הרביעית": 4,
    "החמישית": 5,
    "השישית": 6,
    "השביעית": 7,
    "השמינית": 8,
    "התשיעית": 9,
    "העשירית": 10,
    "האחת-עשרה": 11,
    "האחת עשרה": 11,
    "השתים-עשרה": 12,
    "השתים עשרה": 12,
    "השתיים-עשרה": 12,
    "השתיים עשרה": 12,
    "השלוש-עשרה": 13,
    "השלוש עשרה": 13,
    "הארבע-עשרה": 14,
    "הארבע עשרה": 14,
    "החמש-עשרה": 15,
    "החמש עשרה": 15,
    "השש-עשרה": 16,
    "השש עשרה": 16,
    "השבע-עשרה": 17,
    "השבע עשרה": 17,
    "השמונה-עשרה": 18,
    "השמונה עשרה": 18,
    "התשע-עשרה": 19,
    "התשע עשרה": 19,
    "העשרים": 20,
    "העשרים-ואחת": 21,
    "העשרים ואחת": 21,
    "העשרים-ואחד": 21,
    "העשרים ואחד": 21,
    "העשרים-ושתיים": 22,
    "העשרים ושתיים": 22,
    "העשרים-ושלוש": 23,
    "העשרים ושלוש": 23,
    "העשרים-וארבע": 24,
    "העשרים וארבע": 24,
    "העשרים-וחמש": 25,
    "העשרים וחמש": 25,
}
_PATTERN_SUBMISSION_DATE = re.compile(r"(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})")
_PATTERN_CONTEXT_BREAK = re.compile(r"(?:\n\s*\n|[.!?])")
_PATTERN_PREVIOUS_KNESSET = re.compile(r"(?:בכנסת|הכנסת)\s+הקודמת")
_PATTERN_NUMERIC_KNESSET = re.compile(r"(?:בכנסת|הכנסת)\s+ה[-\s]*(\d{1,2})(?:\b|$)")
_DASH_RE = re.compile(r"[\u2010\u2011\u2012\u2013\u2014\u2015\u05BE]")
_WHITESPACE_RE = re.compile(r"\s+")
_NORMALIZED_KNESSET_NUMS = {
    None: None,
}

_SCAN_CHAR_LIMIT = 4000
_SUBMISSION_DATE_TAIL_CHARS = 2000
_CONTEXT_WINDOW_CHARS = 1600

for _phrase, _number in _HEBREW_KNESSET_NUMS.items():
    normalized_phrase = _WHITESPACE_RE.sub(" ", _DASH_RE.sub("-", _phrase)).strip()
    _NORMALIZED_KNESSET_NUMS[normalized_phrase] = _number
_SORTED_NORMALIZED_KNESSET_PHRASES = sorted(
    (phrase for phrase in _NORMALIZED_KNESSET_NUMS if phrase),
    key=len,
    reverse=True,
)


def download_doc(
    url: str,
    cache_path: Path,
    *,
    force_refresh: bool = False,
    timeout_s: int = DEFAULT_TIMEOUT_S,
) -> Path | None:
    """Download a bill document to ``cache_path``. Returns path, or None on 4xx/404."""
    cache_path = Path(cache_path)
    if cache_path.exists() and not force_refresh:
        return cache_path

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    resp = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=timeout_s,
        allow_redirects=True,
    )
    if resp.status_code == 404:
        log.warning("Doc not found (404): %s", url)
        return None
    resp.raise_for_status()
    cache_path.write_bytes(resp.content)
    return cache_path


def extract_text(path: Path) -> str | None:
    """Extract plain text from a .doc / .docx / .pdf file."""
    path = Path(path)
    ext = path.suffix.lower()

    if ext in (".doc", ".docx"):
        try:
            result = subprocess.run(
                ["textutil", "-convert", "txt", "-stdout", str(path)],
                capture_output=True,
                timeout=TEXTUTIL_TIMEOUT_S,
            )
            if result.returncode != 0:
                log.warning("textutil failed on %s: %s", path, result.stderr[:200])
                return None
            return result.stdout.decode("utf-8", errors="replace")
        except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
            log.warning("textutil invocation error on %s: %s", path, exc)
            return None

    if ext == ".pdf":
        try:
            from pypdf import PdfReader

            reader = PdfReader(str(path))
            parts = []
            for page in reader.pages[:10]:
                parts.append(page.extract_text() or "")
            text = "\n".join(parts)
            if text and _hebrew_ratio(text) < 0.1:
                log.warning("PDF text has low Hebrew ratio (likely custom font): %s", path)
                return None
            return text
        except Exception as exc:  # noqa: BLE001
            log.warning("pypdf extraction failed on %s: %s", path, exc)
            return None

    log.warning("Unsupported extension %s on %s", ext, path)
    return None


def _hebrew_ratio(text: str) -> float:
    if not text:
        return 0.0
    non_ws = [char for char in text if not char.isspace()]
    if not non_ws:
        return 0.0
    hebrew = sum(1 for char in non_ws if "\u0590" <= char <= "\u05FF")
    return hebrew / len(non_ws)


def _normalize_submission_date(raw_date: str) -> str | None:
    match = _PATTERN_SUBMISSION_DATE.search(raw_date)
    if not match:
        return None

    day = int(match.group(1))
    month = int(match.group(2))
    year = int(match.group(3))
    if year < 100:
        year += 2000 if year <= 49 else 1900

    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def extract_submission_date(text: str) -> str | None:
    """Extract the normalized submission date from the bottom of the doc."""
    if not text:
        return None

    tail = text[-_SUBMISSION_DATE_TAIL_CHARS:]
    candidates: list[tuple[int, int, str]] = []
    for match in _PATTERN_SUBMISSION_DATE.finditer(tail):
        normalized = _normalize_submission_date(match.group(0))
        if not normalized:
            continue

        local = tail[max(0, match.start() - 120): min(len(tail), match.end() + 120)]
        score = 0
        if "הונחה על שולחן הכנסת" in local or "הוגשה ליו\"ר הכנסת" in local:
            score += 3
        if "הוגשה" in local or "הונחה" in local:
            score += 1
        if match.start() >= int(len(tail) * 0.6):
            score += 1
        candidates.append((score, match.start(), normalized))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[-1][2]


def _find_context_start(text: str, start: int, lower_bound: int) -> int:
    chunk = text[lower_bound:start]
    match = None
    for match in _PATTERN_CONTEXT_BREAK.finditer(chunk):
        pass
    if match is None:
        return lower_bound
    return lower_bound + match.end()


def _find_context_end(text: str, end: int, upper_bound: int) -> int:
    chunk = text[end:upper_bound]
    match = _PATTERN_CONTEXT_BREAK.search(chunk)
    if match is None:
        return upper_bound
    return end + match.start()


def _extract_local_context(text: str, start: int, end: int) -> tuple[int, str]:
    lower_bound = max(0, start - _CONTEXT_WINDOW_CHARS)
    upper_bound = min(len(text), end + _CONTEXT_WINDOW_CHARS)

    context_start = _find_context_start(text, start, lower_bound)
    context_end = _find_context_end(text, end, upper_bound)
    context = text[context_start:context_end].strip()
    if context:
        return context_start, context
    return lower_bound, text[lower_bound:upper_bound].strip()


def _normalize_hebrew_phrase_text(text: str) -> str:
    return _WHITESPACE_RE.sub(" ", _DASH_RE.sub("-", text or "")).strip()


def _extract_contextual_knesset(text: str, *, current_knesset: int | None = None) -> int | None:
    normalized = _normalize_hebrew_phrase_text(text)
    if current_knesset and _PATTERN_PREVIOUS_KNESSET.search(normalized):
        return current_knesset - 1 if current_knesset > 1 else None

    numeric_match = _PATTERN_NUMERIC_KNESSET.search(normalized)
    if numeric_match:
        knesset_num = int(numeric_match.group(1))
        return knesset_num if 1 <= knesset_num <= 25 else None

    for phrase in _SORTED_NORMALIZED_KNESSET_PHRASES:
        pattern = rf"(?:בכנסת|הכנסת)\s+{re.escape(phrase)}(?:\b|$)"
        if re.search(pattern, normalized):
            return _NORMALIZED_KNESSET_NUMS[phrase]
    return None


def _find_recurrence_occurrences(text: str, *, current_knesset: int | None = None) -> list[dict]:
    head = text[:_SCAN_CHAR_LIMIT]
    occurrences: list[dict] = []
    seen_spans: set[tuple[int, int, str]] = set()

    for order, spec in enumerate(_RECURRENCE_PATTERNS):
        for match in spec["pattern"].finditer(head):
            key = (match.start(), match.end(), spec["recurrence_type"])
            if key in seen_spans:
                continue
            seen_spans.add(key)
            context_start, context = _extract_local_context(head, match.start(), match.end())
            occurrences.append(
                {
                    "phrase_text": match.group(0),
                    "phrase_start": match.start(),
                    "phrase_order": order,
                    "recurrence_type": spec["recurrence_type"],
                    "context": context,
                    "phrase_in_context_start": max(0, match.start() - context_start),
                    "contextual_knesset": _extract_contextual_knesset(
                        context,
                        current_knesset=current_knesset,
                    ),
                }
            )

    occurrences.sort(key=lambda item: (item["phrase_start"], item["phrase_order"]))
    return occurrences


def _extract_reference_mentions(occurrences: list[dict]) -> list[dict]:
    mentions: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for phrase_index, occurrence in enumerate(occurrences):
        ref_matches = list(_PATTERN_PRIVATE_REF.finditer(occurrence["context"]))
        post_phrase_matches = [
            match for match in ref_matches
            if match.start() >= occurrence.get("phrase_in_context_start", 0)
        ]
        chosen_matches = post_phrase_matches or ref_matches
        for reference_index, match in enumerate(chosen_matches):
            dedupe_key = (occurrence["context"], match.group(0))
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            explicit_knesset = int(match.group(2)) if match.group(2) else None
            phrase_start = occurrence.get("phrase_in_context_start", 0)
            phrase_end = phrase_start + len(occurrence["phrase_text"])
            appears_after_phrase = match.start() >= phrase_end
            if appears_after_phrase:
                char_distance = match.start() - phrase_end
            else:
                char_distance = max(0, phrase_start - match.end())
            mentions.append(
                {
                    "phrase_index": phrase_index,
                    "phrase_text": occurrence["phrase_text"],
                    "recurrence_type": occurrence["recurrence_type"],
                    "context": occurrence["context"],
                    "reference_index": reference_index,
                    "reference_text": match.group(0),
                    "private_number": int(match.group(1)),
                    "explicit_knesset": explicit_knesset,
                    "contextual_knesset": occurrence["contextual_knesset"],
                    "referenced_knesset": explicit_knesset or occurrence["contextual_knesset"],
                    "appears_after_phrase": appears_after_phrase,
                    "char_distance_from_phrase": int(char_distance),
                }
            )
    return mentions


def parse_recurrence_signals(text: str, *, current_knesset: int | None = None) -> dict:
    """Extract recurrence markers from explanatory notes."""
    if not text:
        return {
            "is_recurring": False,
            "matched_phrase": None,
            "referenced_private_numbers": [],
            "referenced_knesset": None,
            "recurrence_phrases": [],
            "reference_candidates": [],
            "reference_candidate_count": 0,
            "multiple_references_detected": False,
            "submission_date": None,
        }

    occurrences = _find_recurrence_occurrences(text, current_knesset=current_knesset)
    mentions = _extract_reference_mentions(occurrences)
    referenced_knesset = next(
        (
            mention["referenced_knesset"]
            for mention in mentions
            if mention["referenced_knesset"] is not None
        ),
        None,
    )

    return {
        "is_recurring": bool(occurrences),
        "matched_phrase": occurrences[0]["phrase_text"] if occurrences else None,
        "referenced_private_numbers": [mention["private_number"] for mention in mentions],
        "referenced_knesset": referenced_knesset,
        "recurrence_phrases": occurrences,
        "reference_candidates": mentions,
        "reference_candidate_count": len(mentions),
        "multiple_references_detected": len(mentions) > 1,
        "submission_date": extract_submission_date(text),
    }


def _query_exact_bill_id(
    *,
    private_number: int,
    knesset_num: int,
    warehouse_con,
    exclude_bill_id: int | None = None,
) -> int | None:
    rows = warehouse_con.execute(
        """
        SELECT BillID FROM KNS_Bill
        WHERE PrivateNumber = ? AND KnessetNum = ?
        ORDER BY BillID ASC
        """,
        [private_number, knesset_num],
    ).fetchall()
    for row in rows:
        bill_id = int(row[0])
        if exclude_bill_id is None or bill_id != exclude_bill_id:
            return bill_id
    return None


def _query_prior_bill_id(
    *,
    private_number: int,
    current_knesset: int,
    warehouse_con,
    exclude_bill_id: int | None = None,
) -> int | None:
    rows = warehouse_con.execute(
        """
        SELECT BillID FROM KNS_Bill
        WHERE PrivateNumber = ? AND KnessetNum < ?
        ORDER BY KnessetNum DESC, BillID ASC
        """,
        [private_number, current_knesset],
    ).fetchall()
    for row in rows:
        bill_id = int(row[0])
        if exclude_bill_id is None or bill_id != exclude_bill_id:
            return bill_id
    return None


def resolve_link_back(
    *,
    private_number: int,
    referenced_knesset: int | None,
    current_knesset: int,
    warehouse_con,
    current_bill_id: int | None = None,
) -> int | None:
    """Map a ``פ/NNN`` reference to a BillID via KNS_Bill.PrivateNumber."""
    if referenced_knesset is not None:
        return _query_exact_bill_id(
            private_number=private_number,
            knesset_num=referenced_knesset,
            warehouse_con=warehouse_con,
            exclude_bill_id=current_bill_id,
        )

    same_knesset = _query_exact_bill_id(
        private_number=private_number,
        knesset_num=current_knesset,
        warehouse_con=warehouse_con,
        exclude_bill_id=current_bill_id,
    )
    if same_knesset is not None:
        return same_knesset

    return _query_prior_bill_id(
        private_number=private_number,
        current_knesset=current_knesset,
        warehouse_con=warehouse_con,
        exclude_bill_id=current_bill_id,
    )


def _resolve_reference_candidate(
    *,
    mention: dict,
    current_bill_id: int,
    current_knesset: int,
    warehouse_con,
) -> dict:
    candidate = dict(mention)
    candidate["resolved_bill_id"] = None
    candidate["reference_resolution_reason"] = None
    candidate["reference_resolution_confidence"] = None
    candidate["priority"] = 0
    candidate["selected"] = False
    candidate["selection_rank"] = None
    candidate["suspicious_self_resolution"] = False
    candidate["tied_for_best"] = False

    if mention["explicit_knesset"] is not None:
        resolved_bill_id = _query_exact_bill_id(
            private_number=mention["private_number"],
            knesset_num=mention["explicit_knesset"],
            warehouse_con=warehouse_con,
        )
        if resolved_bill_id == current_bill_id:
            log.warning(
                "Suspicious self-resolution for bill %d via explicit ref %s",
                current_bill_id,
                mention["reference_text"],
            )
            candidate["resolved_bill_id"] = current_bill_id
            candidate["reference_resolution_reason"] = "suspicious_self_reference"
            candidate["reference_resolution_confidence"] = 0.0
            candidate["priority"] = -1
            candidate["suspicious_self_resolution"] = True
            return candidate
        if resolved_bill_id is not None:
            candidate["resolved_bill_id"] = resolved_bill_id
            candidate["reference_resolution_reason"] = "explicit_private_number_and_knesset"
            candidate["reference_resolution_confidence"] = 0.99
            candidate["priority"] = 4
            return candidate

        candidate["reference_resolution_reason"] = "explicit_reference_unresolved"
        candidate["reference_resolution_confidence"] = 0.25
        return candidate

    if mention["referenced_knesset"] is not None:
        resolved_bill_id = _query_exact_bill_id(
            private_number=mention["private_number"],
            knesset_num=mention["referenced_knesset"],
            warehouse_con=warehouse_con,
            exclude_bill_id=current_bill_id,
        )
        if resolved_bill_id is not None:
            candidate["resolved_bill_id"] = resolved_bill_id
            candidate["reference_resolution_reason"] = "contextual_knesset_phrase_match"
            candidate["reference_resolution_confidence"] = 0.92
            candidate["priority"] = 3
            return candidate

    same_knesset = _query_exact_bill_id(
        private_number=mention["private_number"],
        knesset_num=current_knesset,
        warehouse_con=warehouse_con,
        exclude_bill_id=current_bill_id,
    )
    if same_knesset is not None:
        candidate["resolved_bill_id"] = same_knesset
        candidate["reference_resolution_reason"] = "same_knesset_private_number_fallback"
        candidate["reference_resolution_confidence"] = 0.78
        candidate["priority"] = 2
        return candidate

    prior_knesset = _query_prior_bill_id(
        private_number=mention["private_number"],
        current_knesset=current_knesset,
        warehouse_con=warehouse_con,
        exclude_bill_id=current_bill_id,
    )
    if prior_knesset is not None:
        candidate["resolved_bill_id"] = prior_knesset
        candidate["reference_resolution_reason"] = "prior_knesset_private_number_fallback"
        candidate["reference_resolution_confidence"] = 0.6
        candidate["priority"] = 1
        return candidate

    candidate["reference_resolution_reason"] = "no_matching_bill_for_reference"
    candidate["reference_resolution_confidence"] = 0.2
    return candidate


def _pick_primary_candidate(candidates: list[dict]) -> dict | None:
    selectable = [
        candidate
        for candidate in candidates
        if candidate.get("resolved_bill_id") is not None
        and not candidate.get("suspicious_self_resolution", False)
    ]
    if not selectable:
        return None

    def _selection_rank(candidate: dict) -> tuple[int, int, int, int]:
        return (
            int(candidate.get("priority", 0)),
            1 if candidate.get("recurrence_type") == "identical" else 0,
            1 if candidate.get("appears_after_phrase", False) else 0,
            -int(candidate.get("char_distance_from_phrase", 1_000_000)),
        )

    best_by_bill: dict[int, tuple[tuple[int, int, int, int], dict]] = {}
    for candidate in selectable:
        rank = _selection_rank(candidate)
        candidate["selection_rank"] = list(rank)
        resolved_bill_id = int(candidate["resolved_bill_id"])
        current = best_by_bill.get(resolved_bill_id)
        if current is None or rank > current[0]:
            best_by_bill[resolved_bill_id] = (rank, candidate)

    top_rank = max(rank for rank, _ in best_by_bill.values())
    top_candidates = [
        candidate
        for rank, candidate in best_by_bill.values()
        if rank == top_rank
    ]
    if len(top_candidates) > 1:
        for candidate in top_candidates:
            candidate["tied_for_best"] = True
        return None

    primary = top_candidates[0]
    primary["selected"] = True
    return primary


def _has_ambiguous_primary_candidates(candidates: list[dict]) -> bool:
    selectable = [
        candidate
        for candidate in candidates
        if candidate.get("resolved_bill_id") is not None
        and not candidate.get("suspicious_self_resolution", False)
    ]
    if not selectable:
        return False

    best_rank_by_bill: dict[int, tuple[int, int, int, int]] = {}
    for candidate in selectable:
        selection_rank = candidate.get("selection_rank")
        if selection_rank is None:
            selection_rank = [
                int(candidate.get("priority", 0)),
                1 if candidate.get("recurrence_type") == "identical" else 0,
                1 if candidate.get("appears_after_phrase", False) else 0,
                -int(candidate.get("char_distance_from_phrase", 1_000_000)),
            ]
        resolved_bill_id = int(candidate["resolved_bill_id"])
        rank_tuple = tuple(int(value) for value in selection_rank)
        current = best_rank_by_bill.get(resolved_bill_id)
        if current is None or rank_tuple > current:
            best_rank_by_bill[resolved_bill_id] = rank_tuple

    top_rank = max(best_rank_by_bill.values())
    top_bill_ids = [
        bill_id
        for bill_id, rank in best_rank_by_bill.items()
        if rank == top_rank
    ]
    return len(top_bill_ids) > 1


def _build_name_fallback_candidate(
    *,
    occurrence: dict,
    resolved_bill_id: int | None,
    resolution_reason: str,
    confidence: float,
    priority: int,
) -> dict:
    return {
        "phrase_index": 0,
        "phrase_text": occurrence["phrase_text"],
        "recurrence_type": occurrence["recurrence_type"],
        "context": occurrence["context"],
        "reference_index": -1,
        "reference_text": None,
        "private_number": None,
        "explicit_knesset": None,
        "contextual_knesset": occurrence.get("contextual_knesset"),
        "referenced_knesset": occurrence.get("contextual_knesset"),
        "resolved_bill_id": resolved_bill_id,
        "reference_resolution_reason": resolution_reason,
        "reference_resolution_confidence": confidence,
        "priority": priority,
        "selected": False,
        "selection_rank": None,
        "suspicious_self_resolution": False,
        "tied_for_best": False,
    }


def _query_name_match_candidates(
    *,
    current_bill_id: int,
    current_knesset: int,
    warehouse_con,
) -> list[tuple[int, int]]:
    current_row = warehouse_con.execute(
        """
        SELECT Name FROM KNS_Bill
        WHERE BillID = ?
        LIMIT 1
        """,
        [current_bill_id],
    ).fetchone()
    if not current_row or not current_row[0]:
        return []

    current_norm = normalize_name(current_row[0])
    if not current_norm:
        return []

    rows = warehouse_con.execute(
        """
        SELECT BillID, KnessetNum, Name
        FROM KNS_Bill
        WHERE BillID <> ? AND KnessetNum <= ? AND Name IS NOT NULL
        ORDER BY KnessetNum DESC, BillID ASC
        """,
        [current_bill_id, current_knesset],
    ).fetchall()
    return [
        (int(bill_id), int(knesset_num))
        for bill_id, knesset_num, name in rows
        if normalize_name(name) == current_norm
    ]


def _resolve_by_name_fallback(
    *,
    signals: dict,
    current_bill_id: int,
    current_knesset: int,
    warehouse_con,
) -> dict | None:
    name_matches = _query_name_match_candidates(
        current_bill_id=current_bill_id,
        current_knesset=current_knesset,
        warehouse_con=warehouse_con,
    )
    if not name_matches or not signals["recurrence_phrases"]:
        return None

    occurrence = signals["recurrence_phrases"][0]
    contextual_knessets = [
        phrase["contextual_knesset"]
        for phrase in signals["recurrence_phrases"]
        if phrase.get("contextual_knesset") is not None
    ]

    for contextual_knesset in contextual_knessets:
        scoped = [match for match in name_matches if match[1] == contextual_knesset]
        if len(scoped) == 1:
            return _build_name_fallback_candidate(
                occurrence=occurrence,
                resolved_bill_id=scoped[0][0],
                resolution_reason="contextual_knesset_name_match",
                confidence=0.72,
                priority=3,
            )

    same_knesset = [match for match in name_matches if match[1] == current_knesset]
    if len(same_knesset) == 1:
        return _build_name_fallback_candidate(
            occurrence=occurrence,
            resolved_bill_id=same_knesset[0][0],
            resolution_reason="same_knesset_name_fallback",
            confidence=0.58,
            priority=2,
        )

    prior_knessets = [match for match in name_matches if match[1] < current_knesset]
    if len(prior_knessets) == 1:
        return _build_name_fallback_candidate(
            occurrence=occurrence,
            resolved_bill_id=prior_knessets[0][0],
            resolution_reason="prior_knesset_name_fallback",
            confidence=0.52,
            priority=1,
        )

    return None


def classify_bill_from_doc(
    *,
    bill_id: int,
    current_knesset: int,
    doc_url: str,
    cache_dir: Path,
    warehouse_con,
    delay_s: float = 0.3,
) -> dict:
    """End-to-end classification for a single bill using its explanatory notes."""
    cache_dir = Path(cache_dir)
    ext = doc_url.rsplit(".", 1)[-1].lower()
    if ext not in ("doc", "docx", "pdf"):
        return {
            "is_recurring": False,
            "original_bill_id": None,
            "matched_phrase": None,
            "method": "doc_fetch_failed",
            "reference_candidates": [],
            "reference_candidate_count": 0,
            "reference_resolution_reason": None,
            "reference_resolution_confidence": None,
            "multiple_references_detected": False,
            "submission_date": None,
            "suspicious_self_resolution": False,
            "ambiguous_reference_resolution": False,
            "ambiguous_reference_reason": None,
        }

    cache_path = cache_dir / f"{bill_id}.{ext}"
    if not cache_path.exists():
        time.sleep(delay_s)
        try:
            path = download_doc(doc_url, cache_path)
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            log.warning("Download failed for bill %d: %s", bill_id, exc)
            return {
                "is_recurring": False,
                "original_bill_id": None,
                "matched_phrase": None,
                "method": "doc_fetch_failed",
                "reference_candidates": [],
                "reference_candidate_count": 0,
                "reference_resolution_reason": None,
                "reference_resolution_confidence": None,
                "multiple_references_detected": False,
                "submission_date": None,
                "suspicious_self_resolution": False,
                "ambiguous_reference_resolution": False,
                "ambiguous_reference_reason": None,
            }
        if path is None:
            return {
                "is_recurring": False,
                "original_bill_id": None,
                "matched_phrase": None,
                "method": "doc_fetch_failed",
                "reference_candidates": [],
                "reference_candidate_count": 0,
                "reference_resolution_reason": None,
                "reference_resolution_confidence": None,
                "multiple_references_detected": False,
                "submission_date": None,
                "suspicious_self_resolution": False,
                "ambiguous_reference_resolution": False,
                "ambiguous_reference_reason": None,
            }
    else:
        path = cache_path

    text = extract_text(path)
    if text is None:
        return {
            "is_recurring": False,
            "original_bill_id": None,
            "matched_phrase": None,
            "method": "doc_fetch_failed",
            "reference_candidates": [],
            "reference_candidate_count": 0,
            "reference_resolution_reason": None,
            "reference_resolution_confidence": None,
            "multiple_references_detected": False,
            "submission_date": None,
            "suspicious_self_resolution": False,
            "ambiguous_reference_resolution": False,
            "ambiguous_reference_reason": None,
        }

    signals = parse_recurrence_signals(text, current_knesset=current_knesset)

    if not signals["is_recurring"]:
        return {
            "is_recurring": False,
            "original_bill_id": None,
            "matched_phrase": None,
            "method": "doc_no_pattern",
            "reference_candidates": [],
            "reference_candidate_count": 0,
            "reference_resolution_reason": None,
            "reference_resolution_confidence": None,
            "multiple_references_detected": False,
            "submission_date": signals["submission_date"],
            "suspicious_self_resolution": False,
            "ambiguous_reference_resolution": False,
            "ambiguous_reference_reason": None,
        }

    resolved_candidates = [
        _resolve_reference_candidate(
            mention=mention,
            current_bill_id=bill_id,
            current_knesset=current_knesset,
            warehouse_con=warehouse_con,
        )
        for mention in signals["reference_candidates"]
    ]
    name_fallback_candidate = _resolve_by_name_fallback(
        signals=signals,
        current_bill_id=bill_id,
        current_knesset=current_knesset,
        warehouse_con=warehouse_con,
    )
    if name_fallback_candidate is not None:
        resolved_candidates.append(name_fallback_candidate)
    primary = _pick_primary_candidate(resolved_candidates)
    ambiguous_reference_resolution = primary is None and _has_ambiguous_primary_candidates(
        resolved_candidates
    )
    suspicious_self_resolution = any(
        candidate.get("suspicious_self_resolution", False)
        for candidate in resolved_candidates
    )
    matched_phrase = primary["phrase_text"] if primary is not None else signals["matched_phrase"]

    if primary is not None:
        return {
            "is_recurring": True,
            "original_bill_id": primary["resolved_bill_id"],
            "matched_phrase": matched_phrase,
            "method": "doc_pattern_linked",
            "reference_candidates": resolved_candidates,
            "reference_candidate_count": len(resolved_candidates),
            "reference_resolution_reason": primary["reference_resolution_reason"],
            "reference_resolution_confidence": primary["reference_resolution_confidence"],
            "multiple_references_detected": signals["multiple_references_detected"],
            "submission_date": signals["submission_date"],
            "suspicious_self_resolution": suspicious_self_resolution,
            "ambiguous_reference_resolution": False,
            "ambiguous_reference_reason": None,
        }

    if ambiguous_reference_resolution:
        resolution_reason = "ambiguous_primary_reference_candidates"
        ambiguity_reason = "multiple_equally_strong_candidates"
    elif suspicious_self_resolution:
        resolution_reason = "suspicious_self_reference_only"
        ambiguity_reason = None
    elif resolved_candidates:
        resolution_reason = "no_resolved_reference_candidates"
        ambiguity_reason = None
    else:
        resolution_reason = "no_reference_candidates_in_recurrence_context"
        ambiguity_reason = None

    return {
        "is_recurring": True,
        "original_bill_id": None,
        "matched_phrase": matched_phrase,
        "method": "doc_pattern_unresolved",
        "reference_candidates": resolved_candidates,
        "reference_candidate_count": len(resolved_candidates),
        "reference_resolution_reason": resolution_reason,
        "reference_resolution_confidence": 0.0 if suspicious_self_resolution else None,
        "multiple_references_detected": signals["multiple_references_detected"],
        "submission_date": signals["submission_date"],
        "suspicious_self_resolution": suspicious_self_resolution,
        "ambiguous_reference_resolution": ambiguous_reference_resolution,
        "ambiguous_reference_reason": ambiguity_reason,
    }

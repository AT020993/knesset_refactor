"""Download + parse Knesset bill documents directly from fs.knesset.gov.il.

Used to extend recurring-bills classification to K16-K18 (which Tal Alovitz's
API at pmb.teca-it.com does not cover). Replicates Tal's method:

1. Download the bill's first document URL from ``fs.knesset.gov.il``.
2. Extract text — ``textutil`` (macOS built-in) for .doc/.docx; ``pypdf`` for .pdf.
3. Scan the text for Hebrew recurring-bill markers ("הצעת חוק דומה",
   "הוגש בכנסת ה־NN", references like "פ/285").
4. Resolve ``פ/NNN`` references back to BillIDs via ``KNS_Bill.PrivateNumber``.

The host ``fs.knesset.gov.il`` has no bot protection, unlike the main SPA
at ``main.knesset.gov.il`` (which uses Kasada).
"""

from __future__ import annotations

import logging
import re
import subprocess
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)

# Reuses the politeness defaults from fetch_tal
USER_AGENT = "knesset-refactor-research-bot/1.0 (contact: amirgo12@gmail.com)"
DEFAULT_TIMEOUT_S = 30
TEXTUTIL_TIMEOUT_S = 30

# Recurrence signal patterns. Ordered by specificity — more-specific first.
# Covers Tal's two primary signal families:
#   1. Explicit "similar bill" phrases
#   2. References to earlier bills by private number (פ/NNN or ק/NNN)
_PATTERNS_SIMILAR = [
    re.compile(r"הצעת\s+חוק\s+דומה"),            # "a similar bill"
    re.compile(r"הצעה\s+זהה"),                   # "identical proposal"
    re.compile(r"חוזרת\s+ומוגש"),                # "returns and is submitted"
    re.compile(r"הוגש[הה]?\s+בכנסת\s+ה[־\-]?\s*\S+"),  # "was submitted in Knesset X"
    re.compile(r"בהמשך\s+להצעת\s+חוק"),          # "following the bill"
]

# Explicit private-bill references: פ/NNN or ק/NNN, optionally with /K knesset suffix
_PATTERN_PRIVATE_REF = re.compile(r"[פק]\s*/\s*(\d{2,5})(?:\s*/\s*(\d{1,2}))?")

# Knesset number in Hebrew word form — maps Hebrew Knesset names to integers
_HEBREW_KNESSET_NUMS = {
    "השש-עשרה": 16, "השש עשרה": 16,
    "השבע-עשרה": 17, "השבע עשרה": 17,
    "השמונה-עשרה": 18, "השמונה עשרה": 18,
    "התשע-עשרה": 19, "התשע עשרה": 19,
    "העשרים": 20,
    "העשרים-ואחת": 21, "העשרים ואחת": 21, "העשרים-ואחד": 21, "העשרים ואחד": 21,
    "העשרים-ושתיים": 22, "העשרים ושתיים": 22,
    "העשרים-ושלוש": 23, "העשרים ושלוש": 23,
    "העשרים-וארבע": 24, "העשרים וארבע": 24,
    "העשרים-וחמש": 25, "העשרים וחמש": 25,
}
_PATTERN_KNESSET_REF = re.compile(
    r"בכנסת\s+(" + "|".join(_HEBREW_KNESSET_NUMS.keys()) + r")"
)

# Max text slice we scan — explanatory notes are always in the first ~2KB
_SCAN_CHAR_LIMIT = 4000


def download_doc(
    url: str,
    cache_path: Path,
    *,
    force_refresh: bool = False,
    timeout_s: int = DEFAULT_TIMEOUT_S,
) -> Path | None:
    """Download a bill document to ``cache_path``. Returns path, or None on 4xx/404.

    Cache hit (file exists, force_refresh=False) skips HTTP entirely.
    5xx / network errors raise — caller decides whether to retry.
    """
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
    """Extract plain text from a .doc / .docx / .pdf file.

    - ``.doc`` / ``.docx``: uses macOS ``textutil`` (built-in, handles Hebrew)
    - ``.pdf``: uses ``pypdf`` (Hebrew support depends on the PDF's font encoding;
      older Knesset PDFs sometimes embed custom encodings that cannot be
      recovered — returns None in that case, caller falls back to name match)

    Returns None on any extraction failure.
    """
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
            for page in reader.pages[:10]:  # explanatory notes are always first few pages
                parts.append(page.extract_text() or "")
            text = "\n".join(parts)
            # Heuristic: if less than 10% of chars are in Hebrew Unicode block,
            # the PDF's font encoding is unrecoverable — bail out to name match.
            if text and _hebrew_ratio(text) < 0.1:
                log.warning("PDF text has low Hebrew ratio (likely custom font): %s", path)
                return None
            return text
        except Exception as exc:  # noqa: BLE001 — pypdf raises many exception types
            log.warning("pypdf extraction failed on %s: %s", path, exc)
            return None

    log.warning("Unsupported extension %s on %s", ext, path)
    return None


def _hebrew_ratio(text: str) -> float:
    """Fraction of non-whitespace chars that are in the Hebrew Unicode block."""
    if not text:
        return 0.0
    non_ws = [c for c in text if not c.isspace()]
    if not non_ws:
        return 0.0
    hebrew = sum(1 for c in non_ws if "\u0590" <= c <= "\u05FF")
    return hebrew / len(non_ws)


def parse_recurrence_signals(text: str) -> dict:
    """Extract recurrence markers from explanatory notes.

    Returns a dict shaped::

        {
          "is_recurring": bool,               # any similar-bill phrase matched
          "matched_phrase": str | None,       # the phrase that matched (for audit)
          "referenced_private_numbers": [int],# e.g. [285] from "פ/285"
          "referenced_knesset": int | None,   # e.g. 16 from "בכנסת השש-עשרה"
        }

    Scans only the first ``_SCAN_CHAR_LIMIT`` characters — explanatory notes
    always appear near the top of the document, so scanning further wastes time
    on operative clauses that happen to contain amendment-reference numbers.
    """
    if not text:
        return {
            "is_recurring": False,
            "matched_phrase": None,
            "referenced_private_numbers": [],
            "referenced_knesset": None,
        }

    head = text[:_SCAN_CHAR_LIMIT]

    matched_phrase = None
    for pat in _PATTERNS_SIMILAR:
        m = pat.search(head)
        if m:
            matched_phrase = m.group(0)
            break

    private_refs = []
    for m in _PATTERN_PRIVATE_REF.finditer(head):
        try:
            private_refs.append(int(m.group(1)))
        except (TypeError, ValueError):
            continue

    knesset_ref = None
    m = _PATTERN_KNESSET_REF.search(head)
    if m:
        knesset_ref = _HEBREW_KNESSET_NUMS.get(m.group(1))

    return {
        "is_recurring": matched_phrase is not None,
        "matched_phrase": matched_phrase,
        "referenced_private_numbers": private_refs,
        "referenced_knesset": knesset_ref,
    }


def resolve_link_back(
    *,
    private_number: int,
    referenced_knesset: int | None,
    current_knesset: int,
    warehouse_con,
) -> int | None:
    """Map a ``פ/NNN`` reference to a BillID via KNS_Bill.PrivateNumber.

    Strategy:
    - If ``referenced_knesset`` is given, look up that specific Knesset.
    - Otherwise, search Knessets strictly earlier than ``current_knesset``
      (reprises reference older bills, never newer) and pick the most recent.
    - Returns None if no match (then caller leaves as self-reference).
    """
    if referenced_knesset is not None:
        row = warehouse_con.execute(
            """
            SELECT BillID FROM KNS_Bill
            WHERE PrivateNumber = ? AND KnessetNum = ?
            ORDER BY BillID ASC
            LIMIT 1
            """,
            [private_number, referenced_knesset],
        ).fetchone()
        return int(row[0]) if row else None

    row = warehouse_con.execute(
        """
        SELECT BillID FROM KNS_Bill
        WHERE PrivateNumber = ? AND KnessetNum < ?
        ORDER BY KnessetNum DESC, BillID ASC
        LIMIT 1
        """,
        [private_number, current_knesset],
    ).fetchone()
    return int(row[0]) if row else None


def classify_bill_from_doc(
    *,
    bill_id: int,
    current_knesset: int,
    doc_url: str,
    cache_dir: Path,
    warehouse_con,
    delay_s: float = 0.3,
) -> dict:
    """End-to-end classification for a single bill using its explanatory notes.

    Returns a dict with the same schema build_tal_classifications uses:
    ``{"is_recurring": bool, "original_bill_id": int | None,
        "matched_phrase": str | None, "method": str}``

    ``method`` is one of:
    - ``doc_pattern_linked``     — matched a similar-bill phrase AND resolved the ref
    - ``doc_pattern_unresolved`` — matched phrase but couldn't resolve the ref
    - ``doc_no_pattern``         — doc fetched/parsed but no recurrence signal
    - ``doc_fetch_failed``       — download/parse error (caller should fall back)
    """
    cache_dir = Path(cache_dir)
    ext = doc_url.rsplit(".", 1)[-1].lower()
    if ext not in ("doc", "docx", "pdf"):
        return {"is_recurring": False, "original_bill_id": None, "matched_phrase": None,
                "method": "doc_fetch_failed"}

    cache_path = cache_dir / f"{bill_id}.{ext}"
    if not cache_path.exists():
        time.sleep(delay_s)
        try:
            path = download_doc(doc_url, cache_path)
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            log.warning("Download failed for bill %d: %s", bill_id, exc)
            return {"is_recurring": False, "original_bill_id": None, "matched_phrase": None,
                    "method": "doc_fetch_failed"}
        if path is None:
            return {"is_recurring": False, "original_bill_id": None, "matched_phrase": None,
                    "method": "doc_fetch_failed"}
    else:
        path = cache_path

    text = extract_text(path)
    if text is None:
        return {"is_recurring": False, "original_bill_id": None, "matched_phrase": None,
                "method": "doc_fetch_failed"}

    signals = parse_recurrence_signals(text)

    if not signals["is_recurring"]:
        return {"is_recurring": False, "original_bill_id": None, "matched_phrase": None,
                "method": "doc_no_pattern"}

    # Try to resolve link-back. Use the first referenced private_number that
    # resolves to a real bill (some references may be to laws, not bills).
    for pn in signals["referenced_private_numbers"]:
        linked = resolve_link_back(
            private_number=pn,
            referenced_knesset=signals["referenced_knesset"],
            current_knesset=current_knesset,
            warehouse_con=warehouse_con,
        )
        if linked is not None and linked != bill_id:
            return {"is_recurring": True, "original_bill_id": linked,
                    "matched_phrase": signals["matched_phrase"],
                    "method": "doc_pattern_linked"}

    return {"is_recurring": True, "original_bill_id": None,
            "matched_phrase": signals["matched_phrase"],
            "method": "doc_pattern_unresolved"}

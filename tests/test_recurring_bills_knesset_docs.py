"""Unit tests for src/data/recurring_bills/knesset_docs.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from data.recurring_bills.knesset_docs import (
    classify_bill_from_doc,
    download_doc,
    parse_recurrence_signals,
    resolve_link_back,
)


class TestParseRecurrenceSignals:
    def test_detects_simili_phrase(self):
        text = "מבוא כלשהו.\nהצעת חוק דומה הוגשה בכנסת ה־17 על ידי חבר הכנסת ש."
        s = parse_recurrence_signals(text)
        assert s["is_recurring"] is True
        assert s["matched_phrase"] == "הצעת חוק דומה"

    def test_detects_identical_phrase(self):
        text = "הצעה זהה הונחה על שולחן הכנסת ה־18."
        s = parse_recurrence_signals(text)
        assert s["is_recurring"] is True
        assert "הצעה זהה" in s["matched_phrase"]

    def test_no_recurrence_markers(self):
        text = "מבוא לחוק. תיקון חדש לחוק הביטוח הלאומי שמציע שינוי."
        s = parse_recurrence_signals(text)
        assert s["is_recurring"] is False
        assert s["matched_phrase"] is None

    def test_extracts_private_number_ref(self):
        text = "הצעת חוק דומה הונחה בכנסת ה־16 (פ/285)."
        s = parse_recurrence_signals(text)
        assert 285 in s["referenced_private_numbers"]

    def test_extracts_knesset_number_from_hebrew_word(self):
        text = "הצעת חוק דומה הוגשה בכנסת השש-עשרה."
        s = parse_recurrence_signals(text)
        assert s["referenced_knesset"] == 16

    def test_scan_limited_to_head(self):
        """Pattern in the first 4000 chars is found; past that is ignored."""
        head = "מבוא ארוך. " * 400  # ~4000 chars, no markers
        tail = "הצעת חוק דומה בכנסת ה־17."
        text = head + tail
        s = parse_recurrence_signals(text)
        assert s["is_recurring"] is False  # marker is past the scan window

    def test_empty_text_returns_default(self):
        s = parse_recurrence_signals("")
        assert s == {
            "is_recurring": False,
            "matched_phrase": None,
            "referenced_private_numbers": [],
            "referenced_knesset": None,
        }


class TestResolveLinkBack:
    def _fake_con(self, rows: dict[tuple, tuple | None]):
        """Build a mock connection that returns specific rows for known queries.

        ``rows`` maps (private_number, knesset|None) -> (BillID,) or None.
        """
        con = MagicMock()

        def execute(sql, params):
            result = MagicMock()
            # Approximate match: lookup by (pn, knesset) for the first arg combo in rows
            key = tuple(params)
            result.fetchone = MagicMock(return_value=rows.get(key))
            return result

        con.execute = MagicMock(side_effect=execute)
        return con

    def test_exact_knesset_match(self):
        con = self._fake_con({(285, 16): (167458,)})
        result = resolve_link_back(
            private_number=285,
            referenced_knesset=16,
            current_knesset=17,
            warehouse_con=con,
        )
        assert result == 167458

    def test_fallback_search_earlier_knessets(self):
        """When referenced_knesset is None, search strictly earlier bills."""
        con = self._fake_con({(285, 18): (167458,)})
        result = resolve_link_back(
            private_number=285,
            referenced_knesset=None,
            current_knesset=18,
            warehouse_con=con,
        )
        assert result == 167458

    def test_returns_none_on_no_match(self):
        con = self._fake_con({})
        result = resolve_link_back(
            private_number=99999,
            referenced_knesset=16,
            current_knesset=17,
            warehouse_con=con,
        )
        assert result is None


class TestDownloadDoc:
    def test_cache_hit_skips_http(self, tmp_path: Path):
        cache = tmp_path / "10001.doc"
        cache.write_bytes(b"cached content")

        with patch("data.recurring_bills.knesset_docs.requests.get") as mock_get:
            path = download_doc("https://example/x.doc", cache)

        assert path == cache
        assert mock_get.call_count == 0

    def test_404_returns_none(self, tmp_path: Path):
        cache = tmp_path / "10001.doc"
        resp = MagicMock(status_code=404)
        with patch("data.recurring_bills.knesset_docs.requests.get", return_value=resp):
            result = download_doc("https://example/missing.doc", cache)
        assert result is None
        assert not cache.exists()

    def test_200_writes_content(self, tmp_path: Path):
        cache = tmp_path / "10001.doc"
        resp = MagicMock(status_code=200, content=b"new doc bytes")
        resp.raise_for_status = MagicMock()
        with patch("data.recurring_bills.knesset_docs.requests.get", return_value=resp):
            path = download_doc("https://example/x.doc", cache)
        assert path == cache
        assert cache.read_bytes() == b"new doc bytes"


class TestClassifyBillFromDoc:
    def test_unsupported_extension_returns_failure(self, tmp_path: Path):
        result = classify_bill_from_doc(
            bill_id=10001,
            current_knesset=16,
            doc_url="https://example/nope.txt",
            cache_dir=tmp_path,
            warehouse_con=MagicMock(),
        )
        assert result["method"] == "doc_fetch_failed"

    def test_404_returns_fetch_failed(self, tmp_path: Path):
        """Graceful handling when Knesset server returns 404 for a doc."""
        resp = MagicMock(status_code=404)
        with patch("data.recurring_bills.knesset_docs.requests.get", return_value=resp), \
             patch("data.recurring_bills.knesset_docs.time.sleep"):
            result = classify_bill_from_doc(
                bill_id=10001,
                current_knesset=16,
                doc_url="https://example/missing.doc",
                cache_dir=tmp_path,
                warehouse_con=MagicMock(),
            )
        assert result["method"] == "doc_fetch_failed"
        assert result["is_recurring"] is False

    def test_pattern_detected_and_linked(self, tmp_path: Path):
        """Doc matches similar-bill phrase AND ref resolves → doc_pattern_linked."""
        cache = tmp_path / "10001.doc"
        cache.write_bytes(b"fake .doc bytes")  # pre-cached to skip download

        # Mock textutil to return text with a recurrence marker
        fake_extraction = MagicMock(
            returncode=0,
            stdout="הצעת חוק דומה הונחה בכנסת ה־16 (פ/285).".encode(),
            stderr=b"",
        )

        # Mock warehouse to resolve פ/285 → BillID 99999
        con = MagicMock()
        con.execute = MagicMock(return_value=MagicMock(fetchone=MagicMock(return_value=(99999,))))

        with patch("data.recurring_bills.knesset_docs.subprocess.run", return_value=fake_extraction):
            result = classify_bill_from_doc(
                bill_id=10001,
                current_knesset=17,
                doc_url="https://example/x.doc",
                cache_dir=tmp_path,
                warehouse_con=con,
            )

        assert result["is_recurring"] is True
        assert result["original_bill_id"] == 99999
        assert result["method"] == "doc_pattern_linked"

    def test_pattern_detected_but_unresolved(self, tmp_path: Path):
        """Doc matches similar-bill phrase but ref doesn't resolve."""
        cache = tmp_path / "10001.doc"
        cache.write_bytes(b"fake .doc bytes")

        fake_extraction = MagicMock(
            returncode=0,
            stdout="הצעת חוק דומה הונחה בכנסת ה־16 (פ/285).".encode(),
            stderr=b"",
        )
        con = MagicMock()
        con.execute = MagicMock(return_value=MagicMock(fetchone=MagicMock(return_value=None)))

        with patch("data.recurring_bills.knesset_docs.subprocess.run", return_value=fake_extraction):
            result = classify_bill_from_doc(
                bill_id=10001,
                current_knesset=17,
                doc_url="https://example/x.doc",
                cache_dir=tmp_path,
                warehouse_con=con,
            )

        assert result["is_recurring"] is True
        assert result["original_bill_id"] is None
        assert result["method"] == "doc_pattern_unresolved"

    def test_no_pattern(self, tmp_path: Path):
        """Doc fetched/parsed but no recurrence marker found."""
        cache = tmp_path / "10001.doc"
        cache.write_bytes(b"fake .doc bytes")

        fake_extraction = MagicMock(
            returncode=0,
            stdout="מבוא. תיקון חדש לחוק.".encode(),
            stderr=b"",
        )

        with patch("data.recurring_bills.knesset_docs.subprocess.run", return_value=fake_extraction):
            result = classify_bill_from_doc(
                bill_id=10001,
                current_knesset=17,
                doc_url="https://example/x.doc",
                cache_dir=tmp_path,
                warehouse_con=MagicMock(),
            )

        assert result["is_recurring"] is False
        assert result["method"] == "doc_no_pattern"

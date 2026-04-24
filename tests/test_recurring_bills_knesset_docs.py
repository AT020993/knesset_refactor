"""Unit tests for src/data/recurring_bills/knesset_docs.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb

from data.recurring_bills.knesset_docs import (
    classify_recurrence_phrase,
    classify_bill_from_doc,
    download_doc,
    extract_submission_date,
    parse_recurrence_signals,
    resolve_link_back,
    validate_submission_date,
)


# Derived from Amnon's reported doc:
# https://fs.knesset.gov.il/19/law/19_lst_268588.docx
AMNON_CASE_544468_TEXT = """
פ/2056/19
דברי הסבר
הצעת חוק זהה הונחה על שולחן הכנסת התשע-עשרה על ידי חבר הכנסת שמעון סולומון
וקבוצת חברי הכנסת (פ/1915/19).

---------------------------------
הוגשה ליו"ר הכנסת והסגנים
והונחה על שולחן הכנסת ביום
י"ד בשבט התשע"ד – 15.1.14
"""

# Derived from Amnon's reported doc:
# https://fs.knesset.gov.il/19/law/19_lst_273433.docx
AMNON_CASE_548636_TEXT = """
דברי הסבר
הצעות חוק דומות בעיקרן הונחו על שולחן הכנסת השש-עשרה על ידי חברת הכנסת ענבל גבריאלי
וקבוצת חברי הכנסת (פ/611), על ידי חבר הכנסת אילן שלגי וקבוצת חברי הכנסת (פ/3064)
וחבר הכנסת דני יתום (פ/3249), על שולחן הכנסת השבע-עשרה על ידי חבר הכנסת יואל חסון
(פ/818/17) וחבר הכנסת יולי יואל אדלשטיין (פ/2457/17), ועל שולחן הכנסת השמונה-עשרה
על ידי חבר הכנסת יולי יואל אדלשטיין (פ/131/18).
הצעת חוק זהה הונחה על שולחן הכנסת השמונה-עשרה על ידי חבר הכנסת אורי אורבך (פ/3068/18).

---------------------------------
הוגשה ליו"ר הכנסת והסגנים
והונחה על שולחן הכנסת ביום
כ"ד באדר א' התשע"ד – 24.2.14
"""

K1_FOOTNOTE_TAIL_TEXT = """
1950
23.11.49)תש"י בכסלו ג' מיום 27 החוקים ספר 1
חוקת המדינה.
"""

K8_SUBMISSION_TAIL_TEXT = """
1עמי14.5.48תש"חבאייר ה1מסרשמי עחון1
.1973פברואר2
.74עמי28.6.67)ו499חוקיםבספר שתוקןכפי 3
.3עמ14.5.48)תש"חנ!<ייו ה'מיום 1מטרשמי עתון4
והסגניםהכנסת ליו"רהוגשה
תלזל"הן בחשוה' שני,ביום
1974באוקטובר21
'*
"""

K14_SUBMISSION_TAIL_TEXT = """
הצעת חוק זהה הוגשה ומספרה פ/2441.

––––––––––––––––––––––––––
הוגשה ליו"ר הכנסת והסגנים
והונחה על שלחן הכנסת ביום
ד` באב התשנ"ח – 98.7.27
"""


def _make_con(rows: list[tuple[int, int, int]]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE KNS_Bill (
            BillID BIGINT,
            KnessetNum BIGINT,
            PrivateNumber BIGINT,
            Name VARCHAR
        )
        """
    )
    if rows:
        con.executemany(
            "INSERT INTO KNS_Bill (BillID, KnessetNum, PrivateNumber, Name) VALUES (?, ?, ?, ?)",
            [
                (bill_id, knesset_num, private_number, f"Bill {bill_id}")
                for bill_id, knesset_num, private_number in rows
            ],
        )
    return con


def _make_named_con(rows: list[tuple[int, int, int, str]]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE KNS_Bill (
            BillID BIGINT,
            KnessetNum BIGINT,
            PrivateNumber BIGINT,
            Name VARCHAR
        )
        """
    )
    if rows:
        con.executemany(
            "INSERT INTO KNS_Bill (BillID, KnessetNum, PrivateNumber, Name) VALUES (?, ?, ?, ?)",
            rows,
        )
    return con


def _classify_from_text(
    *,
    tmp_path: Path,
    warehouse_con,
    text: str,
    bill_id: int,
    current_knesset: int,
) -> dict:
    cache = tmp_path / f"{bill_id}.doc"
    cache.write_bytes(b"cached")
    extraction = MagicMock(returncode=0, stdout=text.encode(), stderr=b"")
    with patch(
        "data.recurring_bills.knesset_docs.subprocess.run", return_value=extraction
    ):
        return classify_bill_from_doc(
            bill_id=bill_id,
            current_knesset=current_knesset,
            doc_url="https://example/x.doc",
            cache_dir=tmp_path,
            warehouse_con=warehouse_con,
        )


class TestParseRecurrenceSignals:
    def test_detects_similar_phrase(self):
        text = "מבוא כלשהו.\nהצעת חוק דומה הוגשה בכנסת ה־17 על ידי חבר הכנסת ש."
        signals = parse_recurrence_signals(text)
        assert signals["is_recurring"] is True
        assert signals["matched_phrase"] == "הצעת חוק דומה"

    def test_detects_identical_phrase(self):
        text = "הצעה זהה הונחה על שולחן הכנסת ה־18."
        signals = parse_recurrence_signals(text)
        assert signals["is_recurring"] is True
        assert "הצעה זהה" in signals["matched_phrase"]

    def test_no_recurrence_markers(self):
        text = "מבוא לחוק. תיקון חדש לחוק הביטוח הלאומי שמציע שינוי."
        signals = parse_recurrence_signals(text)
        assert signals["is_recurring"] is False
        assert signals["matched_phrase"] is None

    def test_extracts_only_local_references_from_recurrence_context(self):
        text = """
        פ/2056/19
        דברי הסבר
        הצעת חוק זהה הונחה על שולחן הכנסת התשע-עשרה (פ/1915/19).
        """
        signals = parse_recurrence_signals(text)
        assert signals["referenced_private_numbers"] == [1915]
        assert signals["reference_candidate_count"] == 1
        assert signals["reference_candidates"][0]["reference_text"] == "פ/1915/19"

    def test_extracts_submission_date_from_bottom_boilerplate(self):
        text = """
        דברי הסבר
        הצעת חוק דומה הונחה בכנסת ה־16 (פ/285).

        ---------------------------------
        הוגשה ליו"ר הכנסת והסגנים
        והונחה על שולחן הכנסת ביום
        י"ד בשבט התשע"ד – 15.1.14
        """
        signals = parse_recurrence_signals(text)
        assert signals["submission_date"] == "2014-01-15"

    def test_extracts_old_knesset_two_digit_year_without_promoting_to_2049(self):
        text = """
        דברי הסבר
        הוגשה ליו"ר הכנסת והסגנים
        והונחה על שולחן הכנסת ביום
        23.11.49
        """
        assert extract_submission_date(text, current_knesset=1) == "1949-11-23"

    def test_rejects_unanchored_footnote_date_from_k1_tail(self):
        assert extract_submission_date(K1_FOOTNOTE_TAIL_TEXT, current_knesset=1) is None

    def test_prefers_contextual_submission_block_over_earlier_tail_dates(self):
        assert (
            extract_submission_date(K8_SUBMISSION_TAIL_TEXT, current_knesset=8)
            == "1974-10-21"
        )

    def test_extracts_numeric_ymd_submission_date_from_bottom_block(self):
        assert (
            extract_submission_date(K14_SUBMISSION_TAIL_TEXT, current_knesset=14)
            == "1998-07-27"
        )

    def test_rejects_future_submission_date(self):
        text = """
        הוגשה ליו"ר הכנסת והסגנים
        והונחה על שלחן הכנסת ביום
        1.12.31
        """
        assert extract_submission_date(text, current_knesset=14) is None

    def test_rejects_future_iso_submission_date(self):
        assert validate_submission_date("2031-12-01", current_knesset=14) is None

    def test_rejects_pre_state_submission_date(self):
        assert validate_submission_date("0196-11-29", current_knesset=5) is None

    def test_rejects_out_of_period_submission_date(self):
        assert validate_submission_date("2019-08-05", current_knesset=3) is None

    def test_classifies_plural_recurrence_phrases(self):
        assert classify_recurrence_phrase("הצעות חוק זהות") == "identical"
        assert classify_recurrence_phrase("הצעות חוק דומות") == "similar"
        assert classify_recurrence_phrase("הצעות חוק דומות בעיקרן") == "similar"
        assert classify_recurrence_phrase("הצעות  חוק  זהות") == "identical"

    def test_amnon_plural_similar_fixture_preserves_all_reference_evidence(self):
        signals = parse_recurrence_signals(AMNON_CASE_548636_TEXT, current_knesset=19)
        assert signals["is_recurring"] is True
        assert signals["multiple_references_detected"] is True
        assert signals["reference_candidate_count"] == 7
        recurrence_types = {
            candidate["recurrence_type"]
            for candidate in signals["reference_candidates"]
        }
        assert recurrence_types == {"identical", "similar"}
        assert signals["submission_date"] == "2014-02-24"

    def test_extracts_older_knesset_name(self):
        text = "הצעת חוק זהה הונחה על שולחן הכנסת הארבע-עשרה."
        signals = parse_recurrence_signals(text, current_knesset=15)
        assert signals["recurrence_phrases"][0]["contextual_knesset"] == 14

    def test_extracts_numeric_knesset_form(self):
        text = "הצעת חוק זהה הונחה על שולחן הכנסת ה-14."
        signals = parse_recurrence_signals(text, current_knesset=15)
        assert signals["recurrence_phrases"][0]["contextual_knesset"] == 14

    def test_extracts_previous_knesset_phrase(self):
        text = "הצעה זהה הוגשה בכנסת הקודמת."
        signals = parse_recurrence_signals(text, current_knesset=12)
        assert signals["recurrence_phrases"][0]["contextual_knesset"] == 11

    def test_empty_text_returns_default(self):
        signals = parse_recurrence_signals("")
        assert signals == {
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


class TestResolveLinkBack:
    def test_exact_knesset_match(self):
        con = _make_con([(167458, 16, 285), (200001, 17, 285)])
        result = resolve_link_back(
            private_number=285,
            referenced_knesset=16,
            current_knesset=17,
            warehouse_con=con,
        )
        assert result == 167458

    def test_missing_knesset_does_not_fall_back_to_source_knesset(self):
        con = _make_con([(167458, 16, 285), (200001, 17, 285)])
        result = resolve_link_back(
            private_number=285,
            referenced_knesset=None,
            current_knesset=17,
            warehouse_con=con,
        )
        assert result is None

    def test_returns_none_on_no_match(self):
        con = _make_con([])
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
            warehouse_con=_make_con([]),
        )
        assert result["method"] == "doc_fetch_failed"

    def test_404_returns_fetch_failed(self, tmp_path: Path):
        resp = MagicMock(status_code=404)
        with (
            patch("data.recurring_bills.knesset_docs.requests.get", return_value=resp),
            patch("data.recurring_bills.knesset_docs.time.sleep"),
        ):
            result = classify_bill_from_doc(
                bill_id=10001,
                current_knesset=16,
                doc_url="https://example/missing.doc",
                cache_dir=tmp_path,
                warehouse_con=_make_con([]),
            )
        assert result["method"] == "doc_fetch_failed"
        assert result["is_recurring"] is False

    def test_bare_private_number_does_not_infer_source_knesset(self, tmp_path: Path):
        con = _make_con(
            [
                (10001, 17, 285),
                (167458, 16, 285),
            ]
        )
        text = "הצעת חוק דומה הונחה על שולחן הכנסת (פ/285)."
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=con,
            text=text,
            bill_id=20000,
            current_knesset=17,
        )

        assert result["is_recurring"] is True
        assert result["original_bill_id"] is None
        assert result["method"] == "doc_pattern_unresolved"
        assert (
            result["reference_resolution_reason"] == "unresolved_missing_target_knesset"
        )
        assert result["reference_candidates"][0]["private_number"] == 285
        assert result["reference_candidates"][0]["referenced_knesset"] is None

    def test_explicit_older_knesset_name_beats_same_knesset_fallback(
        self, tmp_path: Path
    ):
        con = _make_con(
            [
                (170094, 15, 2582),
                (164137, 14, 2582),
            ]
        )
        text = "הצעת חוק זהה הונחה על שולחן הכנסת הארבע-עשרה ומספרה פ/2582."
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=con,
            text=text,
            bill_id=165178,
            current_knesset=15,
        )

        assert result["original_bill_id"] == 164137
        assert (
            result["reference_resolution_reason"] == "contextual_knesset_phrase_match"
        )

    def test_numeric_older_knesset_phrase_beats_same_knesset_fallback(
        self, tmp_path: Path
    ):
        con = _make_con(
            [
                (170608, 15, 1147),
                (164020, 14, 1147),
            ]
        )
        text = "הצעה זהה הונחה על שולחן הכנסת ה-14 ומספרה פ/1147."
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=con,
            text=text,
            bill_id=167879,
            current_knesset=15,
        )

        assert result["original_bill_id"] == 164020
        assert (
            result["reference_resolution_reason"] == "contextual_knesset_phrase_match"
        )

    def test_explicit_private_number_and_knesset_beats_bare_header_reference(
        self, tmp_path: Path
    ):
        con = _make_con(
            [
                (544468, 19, 2056),
                (331944, 18, 2056),
                (488544, 19, 1915),
            ]
        )
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=con,
            text=AMNON_CASE_544468_TEXT,
            bill_id=544468,
            current_knesset=19,
        )

        assert result["original_bill_id"] == 488544
        assert (
            result["reference_resolution_reason"]
            == "explicit_private_number_and_knesset"
        )
        assert result["submission_date"] == "2014-01-15"
        assert result["reference_candidate_count"] == 1
        assert result["reference_candidates"][0]["reference_text"] == "פ/1915/19"

    def test_explicit_reference_does_not_gain_name_fallback_candidate(
        self, tmp_path: Path
    ):
        con = _make_named_con(
            [
                (544468, 19, 2056, "הצעת חוק לדוגמה"),
                (488544, 19, 1915, "הצעת חוק לדוגמה"),
            ]
        )
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=con,
            text=AMNON_CASE_544468_TEXT,
            bill_id=544468,
            current_knesset=19,
        )

        assert result["original_bill_id"] == 488544
        assert result["reference_candidate_count"] == 1
        assert [
            candidate["reference_text"] for candidate in result["reference_candidates"]
        ] == ["פ/1915/19"]

    def test_multiple_references_do_not_pick_unrelated_first_match(
        self, tmp_path: Path
    ):
        con = _make_con(
            [
                (544468, 19, 2056),
                (331944, 18, 2056),
                (488544, 19, 1915),
            ]
        )
        text = """
        פ/2056/19
        הצעת חוק זהה הונחה על שולחן הכנסת התשע-עשרה (פ/1915/19).
        """
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=con,
            text=text,
            bill_id=544468,
            current_knesset=19,
        )

        assert result["original_bill_id"] == 488544
        assert all(
            candidate["private_number"] != 2056
            for candidate in result["reference_candidates"]
        )

    def test_non_original_bill_does_not_silently_self_resolve(self, tmp_path: Path):
        con = _make_con([(544468, 19, 2056)])
        text = "הצעת חוק זהה הונחה על שולחן הכנסת התשע-עשרה (פ/2056/19)."
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=con,
            text=text,
            bill_id=544468,
            current_knesset=19,
        )

        assert result["is_recurring"] is True
        assert result["original_bill_id"] is None
        assert result["method"] == "doc_pattern_unresolved"
        assert result["suspicious_self_resolution"] is True
        assert result["reference_resolution_reason"] == "suspicious_self_reference_only"

    def test_reference_less_old_knesset_phrase_does_not_resolve_from_source_metadata(
        self, tmp_path: Path
    ):
        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE TABLE KNS_Bill (
                BillID BIGINT,
                KnessetNum BIGINT,
                PrivateNumber BIGINT,
                Name VARCHAR
            )
            """
        )
        con.executemany(
            """
            INSERT INTO KNS_Bill (BillID, KnessetNum, PrivateNumber, Name)
            VALUES (?, ?, ?, ?)
            """,
            [
                (150383, 10, 90, 'הצעת חוק מורשת העדות, התשמ"ה-1985'),
                (151979, 11, 161, 'הצעת חוק מורשת העדות, התשמ"ה-1985'),
                (151980, 11, 294, 'הצעת חוק מורשת העדות, התשמ"ה-1985'),
            ],
        )
        text = "הצעת חוק זהה הוגשה בכנסת העשירית."
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=con,
            text=text,
            bill_id=151979,
            current_knesset=11,
        )

        assert result["original_bill_id"] is None
        assert result["method"] == "doc_pattern_unresolved"
        assert result["reference_resolution_reason"] == "unresolved_no_link_or_number"
        assert result["reference_candidate_count"] == 1
        candidate = result["reference_candidates"][0]
        assert candidate["private_number"] is None
        assert candidate["referenced_knesset"] == 10
        assert candidate["resolved_bill_id"] is None

    def test_unresolved_explicit_reference_does_not_fall_back_by_name(
        self, tmp_path: Path
    ):
        con = _make_named_con(
            [
                (150383, 10, 90, 'הצעת חוק מורשת העדות, התשמ"ה-1985'),
                (151979, 11, 161, 'הצעת חוק מורשת העדות, התשמ"ה-1985'),
            ]
        )
        text = "הצעת חוק זהה הוגשה בכנסת העשירית (פ/9999/10)."
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=con,
            text=text,
            bill_id=151979,
            current_knesset=11,
        )

        assert result["original_bill_id"] is None
        assert result["method"] == "doc_pattern_unresolved"
        assert (
            result["reference_resolution_reason"] == "no_resolved_reference_candidates"
        )
        assert result["reference_candidate_count"] == 1
        assert result["reference_candidates"][0]["reference_text"] == "פ/9999/10"

    def test_both_identical_and_similar_references_are_preserved(self, tmp_path: Path):
        con = _make_con(
            [
                (160611, 16, 611),
                (163064, 16, 3064),
                (163249, 16, 3249),
                (170818, 17, 818),
                (172457, 17, 2457),
                (180131, 18, 131),
                (183068, 18, 3068),
            ]
        )
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=con,
            text=AMNON_CASE_548636_TEXT,
            bill_id=548636,
            current_knesset=19,
        )

        assert result["original_bill_id"] == 183068
        assert result["multiple_references_detected"] is True
        assert result["reference_candidate_count"] == 7
        recurrence_types = {
            candidate["recurrence_type"] for candidate in result["reference_candidates"]
        }
        assert recurrence_types == {"identical", "similar"}
        assert (
            result["reference_resolution_reason"]
            == "explicit_private_number_and_knesset"
        )

    def test_equally_strong_primary_candidates_are_marked_ambiguous(
        self, tmp_path: Path
    ):
        con = _make_con(
            [
                (180100, 18, 100),
                (180101, 18, 101),
            ]
        )
        text = """
        הצעת חוק זהה הונחה על שולחן הכנסת השמונה-עשרה (פ/100/18).
        הצעת חוק זהה הונחה על שולחן הכנסת השמונה-עשרה (פ/101/18).
        """
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=con,
            text=text,
            bill_id=190001,
            current_knesset=19,
        )

        assert result["original_bill_id"] is None
        assert result["method"] == "doc_pattern_unresolved"
        assert result["ambiguous_reference_resolution"] is True
        assert (
            result["ambiguous_reference_reason"] == "multiple_equally_strong_candidates"
        )
        assert (
            result["reference_resolution_reason"]
            == "ambiguous_primary_reference_candidates"
        )
        top_candidates = [
            c for c in result["reference_candidates"] if c["tied_for_best"]
        ]
        assert {candidate["resolved_bill_id"] for candidate in top_candidates} == {
            180100,
            180101,
        }

    def test_suspicious_self_reference_never_wins_over_valid_candidate(
        self, tmp_path: Path
    ):
        con = _make_con(
            [
                (544468, 19, 2056),
                (488544, 19, 1915),
            ]
        )
        text = """
        הצעת חוק זהה הונחה על שולחן הכנסת התשע-עשרה (פ/2056/19).
        הצעת חוק זהה הונחה על שולחן הכנסת התשע-עשרה (פ/1915/19).
        """
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=con,
            text=text,
            bill_id=544468,
            current_knesset=19,
        )

        assert result["original_bill_id"] == 488544
        assert result["suspicious_self_resolution"] is True
        assert result["ambiguous_reference_resolution"] is False
        selected = [
            candidate
            for candidate in result["reference_candidates"]
            if candidate["selected"]
        ]
        assert [candidate["resolved_bill_id"] for candidate in selected] == [488544]
        assert all(candidate["resolved_bill_id"] != 544468 for candidate in selected)

    def test_no_pattern(self, tmp_path: Path):
        result = _classify_from_text(
            tmp_path=tmp_path,
            warehouse_con=_make_con([]),
            text="מבוא. תיקון חדש לחוק.",
            bill_id=10001,
            current_knesset=17,
        )
        assert result["is_recurring"] is False
        assert result["method"] == "doc_no_pattern"

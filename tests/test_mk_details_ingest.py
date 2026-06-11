"""Tests for the MK-details parsing/cleaning helpers (no network)."""

from __future__ import annotations

from data.mk_details.ingest import (
    _birth_date,
    _clean,
    _committee_rows,
    _cv_row,
)


def test_clean_unescapes_entities_and_flattens_whitespace() -> None:
    assert _clean("תואר ראשון&#x0D;\nתואר שני") == "תואר ראשון תואר שני"
    assert _clean("  a   b  ") == "a b"
    assert _clean("") is None
    assert _clean(None) is None


def test_birth_date_prefers_gregorian_but_falls_back() -> None:
    assert _birth_date('כ"ו בכסלו תשי"ג , 14/12/1952') == "14/12/1952"
    assert _birth_date("ללא תאריך לועזי") == "ללא תאריך לועזי"
    assert _birth_date(None) is None


def test_cv_row_skips_records_with_no_content() -> None:
    assert _cv_row(7, {}) is None
    assert _cv_row(7, {"DateOfBirth": None, "Education": ""}) is None
    row = _cv_row(7, {"PlaceOfBirth": "חיפה", "Languages": "עברית"})
    assert row == {
        "mk_id": 7,
        "birth_date": None,
        "birth_place_he": "חיפה",
        "education_he": None,
        "military_service_he": None,
        "languages_he": "עברית",
    }


def test_committee_rows_filters_to_target_knesset() -> None:
    positions = [
        {
            "KnessetId": 25,
            "Committee": [
                {
                    "CommitteeName": "ועדת הכספים",
                    "Name": "חבר בוועדת הכספים",
                    "FromDate": "2022-12-29T00:00:00",
                    "ToDate": None,
                }
            ],
        },
        {  # other Knesset — must be ignored
            "KnessetId": 24,
            "Committee": [{"CommitteeName": "ועדת החינוך", "Name": "חבר"}],
        },
    ]
    rows = _committee_rows(7, positions, 25)
    assert rows == [
        {
            "mk_id": 7,
            "knesset_num": 25,
            "committee_name_he": "ועדת הכספים",
            "role_he": "חבר בוועדת הכספים",
            "from_date": "2022-12-29T00:00:00",
            "to_date": None,
        }
    ]

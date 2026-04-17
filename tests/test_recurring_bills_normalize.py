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

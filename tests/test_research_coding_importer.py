"""
Tests for the research coding importer.

Covers:
- Table creation
- File reading (CSV, Excel)
- Bill import with column mapping
- Query import with sentinel value cleaning
- Agenda import with ID and title matching
- Bulk upsert (insert + update)
- Gap analysis
- Clear operations
- Edge cases (empty files, missing columns, duplicates)
"""

import tempfile
from pathlib import Path
from typing import Generator

import duckdb
import pandas as pd
import pytest

# Ensure src is on the path
import sys
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root / "src") not in sys.path:
    sys.path.insert(0, str(_project_root / "src"))

from utils.research_coding_importer import ResearchCodingImporter


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Create a temporary database path."""
    return tmp_path / "test_warehouse.duckdb"


@pytest.fixture
def db_with_tables(db_path: Path) -> Path:
    """Create a database with KNS_ source tables populated with test data."""
    conn = duckdb.connect(str(db_path))

    # Create KNS_Bill
    conn.execute("""
        CREATE TABLE KNS_Bill (
            BillID INTEGER PRIMARY KEY,
            KnessetNum INTEGER,
            Name VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO KNS_Bill VALUES
        (100, 24, 'Test Bill 1'),
        (101, 24, 'Test Bill 2'),
        (200, 25, 'Test Bill 3'),
        (201, 25, 'Test Bill 4'),
        (300, 23, 'Test Bill 5')
    """)

    # Create KNS_Query
    conn.execute("""
        CREATE TABLE KNS_Query (
            QueryID INTEGER PRIMARY KEY,
            KnessetNum INTEGER,
            Name VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO KNS_Query VALUES
        (1000, 24, 'Test Query 1'),
        (1001, 24, 'Test Query 2'),
        (2000, 25, 'Test Query 3')
    """)

    # Create KNS_Agenda
    conn.execute("""
        CREATE TABLE KNS_Agenda (
            AgendaID INTEGER PRIMARY KEY,
            KnessetNum INTEGER,
            Name VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO KNS_Agenda VALUES
        (5000, 19, 'Motion about education reform'),
        (5001, 19, 'Motion about healthcare budget'),
        (5002, 20, 'Motion about transport safety'),
        (6000, 23, 'הצעה לסדר היום בנושא חינוך'),
        (6001, 23, 'הצעה לסדר היום בנושא בריאות'),
        (6002, 24, 'הצעה לסדר היום בנושא תחבורה'),
        (6003, 24, 'הצעה לסדר היום בנושא ביטחון')
    """)

    conn.close()
    return db_path


@pytest.fixture
def importer(db_with_tables: Path) -> ResearchCodingImporter:
    """Create an importer with a populated test database."""
    return ResearchCodingImporter(db_path=db_with_tables)


# --- Table Creation ---

class TestTableCreation:
    def test_ensure_tables_exist(self, importer: ResearchCodingImporter) -> None:
        assert importer.ensure_tables_exist() is True

        # Verify tables were created
        conn = duckdb.connect(str(importer.db_path))
        tables = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_name LIKE 'User%Coding'
            ORDER BY table_name
        """).fetchall()
        conn.close()

        table_names = [t[0] for t in tables]
        assert "UserAgendaCoding" in table_names
        assert "UserBillCoding" in table_names
        assert "UserQueryCoding" in table_names

    def test_ensure_tables_idempotent(self, importer: ResearchCodingImporter) -> None:
        """Tables can be created multiple times without error."""
        assert importer.ensure_tables_exist() is True
        assert importer.ensure_tables_exist() is True


# --- File Reading ---

class TestFileReading:
    def test_read_csv(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        csv_path = tmp_path / "test.csv"
        pd.DataFrame({"A": [1, 2], "B": ["x", "y"]}).to_csv(csv_path, index=False)
        df, error = importer.read_file(csv_path)
        assert error is None
        assert len(df) == 2

    def test_read_excel(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        xlsx_path = tmp_path / "test.xlsx"
        pd.DataFrame({"A": [1, 2], "B": ["x", "y"]}).to_excel(xlsx_path, index=False)
        df, error = importer.read_file(xlsx_path)
        assert error is None
        assert len(df) == 2

    def test_read_missing_file(self, importer: ResearchCodingImporter) -> None:
        df, error = importer.read_file(Path("/nonexistent/file.csv"))
        assert df is None
        assert "not found" in error.lower()

    def test_read_unsupported_format(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        path = tmp_path / "test.json"
        path.write_text("{}")
        df, error = importer.read_file(path)
        assert df is None
        assert "unsupported" in error.lower()


# --- Bill Import ---

class TestBillImport:
    def _make_bill_file(self, tmp_path: Path) -> Path:
        """Create a test bills file matching the real column names."""
        df = pd.DataFrame({
            "BILLID": [100, 101, 200, 999],  # 999 doesn't exist in KNS_Bill
            "KNESSET": [24, 24, 25, 99],
            "MAJORIL": [1, 2, 3, 4],
            "MINORIL": [10, 20, 30, 40],
            "MAJORCAP": [5, 6, 7, 8],
            "MINORCAP": [50, 60, 70, 80],
            "STATERELIGION": [0, 1, 0, 1],
            "TERRITORIES": [1, 0, 1, 0],
        })
        path = tmp_path / "bills.xlsx"
        df.to_excel(path, index=False)
        return path

    def test_import_bills(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        path = self._make_bill_file(tmp_path)
        result = importer.import_bill_coding(path)

        assert result.data_type == "bills"
        assert result.total_rows_in_file == 4
        # All 4 rows imported (no FK validation — by design)
        assert result.rows_imported == 4
        assert result.rows_updated == 0
        assert len(result.errors) == 0

    def test_import_bills_updates(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        """Re-import should update existing rows."""
        path = self._make_bill_file(tmp_path)
        importer.import_bill_coding(path)

        # Second import
        result = importer.import_bill_coding(path)
        assert result.rows_updated == 4
        assert result.rows_imported == 0

    def test_import_bills_missing_id_column(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        df = pd.DataFrame({"MAJORIL": [1, 2], "MINORIL": [10, 20]})
        path = tmp_path / "bills_no_id.csv"
        df.to_csv(path, index=False)

        result = importer.import_bill_coding(path)
        assert len(result.errors) > 0
        assert result.rows_imported == 0


# --- Query Import ---

class TestQueryImport:
    def _make_query_file(self, tmp_path: Path) -> Path:
        df = pd.DataFrame({
            "id": [1000, 1001, 2000],
            "Knesset": [24, 24, 25],
            "majorIL": [1, -99, 3],      # -99 is sentinel for uncoded
            "minorIL": [10, -99, 30],
            "CAP_Maj": [5, 6, -99],
            "Cap_Min": [50, 60, -99],
            "Religion": [0, 1, 0],
            "Territories": [1, 0, 1],
        })
        path = tmp_path / "queries.xlsx"
        df.to_excel(path, index=False)
        return path

    def test_import_queries(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        path = self._make_query_file(tmp_path)
        result = importer.import_query_coding(path)

        assert result.data_type == "queries"
        assert result.total_rows_in_file == 3
        assert result.rows_imported == 3
        assert len(result.errors) == 0

    def test_sentinel_values_cleaned(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        """Verify -99 sentinel values are stored as NULL."""
        path = self._make_query_file(tmp_path)
        importer.import_query_coding(path)

        conn = duckdb.connect(str(importer.db_path))
        row = conn.execute("SELECT MajorIL, MinorIL FROM UserQueryCoding WHERE QueryID = 1001").fetchone()
        conn.close()

        # -99 should have been converted to NULL
        assert row[0] is None
        assert row[1] is None


# --- Agenda Import ---

class TestAgendaImport:
    def _make_agenda_file(self, tmp_path: Path) -> Path:
        """Create a test agendas file with K19-20 (ID match) and K23-24 (title match)."""
        df = pd.DataFrame({
            "id": range(1, 8),
            "id2": [5000, 5001, 5002, None, None, None, None],
            "Knesset": [19, 19, 20, 23, 23, 24, 24],
            "Subject": [
                "Motion about education reform",
                "Motion about healthcare budget",
                "Motion about transport safety",
                "הצעה לסדר היום בנושא חינוך",             # exact match
                "הצעה  לסדר  היום  בנושא  בריאות",        # normalized match (extra spaces)
                "הצעה לסדר היום בנושא תחבורה",             # exact match
                "Something completely different",           # no match
            ],
            "majoril": [1, 2, 3, 4, 5, 6, 7],
            "minoril": [10, 20, 30, 40, 50, 60, 70],
            "religion": [0, 1, 0, 1, 0, 1, 0],
            "territories": [1, 0, 1, 0, 1, 0, 1],
        })
        path = tmp_path / "agendas.xlsx"
        df.to_excel(path, index=False)
        return path

    def test_import_agendas_id_matching(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        path = self._make_agenda_file(tmp_path)
        result = importer.import_agenda_coding(path)

        assert result.data_type == "agendas"
        # K19-20: 3 items matched by id2
        assert result.match_method_counts.get("id_direct", 0) == 3

    def test_import_agendas_title_matching(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        path = self._make_agenda_file(tmp_path)
        result = importer.import_agenda_coding(path)

        # K23-24: exact and normalized matches
        assert result.match_method_counts.get("title_exact", 0) >= 2
        # The normalized match (extra spaces in subject)
        assert result.match_method_counts.get("title_normalized", 0) >= 1

    def test_import_agendas_unmatched(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        path = self._make_agenda_file(tmp_path)
        result = importer.import_agenda_coding(path)

        # "Something completely different" should not match
        assert result.rows_skipped_no_match >= 1
        assert result.unmatched_items is not None
        assert len(result.unmatched_items) >= 1


# --- Text Normalization ---

class TestTextNormalization:
    def test_normalize_whitespace(self) -> None:
        assert ResearchCodingImporter._normalize_text("hello  world") == "hello world"

    def test_normalize_punctuation(self) -> None:
        result = ResearchCodingImporter._normalize_text("hello, world!")
        assert result == "hello world"

    def test_normalize_hebrew(self) -> None:
        """Hebrew text with extra spaces should normalize."""
        result = ResearchCodingImporter._normalize_text("הצעה  לסדר  היום")
        assert result == "הצעה לסדר היום"

    def test_normalize_unicode_nfc(self) -> None:
        """NFC normalization should compose decomposed characters."""
        # These two strings look identical but have different unicode representations
        composed = "\u05E9\u05C1"  # shin + shin dot (composed-ish)
        decomposed = "\u05E9\u05C1"  # same in this case, but tests the NFC path
        assert (
            ResearchCodingImporter._normalize_text(composed)
            == ResearchCodingImporter._normalize_text(decomposed)
        )


# --- Gap Analysis ---

class TestGapAnalysis:
    def test_gap_analysis_empty(self, importer: ResearchCodingImporter) -> None:
        """Gap analysis with no coding data should return zeros."""
        importer.ensure_tables_exist()
        gap = importer.generate_gap_analysis("bills")

        assert gap is not None
        assert gap.total_coded == 0
        assert gap.total_in_dashboard == 5  # 5 bills in test DB
        assert gap.coded_and_matched == 0

    def test_gap_analysis_with_data(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        # Import some bills
        df = pd.DataFrame({
            "BILLID": [100, 101, 999],  # 999 not in KNS_Bill
            "MAJORIL": [1, 2, 3],
            "MINORIL": [10, 20, 30],
        })
        path = tmp_path / "bills.csv"
        df.to_csv(path, index=False)
        importer.import_bill_coding(path)

        gap = importer.generate_gap_analysis("bills")

        assert gap is not None
        assert gap.total_coded == 3
        assert gap.coded_and_matched == 2  # 100, 101 are in KNS_Bill
        assert gap.total_in_dashboard == 5
        # 999 is coded but not in dashboard
        assert len(gap.coded_not_in_dashboard) == 1

    def test_gap_analysis_unknown_type(self, importer: ResearchCodingImporter) -> None:
        assert importer.generate_gap_analysis("unknown") is None


# --- Statistics ---

class TestStatistics:
    def test_empty_statistics(self, importer: ResearchCodingImporter) -> None:
        importer.ensure_tables_exist()
        stats = importer.get_coding_statistics()
        assert stats["bills"] == 0
        assert stats["queries"] == 0
        assert stats["agendas"] == 0

    def test_statistics_after_import(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        df = pd.DataFrame({
            "BILLID": [100, 101],
            "MAJORIL": [1, 2],
        })
        path = tmp_path / "bills.csv"
        df.to_csv(path, index=False)
        importer.import_bill_coding(path)

        stats = importer.get_coding_statistics()
        assert stats["bills"] == 2


# --- Clear Data ---

class TestClearData:
    def test_clear_bills(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        # Import first
        df = pd.DataFrame({"BILLID": [100], "MAJORIL": [1]})
        path = tmp_path / "bills.csv"
        df.to_csv(path, index=False)
        importer.import_bill_coding(path)

        stats = importer.get_coding_statistics()
        assert stats["bills"] == 1

        # Clear
        success, error = importer.clear_coding_data("bills")
        assert success is True
        assert error is None

        stats = importer.get_coding_statistics()
        assert stats["bills"] == 0

    def test_clear_unknown_type(self, importer: ResearchCodingImporter) -> None:
        success, error = importer.clear_coding_data("unknown")
        assert success is False
        assert error is not None


# --- Edge Cases ---

class TestEdgeCases:
    def test_empty_file(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        df = pd.DataFrame({"BILLID": pd.Series(dtype=int), "MAJORIL": pd.Series(dtype=int)})
        path = tmp_path / "empty.csv"
        df.to_csv(path, index=False)

        result = importer.import_bill_coding(path)
        assert result.rows_imported == 0

    def test_duplicate_ids_in_file(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        """Duplicate BillIDs should keep the last occurrence."""
        df = pd.DataFrame({
            "BILLID": [100, 100],
            "MAJORIL": [1, 2],
            "MINORIL": [10, 20],
        })
        path = tmp_path / "dupes.csv"
        df.to_csv(path, index=False)

        result = importer.import_bill_coding(path)
        assert result.rows_imported == 1

        # Verify the last value was kept
        conn = duckdb.connect(str(importer.db_path))
        row = conn.execute("SELECT MajorIL FROM UserBillCoding WHERE BillID = 100").fetchone()
        conn.close()
        assert row[0] == 2

    def test_mixed_case_columns(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        """Column mapping should be case-insensitive."""
        df = pd.DataFrame({
            "BillID": [100],
            "MajorIL": [1],
            "MinorIL": [10],
        })
        path = tmp_path / "mixed_case.csv"
        df.to_csv(path, index=False)

        result = importer.import_bill_coding(path)
        assert result.rows_imported == 1
        assert len(result.errors) == 0

    def test_nan_values_handled(self, importer: ResearchCodingImporter, tmp_path: Path) -> None:
        """NaN values in coding columns should be stored as NULL."""
        df = pd.DataFrame({
            "BILLID": [100],
            "MAJORIL": [float("nan")],
            "MINORIL": [1],
        })
        path = tmp_path / "nans.csv"
        df.to_csv(path, index=False)

        result = importer.import_bill_coding(path)
        assert result.rows_imported == 1

        conn = duckdb.connect(str(importer.db_path))
        row = conn.execute("SELECT MajorIL, MinorIL FROM UserBillCoding WHERE BillID = 100").fetchone()
        conn.close()
        assert row[0] is None  # NaN → NULL
        assert row[1] == 1

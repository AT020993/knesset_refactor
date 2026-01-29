"""
Tests for CAP (Comparative Agendas Project) annotation services.

These tests verify the CAP service modules work correctly:
- CAPTaxonomyService: Table creation, taxonomy loading, category lookups
- CAPAnnotationRepository: CRUD operations for bill annotations
- CAPStatisticsService: Annotation statistics and export
"""
import pytest
import pandas as pd
from pathlib import Path
from unittest import mock
import tempfile
import duckdb


@pytest.fixture
def temp_db_path(tmp_path):
    """Create a temporary database path."""
    return tmp_path / "test_cap.duckdb"


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    return mock.MagicMock()


@pytest.fixture
def initialized_db(temp_db_path):
    """Create and initialize a database with required tables for testing."""
    conn = duckdb.connect(str(temp_db_path))

    # Create minimal KNS_Bill table for testing
    conn.execute("""
        CREATE TABLE KNS_Bill (
            BillID INTEGER PRIMARY KEY,
            KnessetNum INTEGER,
            Name VARCHAR,
            SubTypeDesc VARCHAR,
            PrivateNumber INTEGER,
            PublicationDate TIMESTAMP,
            LastUpdatedDate TIMESTAMP,
            StatusID INTEGER
        )
    """)

    # Create KNS_Status table
    conn.execute("""
        CREATE TABLE KNS_Status (
            StatusID INTEGER PRIMARY KEY,
            "Desc" VARCHAR
        )
    """)

    # Create UserResearchers table for multi-annotator support
    conn.execute("""
        CREATE TABLE UserResearchers (
            ResearcherID INTEGER PRIMARY KEY,
            Username VARCHAR UNIQUE NOT NULL,
            DisplayName VARCHAR NOT NULL,
            PasswordHash VARCHAR NOT NULL,
            Role VARCHAR DEFAULT 'researcher',
            IsActive BOOLEAN DEFAULT TRUE,
            CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            LastLogin TIMESTAMP
        )
    """)

    # Insert sample bills
    conn.execute("""
        INSERT INTO KNS_Bill VALUES
        (1, 25, 'Test Bill 1', 'Private', 12345, '2024-01-01', '2024-01-15', 100),
        (2, 25, 'Test Bill 2', 'Government', NULL, '2024-02-01', '2024-02-15', 118),
        (3, 24, 'Test Bill 3', 'Private', 12346, '2023-06-01', '2023-06-15', 104)
    """)

    conn.execute("""
        INSERT INTO KNS_Status VALUES
        (100, 'In Progress'),
        (118, 'Passed Third Reading'),
        (104, 'First Reading')
    """)

    # Insert test researchers
    conn.execute("""
        INSERT INTO UserResearchers (ResearcherID, Username, DisplayName, PasswordHash, Role, IsActive)
        VALUES
        (1, 'researcher1', 'Test Researcher 1', 'hash1', 'researcher', TRUE),
        (2, 'researcher2', 'Test Researcher 2', 'hash2', 'researcher', TRUE),
        (3, 'admin', 'Admin User', 'hash3', 'admin', TRUE)
    """)

    conn.close()
    return temp_db_path


class TestCAPTaxonomyService:
    """Tests for the CAPTaxonomyService class."""

    def test_direction_column_removed_from_schema(self, temp_db_path, mock_logger):
        """Verify Direction column no longer exists in UserBillCAP table."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from backend.connection_manager import get_db_connection

        service = CAPTaxonomyService(temp_db_path, mock_logger)
        service.ensure_tables_exist()

        with get_db_connection(temp_db_path, read_only=True) as conn:
            columns = conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'UserBillCAP'"
            ).fetchdf()
            column_names = columns["column_name"].str.lower().tolist()

        assert "direction" not in column_names, "Direction column should be removed"

    def test_indexes_created_on_userbillcap(self, temp_db_path, mock_logger):
        """Test that performance indexes are created on UserBillCAP table."""
        from ui.services.cap.taxonomy import CAPTaxonomyService

        service = CAPTaxonomyService(temp_db_path, mock_logger)
        service.ensure_tables_exist()

        import duckdb
        conn = duckdb.connect(str(temp_db_path))
        indexes = conn.execute("""
            SELECT index_name FROM duckdb_indexes()
            WHERE table_name = 'UserBillCAP'
        """).fetchall()
        index_names = [idx[0] for idx in indexes]
        conn.close()

        assert 'idx_userbillcap_billid' in index_names
        assert 'idx_userbillcap_researcherid' in index_names
        assert 'idx_userbillcap_bill_researcher' in index_names

    def test_ensure_tables_exist_creates_tables(self, temp_db_path, mock_logger):
        """Test that ensure_tables_exist creates the CAP tables."""
        from ui.services.cap.taxonomy import CAPTaxonomyService

        service = CAPTaxonomyService(temp_db_path, mock_logger)
        result = service.ensure_tables_exist()

        assert result is True

        # Verify tables exist
        conn = duckdb.connect(str(temp_db_path))
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'User%'"
        ).fetchall()
        table_names = [t[0] for t in tables]

        assert 'UserCAPTaxonomy' in table_names
        assert 'UserBillCAP' in table_names
        conn.close()

    def test_direction_constants_defined(self, temp_db_path, mock_logger):
        """Test that direction constants are properly defined."""
        from ui.services.cap.taxonomy import CAPTaxonomyService

        assert CAPTaxonomyService.DIRECTION_STRENGTHENING == 1
        assert CAPTaxonomyService.DIRECTION_WEAKENING == -1
        assert CAPTaxonomyService.DIRECTION_NEUTRAL == 0

    def test_direction_labels_have_both_languages(self, temp_db_path, mock_logger):
        """Test that direction labels have Hebrew and English versions."""
        from ui.services.cap.taxonomy import CAPTaxonomyService

        for direction, labels in CAPTaxonomyService.DIRECTION_LABELS.items():
            assert len(labels) == 2, f"Direction {direction} should have 2 labels"
            assert isinstance(labels[0], str), f"Hebrew label for {direction} should be string"
            assert isinstance(labels[1], str), f"English label for {direction} should be string"

    def test_get_taxonomy_returns_dataframe(self, temp_db_path, mock_logger):
        """Test that get_taxonomy returns a DataFrame."""
        from ui.services.cap.taxonomy import CAPTaxonomyService

        service = CAPTaxonomyService(temp_db_path, mock_logger)
        service.ensure_tables_exist()

        result = service.get_taxonomy()
        assert isinstance(result, pd.DataFrame)

    def test_get_major_categories_returns_list(self, temp_db_path, mock_logger):
        """Test that get_major_categories returns a list."""
        from ui.services.cap.taxonomy import CAPTaxonomyService

        service = CAPTaxonomyService(temp_db_path, mock_logger)
        service.ensure_tables_exist()

        result = service.get_major_categories()
        assert isinstance(result, list)


class TestCAPAnnotationRepository:
    """Tests for the CAPAnnotationRepository class."""

    def test_save_annotation_rejects_string_researcher_id(self, initialized_db, mock_logger):
        """Test that save_annotation rejects string researcher_id with clear error."""
        from ui.services.cap.repository import CAPAnnotationRepository
        from ui.services.cap.taxonomy import CAPTaxonomyService

        # Setup
        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        import duckdb
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'Test Major', 'Test Major EN', 101, 'Test Minor', 'Test Minor EN')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Should fail with string (common mistake: passing cap_researcher_name)
        result = repo.save_annotation(
            bill_id=1,
            cap_minor_code=101,
            researcher_id="John Doe",  # Wrong! Should be int
        )

        assert result is False
        # Verify error was logged with helpful message about the type confusion
        error_calls = [str(call) for call in mock_logger.error.call_args_list]
        assert any("researcher_id" in str(call) and ("str" in str(call) or "int" in str(call)) for call in error_calls), \
            f"Expected error message about researcher_id type, got: {error_calls}"

    def test_save_annotation_rejects_invalid_researcher_id(self, initialized_db, mock_logger):
        """Test that save_annotation rejects zero or negative researcher_id."""
        from ui.services.cap.repository import CAPAnnotationRepository
        from ui.services.cap.taxonomy import CAPTaxonomyService

        # Setup
        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        import duckdb
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'Test Major', 'Test Major EN', 101, 'Test Minor', 'Test Minor EN')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Should fail with zero
        result_zero = repo.save_annotation(
            bill_id=1,
            cap_minor_code=101,
            researcher_id=0,
        )
        assert result_zero is False

        # Should fail with negative
        result_negative = repo.save_annotation(
            bill_id=1,
            cap_minor_code=101,
            researcher_id=-1,
        )
        assert result_negative is False

    def test_get_uncoded_bills_returns_dataframe(self, initialized_db, mock_logger):
        """Test that get_uncoded_bills returns a DataFrame."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        # First create the CAP tables
        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)
        result = repo.get_uncoded_bills()

        assert isinstance(result, pd.DataFrame)
        # Should have 3 uncoded bills (no annotations yet)
        assert len(result) == 3

    def test_get_uncoded_bills_with_knesset_filter(self, initialized_db, mock_logger):
        """Test that get_uncoded_bills respects Knesset filter."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)
        result = repo.get_uncoded_bills(knesset_num=25)

        assert isinstance(result, pd.DataFrame)
        # Should only return Knesset 25 bills
        assert len(result) == 2
        assert all(result['KnessetNum'] == 25)

    def test_get_uncoded_bills_respects_limit(self, initialized_db, mock_logger):
        """Test that get_uncoded_bills respects the limit parameter."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)
        result = repo.get_uncoded_bills(limit=1)

        assert len(result) == 1

    def test_get_coded_bills_empty_when_no_annotations(self, initialized_db, mock_logger):
        """Test that get_coded_bills returns empty when no annotations exist."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)
        result = repo.get_coded_bills()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_save_annotation_creates_record(self, initialized_db, mock_logger):
        """Test that save_annotation creates a new annotation record."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert a taxonomy entry for testing
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)
        result = repo.save_annotation(
            bill_id=1,
            cap_minor_code=101,
            researcher_id=1,  # Use researcher_id instead of assigned_by
            confidence="High",
            notes="Test annotation"
        )

        assert result is True

        # Verify annotation was created
        coded_bills = repo.get_coded_bills()
        assert len(coded_bills) == 1
        assert coded_bills.iloc[0]['BillID'] == 1

    def test_get_annotation_by_bill_id(self, initialized_db, mock_logger):
        """Test retrieving annotation by bill ID."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy and annotation
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)
        repo.save_annotation(
            bill_id=1,
            cap_minor_code=101,
            researcher_id=1  # Use researcher_id instead of assigned_by
        )

        annotation = repo.get_annotation_by_bill_id(1, researcher_id=1)

        assert annotation is not None
        assert annotation['BillID'] == 1
        assert annotation['CAPMinorCode'] == 101

    def test_delete_annotation(self, initialized_db, mock_logger):
        """Test deleting an annotation."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy and annotation
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)

        # Verify annotation exists
        assert repo.get_annotation_by_bill_id(1, researcher_id=1) is not None

        # Delete annotation (must specify researcher_id in multi-annotator mode)
        result = repo.delete_annotation(1, researcher_id=1)
        assert result is True

        # Verify annotation is gone
        assert repo.get_annotation_by_bill_id(1, researcher_id=1) is None

    def test_multiple_researchers_annotate_same_bill(self, initialized_db, mock_logger):
        """Test that multiple researchers can annotate the same bill independently."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts'),
                   (1, 'מוסדות שלטון', 'Government Institutions', 102, 'ממשלה', 'Government')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Researcher 1 annotates bill 1 with code 101
        result1 = repo.save_annotation(
            bill_id=1, cap_minor_code=101, researcher_id=1
        )
        assert result1 is True

        # Researcher 2 annotates the SAME bill with different code
        result2 = repo.save_annotation(
            bill_id=1, cap_minor_code=102, researcher_id=2
        )
        assert result2 is True

        # Both annotations should exist
        ann1 = repo.get_annotation_by_bill_id(1, researcher_id=1)
        ann2 = repo.get_annotation_by_bill_id(1, researcher_id=2)

        assert ann1 is not None
        assert ann2 is not None
        assert ann1['CAPMinorCode'] == 101
        assert ann2['CAPMinorCode'] == 102

    def test_get_all_annotations_for_bill(self, initialized_db, mock_logger):
        """Test retrieving all annotations for a bill from all researchers."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Two researchers annotate the same bill
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=2)

        # Get all annotations
        all_annotations = repo.get_all_annotations_for_bill(1)

        assert len(all_annotations) == 2
        assert set(all_annotations['ResearcherID'].tolist()) == {1, 2}

    def test_get_uncoded_bills_filters_by_researcher(self, initialized_db, mock_logger):
        """Test that get_uncoded_bills only shows bills not annotated by specific researcher."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Researcher 1 annotates bill 1
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)

        # Researcher 1 should see 2 uncoded bills (bills 2 and 3)
        uncoded_r1 = repo.get_uncoded_bills(researcher_id=1)
        assert len(uncoded_r1) == 2
        assert 1 not in uncoded_r1['BillID'].tolist()

        # Researcher 2 should see all 3 uncoded bills (hasn't annotated any)
        uncoded_r2 = repo.get_uncoded_bills(researcher_id=2)
        assert len(uncoded_r2) == 3

    def test_save_annotation_upsert_updates_existing(self, initialized_db, mock_logger):
        """Test that save_annotation updates when researcher re-annotates same bill."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy entries
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts'),
                   (1, 'מוסדות שלטון', 'Government Institutions', 102, 'ממשלה', 'Government')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # First annotation
        result1 = repo.save_annotation(
            bill_id=1, cap_minor_code=101, researcher_id=1,
            notes="First annotation"
        )
        assert result1 is True

        # Same researcher updates their annotation on the same bill
        result2 = repo.save_annotation(
            bill_id=1, cap_minor_code=102, researcher_id=1,
            notes="Updated annotation"
        )
        assert result2 is True

        # Should only have ONE annotation for this researcher/bill pair
        annotation = repo.get_annotation_by_bill_id(1, researcher_id=1)
        assert annotation is not None
        assert annotation['CAPMinorCode'] == 102  # Updated value
        assert annotation['Notes'] == "Updated annotation"  # Updated value

        # Verify only one annotation exists (not two)
        all_annotations = repo.get_all_annotations_for_bill(1)
        assert len(all_annotations) == 1

    def test_unique_constraint_prevents_duplicate_researcher_bill(self, initialized_db, mock_logger):
        """Test that UNIQUE(BillID, ResearcherID) constraint is enforced via upsert."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Both annotations should succeed (upsert handles duplicates)
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)

        # Verify database integrity - only one annotation per researcher per bill
        all_for_bill = repo.get_all_annotations_for_bill(1)
        researcher_1_annotations = all_for_bill[all_for_bill['ResearcherID'] == 1]
        assert len(researcher_1_annotations) == 1

    def test_get_coded_bills_shows_annotation_count(self, initialized_db, mock_logger):
        """Test that get_coded_bills shows annotation count per bill.

        Note: get_coded_bills() returns one row per ANNOTATION (not per bill).
        In multi-annotator mode, a bill with 2 annotations returns 2 rows.
        The AnnotationCount column shows the total annotations for that bill.
        """
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Bill 1: annotated by 2 researchers
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=2)

        # Bill 2: annotated by 1 researcher
        repo.save_annotation(bill_id=2, cap_minor_code=101, researcher_id=1)

        coded_bills = repo.get_coded_bills()

        # get_coded_bills returns one row per annotation, not per unique bill
        # Bill 1 has 2 annotations, Bill 2 has 1 annotation = 3 total rows
        assert len(coded_bills) == 3

        # Verify 2 unique bills are represented
        unique_bills = coded_bills['BillID'].unique()
        assert len(unique_bills) == 2

        # Check annotation counts - each row for bill 1 should show count=2
        if 'AnnotationCount' in coded_bills.columns:
            bill1_rows = coded_bills[coded_bills['BillID'] == 1]
            bill2_rows = coded_bills[coded_bills['BillID'] == 2]

            # Bill 1 has 2 annotations
            assert len(bill1_rows) == 2
            assert all(bill1_rows['AnnotationCount'] == 2)

            # Bill 2 has 1 annotation
            assert len(bill2_rows) == 1
            assert bill2_rows.iloc[0]['AnnotationCount'] == 1

    def test_get_bills_with_status_researcher_specific(self, initialized_db, mock_logger):
        """Test that get_bills_with_status correctly shows coded status per researcher."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Researcher 1 annotates bill 1
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)

        # Get bills with status for researcher 1 (include coded)
        bills_r1 = repo.get_bills_with_status(include_coded=True, researcher_id=1)
        bill1_r1 = bills_r1[bills_r1['BillID'] == 1]
        assert len(bill1_r1) == 1
        assert bill1_r1.iloc[0]['IsCoded'] == 1  # Coded BY researcher 1

        # Get bills with status for researcher 2 (include coded)
        bills_r2 = repo.get_bills_with_status(include_coded=True, researcher_id=2)
        bill1_r2 = bills_r2[bills_r2['BillID'] == 1]
        assert len(bill1_r2) == 1
        assert bill1_r2.iloc[0]['IsCoded'] == 0  # NOT coded by researcher 2

    def test_get_recent_annotations(self, initialized_db, mock_logger):
        """Test that get_recent_annotations returns the most recent annotations."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Create several annotations
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)
        repo.save_annotation(bill_id=2, cap_minor_code=101, researcher_id=1)
        repo.save_annotation(bill_id=3, cap_minor_code=101, researcher_id=2)

        # Get recent annotations (limit 2)
        recent = repo.get_recent_annotations(limit=2)
        assert len(recent) == 2

        # Get recent for researcher 1 only
        recent_r1 = repo.get_recent_annotations(limit=5, researcher_id=1)
        assert len(recent_r1) == 2
        assert all(recent_r1['ResearcherID'] == 1)

    def test_get_coded_bills_filters_by_researcher(self, initialized_db, mock_logger):
        """Test that get_coded_bills can filter by specific researcher."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Researcher 1 annotates bills 1 and 2
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)
        repo.save_annotation(bill_id=2, cap_minor_code=101, researcher_id=1)

        # Researcher 2 annotates bill 3
        repo.save_annotation(bill_id=3, cap_minor_code=101, researcher_id=2)

        # Get all coded bills
        all_coded = repo.get_coded_bills()
        assert len(all_coded) == 3

        # Get only researcher 1's coded bills
        coded_r1 = repo.get_coded_bills(researcher_id=1)
        assert len(coded_r1) == 2
        assert set(coded_r1['BillID'].tolist()) == {1, 2}

        # Get only researcher 2's coded bills
        coded_r2 = repo.get_coded_bills(researcher_id=2)
        assert len(coded_r2) == 1
        assert coded_r2.iloc[0]['BillID'] == 3


class TestCAPAnnotationValidation:
    """Tests for annotation validation - FK checks before save."""

    def test_save_annotation_rejects_nonexistent_researcher(self, initialized_db, mock_logger):
        """Verify annotation fails if researcher doesn't exist."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert a valid CAP code so we're only testing researcher validation
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'Test Major', 'Test Major EN', 100, 'Test Minor', 'Test Minor EN')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        result = repo.save_annotation(
            bill_id=12345,
            cap_minor_code=100,
            researcher_id=99999,  # Non-existent researcher
            confidence="Medium",
            notes="Test",
            source="Database",
        )

        assert result is False, "Should reject non-existent researcher"

    def test_save_annotation_rejects_inactive_researcher(self, initialized_db, mock_logger):
        """Verify annotation fails if researcher is inactive."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert a valid CAP code
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'Test Major', 'Test Major EN', 100, 'Test Minor', 'Test Minor EN')
        """)
        # Create an inactive researcher
        conn.execute("""
            INSERT INTO UserResearchers (ResearcherID, Username, DisplayName, PasswordHash, Role, IsActive)
            VALUES (999, 'inactive_user', 'Inactive User', 'hash', 'researcher', FALSE)
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        result = repo.save_annotation(
            bill_id=12345,
            cap_minor_code=100,
            researcher_id=999,  # Inactive researcher
            confidence="Medium",
            notes="Test",
            source="Database",
        )

        assert result is False, "Should reject inactive researcher"

    def test_save_annotation_rejects_invalid_cap_code(self, initialized_db, mock_logger):
        """Verify annotation fails if CAP code doesn't exist."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()
        taxonomy.load_taxonomy_from_csv()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        result = repo.save_annotation(
            bill_id=12345,
            cap_minor_code=99999,  # Non-existent CAP code
            researcher_id=1,  # Valid researcher from initialized_db
            confidence="Medium",
            notes="Test",
            source="Database",
        )

        assert result is False, "Should reject non-existent CAP code"

    def test_save_annotation_succeeds_with_valid_foreign_keys(self, initialized_db, mock_logger):
        """Verify annotation succeeds when both researcher and CAP code exist."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert a valid CAP code
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'Test Major', 'Test Major EN', 100, 'Test Minor', 'Test Minor EN')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        result = repo.save_annotation(
            bill_id=1,  # Valid bill from initialized_db
            cap_minor_code=100,  # Valid CAP code
            researcher_id=1,  # Valid, active researcher from initialized_db
            confidence="Medium",
            notes="Test",
            source="Database",
        )

        assert result is True, "Should succeed with valid foreign keys"


class TestCAPStatisticsService:
    """Tests for the CAPStatisticsService class."""

    def test_get_annotation_stats_returns_dict(self, initialized_db, mock_logger):
        """Test that get_annotation_stats returns a dictionary."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.statistics import CAPStatisticsService

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        stats_service = CAPStatisticsService(initialized_db, mock_logger)
        result = stats_service.get_annotation_stats()

        assert isinstance(result, dict)
        assert 'total_coded' in result
        assert 'total_bills' in result

    def test_get_annotation_stats_counts_correctly(self, initialized_db, mock_logger):
        """Test that statistics count bills correctly."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository
        from ui.services.cap.statistics import CAPStatisticsService

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Add taxonomy entry
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        # Add annotations from researcher 1
        repo = CAPAnnotationRepository(initialized_db, mock_logger)
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)
        repo.save_annotation(bill_id=2, cap_minor_code=101, researcher_id=1)

        stats_service = CAPStatisticsService(initialized_db, mock_logger)
        result = stats_service.get_annotation_stats()

        assert result['total_coded'] == 2
        assert result['total_bills'] == 3  # 3 bills in test data

    def test_get_coverage_stats_returns_dict(self, initialized_db, mock_logger):
        """Test that get_coverage_stats returns a dictionary."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.statistics import CAPStatisticsService

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        stats_service = CAPStatisticsService(initialized_db, mock_logger)
        result = stats_service.get_coverage_stats()

        assert isinstance(result, dict)

    def test_statistics_counts_distinct_bills_not_annotations(self, initialized_db, mock_logger):
        """Test that statistics count distinct bills, not individual annotations.

        When multiple researchers annotate the same bill, statistics should
        report the bill as coded ONCE, not multiple times.
        """
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository
        from ui.services.cap.statistics import CAPStatisticsService

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Add taxonomy entry
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Bill 1: annotated by 3 different researchers (should count as 1 bill)
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=2)
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=3)

        # Bill 2: annotated by 1 researcher (should count as 1 bill)
        repo.save_annotation(bill_id=2, cap_minor_code=101, researcher_id=1)

        stats_service = CAPStatisticsService(initialized_db, mock_logger)
        result = stats_service.get_annotation_stats()

        # Should count 2 coded bills, NOT 4 annotations
        assert result['total_coded'] == 2
        assert result['total_bills'] == 3  # 3 bills in test data


class TestCAPUserService:
    """Tests for the CAPUserService class."""

    def test_get_user_annotation_count_uses_researcher_id(self, initialized_db, mock_logger):
        """Test that get_user_annotation_count uses ResearcherID column (not AssignedBy).

        This test verifies Bug 3 fix - the query should use ResearcherID directly,
        not a subquery lookup on AssignedBy/DisplayName.
        """
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository
        from ui.services.cap.user_service import CAPUserService

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        # Create annotations for researcher 1 (ID=1)
        repo = CAPAnnotationRepository(initialized_db, mock_logger)
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)
        repo.save_annotation(bill_id=2, cap_minor_code=101, researcher_id=1)

        # Create annotation for researcher 2 (ID=2)
        repo.save_annotation(bill_id=3, cap_minor_code=101, researcher_id=2)

        user_service = CAPUserService(initialized_db, mock_logger)

        # Researcher 1 should have 2 annotations
        count_r1 = user_service.get_user_annotation_count(1)
        assert count_r1 == 2

        # Researcher 2 should have 1 annotation
        count_r2 = user_service.get_user_annotation_count(2)
        assert count_r2 == 1

        # Non-existent researcher should have 0 annotations
        count_r99 = user_service.get_user_annotation_count(99)
        assert count_r99 == 0

    def test_get_user_annotation_count_no_annotations_table(self, temp_db_path, mock_logger):
        """Test get_user_annotation_count returns 0 when UserBillCAP table doesn't exist."""
        from ui.services.cap.user_service import CAPUserService

        # Create just the UserResearchers table
        user_service = CAPUserService(temp_db_path, mock_logger)
        user_service.ensure_table_exists()

        # Should return 0, not raise an error
        count = user_service.get_user_annotation_count(1)
        assert count == 0


class TestCAPStatisticsServiceAdditional:
    """Additional tests for CAPStatisticsService edge cases."""

    def test_get_annotation_stats_empty_database(self, initialized_db, mock_logger):
        """Test get_annotation_stats handles empty annotations gracefully."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.statistics import CAPStatisticsService

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        stats_service = CAPStatisticsService(initialized_db, mock_logger)
        result = stats_service.get_annotation_stats()

        assert isinstance(result, dict)
        assert result['total_coded'] == 0
        assert result['total_annotations'] == 0
        assert result['total_bills'] == 3  # From initialized_db fixture
        assert result['total_researchers'] == 0
        assert result['by_major_category'] == []
        assert result['by_direction'] == []
        assert result['by_researcher'] == []

    def test_get_coverage_stats_no_division_by_zero(self, temp_db_path, mock_logger):
        """Test get_coverage_stats handles Knessets with zero bills gracefully.

        This test verifies Bug 7 fix - NULLIF prevents division by zero.
        """
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.statistics import CAPStatisticsService

        # Create minimal tables without any bills
        conn = duckdb.connect(str(temp_db_path))
        conn.execute("""
            CREATE TABLE KNS_Bill (
                BillID INTEGER PRIMARY KEY,
                KnessetNum INTEGER,
                Name VARCHAR
            )
        """)
        conn.close()

        taxonomy = CAPTaxonomyService(temp_db_path, mock_logger)
        taxonomy.ensure_tables_exist()

        stats_service = CAPStatisticsService(temp_db_path, mock_logger)

        # Should not raise division by zero error
        result = stats_service.get_coverage_stats()
        assert isinstance(result, dict)
        assert 'by_knesset' in result
        # Empty result because no bills exist
        assert result['by_knesset'] == []

    def test_export_annotations_creates_csv(self, initialized_db, mock_logger, tmp_path):
        """Test that export_annotations creates a valid CSV file."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository
        from ui.services.cap.statistics import CAPStatisticsService

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        # Create annotations
        repo = CAPAnnotationRepository(initialized_db, mock_logger)
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1, notes="Test note")
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=2)

        # Export
        stats_service = CAPStatisticsService(initialized_db, mock_logger)
        export_path = tmp_path / "test_export.csv"
        result = stats_service.export_annotations(export_path)

        assert result is True
        assert export_path.exists()

        # Verify CSV content
        exported_df = pd.read_csv(export_path)
        assert len(exported_df) == 2  # Two annotations
        assert 'BillID' in exported_df.columns
        assert 'ResearcherID' in exported_df.columns
        assert 'ResearcherName' in exported_df.columns
        assert set(exported_df['BillID'].tolist()) == {1}  # Both annotations for bill 1
        assert set(exported_df['ResearcherID'].tolist()) == {1, 2}  # Two researchers


class TestCAPAnnotationCountsCache:
    """Tests for annotation counts caching."""

    def test_get_annotation_counts_returns_dict(self, initialized_db, mock_logger):
        """Test that get_annotation_counts returns a dictionary."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)
        counts = repo.get_annotation_counts()

        assert isinstance(counts, dict)

    def test_get_annotation_counts_correct_values(self, initialized_db, mock_logger):
        """Test that get_annotation_counts returns correct values."""
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository, clear_annotation_counts_cache

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'Test', 'Test EN', 101, 'Test Minor', 'Test Minor EN')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Clear cache first
        clear_annotation_counts_cache()

        # Add annotations
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=2)
        repo.save_annotation(bill_id=2, cap_minor_code=101, researcher_id=1)

        counts = repo.get_annotation_counts()

        assert counts.get(1) == 2  # Bill 1 has 2 annotations
        assert counts.get(2) == 1  # Bill 2 has 1 annotation
        assert counts.get(999) is None  # Bill 999 doesn't exist

    def test_clear_annotation_counts_cache(self, initialized_db, mock_logger):
        """Test that cache clearing works."""
        from ui.services.cap.repository import clear_annotation_counts_cache

        # Should not raise an error
        clear_annotation_counts_cache()


class TestCAPRepositoryAdditional:
    """Additional tests for CAPAnnotationRepository edge cases."""

    def test_get_bills_not_in_database_with_researcher_filter(self, initialized_db, mock_logger):
        """Test that get_bills_not_in_database correctly filters by researcher.

        Bills that are in the database AND already annotated by the researcher
        should be excluded from the API fetch results.
        """
        from ui.services.cap.taxonomy import CAPTaxonomyService
        from ui.services.cap.repository import CAPAnnotationRepository

        taxonomy = CAPTaxonomyService(initialized_db, mock_logger)
        taxonomy.ensure_tables_exist()

        # Insert taxonomy
        conn = duckdb.connect(str(initialized_db))
        conn.execute("""
            INSERT INTO UserCAPTaxonomy
            (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode, MinorTopic_HE, MinorTopic_EN)
            VALUES (1, 'מוסדות שלטון', 'Government Institutions', 101, 'בתי משפט', 'Courts')
        """)
        conn.close()

        repo = CAPAnnotationRepository(initialized_db, mock_logger)

        # Researcher 1 annotates bill 1
        repo.save_annotation(bill_id=1, cap_minor_code=101, researcher_id=1)

        # Simulate API bills DataFrame (bills 1, 2, and a new bill 999)
        api_bills = pd.DataFrame({
            'BillID': [1, 2, 999],  # 1 is in DB and coded by R1, 2 is in DB but not coded, 999 is new
            'Name': ['Bill 1', 'Bill 2', 'New Bill'],
            'KnessetNum': [25, 25, 25],
        })

        # For researcher 1: should exclude bill 1 (already coded by them)
        # Bills 2 (in DB, not coded by R1) and 999 (not in DB) should be included
        result_r1 = repo.get_bills_not_in_database(api_bills, limit=100, researcher_id=1)

        # Bill 1 is in DB AND coded by R1 -> excluded
        # Bill 2 is in DB but not coded by R1 -> depends on implementation
        # Bill 999 is not in DB -> included
        assert 999 in result_r1['BillID'].tolist()
        assert 1 not in result_r1['BillID'].tolist()

        # For researcher 2: bill 1 was coded by R1, not R2
        # So R2 should see bill 999 (not in DB)
        result_r2 = repo.get_bills_not_in_database(api_bills, limit=100, researcher_id=2)
        assert 999 in result_r2['BillID'].tolist()


class TestCAPServiceFacade:
    """Tests for the CAPAnnotationService facade."""

    def test_facade_imports_correctly(self):
        """Test that the facade can be imported."""
        from ui.services.cap_service import CAPAnnotationService
        assert CAPAnnotationService is not None

    def test_facade_factory_function(self, temp_db_path, mock_logger):
        """Test the get_cap_service factory function."""
        from ui.services.cap_service import get_cap_service

        service = get_cap_service(temp_db_path, mock_logger)
        assert service is not None
        assert hasattr(service, 'ensure_tables_exist')
        assert hasattr(service, 'save_annotation')
        assert hasattr(service, 'get_annotation_stats')

    def test_facade_delegates_to_taxonomy(self, temp_db_path, mock_logger):
        """Test that facade delegates taxonomy operations correctly."""
        from ui.services.cap_service import CAPAnnotationService

        service = CAPAnnotationService(temp_db_path, mock_logger)
        result = service.ensure_tables_exist()

        assert result is True

    def test_facade_has_required_methods(self):
        """Test that facade has the required methods."""
        from ui.services.cap_service import CAPAnnotationService

        # Verify key methods exist on the facade
        assert hasattr(CAPAnnotationService, 'ensure_tables_exist')
        assert hasattr(CAPAnnotationService, 'save_annotation')
        assert hasattr(CAPAnnotationService, 'get_annotation_stats')


class TestCAPUserServiceActiveStatus:
    """Tests for user active status checking.

    These tests verify the is_user_active() method that allows checking
    if a user is currently active (not deactivated by admin).
    """

    def test_is_user_active_returns_true_for_active_user(self, initialized_db, mock_logger):
        """Test that is_user_active returns True for an active user."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(initialized_db, mock_logger)

        # Researcher 1 is active in initialized_db fixture
        result = user_service.is_user_active(1)

        assert result is True

    def test_is_user_active_returns_false_for_deactivated_user(self, initialized_db, mock_logger):
        """Test that is_user_active returns False for a deactivated user."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(initialized_db, mock_logger)

        # Deactivate researcher 1
        user_service.delete_user(1)  # soft delete sets IsActive=FALSE

        result = user_service.is_user_active(1)

        assert result is False

    def test_is_user_active_returns_false_for_nonexistent_user(self, initialized_db, mock_logger):
        """Test that is_user_active returns False for a user that doesn't exist."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(initialized_db, mock_logger)

        # User ID 99999 doesn't exist
        result = user_service.is_user_active(99999)

        assert result is False

    def test_is_user_active_returns_false_on_database_error(self, temp_db_path, mock_logger):
        """Test that is_user_active returns False on database errors (fail secure)."""
        from ui.services.cap.user_service import CAPUserService

        # Create user service with a path that doesn't have the table
        user_service = CAPUserService(temp_db_path, mock_logger)
        # Don't call ensure_table_exists() - table doesn't exist

        # Should return False (fail secure), not raise an error
        result = user_service.is_user_active(1)

        assert result is False

    def test_is_user_active_handles_invalid_user_id_types(self, initialized_db, mock_logger):
        """Test that is_user_active handles invalid user_id types gracefully (fail secure)."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(initialized_db, mock_logger)

        # Should return False (fail secure) for invalid types, not crash
        # None should be handled gracefully
        assert user_service.is_user_active(None) is False
        # Zero is technically valid but no user has ID 0
        assert user_service.is_user_active(0) is False
        # Negative IDs don't exist
        assert user_service.is_user_active(-1) is False


class TestCAPUserServiceSequence:
    """Tests for user service with proper DuckDB sequences.

    These tests verify that user ID generation uses sequences instead of
    MAX()+1, which prevents race conditions when multiple admins create
    users simultaneously.
    """

    def test_create_user_uses_sequence(self, temp_db_path, mock_logger):
        """Verify user creation uses sequence, not MAX()+1."""
        from ui.services.cap.user_service import CAPUserService
        from backend.connection_manager import get_db_connection

        user_service = CAPUserService(temp_db_path, mock_logger)

        # Create first user
        success = user_service.create_user(
            username="seq_test_1",
            display_name="Seq Test 1",
            password="Password1",
            role="researcher",
        )
        assert success, "Failed to create first user"

        # Check sequence exists (use duckdb_sequences() function, not information_schema)
        with get_db_connection(temp_db_path, read_only=True) as conn:
            result = conn.execute(
                "SELECT sequence_name FROM duckdb_sequences() "
                "WHERE sequence_name = 'seq_researcher_id'"
            ).fetchone()
            assert result is not None, "Sequence seq_researcher_id should exist"

    def test_create_multiple_users_no_id_collision(self, temp_db_path, mock_logger):
        """Verify multiple users get unique IDs via sequence."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(temp_db_path, mock_logger)

        # Create multiple users
        for i in range(5):
            success = user_service.create_user(
                username=f"multi_user_{i}",
                display_name=f"Multi User {i}",
                password="Password1",
                role="researcher",
            )
            assert success, f"Failed to create user {i}"

        users_df = user_service.get_all_users()
        multi_users = users_df[users_df["Username"].str.startswith("multi_user_")]
        ids = multi_users["ResearcherID"].tolist()

        # All IDs should be unique
        assert len(ids) == len(set(ids)), f"Duplicate IDs found: {ids}"
        assert len(ids) == 5, f"Expected 5 users, got {len(ids)}"

    def test_sequence_continues_after_delete(self, temp_db_path, mock_logger):
        """Verify sequence doesn't reset after deleting users."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(temp_db_path, mock_logger)

        # Create user and get their ID
        user_service.create_user(
            username="user_to_delete",
            display_name="Delete Me",
            password="Password1",
            role="researcher",
        )
        users_df = user_service.get_all_users()
        first_id = users_df[users_df["Username"] == "user_to_delete"]["ResearcherID"].iloc[0]

        # Hard delete the user (soft delete won't affect this test)
        user_service.hard_delete_user(first_id)

        # Create another user
        user_service.create_user(
            username="user_after_delete",
            display_name="After Delete",
            password="Password1",
            role="researcher",
        )
        users_df = user_service.get_all_users()
        second_id = users_df[users_df["Username"] == "user_after_delete"]["ResearcherID"].iloc[0]

        # New user should have a higher ID, not reuse the deleted one
        assert second_id > first_id, f"ID should continue from sequence, got {second_id} after {first_id}"


class TestCAPUserServiceValidation:
    """Tests for user creation validation - Task 9 QA Plan.

    These tests verify pre-validation before user creation to provide
    clear error messages instead of relying on DB constraint errors.
    """

    def test_user_exists_returns_true_for_existing_user(self, initialized_db, mock_logger):
        """Test that user_exists returns True for an existing username."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(initialized_db, mock_logger)

        # 'researcher1' exists in initialized_db fixture
        result = user_service.user_exists("researcher1")

        assert result is True

    def test_user_exists_returns_false_for_nonexistent_user(self, initialized_db, mock_logger):
        """Test that user_exists returns False for a non-existent username."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(initialized_db, mock_logger)

        result = user_service.user_exists("nonexistent_username_xyz")

        assert result is False

    def test_user_exists_returns_true_on_error_fail_secure(self, temp_db_path, mock_logger):
        """Test that user_exists returns True on database errors (fail secure).

        When we can't verify if a user exists, we should assume they do
        to prevent accidental duplicate creation.
        """
        from ui.services.cap.user_service import CAPUserService

        # Create service but don't initialize tables - database access will fail
        user_service = CAPUserService(temp_db_path, mock_logger)
        # Force _table_ensured to False to skip table creation
        user_service._table_ensured = False

        # Corrupt the connection by closing the underlying database
        # Actually, let's test with a mock that raises an exception
        with mock.patch.object(user_service, 'ensure_table_exists', return_value=True):
            with mock.patch('ui.services.cap.user_service.get_db_connection') as mock_conn:
                mock_conn.side_effect = Exception("Database connection error")
                result = user_service.user_exists("any_user")

        # Should return True (fail secure) on error
        assert result is True

    def test_create_user_with_validation_success(self, temp_db_path, mock_logger):
        """Test create_user_with_validation succeeds with valid inputs."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(temp_db_path, mock_logger)

        user_id, error = user_service.create_user_with_validation(
            username="valid_user",
            display_name="Valid User",
            password="Password1",
            role="researcher"
        )

        assert error is None, f"Expected no error, got: {error}"
        assert user_id is not None
        assert isinstance(user_id, int)
        assert user_id > 0

        # Verify user was actually created
        user = user_service.get_user_by_id(user_id)
        assert user is not None
        assert user["username"] == "valid_user"
        assert user["display_name"] == "Valid User"
        assert user["role"] == "researcher"

    def test_create_user_with_validation_rejects_duplicate_username(self, temp_db_path, mock_logger):
        """Test create_user_with_validation rejects duplicate username."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(temp_db_path, mock_logger)

        # Create first user
        user_id1, error1 = user_service.create_user_with_validation(
            username="duplicate_test",
            display_name="First User",
            password="Password1",
            role="researcher"
        )
        assert error1 is None

        # Try to create second user with same username
        user_id2, error2 = user_service.create_user_with_validation(
            username="duplicate_test",
            display_name="Second User",
            password="DiffPass2",
            role="admin"
        )

        assert user_id2 is None
        assert error2 is not None
        assert "username" in error2.lower() and ("exists" in error2.lower() or "taken" in error2.lower())

    def test_create_user_with_validation_rejects_short_username(self, temp_db_path, mock_logger):
        """Test create_user_with_validation rejects username shorter than 3 chars."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(temp_db_path, mock_logger)

        user_id, error = user_service.create_user_with_validation(
            username="ab",  # Too short - only 2 chars
            display_name="Short Username User",
            password="Password1",
            role="researcher"
        )

        assert user_id is None
        assert error is not None
        assert "username" in error.lower()
        assert "3" in error or "character" in error.lower()

    def test_create_user_with_validation_rejects_invalid_username_chars(self, temp_db_path, mock_logger):
        """Test create_user_with_validation rejects username with invalid characters."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(temp_db_path, mock_logger)

        # Test with spaces
        user_id1, error1 = user_service.create_user_with_validation(
            username="user name",
            display_name="User with Space",
            password="Password1",
            role="researcher"
        )
        assert user_id1 is None
        assert error1 is not None
        assert "username" in error1.lower()

        # Test with special characters
        user_id2, error2 = user_service.create_user_with_validation(
            username="user@name",
            display_name="User with At",
            password="Password1",
            role="researcher"
        )
        assert user_id2 is None
        assert error2 is not None

    def test_create_user_with_validation_accepts_underscore_in_username(self, temp_db_path, mock_logger):
        """Test create_user_with_validation accepts underscores in username."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(temp_db_path, mock_logger)

        user_id, error = user_service.create_user_with_validation(
            username="valid_user_name",
            display_name="User with Underscores",
            password="Password1",
            role="researcher"
        )

        assert error is None, f"Expected no error, got: {error}"
        assert user_id is not None

    def test_create_user_with_validation_rejects_short_password(self, temp_db_path, mock_logger):
        """Test create_user_with_validation rejects password shorter than 6 chars."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(temp_db_path, mock_logger)

        user_id, error = user_service.create_user_with_validation(
            username="valid_user",
            display_name="Valid Name",
            password="12345",  # Too short - only 5 chars
            role="researcher"
        )

        assert user_id is None
        assert error is not None
        assert "password" in error.lower()
        assert "6" in error or "character" in error.lower()

    def test_create_user_with_validation_rejects_empty_display_name(self, temp_db_path, mock_logger):
        """Test create_user_with_validation rejects empty display name."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(temp_db_path, mock_logger)

        # Test empty string
        user_id1, error1 = user_service.create_user_with_validation(
            username="valid_user",
            display_name="",
            password="Password1",
            role="researcher"
        )
        assert user_id1 is None
        assert error1 is not None
        assert "display" in error1.lower() or "name" in error1.lower()

        # Test whitespace only
        user_id2, error2 = user_service.create_user_with_validation(
            username="valid_user2",
            display_name="   ",
            password="Password1",
            role="researcher"
        )
        assert user_id2 is None
        assert error2 is not None

    def test_create_user_with_validation_rejects_invalid_role(self, temp_db_path, mock_logger):
        """Test create_user_with_validation rejects invalid role."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(temp_db_path, mock_logger)

        user_id, error = user_service.create_user_with_validation(
            username="valid_user",
            display_name="Valid Name",
            password="Password1",
            role="superuser"  # Invalid role
        )

        assert user_id is None
        assert error is not None
        assert "role" in error.lower()

    def test_create_user_with_validation_accepts_admin_role(self, temp_db_path, mock_logger):
        """Test create_user_with_validation accepts 'admin' role."""
        from ui.services.cap.user_service import CAPUserService

        user_service = CAPUserService(temp_db_path, mock_logger)

        user_id, error = user_service.create_user_with_validation(
            username="admin_user",
            display_name="Admin User",
            password="Password1",
            role="admin"
        )

        assert error is None
        assert user_id is not None

        user = user_service.get_user_by_id(user_id)
        assert user["role"] == "admin"


class TestCAPAPIService:
    """Tests for CAPAPIService error handling.

    These tests verify that API methods return tuple (results, error_message)
    to distinguish between "no results found" and "API error".
    """

    def test_search_bills_returns_tuple_on_success(self, mock_logger):
        """Test that search_bills_by_name returns (results, None) on success."""
        import asyncio
        from unittest.mock import AsyncMock, patch, MagicMock
        from ui.services.cap_api_service import CAPAPIService

        service = CAPAPIService(mock_logger)

        # Mock successful API response
        mock_response = {
            "value": [
                {"BillID": 1, "Name": "Test Bill 1"},
                {"BillID": 2, "Name": "Test Bill 2"},
            ]
        }

        async def run_test():
            with patch.object(service, "_fetch_json", new_callable=AsyncMock) as mock_fetch:
                mock_fetch.return_value = mock_response
                result, error = await service.search_bills_by_name("Test")

                assert error is None, f"Expected no error, got: {error}"
                assert isinstance(result, list)
                assert len(result) == 2
                assert result[0]["BillID"] == 1

        asyncio.run(run_test())

    def test_search_bills_returns_tuple_on_empty_results(self, mock_logger):
        """Test that search_bills_by_name returns ([], None) when no results found."""
        import asyncio
        from unittest.mock import AsyncMock, patch
        from ui.services.cap_api_service import CAPAPIService

        service = CAPAPIService(mock_logger)

        # Mock empty API response (valid response, just no results)
        mock_response = {"value": []}

        async def run_test():
            with patch.object(service, "_fetch_json", new_callable=AsyncMock) as mock_fetch:
                mock_fetch.return_value = mock_response
                result, error = await service.search_bills_by_name("NonexistentBill")

                assert error is None, "Empty results should not be an error"
                assert isinstance(result, list)
                assert len(result) == 0

        asyncio.run(run_test())

    def test_search_bills_returns_error_on_timeout(self, mock_logger):
        """Test that search_bills_by_name returns ([], error) on timeout."""
        import asyncio
        from unittest.mock import AsyncMock, patch
        from ui.services.cap_api_service import CAPAPIService

        service = CAPAPIService(mock_logger)

        async def run_test():
            with patch.object(service, "_fetch_json", new_callable=AsyncMock) as mock_fetch:
                mock_fetch.side_effect = asyncio.TimeoutError()
                result, error = await service.search_bills_by_name("Test")

                assert isinstance(result, list)
                assert len(result) == 0
                assert error is not None
                assert "timeout" in error.lower() or "timed out" in error.lower()
                assert "try again" in error.lower()

        asyncio.run(run_test())

    def test_search_bills_returns_error_on_network_failure(self, mock_logger):
        """Test that search_bills_by_name returns ([], error) on network error."""
        import asyncio
        import aiohttp
        from unittest.mock import AsyncMock, patch
        from ui.services.cap_api_service import CAPAPIService

        service = CAPAPIService(mock_logger)

        async def run_test():
            with patch.object(service, "_fetch_json", new_callable=AsyncMock) as mock_fetch:
                mock_fetch.side_effect = aiohttp.ClientError("Connection refused")
                result, error = await service.search_bills_by_name("Test")

                assert isinstance(result, list)
                assert len(result) == 0
                assert error is not None
                assert "network" in error.lower()

        asyncio.run(run_test())

    def test_search_bills_returns_error_on_unexpected_exception(self, mock_logger):
        """Test that search_bills_by_name returns ([], error) on unexpected error."""
        import asyncio
        from unittest.mock import AsyncMock, patch
        from ui.services.cap_api_service import CAPAPIService

        service = CAPAPIService(mock_logger)

        async def run_test():
            with patch.object(service, "_fetch_json", new_callable=AsyncMock) as mock_fetch:
                mock_fetch.side_effect = ValueError("Unexpected JSON parse error")
                result, error = await service.search_bills_by_name("Test")

                assert isinstance(result, list)
                assert len(result) == 0
                assert error is not None
                assert "unexpected" in error.lower()

        asyncio.run(run_test())

    def test_search_bills_sync_returns_tuple(self, mock_logger):
        """Test that the synchronous wrapper also returns a tuple."""
        import asyncio
        from unittest.mock import AsyncMock, patch
        from ui.services.cap_api_service import CAPAPIService

        service = CAPAPIService(mock_logger)

        # Mock the async method to return expected tuple
        async def mock_search(*args, **kwargs):
            return [{"BillID": 1, "Name": "Test"}], None

        with patch.object(service, "search_bills_by_name", side_effect=mock_search):
            result, error = service.search_bills_by_name_sync("Test")

            assert error is None
            assert len(result) == 1
            assert result[0]["BillID"] == 1

    def test_search_bills_sync_returns_error_tuple(self, mock_logger):
        """Test that sync wrapper returns error tuple on failure."""
        import asyncio
        from unittest.mock import AsyncMock, patch
        from ui.services.cap_api_service import CAPAPIService

        service = CAPAPIService(mock_logger)

        # Mock the async method to return error tuple
        async def mock_search(*args, **kwargs):
            return [], "Network error: Connection refused"

        with patch.object(service, "search_bills_by_name", side_effect=mock_search):
            result, error = service.search_bills_by_name_sync("Test")

            assert len(result) == 0
            assert error is not None
            assert "network" in error.lower()

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

    conn.close()
    return temp_db_path


class TestCAPTaxonomyService:
    """Tests for the CAPTaxonomyService class."""

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
            direction=1,
            assigned_by="test@example.com",
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
            direction=-1,
            assigned_by="researcher@test.com"
        )

        annotation = repo.get_annotation_by_bill_id(1)

        assert annotation is not None
        assert annotation['BillID'] == 1
        assert annotation['CAPMinorCode'] == 101
        assert annotation['Direction'] == -1

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
        repo.save_annotation(bill_id=1, cap_minor_code=101, direction=0, assigned_by="test")

        # Verify annotation exists
        assert repo.get_annotation_by_bill_id(1) is not None

        # Delete annotation
        result = repo.delete_annotation(1)
        assert result is True

        # Verify annotation is gone
        assert repo.get_annotation_by_bill_id(1) is None


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

        # Add annotations
        repo = CAPAnnotationRepository(initialized_db, mock_logger)
        repo.save_annotation(bill_id=1, cap_minor_code=101, direction=1, assigned_by="test")
        repo.save_annotation(bill_id=2, cap_minor_code=101, direction=-1, assigned_by="test")

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

    def test_facade_constants_re_exported(self):
        """Test that direction constants are re-exported on facade."""
        from ui.services.cap_service import CAPAnnotationService

        assert CAPAnnotationService.DIRECTION_STRENGTHENING == 1
        assert CAPAnnotationService.DIRECTION_WEAKENING == -1
        assert CAPAnnotationService.DIRECTION_NEUTRAL == 0

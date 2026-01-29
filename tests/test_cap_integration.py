"""
CAP Annotation Integration Tests

Tests the full annotation workflow from researcher login to annotation persistence.
Validates end-to-end flow including:
- Researcher authentication
- Taxonomy loading and category lookup
- Annotation creation with proper researcher_id
- Multi-annotator visibility and independence
- Annotation persistence and retrieval
"""

import pytest
from pathlib import Path
import tempfile
import duckdb

from ui.services.cap.user_service import CAPUserService
from ui.services.cap.taxonomy import CAPTaxonomyService
from ui.services.cap.repository import CAPAnnotationRepository
from backend.connection_manager import get_db_connection


class TestCAPAnnotationWorkflow:
    """Integration tests for full annotation workflow."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary database for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            yield db_path

    @pytest.fixture
    def setup_system(self, temp_db):
        """Set up complete CAP system with users and taxonomy."""
        user_service = CAPUserService(temp_db)
        taxonomy_service = CAPTaxonomyService(temp_db)
        repository = CAPAnnotationRepository(temp_db)

        # Create tables
        user_service.ensure_table_exists()
        taxonomy_service.ensure_tables_exist()
        taxonomy_service.load_taxonomy_from_csv()

        # Create test users
        user_service.create_user(
            username="researcher_a",
            display_name="Researcher A",
            password="test123",
            role="researcher",
        )
        user_service.create_user(
            username="researcher_b",
            display_name="Researcher B",
            password="test456",
            role="researcher",
        )
        user_service.create_user(
            username="admin_user",
            display_name="Admin User",
            password="admin789",
            role="admin",
        )

        # Create sample bills for testing
        conn = duckdb.connect(str(temp_db))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS KNS_Bill (
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS KNS_Status (
                StatusID INTEGER PRIMARY KEY,
                "Desc" VARCHAR
            )
        """)
        conn.execute("""
            INSERT INTO KNS_Bill VALUES
            (100, 25, 'Test Bill 100', 'Private', 12345, '2024-01-01', '2024-01-15', 100),
            (200, 25, 'Test Bill 200', 'Government', NULL, '2024-02-01', '2024-02-15', 118),
            (300, 24, 'Test Bill 300', 'Private', 12346, '2023-06-01', '2023-06-15', 104)
        """)
        conn.execute("""
            INSERT INTO KNS_Status VALUES
            (100, 'In Progress'),
            (118, 'Passed'),
            (104, 'First Reading')
        """)
        conn.close()

        return {
            "db_path": temp_db,
            "user_service": user_service,
            "taxonomy_service": taxonomy_service,
            "repository": repository,
        }

    def test_full_annotation_workflow(self, setup_system):
        """Test complete workflow: login -> annotate -> verify persistence."""
        db_path = setup_system["db_path"]
        user_service = setup_system["user_service"]
        repository = setup_system["repository"]
        taxonomy_service = setup_system["taxonomy_service"]

        # Step 1: Authenticate researcher A
        user_a = user_service.authenticate("researcher_a", "test123")
        assert user_a is not None, "Researcher A authentication failed"
        assert user_a["username"] == "researcher_a"
        assert user_a["display_name"] == "Researcher A"
        assert user_a["role"] == "researcher"
        researcher_a_id = user_a["id"]

        # Step 2: Get taxonomy for valid CAP code
        major_cats = taxonomy_service.get_major_categories()
        assert len(major_cats) > 0, "No major categories found in taxonomy"
        first_major = major_cats[0]["MajorCode"]
        minor_cats = taxonomy_service.get_minor_categories(first_major)
        assert len(minor_cats) > 0, "No minor categories found for first major"
        valid_minor_code = minor_cats[0]["MinorCode"]

        # Step 3: Researcher A annotates bill 100
        result = repository.save_annotation(
            bill_id=100,
            cap_minor_code=valid_minor_code,
            researcher_id=researcher_a_id,
            confidence="High",
            notes="Test annotation by Researcher A",
            source="Database",
        )
        assert result is True, "Failed to save annotation for Researcher A"

        # Step 4: Verify annotation persisted
        with get_db_connection(db_path, read_only=True) as conn:
            annotation = conn.execute(
                "SELECT * FROM UserBillCAP WHERE BillID = 100 AND ResearcherID = ?",
                [researcher_a_id],
            ).fetchone()
            assert annotation is not None, "Annotation not found in database"

        # Step 5: Researcher B authenticates
        user_b = user_service.authenticate("researcher_b", "test456")
        assert user_b is not None, "Researcher B authentication failed"
        researcher_b_id = user_b["id"]

        # Step 6: Researcher B also annotates bill 100 (multi-annotator)
        result = repository.save_annotation(
            bill_id=100,
            cap_minor_code=valid_minor_code,
            researcher_id=researcher_b_id,
            confidence="Medium",
            notes="Test annotation by Researcher B",
            source="Database",
        )
        assert result is True, "Failed to save annotation for Researcher B"

        # Step 7: Verify both annotations exist
        all_annotations = repository.get_all_annotations_for_bill(100)
        assert len(all_annotations) == 2, f"Expected 2 annotations, got {len(all_annotations)}"

        # Step 8: Verify annotations are distinct per researcher
        a_annotation = all_annotations[all_annotations["ResearcherID"] == researcher_a_id]
        b_annotation = all_annotations[all_annotations["ResearcherID"] == researcher_b_id]
        assert len(a_annotation) == 1, "Expected 1 annotation from Researcher A"
        assert len(b_annotation) == 1, "Expected 1 annotation from Researcher B"

    def test_annotation_visible_to_other_researchers(self, setup_system):
        """Test that one researcher's annotations are visible to others."""
        repository = setup_system["repository"]
        user_service = setup_system["user_service"]
        taxonomy_service = setup_system["taxonomy_service"]

        # Get valid CAP code
        major_cats = taxonomy_service.get_major_categories()
        first_major = major_cats[0]["MajorCode"]
        minor_cats = taxonomy_service.get_minor_categories(first_major)
        valid_minor_code = minor_cats[0]["MinorCode"]

        # Researcher A annotates bill 200
        user_a = user_service.authenticate("researcher_a", "test123")
        repository.save_annotation(
            bill_id=200,
            cap_minor_code=valid_minor_code,
            researcher_id=user_a["id"],
            confidence="Low",
            notes="Annotation visible to other researchers",
            source="Database",
        )

        # Researcher B should see A's annotation via get_all_annotations_for_bill
        all_annotations = repository.get_all_annotations_for_bill(200)

        # B sees A's annotation
        assert len(all_annotations) == 1
        assert all_annotations.iloc[0]["ResearcherID"] == user_a["id"]
        assert all_annotations.iloc[0]["Notes"] == "Annotation visible to other researchers"

    def test_uncoded_bills_queue_per_researcher(self, setup_system):
        """Test that each researcher has their own independent uncoded bills queue."""
        repository = setup_system["repository"]
        user_service = setup_system["user_service"]
        taxonomy_service = setup_system["taxonomy_service"]

        # Get valid CAP code
        major_cats = taxonomy_service.get_major_categories()
        first_major = major_cats[0]["MajorCode"]
        minor_cats = taxonomy_service.get_minor_categories(first_major)
        valid_minor_code = minor_cats[0]["MinorCode"]

        user_a = user_service.authenticate("researcher_a", "test123")
        user_b = user_service.authenticate("researcher_b", "test456")

        # Initially both researchers see all 3 bills as uncoded
        uncoded_a_before = repository.get_uncoded_bills(researcher_id=user_a["id"])
        uncoded_b_before = repository.get_uncoded_bills(researcher_id=user_b["id"])
        assert len(uncoded_a_before) == 3, "Researcher A should see 3 uncoded bills initially"
        assert len(uncoded_b_before) == 3, "Researcher B should see 3 uncoded bills initially"

        # Researcher A annotates bill 100
        repository.save_annotation(
            bill_id=100,
            cap_minor_code=valid_minor_code,
            researcher_id=user_a["id"],
            source="Database",
        )

        # Now A sees 2 uncoded bills (100 removed from their queue)
        uncoded_a_after = repository.get_uncoded_bills(researcher_id=user_a["id"])
        assert len(uncoded_a_after) == 2, "Researcher A should see 2 uncoded bills after annotation"
        assert 100 not in uncoded_a_after["BillID"].tolist(), "Bill 100 should be removed from A's queue"

        # B still sees 3 uncoded bills (100 is still in their queue)
        uncoded_b_after = repository.get_uncoded_bills(researcher_id=user_b["id"])
        assert len(uncoded_b_after) == 3, "Researcher B should still see 3 uncoded bills"
        assert 100 in uncoded_b_after["BillID"].tolist(), "Bill 100 should remain in B's queue"

    def test_annotation_upsert_updates_not_duplicates(self, setup_system):
        """Test that re-annotating a bill updates rather than creates duplicate."""
        repository = setup_system["repository"]
        user_service = setup_system["user_service"]
        taxonomy_service = setup_system["taxonomy_service"]

        # Get valid CAP codes
        major_cats = taxonomy_service.get_major_categories()
        first_major = major_cats[0]["MajorCode"]
        minor_cats = taxonomy_service.get_minor_categories(first_major)
        code_1 = minor_cats[0]["MinorCode"]
        code_2 = minor_cats[1]["MinorCode"] if len(minor_cats) > 1 else code_1

        user_a = user_service.authenticate("researcher_a", "test123")

        # First annotation
        repository.save_annotation(
            bill_id=300,
            cap_minor_code=code_1,
            researcher_id=user_a["id"],
            notes="Original annotation",
            source="Database",
        )

        # Re-annotate with different values
        repository.save_annotation(
            bill_id=300,
            cap_minor_code=code_2,
            researcher_id=user_a["id"],
            notes="Updated annotation",
            source="Database",
        )

        # Should have only 1 annotation (not 2)
        all_annotations = repository.get_all_annotations_for_bill(300)
        assert len(all_annotations) == 1, "Should have 1 annotation, not duplicates"
        assert all_annotations.iloc[0]["CAPMinorCode"] == code_2, "Should have updated code"
        assert all_annotations.iloc[0]["Notes"] == "Updated annotation", "Should have updated notes"

    def test_authentication_failure_with_wrong_password(self, setup_system):
        """Test that authentication fails with wrong password."""
        user_service = setup_system["user_service"]

        # Try to authenticate with wrong password
        result = user_service.authenticate("researcher_a", "wrong_password")
        assert result is None, "Authentication should fail with wrong password"

    def test_authentication_failure_for_nonexistent_user(self, setup_system):
        """Test that authentication fails for non-existent user."""
        user_service = setup_system["user_service"]

        # Try to authenticate non-existent user
        result = user_service.authenticate("nonexistent_user", "any_password")
        assert result is None, "Authentication should fail for non-existent user"

    def test_admin_role_authentication(self, setup_system):
        """Test that admin user authenticates with admin role."""
        user_service = setup_system["user_service"]

        admin = user_service.authenticate("admin_user", "admin789")
        assert admin is not None, "Admin authentication failed"
        assert admin["role"] == "admin", "Admin should have admin role"
        assert admin["display_name"] == "Admin User"

    def test_get_annotation_by_bill_id_with_researcher_filter(self, setup_system):
        """Test retrieving specific researcher's annotation for a bill."""
        repository = setup_system["repository"]
        user_service = setup_system["user_service"]
        taxonomy_service = setup_system["taxonomy_service"]

        # Get valid CAP code
        major_cats = taxonomy_service.get_major_categories()
        first_major = major_cats[0]["MajorCode"]
        minor_cats = taxonomy_service.get_minor_categories(first_major)
        valid_minor_code = minor_cats[0]["MinorCode"]

        user_a = user_service.authenticate("researcher_a", "test123")
        user_b = user_service.authenticate("researcher_b", "test456")

        # Both researchers annotate bill 100
        repository.save_annotation(
            bill_id=100,
            cap_minor_code=valid_minor_code,
            researcher_id=user_a["id"],
            notes="A's annotation",
            source="Database",
        )
        repository.save_annotation(
            bill_id=100,
            cap_minor_code=valid_minor_code,
            researcher_id=user_b["id"],
            notes="B's annotation",
            source="Database",
        )

        # Get A's annotation specifically
        a_annotation = repository.get_annotation_by_bill_id(100, researcher_id=user_a["id"])
        assert a_annotation is not None
        assert a_annotation["Notes"] == "A's annotation"

        # Get B's annotation specifically
        b_annotation = repository.get_annotation_by_bill_id(100, researcher_id=user_b["id"])
        assert b_annotation is not None
        assert b_annotation["Notes"] == "B's annotation"

    def test_delete_annotation_per_researcher(self, setup_system):
        """Test deleting annotation for specific researcher only."""
        repository = setup_system["repository"]
        user_service = setup_system["user_service"]
        taxonomy_service = setup_system["taxonomy_service"]

        # Get valid CAP code
        major_cats = taxonomy_service.get_major_categories()
        first_major = major_cats[0]["MajorCode"]
        minor_cats = taxonomy_service.get_minor_categories(first_major)
        valid_minor_code = minor_cats[0]["MinorCode"]

        user_a = user_service.authenticate("researcher_a", "test123")
        user_b = user_service.authenticate("researcher_b", "test456")

        # Both researchers annotate bill 100
        repository.save_annotation(
            bill_id=100,
            cap_minor_code=valid_minor_code,
            researcher_id=user_a["id"],
            source="Database",
        )
        repository.save_annotation(
            bill_id=100,
            cap_minor_code=valid_minor_code,
            researcher_id=user_b["id"],
            source="Database",
        )

        # Delete A's annotation only
        result = repository.delete_annotation(100, researcher_id=user_a["id"])
        assert result is True

        # A's annotation is gone
        a_annotation = repository.get_annotation_by_bill_id(100, researcher_id=user_a["id"])
        assert a_annotation is None, "A's annotation should be deleted"

        # B's annotation still exists
        b_annotation = repository.get_annotation_by_bill_id(100, researcher_id=user_b["id"])
        assert b_annotation is not None, "B's annotation should still exist"

    def test_coded_bills_list_with_annotation_count(self, setup_system):
        """Test that coded bills include annotation count badge info."""
        repository = setup_system["repository"]
        user_service = setup_system["user_service"]
        taxonomy_service = setup_system["taxonomy_service"]

        # Get valid CAP code
        major_cats = taxonomy_service.get_major_categories()
        first_major = major_cats[0]["MajorCode"]
        minor_cats = taxonomy_service.get_minor_categories(first_major)
        valid_minor_code = minor_cats[0]["MinorCode"]

        user_a = user_service.authenticate("researcher_a", "test123")
        user_b = user_service.authenticate("researcher_b", "test456")

        # Both researchers annotate bill 100
        repository.save_annotation(
            bill_id=100,
            cap_minor_code=valid_minor_code,
            researcher_id=user_a["id"],
            source="Database",
        )
        repository.save_annotation(
            bill_id=100,
            cap_minor_code=valid_minor_code,
            researcher_id=user_b["id"],
            source="Database",
        )

        # Get coded bills for A
        coded_bills_a = repository.get_coded_bills(researcher_id=user_a["id"])
        assert len(coded_bills_a) == 1
        # Should have AnnotationCount column showing 2 annotations total
        assert "AnnotationCount" in coded_bills_a.columns
        assert coded_bills_a.iloc[0]["AnnotationCount"] == 2

    def test_password_hashing_and_verification(self, setup_system):
        """Test that passwords are properly hashed and verified."""
        user_service = setup_system["user_service"]

        # Verify password is not stored in plain text
        with get_db_connection(setup_system["db_path"], read_only=True) as conn:
            result = conn.execute(
                "SELECT PasswordHash FROM UserResearchers WHERE Username = 'researcher_a'"
            ).fetchone()
            password_hash = result[0]

            # Password hash should not equal the plain password
            assert password_hash != "test123", "Password should be hashed, not plain text"
            # Password hash should start with $2b$ (bcrypt marker)
            assert password_hash.startswith("$2"), "Password should use bcrypt hashing"

    def test_inactive_user_cannot_authenticate(self, setup_system):
        """Test that deactivated users cannot log in."""
        user_service = setup_system["user_service"]

        # Get researcher_a's ID
        user_a = user_service.authenticate("researcher_a", "test123")
        assert user_a is not None
        researcher_a_id = user_a["id"]

        # Deactivate the user
        result = user_service.delete_user(researcher_a_id)  # Soft delete
        assert result is True

        # User can no longer authenticate
        result = user_service.authenticate("researcher_a", "test123")
        assert result is None, "Inactive user should not be able to authenticate"

    def test_last_login_updated_on_authentication(self, setup_system):
        """Test that last login timestamp is updated on successful authentication."""
        user_service = setup_system["user_service"]

        # Check initial last login (should be None)
        user_before = user_service.get_user_by_id(1)  # First user created
        # Note: LastLoginAt might be None for newly created users

        # Authenticate
        user_a = user_service.authenticate("researcher_a", "test123")
        assert user_a is not None

        # Check that last login was updated
        with get_db_connection(setup_system["db_path"], read_only=True) as conn:
            result = conn.execute(
                "SELECT LastLoginAt FROM UserResearchers WHERE Username = 'researcher_a'"
            ).fetchone()
            assert result[0] is not None, "LastLoginAt should be updated after login"

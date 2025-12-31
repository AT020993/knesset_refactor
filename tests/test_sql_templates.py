"""
Tests for SQL template validation.

These tests verify the SQL templates in sql_templates.py are syntactically correct
and can be executed against a database without errors.
"""
import pytest
import duckdb


@pytest.fixture
def in_memory_db():
    """Create an in-memory database with sample schema for SQL validation."""
    conn = duckdb.connect(':memory:')

    # Create minimal tables needed for template validation
    conn.execute("""
        CREATE TABLE KNS_PersonToPosition (
            PersonID INTEGER,
            KnessetNum INTEGER,
            FactionID INTEGER,
            StartDate TIMESTAMP,
            FinishDate TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE KNS_Bill (
            BillID INTEGER PRIMARY KEY,
            KnessetNum INTEGER,
            StatusID INTEGER,
            PublicationDate TIMESTAMP,
            LastUpdatedDate TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE KNS_BillInitiator (
            BillID INTEGER,
            PersonID INTEGER,
            Ordinal INTEGER,
            LastUpdatedDate TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE KNS_CmtSessionItem (
            ItemID INTEGER,
            CommitteeSessionID INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE KNS_CommitteeSession (
            CommitteeSessionID INTEGER PRIMARY KEY,
            StartDate TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE KNS_PlmSessionItem (
            ItemID INTEGER,
            PlenumSessionID INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE KNS_PlenumSession (
            PlenumSessionID INTEGER PRIMARY KEY,
            StartDate TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE KNS_Query (
            QueryID INTEGER PRIMARY KEY,
            StatusID INTEGER,
            ReplyMinisterID INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE KNS_Status (
            StatusID INTEGER PRIMARY KEY,
            "Desc" VARCHAR
        )
    """)

    # Insert sample data
    conn.execute("""
        INSERT INTO KNS_PersonToPosition VALUES
        (1, 25, 100, '2022-01-01', '2024-12-31'),
        (1, 25, 101, '2022-06-01', NULL),
        (2, 24, 100, '2018-01-01', '2022-12-31')
    """)

    conn.execute("""
        INSERT INTO KNS_Bill VALUES
        (1, 25, 118, '2024-01-01', '2024-06-01'),
        (2, 25, 104, '2024-02-01', '2024-07-01'),
        (3, 25, 100, '2024-03-01', '2024-08-01')
    """)

    conn.execute("""
        INSERT INTO KNS_BillInitiator VALUES
        (1, 1, 1, '2024-01-01'),
        (2, 2, 1, '2024-02-01')
    """)

    # Insert sample status data for query status tests
    conn.execute("""
        INSERT INTO KNS_Status VALUES
        (131, 'התקבלה תשובה'),
        (132, 'נדחתה'),
        (133, 'הוסרה'),
        (134, 'נקבע תאריך תשובה')
    """)

    yield conn
    conn.close()


class TestSQLTemplatesSyntax:
    """Tests for SQL template syntax validation."""

    def test_standard_faction_lookup_is_valid_sql(self, in_memory_db):
        """Test that STANDARD_FACTION_LOOKUP produces valid SQL."""
        from ui.queries.sql_templates import SQLTemplates

        query = f"""
        WITH {SQLTemplates.STANDARD_FACTION_LOOKUP}
        SELECT * FROM StandardFactionLookup LIMIT 1
        """

        # Should not raise an exception
        result = in_memory_db.execute(query).fetchdf()
        assert result is not None

    def test_bill_first_submission_is_valid_sql(self, in_memory_db):
        """Test that BILL_FIRST_SUBMISSION produces valid SQL."""
        from ui.queries.sql_templates import SQLTemplates

        query = f"""
        WITH {SQLTemplates.BILL_FIRST_SUBMISSION}
        SELECT * FROM BillFirstSubmission LIMIT 1
        """

        # Should not raise an exception
        result = in_memory_db.execute(query).fetchdf()
        assert result is not None

    def test_bill_status_case_he_is_valid_sql(self, in_memory_db):
        """Test that BILL_STATUS_CASE_HE produces valid SQL."""
        from ui.queries.sql_templates import SQLTemplates

        query = f"""
        SELECT
            b.BillID,
            {SQLTemplates.BILL_STATUS_CASE_HE} AS Status
        FROM KNS_Bill b
        LIMIT 1
        """

        result = in_memory_db.execute(query).fetchdf()
        assert result is not None

    def test_bill_status_case_en_is_valid_sql(self, in_memory_db):
        """Test that BILL_STATUS_CASE_EN produces valid SQL."""
        from ui.queries.sql_templates import SQLTemplates

        query = f"""
        SELECT
            b.BillID,
            {SQLTemplates.BILL_STATUS_CASE_EN} AS Status
        FROM KNS_Bill b
        LIMIT 1
        """

        result = in_memory_db.execute(query).fetchdf()
        assert result is not None

    def test_query_status_case_is_valid_sql(self, in_memory_db):
        """Test that QUERY_STATUS_CASE produces valid SQL."""
        from ui.queries.sql_templates import SQLTemplates

        # Insert a sample query (StatusID 131 = 'התקבלה תשובה' which is 'Answered')
        in_memory_db.execute("""
            INSERT INTO KNS_Query VALUES (1, 131, 10)
        """)

        # Note: QUERY_STATUS_CASE references S."Desc" so we must JOIN KNS_Status as S
        query = f"""
        SELECT
            q.QueryID,
            {SQLTemplates.QUERY_STATUS_CASE} AS AnswerStatus
        FROM KNS_Query q
        LEFT JOIN KNS_Status S ON q.StatusID = S.StatusID
        LIMIT 1
        """

        result = in_memory_db.execute(query).fetchdf()
        assert result is not None


class TestSQLTemplatesExecution:
    """Tests for SQL template execution correctness."""

    def test_faction_lookup_returns_correct_columns(self, in_memory_db):
        """Test that faction lookup CTE returns expected columns."""
        from ui.queries.sql_templates import SQLTemplates

        query = f"""
        WITH {SQLTemplates.STANDARD_FACTION_LOOKUP}
        SELECT * FROM StandardFactionLookup LIMIT 5
        """

        result = in_memory_db.execute(query).fetchdf()

        # Verify expected columns exist
        expected_columns = {'PersonID', 'KnessetNum', 'FactionID', 'rn'}
        assert expected_columns.issubset(set(result.columns))

    def test_faction_lookup_deduplicates_correctly(self, in_memory_db):
        """Test that faction lookup uses ROW_NUMBER for deduplication."""
        from ui.queries.sql_templates import SQLTemplates

        query = f"""
        WITH {SQLTemplates.STANDARD_FACTION_LOOKUP}
        SELECT PersonID, KnessetNum, COUNT(*) as count
        FROM StandardFactionLookup
        WHERE rn = 1
        GROUP BY PersonID, KnessetNum
        HAVING COUNT(*) > 1
        """

        result = in_memory_db.execute(query).fetchdf()
        # With rn=1 filter, each PersonID/KnessetNum should appear once
        assert len(result) == 0

    def test_bill_first_submission_returns_correct_columns(self, in_memory_db):
        """Test that bill first submission CTE returns expected columns."""
        from ui.queries.sql_templates import SQLTemplates

        query = f"""
        WITH {SQLTemplates.BILL_FIRST_SUBMISSION}
        SELECT * FROM BillFirstSubmission LIMIT 5
        """

        result = in_memory_db.execute(query).fetchdf()

        # Verify expected columns exist
        expected_columns = {'BillID', 'FirstSubmissionDate'}
        assert expected_columns.issubset(set(result.columns))

    def test_bill_status_categorizes_correctly(self, in_memory_db):
        """Test that bill status CASE statement categorizes correctly."""
        from ui.queries.sql_templates import SQLTemplates

        query = f"""
        SELECT
            b.BillID,
            b.StatusID,
            {SQLTemplates.BILL_STATUS_CASE_EN} AS Status
        FROM KNS_Bill b
        """

        result = in_memory_db.execute(query).fetchdf()

        # Bill with StatusID 118 should be "Passed"
        passed_bill = result[result['StatusID'] == 118]
        assert len(passed_bill) == 1
        assert passed_bill.iloc[0]['Status'] == 'Passed'

        # Bill with StatusID 104 should be "First Reading"
        first_reading = result[result['StatusID'] == 104]
        assert len(first_reading) == 1
        assert first_reading.iloc[0]['Status'] == 'First Reading'

        # Bill with StatusID 100 should be "Stopped"
        stopped = result[result['StatusID'] == 100]
        assert len(stopped) == 1
        assert stopped.iloc[0]['Status'] == 'Stopped'


class TestSQLTemplatesCombined:
    """Tests for combining multiple SQL templates in a single query."""

    def test_faction_and_bill_submission_combine(self, in_memory_db):
        """Test that faction lookup and bill submission CTEs can be combined."""
        from ui.queries.sql_templates import SQLTemplates

        query = f"""
        WITH {SQLTemplates.STANDARD_FACTION_LOOKUP},
        {SQLTemplates.BILL_FIRST_SUBMISSION}
        SELECT
            bfs.BillID,
            bfs.FirstSubmissionDate,
            sfl.FactionID
        FROM BillFirstSubmission bfs
        LEFT JOIN KNS_BillInitiator bi ON bfs.BillID = bi.BillID AND bi.Ordinal = 1
        LEFT JOIN KNS_Bill b ON bfs.BillID = b.BillID
        LEFT JOIN StandardFactionLookup sfl ON bi.PersonID = sfl.PersonID
            AND b.KnessetNum = sfl.KnessetNum
            AND sfl.rn = 1
        LIMIT 5
        """

        # Should not raise an exception
        result = in_memory_db.execute(query).fetchdf()
        assert result is not None


class TestSQLTemplatesClass:
    """Tests for the SQLTemplates class structure."""

    def test_class_has_expected_attributes(self):
        """Test that SQLTemplates has all expected template attributes."""
        from ui.queries.sql_templates import SQLTemplates

        expected_templates = [
            'STANDARD_FACTION_LOOKUP',
            'BILL_FIRST_SUBMISSION',
            'BILL_STATUS_CASE_HE',
            'BILL_STATUS_CASE_EN',
            'QUERY_STATUS_CASE',
        ]

        for template_name in expected_templates:
            assert hasattr(SQLTemplates, template_name), f"Missing template: {template_name}"
            template = getattr(SQLTemplates, template_name)
            assert isinstance(template, str), f"{template_name} should be a string"
            assert len(template) > 0, f"{template_name} should not be empty"

    def test_get_standard_faction_lookup_method(self):
        """Test the get_standard_faction_lookup static method."""
        from ui.queries.sql_templates import SQLTemplates

        # Default alias
        result = SQLTemplates.get_standard_faction_lookup()
        assert 'StandardFactionLookup' in result
        assert 'ptp.' in result

        # Custom alias
        result_custom = SQLTemplates.get_standard_faction_lookup('custom_alias')
        assert 'StandardFactionLookup' in result_custom
        assert 'custom_alias.' in result_custom

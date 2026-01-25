"""
Tests for CAP renderer components.

These tests verify the CAP renderer components work correctly:
- CAPBillQueueRenderer: Bill queue display and formatting
- CAPPDFViewer: PDF document embedding
- CAPCategorySelector: Category selection UI
"""

import pytest
from unittest import mock
import pandas as pd


class TestCAPBillQueueRenderer:
    """Tests for CAPBillQueueRenderer."""

    def test_format_bill_option_coded(self):
        """Test formatting a coded bill option."""
        from ui.renderers.cap.bill_queue_renderer import CAPBillQueueRenderer

        mock_service = mock.MagicMock()
        renderer = CAPBillQueueRenderer(mock_service)

        bill = pd.Series({
            "BillID": 123,
            "BillName": "Test Bill",
            "IsCoded": 1,
            "MinorCode": 101,
            "AnnotationCount": 2,
        })

        result = renderer._format_bill_option(bill)

        assert "✅" in result
        assert "123" in result
        assert "[101]" in result
        assert "👥2" in result

    def test_format_bill_option_uncoded(self):
        """Test formatting an uncoded bill option."""
        from ui.renderers.cap.bill_queue_renderer import CAPBillQueueRenderer

        mock_service = mock.MagicMock()
        renderer = CAPBillQueueRenderer(mock_service)

        bill = pd.Series({
            "BillID": 456,
            "BillName": "Another Bill",
            "IsCoded": 0,
            "AnnotationCount": 0,
        })

        result = renderer._format_bill_option(bill)

        assert "⭕" in result
        assert "456" in result
        assert "👥" not in result

    def test_format_bill_option_with_multiple_annotations(self):
        """Test formatting shows annotation count badge."""
        from ui.renderers.cap.bill_queue_renderer import CAPBillQueueRenderer

        mock_service = mock.MagicMock()
        renderer = CAPBillQueueRenderer(mock_service)

        bill = pd.Series({
            "BillID": 789,
            "BillName": "Multi-Annotated Bill",
            "IsCoded": 0,
            "AnnotationCount": 3,
        })

        result = renderer._format_bill_option(bill)

        assert "👥3" in result

    def test_format_bill_option_truncates_long_names(self):
        """Test that long bill names are truncated."""
        from ui.renderers.cap.bill_queue_renderer import CAPBillQueueRenderer

        mock_service = mock.MagicMock()
        renderer = CAPBillQueueRenderer(mock_service)

        long_name = "A" * 100  # 100 character name
        bill = pd.Series({
            "BillID": 123,
            "BillName": long_name,
            "IsCoded": 0,
            "AnnotationCount": 0,
        })

        result = renderer._format_bill_option(bill)

        # Should be truncated to 60 chars + "..."
        assert "..." in result
        assert len(result) < len(long_name) + 20  # Some overhead for emojis/IDs

    def test_format_bill_option_handles_none_name(self):
        """Test formatting handles None bill name gracefully."""
        from ui.renderers.cap.bill_queue_renderer import CAPBillQueueRenderer

        mock_service = mock.MagicMock()
        renderer = CAPBillQueueRenderer(mock_service)

        bill = pd.Series({
            "BillID": 123,
            "BillName": None,
            "IsCoded": 0,
            "AnnotationCount": 0,
        })

        result = renderer._format_bill_option(bill)

        assert "Unknown" in result
        assert "123" in result


class TestCAPPDFViewer:
    """Tests for CAPPDFViewer."""

    def test_doc_type_labels_mapping(self):
        """Test document type labels are properly defined."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer

        labels = CAPPDFViewer.DOC_TYPE_LABELS

        assert "חוק - פרסום ברשומות" in labels
        assert "📜" in labels["חוק - פרסום ברשומות"]
        assert "הצעת חוק לקריאה הראשונה" in labels
        assert "📋" in labels["הצעת חוק לקריאה הראשונה"]

    def test_doc_type_labels_has_all_types(self):
        """Test all expected document types have labels."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer

        labels = CAPPDFViewer.DOC_TYPE_LABELS

        expected_types = [
            "חוק - פרסום ברשומות",
            "הצעת חוק לקריאה הראשונה",
            "הצעת חוק לקריאה השנייה והשלישית",
            "הצעת חוק לדיון מוקדם",
        ]

        for doc_type in expected_types:
            assert doc_type in labels, f"Missing label for: {doc_type}"

    def test_get_doc_type_label_known_type(self):
        """Test get_doc_type_label returns correct label for known type."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer

        mock_service = mock.MagicMock()
        viewer = CAPPDFViewer(mock_service)

        result = viewer.get_doc_type_label("חוק - פרסום ברשומות")
        assert result == "📜 Published Law"

    def test_get_doc_type_label_unknown_type(self):
        """Test get_doc_type_label returns fallback for unknown type."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer

        mock_service = mock.MagicMock()
        viewer = CAPPDFViewer(mock_service)

        result = viewer.get_doc_type_label("Unknown Document Type")
        assert result == "📄 Unknown Document Type"

    def test_render_bill_documents_handles_empty(self):
        """Test render_bill_documents handles empty document list."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer

        mock_service = mock.MagicMock()
        mock_service.get_bill_documents.return_value = pd.DataFrame()

        viewer = CAPPDFViewer(mock_service)
        # Should not raise an error
        viewer.render_bill_documents(bill_id=123)


class TestCAPCategorySelector:
    """Tests for CAPCategorySelector."""

    def test_clear_session_state(self):
        """Test that session state is properly cleared."""
        from ui.renderers.cap.category_selector import CAPCategorySelector
        import streamlit as st

        mock_service = mock.MagicMock()
        selector = CAPCategorySelector(mock_service)

        # Setup mock session state
        st.session_state["db_cap_selected_major"] = 1
        st.session_state["db_cap_selected_minor"] = 101
        st.session_state["db_cap_selected_minor_label"] = "Test"

        selector.clear_session_state("db_")

        assert "db_cap_selected_major" not in st.session_state
        assert "db_cap_selected_minor" not in st.session_state
        assert "db_cap_selected_minor_label" not in st.session_state

    def test_init_session_state(self):
        """Test that session state is properly initialized."""
        from ui.renderers.cap.category_selector import CAPCategorySelector
        import streamlit as st

        mock_service = mock.MagicMock()
        selector = CAPCategorySelector(mock_service)

        # Clear any existing state
        for key in ["test_cap_selected_major", "test_cap_selected_minor", "test_cap_selected_minor_label"]:
            if key in st.session_state:
                del st.session_state[key]

        selector.init_session_state("test_")

        assert "test_cap_selected_major" in st.session_state
        assert "test_cap_selected_minor" in st.session_state
        assert "test_cap_selected_minor_label" in st.session_state
        assert st.session_state["test_cap_selected_major"] is None

    def test_on_major_category_change_clears_minor(self):
        """Test that changing major category clears minor selection."""
        from ui.renderers.cap.category_selector import CAPCategorySelector
        import streamlit as st

        mock_service = mock.MagicMock()
        selector = CAPCategorySelector(mock_service)

        # Setup with existing minor selection
        st.session_state["prefix_cap_selected_minor"] = 101
        st.session_state["prefix_cap_selected_minor_label"] = "Test Minor"

        selector._on_major_category_change("prefix_")

        assert st.session_state["prefix_cap_selected_minor"] is None
        assert st.session_state["prefix_cap_selected_minor_label"] is None

    def test_show_category_description_with_description(self):
        """Test showing category description."""
        from ui.renderers.cap.category_selector import CAPCategorySelector

        mock_service = mock.MagicMock()
        mock_service.get_minor_categories.return_value = [
            {
                "MinorCode": 101,
                "MinorTopic_HE": "Test Topic",
                "Description_HE": "Test Description",
                "Examples_HE": "Example 1, Example 2",
            }
        ]

        selector = CAPCategorySelector(mock_service)
        # Should not raise - just exercises the code path
        selector.show_category_description(101)

    def test_show_category_description_no_description(self):
        """Test showing category when no description exists."""
        from ui.renderers.cap.category_selector import CAPCategorySelector

        mock_service = mock.MagicMock()
        mock_service.get_minor_categories.return_value = [
            {
                "MinorCode": 101,
                "MinorTopic_HE": "Test Topic",
                "Description_HE": None,
                "Examples_HE": None,
            }
        ]

        selector = CAPCategorySelector(mock_service)
        # Should not raise - gracefully handles missing description
        selector.show_category_description(101)


class TestCAPAuthHandler:
    """Tests for CAPAuthHandler session timeout functionality."""

    def test_session_timeout_check_valid_session(self):
        """Test that a recent session (< 2 hours) returns True."""
        from ui.renderers.cap.auth_handler import CAPAuthHandler
        from datetime import datetime, timedelta
        import streamlit as st

        # Setup: authenticated with recent login time
        st.session_state.cap_authenticated = True
        st.session_state.cap_login_time = datetime.now() - timedelta(minutes=30)

        result = CAPAuthHandler.is_session_valid()

        assert result is True

        # Cleanup
        st.session_state.pop("cap_authenticated", None)
        st.session_state.pop("cap_login_time", None)

    def test_session_timeout_check_expired_session(self):
        """Test that an old session (> 2 hours) returns False."""
        from ui.renderers.cap.auth_handler import CAPAuthHandler
        from datetime import datetime, timedelta
        import streamlit as st

        # Setup: authenticated but login was 3 hours ago
        st.session_state.cap_authenticated = True
        st.session_state.cap_login_time = datetime.now() - timedelta(hours=3)

        result = CAPAuthHandler.is_session_valid()

        assert result is False

        # Cleanup
        st.session_state.pop("cap_authenticated", None)
        st.session_state.pop("cap_login_time", None)

    def test_session_timeout_missing_login_time(self):
        """Test that missing login time returns False."""
        from ui.renderers.cap.auth_handler import CAPAuthHandler
        import streamlit as st

        # Setup: authenticated but no login time recorded
        st.session_state.cap_authenticated = True
        if "cap_login_time" in st.session_state:
            del st.session_state["cap_login_time"]

        result = CAPAuthHandler.is_session_valid()

        assert result is False

        # Cleanup
        st.session_state.pop("cap_authenticated", None)

    def test_session_timeout_not_authenticated(self):
        """Test that unauthenticated session returns False."""
        from ui.renderers.cap.auth_handler import CAPAuthHandler
        from datetime import datetime
        import streamlit as st

        # Setup: not authenticated
        st.session_state.cap_authenticated = False
        st.session_state.cap_login_time = datetime.now()

        result = CAPAuthHandler.is_session_valid()

        assert result is False

        # Cleanup
        st.session_state.pop("cap_authenticated", None)
        st.session_state.pop("cap_login_time", None)

    def test_clear_session_clears_all_keys(self):
        """Test that _clear_session clears all CAP session keys."""
        from ui.renderers.cap.auth_handler import CAPAuthHandler
        from datetime import datetime
        import streamlit as st

        # Setup: populate all session keys
        st.session_state.cap_authenticated = True
        st.session_state.cap_user_id = 42
        st.session_state.cap_user_role = "admin"
        st.session_state.cap_researcher_name = "Test User"
        st.session_state.cap_username = "testuser"
        st.session_state.cap_login_time = datetime.now()

        CAPAuthHandler._clear_session()

        # All keys should be cleared or set to default values
        assert st.session_state.get("cap_authenticated") is False
        assert st.session_state.get("cap_user_id") is None
        assert st.session_state.get("cap_user_role") == ""
        assert st.session_state.get("cap_researcher_name") == ""
        assert st.session_state.get("cap_username") == ""
        assert st.session_state.get("cap_login_time") is None

    def test_check_authentication_clears_expired_session(self):
        """Test that check_authentication clears an expired session."""
        from ui.renderers.cap.auth_handler import CAPAuthHandler
        from datetime import datetime, timedelta
        import streamlit as st

        # Setup: authenticated but expired (3 hours old)
        st.session_state.cap_authenticated = True
        st.session_state.cap_user_id = 42
        st.session_state.cap_researcher_name = "Test User"
        st.session_state.cap_login_time = datetime.now() - timedelta(hours=3)

        # Mock _get_cap_secrets to return enabled=True
        with mock.patch("ui.renderers.cap.auth_handler._get_cap_secrets") as mock_secrets:
            mock_secrets.return_value = {"enabled": True}

            is_auth, name = CAPAuthHandler.check_authentication()

        # Should return not authenticated
        assert is_auth is False
        assert name == ""

        # Session should be cleared
        assert st.session_state.get("cap_authenticated") is False
        assert st.session_state.get("cap_user_id") is None

    def test_session_timeout_constant_exists(self):
        """Test that SESSION_TIMEOUT_HOURS constant is defined."""
        from ui.renderers.cap.auth_handler import SESSION_TIMEOUT_HOURS

        assert SESSION_TIMEOUT_HOURS == 2

    def test_check_authentication_validates_user_still_active(self):
        """Test that check_authentication logs out a deactivated user.

        When an admin deactivates a user, that user should be logged out
        on their next request (not continue working indefinitely).
        """
        from ui.renderers.cap.auth_handler import CAPAuthHandler
        from datetime import datetime, timedelta
        import streamlit as st

        # Setup: authenticated user with valid (non-expired) session
        st.session_state.cap_authenticated = True
        st.session_state.cap_user_id = 42
        st.session_state.cap_researcher_name = "Test User"
        st.session_state.cap_username = "testuser"
        st.session_state.cap_user_role = "researcher"
        st.session_state.cap_login_time = datetime.now() - timedelta(minutes=30)

        # Create mock user service that says user is NOT active
        mock_user_service = mock.MagicMock()
        mock_user_service.is_user_active.return_value = False

        # Mock _get_cap_secrets to return enabled=True
        with mock.patch("ui.renderers.cap.auth_handler._get_cap_secrets") as mock_secrets:
            mock_secrets.return_value = {"enabled": True}

            # Call check_authentication with user_service parameter
            is_auth, name = CAPAuthHandler.check_authentication(user_service=mock_user_service)

        # Should return not authenticated because user was deactivated
        assert is_auth is False
        assert name == ""

        # User service should have been called to check active status
        mock_user_service.is_user_active.assert_called_once_with(42)

        # Session should be cleared
        assert st.session_state.get("cap_authenticated") is False
        assert st.session_state.get("cap_user_id") is None

    def test_check_authentication_backward_compatible_without_user_service(self):
        """Test that check_authentication works without user_service parameter.

        For backward compatibility, check_authentication should work without
        the optional user_service parameter - it just won't check active status.
        """
        from ui.renderers.cap.auth_handler import CAPAuthHandler
        from datetime import datetime, timedelta
        import streamlit as st

        # Setup: authenticated user with valid session
        st.session_state.cap_authenticated = True
        st.session_state.cap_user_id = 42
        st.session_state.cap_researcher_name = "Test User"
        st.session_state.cap_login_time = datetime.now() - timedelta(minutes=30)

        # Mock _get_cap_secrets to return enabled=True
        with mock.patch("ui.renderers.cap.auth_handler._get_cap_secrets") as mock_secrets:
            mock_secrets.return_value = {"enabled": True}

            # Call without user_service parameter (backward compatible)
            is_auth, name = CAPAuthHandler.check_authentication()

        # Should return authenticated (no active status check without user_service)
        assert is_auth is True
        assert name == "Test User"

        # Cleanup
        st.session_state.pop("cap_authenticated", None)
        st.session_state.pop("cap_user_id", None)
        st.session_state.pop("cap_researcher_name", None)
        st.session_state.pop("cap_login_time", None)


class TestCAPBillQueueRendererIntegration:
    """Integration tests for CAPBillQueueRenderer with mock service."""

    def test_render_recent_annotations_empty(self):
        """Test that empty recent annotations doesn't crash."""
        from ui.renderers.cap.bill_queue_renderer import CAPBillQueueRenderer

        mock_service = mock.MagicMock()
        mock_service.get_recent_annotations.return_value = pd.DataFrame()

        renderer = CAPBillQueueRenderer(mock_service)
        # Should not raise
        renderer._render_recent_annotations(researcher_id=1)

    def test_render_other_annotations_handles_missing_column(self):
        """Test graceful handling when ResearcherID column is missing."""
        from ui.renderers.cap.bill_queue_renderer import CAPBillQueueRenderer

        mock_logger = mock.MagicMock()
        mock_service = mock.MagicMock()
        # Return DataFrame without ResearcherID column
        mock_service.get_all_annotations_for_bill.return_value = pd.DataFrame({
            "BillID": [1],
            "Direction": [1],
        })

        renderer = CAPBillQueueRenderer(mock_service, logger_obj=mock_logger)
        # Should not crash, should log warning
        renderer._render_other_annotations(bill_id=1, current_researcher_id=1)
        mock_logger.warning.assert_called()

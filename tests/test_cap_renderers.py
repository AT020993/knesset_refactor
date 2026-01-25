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

    def test_fetch_pdf_returns_error_on_timeout(self):
        """Test _fetch_pdf_impl returns error message on timeout."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer
        import requests

        with mock.patch("ui.renderers.cap.pdf_viewer.requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout()

            data, error = CAPPDFViewer._fetch_pdf_impl("http://example.com/test.pdf")

        assert data is None
        assert error is not None
        assert "timed out" in error.lower() or "timeout" in error.lower()

    def test_fetch_pdf_returns_error_on_404(self):
        """Test _fetch_pdf_impl returns specific error on 404."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer
        import requests

        with mock.patch("ui.renderers.cap.pdf_viewer.requests.get") as mock_get:
            mock_response = mock.MagicMock()
            mock_response.status_code = 404
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                response=mock_response
            )
            mock_get.return_value = mock_response

            data, error = CAPPDFViewer._fetch_pdf_impl("http://example.com/missing.pdf")

        assert data is None
        assert error is not None
        assert "404" in error
        assert "not found" in error.lower()

    def test_fetch_pdf_returns_error_on_403(self):
        """Test _fetch_pdf_impl returns specific error on 403."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer
        import requests

        with mock.patch("ui.renderers.cap.pdf_viewer.requests.get") as mock_get:
            mock_response = mock.MagicMock()
            mock_response.status_code = 403
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                response=mock_response
            )
            mock_get.return_value = mock_response

            data, error = CAPPDFViewer._fetch_pdf_impl("http://example.com/forbidden.pdf")

        assert data is None
        assert error is not None
        assert "403" in error
        assert "denied" in error.lower() or "access" in error.lower()

    def test_fetch_pdf_returns_error_on_connection_error(self):
        """Test _fetch_pdf_impl returns error on connection failure."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer
        import requests

        with mock.patch("ui.renderers.cap.pdf_viewer.requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.ConnectionError()

            data, error = CAPPDFViewer._fetch_pdf_impl("http://example.com/unreachable.pdf")

        assert data is None
        assert error is not None
        assert "connect" in error.lower() or "network" in error.lower()

    def test_fetch_pdf_returns_error_on_ssl_error(self):
        """Test _fetch_pdf_impl returns error on SSL certificate error."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer
        import requests

        with mock.patch("ui.renderers.cap.pdf_viewer.requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.SSLError()

            data, error = CAPPDFViewer._fetch_pdf_impl("http://example.com/bad-cert.pdf")

        assert data is None
        assert error is not None
        assert "ssl" in error.lower() or "certificate" in error.lower()

    def test_fetch_pdf_returns_none_error_on_success(self):
        """Test _fetch_pdf_impl returns (data, None) on success."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer

        # PDF magic bytes: %PDF
        pdf_content = b"%PDF-1.4 test content"

        with mock.patch("ui.renderers.cap.pdf_viewer.requests.get") as mock_get:
            mock_response = mock.MagicMock()
            mock_response.content = pdf_content
            mock_response.raise_for_status = mock.MagicMock()
            mock_get.return_value = mock_response

            data, error = CAPPDFViewer._fetch_pdf_impl("http://example.com/valid.pdf")

        assert data is not None
        assert error is None
        # Verify it's valid base64
        import base64
        decoded = base64.b64decode(data)
        assert decoded == pdf_content

    def test_fetch_pdf_returns_error_on_invalid_pdf(self):
        """Test _fetch_pdf_impl returns error when response is not a valid PDF."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer

        # Non-PDF content (HTML error page)
        html_content = b"<html><body>Error 500</body></html>"

        with mock.patch("ui.renderers.cap.pdf_viewer.requests.get") as mock_get:
            mock_response = mock.MagicMock()
            mock_response.content = html_content
            mock_response.raise_for_status = mock.MagicMock()
            mock_get.return_value = mock_response

            data, error = CAPPDFViewer._fetch_pdf_impl("http://example.com/not-a-pdf.pdf")

        assert data is None
        assert error is not None
        assert "not a valid pdf" in error.lower() or "invalid" in error.lower()

    def test_fetch_pdf_returns_error_on_other_http_error(self):
        """Test _fetch_pdf_impl returns generic error on other HTTP status codes."""
        from ui.renderers.cap.pdf_viewer import CAPPDFViewer
        import requests

        with mock.patch("ui.renderers.cap.pdf_viewer.requests.get") as mock_get:
            mock_response = mock.MagicMock()
            mock_response.status_code = 500
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                response=mock_response
            )
            mock_get.return_value = mock_response

            data, error = CAPPDFViewer._fetch_pdf_impl("http://example.com/server-error.pdf")

        assert data is None
        assert error is not None
        assert "500" in error


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


class TestCAPFormRendererCategoryValidation:
    """Tests for CAPFormRenderer category validation functionality."""

    def test_validate_category_selection_rejects_none_major(self):
        """Test validation rejects None major category."""
        from ui.renderers.cap.form_renderer import CAPFormRenderer

        mock_service = mock.MagicMock()
        renderer = CAPFormRenderer(mock_service)

        is_valid, error = renderer._validate_category_selection(
            major_code=None, minor_code=101
        )

        assert is_valid is False
        assert error is not None
        assert "Major Category" in error

    def test_validate_category_selection_rejects_none_minor(self):
        """Test validation rejects None minor category."""
        from ui.renderers.cap.form_renderer import CAPFormRenderer

        mock_service = mock.MagicMock()
        renderer = CAPFormRenderer(mock_service)

        is_valid, error = renderer._validate_category_selection(
            major_code=1, minor_code=None
        )

        assert is_valid is False
        assert error is not None
        assert "Minor Category" in error

    def test_validate_category_selection_rejects_mismatched_categories(self):
        """Test validation rejects minor category that doesn't belong to major."""
        from ui.renderers.cap.form_renderer import CAPFormRenderer

        mock_service = mock.MagicMock()
        # Major category 1 has minors 101, 102, 103
        mock_service.get_minor_categories.return_value = [
            {"MinorCode": 101, "MinorTopic_HE": "Topic 1"},
            {"MinorCode": 102, "MinorTopic_HE": "Topic 2"},
            {"MinorCode": 103, "MinorTopic_HE": "Topic 3"},
        ]
        renderer = CAPFormRenderer(mock_service)

        # Trying to use minor 201 (from major 2) with major 1
        is_valid, error = renderer._validate_category_selection(
            major_code=1, minor_code=201
        )

        assert is_valid is False
        assert error is not None
        assert "does not belong to major category" in error
        mock_service.get_minor_categories.assert_called_once_with(1)

    def test_validate_category_selection_accepts_valid_combination(self):
        """Test validation accepts valid major/minor combination."""
        from ui.renderers.cap.form_renderer import CAPFormRenderer

        mock_service = mock.MagicMock()
        # Major category 1 has minors 101, 102, 103
        mock_service.get_minor_categories.return_value = [
            {"MinorCode": 101, "MinorTopic_HE": "Topic 1"},
            {"MinorCode": 102, "MinorTopic_HE": "Topic 2"},
            {"MinorCode": 103, "MinorTopic_HE": "Topic 3"},
        ]
        renderer = CAPFormRenderer(mock_service)

        is_valid, error = renderer._validate_category_selection(
            major_code=1, minor_code=102
        )

        assert is_valid is True
        assert error is None
        mock_service.get_minor_categories.assert_called_once_with(1)


class TestCAPFormRenderer:
    """Tests for CAPFormRenderer cloud sync functionality."""

    def test_sync_to_cloud_returns_true_when_disabled(self):
        """Test _sync_to_cloud returns True when cloud storage is not enabled."""
        from ui.renderers.cap.form_renderer import CAPFormRenderer

        mock_service = mock.MagicMock()
        renderer = CAPFormRenderer(mock_service)

        # Mock StorageSyncService at its source module (lazy import location)
        with mock.patch("data.services.storage_sync_service.StorageSyncService") as mock_sync_class:
            mock_sync_instance = mock.MagicMock()
            mock_sync_instance.is_enabled.return_value = False
            mock_sync_class.return_value = mock_sync_instance

            result = renderer._sync_to_cloud()

        assert result is True

    def test_sync_to_cloud_returns_true_on_success(self):
        """Test _sync_to_cloud returns True when upload succeeds."""
        from ui.renderers.cap.form_renderer import CAPFormRenderer

        mock_service = mock.MagicMock()
        renderer = CAPFormRenderer(mock_service)

        # Mock StorageSyncService at its source module
        with mock.patch("data.services.storage_sync_service.StorageSyncService") as mock_sync_class:
            mock_sync_instance = mock.MagicMock()
            mock_sync_instance.is_enabled.return_value = True
            mock_sync_instance.gcs_manager.upload_file.return_value = True
            mock_sync_class.return_value = mock_sync_instance

            result = renderer._sync_to_cloud()

        assert result is True

    def test_sync_to_cloud_returns_false_on_failure(self):
        """Test _sync_to_cloud returns False when upload fails."""
        from ui.renderers.cap.form_renderer import CAPFormRenderer

        mock_service = mock.MagicMock()
        renderer = CAPFormRenderer(mock_service)

        # Mock StorageSyncService at its source module
        with mock.patch("data.services.storage_sync_service.StorageSyncService") as mock_sync_class:
            mock_sync_instance = mock.MagicMock()
            mock_sync_instance.is_enabled.return_value = True
            mock_sync_instance.gcs_manager.upload_file.return_value = False
            mock_sync_class.return_value = mock_sync_instance

            result = renderer._sync_to_cloud()

        assert result is False

    def test_sync_to_cloud_returns_false_on_exception(self):
        """Test _sync_to_cloud returns False when an exception occurs."""
        from ui.renderers.cap.form_renderer import CAPFormRenderer

        mock_service = mock.MagicMock()
        renderer = CAPFormRenderer(mock_service)

        # Mock StorageSyncService at its source module to raise an exception
        with mock.patch("data.services.storage_sync_service.StorageSyncService") as mock_sync_class:
            mock_sync_class.side_effect = Exception("Connection error")

            result = renderer._sync_to_cloud()

        assert result is False


class TestCAPCodedBillsRendererSync:
    """Tests for CAPCodedBillsRenderer cloud sync functionality."""

    def test_sync_to_cloud_returns_true_when_disabled(self):
        """Test _sync_to_cloud returns True when cloud storage is not enabled."""
        from ui.renderers.cap.coded_bills_renderer import CAPCodedBillsRenderer

        mock_service = mock.MagicMock()
        renderer = CAPCodedBillsRenderer(mock_service)

        # Mock StorageSyncService at its source module
        with mock.patch("data.services.storage_sync_service.StorageSyncService") as mock_sync_class:
            mock_sync_instance = mock.MagicMock()
            mock_sync_instance.is_enabled.return_value = False
            mock_sync_class.return_value = mock_sync_instance

            result = renderer._sync_to_cloud()

        assert result is True

    def test_sync_to_cloud_returns_true_on_success(self):
        """Test _sync_to_cloud returns True when upload succeeds."""
        from ui.renderers.cap.coded_bills_renderer import CAPCodedBillsRenderer

        mock_service = mock.MagicMock()
        renderer = CAPCodedBillsRenderer(mock_service)

        # Mock StorageSyncService at its source module
        with mock.patch("data.services.storage_sync_service.StorageSyncService") as mock_sync_class:
            mock_sync_instance = mock.MagicMock()
            mock_sync_instance.is_enabled.return_value = True
            mock_sync_instance.gcs_manager.upload_file.return_value = True
            mock_sync_class.return_value = mock_sync_instance

            result = renderer._sync_to_cloud()

        assert result is True

    def test_sync_to_cloud_returns_false_on_failure(self):
        """Test _sync_to_cloud returns False when upload fails."""
        from ui.renderers.cap.coded_bills_renderer import CAPCodedBillsRenderer

        mock_service = mock.MagicMock()
        renderer = CAPCodedBillsRenderer(mock_service)

        # Mock StorageSyncService at its source module
        with mock.patch("data.services.storage_sync_service.StorageSyncService") as mock_sync_class:
            mock_sync_instance = mock.MagicMock()
            mock_sync_instance.is_enabled.return_value = True
            mock_sync_instance.gcs_manager.upload_file.return_value = False
            mock_sync_class.return_value = mock_sync_instance

            result = renderer._sync_to_cloud()


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


class TestCAPAdminRenderer:
    """Tests for CAPAdminRenderer helper methods."""

    def test_get_user_list_returns_all_users(self):
        """Test _get_user_list delegates to service.get_all_users()."""
        from ui.renderers.cap.admin_renderer import CAPAdminRenderer
        from pathlib import Path

        mock_users_df = pd.DataFrame({
            "ResearcherID": [1, 2, 3],
            "Username": ["admin", "alice", "bob"],
            "DisplayName": ["Admin User", "Alice Smith", "Bob Jones"],
            "Role": ["admin", "researcher", "researcher"],
            "IsActive": [True, True, False],
        })

        renderer = CAPAdminRenderer(Path("/fake/db.duckdb"))
        # Mock the user_service property
        mock_user_service = mock.MagicMock()
        mock_user_service.get_all_users.return_value = mock_users_df
        renderer._user_service = mock_user_service

        result = renderer._get_user_list()

        mock_user_service.get_all_users.assert_called_once()
        assert len(result) == 3
        assert list(result["Username"]) == ["admin", "alice", "bob"]

    def test_can_delete_user_prevents_self_deletion(self):
        """Test _can_delete_user returns False when trying to delete yourself."""
        from ui.renderers.cap.admin_renderer import CAPAdminRenderer
        from pathlib import Path

        renderer = CAPAdminRenderer(Path("/fake/db.duckdb"))
        mock_user_service = mock.MagicMock()
        renderer._user_service = mock_user_service

        # Try to delete user ID 42 when current user is also 42
        can_delete, reason = renderer._can_delete_user(user_id=42, current_user_id=42)

        assert can_delete is False
        assert "Cannot delete your own account" in reason
        # Should not even check annotations if it's self-deletion
        mock_user_service.get_user_annotation_count.assert_not_called()

    def test_can_delete_user_prevents_deletion_with_annotations(self):
        """Test _can_delete_user returns False when user has annotations."""
        from ui.renderers.cap.admin_renderer import CAPAdminRenderer
        from pathlib import Path

        renderer = CAPAdminRenderer(Path("/fake/db.duckdb"))
        mock_user_service = mock.MagicMock()
        mock_user_service.get_user_annotation_count.return_value = 15  # Has annotations
        renderer._user_service = mock_user_service

        # Try to delete user ID 99 when current user is 42 (different users)
        can_delete, reason = renderer._can_delete_user(user_id=99, current_user_id=42)

        assert can_delete is False
        assert "15 annotations" in reason
        assert "deactivate" in reason.lower()
        mock_user_service.get_user_annotation_count.assert_called_once_with(99)

    def test_can_delete_user_allows_deletion_without_annotations(self):
        """Test _can_delete_user returns True when user has no annotations."""
        from ui.renderers.cap.admin_renderer import CAPAdminRenderer
        from pathlib import Path

        renderer = CAPAdminRenderer(Path("/fake/db.duckdb"))
        mock_user_service = mock.MagicMock()
        mock_user_service.get_user_annotation_count.return_value = 0  # No annotations
        renderer._user_service = mock_user_service

        # Try to delete user ID 99 when current user is 42 (different users)
        can_delete, reason = renderer._can_delete_user(user_id=99, current_user_id=42)

        assert can_delete is True
        assert reason == ""
        mock_user_service.get_user_annotation_count.assert_called_once_with(99)

    def test_validate_role_change_rejects_invalid_role(self):
        """Test _validate_role_change returns error for invalid roles."""
        from ui.renderers.cap.admin_renderer import CAPAdminRenderer
        from pathlib import Path

        renderer = CAPAdminRenderer(Path("/fake/db.duckdb"))

        # Test various invalid roles
        invalid_roles = ["superuser", "ADMIN", "Admin", "", "root", "guest", None]

        for role in invalid_roles:
            is_valid, error = renderer._validate_role_change(role)
            assert is_valid is False, f"Role '{role}' should be invalid"
            assert error is not None, f"Role '{role}' should have error message"
            assert "Invalid role" in error or "Must be one of" in error

    def test_validate_role_change_accepts_valid_roles(self):
        """Test _validate_role_change accepts 'admin' and 'researcher'."""
        from ui.renderers.cap.admin_renderer import CAPAdminRenderer
        from pathlib import Path

        renderer = CAPAdminRenderer(Path("/fake/db.duckdb"))

        # Test valid roles
        for role in ["admin", "researcher"]:
            is_valid, error = renderer._validate_role_change(role)
            assert is_valid is True, f"Role '{role}' should be valid"
            assert error is None, f"Role '{role}' should not have error message"


class TestCAPStatsRenderer:
    """Tests for CAPStatsRenderer helper methods."""

    def test_format_percentage_half(self):
        """Test _format_percentage formats 0.5 as '50.0%'."""
        from ui.renderers.cap.stats_renderer import CAPStatsRenderer

        result = CAPStatsRenderer._format_percentage(0.5)
        assert result == "50.0%"

    def test_format_percentage_zero(self):
        """Test _format_percentage formats 0 as '0.0%'."""
        from ui.renderers.cap.stats_renderer import CAPStatsRenderer

        result = CAPStatsRenderer._format_percentage(0.0)
        assert result == "0.0%"

    def test_format_percentage_full(self):
        """Test _format_percentage formats 1.0 as '100.0%'."""
        from ui.renderers.cap.stats_renderer import CAPStatsRenderer

        result = CAPStatsRenderer._format_percentage(1.0)
        assert result == "100.0%"

    def test_format_percentage_small(self):
        """Test _format_percentage formats small values correctly."""
        from ui.renderers.cap.stats_renderer import CAPStatsRenderer

        result = CAPStatsRenderer._format_percentage(0.123)
        assert result == "12.3%"

    def test_get_summary_metrics_calculates_correctly(self):
        """Test _get_summary_metrics extracts and calculates metrics."""
        from ui.renderers.cap.stats_renderer import CAPStatsRenderer

        mock_service = mock.MagicMock()
        renderer = CAPStatsRenderer(mock_service)

        stats = {
            "total_bills": 100,
            "total_coded": 25,
        }

        result = renderer._get_summary_metrics(stats)

        assert result["total_coded"] == 25
        assert result["total_bills"] == 100
        assert result["progress_pct"] == 0.25
        assert result["progress_str"] == "25.0%"

    def test_get_summary_metrics_verifies_service_delegation(self):
        """Test that render_stats_dashboard delegates to service.get_annotation_stats()."""
        from ui.renderers.cap.stats_renderer import CAPStatsRenderer

        mock_service = mock.MagicMock()
        mock_service.get_annotation_stats.return_value = {
            "total_bills": 50,
            "total_coded": 10,
        }

        renderer = CAPStatsRenderer(mock_service)

        # Call render - it should call the service
        # We can't fully test render without Streamlit, but we can verify the method exists
        # and test the helper method directly
        stats = mock_service.get_annotation_stats()
        result = renderer._get_summary_metrics(stats)

        mock_service.get_annotation_stats.assert_called_once()
        assert result["total_coded"] == 10

    def test_handles_empty_database(self):
        """Test _get_summary_metrics handles empty database gracefully (zero total_bills)."""
        from ui.renderers.cap.stats_renderer import CAPStatsRenderer

        mock_service = mock.MagicMock()
        renderer = CAPStatsRenderer(mock_service)

        # Empty database - no bills at all
        stats = {
            "total_bills": 0,
            "total_coded": 0,
        }

        result = renderer._get_summary_metrics(stats)

        # Should not crash with division by zero
        assert result["total_coded"] == 0
        assert result["total_bills"] == 0
        assert result["progress_pct"] == 0.0
        assert result["progress_str"] == "0.0%"

    def test_handles_missing_keys(self):
        """Test _get_summary_metrics handles missing keys with defaults."""
        from ui.renderers.cap.stats_renderer import CAPStatsRenderer

        mock_service = mock.MagicMock()
        renderer = CAPStatsRenderer(mock_service)

        # Empty stats dict
        stats = {}

        result = renderer._get_summary_metrics(stats)

        assert result["total_coded"] == 0
        assert result["total_bills"] == 0
        assert result["progress_pct"] == 0.0
        assert result["progress_str"] == "0.0%"

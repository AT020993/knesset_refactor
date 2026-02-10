"""Runtime patch fixtures for Streamlit/cache and external integrations."""

from __future__ import annotations

import os
import importlib

import pytest


os.environ["STREAMLIT_CACHE_DISABLED"] = "1"


def passthrough_decorator(func=None, *args, **kwargs):
    """Decorator that returns the original function unchanged."""
    if func is None:
        def wrapper(fn):
            return fn
        return wrapper
    return func

st = importlib.import_module("streamlit")
original_cache_data = getattr(st, "cache_data", None)
original_cache_resource = getattr(st, "cache_resource", None)
st.cache_data = passthrough_decorator
st.cache_resource = passthrough_decorator

try:
    from data.services.data_refresh_service import DataRefreshService
except ImportError:
    DataRefreshService = None


@pytest.fixture(autouse=True)
def stub_download_table(request, monkeypatch):
    """Avoid live OData downloads during tests by stubbing download paths."""
    if "no_autouse_stub" in request.keywords:
        return

    async def fake_download_table(self, table, resume_state=None):
        import pandas as pd

        return pd.DataFrame([])

    try:
        import backend.fetch_table as ft

        monkeypatch.setattr(ft, "download_table", fake_download_table)
    except (ImportError, AttributeError):
        pass

    if DataRefreshService:
        from api.odata_client import ODataClient

        monkeypatch.setattr(ODataClient, "download_table", fake_download_table)


@pytest.fixture(autouse=True)
def disable_cloud_storage(monkeypatch):
    """Disable cloud sync by default in tests (no Streamlit secrets dependency)."""
    try:
        from data.services.storage_sync_service import StorageSyncService

        def mock_init(self, gcs_manager=None, logger_obj=None):
            import logging

            self.logger = logger_obj or logging.getLogger(__name__)
            self.gcs_manager = None
            self.enabled = False

        def mock_is_enabled(self):
            return False

        def mock_sync_after_refresh(self, progress_callback=None):
            return True

        monkeypatch.setattr(StorageSyncService, "__init__", mock_init)
        monkeypatch.setattr(StorageSyncService, "is_enabled", mock_is_enabled)
        monkeypatch.setattr(StorageSyncService, "sync_after_refresh", mock_sync_after_refresh)
    except ImportError:
        pass

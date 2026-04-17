"""Unit tests for src/data/recurring_bills/fetch_tal.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from data.recurring_bills.fetch_tal import download_bulk_csv


FIXTURE_CSV = Path(__file__).parent / "fixtures" / "recurring_bills" / "tal_bulk_sample.csv"


class TestDownloadBulkCsv:
    def _mock_response(self, body: bytes, status: int = 200, headers: dict | None = None):
        resp = MagicMock()
        resp.status_code = status
        resp.content = body
        resp.iter_content = lambda chunk_size: [body]
        resp.headers = headers or {}
        resp.raise_for_status = MagicMock()
        return resp

    def test_writes_csv_to_disk(self, tmp_path: Path):
        body = FIXTURE_CSV.read_bytes()
        out = tmp_path / "tal_alovitz_bills.csv"

        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(body)
            path = download_bulk_csv(out)

        assert path == out
        assert out.exists()
        assert out.read_bytes() == body

    def test_sends_research_bot_user_agent(self, tmp_path: Path):
        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(b"bill_id\n1\n")
            download_bulk_csv(tmp_path / "x.csv")

        _, kwargs = mock_get.call_args
        ua = kwargs["headers"]["User-Agent"]
        assert "knesset-refactor-research-bot" in ua

    def test_etag_roundtrip_skips_download_on_304(self, tmp_path: Path):
        out = tmp_path / "tal_alovitz_bills.csv"
        etag_file = tmp_path / "tal_alovitz_bills.csv.etag"
        out.write_bytes(b"old content")
        etag_file.write_text('"abc123"')

        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(b"", status=304)
            path = download_bulk_csv(out)

        assert path == out
        assert out.read_bytes() == b"old content"  # untouched
        _, kwargs = mock_get.call_args
        assert kwargs["headers"]["If-None-Match"] == '"abc123"'

    def test_etag_roundtrip_writes_new_etag_on_200(self, tmp_path: Path):
        out = tmp_path / "tal_alovitz_bills.csv"
        etag_file = tmp_path / "tal_alovitz_bills.csv.etag"

        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(
                b"bill_id\n1\n",
                headers={"ETag": '"xyz789"'},
            )
            download_bulk_csv(out)

        assert etag_file.read_text() == '"xyz789"'

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


import json
from data.recurring_bills.fetch_tal import fetch_bill_detail, fetch_many_details


class TestFetchBillDetail:
    def _mock_json(self, payload: dict, status: int = 200):
        resp = MagicMock()
        resp.status_code = status
        resp.json = MagicMock(return_value=payload)
        resp.raise_for_status = MagicMock()
        return resp

    def test_writes_json_to_cache_on_miss(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        payload = {"bill_id": 477120, "patient_zero_bill_id": 477120, "category": "cross"}

        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            mock_get.return_value = self._mock_json(payload)
            path = fetch_bill_detail(477120, cache_dir)

        assert path == cache_dir / "477120.json"
        assert json.loads(path.read_text()) == payload
        assert mock_get.call_count == 1

    def test_uses_cache_on_hit_and_skips_http(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        cached = cache_dir / "477120.json"
        cached.write_text('{"bill_id": 477120, "cached": true}')

        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            path = fetch_bill_detail(477120, cache_dir)

        assert mock_get.call_count == 0
        assert json.loads(path.read_text())["cached"] is True

    def test_force_refresh_bypasses_cache(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        (cache_dir / "477120.json").write_text('{"cached": true}')

        with patch("data.recurring_bills.fetch_tal.requests.get") as mock_get:
            mock_get.return_value = self._mock_json({"bill_id": 477120, "cached": False})
            fetch_bill_detail(477120, cache_dir, force_refresh=True)

        assert mock_get.call_count == 1


class TestFetchManyDetails:
    def test_respects_delay_and_caches_all(self, tmp_path: Path):
        cache_dir = tmp_path / "cache"
        bill_ids = [477119, 477120, 477137]

        def side_effect(url, **kwargs):
            bid = int(url.rsplit("/", 1)[-1])
            resp = MagicMock()
            resp.status_code = 200
            resp.json = MagicMock(return_value={"bill_id": bid})
            resp.raise_for_status = MagicMock()
            return resp

        with patch("data.recurring_bills.fetch_tal.requests.get", side_effect=side_effect):
            with patch("data.recurring_bills.fetch_tal.time.sleep") as mock_sleep:
                paths = fetch_many_details(bill_ids, cache_dir, delay_s=0.5)

        assert len(paths) == 3
        # Delay is called before each HTTP fetch (3 times)
        assert mock_sleep.call_count == 3
        for call in mock_sleep.call_args_list:
            assert call.args == (0.5,)

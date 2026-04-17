"""Unit tests for src/data/recurring_bills/report.py."""

from __future__ import annotations

import pandas as pd

from data.recurring_bills.report import compute_stats, render_markdown


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame([
        {"BillID": 1, "KnessetNum": 17, "Name": "a", "is_original": True,
         "original_bill_id": 1, "classification_source": "name_fallback_k16_k18",
         "predecessor_bill_ids": []},
        {"BillID": 2, "KnessetNum": 18, "Name": "a", "is_original": False,
         "original_bill_id": 1, "classification_source": "name_fallback_k16_k18",
         "predecessor_bill_ids": [1]},
        {"BillID": 3, "KnessetNum": 20, "Name": "b", "is_original": True,
         "original_bill_id": 3, "classification_source": "tal_alovitz",
         "predecessor_bill_ids": []},
        {"BillID": 4, "KnessetNum": 21, "Name": "b", "is_original": False,
         "original_bill_id": 3, "classification_source": "tal_alovitz",
         "predecessor_bill_ids": [3]},
    ])


class TestComputeStats:
    def test_total_counts(self):
        stats = compute_stats(_sample_df())
        assert stats["total"] == 4
        assert stats["by_source"]["tal_alovitz"] == 2
        assert stats["by_source"]["name_fallback_k16_k18"] == 2

    def test_original_vs_recurring_split(self):
        stats = compute_stats(_sample_df())
        assert stats["originals"] == 2
        assert stats["recurring"] == 2

    def test_per_knesset_breakdown(self):
        stats = compute_stats(_sample_df())
        assert stats["by_knesset"][17]["total"] == 1
        assert stats["by_knesset"][20]["originals"] == 1


class TestRenderMarkdown:
    def test_includes_summary_headers(self):
        md = render_markdown(compute_stats(_sample_df()))
        assert "# Recurring Bills Classification Coverage" in md
        assert "## Summary" in md
        assert "## By Knesset" in md
        assert "## By Source" in md

    def test_renders_counts_in_body(self):
        md = render_markdown(compute_stats(_sample_df()))
        assert "Total bills classified | 4" in md or "| 4 |" in md

"""Build a markdown coverage report from the classified DataFrame."""

from __future__ import annotations

from io import StringIO

import pandas as pd


def compute_stats(df: pd.DataFrame) -> dict:
    """Compute summary statistics for the coverage report.

    Returns a dict shaped::

        {
          "total": int,
          "originals": int,
          "recurring": int,
          "by_source": {source: count},
          "by_knesset": {knesset_num: {"total": int, "originals": int, "recurring": int}},
        }
    """
    return {
        "total": int(len(df)),
        "originals": int(df["is_original"].sum()),
        "recurring": int((~df["is_original"]).sum()),
        "by_source": df["classification_source"].value_counts().to_dict(),
        "by_knesset": {
            int(kn): {
                "total": int(len(group)),
                "originals": int(group["is_original"].sum()),
                "recurring": int((~group["is_original"]).sum()),
            }
            for kn, group in df.groupby("KnessetNum")
        },
    }


def render_markdown(stats: dict) -> str:
    """Render stats dict to a human-readable markdown report."""
    buf = StringIO()
    buf.write("# Recurring Bills Classification Coverage\n\n")
    buf.write("## Summary\n\n")
    buf.write("| Metric | Value |\n")
    buf.write("|---|---|\n")
    buf.write(f"| Total bills classified | {stats['total']} |\n")
    buf.write(f"| Originals | {stats['originals']} |\n")
    buf.write(f"| Recurring | {stats['recurring']} |\n")
    if stats["total"]:
        pct = 100 * stats["recurring"] / stats["total"]
        buf.write(f"| Recurring % | {pct:.1f} |\n")
    buf.write("\n")

    buf.write("## By Source\n\n| Source | Count |\n|---|---|\n")
    for source, count in sorted(stats["by_source"].items()):
        buf.write(f"| {source} | {count} |\n")
    buf.write("\n")

    buf.write("## By Knesset\n\n| Knesset | Total | Originals | Recurring |\n|---|---|---|---|\n")
    for kn in sorted(stats["by_knesset"]):
        row = stats["by_knesset"][kn]
        buf.write(f"| K{kn} | {row['total']} | {row['originals']} | {row['recurring']} |\n")
    buf.write("\n")
    return buf.getvalue()

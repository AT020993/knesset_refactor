"""MAJORIL code-to-label mapping for Israeli policy topic codes.

Based on the Israeli Codebook (ספר הקוד הישראלי) pages 2-3.
Codes 11 and 22 are not used in the Israeli codebook.
"""

import csv
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

# Inline fallback mapping (used if CSV is missing)
_MAJORIL_LABELS: Dict[int, Dict[str, str]] = {
    1: {"he": "מקרו-כלכלה", "en": "Macro-economics"},
    2: {"he": "זכויות אזרח", "en": "Civil Rights"},
    3: {"he": "בריאות", "en": "Health"},
    4: {"he": "חקלאות", "en": "Agriculture"},
    5: {"he": "עבודה ותעסוקה", "en": "Labor"},
    6: {"he": "חינוך", "en": "Education"},
    7: {"he": "סביבה", "en": "Environment"},
    8: {"he": "אנרגיה", "en": "Energy"},
    9: {"he": "הגירה", "en": "Immigration"},
    10: {"he": "תחבורה", "en": "Transportation"},
    12: {"he": "חוק, פשע וענייני משפחה", "en": "Law & Crime"},
    13: {"he": "רווחה חברתית", "en": "Welfare"},
    14: {"he": "דיור ותכנון", "en": "Housing"},
    15: {"he": "מסחר מקומי", "en": "Commerce"},
    16: {"he": "ביטחון", "en": "Defense"},
    17: {"he": "טכנולוגיה", "en": "Technology"},
    18: {"he": "סחר חוץ", "en": "Trade"},
    19: {"he": "יחסים בינלאומיים", "en": "International Affairs"},
    20: {"he": "פעולות ממשלתיות", "en": "Government"},
    21: {"he": "קרקעות ציבוריות", "en": "Public Lands"},
    23: {"he": "תרבות, זהות וחברה", "en": "Culture"},
}


@lru_cache(maxsize=1)
def load_majoril_labels(
    csv_path: Optional[Path] = None,
) -> Dict[int, Dict[str, str]]:
    """Load MAJORIL code-to-label mapping.

    Tries CSV file first, falls back to inline dictionary.

    Returns:
        Dict mapping code (int) to {"he": str, "en": str}
    """
    if csv_path is None:
        csv_path = (
            Path(__file__).parent.parent.parent
            / "data"
            / "taxonomies"
            / "majoril_labels.csv"
        )

    if csv_path.exists():
        labels: Dict[int, Dict[str, str]] = {}
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = int(row["Code"])
                labels[code] = {"he": row["Hebrew"], "en": row["English"]}
        return labels

    return _MAJORIL_LABELS


def get_majoril_label(code: int, language: str = "he") -> str:
    """Get human-readable label for a MAJORIL code.

    Args:
        code: MAJORIL numeric code
        language: "he" for Hebrew, "en" for English

    Returns:
        Label string, or "Unknown ({code})" if code not found
    """
    labels = load_majoril_labels()
    entry = labels.get(code)
    if entry:
        return entry.get(language, entry.get("he", f"Unknown ({code})"))
    return f"Unknown ({code})"


def get_majoril_display(code: int, language: str = "he") -> str:
    """Get display string with code and label: '6 - חינוך'.

    Used for chart axis labels.
    """
    label = get_majoril_label(code, language)
    return f"{code} - {label}"


def apply_majoril_labels(
    df: pd.DataFrame,
    code_column: str = "TopicCode",
    language: str = "he",
) -> pd.DataFrame:
    """Apply MAJORIL labels to a DataFrame column.

    Adds a 'TopicLabel' column with formatted labels like '6 - חינוך'.

    Returns:
        DataFrame with TopicLabel column added.
    """
    labels = load_majoril_labels()
    df = df.copy()
    df["TopicLabel"] = df[code_column].apply(
        lambda c: (
            f"{int(c)} - {labels.get(int(c), {}).get(language, f'Unknown ({c})')}"
            if pd.notna(c)
            else "Unknown"
        )
    )
    return df

"""CAP topic query definitions.

MVP shape for Phase 0. Counts bills coded under each CAP major topic via
``UserBillCAP``; agenda/query item counts join in Phase 4 once each domain
has its own coding table wired consistently.
"""

from __future__ import annotations

from typing import Any

TOPICS_QUERIES: dict[str, dict[str, Any]] = {
    "topic_list": {
        "sql": """
WITH MajorTopics AS (
    SELECT DISTINCT
        MajorCode,
        MajorTopic_HE,
        MajorTopic_EN
    FROM UserCAPTaxonomy
    WHERE MajorCode IS NOT NULL
),
BillCountsByMajor AS (
    SELECT
        t.MajorCode,
        COUNT(DISTINCT ubc.BillID) AS bill_count
    FROM UserBillCAP ubc
    JOIN UserCAPTaxonomy t ON ubc.CAPMinorCode = t.MinorCode
    GROUP BY t.MajorCode
)
SELECT
    mt.MajorCode                        AS cap_code,
    mt.MajorTopic_HE                    AS title_he,
    mt.MajorTopic_EN                    AS title_en,
    COALESCE(bc.bill_count, 0)          AS bill_count
FROM MajorTopics mt
LEFT JOIN BillCountsByMajor bc ON mt.MajorCode = bc.MajorCode
ORDER BY mt.MajorCode
""".strip(),
        "knesset_filter_column": None,
        "faction_filter_column": None,
        "description": "CAP major topics with bill count coded under each.",
    },
}

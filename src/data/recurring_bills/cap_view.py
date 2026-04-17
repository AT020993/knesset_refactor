"""CAP integration: expose bill_classifications alongside UserBillCAP via a view."""

from __future__ import annotations

from pathlib import Path

import duckdb


VIEW_SQL = """
CREATE OR REPLACE VIEW v_cap_bills_with_recurrence AS
SELECT
    ubc.*,
    bc.is_original,
    bc.original_bill_id,
    bc.tal_category,
    bc.classification_source
FROM UserBillCAP ubc
LEFT JOIN bill_classifications bc USING (BillID)
"""


def create_cap_view(*, db_path: Path) -> None:
    """Create or replace ``v_cap_bills_with_recurrence`` in the warehouse.

    Expects ``UserBillCAP`` and ``bill_classifications`` tables to exist.
    """
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        con.execute(VIEW_SQL)
    finally:
        con.close()

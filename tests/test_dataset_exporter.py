import logging
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd

from ui.renderers.data_refresh.dataset_exporter import DatasetExporter


def _exporter(db_path: Path) -> DatasetExporter:
    return DatasetExporter(db_path, logging.getLogger(__name__))


def test_count_query_removes_only_top_level_order_by() -> None:
    sql = """
    SELECT
        id,
        ROW_NUMBER() OVER (PARTITION BY grp ORDER BY created_at DESC) AS rn
    FROM items
    ORDER BY id DESC
    LIMIT 1000
    """

    count_sql = DatasetExporter.build_count_query(sql)

    assert "ROW_NUMBER() OVER (PARTITION BY grp ORDER BY created_at DESC)" in count_sql
    assert "ORDER BY id DESC" not in count_sql
    assert "LIMIT 1000" not in count_sql


def test_export_full_dataset_csv_uses_duckdb_copy_not_dataframe_fetch(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(db_path.as_posix()) as con:
        con.execute("CREATE TABLE bills AS SELECT range AS BillID FROM range(3)")

    exporter = _exporter(db_path)

    with patch.object(
        DatasetExporter,
        "fetch_full_dataset",
        side_effect=AssertionError("CSV export must not materialize a DataFrame"),
    ):
        csv_data, row_count = exporter.export_full_dataset_csv_bytes(
            "SELECT BillID FROM bills ORDER BY BillID LIMIT 1"
        )

    assert row_count == 3
    assert csv_data.decode("utf-8-sig").splitlines() == ["BillID", "0", "1", "2"]


def test_export_full_dataset_csv_handles_query_with_trailing_semicolon(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(db_path.as_posix()) as con:
        con.execute("CREATE TABLE bills AS SELECT range AS BillID FROM range(2)")

    exporter = _exporter(db_path)

    csv_data, row_count = exporter.export_full_dataset_csv_bytes(
        "SELECT BillID FROM bills ORDER BY BillID LIMIT 1;"
    )

    assert row_count == 2
    assert csv_data.decode("utf-8-sig").splitlines() == ["BillID", "0", "1"]


def test_excel_export_keeps_dataframe_path_for_hyperlink_workflow(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse.duckdb"
    exporter = _exporter(db_path)
    expected_df = pd.DataFrame({"BillID": [1]})

    with patch.object(DatasetExporter, "fetch_full_dataset", return_value=expected_df):
        result = exporter.fetch_full_dataset("SELECT 1 AS BillID")

    assert result is expected_df

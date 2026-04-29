#!/usr/bin/env python3
"""
Export a sendable all-parliamentary-queries CSV.

Inputs:
- Official/project warehouse data from data/warehouse.duckdb.
- Optional local researcher workbook with collected K17-K24 query coding and
  query-level coalition labels.

The output is intentionally narrower than a raw debug export: one row per
QueryID, one main value per business field, plus source/caveat columns where
the collected workbook and warehouse disagree.

Usage:
    PYTHONPATH="./src" python scripts/export_parliamentary_queries_sendable.py
    PYTHONPATH="./src" python scripts/export_parliamentary_queries_sendable.py \
        --coded-query-xlsx parliamentary_queries_coded_KN17_24_Feb2026.xlsx \
        --output data/exports/parliamentary_queries_all_columns_simplified_2026-04-29.csv
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "data" / "warehouse.duckdb"
DEFAULT_CODED_QUERY_XLSX = PROJECT_ROOT / "parliamentary_queries_coded_KN17_24_Feb2026.xlsx"
DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "data"
    / "exports"
    / "parliamentary_queries_all_columns_simplified_2026-04-29.csv"
)


WAREHOUSE_QUERY_SQL = r"""
WITH exact_pos AS (
    SELECT
        q.QueryID,
        ptp.PersonID,
        CAST(ptp.KnessetNum AS BIGINT) AS KnessetNum,
        CAST(ptp.FactionID AS BIGINT) AS FactionID,
        ptp.FactionName,
        ptp.StartDate,
        ptp.FinishDate,
        ROW_NUMBER() OVER (
            PARTITION BY q.QueryID
            ORDER BY
                TRY_CAST(ptp.StartDate AS TIMESTAMP) DESC NULLS LAST,
                ptp.PersonToPositionID DESC NULLS LAST
        ) AS rn
    FROM KNS_Query q
    JOIN KNS_PersonToPosition ptp
      ON q.PersonID = ptp.PersonID
     AND CAST(q.KnessetNum AS BIGINT) = CAST(ptp.KnessetNum AS BIGINT)
     AND ptp.FactionID IS NOT NULL
     AND TRY_CAST(q.SubmitDate AS TIMESTAMP) IS NOT NULL
     AND TRY_CAST(q.SubmitDate AS TIMESTAMP)
         BETWEEN TRY_CAST(ptp.StartDate AS TIMESTAMP)
             AND COALESCE(TRY_CAST(ptp.FinishDate AS TIMESTAMP), TIMESTAMP '9999-12-31')
),
fallback_pos AS (
    SELECT
        PersonID,
        CAST(KnessetNum AS BIGINT) AS KnessetNum,
        CAST(FactionID AS BIGINT) AS FactionID,
        FactionName,
        StartDate,
        FinishDate,
        ROW_NUMBER() OVER (
            PARTITION BY PersonID, CAST(KnessetNum AS BIGINT)
            ORDER BY
                TRY_CAST(StartDate AS TIMESTAMP) DESC NULLS LAST,
                PersonToPositionID DESC NULLS LAST
        ) AS rn
    FROM KNS_PersonToPosition
    WHERE FactionID IS NOT NULL
),
preferred_pos AS (
    SELECT
        q.QueryID,
        COALESCE(ep.FactionID, fp.FactionID) AS FactionID,
        COALESCE(ep.FactionName, fp.FactionName) AS FactionName,
        COALESCE(ep.StartDate, fp.StartDate) AS StartDate,
        COALESCE(ep.FinishDate, fp.FinishDate) AS FinishDate,
        CASE
            WHEN ep.FactionID IS NOT NULL THEN 'date_matched_person_position'
            WHEN fp.FactionID IS NOT NULL THEN 'fallback_latest_person_position'
            ELSE 'no_person_position_faction'
        END AS FactionMatchMethod
    FROM KNS_Query q
    LEFT JOIN exact_pos ep ON q.QueryID = ep.QueryID AND ep.rn = 1
    LEFT JOIN fallback_pos fp
      ON q.PersonID = fp.PersonID
     AND CAST(q.KnessetNum AS BIGINT) = fp.KnessetNum
     AND fp.rn = 1
)
SELECT
    q.QueryID,
    q.Number AS WarehouseNumber,
    CAST(q.KnessetNum AS BIGINT) AS KnessetNum,
    q.Name AS QueryName,
    q.TypeID AS QueryTypeID,
    q.TypeDesc AS QueryType,
    q.StatusID AS QueryStatusID,
    s."Desc" AS QueryStatus,
    s.TypeDesc AS ItemType,
    q.PersonID AS SubmitterPersonID,
    p.FirstName AS SubmitterFirstName,
    p.LastName AS SubmitterLastName,
    TRIM(COALESCE(p.FirstName, '') || ' ' || COALESCE(p.LastName, '')) AS SubmitterFullName,
    p.GenderDesc AS SubmitterGender,
    p.Email AS SubmitterEmail,
    p.IsCurrent AS SubmitterIsCurrent,
    pp.FactionID AS SubmitterFactionID,
    pp.FactionName AS SubmitterFactionName,
    pp.FactionMatchMethod,
    f.Name AS RawFactionName,
    COALESCE(ufs.NewFactionName, f.Name, pp.FactionName) AS UnifiedFactionName,
    pp.StartDate AS FactionStartDate,
    pp.FinishDate AS FactionFinishDate,
    ufs.CoalitionStatus AS CoalitionStatusAtKnessetStart,
    ufs.DateJoinedCoalition,
    ufs.DateLeftCoalition,
    CASE
        WHEN ufs.CoalitionStatus = 'Coalition'
             AND ufs.DateLeftCoalition IS NOT NULL
             AND TRY_CAST(q.SubmitDate AS TIMESTAMP) >= CAST(ufs.DateLeftCoalition AS TIMESTAMP)
            THEN 'Opposition'
        WHEN ufs.CoalitionStatus = 'Opposition'
             AND ufs.DateJoinedCoalition IS NOT NULL
             AND TRY_CAST(q.SubmitDate AS TIMESTAMP) >= CAST(ufs.DateJoinedCoalition AS TIMESTAMP)
            THEN 'Coalition'
        ELSE COALESCE(ufs.CoalitionStatus, 'Unknown')
    END AS WarehouseCoalitionStatus,
    q.GovMinistryID AS MinistryID,
    gm.Name AS MinistryName,
    gm.IsActive AS MinistryIsActive,
    q.SubmitDate,
    q.ReplyMinisterDate,
    q.ReplyDatePlanned,
    q.LastUpdatedDate,
    uqc.MajorIL AS WarehouseMajorIL,
    uqc.MinorIL AS WarehouseMinorIL,
    uqc.MajorCAP AS WarehouseMajorCAP,
    uqc.MinorCAP AS WarehouseMinorCAP,
    uqc.Religion AS WarehouseReligion,
    uqc.Territories AS WarehouseTerritories,
    uqc.Source AS WarehouseCodingSource,
    uqc.ImportedAt AS CodingImportedAt
FROM KNS_Query q
LEFT JOIN KNS_Status s ON q.StatusID = s.StatusID
LEFT JOIN KNS_Person p ON q.PersonID = p.PersonID
LEFT JOIN preferred_pos pp ON q.QueryID = pp.QueryID
LEFT JOIN KNS_Faction f
  ON pp.FactionID = f.FactionID
 AND CAST(q.KnessetNum AS BIGINT) = CAST(f.KnessetNum AS BIGINT)
LEFT JOIN UserFactionCoalitionStatus ufs
  ON pp.FactionID = ufs.FactionID
 AND CAST(q.KnessetNum AS BIGINT) = CAST(ufs.KnessetNum AS BIGINT)
LEFT JOIN KNS_GovMinistry gm ON q.GovMinistryID = gm.GovMinistryID
LEFT JOIN UserQueryCoding uqc ON q.QueryID = uqc.QueryID
ORDER BY CAST(q.KnessetNum AS BIGINT), q.QueryID
"""


def first_nonempty(frame: pd.DataFrame, *cols: str) -> pd.Series:
    result = pd.Series(pd.NA, index=frame.index, dtype="object")
    for col in cols:
        if col not in frame:
            continue
        series = frame[col]
        series = series.mask(series.astype(str).str.strip().isin(["", "nan", "NaN", "<NA>"]))
        result = result.combine_first(series)
    return result


def excel_date_to_iso(value: object) -> object:
    if pd.isna(value) or value == "":
        return pd.NA
    if isinstance(value, (int, float)) and not pd.isna(value):
        parsed = pd.to_datetime(value, unit="D", origin="1899-12-30", errors="coerce")
    else:
        parsed = pd.to_datetime(value, errors="coerce")
    return pd.NA if pd.isna(parsed) else parsed.date().isoformat()


def duplicate_row_summary(row: pd.Series) -> str:
    return json.dumps(
        {
            "row": int(row["SourceSpreadsheetRowNumber"]),
            "coder": None if pd.isna(row.get("CODER")) else str(row.get("CODER")),
            "cap": None if pd.isna(row.get("CAP_Maj")) else f"{row.get('CAP_Maj')}/{row.get('Cap_Min')}",
            "majoril": None if pd.isna(row.get("majorIL")) else f"{row.get('majorIL')}/{row.get('minorIL')}",
            "coalition": None if pd.isna(row.get("Coalition")) else int(row.get("Coalition")),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def load_collected_queries(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["QueryID"])

    source = pd.read_excel(path)
    source = source.rename(
        columns={
            "id": "QueryID",
            "Knesset": "CollectedKnessetNum",
            "Number": "CollectedNumber",
            "ItemName": "CollectedQueryName",
            "Subtype": "CollectedQueryType",
            "Status": "CollectedStatus",
            "Presenter": "CollectedPresenter",
            "MinistryQuestioned": "CollectedMinistryName",
            "MinistryNumber": "CollectedMinistryNumber",
            "majorIL": "CollectedMajorIL",
            "minorIL": "CollectedMinorIL",
            "CAP_Maj": "CollectedMajorCAP",
            "Cap_Min": "CollectedMinorCAP",
        }
    )
    source["SourceSpreadsheetRowNumber"] = range(2, len(source) + 2)
    source["QueryID"] = pd.to_numeric(source["QueryID"], errors="coerce").astype("Int64")
    source["CollectedCoalitionStatus"] = source["Coalition"].map({0: "Opposition", 1: "Coalition"})
    source["CollectedTransferDateISO"] = source["TransferDate"].map(excel_date_to_iso)
    source["CollectedAnswerDateISO"] = source["AnswerDate"].map(excel_date_to_iso)

    conflict_cols = [
        "CollectedQueryName",
        "CollectedPresenter",
        "CollectedMinistryName",
        "CollectedStatus",
        "StopReason",
        "AnswerType",
        "CollectedMajorCAP",
        "CollectedMinorCAP",
        "CollectedMajorIL",
        "CollectedMinorIL",
        "Religion",
        "Territories",
        "Coalition",
        "session_id",
    ]

    def conflict_columns(group: pd.DataFrame) -> str:
        return ",".join(
            [
                col
                for col in conflict_cols
                if col in group and group[col].astype("string").fillna("<NA>").nunique() > 1
            ]
        )

    source["DuplicateSourceRowCount"] = source.groupby("QueryID", dropna=False)["QueryID"].transform(
        "size"
    )
    conflicts = source.groupby("QueryID", dropna=False).apply(
        conflict_columns, include_groups=False
    )
    summaries = source.groupby("QueryID", dropna=False).apply(
        lambda group: " | ".join(group.apply(duplicate_row_summary, axis=1)),
        include_groups=False,
    )
    source["DuplicateConflictColumns"] = source["QueryID"].map(conflicts).fillna("")
    source["DuplicateRowsSummary"] = source["QueryID"].map(summaries).fillna("")
    source["CanonicalRank"] = (
        source["CODER"].astype("string").str.casefold().eq("reliability").fillna(False).astype(int)
    )
    return (
        source.sort_values(["QueryID", "CanonicalRank", "SourceSpreadsheetRowNumber"])
        .drop_duplicates("QueryID", keep="first")
        .copy()
    )


def build_export(warehouse: pd.DataFrame, collected: pd.DataFrame) -> pd.DataFrame:
    merged = warehouse.merge(collected, on="QueryID", how="outer", suffixes=("", "_collected"))
    merged["RecordSource"] = "warehouse_only"
    merged.loc[merged["CollectedKnessetNum"].notna() & merged["KnessetNum"].isna(), "RecordSource"] = (
        "collected_spreadsheet_only"
    )
    merged.loc[
        merged["CollectedKnessetNum"].notna() & merged["KnessetNum"].notna(),
        "RecordSource",
    ] = "collected_spreadsheet_and_warehouse"

    out = pd.DataFrame(index=merged.index)
    out["QueryID"] = merged["QueryID"]
    out["KnessetNum"] = first_nonempty(merged, "CollectedKnessetNum", "KnessetNum")
    out["QueryNumber"] = first_nonempty(merged, "CollectedNumber", "WarehouseNumber")
    out["QueryName"] = first_nonempty(merged, "CollectedQueryName", "QueryName")
    out["ItemType"] = first_nonempty(merged, "ItemType", "ItemType_collected")
    out["QueryType"] = first_nonempty(merged, "CollectedQueryType", "QueryType")
    out["QueryTypeID"] = merged["QueryTypeID"]
    out["QueryStatus"] = first_nonempty(merged, "CollectedStatus", "QueryStatus")
    out["QueryStatusID"] = merged["QueryStatusID"]
    out["StopReason"] = merged.get("StopReason")
    out["AnswerType"] = merged.get("AnswerType")
    out["PresenterName"] = first_nonempty(merged, "CollectedPresenter", "SubmitterFullName")
    out["SubmitterPersonID"] = merged["SubmitterPersonID"]
    out["SubmitterFirstName"] = merged["SubmitterFirstName"]
    out["SubmitterLastName"] = merged["SubmitterLastName"]
    out["SubmitterGender"] = merged["SubmitterGender"]
    out["SubmitterEmail"] = merged["SubmitterEmail"]
    out["SubmitterIsCurrent"] = merged["SubmitterIsCurrent"]
    out["SubmitterFactionID"] = merged["SubmitterFactionID"]
    out["SubmitterFactionName"] = first_nonempty(
        merged, "UnifiedFactionName", "SubmitterFactionName", "RawFactionName"
    )
    out["RawFactionName"] = merged["RawFactionName"]
    out["FactionMatchMethod"] = merged["FactionMatchMethod"]
    out["FactionStartDate"] = merged["FactionStartDate"]
    out["FactionFinishDate"] = merged["FactionFinishDate"]

    out["CollectedCoalitionStatus"] = merged["CollectedCoalitionStatus"]
    out["CollectedCoalitionRaw"] = merged.get("Coalition")
    out["WarehouseCoalitionStatus"] = merged["WarehouseCoalitionStatus"]
    out["CoalitionStatusAtKnessetStart"] = merged["CoalitionStatusAtKnessetStart"]
    out["DateJoinedCoalition"] = merged["DateJoinedCoalition"]
    out["DateLeftCoalition"] = merged["DateLeftCoalition"]
    out["CoalitionStatusDisagreement"] = (
        out["CollectedCoalitionStatus"].notna()
        & out["WarehouseCoalitionStatus"].notna()
        & out["WarehouseCoalitionStatus"].ne("Unknown")
        & out["CollectedCoalitionStatus"].ne(out["WarehouseCoalitionStatus"])
    )
    out["CoalitionStatus"] = out["CollectedCoalitionStatus"].combine_first(
        out["WarehouseCoalitionStatus"].where(out["WarehouseCoalitionStatus"].ne("Unknown"))
    ).fillna("Unknown")
    out["CoalitionSource"] = "unknown_or_unmapped"
    out.loc[out["CollectedCoalitionStatus"].notna(), "CoalitionSource"] = "collected_spreadsheet"
    out.loc[
        out["CollectedCoalitionStatus"].isna()
        & out["WarehouseCoalitionStatus"].notna()
        & out["WarehouseCoalitionStatus"].ne("Unknown"),
        "CoalitionSource",
    ] = "warehouse_person_faction_join"

    out["CoalitionCaveat"] = ""
    out.loc[out["CoalitionStatusDisagreement"], "CoalitionCaveat"] = (
        "Collected spreadsheet coalition label disagrees with warehouse faction/date join; "
        "unified value uses collected spreadsheet."
    )
    out.loc[out["CoalitionStatus"].eq("Unknown"), "CoalitionCaveat"] = (
        "No reliable coalition/opposition label available from collected spreadsheet or warehouse join."
    )

    out["MinistryID"] = first_nonempty(merged, "MinistryID", "CollectedMinistryNumber")
    out["MinistryName"] = first_nonempty(merged, "CollectedMinistryName", "MinistryName")
    out["MinistryIsActive"] = merged["MinistryIsActive"]
    out["CollectedMinistryNumber"] = merged.get("CollectedMinistryNumber")
    out["SubmitDate"] = first_nonempty(merged, "SubmitDate", "CollectedTransferDateISO")
    out["CollectedTransferDateRaw"] = merged.get("TransferDate")
    out["CollectedTransferDateISO"] = merged.get("CollectedTransferDateISO")
    out["CollectedAnswerDateRaw"] = merged.get("AnswerDate")
    out["CollectedAnswerDateISO"] = merged.get("CollectedAnswerDateISO")
    out["ReplyMinisterDate"] = first_nonempty(merged, "ReplyMinisterDate", "CollectedAnswerDateISO")
    out["ReplyDatePlanned"] = merged["ReplyDatePlanned"]
    out["LastUpdatedDate"] = merged["LastUpdatedDate"]
    out["MajorIL"] = first_nonempty(merged, "CollectedMajorIL", "WarehouseMajorIL")
    out["MinorIL"] = first_nonempty(merged, "CollectedMinorIL", "WarehouseMinorIL")
    out["MajorCAP"] = first_nonempty(merged, "CollectedMajorCAP", "WarehouseMajorCAP")
    out["MinorCAP"] = first_nonempty(merged, "CollectedMinorCAP", "WarehouseMinorCAP")
    out["Religion"] = first_nonempty(merged, "Religion", "WarehouseReligion")
    out["Territories"] = first_nonempty(merged, "Territories", "WarehouseTerritories")
    out["CollectedDummy"] = merged.get("Dummy")
    out["Coder"] = merged.get("CODER")
    out["CodingSource"] = first_nonempty(merged, "WarehouseCodingSource", "CODER")
    out["CodingImportedAt"] = merged["CodingImportedAt"]
    out["Link"] = merged.get("Link")
    out["Notes"] = merged.get("Notes")
    out["SessionID"] = merged.get("session_id")
    out["RecordSource"] = merged["RecordSource"]
    out["SourceSpreadsheetRowNumber"] = merged.get("SourceSpreadsheetRowNumber")
    out["DuplicateSourceRowCount"] = pd.to_numeric(
        merged.get("DuplicateSourceRowCount"), errors="coerce"
    ).fillna(0)
    out["DuplicateConflictColumns"] = merged.get("DuplicateConflictColumns")
    out["DuplicateRowsSummary"] = merged.get("DuplicateRowsSummary")
    out["DuplicateCodingCaveat"] = ""
    duplicate_mask = out["DuplicateSourceRowCount"] > 1
    out.loc[duplicate_mask, "DuplicateCodingCaveat"] = (
        "Original coded spreadsheet had duplicate rows for this QueryID; one canonical row is shown."
    )
    conflict_mask = out["DuplicateConflictColumns"].fillna("").astype(str).ne("")
    out.loc[conflict_mask, "DuplicateCodingCaveat"] = (
        out.loc[conflict_mask, "DuplicateCodingCaveat"]
        + " Conflicting duplicate columns: "
        + out.loc[conflict_mask, "DuplicateConflictColumns"].fillna("").astype(str)
        + "."
    )
    out.loc[~duplicate_mask, ["DuplicateConflictColumns", "DuplicateRowsSummary"]] = pd.NA

    for col in (
        "QueryID",
        "KnessetNum",
        "QueryNumber",
        "QueryTypeID",
        "QueryStatusID",
        "SubmitterPersonID",
        "SubmitterFactionID",
        "MinistryID",
        "CollectedMinistryNumber",
        "SourceSpreadsheetRowNumber",
        "DuplicateSourceRowCount",
    ):
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")

    return out.sort_values(["KnessetNum", "QueryID"]).reset_index(drop=True)


def validate_export(frame: pd.DataFrame) -> None:
    checks = {
        "QueryID": int(frame["QueryID"].isna().sum()),
        "KnessetNum": int(frame["KnessetNum"].isna().sum()),
        "QueryName": int(frame["QueryName"].isna().sum()),
        "CoalitionStatus": int(frame["CoalitionStatus"].isna().sum()),
        "RecordSource": int(frame["RecordSource"].isna().sum()),
    }
    if any(checks.values()):
        raise ValueError(f"Missing required export fields: {checks}")
    duplicate_rows = int(frame.duplicated("QueryID", keep=False).sum())
    if duplicate_rows:
        raise ValueError(f"Export has {duplicate_rows} duplicate QueryID rows")
    duplicate_columns = int(frame.columns.duplicated().sum())
    if duplicate_columns:
        raise ValueError(f"Export has {duplicate_columns} duplicate column headers")
    bad_statuses = set(frame["CoalitionStatus"].dropna()) - {"Coalition", "Opposition", "Unknown"}
    if bad_statuses:
        raise ValueError(f"Unexpected CoalitionStatus values: {sorted(bad_statuses)}")


def write_summary(frame: pd.DataFrame, output_path: Path) -> None:
    rows = [
        ("output_rows", len(frame)),
        ("output_columns", len(frame.columns)),
        ("distinct_query_ids", int(frame["QueryID"].nunique(dropna=True))),
        ("duplicate_query_id_rows", int(frame.duplicated("QueryID", keep=False).sum())),
        ("exact_duplicate_column_headers", int(frame.columns.duplicated().sum())),
        ("rows_with_duplicate_coding_caveat", int(frame["DuplicateCodingCaveat"].fillna("").ne("").sum())),
        ("coalition_unknown_rows", int(frame["CoalitionStatus"].eq("Unknown").sum())),
        ("coalition_opposition_rows", int(frame["CoalitionStatus"].eq("Opposition").sum())),
        ("coalition_coalition_rows", int(frame["CoalitionStatus"].eq("Coalition").sum())),
        ("coalition_disagreement_rows", int(frame["CoalitionStatusDisagreement"].sum())),
    ]
    for key, value in frame["RecordSource"].value_counts(dropna=False).items():
        rows.append((f"record_source__{key}", int(value)))
    pd.DataFrame(rows, columns=["metric", "value"]).to_csv(
        output_path, index=False, encoding="utf-8-sig"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export sendable all-query CSV")
    parser.add_argument("--warehouse", type=Path, default=DEFAULT_DB)
    parser.add_argument("--coded-query-xlsx", type=Path, default=DEFAULT_CODED_QUERY_XLSX)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--summary-output",
        type=Path,
        default=None,
        help="Optional summary CSV path. Defaults to <output stem>_summary.csv.",
    )
    args = parser.parse_args()

    with duckdb.connect(str(args.warehouse), read_only=True) as con:
        warehouse = con.execute(WAREHOUSE_QUERY_SQL).fetchdf()
    collected = load_collected_queries(args.coded_query_xlsx)
    export = build_export(warehouse, collected)
    validate_export(export)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    export.to_csv(args.output, index=False, encoding="utf-8-sig")
    summary_output = args.summary_output or args.output.with_name(f"{args.output.stem}_summary.csv")
    write_summary(export, summary_output)

    print(f"Exported {len(export):,} rows and {len(export.columns)} columns to {args.output}")
    print(f"Summary written to {summary_output}")


if __name__ == "__main__":
    main()

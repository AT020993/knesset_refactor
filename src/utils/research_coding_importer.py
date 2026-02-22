"""
Research coding data importer for parliamentary items.

Imports externally coded classification data (majorIL, minorIL, CAP codes, etc.)
from researcher-provided files into the dashboard's DuckDB database.

Supports 3 data types:
- Bills: matched on BillID (column BILLID in source)
- Parliamentary Queries: matched on QueryID (column id in source)
- Agenda Motions: matched on AgendaID via id2 (K19-20) or title matching (K23-24)

Tables created:
- UserBillCoding: Bill policy classification codes
- UserQueryCoding: Query policy classification codes
- UserAgendaCoding: Agenda policy classification codes with match metadata
"""

import difflib
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from backend.connection_manager import get_db_connection, safe_execute_query


# --- Data classes for structured results ---

@dataclass
class ImportResult:
    """Result of an import operation."""
    data_type: str
    total_rows_in_file: int
    rows_imported: int
    rows_updated: int
    rows_skipped_no_match: int
    rows_skipped_error: int
    errors: List[str]
    unmatched_items: Optional[pd.DataFrame] = None
    match_method_counts: Dict[str, int] = field(default_factory=dict)


@dataclass
class GapAnalysisResult:
    """Result of a gap analysis between coded data and dashboard data."""
    data_type: str
    total_in_dashboard: int
    total_coded: int
    coded_and_matched: int
    coverage_by_knesset: pd.DataFrame
    coded_not_in_dashboard: pd.DataFrame
    uncoded_in_dashboard: pd.DataFrame


# --- SQL table definitions ---

CREATE_BILL_CODING_SQL = """
CREATE TABLE IF NOT EXISTS UserBillCoding (
    BillID INTEGER PRIMARY KEY,
    MajorIL INTEGER,
    MinorIL INTEGER,
    MajorCAP INTEGER,
    MinorCAP INTEGER,
    StateReligion INTEGER,
    Territories INTEGER,
    Source VARCHAR DEFAULT 'researcher_import',
    ImportedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_QUERY_CODING_SQL = """
CREATE TABLE IF NOT EXISTS UserQueryCoding (
    QueryID INTEGER PRIMARY KEY,
    MajorIL INTEGER,
    MinorIL INTEGER,
    MajorCAP INTEGER,
    MinorCAP INTEGER,
    Religion INTEGER,
    Territories INTEGER,
    Source VARCHAR DEFAULT 'researcher_import',
    ImportedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_AGENDA_CODING_SQL = """
CREATE TABLE IF NOT EXISTS UserAgendaCoding (
    AgendaID INTEGER PRIMARY KEY,
    MajorIL INTEGER,
    MinorIL INTEGER,
    Religion INTEGER,
    Territories INTEGER,
    MatchMethod VARCHAR(50),
    MatchConfidence DECIMAL(3,2),
    Source VARCHAR DEFAULT 'researcher_import',
    ImportedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# Sentinel value used in some source files for "uncoded"
UNCODED_SENTINEL = -99


class ResearchCodingImporter:
    """Import and manage research coding classifications for parliamentary items."""

    # Column name mappings: source file column (lowercased) -> DB column
    # Each data type maps differently based on its source file structure.
    BILL_COLUMN_MAP = {
        "billid": "BillID",
        "majoril": "MajorIL",
        "minoril": "MinorIL",
        "majorcap": "MajorCAP",
        "minorcap": "MinorCAP",
        "statereligion": "StateReligion",
        "territories": "Territories",
    }

    QUERY_COLUMN_MAP = {
        "id": "QueryID",
        "majoril": "MajorIL",
        "minoril": "MinorIL",
        "cap_maj": "MajorCAP",
        "cap_min": "MinorCAP",
        "religion": "Religion",
        "territories": "Territories",
    }

    AGENDA_COLUMN_MAP = {
        "majoril": "MajorIL",
        "minoril": "MinorIL",
        "religion": "Religion",
        "territories": "Territories",
    }

    def __init__(self, db_path: Path, logger: Optional[logging.Logger] = None):
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)

    # --- Table management ---

    def ensure_tables_exist(self) -> bool:
        """Create coding tables if they don't exist."""
        try:
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as conn:
                conn.execute(CREATE_BILL_CODING_SQL)
                conn.execute(CREATE_QUERY_CODING_SQL)
                conn.execute(CREATE_AGENDA_CODING_SQL)
                self.logger.info("Research coding tables created/verified")
                return True
        except Exception as e:
            self.logger.error(f"Error creating coding tables: {e}", exc_info=True)
            return False

    # --- File reading ---

    def read_file(self, filepath: Path) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Read a CSV or Excel file with encoding fallback.

        Returns:
            (DataFrame, None) on success, (None, error_message) on failure.
        """
        filepath = Path(filepath)
        if not filepath.exists():
            return None, f"File not found: {filepath}"

        suffix = filepath.suffix.lower()
        try:
            if suffix in (".xlsx", ".xls"):
                df = pd.read_excel(filepath, engine="openpyxl" if suffix == ".xlsx" else "xlrd")
            elif suffix == ".csv":
                # Try encodings common with Hebrew data
                for encoding in ("utf-8-sig", "utf-8", "windows-1255", "iso-8859-8"):
                    try:
                        df = pd.read_csv(filepath, encoding=encoding)
                        break
                    except (UnicodeDecodeError, UnicodeError):
                        continue
                else:
                    return None, f"Could not decode CSV with any supported encoding"
            else:
                return None, f"Unsupported file format: {suffix}"

            self.logger.info(f"Read {len(df)} rows from {filepath.name} ({len(df.columns)} columns)")
            return df, None

        except Exception as e:
            return None, f"Error reading file: {e}"

    # --- Column mapping helpers ---

    def _normalize_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """Build a map from lowercased source columns to actual DataFrame column names."""
        return {col.lower().strip(): col for col in df.columns}

    def _map_columns(
        self,
        df: pd.DataFrame,
        column_map: Dict[str, str],
    ) -> Tuple[pd.DataFrame, List[str], List[str]]:
        """
        Map source columns to standardized DB column names.

        Returns:
            (mapped_df, mapped_columns, missing_columns)
        """
        col_lookup = self._normalize_columns(df)
        mapped = {}
        missing = []

        for source_key, db_col in column_map.items():
            actual_col = col_lookup.get(source_key)
            if actual_col is not None:
                mapped[db_col] = df[actual_col]
            else:
                missing.append(source_key)

        if not mapped:
            return pd.DataFrame(), list(column_map.values()), missing

        result = pd.DataFrame(mapped)
        return result, list(mapped.keys()), missing

    def _clean_coding_values(self, df: pd.DataFrame, int_columns: List[str]) -> pd.DataFrame:
        """
        Clean coding values: replace sentinel -99 with NaN, convert to nullable int.
        """
        df = df.copy()
        for col in int_columns:
            if col in df.columns:
                # Replace sentinel values
                df[col] = df[col].replace(UNCODED_SENTINEL, pd.NA)
                # Convert to nullable integer (allows NaN)
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        return df

    # --- Bill import ---

    def import_bill_coding(self, filepath: Path) -> ImportResult:
        """Import bill coding data from researcher file."""
        result = ImportResult(
            data_type="bills",
            total_rows_in_file=0,
            rows_imported=0,
            rows_updated=0,
            rows_skipped_no_match=0,
            rows_skipped_error=0,
            errors=[],
        )

        df, error = self.read_file(filepath)
        if error:
            result.errors.append(error)
            return result
        if df is None:
            result.errors.append("No data loaded from file")
            return result

        result.total_rows_in_file = len(df)

        # Map columns
        mapped_df, mapped_cols, missing = self._map_columns(df, self.BILL_COLUMN_MAP)
        if "BillID" not in mapped_cols:
            result.errors.append(f"Missing required column: BILLID. Found: {list(df.columns)}")
            return result
        if missing:
            self.logger.warning(f"Bills: missing optional columns: {missing}")

        # Clean coding values
        int_cols = [c for c in mapped_cols if c != "BillID"]
        mapped_df = self._clean_coding_values(mapped_df, int_cols)

        # Drop rows without BillID
        mapped_df = mapped_df.dropna(subset=["BillID"])
        mapped_df["BillID"] = mapped_df["BillID"].astype(int)

        # Remove exact duplicates on BillID — keep last (most updated)
        mapped_df = mapped_df.drop_duplicates(subset=["BillID"], keep="last")

        self.ensure_tables_exist()
        result = self._bulk_upsert(mapped_df, "UserBillCoding", "BillID", result)
        return result

    # --- Query import ---

    def import_query_coding(self, filepath: Path) -> ImportResult:
        """Import parliamentary query coding data from researcher file."""
        result = ImportResult(
            data_type="queries",
            total_rows_in_file=0,
            rows_imported=0,
            rows_updated=0,
            rows_skipped_no_match=0,
            rows_skipped_error=0,
            errors=[],
        )

        df, error = self.read_file(filepath)
        if error:
            result.errors.append(error)
            return result
        if df is None:
            result.errors.append("No data loaded from file")
            return result

        result.total_rows_in_file = len(df)

        mapped_df, mapped_cols, missing = self._map_columns(df, self.QUERY_COLUMN_MAP)
        if "QueryID" not in mapped_cols:
            result.errors.append(f"Missing required column: id. Found: {list(df.columns)}")
            return result
        if missing:
            self.logger.warning(f"Queries: missing optional columns: {missing}")

        int_cols = [c for c in mapped_cols if c != "QueryID"]
        mapped_df = self._clean_coding_values(mapped_df, int_cols)

        mapped_df = mapped_df.dropna(subset=["QueryID"])
        mapped_df["QueryID"] = mapped_df["QueryID"].astype(int)
        mapped_df = mapped_df.drop_duplicates(subset=["QueryID"], keep="last")

        self.ensure_tables_exist()
        result = self._bulk_upsert(mapped_df, "UserQueryCoding", "QueryID", result)
        return result

    # --- Agenda import (most complex — split matching strategy) ---

    def import_agenda_coding(self, filepath: Path) -> ImportResult:
        """
        Import agenda motion coding data with split matching strategy.

        K19-20: Match via id2 column → AgendaID (direct ID match)
        K23-24: Match via subject → KNS_Agenda.Name (tiered title matching)
        """
        result = ImportResult(
            data_type="agendas",
            total_rows_in_file=0,
            rows_imported=0,
            rows_updated=0,
            rows_skipped_no_match=0,
            rows_skipped_error=0,
            errors=[],
            match_method_counts={},
        )

        df, error = self.read_file(filepath)
        if error:
            result.errors.append(error)
            return result
        if df is None:
            result.errors.append("No data loaded from file")
            return result

        result.total_rows_in_file = len(df)

        # Normalize column names for lookup
        col_lookup = self._normalize_columns(df)

        # Identify knesset column
        knesset_col = col_lookup.get("knesset")
        if not knesset_col:
            result.errors.append(f"Missing required column: Knesset. Found: {list(df.columns)}")
            return result

        # Map coding columns
        mapped_df, mapped_cols, missing = self._map_columns(df, self.AGENDA_COLUMN_MAP)
        if not any(c in mapped_cols for c in ("MajorIL", "MinorIL")):
            result.errors.append("No coding columns found (need at least majoril or minoril)")
            return result

        # Add knesset number for splitting
        mapped_df["KnessetNum"] = df[knesset_col].astype(int)

        # Add source columns needed for matching
        id2_col = col_lookup.get("id2")
        subject_col = col_lookup.get("subject")
        if id2_col:
            mapped_df["_id2"] = df[id2_col]
        if subject_col:
            mapped_df["_subject"] = df[subject_col]

        # Clean coding values
        int_cols = [c for c in mapped_cols if c not in ("AgendaID",)]
        mapped_df = self._clean_coding_values(mapped_df, int_cols)

        # Split by knesset
        k19_20 = mapped_df[mapped_df["KnessetNum"].isin([19, 20])].copy()
        k23_24 = mapped_df[mapped_df["KnessetNum"].isin([23, 24])].copy()
        other_k = mapped_df[~mapped_df["KnessetNum"].isin([19, 20, 23, 24])].copy()

        all_matched = []
        all_unmatched = []

        # --- K19-20: Direct ID match via id2 ---
        if len(k19_20) > 0 and "_id2" in k19_20.columns:
            id_matched = k19_20.dropna(subset=["_id2"]).copy()
            id_matched["AgendaID"] = pd.to_numeric(id_matched["_id2"], errors="coerce").astype("Int64")
            id_matched = id_matched.dropna(subset=["AgendaID"])
            id_matched["AgendaID"] = id_matched["AgendaID"].astype(int)
            id_matched["MatchMethod"] = "id_direct"
            id_matched["MatchConfidence"] = 1.0

            # Validate against KNS_Agenda
            valid_ids = self._get_valid_agenda_ids([19, 20])
            if valid_ids is not None:
                matched_mask = id_matched["AgendaID"].isin(valid_ids)
                unmatched_k19_20 = id_matched[~matched_mask].copy()
                id_matched = id_matched[matched_mask]
                if len(unmatched_k19_20) > 0:
                    all_unmatched.append(unmatched_k19_20)
                    self.logger.info(
                        f"K19-20: {len(unmatched_k19_20)} id2 values not found in KNS_Agenda"
                    )

            # Also collect rows with no id2
            no_id2 = k19_20[k19_20.get("_id2", pd.Series(dtype=float)).isna()]
            if len(no_id2) > 0:
                all_unmatched.append(no_id2)

            all_matched.append(id_matched)
            result.match_method_counts["id_direct"] = len(id_matched)
            self.logger.info(f"K19-20: {len(id_matched)} matched by id2")

        # --- K23-24: Title matching ---
        if len(k23_24) > 0 and "_subject" in k23_24.columns:
            title_matched, title_unmatched = self._match_agenda_by_title(k23_24, [23, 24])
            if len(title_matched) > 0:
                all_matched.append(title_matched)
            if len(title_unmatched) > 0:
                all_unmatched.append(title_unmatched)

            # Aggregate match method counts
            if len(title_matched) > 0:
                for method, count in title_matched["MatchMethod"].value_counts().items():
                    method_key = str(method)
                    result.match_method_counts[method_key] = (
                        result.match_method_counts.get(method_key, 0) + int(count)
                    )

        # --- Other knessets: try id2 if available ---
        if len(other_k) > 0:
            if "_id2" in other_k.columns:
                ok_matched = other_k.dropna(subset=["_id2"]).copy()
                ok_matched["AgendaID"] = pd.to_numeric(ok_matched["_id2"], errors="coerce").astype("Int64")
                ok_matched = ok_matched.dropna(subset=["AgendaID"])
                ok_matched["AgendaID"] = ok_matched["AgendaID"].astype(int)
                ok_matched["MatchMethod"] = "id_direct"
                ok_matched["MatchConfidence"] = 1.0
                all_matched.append(ok_matched)
                result.match_method_counts["id_direct"] = (
                    result.match_method_counts.get("id_direct", 0) + len(ok_matched)
                )
            else:
                all_unmatched.append(other_k)

        # Combine results
        if all_matched:
            combined = pd.concat(all_matched, ignore_index=True)
            # Keep only DB columns
            db_cols = ["AgendaID"] + [c for c in mapped_cols if c != "AgendaID"] + [
                "MatchMethod", "MatchConfidence"
            ]
            db_cols = [c for c in db_cols if c in combined.columns]
            combined = combined[db_cols].drop_duplicates(subset=["AgendaID"], keep="first")

            self.ensure_tables_exist()
            result = self._bulk_upsert(combined, "UserAgendaCoding", "AgendaID", result)

        if all_unmatched:
            result.unmatched_items = pd.concat(all_unmatched, ignore_index=True)
            # Clean internal columns from unmatched output
            drop_cols = [c for c in result.unmatched_items.columns if c.startswith("_")]
            result.unmatched_items = result.unmatched_items.drop(columns=drop_cols, errors="ignore")
            result.rows_skipped_no_match = len(result.unmatched_items)

        return result

    def _get_valid_agenda_ids(self, knesset_nums: List[int]) -> Optional[set]:
        """Get set of valid AgendaIDs from KNS_Agenda for given knesset numbers."""
        try:
            placeholders = ", ".join("?" * len(knesset_nums))
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                rows = conn.execute(
                    f"SELECT AgendaID FROM KNS_Agenda WHERE KnessetNum IN ({placeholders})",
                    knesset_nums,
                ).fetchall()
                return {r[0] for r in rows}
        except Exception as e:
            self.logger.warning(f"Could not validate agenda IDs: {e}")
            return None

    def _match_agenda_by_title(
        self, df: pd.DataFrame, knesset_nums: List[int]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Match agenda items by title against KNS_Agenda.Name.

        Three matching tiers:
        1. Exact match (confidence 1.0)
        2. Normalized match — NFC unicode, stripped whitespace/punctuation (confidence 0.95)
        3. Fuzzy match — SequenceMatcher ratio >= 0.85 (confidence = ratio)

        Returns:
            (matched_df, unmatched_df)
        """
        try:
            placeholders = ", ".join("?" * len(knesset_nums))
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                agenda_df = safe_execute_query(
                    conn,
                    f"SELECT AgendaID, Name, KnessetNum FROM KNS_Agenda WHERE KnessetNum IN ({placeholders})",
                    self.logger,
                    params=knesset_nums,
                )
        except Exception as e:
            self.logger.error(f"Error loading agenda data for title matching: {e}")
            return pd.DataFrame(), df

        if agenda_df is None or agenda_df.empty:
            self.logger.warning("No agenda data found for title matching")
            return pd.DataFrame(), df

        # Build lookup structures
        # exact: name -> AgendaID
        exact_lookup: Dict[str, int] = {}
        # normalized: normalized_name -> AgendaID
        norm_lookup: Dict[str, int] = {}
        # For fuzzy: list of (normalized_name, AgendaID)
        fuzzy_candidates: List[Tuple[str, int]] = []

        for _, row in agenda_df.iterrows():
            aid = row["AgendaID"]
            name = str(row.get("Name", ""))
            if not name or name == "nan":
                continue
            exact_lookup[name] = aid
            norm = self._normalize_text(name)
            norm_lookup[norm] = aid
            fuzzy_candidates.append((norm, aid))

        matched_rows = []
        unmatched_rows = []

        for idx, row in df.iterrows():
            subject = str(row.get("_subject", ""))
            if not subject or subject == "nan":
                unmatched_rows.append(row)
                continue

            # Tier 1: Exact match
            if subject in exact_lookup:
                new_row = row.copy()
                new_row["AgendaID"] = exact_lookup[subject]
                new_row["MatchMethod"] = "title_exact"
                new_row["MatchConfidence"] = 1.0
                matched_rows.append(new_row)
                continue

            # Tier 2: Normalized match
            norm_subject = self._normalize_text(subject)
            if norm_subject in norm_lookup:
                new_row = row.copy()
                new_row["AgendaID"] = norm_lookup[norm_subject]
                new_row["MatchMethod"] = "title_normalized"
                new_row["MatchConfidence"] = 0.95
                matched_rows.append(new_row)
                continue

            # Tier 3: Fuzzy match (best ratio >= 0.85)
            best_ratio = 0.0
            best_aid = None
            for cand_norm, cand_aid in fuzzy_candidates:
                ratio = difflib.SequenceMatcher(None, norm_subject, cand_norm).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_aid = cand_aid

            if best_ratio >= 0.85 and best_aid is not None:
                new_row = row.copy()
                new_row["AgendaID"] = best_aid
                new_row["MatchMethod"] = "title_fuzzy"
                new_row["MatchConfidence"] = round(best_ratio, 2)
                matched_rows.append(new_row)
            else:
                unmatched_rows.append(row)

        matched_df = pd.DataFrame(matched_rows) if matched_rows else pd.DataFrame()
        unmatched_df = pd.DataFrame(unmatched_rows) if unmatched_rows else pd.DataFrame()

        self.logger.info(
            f"Title matching K{knesset_nums}: "
            f"{len(matched_rows)} matched, {len(unmatched_rows)} unmatched"
        )
        return matched_df, unmatched_df

    @staticmethod
    def _normalize_text(text: str) -> str:
        """Hebrew-aware text normalization for matching."""
        # NFC normalization (compose characters)
        text = unicodedata.normalize("NFC", text)
        # Strip whitespace
        text = text.strip()
        # Collapse multiple whitespace
        text = re.sub(r"\s+", " ", text)
        # Remove common punctuation that might differ
        text = re.sub(r'["\'\-–—:;.,!?()[\]{}]', "", text)
        # Strip again after punctuation removal
        text = text.strip()
        return text

    # --- Bulk upsert ---

    def _bulk_upsert(
        self,
        df: pd.DataFrame,
        table_name: str,
        pk_column: str,
        result: ImportResult,
    ) -> ImportResult:
        """
        Bulk insert/update data using DuckDB's register + INSERT ... ON CONFLICT.

        This is much faster than row-by-row for large datasets (10K+ rows).
        """
        if df.empty:
            return result

        try:
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as conn:
                # Count existing rows that will be updated
                existing_ids = set()
                try:
                    rows = conn.execute(
                        f"SELECT {pk_column} FROM {table_name}"
                    ).fetchall()
                    existing_ids = {r[0] for r in rows}
                except Exception:
                    pass  # Table might be empty

                updates = len(set(df[pk_column].values) & existing_ids)

                # Register DataFrame as virtual table
                staging_name = f"_staging_{table_name}"
                conn.register(staging_name, df)

                # Build column list (exclude Source and ImportedAt — use defaults)
                db_columns = [c for c in df.columns if c not in ("Source", "ImportedAt")]
                col_list = ", ".join(db_columns)
                # Build the ON CONFLICT update set
                update_cols = [c for c in db_columns if c != pk_column]
                update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
                update_set += ", ImportedAt = now()"

                sql = f"""
                    INSERT INTO {table_name} ({col_list})
                    SELECT {col_list} FROM {staging_name}
                    ON CONFLICT ({pk_column}) DO UPDATE SET {update_set}
                """
                conn.execute(sql)

                # Unregister staging table
                conn.unregister(staging_name)

                result.rows_imported = len(df) - updates
                result.rows_updated = updates
                self.logger.info(
                    f"{table_name}: {result.rows_imported} inserted, "
                    f"{result.rows_updated} updated"
                )

        except Exception as e:
            result.errors.append(f"Bulk upsert failed: {e}")
            self.logger.error(f"Bulk upsert error for {table_name}: {e}", exc_info=True)

        return result

    # --- Gap analysis ---

    def generate_gap_analysis(self, data_type: str) -> Optional[GapAnalysisResult]:
        """
        Generate gap analysis comparing coded data with dashboard data.

        Args:
            data_type: 'bills', 'queries', or 'agendas'

        Returns:
            GapAnalysisResult or None on error
        """
        config = {
            "bills": ("UserBillCoding", "BillID", "KNS_Bill", "BillID", "KnessetNum"),
            "queries": ("UserQueryCoding", "QueryID", "KNS_Query", "QueryID", "KnessetNum"),
            "agendas": ("UserAgendaCoding", "AgendaID", "KNS_Agenda", "AgendaID", "KnessetNum"),
        }

        if data_type not in config:
            self.logger.error(f"Unknown data type: {data_type}")
            return None

        coding_table, coding_pk, source_table, source_pk, knesset_col = config[data_type]

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                # Check if coding table exists and has data
                try:
                    total_coded_row = conn.execute(
                        f"SELECT COUNT(*) FROM {coding_table}"
                    ).fetchone()
                    total_coded = int(total_coded_row[0]) if total_coded_row else 0
                except Exception:
                    total_coded = 0

                total_dashboard_row = conn.execute(
                    f"SELECT COUNT(*) FROM {source_table}"
                ).fetchone()
                total_dashboard = int(total_dashboard_row[0]) if total_dashboard_row else 0

                if total_coded == 0:
                    return GapAnalysisResult(
                        data_type=data_type,
                        total_in_dashboard=total_dashboard,
                        total_coded=0,
                        coded_and_matched=0,
                        coverage_by_knesset=pd.DataFrame(),
                        coded_not_in_dashboard=pd.DataFrame(),
                        uncoded_in_dashboard=pd.DataFrame(),
                    )

                # Matched count
                coded_and_matched_row = conn.execute(f"""
                    SELECT COUNT(*) FROM {coding_table} c
                    INNER JOIN {source_table} s ON c.{coding_pk} = s.{source_pk}
                """).fetchone()
                coded_and_matched = (
                    int(coded_and_matched_row[0]) if coded_and_matched_row else 0
                )

                # Coverage by Knesset
                coverage_df = safe_execute_query(conn, f"""
                    SELECT
                        s.{knesset_col} AS KnessetNum,
                        COUNT(DISTINCT s.{source_pk}) AS TotalInDashboard,
                        COUNT(DISTINCT c.{coding_pk}) AS TotalCoded,
                        COALESCE(
                            ROUND(100.0 * COUNT(DISTINCT c.{coding_pk})
                                / NULLIF(COUNT(DISTINCT s.{source_pk}), 0), 1),
                            0.0
                        ) AS CoveragePct
                    FROM {source_table} s
                    LEFT JOIN {coding_table} c ON s.{source_pk} = c.{coding_pk}
                    GROUP BY s.{knesset_col}
                    ORDER BY s.{knesset_col}
                """, self.logger)

                # Coded but not in dashboard
                coded_not_in_dash = safe_execute_query(conn, f"""
                    SELECT c.{coding_pk}
                    FROM {coding_table} c
                    LEFT JOIN {source_table} s ON c.{coding_pk} = s.{source_pk}
                    WHERE s.{source_pk} IS NULL
                """, self.logger)

                # Uncoded in dashboard (per Knesset)
                uncoded_df = safe_execute_query(conn, f"""
                    SELECT
                        s.{knesset_col} AS KnessetNum,
                        COUNT(*) AS UncodedCount
                    FROM {source_table} s
                    LEFT JOIN {coding_table} c ON s.{source_pk} = c.{coding_pk}
                    WHERE c.{coding_pk} IS NULL
                    GROUP BY s.{knesset_col}
                    ORDER BY s.{knesset_col}
                """, self.logger)

                return GapAnalysisResult(
                    data_type=data_type,
                    total_in_dashboard=total_dashboard,
                    total_coded=total_coded,
                    coded_and_matched=coded_and_matched,
                    coverage_by_knesset=coverage_df if coverage_df is not None else pd.DataFrame(),
                    coded_not_in_dashboard=coded_not_in_dash if coded_not_in_dash is not None else pd.DataFrame(),
                    uncoded_in_dashboard=uncoded_df if uncoded_df is not None else pd.DataFrame(),
                )

        except Exception as e:
            self.logger.error(f"Gap analysis error for {data_type}: {e}", exc_info=True)
            return None

    def generate_uncoded_items_detail(self, data_type: str) -> Optional[pd.DataFrame]:
        """
        Generate detailed list of items that lack MajorIL/MinorIL coding.

        Returns a DataFrame with full item context (name, knesset, date, etc.)
        for items that either have no coding row or have NULL MajorIL.

        Args:
            data_type: 'bills', 'queries', or 'agendas'

        Returns:
            DataFrame with uncoded item details, or None on error
        """
        queries = {
            "bills": """
                SELECT
                    b.BillID,
                    b.KnessetNum,
                    b.Name AS BillName,
                    b.SubTypeDesc,
                    b.StatusID,
                    CASE WHEN b.PrivateNumber IS NOT NULL THEN 'Private' ELSE 'Government' END AS BillOrigin,
                    strftime(CAST(b.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d') AS LastUpdatedDate,
                    ubc.MajorIL AS ExistingMajorIL,
                    ubc.MinorIL AS ExistingMinorIL
                FROM KNS_Bill b
                LEFT JOIN UserBillCoding ubc ON b.BillID = ubc.BillID
                WHERE ubc.BillID IS NULL OR ubc.MajorIL IS NULL
                ORDER BY b.KnessetNum, b.BillID
            """,
            "queries": """
                SELECT
                    q.QueryID,
                    q.KnessetNum,
                    q.Name AS QueryName,
                    q.TypeDesc AS QueryType,
                    strftime(CAST(q.SubmitDate AS TIMESTAMP), '%Y-%m-%d') AS SubmitDate,
                    p.FirstName || ' ' || p.LastName AS SubmitterName,
                    m.Name AS MinistryName,
                    uqc.MajorIL AS ExistingMajorIL,
                    uqc.MinorIL AS ExistingMinorIL
                FROM KNS_Query q
                LEFT JOIN KNS_Person p ON q.PersonID = p.PersonID
                LEFT JOIN KNS_GovMinistry m ON q.GovMinistryID = m.GovMinistryID
                LEFT JOIN UserQueryCoding uqc ON q.QueryID = uqc.QueryID
                WHERE uqc.QueryID IS NULL OR uqc.MajorIL IS NULL
                ORDER BY q.KnessetNum, q.QueryID
            """,
            "agendas": """
                SELECT
                    a.AgendaID,
                    a.KnessetNum,
                    a.Name AS AgendaName,
                    a.ClassificationDesc,
                    strftime(CAST(a.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d') AS LastUpdatedDate,
                    p.FirstName || ' ' || p.LastName AS InitiatorName,
                    uac.MajorIL AS ExistingMajorIL,
                    uac.MinorIL AS ExistingMinorIL
                FROM KNS_Agenda a
                LEFT JOIN KNS_Person p ON a.InitiatorPersonID = p.PersonID
                LEFT JOIN UserAgendaCoding uac ON a.AgendaID = uac.AgendaID
                WHERE uac.AgendaID IS NULL OR uac.MajorIL IS NULL
                ORDER BY a.KnessetNum, a.AgendaID
            """,
        }

        if data_type not in queries:
            self.logger.error(f"Unknown data type for uncoded detail: {data_type}")
            return None

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                df = safe_execute_query(conn, queries[data_type], self.logger)
                if df is None:
                    return pd.DataFrame()
                self.logger.info(
                    f"Found {len(df)} uncoded {data_type} items"
                )
                return df

        except Exception as e:
            self.logger.error(f"Error generating uncoded detail for {data_type}: {e}", exc_info=True)
            return None

    # --- Statistics ---

    def get_coding_statistics(self) -> Dict[str, int]:
        """Get row counts for each coding table."""
        stats = {"bills": 0, "queries": 0, "agendas": 0}
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                tables_result = conn.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_name LIKE 'User%Coding'
                """).fetchall()
                existing = {r[0] for r in tables_result}

                for table, key in [
                    ("UserBillCoding", "bills"),
                    ("UserQueryCoding", "queries"),
                    ("UserAgendaCoding", "agendas"),
                ]:
                    if table in existing:
                        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                        count = int(row[0]) if row else 0
                        stats[key] = count

        except Exception as e:
            self.logger.error(f"Error getting coding statistics: {e}")

        return stats

    # --- Clear data ---

    def clear_coding_data(self, data_type: str) -> Tuple[bool, Optional[str]]:
        """
        Clear a specific coding table.

        Returns:
            (success, error_message)
        """
        table_map = {
            "bills": "UserBillCoding",
            "queries": "UserQueryCoding",
            "agendas": "UserAgendaCoding",
        }
        table = table_map.get(data_type)
        if not table:
            return False, f"Unknown data type: {data_type}"

        try:
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as conn:
                conn.execute(f"DELETE FROM {table}")
                self.logger.info(f"Cleared all data from {table}")
                return True, None
        except Exception as e:
            return False, f"Error clearing {table}: {e}"

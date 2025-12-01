"""
Topic classification import and management utility.

This module provides infrastructure for importing and managing topic/subject
classifications for parliamentary items (agendas, queries, bills).

The system supports:
- Topic taxonomy hierarchy (multi-level topics)
- Topic-to-item mappings with confidence scores
- Multiple data sources (manual, imported, ML-generated)
- CSV import for external topic data

Tables:
- UserTopicTaxonomy: Topic definitions and hierarchy
- UserAgendaTopics: Agenda-to-topic mappings
- UserQueryTopics: Query-to-topic mappings
- UserBillTopics: Bill-to-topic mappings
"""

import io
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from backend.connection_manager import get_db_connection, safe_execute_query


class TopicImporter:
    """Utility for importing and managing topic classifications."""

    # SQL for creating topic tables
    CREATE_TOPIC_TAXONOMY_SQL = """
    CREATE TABLE IF NOT EXISTS UserTopicTaxonomy (
        TopicID INTEGER PRIMARY KEY,
        TopicNameHE VARCHAR(255),
        TopicNameEN VARCHAR(255),
        ParentTopicID INTEGER,
        TopicLevel INTEGER DEFAULT 1,
        Description TEXT,
        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    CREATE_AGENDA_TOPICS_SQL = """
    CREATE TABLE IF NOT EXISTS UserAgendaTopics (
        AgendaID INTEGER,
        TopicID INTEGER,
        ConfidenceScore DECIMAL(3,2) DEFAULT 1.0,
        Source VARCHAR(50) DEFAULT 'imported',
        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (AgendaID, TopicID)
    )
    """

    CREATE_QUERY_TOPICS_SQL = """
    CREATE TABLE IF NOT EXISTS UserQueryTopics (
        QueryID INTEGER,
        TopicID INTEGER,
        ConfidenceScore DECIMAL(3,2) DEFAULT 1.0,
        Source VARCHAR(50) DEFAULT 'imported',
        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (QueryID, TopicID)
    )
    """

    CREATE_BILL_TOPICS_SQL = """
    CREATE TABLE IF NOT EXISTS UserBillTopics (
        BillID INTEGER,
        TopicID INTEGER,
        ConfidenceScore DECIMAL(3,2) DEFAULT 1.0,
        Source VARCHAR(50) DEFAULT 'imported',
        CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (BillID, TopicID)
    )
    """

    def __init__(self, db_path: Path, logger: Optional[logging.Logger] = None):
        """
        Initialize the topic importer.

        Args:
            db_path: Path to the DuckDB database
            logger: Optional logger instance
        """
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)

    def ensure_tables_exist(self) -> bool:
        """
        Ensure all topic tables exist in the database.

        Returns:
            True if tables were created or already exist
        """
        try:
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as con:
                con.execute(self.CREATE_TOPIC_TAXONOMY_SQL)
                con.execute(self.CREATE_AGENDA_TOPICS_SQL)
                con.execute(self.CREATE_QUERY_TOPICS_SQL)
                con.execute(self.CREATE_BILL_TOPICS_SQL)
                self.logger.info("Topic tables created/verified successfully")
                return True
        except Exception as e:
            self.logger.error(f"Error creating topic tables: {e}", exc_info=True)
            return False

    def import_taxonomy_from_csv(
        self,
        filepath: Path,
        encoding: str = "utf-8-sig"
    ) -> Tuple[int, List[str]]:
        """
        Import topic taxonomy from a CSV file.

        Expected CSV columns:
        - TopicID (required): Unique topic identifier
        - TopicNameHE (required): Hebrew topic name
        - TopicNameEN (optional): English topic name
        - ParentTopicID (optional): Parent topic for hierarchy
        - TopicLevel (optional): Level in hierarchy (1 = top)
        - Description (optional): Topic description

        Args:
            filepath: Path to the CSV file
            encoding: File encoding (default: utf-8-sig for Excel compatibility)

        Returns:
            Tuple of (rows_imported, list of error messages)
        """
        errors = []
        rows_imported = 0

        try:
            df = pd.read_csv(filepath, encoding=encoding)

            # Validate required columns
            required_cols = ["TopicID", "TopicNameHE"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                errors.append(f"Missing required columns: {missing_cols}")
                return 0, errors

            # Fill optional columns with defaults
            if "TopicNameEN" not in df.columns:
                df["TopicNameEN"] = None
            if "ParentTopicID" not in df.columns:
                df["ParentTopicID"] = None
            if "TopicLevel" not in df.columns:
                df["TopicLevel"] = 1
            if "Description" not in df.columns:
                df["Description"] = None

            # Ensure tables exist
            self.ensure_tables_exist()

            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as con:
                for _, row in df.iterrows():
                    try:
                        con.execute("""
                            INSERT INTO UserTopicTaxonomy
                            (TopicID, TopicNameHE, TopicNameEN, ParentTopicID, TopicLevel, Description)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT (TopicID) DO UPDATE SET
                                TopicNameHE = EXCLUDED.TopicNameHE,
                                TopicNameEN = EXCLUDED.TopicNameEN,
                                ParentTopicID = EXCLUDED.ParentTopicID,
                                TopicLevel = EXCLUDED.TopicLevel,
                                Description = EXCLUDED.Description,
                                UpdatedAt = CURRENT_TIMESTAMP
                        """, [
                            row["TopicID"],
                            row["TopicNameHE"],
                            row.get("TopicNameEN"),
                            row.get("ParentTopicID"),
                            row.get("TopicLevel", 1),
                            row.get("Description")
                        ])
                        rows_imported += 1
                    except Exception as e:
                        errors.append(f"Row {row['TopicID']}: {e}")

            self.logger.info(f"Imported {rows_imported} topics from {filepath}")

        except Exception as e:
            errors.append(f"Failed to import taxonomy: {e}")
            self.logger.error(f"Taxonomy import error: {e}", exc_info=True)

        return rows_imported, errors

    def import_agenda_topics(
        self,
        filepath: Path,
        encoding: str = "utf-8-sig",
        source: str = "imported"
    ) -> Tuple[int, List[str]]:
        """
        Import agenda-to-topic mappings from a CSV file.

        Expected CSV columns:
        - AgendaID (required): Agenda item identifier
        - TopicID (required): Topic identifier
        - ConfidenceScore (optional): 0.0-1.0, default 1.0
        - Source (optional): Data source label

        Args:
            filepath: Path to the CSV file
            encoding: File encoding
            source: Default source label if not in CSV

        Returns:
            Tuple of (rows_imported, list of error messages)
        """
        return self._import_item_topics(
            filepath, "UserAgendaTopics", "AgendaID", encoding, source
        )

    def import_query_topics(
        self,
        filepath: Path,
        encoding: str = "utf-8-sig",
        source: str = "imported"
    ) -> Tuple[int, List[str]]:
        """
        Import query-to-topic mappings from a CSV file.

        Expected CSV columns:
        - QueryID (required): Query identifier
        - TopicID (required): Topic identifier
        - ConfidenceScore (optional): 0.0-1.0, default 1.0
        - Source (optional): Data source label

        Args:
            filepath: Path to the CSV file
            encoding: File encoding
            source: Default source label if not in CSV

        Returns:
            Tuple of (rows_imported, list of error messages)
        """
        return self._import_item_topics(
            filepath, "UserQueryTopics", "QueryID", encoding, source
        )

    def import_bill_topics(
        self,
        filepath: Path,
        encoding: str = "utf-8-sig",
        source: str = "imported"
    ) -> Tuple[int, List[str]]:
        """
        Import bill-to-topic mappings from a CSV file.

        Expected CSV columns:
        - BillID (required): Bill identifier
        - TopicID (required): Topic identifier
        - ConfidenceScore (optional): 0.0-1.0, default 1.0
        - Source (optional): Data source label

        Args:
            filepath: Path to the CSV file
            encoding: File encoding
            source: Default source label if not in CSV

        Returns:
            Tuple of (rows_imported, list of error messages)
        """
        return self._import_item_topics(
            filepath, "UserBillTopics", "BillID", encoding, source
        )

    def _import_item_topics(
        self,
        filepath: Path,
        table_name: str,
        item_id_col: str,
        encoding: str,
        source: str
    ) -> Tuple[int, List[str]]:
        """
        Internal method to import item-to-topic mappings.

        Args:
            filepath: Path to the CSV file
            table_name: Target table name
            item_id_col: Name of the item ID column (AgendaID, QueryID, BillID)
            encoding: File encoding
            source: Default source label

        Returns:
            Tuple of (rows_imported, list of error messages)
        """
        errors = []
        rows_imported = 0

        try:
            df = pd.read_csv(filepath, encoding=encoding)

            # Validate required columns
            required_cols = [item_id_col, "TopicID"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                errors.append(f"Missing required columns: {missing_cols}")
                return 0, errors

            # Fill optional columns with defaults
            if "ConfidenceScore" not in df.columns:
                df["ConfidenceScore"] = 1.0
            if "Source" not in df.columns:
                df["Source"] = source

            # Ensure tables exist
            self.ensure_tables_exist()

            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as con:
                for _, row in df.iterrows():
                    try:
                        con.execute(f"""
                            INSERT INTO {table_name}
                            ({item_id_col}, TopicID, ConfidenceScore, Source)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT ({item_id_col}, TopicID) DO UPDATE SET
                                ConfidenceScore = EXCLUDED.ConfidenceScore,
                                Source = EXCLUDED.Source
                        """, [
                            row[item_id_col],
                            row["TopicID"],
                            row.get("ConfidenceScore", 1.0),
                            row.get("Source", source)
                        ])
                        rows_imported += 1
                    except Exception as e:
                        errors.append(f"Row {row[item_id_col]}-{row['TopicID']}: {e}")

            self.logger.info(f"Imported {rows_imported} mappings to {table_name} from {filepath}")

        except Exception as e:
            errors.append(f"Failed to import mappings: {e}")
            self.logger.error(f"Topic mapping import error: {e}", exc_info=True)

        return rows_imported, errors

    def get_topic_taxonomy(self) -> pd.DataFrame:
        """
        Get all topics from the taxonomy.

        Returns:
            DataFrame with topic taxonomy
        """
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                return safe_execute_query(con, """
                    SELECT
                        t.TopicID,
                        t.TopicNameHE,
                        t.TopicNameEN,
                        t.ParentTopicID,
                        t.TopicLevel,
                        t.Description,
                        p.TopicNameHE AS ParentTopicNameHE,
                        (SELECT COUNT(*) FROM UserAgendaTopics WHERE TopicID = t.TopicID) AS AgendaCount,
                        (SELECT COUNT(*) FROM UserQueryTopics WHERE TopicID = t.TopicID) AS QueryCount,
                        (SELECT COUNT(*) FROM UserBillTopics WHERE TopicID = t.TopicID) AS BillCount
                    FROM UserTopicTaxonomy t
                    LEFT JOIN UserTopicTaxonomy p ON t.ParentTopicID = p.TopicID
                    ORDER BY t.TopicLevel, t.TopicNameHE
                """, self.logger)
        except Exception as e:
            self.logger.error(f"Error fetching topic taxonomy: {e}")
            return pd.DataFrame()

    def get_topics_for_agenda(self, agenda_id: int) -> pd.DataFrame:
        """
        Get topics assigned to an agenda item.

        Args:
            agenda_id: The agenda ID

        Returns:
            DataFrame with topic assignments
        """
        return self._get_topics_for_item("UserAgendaTopics", "AgendaID", agenda_id)

    def get_topics_for_query(self, query_id: int) -> pd.DataFrame:
        """
        Get topics assigned to a query.

        Args:
            query_id: The query ID

        Returns:
            DataFrame with topic assignments
        """
        return self._get_topics_for_item("UserQueryTopics", "QueryID", query_id)

    def get_topics_for_bill(self, bill_id: int) -> pd.DataFrame:
        """
        Get topics assigned to a bill.

        Args:
            bill_id: The bill ID

        Returns:
            DataFrame with topic assignments
        """
        return self._get_topics_for_item("UserBillTopics", "BillID", bill_id)

    def _get_topics_for_item(
        self,
        table_name: str,
        item_id_col: str,
        item_id: int
    ) -> pd.DataFrame:
        """
        Internal method to get topics for an item.

        Args:
            table_name: Source mapping table
            item_id_col: Name of the item ID column
            item_id: The item ID

        Returns:
            DataFrame with topic assignments
        """
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                return safe_execute_query(con, f"""
                    SELECT
                        m.{item_id_col},
                        m.TopicID,
                        t.TopicNameHE,
                        t.TopicNameEN,
                        m.ConfidenceScore,
                        m.Source
                    FROM {table_name} m
                    JOIN UserTopicTaxonomy t ON m.TopicID = t.TopicID
                    WHERE m.{item_id_col} = ?
                    ORDER BY m.ConfidenceScore DESC
                """, self.logger, params=[item_id])
        except Exception as e:
            self.logger.error(f"Error fetching topics for {item_id_col}={item_id}: {e}")
            return pd.DataFrame()

    def get_topic_statistics(self) -> Dict[str, int]:
        """
        Get statistics about topic data.

        Returns:
            Dictionary with counts for each table
        """
        stats = {
            "topic_count": 0,
            "agenda_mappings": 0,
            "query_mappings": 0,
            "bill_mappings": 0,
        }

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                # Check if tables exist first
                tables = con.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_name LIKE 'User%Topic%'
                """).fetchall()
                table_names = [t[0] for t in tables]

                if "UserTopicTaxonomy" in table_names:
                    result = con.execute("SELECT COUNT(*) FROM UserTopicTaxonomy").fetchone()
                    stats["topic_count"] = result[0] if result else 0

                if "UserAgendaTopics" in table_names:
                    result = con.execute("SELECT COUNT(*) FROM UserAgendaTopics").fetchone()
                    stats["agenda_mappings"] = result[0] if result else 0

                if "UserQueryTopics" in table_names:
                    result = con.execute("SELECT COUNT(*) FROM UserQueryTopics").fetchone()
                    stats["query_mappings"] = result[0] if result else 0

                if "UserBillTopics" in table_names:
                    result = con.execute("SELECT COUNT(*) FROM UserBillTopics").fetchone()
                    stats["bill_mappings"] = result[0] if result else 0

        except Exception as e:
            self.logger.error(f"Error getting topic statistics: {e}")

        return stats

    def clear_all_topic_data(self) -> bool:
        """
        Clear all topic data (taxonomy and mappings).

        Returns:
            True if successful
        """
        try:
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as con:
                con.execute("DELETE FROM UserAgendaTopics")
                con.execute("DELETE FROM UserQueryTopics")
                con.execute("DELETE FROM UserBillTopics")
                con.execute("DELETE FROM UserTopicTaxonomy")
                self.logger.info("All topic data cleared")
                return True
        except Exception as e:
            self.logger.error(f"Error clearing topic data: {e}")
            return False

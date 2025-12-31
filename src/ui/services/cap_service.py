"""
CAP (Comparative Agendas Project) Annotation Service

This module provides services for the Democratic Erosion bill annotation system,
allowing researchers to classify bills according to a specialized codebook
for tracking legislation that affects democratic institutions and rights.

The codebook uses:
- Major categories (1=Government Institutions, 2=Civil Institutions, 3=Rights)
- Minor categories (101-108, 201-204, 301-306)
- Direction coding (+1=Strengthening, -1=Weakening, 0=Other)
"""

import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import pandas as pd

from backend.connection_manager import get_db_connection, safe_execute_query


class CAPAnnotationService:
    """Service for managing CAP/Democratic Erosion bill annotations."""
    
    TAXONOMY_FILE = Path("data/taxonomies/democratic_erosion_codebook.csv")
    
    # Direction codes
    DIRECTION_STRENGTHENING = 1
    DIRECTION_WEAKENING = -1
    DIRECTION_NEUTRAL = 0
    
    DIRECTION_LABELS = {
        1: ("הרחבה/חיזוק", "Strengthening/Expansion"),
        -1: ("צמצום/פגיעה", "Weakening/Restriction"),
        0: ("אחר", "Other/Neutral"),
    }
    
    def __init__(self, db_path: Path, logger_obj: Optional[logging.Logger] = None):
        """Initialize the CAP annotation service."""
        self.db_path = db_path
        self.logger = logger_obj or logging.getLogger(__name__)
        self._taxonomy_cache: Optional[pd.DataFrame] = None
    
    def ensure_tables_exist(self) -> bool:
        """
        Create the CAP annotation tables if they don't exist.
        
        Tables created:
        - UserCAPTaxonomy: The codebook taxonomy
        - UserBillCAP: Bill annotations
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as conn:
                # Create taxonomy table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS UserCAPTaxonomy (
                        MajorCode INTEGER NOT NULL,
                        MajorTopic_HE VARCHAR NOT NULL,
                        MajorTopic_EN VARCHAR NOT NULL,
                        MinorCode INTEGER PRIMARY KEY,
                        MinorTopic_HE VARCHAR NOT NULL,
                        MinorTopic_EN VARCHAR NOT NULL,
                        Description_HE VARCHAR,
                        Examples_HE VARCHAR
                    )
                """)
                
                # Create bill annotations table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS UserBillCAP (
                        BillID INTEGER PRIMARY KEY,
                        CAPMinorCode INTEGER NOT NULL,
                        Direction INTEGER NOT NULL DEFAULT 0,
                        AssignedBy VARCHAR NOT NULL,
                        AssignedDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        Confidence VARCHAR DEFAULT 'Medium',
                        Notes VARCHAR,
                        Source VARCHAR DEFAULT 'Database',
                        SubmissionDate VARCHAR,
                        FOREIGN KEY (CAPMinorCode) REFERENCES UserCAPTaxonomy(MinorCode)
                    )
                """)
                
                self.logger.info("CAP annotation tables created/verified successfully")
                return True
                
        except Exception as e:
            self.logger.error(f"Error creating CAP tables: {e}", exc_info=True)
            return False
    
    def load_taxonomy_from_csv(self) -> bool:
        """
        Load the taxonomy from CSV file into the database.

        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.TAXONOMY_FILE.exists():
                self.logger.error(f"Taxonomy file not found: {self.TAXONOMY_FILE}")
                return False

            # Read CSV
            df = pd.read_csv(self.TAXONOMY_FILE, encoding='utf-8')

            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as conn:
                # Use INSERT OR REPLACE to avoid foreign key issues with existing annotations
                for _, row in df.iterrows():
                    conn.execute("""
                        INSERT OR REPLACE INTO UserCAPTaxonomy
                        (MajorCode, MajorTopic_HE, MajorTopic_EN, MinorCode,
                         MinorTopic_HE, MinorTopic_EN, Description_HE, Examples_HE)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        row['MajorCode'],
                        row['MajorTopic_HE'],
                        row['MajorTopic_EN'],
                        row['MinorCode'],
                        row['MinorTopic_HE'],
                        row['MinorTopic_EN'],
                        row.get('Description_HE', ''),
                        row.get('Examples_HE', '')
                    ])

                self.logger.info(f"Loaded {len(df)} taxonomy entries from CSV")
                self._taxonomy_cache = None  # Clear cache
                return True

        except Exception as e:
            self.logger.error(f"Error loading taxonomy from CSV: {e}", exc_info=True)
            return False
    
    def get_taxonomy(self) -> pd.DataFrame:
        """
        Get the full taxonomy as a DataFrame.
        
        Returns:
            DataFrame with taxonomy entries
        """
        if self._taxonomy_cache is not None:
            return self._taxonomy_cache
        
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                result = safe_execute_query(conn, """
                    SELECT * FROM UserCAPTaxonomy 
                    ORDER BY MajorCode, MinorCode
                """, self.logger)
                
                if result is not None:
                    self._taxonomy_cache = result
                    return result
                    
        except Exception as e:
            self.logger.error(f"Error getting taxonomy: {e}", exc_info=True)
        
        return pd.DataFrame()
    
    def get_major_categories(self) -> List[Dict[str, Any]]:
        """Get list of major categories."""
        taxonomy = self.get_taxonomy()
        if taxonomy.empty:
            return []
        
        majors = taxonomy.groupby(['MajorCode', 'MajorTopic_HE', 'MajorTopic_EN']).first().reset_index()
        return majors[['MajorCode', 'MajorTopic_HE', 'MajorTopic_EN']].to_dict('records')
    
    def get_minor_categories(self, major_code: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get list of minor categories, optionally filtered by major code."""
        taxonomy = self.get_taxonomy()
        if taxonomy.empty:
            return []
        
        if major_code is not None:
            taxonomy = taxonomy[taxonomy['MajorCode'] == major_code]
        
        return taxonomy.to_dict('records')
    
    def get_uncoded_bills(
        self, 
        knesset_num: Optional[int] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Get bills that haven't been coded yet.
        
        Args:
            knesset_num: Filter by Knesset number (optional)
            limit: Maximum number of bills to return
            
        Returns:
            DataFrame with uncoded bills
        """
        try:
            query = """
                SELECT 
                    B.BillID,
                    B.KnessetNum,
                    B.Name AS BillName,
                    B.SubTypeDesc AS BillType,
                    B.PrivateNumber,
                    strftime(CAST(B.PublicationDate AS TIMESTAMP), '%Y-%m-%d') AS PublicationDate,
                    strftime(CAST(B.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d') AS LastUpdated,
                    S."Desc" AS StatusDesc,
                    'https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid=' 
                        || CAST(B.BillID AS VARCHAR) AS BillURL
                FROM KNS_Bill B
                LEFT JOIN KNS_Status S ON B.StatusID = S.StatusID
                LEFT JOIN UserBillCAP CAP ON B.BillID = CAP.BillID
                WHERE CAP.BillID IS NULL
            """
            
            params = []
            if knesset_num is not None:
                query += " AND B.KnessetNum = ?"
                params.append(knesset_num)
            
            query += f" ORDER BY B.KnessetNum DESC, B.BillID DESC LIMIT {limit}"
            
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                if params:
                    result = conn.execute(query, params).fetchdf()
                else:
                    result = conn.execute(query).fetchdf()
                return result
                
        except Exception as e:
            self.logger.error(f"Error getting uncoded bills: {e}", exc_info=True)
            return pd.DataFrame()
    
    def get_coded_bills(
        self,
        knesset_num: Optional[int] = None,
        cap_code: Optional[int] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Get bills that have been coded.

        Args:
            knesset_num: Filter by Knesset number (optional)
            cap_code: Filter by CAP code (optional)
            limit: Maximum number of bills to return

        Returns:
            DataFrame with coded bills
        """
        try:
            # Use LEFT JOIN to include API-sourced bills not in local database
            query = """
                SELECT
                    CAP.BillID,
                    COALESCE(B.KnessetNum, 0) AS KnessetNum,
                    COALESCE(B.Name, 'Bill #' || CAST(CAP.BillID AS VARCHAR) || ' (from API)') AS BillName,
                    COALESCE(B.SubTypeDesc, CAP.Source) AS BillType,
                    CAP.CAPMinorCode,
                    T.MinorTopic_HE AS CAPTopic_HE,
                    T.MinorTopic_EN AS CAPTopic_EN,
                    T.MajorTopic_HE AS CAPMajorTopic_HE,
                    CAP.Direction,
                    CAP.AssignedBy,
                    strftime(CAP.AssignedDate, '%Y-%m-%d %H:%M') AS AssignedDate,
                    CAP.Confidence,
                    CAP.Notes,
                    CAP.SubmissionDate,
                    CAP.Source,
                    'https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid='
                        || CAST(CAP.BillID AS VARCHAR) AS BillURL
                FROM UserBillCAP CAP
                LEFT JOIN KNS_Bill B ON CAP.BillID = B.BillID
                JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
            """

            conditions = []
            params = []

            if knesset_num is not None:
                conditions.append("COALESCE(B.KnessetNum, 0) = ?")
                params.append(knesset_num)

            if cap_code is not None:
                conditions.append("CAP.CAPMinorCode = ?")
                params.append(cap_code)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += f" ORDER BY CAP.AssignedDate DESC LIMIT {limit}"

            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                if params:
                    result = conn.execute(query, params).fetchdf()
                else:
                    result = conn.execute(query).fetchdf()
                return result

        except Exception as e:
            self.logger.error(f"Error getting coded bills: {e}", exc_info=True)
            return pd.DataFrame()

    def get_annotation_by_bill_id(self, bill_id: int) -> Optional[Dict[str, Any]]:
        """
        Get the full annotation details for a specific bill.

        Args:
            bill_id: The bill ID

        Returns:
            Dictionary with annotation details or None if not found
        """
        try:
            query = """
                SELECT
                    CAP.BillID,
                    CAP.CAPMinorCode,
                    CAP.Direction,
                    CAP.AssignedBy,
                    strftime(CAP.AssignedDate, '%Y-%m-%d %H:%M') AS AssignedDate,
                    CAP.Confidence,
                    CAP.Notes,
                    CAP.Source,
                    CAP.SubmissionDate,
                    T.MajorCode,
                    T.MajorTopic_HE,
                    T.MajorTopic_EN,
                    T.MinorTopic_HE,
                    T.MinorTopic_EN
                FROM UserBillCAP CAP
                JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
                WHERE CAP.BillID = ?
            """

            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                result = conn.execute(query, [bill_id]).fetchdf()
                if not result.empty:
                    return result.iloc[0].to_dict()
                return None

        except Exception as e:
            self.logger.error(f"Error getting annotation for bill {bill_id}: {e}", exc_info=True)
            return None

    def save_annotation(
        self,
        bill_id: int,
        cap_minor_code: int,
        direction: int,
        assigned_by: str,
        confidence: str = "Medium",
        notes: str = "",
        source: str = "Database",
        submission_date: str = ""
    ) -> bool:
        """
        Save a bill annotation.
        
        Args:
            bill_id: The bill ID to annotate
            cap_minor_code: The CAP minor code (e.g., 101, 201, 301)
            direction: Direction code (+1, -1, or 0)
            assigned_by: Name of the researcher
            confidence: Confidence level (High, Medium, Low)
            notes: Optional notes
            source: Source of the bill (Database or API)
            submission_date: Bill submission date (required if direction is +1 or -1)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as conn:
                # Check if annotation already exists
                existing = conn.execute(
                    "SELECT BillID FROM UserBillCAP WHERE BillID = ?", 
                    [bill_id]
                ).fetchone()
                
                if existing:
                    # Update existing annotation
                    conn.execute("""
                        UPDATE UserBillCAP SET
                            CAPMinorCode = ?,
                            Direction = ?,
                            AssignedBy = ?,
                            AssignedDate = CURRENT_TIMESTAMP,
                            Confidence = ?,
                            Notes = ?,
                            Source = ?,
                            SubmissionDate = ?
                        WHERE BillID = ?
                    """, [cap_minor_code, direction, assigned_by, confidence, 
                          notes, source, submission_date, bill_id])
                    self.logger.info(f"Updated annotation for bill {bill_id}")
                else:
                    # Insert new annotation
                    conn.execute("""
                        INSERT INTO UserBillCAP 
                        (BillID, CAPMinorCode, Direction, AssignedBy, Confidence, Notes, Source, SubmissionDate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, [bill_id, cap_minor_code, direction, assigned_by, 
                          confidence, notes, source, submission_date])
                    self.logger.info(f"Created annotation for bill {bill_id}")
                
                return True
                
        except Exception as e:
            self.logger.error(f"Error saving annotation for bill {bill_id}: {e}", exc_info=True)
            return False
    
    def delete_annotation(self, bill_id: int) -> bool:
        """
        Delete an annotation for a bill.
        
        Args:
            bill_id: The bill ID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with get_db_connection(self.db_path, read_only=False, logger_obj=self.logger) as conn:
                conn.execute("DELETE FROM UserBillCAP WHERE BillID = ?", [bill_id])
                self.logger.info(f"Deleted annotation for bill {bill_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error deleting annotation for bill {bill_id}: {e}", exc_info=True)
            return False
    
    def get_annotation_stats(self) -> Dict[str, Any]:
        """
        Get statistics about annotations.
        
        Returns:
            Dictionary with annotation statistics
        """
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                stats = {}
                
                # Total coded
                result = conn.execute("SELECT COUNT(*) as count FROM UserBillCAP").fetchone()
                stats['total_coded'] = result[0] if result else 0
                
                # Total bills
                result = conn.execute("SELECT COUNT(*) as count FROM KNS_Bill").fetchone()
                stats['total_bills'] = result[0] if result else 0
                
                # By major category
                by_major = conn.execute("""
                    SELECT T.MajorTopic_HE, COUNT(*) as count
                    FROM UserBillCAP CAP
                    JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
                    GROUP BY T.MajorCode, T.MajorTopic_HE
                    ORDER BY T.MajorCode
                """).fetchdf()
                stats['by_major_category'] = by_major.to_dict('records')
                
                # By direction
                by_direction = conn.execute("""
                    SELECT Direction, COUNT(*) as count
                    FROM UserBillCAP
                    GROUP BY Direction
                """).fetchdf()
                stats['by_direction'] = by_direction.to_dict('records')
                
                # By Knesset
                by_knesset = conn.execute("""
                    SELECT B.KnessetNum, COUNT(*) as count
                    FROM UserBillCAP CAP
                    JOIN KNS_Bill B ON CAP.BillID = B.BillID
                    GROUP BY B.KnessetNum
                    ORDER BY B.KnessetNum DESC
                """).fetchdf()
                stats['by_knesset'] = by_knesset.to_dict('records')
                
                return stats

        except Exception as e:
            self.logger.error(f"Error getting annotation stats: {e}", exc_info=True)
            return {}

    def get_bills_not_in_database(
        self,
        api_bills: pd.DataFrame,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Filter API bills to only those not in the local database.

        Args:
            api_bills: DataFrame of bills from API
            limit: Maximum number of results

        Returns:
            DataFrame of bills not in local database
        """
        if api_bills.empty:
            return api_bills

        try:
            bill_ids = api_bills['BillID'].tolist()
            placeholders = ','.join(['?' for _ in bill_ids])

            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                # Get bills that ARE in database
                query = f"SELECT BillID FROM KNS_Bill WHERE BillID IN ({placeholders})"
                existing = conn.execute(query, bill_ids).fetchdf()
                existing_ids = set(existing['BillID'].tolist()) if not existing.empty else set()

                # Filter to bills NOT in database
                not_in_db = api_bills[~api_bills['BillID'].isin(existing_ids)]

                # Also exclude already coded bills
                query2 = f"SELECT BillID FROM UserBillCAP WHERE BillID IN ({placeholders})"
                coded = conn.execute(query2, bill_ids).fetchdf()
                coded_ids = set(coded['BillID'].tolist()) if not coded.empty else set()

                not_in_db = not_in_db[~not_in_db['BillID'].isin(coded_ids)]

                return not_in_db.head(limit)

        except Exception as e:
            self.logger.error(f"Error filtering bills: {e}", exc_info=True)
            return api_bills.head(limit)

    def export_annotations(self, output_path: Path) -> bool:
        """
        Export all annotations to CSV.
        
        Args:
            output_path: Path to save the CSV file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = """
                SELECT 
                    B.BillID,
                    B.KnessetNum,
                    B.Name AS BillName,
                    B.SubTypeDesc AS BillType,
                    B.PrivateNumber,
                    strftime(CAST(B.PublicationDate AS TIMESTAMP), '%Y-%m-%d') AS PublicationDate,
                    CAP.CAPMinorCode,
                    T.MajorCode AS CAPMajorCode,
                    T.MajorTopic_HE AS CAPMajorTopic_HE,
                    T.MajorTopic_EN AS CAPMajorTopic_EN,
                    T.MinorTopic_HE AS CAPMinorTopic_HE,
                    T.MinorTopic_EN AS CAPMinorTopic_EN,
                    CAP.Direction,
                    CASE CAP.Direction 
                        WHEN 1 THEN 'הרחבה/חיזוק'
                        WHEN -1 THEN 'צמצום/פגיעה'
                        ELSE 'אחר'
                    END AS Direction_HE,
                    CAP.AssignedBy,
                    strftime(CAP.AssignedDate, '%Y-%m-%d %H:%M:%S') AS AssignedDate,
                    CAP.Confidence,
                    CAP.Notes,
                    CAP.Source,
                    CAP.SubmissionDate
                FROM UserBillCAP CAP
                JOIN KNS_Bill B ON CAP.BillID = B.BillID
                JOIN UserCAPTaxonomy T ON CAP.CAPMinorCode = T.MinorCode
                ORDER BY B.KnessetNum DESC, B.BillID DESC
            """
            
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as conn:
                result = conn.execute(query).fetchdf()
                result.to_csv(output_path, index=False, encoding='utf-8-sig')
                self.logger.info(f"Exported {len(result)} annotations to {output_path}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error exporting annotations: {e}", exc_info=True)
            return False


def get_cap_service(db_path: Path, logger_obj: Optional[logging.Logger] = None) -> CAPAnnotationService:
    """Factory function to get a CAP annotation service instance."""
    return CAPAnnotationService(db_path, logger_obj)

"""
Export verification utility for ensuring data integrity between source and exported files.

This module provides functionality to verify that data exported from the application
matches the source data in terms of row counts, column structure, and content.
"""

import hashlib
import io
import logging
from typing import Optional, Dict, Any

import pandas as pd


class ExportVerifier:
    """Utility for verifying exported data matches source data."""

    def __init__(self, logger_obj: Optional[logging.Logger] = None):
        """
        Initialize the export verifier.

        Args:
            logger_obj: Logger instance for error reporting
        """
        self.logger = logger_obj or logging.getLogger(__name__)

    def generate_checksum(self, df: pd.DataFrame) -> str:
        """
        Generate an MD5 checksum for a DataFrame's content.

        Args:
            df: DataFrame to generate checksum for

        Returns:
            MD5 hash string of the DataFrame content
        """
        try:
            # Convert DataFrame to bytes in a consistent way
            content = df.to_csv(index=False).encode('utf-8')
            return hashlib.md5(content).hexdigest()
        except Exception as e:
            self.logger.error(f"Error generating checksum: {e}", exc_info=True)
            return ""

    def verify_csv_export(self, source_df: pd.DataFrame, csv_buffer: io.BytesIO) -> Dict[str, Any]:
        """
        Verify that a CSV export matches the source DataFrame.

        Args:
            source_df: Original DataFrame used to create the export
            csv_buffer: BytesIO buffer containing the exported CSV data

        Returns:
            Dictionary with verification results including:
            - is_valid: Overall verification result
            - row_count_match: Whether row counts match
            - column_count_match: Whether column counts match
            - column_names_match: Whether column names match
            - details: Human-readable summary
        """
        result = {
            'is_valid': False,
            'row_count_match': False,
            'column_count_match': False,
            'column_names_match': False,
            'source_rows': len(source_df),
            'export_rows': 0,
            'source_columns': len(source_df.columns),
            'export_columns': 0,
            'details': ''
        }

        try:
            # Read the exported CSV back
            csv_buffer.seek(0)
            exported_df = pd.read_csv(csv_buffer, encoding='utf-8-sig')

            result['export_rows'] = len(exported_df)
            result['export_columns'] = len(exported_df.columns)

            # Check row count
            result['row_count_match'] = len(source_df) == len(exported_df)

            # Check column count
            result['column_count_match'] = len(source_df.columns) == len(exported_df.columns)

            # Check column names (accounting for any renaming during export)
            source_cols = set(source_df.columns)
            export_cols = set(exported_df.columns)
            result['column_names_match'] = source_cols == export_cols

            # Overall validity
            result['is_valid'] = all([
                result['row_count_match'],
                result['column_count_match'],
                result['column_names_match']
            ])

            # Build details message
            details_parts = []
            if result['is_valid']:
                details_parts.append(f"Export verified: {result['source_rows']} rows, {result['source_columns']} columns")
            else:
                if not result['row_count_match']:
                    details_parts.append(f"Row count mismatch: source={result['source_rows']}, export={result['export_rows']}")
                if not result['column_count_match']:
                    details_parts.append(f"Column count mismatch: source={result['source_columns']}, export={result['export_columns']}")
                if not result['column_names_match']:
                    missing_cols = source_cols - export_cols
                    extra_cols = export_cols - source_cols
                    if missing_cols:
                        details_parts.append(f"Missing columns: {missing_cols}")
                    if extra_cols:
                        details_parts.append(f"Extra columns: {extra_cols}")

            result['details'] = '; '.join(details_parts)

        except Exception as e:
            self.logger.error(f"Error verifying CSV export: {e}", exc_info=True)
            result['details'] = f"Verification error: {str(e)}"

        return result

    def verify_excel_export(self, source_df: pd.DataFrame, excel_buffer: io.BytesIO) -> Dict[str, Any]:
        """
        Verify that an Excel export matches the source DataFrame.

        Args:
            source_df: Original DataFrame used to create the export
            excel_buffer: BytesIO buffer containing the exported Excel data

        Returns:
            Dictionary with verification results (same structure as verify_csv_export)
        """
        result = {
            'is_valid': False,
            'row_count_match': False,
            'column_count_match': False,
            'column_names_match': False,
            'source_rows': len(source_df),
            'export_rows': 0,
            'source_columns': len(source_df.columns),
            'export_columns': 0,
            'details': ''
        }

        try:
            # Read the exported Excel back
            excel_buffer.seek(0)
            exported_df = pd.read_excel(excel_buffer, engine='openpyxl')

            result['export_rows'] = len(exported_df)
            result['export_columns'] = len(exported_df.columns)

            # Check row count
            result['row_count_match'] = len(source_df) == len(exported_df)

            # Check column count
            result['column_count_match'] = len(source_df.columns) == len(exported_df.columns)

            # Check column names
            source_cols = set(source_df.columns)
            export_cols = set(exported_df.columns)
            result['column_names_match'] = source_cols == export_cols

            # Overall validity
            result['is_valid'] = all([
                result['row_count_match'],
                result['column_count_match'],
                result['column_names_match']
            ])

            # Build details message
            details_parts = []
            if result['is_valid']:
                details_parts.append(f"Export verified: {result['source_rows']} rows, {result['source_columns']} columns")
            else:
                if not result['row_count_match']:
                    details_parts.append(f"Row count mismatch: source={result['source_rows']}, export={result['export_rows']}")
                if not result['column_count_match']:
                    details_parts.append(f"Column count mismatch: source={result['source_columns']}, export={result['export_columns']}")
                if not result['column_names_match']:
                    missing_cols = source_cols - export_cols
                    extra_cols = export_cols - source_cols
                    if missing_cols:
                        details_parts.append(f"Missing columns: {missing_cols}")
                    if extra_cols:
                        details_parts.append(f"Extra columns: {extra_cols}")

            result['details'] = '; '.join(details_parts)

        except Exception as e:
            self.logger.error(f"Error verifying Excel export: {e}", exc_info=True)
            result['details'] = f"Verification error: {str(e)}"

        return result

    def get_verification_badge(self, verification_result: Dict[str, Any]) -> str:
        """
        Generate a verification badge/status message.

        Args:
            verification_result: Result dictionary from verify_csv_export or verify_excel_export

        Returns:
            Formatted status string with emoji indicator
        """
        if verification_result.get('is_valid'):
            return f"✅ {verification_result['details']}"
        else:
            return f"⚠️ {verification_result['details']}"

    def quick_verify(self, source_df: pd.DataFrame, export_buffer: io.BytesIO, format_type: str = 'csv') -> bool:
        """
        Quick verification check returning just a boolean.

        Args:
            source_df: Original DataFrame
            export_buffer: BytesIO buffer with exported data
            format_type: 'csv' or 'excel'

        Returns:
            True if verification passed, False otherwise
        """
        if format_type.lower() == 'csv':
            result = self.verify_csv_export(source_df, export_buffer)
        elif format_type.lower() in ('excel', 'xlsx'):
            result = self.verify_excel_export(source_df, export_buffer)
        else:
            self.logger.warning(f"Unknown format type: {format_type}")
            return False

        return result.get('is_valid', False)

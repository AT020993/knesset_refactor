"""
Document handling utilities for bill and agenda documents.

This module provides the DocumentHandler class for formatting, displaying,
and exporting document links in query results.
"""

import hashlib
import io
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import streamlit as st

from backend.connection_manager import get_db_connection, safe_execute_query


class DocumentHandler:
    """Handles bill and agenda document formatting and display."""

    def __init__(self, db_path: Path, logger: logging.Logger):
        """
        Initialize the document handler.

        Args:
            db_path: Path to the database
            logger: Logger instance for error reporting
        """
        self.db_path = db_path
        self.logger = logger

    @staticmethod
    def create_document_badge(row: pd.Series) -> str:
        """
        Create a readable document badge for a bill row.

        Args:
            row: Pandas Series containing bill document information

        Returns:
            Formatted string like "📄 Published Law (PDF) +3 more" or "No documents"
        """
        # Check if URL exists (null/NaN check)
        if pd.isna(row.get('BillPrimaryDocumentURL')) or not row.get('BillPrimaryDocumentURL'):
            return "No documents"

        # Get document type and format
        doc_type = row.get('BillPrimaryDocumentType', 'Unknown')
        doc_format = row.get('BillPrimaryDocumentFormat', 'Unknown')

        # Format the primary document
        badge = f"📄 {doc_type} ({doc_format})"

        # Add count of additional documents if > 1
        total_count = row.get('BillDocumentCount', 1)
        if pd.notna(total_count) and total_count > 1:
            additional_count = int(total_count) - 1
            badge += f" +{additional_count} more"

        return badge

    def format_bill_document_links(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Format bill document links by adding a human-readable Documents column.

        Args:
            df: DataFrame with bill document fields

        Returns:
            Modified DataFrame with Documents column added after BillName
        """
        # Create a copy to avoid modifying the original
        df_copy = df.copy()

        # Create the Documents column using the badge creator
        df_copy['Documents'] = df_copy.apply(self.create_document_badge, axis=1)

        # Move Documents column after BillName if BillName exists
        if 'BillName' in df_copy.columns:
            # Get all column names
            cols = df_copy.columns.tolist()

            # Remove Documents from its current position
            cols.remove('Documents')

            # Find BillName index and insert Documents after it
            billname_idx = cols.index('BillName')
            cols.insert(billname_idx + 1, 'Documents')

            # Reorder the dataframe
            df_copy = df_copy[cols]

        return df_copy

    @staticmethod
    def get_column_config(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get column configuration for st.dataframe display.

        Configures BillPrimaryDocumentURL as clickable link and hides technical columns.

        Args:
            df: DataFrame to configure

        Returns:
            Dictionary for st.dataframe column_config parameter
        """
        config: Dict[str, Any] = {}

        # Configure primary document URL as clickable link
        if 'BillPrimaryDocumentURL' in df.columns:
            config['BillPrimaryDocumentURL'] = st.column_config.LinkColumn(
                label="Primary Document",
                help="Click to open the primary bill document",
                display_text="Open Document"
            )

        # Configure Knesset website URL as clickable link
        if 'BillKnessetWebsiteURL' in df.columns:
            config['BillKnessetWebsiteURL'] = st.column_config.LinkColumn(
                label="Knesset Website",
                help="View bill details on Knesset.gov.il",
                display_text="View on Knesset.gov.il"
            )

        # Configure agenda document URL as clickable link
        if 'AgendaPrimaryDocumentURL' in df.columns:
            config['AgendaPrimaryDocumentURL'] = st.column_config.LinkColumn(
                label="Primary Document",
                help="Click to open the primary agenda document",
                display_text="Open Document"
            )

        # Configure Agenda Knesset website URL as clickable link
        if 'AgendaKnessetWebsiteURL' in df.columns:
            config['AgendaKnessetWebsiteURL'] = st.column_config.LinkColumn(
                label="Knesset Website",
                help="View agenda details on Knesset.gov.il",
                display_text="View on Knesset.gov.il"
            )

        # Hide technical document columns (still available for export)
        technical_columns = [
            'BillPrimaryDocumentType',
            'BillPrimaryDocumentFormat',
            'BillPublishedLawDocCount',
            'BillFirstReadingDocCount',
            'BillSecondThirdReadingDocCount',
            'BillEarlyDiscussionDocCount',
            'BillOtherDocCount',
            # Agenda technical columns
            'AgendaPrimaryDocumentType',
            'AgendaPDFDocCount',
            'AgendaWordDocCount',
            'AgendaDocumentTypes'
        ]

        for col in technical_columns:
            if col in df.columns:
                config[col] = None  # None hides the column in Streamlit

        return config

    def create_excel_with_hyperlinks(self, df: pd.DataFrame) -> io.BytesIO:
        """
        Create Excel file with clickable hyperlinks for URL columns.

        Args:
            df: DataFrame to export

        Returns:
            BytesIO buffer containing Excel file with hyperlinks
        """
        from openpyxl.styles import Font

        buffer = io.BytesIO()

        # Identify URL columns
        url_columns = [
            col for col in df.columns
            if 'URL' in col or 'Link' in col or col == 'BillKnessetWebsiteURL'
        ]

        try:
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Results")

                # Get worksheet
                worksheet = writer.sheets["Results"]

                # Add hyperlinks to URL columns
                for col_idx, col_name in enumerate(df.columns, start=1):
                    if col_name in url_columns:
                        for row_idx, url in enumerate(df[col_name], start=2):  # start=2 for header
                            if pd.notna(url) and url:
                                cell = worksheet.cell(row=row_idx, column=col_idx)
                                cell.value = "Open Link"
                                cell.hyperlink = str(url)
                                cell.font = Font(color="0563C1", underline="single")

            buffer.seek(0)
        except Exception as e:
            self.logger.error(f"Error creating Excel with hyperlinks: {e}")
            # Fallback to simple Excel
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Results")
            buffer.seek(0)

        return buffer

    def render_multi_document_view(self, df: pd.DataFrame) -> None:
        """
        Render expandable section for bills with multiple documents.

        Args:
            df: DataFrame with bill data including BillID and BillDocumentCount
        """
        # Find bills with 5+ documents
        if 'BillDocumentCount' not in df.columns:
            return

        multi_doc_bills = df[df['BillDocumentCount'] >= 5].head(10)

        if multi_doc_bills.empty:
            return

        with st.expander("📚 Bills with Multiple Documents (Top 10)", expanded=False):
            st.caption("Bills with 5+ documents - click links to access documents")

            for bill_row_idx, (_, row) in enumerate(multi_doc_bills.iterrows()):
                bill_name = row.get('BillName', 'Unknown Bill')
                bill_id_raw = row.get('BillID')
                if bill_id_raw is None or pd.isna(bill_id_raw):
                    continue
                try:
                    bill_id = int(bill_id_raw)
                except (TypeError, ValueError):
                    continue
                doc_count = row.get('BillDocumentCount', 0)

                st.markdown(f"**{bill_name}** (Bill ID: {bill_id}) - {doc_count} documents")

                # Query for all documents for this bill
                docs_df = self._get_bill_documents(bill_id)

                if not docs_df.empty:
                    # Pass row index to ensure unique key context across multi-document view iterations
                    self._render_document_list(docs_df, bill_id, bill_row_idx)

                st.divider()

    def _get_bill_documents(self, bill_id: int) -> pd.DataFrame:
        """
        Fetch all documents for a specific bill.

        Args:
            bill_id: Bill ID to query

        Returns:
            DataFrame with document information
        """
        query = """
        SELECT
            GroupTypeDesc as DocumentType,
            ApplicationDesc as Format,
            FilePath as URL
        FROM KNS_DocumentBill
        WHERE BillID = ?
            AND FilePath IS NOT NULL
        ORDER BY
            CASE GroupTypeDesc
                WHEN 'חוק - פרסום ברשומות' THEN 1
                WHEN 'הצעת חוק לקריאה הראשונה' THEN 2
                WHEN 'הצעת חוק לקריאה השנייה והשלישית' THEN 3
                WHEN 'הצעת חוק לדיון מוקדם' THEN 4
                ELSE 5
            END,
            ApplicationDesc
        """

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                result = safe_execute_query(con, query, self.logger, params=[bill_id])
                return result if result is not None else pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error fetching bill documents: {e}")
            return pd.DataFrame()

    @staticmethod
    def _render_document_list(
        docs_df: pd.DataFrame,
        bill_id: int,
        bill_row_idx: int = 0
    ) -> None:
        """
        Render organized list of documents by type with PDF preview capability.

        Args:
            docs_df: DataFrame with document information
            bill_id: Bill ID for unique key generation
            bill_row_idx: Row index in the parent multi-document view for key uniqueness
        """
        # Group by document type
        for doc_type, group in docs_df.groupby('DocumentType', sort=False):
            st.markdown(f"**{doc_type}** ({len(group)} files)")

            for doc_group_idx, (idx, doc) in enumerate(group.iterrows()):
                doc_format = doc.get('Format', 'Unknown')
                doc_url = doc.get('URL', '')

                if doc_url:
                    # Create columns for link and preview button
                    col1, col2 = st.columns([3, 1])

                    with col1:
                        st.markdown(f"- [{doc_format}]({doc_url})")

                    # Add PDF preview button for PDF documents
                    with col2:
                        if doc_format and doc_format.upper() == 'PDF':
                            # Create fully unique key with multiple context layers:
                            # 1. bill_id: uniqueness across bills
                            # 2. bill_row_idx: uniqueness across multi-document view rows
                            # 3. doc_group_idx: uniqueness across documents within a bill
                            # 4. URL hash: fallback uniqueness for identical documents
                            url_hash = hashlib.md5(doc_url.encode()).hexdigest()[:8]
                            preview_key = f"preview_{bill_id}_{bill_row_idx}_{doc_group_idx}_{url_hash}"
                            if st.button("👁️ Preview", key=preview_key, help="Preview PDF inline"):
                                # Display PDF using iframe
                                st.markdown(
                                    f'<iframe src="{doc_url}" width="100%" height="600px" type="application/pdf"></iframe>',
                                    unsafe_allow_html=True
                                )

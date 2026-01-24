"""
CAP PDF Viewer Component

Handles rendering of bill documents with embedded PDF viewing:
- Fetches PDFs and embeds using base64 data URIs (bypasses X-Frame-Options)
- Shows document type labels with Hebrew-to-English mapping
- Provides fallback links for non-PDF documents
"""

import logging
from typing import Optional

import streamlit as st
import pandas as pd

from ui.services.cap_service import CAPAnnotationService


class CAPPDFViewer:
    """Renders bill documents with embedded PDF viewing."""

    # Document type label mapping (Hebrew -> English with emoji)
    DOC_TYPE_LABELS = {
        "חוק - פרסום ברשומות": "📜 Published Law",
        "הצעת חוק לקריאה הראשונה": "📋 First Reading Proposal",
        "הצעת חוק לקריאה השנייה והשלישית": "📋 Second/Third Reading Proposal",
        "הצעת חוק לדיון מוקדם": "📋 Early Discussion Proposal",
    }

    def __init__(
        self,
        service: CAPAnnotationService,
        logger_obj: Optional[logging.Logger] = None,
    ):
        """
        Initialize PDF viewer.

        Args:
            service: CAP annotation service (for fetching document metadata)
            logger_obj: Optional logger
        """
        self.service = service
        self.logger = logger_obj or logging.getLogger(__name__)

    @staticmethod
    @st.cache_data(ttl=3600, show_spinner=False)
    def fetch_pdf_as_base64(pdf_url: str) -> Optional[str]:
        """
        Fetch PDF and return as base64 string.

        Cached for 1 hour to avoid repeated network calls for the same PDF.

        Args:
            pdf_url: URL of the PDF to fetch

        Returns:
            Base64-encoded PDF content, or None if fetch failed
        """
        import base64
        import requests

        try:
            response = requests.get(pdf_url, timeout=15)
            response.raise_for_status()
            return base64.b64encode(response.content).decode("utf-8")
        except Exception:
            return None

    def render_bill_documents(self, bill_id: int):
        """
        Render embedded PDF viewer for bill documents.

        Fetches PDF and embeds it using base64 data URI to bypass X-Frame-Options
        restrictions that prevent direct iframe embedding.

        Args:
            bill_id: The bill ID to fetch documents for
        """
        docs = self.service.get_bill_documents(bill_id)

        if docs.empty:
            st.caption("📄 No documents available for this bill")
            return

        # Find PDF documents (prioritized by document type)
        pdf_docs = docs[docs["Format"].str.upper() == "PDF"]

        with st.expander("📄 Bill Documents", expanded=True):
            if not pdf_docs.empty:
                self._render_primary_pdf(pdf_docs, docs)
            else:
                self._render_non_pdf_documents(docs)

    def _render_primary_pdf(self, pdf_docs: pd.DataFrame, all_docs: pd.DataFrame):
        """Render the primary PDF document with embedded viewer."""
        primary_doc = pdf_docs.iloc[0]
        doc_type = primary_doc["DocumentType"]
        pdf_url = primary_doc["URL"]
        display_label = self.DOC_TYPE_LABELS.get(doc_type, f"📄 {doc_type}")

        st.markdown(f"**Showing:** {display_label}")

        # Fetch and embed PDF as base64 (cached)
        with st.spinner("Loading PDF..."):
            pdf_base64 = self.fetch_pdf_as_base64(pdf_url)

        if pdf_base64:
            # Embed using data URI
            st.markdown(
                f'<iframe src="data:application/pdf;base64,{pdf_base64}" '
                f'width="100%" height="600px" '
                f'style="border: 1px solid #ddd; border-radius: 4px;" '
                f'type="application/pdf"></iframe>',
                unsafe_allow_html=True,
            )
        else:
            st.warning("Could not load PDF inline. Use the link below:")

        # Direct link (always shown as fallback)
        st.markdown(
            f'<a href="{pdf_url}" target="_blank" rel="noopener noreferrer" '
            f'style="display: inline-block; padding: 6px 12px; background-color: #0066cc; '
            f'color: white; text-decoration: none; border-radius: 4px; margin-top: 8px; font-size: 14px;">'
            f'📥 Open PDF in New Tab</a>',
            unsafe_allow_html=True,
        )

        # Show other available documents
        other_docs = all_docs[
            ~((all_docs["Format"].str.upper() == "PDF") & (all_docs["URL"] == pdf_url))
        ]
        if not other_docs.empty:
            st.markdown("---")
            st.markdown("**Other documents:**")
            for _, doc in other_docs.iterrows():
                doc_type_display = self.DOC_TYPE_LABELS.get(
                    doc["DocumentType"], doc["DocumentType"]
                )
                st.markdown(
                    f"- [{doc_type_display} ({doc['Format']})]({doc['URL']})"
                )

    def _render_non_pdf_documents(self, docs: pd.DataFrame):
        """Render links to non-PDF documents when no PDF is available."""
        st.info("📄 No PDF documents available. Other formats:")
        for _, doc in docs.iterrows():
            doc_type_display = self.DOC_TYPE_LABELS.get(
                doc["DocumentType"], doc["DocumentType"]
            )
            st.markdown(
                f"- [{doc_type_display} ({doc['Format']})]({doc['URL']})"
            )

    def get_doc_type_label(self, doc_type: str) -> str:
        """
        Get the display label for a document type.

        Args:
            doc_type: Hebrew document type name

        Returns:
            Display label with emoji
        """
        return self.DOC_TYPE_LABELS.get(doc_type, f"📄 {doc_type}")

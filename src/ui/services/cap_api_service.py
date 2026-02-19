"""
CAP API Service - Fetch bills from Knesset OData API

This module provides functionality to fetch bills directly from the Knesset API
for annotation, without requiring them to be in the local database.
"""

import asyncio
import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import pandas as pd

try:
    import aiohttp
except ImportError:
    aiohttp = None  # type: ignore[assignment]


class CAPAPIService:
    """Service for fetching bills from Knesset API for CAP annotation."""

    BASE_URL = "https://knesset.gov.il/Odata/ParliamentInfo.svc"

    # Error messages
    ERROR_TIMEOUT = "Request timed out. The Knesset API may be slow. Please try again."
    ERROR_NETWORK = "Network error: {details}"
    ERROR_UNEXPECTED = "Unexpected error: {details}"

    def __init__(self, logger_obj: Optional[logging.Logger] = None):
        """Initialize the API service."""
        self.logger = logger_obj or logging.getLogger(__name__)
        if aiohttp is None:
            self.logger.warning("aiohttp not installed — CAP API fetching disabled")

    async def _fetch_json(
        self, session: aiohttp.ClientSession, url: str
    ) -> Dict[str, Any]:
        """Fetch JSON data from URL."""
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with session.get(url, timeout=timeout) as resp:
                resp.raise_for_status()
                payload = await resp.json(content_type=None)
                return payload if isinstance(payload, dict) else {}
        except Exception as e:
            self.logger.error(f"API fetch error: {e}")
            raise

    async def fetch_recent_bills(
        self,
        knesset_num: int = 25,
        limit: int = 100,
        skip: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Fetch recent bills from the Knesset API.

        Args:
            knesset_num: Knesset number to fetch bills from
            limit: Maximum number of bills to fetch
            skip: Number of bills to skip (for pagination)

        Returns:
            List of bill dictionaries
        """
        url = (
            f"{self.BASE_URL}/KNS_Bill()?"
            f"$filter=KnessetNum%20eq%20{knesset_num}"
            f"&$orderby=BillID%20desc"
            f"&$top={limit}"
            f"&$skip={skip}"
            f"&$format=json"
        )

        try:
            async with aiohttp.ClientSession() as session:
                data = await self._fetch_json(session, url)
                bills_raw = data.get("value", [])
                bills = [
                    bill
                    for bill in bills_raw
                    if isinstance(bill, dict)
                ] if isinstance(bills_raw, list) else []
                self.logger.info(f"Fetched {len(bills)} bills from API")
                return bills
        except Exception as e:
            self.logger.error(f"Error fetching bills: {e}")
            return []

    async def fetch_bill_by_id(self, bill_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch a specific bill by ID.

        Args:
            bill_id: The bill ID to fetch

        Returns:
            Bill dictionary or None if not found
        """
        url = (
            f"{self.BASE_URL}/KNS_Bill({bill_id})?"
            f"$format=json"
        )

        try:
            async with aiohttp.ClientSession() as session:
                data = await self._fetch_json(session, url)
                return data
        except Exception as e:
            self.logger.error(f"Error fetching bill {bill_id}: {e}")
            return None

    async def search_bills_by_name(
        self,
        search_term: str,
        knesset_num: Optional[int] = None,
        limit: int = 50
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Search bills by name.

        Args:
            search_term: Term to search in bill names
            knesset_num: Optional Knesset number filter
            limit: Maximum results

        Returns:
            Tuple of (results_list, error_message).
            - On success: (list_of_bills, None)
            - On error: ([], "Error description")
        """
        # URL encode the search term
        encoded_term = search_term.replace("'", "''")

        filter_parts = [f"substringof('{encoded_term}', Name)"]
        if knesset_num:
            filter_parts.append(f"KnessetNum eq {knesset_num}")

        filter_str = " and ".join(filter_parts)

        url = (
            f"{self.BASE_URL}/KNS_Bill()?"
            f"$filter={filter_str}"
            f"&$orderby=BillID%20desc"
            f"&$top={limit}"
            f"&$format=json"
        )

        try:
            async with aiohttp.ClientSession() as session:
                data = await self._fetch_json(session, url)
                return data.get("value", []), None
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout searching bills for term: {search_term}")
            return [], self.ERROR_TIMEOUT
        except aiohttp.ClientError as e:
            self.logger.error(f"Network error searching bills: {e}")
            return [], self.ERROR_NETWORK.format(details=str(e))
        except Exception as e:
            self.logger.error(f"Unexpected error searching bills: {e}")
            return [], self.ERROR_UNEXPECTED.format(details=str(e))

    def fetch_recent_bills_sync(
        self,
        knesset_num: int = 25,
        limit: int = 100
    ) -> pd.DataFrame:
        """
        Synchronous wrapper to fetch recent bills.

        Returns:
            DataFrame with bill data
        """
        try:
            bills = asyncio.run(self.fetch_recent_bills(knesset_num, limit))
            if bills:
                df = pd.DataFrame(bills)
                # Add URL column
                df['BillURL'] = df['BillID'].apply(
                    lambda x: f"https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid={x}"
                )
                return df
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error in sync fetch: {e}")
            return pd.DataFrame()

    def fetch_bill_by_id_sync(self, bill_id: int) -> Optional[Dict[str, Any]]:
        """Synchronous wrapper to fetch a bill by ID."""
        try:
            return asyncio.run(self.fetch_bill_by_id(bill_id))
        except Exception as e:
            self.logger.error(f"Error in sync fetch: {e}")
            return None

    def search_bills_by_name_sync(
        self,
        search_term: str,
        knesset_num: Optional[int] = None,
        limit: int = 50
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Synchronous wrapper to search bills by name.

        Args:
            search_term: Term to search in bill names
            knesset_num: Optional Knesset number filter
            limit: Maximum results

        Returns:
            Tuple of (results_list, error_message).
            - On success: (list_of_bills, None)
            - On error: ([], "Error description")
        """
        try:
            return asyncio.run(
                self.search_bills_by_name(search_term, knesset_num, limit)
            )
        except Exception as e:
            self.logger.error(f"Error in sync search: {e}")
            return [], self.ERROR_UNEXPECTED.format(details=str(e))


def get_cap_api_service(logger_obj: Optional[logging.Logger] = None) -> CAPAPIService:
    """Factory function to get a CAP API service instance."""
    return CAPAPIService(logger_obj)

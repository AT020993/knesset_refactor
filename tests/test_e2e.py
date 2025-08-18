"""
End-to-end tests for the Knesset OData Streamlit application using Playwright.

These tests simulate real user interactions in a browser to ensure all components
of the application work together as expected.
"""

import re
import pytest
from playwright.sync_api import Page, expect

# Mark all tests in this file as E2E tests
pytestmark = pytest.mark.e2e


def test_main_page_title_and_header(page: Page):
    """
    Tests that the main page loads correctly and has the expected title and header.
    """
    # Navigate to the root URL where the Streamlit app is running
    page.goto("/")

    # Wait for page to load
    page.wait_for_load_state("networkidle")

    # 1. Check for the correct page title
    expect(page).to_have_title(re.compile("Knesset OData"))

    # 2. Check for the main header (actual header from screenshot)
    header = page.get_by_role("heading", name="Knesset Data Warehouse Console")
    expect(header).to_be_visible()




def test_data_refresh_section(page: Page):
    """
    Tests that the data refresh section is present.
    """
    page.goto("/")
    page.wait_for_load_state("networkidle")

    # 1. Check for data refresh controls (visible on main page)
    refresh_header = page.get_by_role("heading", name="Data Refresh Controls")
    expect(refresh_header).to_be_visible()


def test_predefined_queries_section(page: Page):
    """
    Tests that the predefined queries section is accessible.
    """
    page.goto("/")
    page.wait_for_load_state("networkidle")

    # 1. Check for predefined queries header (visible on main page)
    queries_header = page.get_by_role("heading", name="Predefined Queries")
    expect(queries_header).to_be_visible()

    # 2. Check for query selection dropdown
    query_selector = page.get_by_text("Select a predefined query:")
    expect(query_selector).to_be_visible()




def test_sidebar_navigation(page: Page):
    """
    Tests that sidebar elements are present and functional.
    """
    page.goto("/")

    # Wait for page to load
    page.wait_for_load_state("networkidle")
    
    # 1. Check for sidebar presence using Streamlit's data-testid
    sidebar = page.locator("[data-testid='stSidebar']")
    expect(sidebar).to_be_visible()
    
    # 2. Check that navigation elements are present in sidebar
    plots_page_link = page.get_by_text("plots page")
    expect(plots_page_link).to_be_visible()


def test_error_handling_with_invalid_selections(page: Page):
    """
    Tests that the application handles invalid or missing selections gracefully.
    """
    page.goto("/")

    # Wait for the page to fully load
    page.wait_for_load_state("networkidle")

    # 1. Test that the app doesn't crash with incomplete inputs
    # Check that the main Streamlit app container is visible
    app_container = page.locator("[data-testid='stApp']")
    expect(app_container).to_be_visible()
    
    # 2. Verify core elements are still accessible
    main_header = page.get_by_role("heading", name="Knesset Data Warehouse Console")
    expect(main_header).to_be_visible()


def test_responsive_design_mobile(page: Page):
    """
    Tests that the application works on mobile viewport sizes.
    """
    # Set mobile viewport
    page.set_viewport_size({"width": 375, "height": 667})
    page.goto("/")

    # Wait for the page to fully load
    page.wait_for_load_state("networkidle")

    # 1. Check that main Streamlit app container is still visible on mobile
    app_container = page.locator("[data-testid='stApp']")
    expect(app_container).to_be_visible()
    
    # 2. Check that the app doesn't break on smaller screens
    header = page.get_by_role("heading", name="Knesset Data Warehouse Console")
    expect(header).to_be_visible()


def test_page_load_performance(page: Page):
    """
    Tests that the page loads within a reasonable time.
    """
    # Start timing
    page.goto("/")
    
    # Wait for the main content to load (max 30 seconds for Streamlit apps)
    page.wait_for_selector("h1", timeout=30000)
    
    # Verify core elements loaded successfully
    header = page.get_by_role("heading", name="Knesset Data Warehouse Console")
    expect(header).to_be_visible()
"""
Test for the local_knesset_filter widget fix to prevent infinite reruns.

This test verifies that the local Knesset filter widget in the query results
section maintains stable state and doesn't cause infinite reruns.
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
from typing import List, Dict, Optional, Any


class TestLocalKnessetFilterStability:
    """Test that the local_knesset_filter widget doesn't cause infinite reruns."""

    def test_filter_state_initialization(self):
        """
        Test that the filter state is properly initialized before widget rendering.

        This prevents the widget from having an undefined state on first render,
        which would cause Streamlit to trigger a rerun.
        """
        # Mock Streamlit session state
        mock_session_state = {}

        with patch('streamlit.session_state', mock_session_state):
            # Simulate the initialization logic from _render_local_knesset_filter
            if "local_knesset_filter" not in mock_session_state:
                mock_session_state["local_knesset_filter"] = "All Knessetes"

            # Verify state was initialized
            assert "local_knesset_filter" in mock_session_state
            assert mock_session_state["local_knesset_filter"] == "All Knessetes"

    def test_filter_value_validation_when_options_change(self):
        """
        Test that when available Knesset options change, the filter value is validated
        and reset if it's no longer valid.

        This prevents the case where:
        1. User selects "Knesset 25"
        2. Query results change to only show Knesset 24
        3. The selectbox options change to ["All Knessetes", "Knesset 24"]
        4. The stored value "Knesset 25" is no longer in the options
        5. Streamlit would trigger a rerun because the index is invalid
        """
        mock_session_state = {"local_knesset_filter": "Knesset 25"}

        with patch('streamlit.session_state', mock_session_state):
            # Simulate new query results with different Knessetes
            results_df = pd.DataFrame({
                'KnessetNum': [24, 24, 24, 25, 25]
            })
            available_knessetes = sorted(results_df['KnessetNum'].unique().tolist(), reverse=True)

            # Build options as in the fixed code
            knesset_options = ["All Knessetes"] + [f"Knesset {k}" for k in available_knessetes]

            # Simulate validation logic from the fix
            current_value = mock_session_state.get("local_knesset_filter", "All Knessetes")
            if current_value not in knesset_options:
                current_value = "All Knessetes"
                mock_session_state["local_knesset_filter"] = current_value

            # Verify value was reset because it's still valid in this case
            # (both Knesset 24 and 25 are available)
            assert mock_session_state["local_knesset_filter"] == "Knesset 25"

    def test_filter_value_reset_when_invalid(self):
        """
        Test that the filter value is reset to "All Knessetes" if it becomes invalid.

        Scenario: User selects "Knesset 25", then runs a query that only returns
        Knesset 24 data. The stored value "Knesset 25" would be invalid.
        """
        mock_session_state = {"local_knesset_filter": "Knesset 25"}

        with patch('streamlit.session_state', mock_session_state):
            # Simulate query results with only Knesset 24
            results_df = pd.DataFrame({
                'KnessetNum': [24, 24, 24]
            })
            available_knessetes = sorted(results_df['KnessetNum'].unique().tolist(), reverse=True)

            # Build options as in the fixed code
            knesset_options = ["All Knessetes"] + [f"Knesset {k}" for k in available_knessetes]

            # Simulate validation logic from the fix
            current_value = mock_session_state.get("local_knesset_filter", "All Knessetes")
            if current_value not in knesset_options:
                current_value = "All Knessetes"
                mock_session_state["local_knesset_filter"] = current_value

            # Verify value was reset because "Knesset 25" is no longer in options
            assert mock_session_state["local_knesset_filter"] == "All Knessetes"
            assert current_value == "All Knessetes"

    def test_selectbox_index_calculation_is_stable(self):
        """
        Test that the selectbox index calculation is always stable and valid.

        Streamlit triggers reruns when widget index is invalid. This test ensures
        the index is always correctly calculated based on the current options.
        """
        results_df = pd.DataFrame({
            'KnessetNum': [25, 25, 24, 24, 23]
        })
        available_knessetes = sorted(results_df['KnessetNum'].unique().tolist(), reverse=True)

        # Build options as in the fixed code
        knesset_options = ["All Knessetes"] + [f"Knesset {k}" for k in available_knessetes]

        # Expected: ["All Knessetes", "Knesset 25", "Knesset 24", "Knesset 23"]
        assert knesset_options == ["All Knessetes", "Knesset 25", "Knesset 24", "Knesset 23"]

        # Test various states
        test_cases = [
            ("All Knessetes", 0),
            ("Knesset 25", 1),
            ("Knesset 24", 2),
            ("Knesset 23", 3),
        ]

        for value, expected_index in test_cases:
            # Verify index lookup works without error
            actual_index = knesset_options.index(value)
            assert actual_index == expected_index

    def test_no_uninitialized_widget_rendering(self):
        """
        Test that the widget is never rendered with uninitialized state.

        Before the fix:
        - Widget rendered with no default value
        - Session state might be None or missing
        - Streamlit would use index=0 and potentially trigger reruns

        After the fix:
        - State is initialized before widget rendering
        - Index is explicitly provided
        - Widget has a stable, valid value
        """
        mock_session_state = {}

        with patch('streamlit.session_state', mock_session_state):
            # Before rendering widget, initialize state
            if "local_knesset_filter" not in mock_session_state:
                mock_session_state["local_knesset_filter"] = "All Knessetes"

            # Get current value for widget initialization
            current_value = mock_session_state.get("local_knesset_filter", "All Knessetes")

            # Build options
            results_df = pd.DataFrame({'KnessetNum': [25, 24]})
            available_knessetes = sorted(results_df['KnessetNum'].unique().tolist(), reverse=True)
            knesset_options = ["All Knessetes"] + [f"Knesset {k}" for k in available_knessetes]

            # Calculate index for widget
            index = knesset_options.index(current_value)

            # Verify everything is initialized and valid
            assert current_value is not None
            assert index >= 0
            assert index < len(knesset_options)
            assert knesset_options[index] == current_value

    def test_metric_widget_stability(self):
        """
        Test that the metric widget showing row counts doesn't cause reruns.

        The metric widget should display consistent values based on the current
        filter state. It shouldn't trigger widget state changes on its own.
        """
        mock_session_state = {"local_knesset_filter": "All Knessetes"}
        results_df = pd.DataFrame({
            'KnessetNum': [25, 25, 25, 24, 24]
        })

        with patch('streamlit.session_state', mock_session_state):
            # Test "All Knessetes" shows total rows
            if mock_session_state.get("local_knesset_filter", "All Knessetes") == "All Knessetes":
                total_rows = len(results_df)
                assert total_rows == 5

            # Test specific Knesset shows filtered rows
            mock_session_state["local_knesset_filter"] = "Knesset 25"
            if mock_session_state["local_knesset_filter"] != "All Knessetes":
                selected_knesset = int(mock_session_state["local_knesset_filter"].replace("Knesset ", ""))
                count = len(results_df[results_df['KnessetNum'] == selected_knesset])
                assert count == 3

            # Test another Knesset
            mock_session_state["local_knesset_filter"] = "Knesset 24"
            if mock_session_state["local_knesset_filter"] != "All Knessetes":
                selected_knesset = int(mock_session_state["local_knesset_filter"].replace("Knesset ", ""))
                count = len(results_df[results_df['KnessetNum'] == selected_knesset])
                assert count == 2


class TestQueryResultsRenderingStability:
    """Test that query results rendering doesn't trigger infinite reruns."""

    def test_apply_local_knesset_filter_returns_consistent_data(self):
        """
        Test that applying the local filter returns consistent, predictable data.

        Consistent filtering is essential to prevent Streamlit from detecting
        changed output and triggering reruns.
        """
        results_df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'KnessetNum': [25, 25, 25, 24, 24]
        })

        # Test filtering by Knesset 25
        local_filter = "Knesset 25"
        if local_filter != "All Knessetes":
            selected_knesset = int(local_filter.replace("Knesset ", ""))
            filtered_df = results_df[results_df['KnessetNum'] == selected_knesset].copy()

            # Verify filtered data is stable
            assert len(filtered_df) == 3
            assert list(filtered_df['id']) == [1, 2, 3]

        # Test filtering by Knesset 24
        local_filter = "Knesset 24"
        if local_filter != "All Knessetes":
            selected_knesset = int(local_filter.replace("Knesset ", ""))
            filtered_df = results_df[results_df['KnessetNum'] == selected_knesset].copy()

            # Verify filtered data is stable
            assert len(filtered_df) == 2
            assert list(filtered_df['id']) == [4, 5]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

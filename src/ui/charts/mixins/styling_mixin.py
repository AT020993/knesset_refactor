"""Chart styling mixin for visualization helpers.

This mixin provides styling and helper functionality for chart classes:
- Time series configuration and normalization
- Result handling and empty data messaging
- Chart styling defaults
- Numeric column conversion
"""

from typing import Any, Dict, List

import pandas as pd
import plotly.graph_objects as go


class ChartStylingMixin:
    """Mixin providing styling and helper methods for charts.

    This mixin handles:
    - Time series SQL configuration generation
    - DataFrame normalization for time series
    - Empty result handling with user feedback
    - Common chart styling patterns
    - Data type conversion helpers
    """

    def show_error(self, message: str, level: str = "error") -> None:
        """Display error - must be implemented by host class."""
        raise NotImplementedError("Host class must implement show_error")

    # ============== Time Series Helpers ==============

    @staticmethod
    def get_time_period_config(date_column: str) -> Dict[str, Dict[str, str]]:
        """Return time period SQL configurations for time series charts.

        Consolidates the duplicated time_configs dict that was repeated
        across multiple time-based chart methods.

        Args:
            date_column: The date column to use in SQL (e.g., "q.SubmitDate").

        Returns:
            Dict with Monthly, Quarterly, Yearly configurations:
                - 'sql': SQL expression for the time period grouping
                - 'label': Human-readable axis label
        """
        return {
            "Monthly": {
                "sql": f"strftime(CAST({date_column} AS TIMESTAMP), '%Y-%m')",
                "label": "Year-Month"
            },
            "Quarterly": {
                "sql": (
                    f"strftime(CAST({date_column} AS TIMESTAMP), '%Y') || '-Q' || "
                    f"CAST((CAST(strftime(CAST({date_column} AS TIMESTAMP), '%m') AS INTEGER) - 1) / 3 + 1 AS VARCHAR)"
                ),
                "label": "Year-Quarter"
            },
            "Yearly": {
                "sql": f"strftime(CAST({date_column} AS TIMESTAMP), '%Y')",
                "label": "Year"
            }
        }

    @staticmethod
    def normalize_time_series_df(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame columns for time series charts.

        Converts KnessetNum and TimePeriod columns to strings for proper
        chart rendering (prevents numeric sorting issues).

        Args:
            df: DataFrame with time series data.

        Returns:
            DataFrame with normalized column types.
        """
        df = df.copy()
        if "KnessetNum" in df.columns:
            df["KnessetNum"] = df["KnessetNum"].astype(str)
        if "TimePeriod" in df.columns:
            df["TimePeriod"] = df["TimePeriod"].astype(str)
        return df

    # ============== Result Handling Helpers ==============

    def handle_empty_result(
        self,
        df: pd.DataFrame,
        entity_type: str,
        filters: Dict[str, Any],
        chart_context: str = ""
    ) -> bool:
        """Check if DataFrame is empty and show appropriate message.

        Args:
            df: The DataFrame to check.
            entity_type: Type of data (e.g., "query", "agenda", "bill").
            filters: Filter dict containing 'knesset_title'.
            chart_context: Additional context for the message (e.g., "by Year").

        Returns:
            True if DataFrame is empty (caller should return None), False otherwise.
        """
        if df.empty:
            context_str = f" to visualize '{chart_context}'" if chart_context else ""
            message = (
                f"No {entity_type} data found for "
                f"'{filters.get('knesset_title', 'selected filters')}'"
                f"{context_str} with the current filters."
            )
            self.show_error(message, level="info")
            return True
        return False

    # ============== Data Conversion Helpers ==============

    @staticmethod
    def ensure_numeric_columns(
        df: pd.DataFrame,
        columns: List[str],
        fillna: Any = 0
    ) -> pd.DataFrame:
        """Convert columns to numeric type with safe error handling.

        Reduces code duplication for the common pattern:
            df["Col"] = pd.to_numeric(df["Col"], errors="coerce").fillna(0)

        Args:
            df: The DataFrame to modify.
            columns: List of column names to convert.
            fillna: Value to use for NaN values (default: 0).

        Returns:
            DataFrame with specified columns converted to numeric.
        """
        df = df.copy()
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(fillna)
        return df

    # ============== Chart Styling Helpers ==============

    @staticmethod
    def apply_pie_chart_defaults(fig: go.Figure) -> go.Figure:
        """Apply standard pie chart styling.

        Consolidates the repeated pattern for consistent pie chart appearance:
            - Text positioned inside slices
            - Shows percentage and label
            - Centered title

        Args:
            fig: Plotly Figure to style.

        Returns:
            Styled Figure.
        """
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(title_x=0.5)
        return fig

"""Chart configuration and color schemes."""

from typing import Dict

try:
    import plotly.express as px

    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False
    px = None


class ChartConfig:
    """Chart styling and configuration."""

    # Color schemes
    KNESSET_COLOR_SEQUENCE = (
        px.colors.qualitative.Plotly
        if HAS_PLOTLY
        else [
            "#636EFA",
            "#EF553B",
            "#00CC96",
            "#AB63FA",
            "#FFA15A",
            "#19D3F3",
            "#FF6692",
            "#B6E880",
            "#FF97FF",
            "#FECB52",
        ]
    )

    COALITION_OPPOSITION_COLORS = {"Coalition": "#1f77b4", "Opposition": "#ff7f0e", "Unknown": "#7f7f7f", "": "#c7c7c7"}

    ANSWER_STATUS_COLORS = {
        "Answered": "#2ca02c",
        "Not Answered": "#d62728",
        "Other/In Progress": "#ffbb78",
        "Unknown": "#c7c7c7",
    }

    GENERAL_STATUS_COLORS = {
        "Approved": "#2ca02c",
        "Passed": "#2ca02c",
        "נענתה": "#2ca02c",
        "Rejected": "#d62728",
        "Failed": "#d62728",
        "לא נענתה": "#d62728",
        "נדחתה": "#d62728",
        "In Progress": "#ffbb78",
        "בטיפול": "#ffbb78",
        "הועברה": "#ffbb78",
        "הוסרה": "#ffbb78",
        "Unknown": "#c7c7c7",
    }

    QUERY_TYPE_COLORS = {"רגילה": "#1f77b4", "דחופה": "#ff7f0e", "ישירה": "#2ca02c"}

    # Chart types and requirements
    CHART_TYPES = [
        "Bar Chart",
        "Line Chart",
        "Scatter Plot",
        "Histogram",
        "Box Plot",
        "Violin Plot",
        "Pie Chart",
        "Sunburst Chart",
        "Treemap",
        "Heatmap",
        "Correlation Matrix",
    ]

    CHART_REQUIREMENTS = {
        "Bar Chart": {"x": "categorical", "y": "any", "color": "optional"},
        "Line Chart": {"x": "any", "y": "numeric", "color": "optional"},
        "Scatter Plot": {"x": "numeric", "y": "numeric", "color": "optional", "size": "optional"},
        "Histogram": {"x": "numeric", "color": "optional"},
        "Box Plot": {"x": "categorical", "y": "numeric", "color": "optional"},
        "Violin Plot": {"x": "categorical", "y": "numeric", "color": "optional"},
        "Pie Chart": {"values": "numeric", "names": "categorical"},
        "Sunburst Chart": {"values": "numeric", "path": "categorical_list"},
        "Treemap": {"values": "numeric", "path": "categorical_list"},
        "Heatmap": {"x": "categorical", "y": "categorical", "z": "numeric"},
        "Correlation Matrix": {"columns": "numeric_list"},
    }

    NUMERIC_Y_REQUIRED = ["Line Chart", "Scatter Plot", "Box Plot", "Violin Plot"]
    NON_XY_CHARTS = ["Pie Chart", "Sunburst Chart", "Treemap", "Correlation Matrix"]

    # Default styling
    DEFAULT_CONFIG = {
        "title_font_size": 18,
        "title_font_family": "Arial",
        "axis_label_font_size": 12,
        "legend_orientation": "v",
        "legend_x": 1.02,
        "legend_y": 1,
        "color_palette": "plotly",
        "marker_opacity": 0.7,
    }

    @classmethod
    def get_color_scheme(cls, scheme_name: str) -> Dict[str, str]:
        """Get a specific color scheme."""
        schemes = {
            "coalition_opposition": cls.COALITION_OPPOSITION_COLORS,
            "answer_status": cls.ANSWER_STATUS_COLORS,
            "general_status": cls.GENERAL_STATUS_COLORS,
            "query_type": cls.QUERY_TYPE_COLORS,
        }
        return schemes.get(scheme_name, {})

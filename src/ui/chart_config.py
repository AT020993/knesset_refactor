"""
Chart Builder Configuration and Constants
"""
import plotly.express as px

# Define available Plotly qualitative color scales
PLOTLY_COLOR_SCALES = {
    "Plotly": px.colors.qualitative.Plotly,
    "D3": px.colors.qualitative.D3,
    "G10": px.colors.qualitative.G10,
    "T10": px.colors.qualitative.T10,
    "Alphabet": px.colors.qualitative.Alphabet,
    "Dark24": px.colors.qualitative.Dark24,
    "Light24": px.colors.qualitative.Light24,
    "Set1": px.colors.qualitative.Set1,
    "Pastel1": px.colors.qualitative.Pastel1,
    "Dark2": px.colors.qualitative.Dark2,
}

PLOTLY_FONT_FAMILIES = [
    "Arial", "Balto", "Courier New", "Droid Sans", "Droid Serif", 
    "Droid Sans Mono", "Gravitas One", "Old Standard TT", "Open Sans", 
    "Overpass", "PT Sans Narrow", "Raleway", "Times New Roman"
]

CHART_TYPES = ["bar", "line", "scatter", "pie", "histogram", "box"]

BARMODE_OPTIONS = ["relative", "group", "overlay", "stack"]
LEGEND_ORIENTATIONS = ["v", "h"]

# Default values for chart aesthetics
DEFAULT_CONFIG = {
    "title_font_size": 20,
    "title_font_family": "Open Sans",
    "axis_label_font_size": 14,
    "legend_orientation": "v",
    "legend_x": 1.02,
    "legend_y": 1.0,
    "color_palette": "Plotly",
    "marker_opacity": 1.0,
    "barmode": "stack",
    "log_x": False,
    "log_y": False,
}

# Chart type validation rules
CHART_REQUIREMENTS = {
    "pie": {"required": ["names", "values"], "optional": ["color", "hover_name"]},
    "histogram": {"required": ["x"], "optional": ["color", "facet_row", "facet_col", "hover_name"]},
    "box": {"required_any": ["x", "y"], "optional": ["color", "facet_row", "facet_col", "hover_name"]},
    "bar": {"required": ["x", "y"], "optional": ["color", "facet_row", "facet_col", "hover_name", "barmode"]},
    "line": {"required": ["x", "y"], "optional": ["color", "facet_row", "facet_col", "hover_name"]},
    "scatter": {"required": ["x", "y"], "optional": ["color", "size", "facet_row", "facet_col", "hover_name", "marker_opacity"]},
}

# Charts that require numeric Y-axis
NUMERIC_Y_REQUIRED = ["line", "scatter"]

# Charts that don't use standard X/Y axes
NON_XY_CHARTS = ["pie"]
"""Application-wide settings and configuration."""

from pathlib import Path
from typing import Optional


class Settings:
    """Centralized application settings."""

    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    DATA_DIR = PROJECT_ROOT / "data"
    LOGS_DIR = PROJECT_ROOT / "logs"

    # Default database settings
    DEFAULT_DB_PATH = DATA_DIR / "warehouse.duckdb"
    PARQUET_DIR = DATA_DIR / "parquet"
    RESUME_STATE_FILE = DATA_DIR / ".resume_state.json"
    FACTION_COALITION_STATUS_FILE = DATA_DIR / "faction_coalition_status.csv"

    # UI settings
    MAX_ROWS_FOR_CHART_BUILDER = 50000
    MAX_UNIQUE_VALUES_FOR_FACET = 100

    # Performance settings
    QUERY_TIMEOUT_SECONDS = 60
    CONNECTION_POOL_SIZE = 8
    PAGE_SIZE = 100
    MAX_RETRIES = 8

    # Feature flags
    ENABLE_BACKGROUND_MONITORING = False
    ENABLE_CONNECTION_DASHBOARD = True
    ENABLE_LEGACY_COMPATIBILITY = True

    @classmethod
    def ensure_directories(cls) -> None:
        """Ensure all required directories exist."""
        cls.DATA_DIR.mkdir(parents=True, exist_ok=True)
        cls.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        cls.PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_db_path(cls, custom_path: Optional[Path] = None) -> Path:
        """Get the database path, with optional override."""
        return custom_path or cls.DEFAULT_DB_PATH

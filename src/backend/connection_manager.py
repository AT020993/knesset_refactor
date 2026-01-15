"""
Database connection management utility with proper resource cleanup and monitoring.

Provides context managers and connection monitoring to prevent connection leaks.

Core API:
- get_db_connection(): Context manager for safe database connections
- safe_execute_query(): Execute queries with error handling
- cached_query_with_connection(): Execute cached queries

Diagnostics (see connection_diagnostics.py):
- monitor_connection_health(): Get connection health metrics
- start_background_monitoring(): Enable leak detection

UI Dashboard (see ui.renderers.data_refresh.connection_dashboard):
- render_connection_dashboard(): Streamlit dashboard for connection monitoring

Note: This module uses callback functions for UI notifications instead of
direct Streamlit imports, enabling proper separation of concerns and testability.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
import weakref
from pathlib import Path
from typing import Any, Callable, Generator, List, Optional

import duckdb


# Type alias for UI notification callbacks
# Callback signature: (message: str, level: str) -> None
# Level can be: "info", "warning", "error"
UINotifyCallback = Callable[[str, str], None]


def _get_streamlit_notifier() -> Optional[UINotifyCallback]:
    """
    Get a Streamlit-based notification callback if available.

    Returns None if Streamlit is not available (e.g., in CLI or tests).
    """
    try:
        import streamlit as st

        def notify(message: str, level: str) -> None:
            if level == "info":
                st.info(message)
            elif level == "warning":
                st.warning(message)
            elif level == "error":
                st.error(message)

        return notify
    except ImportError:
        return None


def _default_ui_notifier() -> UINotifyCallback:
    """
    Get the default UI notification callback.

    Uses Streamlit if available, otherwise a no-op function.
    """
    streamlit_notifier = _get_streamlit_notifier()
    if streamlit_notifier:
        return streamlit_notifier

    # No-op callback for non-Streamlit environments
    return lambda msg, level: None


class ConnectionMonitor:
    """Monitors database connection lifecycle to detect leaks.

    Uses weak references to detect connections that are garbage collected
    without explicit close(), which indicates a connection leak.
    """

    def __init__(self):
        self._active_connections: dict[int, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._logger = logging.getLogger(__name__)

    def register_connection(
        self, conn: duckdb.DuckDBPyConnection, db_path: str
    ) -> None:
        """Register a new connection for monitoring."""
        with self._lock:
            conn_id = id(conn)
            self._active_connections[conn_id] = {
                "db_path": db_path,
                "created_at": time.time(),
                "thread_id": threading.get_ident(),
                "weakref": weakref.ref(conn, self._connection_finalized),
            }
            self._logger.debug(f"Registered connection {conn_id} to {db_path}")

    def unregister_connection(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Unregister a connection when properly closed."""
        with self._lock:
            conn_id = id(conn)
            if conn_id in self._active_connections:
                db_path = self._active_connections[conn_id]["db_path"]
                del self._active_connections[conn_id]
                self._logger.debug(f"Unregistered connection {conn_id} to {db_path}")

    def _connection_finalized(self, weakref_obj) -> None:
        """Called when a connection is garbage collected without proper cleanup."""
        with self._lock:
            for conn_id, info in list(self._active_connections.items()):
                if info["weakref"] is weakref_obj:
                    self._logger.warning(
                        f"Connection {conn_id} to {info['db_path']} was garbage collected without explicit close()"
                    )
                    del self._active_connections[conn_id]
                    break

    def get_active_connections(self) -> dict[int, dict[str, Any]]:
        """Get information about currently active connections."""
        with self._lock:
            return dict(self._active_connections)

    def log_connection_stats(self) -> None:
        """Log current connection statistics."""
        stats = self.get_active_connections()
        if stats:
            self._logger.warning(f"Active connections: {len(stats)}")
            for conn_id, info in stats.items():
                age = time.time() - info["created_at"]
                self._logger.warning(
                    f"  Connection {conn_id}: {info['db_path']}, age: {age:.1f}s, thread: {info['thread_id']}"
                )
        else:
            self._logger.debug("No active connections")


# Global connection monitor instance
_connection_monitor = ConnectionMonitor()


@contextlib.contextmanager
def get_db_connection(
    db_path: Path,
    read_only: bool = True,
    logger_obj: logging.Logger | None = None,
    ui_notify: UINotifyCallback | None = None,
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Context manager for DuckDB connections with proper resource cleanup.

    This is the recommended way to work with database connections.
    The connection is automatically closed when the context exits.

    Args:
        db_path: Path to the database file
        read_only: Whether to open in read-only mode
        logger_obj: Optional logger for debug messages
        ui_notify: Optional callback for UI notifications (info/warning/error)
                   If None, uses Streamlit if available, otherwise no-op

    Yields:
        DuckDB connection that will be automatically closed

    Example:
        with get_db_connection(db_path) as conn:
            result = conn.execute("SELECT * FROM table").df()
    """
    if logger_obj is None:
        logger_obj = logging.getLogger(__name__)

    if ui_notify is None:
        ui_notify = _default_ui_notifier()

    # Handle missing database file
    if not db_path.exists() and read_only:
        logger_obj.warning(
            f"Database {db_path} does not exist. Using in-memory fallback."
        )
        ui_notify(
            f"Database {db_path} does not exist. Please run a data refresh first.",
            "warning"
        )

        conn = duckdb.connect(database=":memory:", read_only=False)
        _connection_monitor.register_connection(conn, ":memory:")
        try:
            yield conn
        finally:
            _connection_monitor.unregister_connection(conn)
            conn.close()
            logger_obj.debug("Closed in-memory fallback connection")
        return

    elif not db_path.exists() and not read_only:
        logger_obj.info(f"Database {db_path} does not exist. It will be created.")
        ui_notify(f"Database {db_path} will be created during write operation.", "info")

    conn = None
    try:
        conn = duckdb.connect(database=db_path.as_posix(), read_only=read_only)
        _connection_monitor.register_connection(conn, str(db_path))

        # Test connection
        conn.execute("SELECT 1")
        logger_obj.debug(
            f"Successfully connected to DuckDB at {db_path} (read_only={read_only})"
        )

        yield conn

    except Exception as e:
        logger_obj.error(
            f"Error connecting to database at {db_path}: {e}", exc_info=True
        )
        ui_notify(f"Database connection error: {e}", "error")

        # Provide fallback in-memory connection for read operations
        if read_only:
            if conn:
                _connection_monitor.unregister_connection(conn)
                conn.close()

            conn = duckdb.connect(database=":memory:", read_only=False)
            _connection_monitor.register_connection(conn, ":memory:")
            logger_obj.info("Using in-memory fallback connection due to error")
            yield conn
        else:
            raise

    finally:
        if conn:
            _connection_monitor.unregister_connection(conn)
            conn.close()
            logger_obj.debug(f"Connection to {db_path} closed successfully")


def safe_execute_query(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    logger_obj: logging.Logger | None = None,
    params: Optional[List[Any]] = None,
    ui_notify: UINotifyCallback | None = None,
) -> Any:
    """
    Safely execute a query with proper error handling.

    Args:
        conn: DuckDB connection
        query: SQL query to execute
        logger_obj: Optional logger for debug messages
        params: Optional list of parameters for the query
        ui_notify: Optional callback for UI notifications

    Returns:
        Query result dataframe or empty dataframe on error
    """
    import pandas as pd

    if logger_obj is None:
        logger_obj = logging.getLogger(__name__)

    if ui_notify is None:
        ui_notify = _default_ui_notifier()

    try:
        if params:
            logger_obj.debug(f"Executing query: {query[:200]}... with params: {params}")
            return conn.execute(query, params).df()
        else:
            logger_obj.debug(f"Executing query: {query[:200]}...")
            return conn.execute(query).df()
    except Exception as e:
        query_info = f"Query: {query}"
        if params:
            query_info += f"\nParams: {params}"
        logger_obj.error(f"Query execution error: {e}\n{query_info}", exc_info=True)
        ui_notify(f"Query execution error: {e}", "error")
        return pd.DataFrame()


def get_connection_stats() -> dict[str, Any]:
    """Get current connection monitoring statistics."""
    active_conns = _connection_monitor.get_active_connections()
    return {"active_count": len(active_conns), "connections": active_conns}


def log_connection_leaks() -> None:
    """Log any potential connection leaks for debugging."""
    _connection_monitor.log_connection_stats()


def _cache_data_decorator(ttl=3600):
    """Decorator that uses Streamlit cache if available, otherwise no-op."""

    def decorator(func):
        try:
            import streamlit as st
            if hasattr(st, "cache_data"):
                return st.cache_data(ttl=ttl)(func)
        except ImportError:
            pass
        return func

    return decorator


@_cache_data_decorator(ttl=3600)
def cached_query_with_connection(
    db_path_str: str, query: str, read_only: bool = True
) -> Any:
    """
    Execute a cached query with proper connection management.

    Note: db_path must be string for Streamlit caching compatibility.

    Args:
        db_path_str: String path to the database file
        query: SQL query to execute
        read_only: Whether to open in read-only mode

    Returns:
        Query result dataframe
    """
    import pandas as pd

    db_path = Path(db_path_str)
    logger_obj = logging.getLogger(__name__)

    try:
        with get_db_connection(
            db_path, read_only=read_only, logger_obj=logger_obj
        ) as conn:
            return safe_execute_query(conn, query, logger_obj)
    except Exception as e:
        logger_obj.error(f"Error in cached_query_with_connection: {e}", exc_info=True)
        return pd.DataFrame()


# Legacy compatibility function
def connect_db(
    db_path: Path,
    read_only: bool = True,
    _logger_obj: logging.Logger | None = None,
    ui_notify: UINotifyCallback | None = None,
) -> duckdb.DuckDBPyConnection:
    """
    Legacy connection function - DEPRECATED.
    Use get_db_connection() context manager instead to prevent leaks.

    This function is kept for backward compatibility but will log warnings.
    """
    if _logger_obj is None:
        _logger_obj = logging.getLogger(__name__)

    if ui_notify is None:
        ui_notify = _default_ui_notifier()

    _logger_obj.warning(
        "connect_db() is deprecated. Use get_db_connection() context manager to prevent connection leaks."
    )

    # For legacy compatibility, we still return a connection but warn about proper usage
    if not db_path.exists() and read_only:
        _logger_obj.warning(
            f"Database {db_path} does not exist. Query execution will fail."
        )
        ui_notify(
            f"Database {db_path} does not exist. Please run a data refresh first.",
            "warning"
        )
        conn = duckdb.connect(database=":memory:", read_only=False)
        _connection_monitor.register_connection(conn, ":memory:")
        return conn

    try:
        conn = duckdb.connect(database=db_path.as_posix(), read_only=read_only)
        _connection_monitor.register_connection(conn, str(db_path))
        conn.execute("SELECT 1")  # Test connection
        _logger_obj.debug(
            f"Successfully connected to DuckDB at {db_path} (read_only={read_only})"
        )
        return conn
    except Exception as e:
        _logger_obj.error(
            f"Error connecting to database at {db_path}: {e}", exc_info=True
        )
        ui_notify(f"Database connection error: {e}", "error")
        conn = duckdb.connect(database=":memory:", read_only=False)
        _connection_monitor.register_connection(conn, ":memory:")
        return conn


# ============================================================
# Backward compatibility re-exports from connection_diagnostics
# ============================================================

def monitor_connection_health() -> dict[str, Any]:
    """Monitor connection health. (Backward compatibility - see connection_diagnostics.py)"""
    from .connection_diagnostics import monitor_connection_health as _monitor
    return _monitor()


def create_connection_dashboard() -> None:
    """Create connection dashboard.

    DEPRECATED: Use ui.renderers.data_refresh.render_connection_dashboard() instead.
    This backward compatibility wrapper imports from the UI layer.
    """
    import warnings
    warnings.warn(
        "create_connection_dashboard() is deprecated. "
        "Use ui.renderers.data_refresh.render_connection_dashboard() instead.",
        DeprecationWarning,
        stacklevel=2
    )
    from ui.renderers.data_refresh import render_connection_dashboard
    render_connection_dashboard()


def start_background_monitoring(interval_seconds: int = 60) -> None:
    """Start background monitoring. (Backward compatibility - see connection_diagnostics.py)"""
    from .connection_diagnostics import start_background_monitoring as _start
    _start(interval_seconds)


def stop_background_monitoring() -> None:
    """Stop background monitoring. (Backward compatibility - see connection_diagnostics.py)"""
    from .connection_diagnostics import stop_background_monitoring as _stop
    _stop()

"""
Database connection management utility with proper resource cleanup and monitoring.
Provides context managers and connection pooling to prevent connection leaks.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
import weakref
from pathlib import Path
from typing import Any, Generator, List, Optional

import duckdb

try:
    import streamlit as st

    _STREAMLIT_AVAILABLE = True
except ImportError:
    _STREAMLIT_AVAILABLE = False

    # Create a mock st object for testing
    class MockStreamlit:
        def warning(self, msg):
            pass

        def info(self, msg):
            pass

        def error(self, msg):
            pass

    st = MockStreamlit()


class ConnectionMonitor:
    """Monitors database connection lifecycle to detect leaks."""

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
    db_path: Path, read_only: bool = True, logger_obj: logging.Logger | None = None
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Context manager for DuckDB connections with proper resource cleanup.

    Args:
        db_path: Path to the database file
        read_only: Whether to open in read-only mode
        logger_obj: Optional logger for debug messages

    Yields:
        DuckDB connection that will be automatically closed

    Example:
        with get_db_connection(db_path) as conn:
            result = conn.execute("SELECT * FROM table").df()
    """
    if logger_obj is None:
        logger_obj = logging.getLogger(__name__)

    # Handle missing database file
    if not db_path.exists() and read_only:
        logger_obj.warning(
            f"Database {db_path} does not exist. Using in-memory fallback."
        )
        st.warning(
            f"Database {db_path} does not exist. Please run a data refresh first."
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
        st.info(f"Database {db_path} will be created during write operation.")

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
        st.error(f"Database connection error: {e}")

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
) -> Any:
    """
    Safely execute a query with proper error handling.

    Args:
        conn: DuckDB connection
        query: SQL query to execute
        logger_obj: Optional logger for debug messages
        params: Optional list of parameters for the query

    Returns:
        Query result dataframe or empty dataframe on error
    """
    import pandas as pd

    if logger_obj is None:
        logger_obj = logging.getLogger(__name__)

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
        st.error(f"Query execution error: {e}")
        return pd.DataFrame()


def get_connection_stats() -> dict[str, Any]:
    """Get current connection monitoring statistics."""
    active_conns = _connection_monitor.get_active_connections()
    return {"active_count": len(active_conns), "connections": active_conns}


def log_connection_leaks() -> None:
    """Log any potential connection leaks for debugging."""
    _connection_monitor.log_connection_stats()


def monitor_connection_health() -> dict[str, any]:
    """
    Monitor overall connection health and return diagnostic information.

    Returns:
        Dictionary with connection health metrics
    """
    stats = get_connection_stats()
    current_time = time.time()

    health_info = {
        "total_active": stats["active_count"],
        "connections_by_db": {},
        "long_running_connections": [],
        "connections_by_thread": {},
        "oldest_connection_age": 0,
        "health_status": "healthy",
    }

    for conn_id, info in stats["connections"].items():
        db_path = info["db_path"]
        thread_id = info["thread_id"]
        age = current_time - info["created_at"]

        # Group by database
        if db_path not in health_info["connections_by_db"]:
            health_info["connections_by_db"][db_path] = 0
        health_info["connections_by_db"][db_path] += 1

        # Group by thread
        if thread_id not in health_info["connections_by_thread"]:
            health_info["connections_by_thread"][thread_id] = 0
        health_info["connections_by_thread"][thread_id] += 1

        # Track oldest connection
        if age > health_info["oldest_connection_age"]:
            health_info["oldest_connection_age"] = age

        # Identify long-running connections (> 5 minutes)
        if age > 300:
            health_info["long_running_connections"].append(
                {
                    "conn_id": conn_id,
                    "db_path": db_path,
                    "age_seconds": age,
                    "thread_id": thread_id,
                }
            )

    # Determine health status
    if stats["active_count"] > 50:
        health_info["health_status"] = "critical"
    elif stats["active_count"] > 20 or len(health_info["long_running_connections"]) > 5:
        health_info["health_status"] = "warning"
    elif len(health_info["long_running_connections"]) > 0:
        health_info["health_status"] = "attention"

    return health_info


def create_connection_dashboard() -> None:
    """
    Create a Streamlit dashboard component for monitoring database connections.
    Should only be called from within Streamlit apps.
    """
    try:
        import streamlit as st

        st.subheader("🔌 Database Connection Monitor")

        health_info = monitor_connection_health()

        # Health status indicator
        status_colors = {
            "healthy": "🟢",
            "attention": "🟡",
            "warning": "🟠",
            "critical": "🔴",
        }

        status_icon = status_colors.get(health_info["health_status"], "⚪")
        st.write(f"**Status:** {status_icon} {health_info['health_status'].title()}")

        # Metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Active Connections", health_info["total_active"])

        with col2:
            st.metric("Databases Connected", len(health_info["connections_by_db"]))

        with col3:
            st.metric("Long-Running", len(health_info["long_running_connections"]))

        # Details in expandable sections
        if health_info["total_active"] > 0:
            with st.expander("📊 Connection Details"):
                # Connections by database
                if health_info["connections_by_db"]:
                    st.write("**Connections by Database:**")
                    for db_path, count in health_info["connections_by_db"].items():
                        st.write(f"- `{db_path}`: {count} connection(s)")

                # Connections by thread
                if health_info["connections_by_thread"]:
                    st.write("**Connections by Thread:**")
                    for thread_id, count in health_info[
                        "connections_by_thread"
                    ].items():
                        st.write(f"- Thread `{thread_id}`: {count} connection(s)")

                # Long-running connections
                if health_info["long_running_connections"]:
                    st.warning("**Long-Running Connections (>5 min):**")
                    for conn in health_info["long_running_connections"]:
                        age_min = conn["age_seconds"] / 60
                        st.write(
                            f"- Connection `{conn['conn_id']}` to `{conn['db_path']}`: {age_min:.1f} minutes"
                        )

        # Actions
        col1, col2 = st.columns(2)

        with col1:
            if st.button("🔄 Refresh Stats"):
                st.rerun()

        with col2:
            if st.button("📋 Log Connection Details"):
                log_connection_leaks()
                st.success("Connection details logged to application logs")

    except ImportError:
        logging.getLogger(__name__).warning(
            "Streamlit not available for connection dashboard"
        )


# Background monitoring thread (optional)
_monitoring_enabled = False
_monitoring_thread = None


def start_background_monitoring(interval_seconds: int = 60) -> None:
    """
    Start background monitoring of database connections.

    Args:
        interval_seconds: How often to check for connection leaks
    """
    global _monitoring_enabled, _monitoring_thread

    if _monitoring_enabled:
        return

    logger = logging.getLogger(__name__)

    def monitor_loop():
        while _monitoring_enabled:
            try:
                health_info = monitor_connection_health()

                # Log warnings for concerning states
                if health_info["health_status"] in ["warning", "critical"]:
                    logger.warning(
                        f"Database connection health: {health_info['health_status']}"
                    )
                    logger.warning(f"Active connections: {health_info['total_active']}")

                    if health_info["long_running_connections"]:
                        logger.warning(
                            f"Long-running connections: {len(health_info['long_running_connections'])}"
                        )
                        for conn in health_info["long_running_connections"]:
                            age_min = conn["age_seconds"] / 60
                            logger.warning(
                                f"  - Connection to {conn['db_path']}: {age_min:.1f} minutes old"
                            )

                # Log periodic stats at debug level
                elif health_info["total_active"] > 0:
                    logger.debug(
                        f"Database connections active: {health_info['total_active']}"
                    )

                time.sleep(interval_seconds)

            except Exception as e:
                logger.error(f"Error in connection monitoring: {e}", exc_info=True)
                time.sleep(interval_seconds)

    _monitoring_enabled = True
    _monitoring_thread = threading.Thread(target=monitor_loop, daemon=True)
    _monitoring_thread.start()

    logger.info(
        f"Started background connection monitoring (interval: {interval_seconds}s)"
    )


def stop_background_monitoring() -> None:
    """Stop background monitoring of database connections."""
    global _monitoring_enabled

    if not _monitoring_enabled:
        return

    _monitoring_enabled = False

    if _monitoring_thread and _monitoring_thread.is_alive():
        _monitoring_thread.join(timeout=5)

    logger = logging.getLogger(__name__)
    logger.info("Stopped background connection monitoring")


def _cache_data_decorator(ttl=3600):
    """Decorator that uses Streamlit cache if available, otherwise no-op."""

    def decorator(func):
        if _STREAMLIT_AVAILABLE and hasattr(st, "cache_data"):
            return st.cache_data(ttl=ttl)(func)
        return func

    return decorator


@_cache_data_decorator(ttl=3600)
def cached_query_with_connection(
    db_path_str: str, query: str, read_only: bool = True
) -> Any:
    """
    Execute a cached query with proper connection management.

    Note: db_path must be string for Streamlit caching compatibility.
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


# Legacy compatibility functions
def connect_db(
    db_path: Path, read_only: bool = True, _logger_obj: logging.Logger | None = None
) -> duckdb.DuckDBPyConnection:
    """
    Legacy connection function - DEPRECATED.
    Use get_db_connection() context manager instead to prevent leaks.

    This function is kept for backward compatibility but will log warnings.
    """
    if _logger_obj is None:
        _logger_obj = logging.getLogger(__name__)

    _logger_obj.warning(
        "connect_db() is deprecated. Use get_db_connection() context manager to prevent connection leaks."
    )

    # For legacy compatibility, we still return a connection but warn about proper usage
    if not db_path.exists() and read_only:
        _logger_obj.warning(
            f"Database {db_path} does not exist. Query execution will fail."
        )
        st.warning(
            f"Database {db_path} does not exist. Please run a data refresh first."
        )
        conn = duckdb.connect(database=":memory:", read_only=True)
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
        st.error(f"Database connection error: {e}")
        conn = duckdb.connect(database=":memory:", read_only=True)
        _connection_monitor.register_connection(conn, ":memory:")
        return conn

"""
Connection diagnostics and health monitoring utilities.

This module provides diagnostic and monitoring tools for database connections:
- Health monitoring and metrics
- Background monitoring for leak detection

Note: UI rendering for connection dashboards has been moved to the UI layer.
See: ui.renderers.data_refresh.connection_dashboard.render_connection_dashboard()
"""

import logging
import threading
import time
from typing import Any, Dict

# Import the global monitor from connection_manager
from .connection_manager import (
    _connection_monitor,
    get_connection_stats,
)


def monitor_connection_health() -> Dict[str, Any]:
    """
    Monitor overall connection health and return diagnostic information.

    Returns:
        Dictionary with connection health metrics including:
        - total_active: Number of active connections
        - connections_by_db: Connections grouped by database path
        - long_running_connections: Connections older than 5 minutes
        - connections_by_thread: Connections grouped by thread ID
        - oldest_connection_age: Age of the oldest connection in seconds
        - health_status: "healthy", "attention", "warning", or "critical"
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


# Background monitoring state
_monitoring_enabled = False
_monitoring_thread = None


def start_background_monitoring(interval_seconds: int = 60) -> None:
    """
    Start background monitoring of database connections.

    This creates a daemon thread that periodically checks connection health
    and logs warnings for concerning states.

    Args:
        interval_seconds: How often to check for connection leaks (default: 60)
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


def is_monitoring_active() -> bool:
    """Check if background monitoring is currently active."""
    return _monitoring_enabled

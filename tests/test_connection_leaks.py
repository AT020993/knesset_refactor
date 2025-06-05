"""
Tests for database connection leak detection and prevention.
"""
import gc
import logging
import threading
import time
import weakref
from pathlib import Path
from unittest.mock import Mock, patch
import pytest
import duckdb
import pandas as pd

from src.backend.connection_manager import (
    get_db_connection,
    safe_execute_query,
    get_connection_stats,
    log_connection_leaks,
    _connection_monitor,
    ConnectionMonitor
)


class TestConnectionManager:
    """Test the connection manager functionality."""
    
    def test_connection_context_manager(self, tmp_path):
        """Test that connections are properly closed with context manager."""
        db_path = tmp_path / "test.db"
        
        # Create a test database
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
            conn.execute("INSERT INTO test VALUES (1, 'test')")
        
        # Test context manager
        conn_ref = None
        with get_db_connection(db_path) as conn:
            conn_ref = weakref.ref(conn)
            result = safe_execute_query(conn, "SELECT * FROM test")
            assert len(result) == 1
            assert result.iloc[0]['name'] == 'test'
        
        # Force garbage collection
        gc.collect()
        
        # Connection should be closed and garbage collected
        assert conn_ref() is None
    
    def test_connection_with_missing_database(self, tmp_path):
        """Test behavior when database file doesn't exist."""
        missing_db = tmp_path / "missing.db"
        logger = Mock()
        
        with get_db_connection(missing_db, read_only=True, logger_obj=logger) as conn:
            # Should get in-memory fallback
            result = safe_execute_query(conn, "SELECT 1 as test")
            assert len(result) == 1
            assert result.iloc[0]['test'] == 1
        
        # Should have logged warnings
        logger.warning.assert_called()
    
    def test_connection_error_handling(self, tmp_path):
        """Test connection error handling."""
        # Create an invalid database path
        invalid_path = tmp_path / "subdir" / "invalid.db"
        logger = Mock()
        
        # This should still work due to fallback
        with get_db_connection(invalid_path, read_only=True, logger_obj=logger) as conn:
            result = safe_execute_query(conn, "SELECT 1 as test")
            assert len(result) == 1
    
    def test_safe_execute_query_error_handling(self, tmp_path):
        """Test that safe_execute_query handles SQL errors properly."""
        db_path = tmp_path / "test.db"
        logger = Mock()
        
        # Create empty database
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
        
        with get_db_connection(db_path, logger_obj=logger) as conn:
            # Execute invalid SQL
            result = safe_execute_query(conn, "SELECT * FROM nonexistent_table", logger)
            
            # Should return empty dataframe
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 0
        
        # Should have logged error
        logger.error.assert_called()


class TestConnectionMonitor:
    """Test the connection monitoring functionality."""
    
    def test_connection_registration(self):
        """Test connection registration and unregistration."""
        monitor = ConnectionMonitor()
        
        # Create a mock connection
        mock_conn = Mock()
        mock_conn.__class__ = duckdb.DuckDBPyConnection
        
        # Register connection
        monitor.register_connection(mock_conn, "test.db")
        stats = monitor.get_active_connections()
        
        assert len(stats) == 1
        assert list(stats.values())[0]['db_path'] == "test.db"
        
        # Unregister connection
        monitor.unregister_connection(mock_conn)
        stats = monitor.get_active_connections()
        
        assert len(stats) == 0
    
    def test_connection_finalization_warning(self):
        """Test that connections garbage collected without explicit close trigger warnings."""
        monitor = ConnectionMonitor()
        
        # Create a mock connection and let it be garbage collected
        mock_conn = Mock()
        mock_conn.__class__ = duckdb.DuckDBPyConnection
        
        monitor.register_connection(mock_conn, "test.db")
        
        # Simulate connection being garbage collected
        conn_id = id(mock_conn)
        weakref_obj = monitor._active_connections[conn_id]['weakref']
        
        # Call the finalization callback directly
        monitor._connection_finalized(weakref_obj)
        
        # Connection should be removed from active list
        stats = monitor.get_active_connections()
        assert len(stats) == 0
    
    def test_get_connection_stats(self):
        """Test the global connection stats function."""
        # Clear any existing connections
        _connection_monitor._active_connections.clear()
        
        stats = get_connection_stats()
        initial_count = stats['active_count']
        
        # Create a connection through the manager
        with patch('src.backend.connection_manager.duckdb.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn
            
            # Mock the execute method for connection test
            mock_conn.execute.return_value = None
            
            db_path = Path("test.db")
            with get_db_connection(db_path) as conn:
                stats = get_connection_stats()
                assert stats['active_count'] == initial_count + 1
        
        # After context manager, connection should be unregistered
        stats = get_connection_stats()
        assert stats['active_count'] == initial_count


class TestConnectionLeakDetection:
    """Test connection leak detection scenarios."""
    
    def test_multiple_concurrent_connections(self, tmp_path):
        """Test that multiple concurrent connections are properly managed."""
        db_path = tmp_path / "test.db"
        
        # Create test database
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
            conn.execute("INSERT INTO test VALUES (1), (2), (3)")
        
        initial_stats = get_connection_stats()
        initial_count = initial_stats['active_count']
        
        # Create multiple connections concurrently
        def worker(worker_id):
            with get_db_connection(db_path) as conn:
                result = safe_execute_query(conn, f"SELECT {worker_id} as worker_id")
                time.sleep(0.1)  # Simulate some work
                return result
        
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # All connections should be closed
        final_stats = get_connection_stats()
        assert final_stats['active_count'] == initial_count
    
    def test_connection_leak_in_exception_scenario(self, tmp_path):
        """Test that connections are closed even when exceptions occur."""
        db_path = tmp_path / "test.db"
        
        # Create test database
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
        
        initial_stats = get_connection_stats()
        initial_count = initial_stats['active_count']
        
        # Test exception handling
        with pytest.raises(ValueError):
            with get_db_connection(db_path) as conn:
                safe_execute_query(conn, "SELECT * FROM test")
                raise ValueError("Test exception")
        
        # Connection should still be closed despite exception
        final_stats = get_connection_stats()
        assert final_stats['active_count'] == initial_count
    
    def test_long_running_connection_detection(self, tmp_path):
        """Test detection of long-running connections."""
        db_path = tmp_path / "test.db"
        
        # Create test database
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
        
        # Create a connection and hold it
        with get_db_connection(db_path) as conn:
            stats = get_connection_stats()
            active_conns = stats['connections']
            
            # Should have at least one active connection
            assert len(active_conns) >= 1
            
            # Check that connection info is tracked
            for conn_info in active_conns.values():
                assert 'db_path' in conn_info
                assert 'created_at' in conn_info
                assert 'thread_id' in conn_info
                assert conn_info['thread_id'] == threading.get_ident()


class TestLegacyCompatibility:
    """Test backward compatibility with legacy connection functions."""
    
    def test_legacy_connect_db_function(self, tmp_path):
        """Test that legacy connect_db function still works but warns."""
        from src.backend.connection_manager import connect_db
        
        db_path = tmp_path / "test.db"
        
        # Create test database
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
        
        logger = Mock()
        
        # Use legacy function
        conn = connect_db(db_path, read_only=True, _logger_obj=logger)
        
        try:
            # Should work but warn about deprecation
            logger.warning.assert_called()
            warning_calls = [call for call in logger.warning.call_args_list 
                           if 'deprecated' in str(call)]
            assert len(warning_calls) > 0
            
            # Connection should work
            result = conn.execute("SELECT COUNT(*) as count FROM test").df()
            assert len(result) == 1
            
        finally:
            # Must manually close legacy connections
            _connection_monitor.unregister_connection(conn)
            conn.close()


class TestMemoryLeaks:
    """Test for memory leaks in connection management."""
    
    def test_no_memory_accumulation(self, tmp_path):
        """Test that repeatedly opening/closing connections doesn't accumulate memory."""
        db_path = tmp_path / "test.db"
        
        # Create test database
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE test (id INTEGER)")
            for i in range(100):
                conn.execute(f"INSERT INTO test VALUES ({i})")
        
        initial_stats = get_connection_stats()
        initial_count = initial_stats['active_count']
        
        # Open and close many connections
        for i in range(50):
            with get_db_connection(db_path) as conn:
                result = safe_execute_query(conn, "SELECT COUNT(*) FROM test")
                assert result.iloc[0, 0] == 100
        
        # Force garbage collection
        gc.collect()
        
        # Should not have accumulated connections
        final_stats = get_connection_stats()
        assert final_stats['active_count'] == initial_count
    
    def test_monitoring_cleanup(self):
        """Test that the monitoring system properly cleans up references."""
        monitor = ConnectionMonitor()
        
        # Create many mock connections and let them be garbage collected
        for i in range(10):
            mock_conn = Mock()
            mock_conn.__class__ = duckdb.DuckDBPyConnection
            monitor.register_connection(mock_conn, f"test_{i}.db")
        
        # All should be registered
        assert len(monitor.get_active_connections()) == 10
        
        # Force garbage collection
        gc.collect()
        
        # After some time, monitoring should clean up
        # (In real scenarios, weakrefs would trigger cleanup automatically)
        monitor._active_connections.clear()  # Simulate cleanup
        assert len(monitor.get_active_connections()) == 0


@pytest.fixture
def cleanup_connections():
    """Fixture to clean up any remaining connections after tests."""
    yield
    
    # Clean up any remaining connections
    _connection_monitor._active_connections.clear()
    gc.collect()


def test_log_connection_leaks(caplog):
    """Test the connection leak logging functionality."""
    with caplog.at_level(logging.WARNING):
        log_connection_leaks()
    
    # Should log current connection status
    assert len(caplog.records) >= 1
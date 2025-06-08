"""
Performance tests for database operations and system bottlenecks.

This module provides tests to identify performance issues and ensure
the system meets performance requirements under various load conditions.
"""

import pytest
import time
import sys
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile
import shutil

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import duckdb
import pandas as pd
from backend.duckdb_io import DuckDBIO
from ui.queries.query_executor import QueryExecutor
from ui.queries.predefined_queries import PREDEFINED_QUERIES


class TestDatabasePerformance:
    """Test database operation performance."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database for testing."""
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test_performance.duckdb"
        yield db_path
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def sample_data(self):
        """Generate sample data for performance testing."""
        return pd.DataFrame({
            'PersonID': range(1, 1001),
            'FirstName': [f'Person{i}' for i in range(1, 1001)],
            'LastName': [f'Last{i}' for i in range(1, 1001)],
            'KnessetNum': [25] * 1000,
            'FactionID': [i % 10 + 1 for i in range(1000)]
        })
    
    def test_database_connection_performance(self, temp_db_path):
        """Test database connection establishment time."""
        connection_times = []
        
        for _ in range(10):
            start_time = time.time()
            conn = duckdb.connect(str(temp_db_path))
            conn.close()
            end_time = time.time()
            connection_times.append(end_time - start_time)
        
        avg_connection_time = sum(connection_times) / len(connection_times)
        
        # Connection should be fast (less than 100ms on average)
        assert avg_connection_time < 0.1, f"Average connection time too slow: {avg_connection_time:.3f}s"
    
    def test_table_creation_performance(self, temp_db_path, sample_data):
        """Test table creation and data insertion performance."""
        start_time = time.time()
        
        conn = duckdb.connect(str(temp_db_path))
        conn.execute("CREATE TABLE test_persons AS SELECT * FROM sample_data", {'sample_data': sample_data})
        
        end_time = time.time()
        creation_time = end_time - start_time
        
        # Table creation with 1000 rows should be fast (less than 1 second)
        assert creation_time < 1.0, f"Table creation too slow: {creation_time:.3f}s"
        
        # Verify data was inserted
        row_count = conn.execute("SELECT COUNT(*) FROM test_persons").fetchone()[0]
        assert row_count == 1000
        
        conn.close()
    
    def test_simple_query_performance(self, temp_db_path, sample_data):
        """Test simple query execution performance."""
        # Setup test data
        conn = duckdb.connect(str(temp_db_path))
        conn.execute("CREATE TABLE test_persons AS SELECT * FROM sample_data", {'sample_data': sample_data})
        
        # Test simple SELECT query
        start_time = time.time()
        result = conn.execute("SELECT * FROM test_persons WHERE KnessetNum = 25").fetchdf()
        end_time = time.time()
        
        query_time = end_time - start_time
        
        # Simple query should be very fast (less than 50ms)
        assert query_time < 0.05, f"Simple query too slow: {query_time:.3f}s"
        assert len(result) == 1000
        
        conn.close()
    
    def test_complex_query_performance(self, temp_db_path, sample_data):
        """Test complex query with joins and aggregations."""
        # Setup test data with multiple tables
        conn = duckdb.connect(str(temp_db_path))
        conn.execute("CREATE TABLE test_persons AS SELECT * FROM sample_data", {'sample_data': sample_data})
        
        # Create faction table
        faction_data = pd.DataFrame({
            'FactionID': range(1, 11),
            'FactionName': [f'Faction{i}' for i in range(1, 11)]
        })
        conn.execute("CREATE TABLE test_factions AS SELECT * FROM faction_data", {'faction_data': faction_data})
        
        # Test complex query with JOIN and GROUP BY
        complex_query = """
        SELECT 
            f.FactionName,
            COUNT(*) as PersonCount,
            AVG(p.PersonID) as AvgPersonID
        FROM test_persons p
        JOIN test_factions f ON p.FactionID = f.FactionID
        WHERE p.KnessetNum = 25
        GROUP BY f.FactionName
        ORDER BY PersonCount DESC
        """
        
        start_time = time.time()
        result = conn.execute(complex_query).fetchdf()
        end_time = time.time()
        
        query_time = end_time - start_time
        
        # Complex query should still be reasonably fast (less than 200ms)
        assert query_time < 0.2, f"Complex query too slow: {query_time:.3f}s"
        assert len(result) == 10  # Should have 10 factions
        
        conn.close()
    
    def test_large_result_set_performance(self, temp_db_path):
        """Test performance with larger result sets."""
        # Create larger dataset
        large_data = pd.DataFrame({
            'ID': range(1, 10001),  # 10k rows
            'Value': [f'Value{i}' for i in range(1, 10001)],
            'Category': [i % 100 for i in range(10000)]
        })
        
        conn = duckdb.connect(str(temp_db_path))
        conn.execute("CREATE TABLE large_test AS SELECT * FROM large_data", {'large_data': large_data})
        
        # Test query returning large result set
        start_time = time.time()
        result = conn.execute("SELECT * FROM large_test WHERE Category < 50").fetchdf()
        end_time = time.time()
        
        query_time = end_time - start_time
        
        # Large result set query should complete in reasonable time (less than 500ms)
        assert query_time < 0.5, f"Large result set query too slow: {query_time:.3f}s"
        assert len(result) == 5000  # Half the data (categories 0-49)
        
        conn.close()


class TestQueryExecutorPerformance:
    """Test performance of query executor components."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database for testing."""
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test_query_executor.duckdb"
        yield db_path
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def setup_test_data(self, temp_db_path):
        """Setup test data for query executor tests."""
        # Create test data similar to real Knesset data structure
        conn = duckdb.connect(str(temp_db_path))
        
        # Create KNS_Person table
        person_data = pd.DataFrame({
            'PersonID': range(1, 501),
            'FirstName': [f'Person{i}' for i in range(1, 501)],
            'LastName': [f'Last{i}' for i in range(1, 501)],
        })
        conn.execute("CREATE TABLE KNS_Person AS SELECT * FROM person_data", {'person_data': person_data})
        
        # Create KNS_Query table
        query_data = pd.DataFrame({
            'QueryID': range(1, 1001),
            'PersonID': [i % 500 + 1 for i in range(1000)],
            'KnessetNum': [25] * 1000,
            'StartDate': ['2023-01-01'] * 1000,
            'TypeDesc': ['Regular'] * 800 + ['Urgent'] * 200
        })
        conn.execute("CREATE TABLE KNS_Query AS SELECT * FROM query_data", {'query_data': query_data})
        
        conn.close()
        return temp_db_path
    
    def test_query_executor_initialization_performance(self, setup_test_data):
        """Test query executor initialization time."""
        mock_logger = Mock()
        
        start_time = time.time()
        executor = QueryExecutor(
            db_path=setup_test_data,
            connect_func=lambda read_only=True: duckdb.connect(str(setup_test_data), read_only=read_only),
            logger_obj=mock_logger
        )
        end_time = time.time()
        
        init_time = end_time - start_time
        
        # Initialization should be very fast (less than 10ms)
        assert init_time < 0.01, f"QueryExecutor initialization too slow: {init_time:.3f}s"
    
    def test_predefined_query_execution_performance(self, setup_test_data):
        """Test performance of predefined query execution."""
        mock_logger = Mock()
        executor = QueryExecutor(
            db_path=setup_test_data,
            connect_func=lambda read_only=True: duckdb.connect(str(setup_test_data), read_only=read_only),
            logger_obj=mock_logger
        )
        
        # Create a simple test query
        test_query = "SELECT COUNT(*) as total_queries FROM KNS_Query WHERE KnessetNum = 25"
        
        start_time = time.time()
        df, sql, applied_filters = executor.execute_query_with_filters(
            query_name="test_query",
            custom_sql=test_query
        )
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        # Query execution should be fast (less than 100ms)
        assert execution_time < 0.1, f"Query execution too slow: {execution_time:.3f}s"
        assert len(df) == 1
        assert df.iloc[0]['total_queries'] == 1000
    
    def test_filtered_query_performance(self, setup_test_data):
        """Test performance of queries with filters applied."""
        mock_logger = Mock()
        executor = QueryExecutor(
            db_path=setup_test_data,
            connect_func=lambda read_only=True: duckdb.connect(str(setup_test_data), read_only=read_only),
            logger_obj=mock_logger
        )
        
        # Test query with Knesset filter
        base_query = "SELECT * FROM KNS_Query"
        
        start_time = time.time()
        df, sql, applied_filters = executor.execute_query_with_filters(
            query_name="filtered_test",
            custom_sql=base_query,
            knesset_filter=[25]
        )
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        # Filtered query should still be fast (less than 100ms)
        assert execution_time < 0.1, f"Filtered query too slow: {execution_time:.3f}s"
        assert len(df) == 1000  # All test data is Knesset 25


class TestUIPerformance:
    """Test UI component performance."""
    
    def test_session_state_access_performance(self):
        """Test session state manager performance."""
        from ui.state.session_manager import SessionStateManager
        
        # Mock streamlit session state
        mock_session_state = {}
        
        with patch('streamlit.session_state', mock_session_state):
            # Test initialization performance
            start_time = time.time()
            SessionStateManager.initialize_all_session_state()
            end_time = time.time()
            
            init_time = end_time - start_time
            
            # Session state initialization should be very fast (less than 10ms)
            assert init_time < 0.01, f"Session state initialization too slow: {init_time:.3f}s"
            
            # Test access performance
            start_time = time.time()
            for _ in range(100):
                _ = SessionStateManager.get_show_query_results()
                _ = SessionStateManager.get_selected_query_name()
            end_time = time.time()
            
            access_time = end_time - start_time
            
            # 100 accesses should be very fast (less than 10ms)
            assert access_time < 0.01, f"Session state access too slow: {access_time:.3f}s"
    
    def test_predefined_queries_loading_performance(self):
        """Test performance of loading predefined queries."""
        start_time = time.time()
        
        # Import should be fast
        from ui.queries.predefined_queries import PREDEFINED_QUERIES, get_all_query_names
        
        end_time = time.time()
        import_time = end_time - start_time
        
        # Import should be very fast (less than 50ms)
        assert import_time < 0.05, f"Predefined queries import too slow: {import_time:.3f}s"
        
        # Test query name retrieval
        start_time = time.time()
        query_names = get_all_query_names()
        end_time = time.time()
        
        retrieval_time = end_time - start_time
        
        # Query name retrieval should be very fast (less than 1ms)
        assert retrieval_time < 0.001, f"Query names retrieval too slow: {retrieval_time:.3f}s"
        assert len(query_names) > 0


class TestMemoryPerformance:
    """Test memory usage and efficiency."""
    
    def test_dataframe_memory_efficiency(self):
        """Test DataFrame memory usage for typical operations."""
        # Create test DataFrame similar to typical query results
        df = pd.DataFrame({
            'PersonID': range(1, 1001),
            'QueryID': range(1001, 2001),
            'StartDate': ['2023-01-01'] * 1000,
            'TypeDesc': ['Regular'] * 1000
        })
        
        # Test memory usage
        memory_usage = df.memory_usage(deep=True).sum()
        
        # Memory usage should be reasonable (less than 1MB for 1000 rows)
        assert memory_usage < 1_000_000, f"DataFrame memory usage too high: {memory_usage} bytes"
        
        # Test copy performance
        start_time = time.time()
        df_copy = df.copy()
        end_time = time.time()
        
        copy_time = end_time - start_time
        
        # Copy should be fast (less than 10ms)
        assert copy_time < 0.01, f"DataFrame copy too slow: {copy_time:.3f}s"
    
    def test_database_connection_memory_usage(self):
        """Test database connection doesn't leak memory."""
        import gc
        import psutil
        import os
        
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Create and close many connections
        for _ in range(100):
            conn = duckdb.connect(':memory:')
            conn.execute("CREATE TABLE test AS SELECT 1 as id")
            conn.close()
        
        # Force garbage collection
        gc.collect()
        
        # Check final memory usage
        final_memory = process.memory_info().rss
        memory_growth = final_memory - initial_memory
        
        # Memory growth should be minimal (less than 10MB)
        assert memory_growth < 10_000_000, f"Excessive memory growth: {memory_growth} bytes"


# Performance benchmarking utilities
class PerformanceBenchmark:
    """Utility class for performance benchmarking."""
    
    @staticmethod
    def time_function(func, *args, **kwargs):
        """Time a function execution."""
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        return result, end_time - start_time
    
    @staticmethod
    def benchmark_multiple_runs(func, runs=10, *args, **kwargs):
        """Benchmark function over multiple runs."""
        times = []
        for _ in range(runs):
            _, execution_time = PerformanceBenchmark.time_function(func, *args, **kwargs)
            times.append(execution_time)
        
        return {
            'min': min(times),
            'max': max(times),
            'avg': sum(times) / len(times),
            'times': times
        }


@pytest.mark.performance
class TestPerformanceRegression:
    """Regression tests to ensure performance doesn't degrade."""
    
    def test_query_execution_baseline(self):
        """Establish baseline for query execution performance."""
        # This test establishes performance baselines
        # In a real environment, you'd compare against historical data
        
        # Simple baseline test
        start_time = time.time()
        conn = duckdb.connect(':memory:')
        conn.execute("SELECT 1 as test")
        result = conn.fetchone()
        conn.close()
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        # Baseline should be very fast (less than 1ms)
        assert execution_time < 0.001, f"Baseline query too slow: {execution_time:.6f}s"
        assert result[0] == 1


# Pytest configuration for performance tests
def pytest_configure(config):
    """Configure pytest for performance testing."""
    config.addinivalue_line(
        "markers", "performance: mark test as a performance test"
    )
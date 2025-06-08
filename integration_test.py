#!/usr/bin/env python3
"""
Simple integration test to verify the refactored system works.
Run this with: python integration_test.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_config_system():
    """Test configuration system."""
    print("Testing configuration system...")
    
    from config.settings import Settings
    from config.database import DatabaseConfig
    from config.api import APIConfig
    from config.charts import ChartConfig
    
    # Test settings
    assert Settings.PROJECT_ROOT.exists()
    assert Settings.DEFAULT_DB_PATH.name == "warehouse.duckdb"
    
    # Test database config
    tables = DatabaseConfig.get_all_tables()
    assert len(tables) > 0
    assert "KNS_Query" in tables
    
    # Test API config
    assert APIConfig.BASE_URL == "http://knesset.gov.il/Odata/ParliamentInfo.svc"
    
    # Test chart config
    assert len(ChartConfig.CHART_TYPES) > 0
    
    print("✅ Configuration system works!")

def test_dependency_injection():
    """Test dependency injection system."""
    print("Testing dependency injection...")
    
    from core.dependencies import DependencyContainer
    
    container = DependencyContainer()
    service = container.data_refresh_service
    logger = container.get_logger()
    
    assert service is not None
    assert logger is not None
    
    print("✅ Dependency injection works!")

def test_data_layer():
    """Test data layer without external dependencies."""
    print("Testing data layer...")
    
    from data.repositories.database_repository import DatabaseRepository
    from data.services.resume_state_service import ResumeStateService
    
    # Test that classes can be instantiated
    repo = DatabaseRepository()
    resume_service = ResumeStateService()
    
    assert repo is not None
    assert resume_service is not None
    
    print("✅ Data layer works!")

def test_api_layer():
    """Test API layer."""
    print("Testing API layer...")
    
    from api.error_handling import ErrorCategory, categorize_error
    from api.circuit_breaker import CircuitBreaker
    
    # Test error categorization
    try:
        raise ValueError("Test error")
    except Exception as e:
        category = categorize_error(e)
        assert isinstance(category, ErrorCategory)
    
    # Test circuit breaker
    breaker = CircuitBreaker()
    assert breaker.can_attempt()
    
    print("✅ API layer works!")

def test_legacy_compatibility():
    """Test legacy compatibility layer."""
    print("Testing legacy compatibility...")
    
    # Test that legacy imports work (with warnings)
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        
        # These should import without errors
        from ui.plot_generators import KNESSET_COLOR_SEQUENCE
        from backend.fetch_table import BASE_URL
        
        assert KNESSET_COLOR_SEQUENCE is not None
        assert BASE_URL == "http://knesset.gov.il/Odata/ParliamentInfo.svc"
    
    print("✅ Legacy compatibility works!")

def main():
    """Run all integration tests."""
    print("🚀 Running integration tests for refactored system...\n")
    
    try:
        test_config_system()
        test_dependency_injection()
        test_data_layer()
        test_api_layer()
        test_legacy_compatibility()
        
        print("\n🎉 All integration tests passed!")
        print("✅ Refactored system is working correctly!")
        return True
        
    except Exception as e:
        print(f"\n❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
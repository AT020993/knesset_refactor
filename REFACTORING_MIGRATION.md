# Refactoring Migration Guide

## Overview
This document outlines the refactoring changes made to improve the project's architecture and maintainability.

## Key Changes

### 1. Configuration System
- **New**: Centralized configuration in `src/config/`
- **Files**: `settings.py`, `database.py`, `charts.py`, `api.py`
- **Benefits**: Single source of truth for all settings

### 2. Chart System Refactoring
- **Old**: Monolithic `plot_generators.py` (2,086 lines)
- **New**: Modular chart system in `src/ui/charts/`
- **Structure**:
  ```
  src/ui/charts/
  ├── base.py           # Base chart class
  ├── factory.py        # Chart factory
  ├── time_series.py    # Time-based charts
  ├── distribution.py   # Distribution charts
  ├── comparison.py     # Comparison charts
  ├── network.py        # Network charts
  └── timeline.py       # Timeline charts
  ```

### 3. API Client Refactoring
- **Old**: Mixed in `fetch_table.py` (800 lines)
- **New**: Dedicated API modules in `src/api/`
- **Structure**:
  ```
  src/api/
  ├── odata_client.py      # OData API client
  ├── circuit_breaker.py   # Circuit breaker pattern
  └── error_handling.py    # Error categorization
  ```

### 4. Data Layer
- **New**: Repository pattern in `src/data/`
- **Structure**:
  ```
  src/data/
  ├── repositories/
  │   └── database_repository.py  # Database operations
  └── services/
      ├── data_refresh_service.py # Data refresh coordination
      └── resume_state_service.py # Resume state management
  ```

### 5. Backend Completion
- **Implemented**: Previously empty files
  - `src/backend/tables.py` - Table metadata and definitions
  - `src/backend/duckdb_io.py` - DuckDB utilities
  - `src/backend/utils.py` - Backend helper functions

### 6. CLI Improvements
- **Old**: Problematic try/catch imports with fallbacks
- **New**: Dependency injection with `src/core/dependencies.py`
- **Benefits**: Cleaner imports, better testability

### 7. UI Service Layer
- **New**: Service layer in `src/ui/services/`
- **Purpose**: Decouple UI from backend logic
- **Files**:
  - `chart_service.py` - Chart generation service
  - `data_service.py` - Data operations service

## Migration Steps

### For Chart Usage
**Old way**:
```python
from ui.plot_generators import plot_queries_by_time_period
fig = plot_queries_by_time_period(db_path, connect_func, logger, **kwargs)
```

**New way**:
```python
from ui.services.chart_service import ChartService
chart_service = ChartService(db_path, logger)
fig = chart_service.plot_queries_by_time_period(**kwargs)
```

### For Data Operations
**Old way**:
```python
from backend.fetch_table import refresh_tables
asyncio.run(refresh_tables(tables))
```

**New way**:
```python
from ui.services.data_service import DataService
data_service = DataService(db_path, logger)
data_service.refresh_data(tables)
```

### For CLI Usage
**Old way**:
```python
python src/backend/fetch_table.py --table KNS_Query
```

**New way**:
```python
python src/cli.py refresh --table KNS_Query
```

## Legacy Compatibility

- Chart factory provides legacy methods for existing code
- Original `plot_generators.py` can remain temporarily for backward compatibility
- Services provide both new and legacy interfaces

## Benefits

1. **Maintainability**: Smaller, focused modules
2. **Testability**: Dependency injection enables better testing
3. **Extensibility**: Easier to add new chart types and data sources
4. **Performance**: Better separation of concerns
5. **Configuration**: Centralized settings management

## Next Steps

1. Update existing UI components to use new services
2. Migrate remaining chart implementations from `plot_generators.py`
3. Add comprehensive tests for new modules
4. Remove legacy code once migration is complete
5. Update documentation and examples

## File Status

### Completed
- ✅ Configuration system
- ✅ Chart factory and base classes
- ✅ API client modules
- ✅ Data layer (repositories and services)
- ✅ Backend utilities
- ✅ CLI dependency injection
- ✅ UI service layer

### In Progress
- 🔄 Chart implementations (stubs created, need full implementation)
- 🔄 UI component updates

### Pending
- ⏳ Legacy code removal
- ⏳ Comprehensive testing
- ⏳ Documentation updates
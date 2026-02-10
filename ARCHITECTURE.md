# Architecture Documentation

## Overview

Clean, modular architecture with dependency injection and separation of concerns.  
Recent refactor waves focused on splitting large service/renderer/query modules into stable facades plus focused operation modules.

## Architecture Principles

**Clean Architecture**: Layered separation with dependency inversion and single responsibility
**Design Patterns**: Repository, Factory, Circuit Breaker, Dependency Injection

## Directory Structure

```
src/
├── api/                    # External API with circuit breaker
│   ├── odata_client.py    # Async OData client
│   ├── circuit_breaker.py # Fault tolerance
│   └── error_handling.py  # Error categorization
├── backend/               # Legacy compatibility
│   ├── connection_manager.py # DB connection management
│   ├── duckdb_io.py      # DuckDB I/O
│   └── fetch_table.py    # Legacy layer
├── config/               # Centralized configuration
│   ├── settings.py      # Application settings
│   ├── database.py      # Database config
│   ├── api.py          # API config
│   └── charts.py       # Chart config
├── core/                # Architecture components
│   └── dependencies.py # DI container
├── data/                # Data layer
│   ├── repositories/   # Data access
│   └── services/      # Business logic + sync/data orchestration
└── ui/                 # Modular UI
    ├── charts/        # Factory pattern charts
    ├── renderers/    # Page/component renderers
    ├── queries/      # SQL query packs + registry + executor
    ├── services/     # UI services
    └── state/        # Session state contracts + manager
```

## Layer Responsibilities

### API Layer (`src/api/`)
- Async OData client with connection pooling
- Circuit breaker for fault tolerance
- Error categorization for retry strategies
- Rate limiting and request flow management

### Configuration Layer (`src/config/`)
- Application-wide settings and paths
- Table definitions and database parameters
- API endpoint configurations
- Visualization themes and styling

### Core Layer (`src/core/`)
- Dependency container for lifecycle management
- Service registration and configuration
- Logger factory

### Data Layer (`src/data/`)
**Repositories**: Abstract data access, query building, transaction management
**Services**: Business logic, refresh orchestration, and cloud sync.

Notable modules:
- `storage_sync_service.py` facade with split ops:
  - `storage_sync_transfer_ops.py`
  - `storage_sync_metadata_ops.py`
  - `storage_sync_startup_ops.py`
- `sync_types.py` typed sync contracts
- `sync_data_refresh_service.py` sync wrapper for async refresh flow

### UI Layer (`src/ui/`)
**Charts**: Factory pattern with inheritance hierarchy, modular design
**Renderers**: Stable page facades with extracted ops for heavy logic:
  - `plots_page.py` → `plots/generation_ops.py`, `plots/selection_ops.py`
  - `data_refresh/page.py` → `data_refresh/query_results_ops.py`
  - `cap/admin_renderer.py` → `cap/admin_maintenance_ops.py`
**Queries**: Query packs (`ui/queries/packs/*`) composed by registry, typed via `types.py`
**Services**: CAP facades (`user_service.py`, `repository.py`, `taxonomy.py`) with split `*_ops.py` modules
**State**: `session_manager.py` plus typed state contracts in `state_contracts.py`

## Key Improvements

### Before Refactoring
- Monolithic 624-line files with mixed concerns
- 170+ line queries embedded in UI
- Scattered session state initialization
- Tight UI-database coupling
- Limited testability

### After Refactoring
- Modular architecture with focused modules
- Centralized SQL queries with metadata
- Type-safe session state management
- Dependency injection for loose coupling
- High testability with independent components
- Repository, Factory, Circuit Breaker, DI patterns
- Smart initiator detection and coalition analysis
- Legislative continuity tracking with bill merge relationships
- Backward-compatible facades over decomposed modules:
  - CAP user/repository/taxonomy services
  - data refresh and plots renderers
  - storage sync service

## Data Flow

```
User → UI Page → Query Executor → Repository → Database
         ↓
Session Manager ← Service ← Data ← API Client → External API
```

**Processing**: User interaction → State update → Service layer → Repository → Database → Transform → UI update

## Configuration Management

```python
# settings.py - Application settings
DEFAULT_DB_PATH = DATA_DIR / "warehouse.duckdb"

# database.py - Tables and connections
TABLES = ["KNS_Query", "KNS_Agenda", "KNS_Person", ...]

# api.py - API configuration
BASE_URL = "http://knesset.gov.il/Odata/ParliamentInfo.svc"
PAGE_SIZE = 1000
```

## Dependency Injection

```python
class DependencyContainer:
    @property
    def data_refresh_service(self) -> DataRefreshService:
        if self._data_refresh_service is None:
            self._data_refresh_service = DataRefreshService(self.db_path)
        return self._data_refresh_service
```

**Benefits**: Testability (easy mocking), flexibility (swappable implementations), lifecycle management, consistent configuration

## Error Handling

**Layered Strategy**:
1. **API Layer**: Categorizes errors (network, server, client, timeout)
2. **Circuit Breaker**: Prevents cascade failures
3. **Service Layer**: Business logic error handling
4. **UI Layer**: User-friendly error messages

## Testing Strategy

### Unit Testing
- Component isolation with mock injection
- 60%+ coverage requirement
- pytest framework

### Integration Testing
- End-to-end flows with real database
- API integration with mocks
- Service layer business logic

### End-to-End Testing
- Playwright framework (Chromium, Firefox, WebKit)
- 100% success rate (7/7 tests passing)
- CI/CD automation in GitHub Actions
- Coverage: Page loading, data refresh, queries, navigation, error handling, responsive design, performance

### Test Execution
```bash
pytest                                    # All tests with coverage
pytest -m e2e --base-url http://localhost:8501  # E2E tests
```

## Security

**Data Protection**: Parameterized queries, input validation, secure connections, limited error exposure
**Access Control**: Read-only mode by default, permission isolation, audit logging

## Migration Status

### Completed
- ✅ Main UI refactoring (624 → 120 lines)
- ✅ Query extraction to dedicated modules and pack registry
- ✅ Centralized state management
- ✅ Configuration system
- ✅ Service layer separation
- ✅ Dependency injection implementation
- ✅ E2E testing with Playwright
- ✅ Project cleanup
- ✅ CAP domain decomposition (user/repository/taxonomy split)
- ✅ Storage sync service decomposition (transfer/metadata/startup ops)

### In Progress
- 🔄 Legacy compatibility deprecation path

### Planned
- ⏳ Legacy code removal
- ⏳ Performance optimization
- ⏳ Test coverage expansion

## Future Considerations

**Scalability**: Microservices, message queues, Redis caching, load balancing
**Technology**: DuckDB upgrades, Streamlit updates, Python version support, automated dependency updates

---

*Modular architecture enables incremental improvements and high maintainability for future development.*

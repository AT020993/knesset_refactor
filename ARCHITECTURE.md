# Architecture Documentation

## Overview

This project has been refactored from a monolithic structure to a clean, modular architecture following dependency injection and separation of concerns principles. The refactoring dramatically improved maintainability while preserving all existing functionality.

## Architecture Principles

### 1. Clean Architecture
- **Layered Separation**: Clear boundaries between API, data, UI, and configuration layers
- **Dependency Inversion**: High-level modules don't depend on low-level modules
- **Single Responsibility**: Each module has a focused, well-defined purpose

### 2. Design Patterns
- **Repository Pattern**: Abstracted data access layer for testability
- **Factory Pattern**: Modular chart generation with inheritance hierarchy
- **Circuit Breaker Pattern**: Resilient API communication with fault tolerance
- **Dependency Injection**: Centralized container for managing dependencies

## Directory Structure

```
src/
├── api/                    # External API integration layer
│   ├── odata_client.py    # Async OData client with circuit breaker
│   ├── circuit_breaker.py # Fault tolerance implementation
│   └── error_handling.py  # Error categorization and handling
├── backend/               # Legacy compatibility and core utilities  
│   ├── connection_manager.py # Database connection management
│   ├── duckdb_io.py      # DuckDB I/O operations
│   ├── fetch_table.py    # Legacy compatibility layer
│   └── utils.py          # Utility functions
├── config/               # Centralized configuration management
│   ├── settings.py      # Application settings and paths
│   ├── database.py      # Database configuration
│   ├── api.py          # API configuration
│   └── charts.py       # Chart configuration and themes
├── core/                # Core architecture components
│   └── dependencies.py # Dependency injection container
├── data/                # Data layer with clean architecture
│   ├── repositories/   # Repository pattern implementations
│   └── services/      # Business logic services
└── ui/                 # Modular UI components
    ├── charts/        # Modular chart system
    │   ├── factory.py # Chart factory with inheritance
    │   ├── base.py   # Base chart class
    │   └── [specific chart types]
    ├── pages/        # Page-specific UI components
    │   ├── data_refresh_page.py # Main page renderer
    │   └── plots_page.py # Plots interface renderer
    ├── queries/      # Query management system
    │   ├── predefined_queries.py # SQL definitions
    │   └── query_executor.py # Query execution logic
    ├── services/     # UI business logic services
    ├── state/        # Session state management
    │   └── session_manager.py # Centralized state management
    └── data_refresh.py # Streamlined main interface
```

## Layer Responsibilities

### API Layer (`src/api/`)
**Purpose**: External system integration with reliability patterns

- **OData Client**: Async HTTP client with connection pooling
- **Circuit Breaker**: Prevents cascade failures and provides fallback behavior
- **Error Handling**: Categorizes errors for appropriate retry strategies
- **Rate Limiting**: Prevents API abuse and manages request flow

### Configuration Layer (`src/config/`)
**Purpose**: Centralized configuration management

- **Settings**: Application-wide settings and paths
- **Database**: Table definitions and connection parameters
- **API**: Endpoint configurations and request parameters
- **Charts**: Visualization themes and styling configurations

### Core Layer (`src/core/`)
**Purpose**: Fundamental architecture components

- **Dependency Container**: Manages object lifecycle and dependencies
- **Service Registration**: Configures service dependencies
- **Logger Factory**: Provides configured logger instances

### Data Layer (`src/data/`)
**Purpose**: Business logic and data access abstraction

#### Repositories (`src/data/repositories/`)
- Abstract data access patterns
- Database operations encapsulation
- Query building and execution
- Transaction management

#### Services (`src/data/services/`)
- Business logic implementation
- Cross-cutting concerns (caching, validation)
- Orchestration of repository operations
- Data transformation and enrichment

### UI Layer (`src/ui/`)
**Purpose**: User interface with component-based architecture

#### Charts (`src/ui/charts/`)
- **Factory Pattern**: Dynamic chart creation based on type
- **Inheritance Hierarchy**: Base chart class with specialized implementations
- **Modular Design**: Each chart type in separate module
- **Configuration-driven**: Chart behavior controlled by configuration

#### Pages (`src/ui/pages/`)
- **Single Responsibility**: Each page handles specific UI concern
- **Renderer Pattern**: Separates UI logic from data processing
- **Reusable Components**: Shared UI elements across pages

#### Queries (`src/ui/queries/`)
- **SQL Separation**: Complex queries extracted from UI code
- **Metadata Management**: Query descriptions and filter configurations
- **Execution Logic**: Parameter binding and result processing

#### State (`src/ui/state/`)
- **Centralized Management**: Single source of truth for session state
- **Type Safety**: Type-safe accessors and mutators
- **Encapsulation**: State changes through controlled interfaces

## Key Improvements from Refactoring

### Before Refactoring
- **Monolithic Files**: Single 624-line `data_refresh.py` with mixed concerns
- **Hardcoded SQL**: 170+ line queries embedded in UI code  
- **Scattered State**: Session state initialized throughout codebase
- **Tight Coupling**: UI directly coupled to database operations
- **Limited Testability**: Hard to unit test due to dependencies
- **No Design Patterns**: Lack of structured architectural patterns

### After Refactoring (80% Code Reduction)
- **Modular Architecture**: Clear separation of concerns across focused modules
- **Extracted Queries**: SQL queries centralized with metadata and helpers
- **Centralized State**: Type-safe session state management
- **Loose Coupling**: Dependency injection enables easy testing and changes
- **High Testability**: Each component can be tested independently
- **Design Patterns**: Repository, Factory, Circuit Breaker, and Dependency Injection patterns implemented
- **Legacy Compatibility**: Backward compatibility maintained with deprecation warnings

## Data Flow

```
User Request → UI Page → Query Executor → Repository → Database
                ↓
Session Manager ← Service Layer ← Data Layer ← API Client → External API
```

### Request Processing Flow
1. **User Interaction**: User interacts with Streamlit UI components
2. **State Management**: Session state updated through centralized manager
3. **Service Layer**: Business logic processes request with injected dependencies
4. **Repository Layer**: Data access abstracted through repository pattern
5. **Database Operations**: Optimized queries executed against DuckDB
6. **Result Processing**: Data transformed and returned through layers
7. **UI Update**: Results displayed through component-based UI

## Configuration Management

### Centralized Configuration
All application settings managed through dedicated configuration modules:

```python
# settings.py - Application-wide settings
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_DB_PATH = DATA_DIR / "warehouse.duckdb"

# database.py - Database configuration
TABLES = ["KNS_Query", "KNS_Agenda", "KNS_Person", ...]
CURSOR_TABLES = ["KNS_CommitteeSession", "KNS_Bill"]

# api.py - API configuration  
BASE_URL = "http://knesset.gov.il/Odata/ParliamentInfo.svc"
PAGE_SIZE = 1000
MAX_RETRIES = 3
```

## Dependency Injection

### Container Pattern
```python
class DependencyContainer:
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or Settings.DEFAULT_DB_PATH
        self._data_refresh_service = None
        self._logger = None
    
    @property
    def data_refresh_service(self) -> DataRefreshService:
        if self._data_refresh_service is None:
            self._data_refresh_service = DataRefreshService(self.db_path)
        return self._data_refresh_service
```

### Benefits
- **Testability**: Easy to inject mocks for testing
- **Flexibility**: Service implementations can be swapped
- **Lifecycle Management**: Container manages object creation and cleanup
- **Configuration**: Services configured consistently

## Error Handling Strategy

### Layered Error Handling
1. **API Layer**: Categorizes external API errors (network, server, client)
2. **Circuit Breaker**: Prevents cascade failures with configurable thresholds
3. **Service Layer**: Handles business logic errors with appropriate responses
4. **UI Layer**: Displays user-friendly error messages with context

### Error Categories
```python
class ErrorCategory(Enum):
    NETWORK = "network"     # Connection issues
    SERVER = "server"       # 5xx HTTP responses  
    CLIENT = "client"       # 4xx HTTP responses
    TIMEOUT = "timeout"     # Request timeouts
    DATA = "data"          # Data validation errors
    UNKNOWN = "unknown"     # Unhandled errors
```

## Testing Strategy

### Unit Testing
- **Component Isolation**: Each module tested independently
- **Mock Injection**: Dependencies mocked through injection container
- **Focused Tests**: Tests target specific functionality without side effects

### Integration Testing
- **End-to-End Flows**: Complete request processing tested
- **Database Integration**: Real database operations in controlled environment
- **UI Components**: Streamlit components tested with proper mocking

## Performance Considerations

### Optimization Strategies
- **Connection Pooling**: Reuse database connections across requests
- **Lazy Loading**: Services instantiated only when needed
- **Query Optimization**: Parameterized queries with proper indexing
- **Caching**: Strategic caching at service and repository levels
- **Async Operations**: Non-blocking operations for external API calls

### Memory Management
- **Resource Cleanup**: Proper disposal of database connections
- **State Management**: Efficient session state storage
- **Streaming**: Large dataset processing without loading entire result sets

## Security Considerations

### Data Protection
- **SQL Injection Prevention**: Parameterized queries throughout
- **Input Validation**: All user inputs validated before processing
- **Connection Security**: Secure database connections with proper credentials
- **Error Information**: Limited error information exposed to users

### Access Control
- **Read-only Operations**: Database connections use read-only mode by default
- **Permission Isolation**: Different permission levels for different operations
- **Audit Logging**: Comprehensive logging of data access operations

## Migration Notes

### Backward Compatibility
- **Legacy Imports**: Old import paths still work with deprecation warnings
- **Function Signatures**: Existing function signatures preserved where possible
- **Configuration**: Existing configuration files continue to work
- **Data Formats**: All existing data formats supported

### Deprecation Strategy
```python
warnings.warn(
    "plot_generators module is deprecated. Use ui.charts.factory.ChartFactory instead.",
    DeprecationWarning,
    stacklevel=2
)
```

## Current Migration Status

### Completed Migrations
- ✅ **Main UI Refactoring**: `data_refresh.py` reduced from 624 to ~120 lines
- ✅ **Query Extraction**: SQL queries moved to dedicated modules
- ✅ **State Management**: Centralized session state with type safety
- ✅ **Configuration System**: All settings in dedicated configuration modules
- ✅ **Service Layer**: Business logic separated from UI concerns
- ✅ **Dependency Injection**: Container pattern implemented throughout

### In Progress
- 🔄 **Chart System Migration**: Factory pattern implemented, some chart types still need full implementation
- 🔄 **Legacy Deprecation**: Gradual phase-out of old modules with warnings

### Planned
- ⏳ **Legacy Code Removal**: Remove deprecated modules once new system is fully tested
- ⏳ **Performance Optimization**: Further optimization of large files
- ⏳ **Test Coverage Expansion**: Comprehensive testing for new modular components

## Future Architecture Considerations

### Scalability Enhancements
- **Microservices**: Split into independent, scalable services
- **Message Queues**: Async processing with message queue integration
- **Caching Layer**: Redis integration for distributed caching
- **Load Balancing**: Multi-instance deployment with load balancing

### Technology Evolution
- **Database Upgrades**: Migration path to newer DuckDB versions
- **Framework Updates**: Streamlit version upgrade compatibility
- **Python Version**: Support for newer Python versions
- **Dependency Management**: Automated dependency updates with testing

This architecture provides a solid foundation for ongoing development while maintaining the high-quality, maintainable codebase that was achieved through the refactoring process. The modular design enables incremental improvements and makes the system highly maintainable for future development.
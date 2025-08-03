# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing
```bash
# Run unit tests with coverage
pytest

# Run specific test categories
pytest -m "not slow"           # Skip slow tests
pytest -m integration          # Run integration tests only
pytest -m performance          # Run performance tests only
```

### Code Quality
```bash
# Code formatting (if enabled)
black src/
isort src/

# Type checking (if enabled)
mypy src/

# Linting (if enabled)
flake8 src/
```

### Data Operations
```bash
# Refresh all data tables
PYTHONPATH="./src" python -m backend.fetch_table --all

# Refresh specific table
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Query

# Download complete committee session data (RECOMMENDED for full accuracy)
# This downloads 74,951 committee session items for accurate bill-to-session mapping
python download_committee_sessions.py

# Refresh committee data with historical coverage (requires KnessetNum filtering)
# Note: Default fetch only gets recent committees. For complete historical data,
# use manual KnessetNum filtering as implemented in the improved committee resolution.

# List available tables
PYTHONPATH="./src" python -m backend.fetch_table --list-tables

# Execute SQL query
PYTHONPATH="./src" python -m backend.fetch_table --sql "SELECT * FROM KNS_Person LIMIT 5;"
```

### Application Launch
```bash
# Start Streamlit UI
streamlit run src/ui/data_refresh.py --server.port 8501

# Alternative using startup script
./start-knesset.sh
```

## Architecture Overview

This is a **Knesset parliamentary data analysis platform** built with **clean architecture principles**:

### Core Components
- **API Layer** (`src/api/`): Async OData client with circuit breaker pattern for resilient data fetching
- **Data Layer** (`src/data/`): Repository pattern with business logic services and dependency injection
- **UI Layer** (`src/ui/`): Component-based Streamlit interface with modular chart system
- **Configuration** (`src/config/`): Centralized settings management
- **Backend** (`src/backend/`): Legacy compatibility layer and core utilities

### Key Design Patterns
- **Repository Pattern**: Abstracted data access in `src/data/repositories/`
- **Factory Pattern**: Modular chart generation in `src/ui/charts/`
- **Circuit Breaker**: Fault tolerance in `src/api/circuit_breaker.py`
- **Dependency Injection**: Centralized in `src/core/dependencies.py`

### Database Architecture
- **DuckDB**: High-performance analytical database (`data/warehouse.duckdb`)
- **Parquet Files**: Compressed backup storage (`data/parquet/`)
- **Resume State**: Checkpoint system for interrupted downloads (`data/.resume_state.json`)

## Data Flow

1. **External API**: Fetch from Knesset OData API (`http://knesset.gov.il/Odata/ParliamentInfo.svc`)
2. **Storage**: Store in DuckDB warehouse + Parquet files with complete schema coverage
3. **Processing**: Advanced business logic with comprehensive analysis:
   - Committee session activity and classification
   - Political coalition/opposition analysis
   - Bill-to-plenum session integration
   - Document link aggregation
   - Legislative merge tracking
   - Member faction analysis with percentages
4. **Presentation**: Interactive Streamlit UI with 15+ visualizations and enhanced query capabilities

## Memory Notes

### Chart Filtering Strategy
- Don't need additional filters for predefined charts
- Keep only the knesset number filter for each chart

[Rest of the existing content remains unchanged]
# Knesset OData Explorer

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![CI Status](https://github.com/AT020993/knesset_refactor/workflows/CI%20-%20Automated%20Testing%20for%20AI-Generated%20Branches/badge.svg)](https://github.com/AT020993/knesset_refactor/actions)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.2.2-yellow.svg)](https://duckdb.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.44.1-red.svg)](https://streamlit.io/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](#-license)

A comprehensive platform for fetching, storing, analyzing, and visualizing Israeli parliamentary data from the Knesset's official OData API. This project democratizes access to parliamentary data, enabling researchers, analysts, and citizens to easily explore Israeli legislative activities.

## ⚡ Quick Overview

Transform complex parliamentary data into actionable insights with:
- 🔄 **Automated data fetching** from official Knesset OData API
- 💾 **Efficient storage** in DuckDB with Parquet backup
- 📊 **15+ interactive visualizations** for parliamentary analysis  
- 🖥️ **User-friendly Streamlit interface** for non-technical users
- ⚙️ **Robust CLI tools** for automated workflows
- 📈 **Custom chart builder** for exploratory data analysis

## 🐳 **AI & Development Environment**

**For AI tools (Jules, Codex, etc.) and developers:** This project includes a complete Docker setup for isolated, VM-like development:

```bash
# Quick start for AI tools
./docker-setup.sh setup      # One-time setup with sample data
./docker-setup.sh up dev     # Start development environment
./docker-setup.sh shell      # Get shell access for coding
```

- 🔗 **Development UI**: http://localhost:8502
- 📖 **Full AI Guide**: See [`AI_SETUP.md`](AI_SETUP.md) for complete instructions
- 🤖 **CI/CD Ready**: Automatic testing for AI-generated branches

## 🎯 Project Goals

* **Data Accessibility:** Provide easy access to Israeli parliamentary data for researchers and analysts.
* **Self-Service:** Enable users (including non-technical ones) to independently refresh, explore, and download curated and raw data.
* **Comprehensive Data Management:** Support robust and resumable fetching, efficient storage, and timely updates of parliamentary data.
* **User-Friendly Interface:** Offer an intuitive Streamlit-based interface for data interaction, exploration, and exportation.

## ✨ Key Features

### Backend Architecture (Modular & Clean)
* **Data Services Layer (`src/data/`):**
    * **Automated OData Fetching:** Retrieves data directly from the official Knesset OData API with circuit breaker pattern
    * **Repository Pattern:** Clean separation between data access and business logic
    * **Dependency Injection:** Testable, maintainable architecture with centralized configuration
* **API Layer (`src/api/`):**
    * **Robust OData Client:** Implements circuit breaker, retry logic, and proper error categorization
    * **Checkpoint-Resume:** For large, cursor-paged tables, download progress is saved for interruption recovery
    * **Parallel Downloads:** Utilizes `asyncio` for concurrent fetching, significantly speeding up data acquisition
    * **Connection Management:** Monitors and prevents database connection leaks
* **Storage & Configuration:**
    * **DuckDB Warehouse:** Stores all fetched data in a local DuckDB database (`data/warehouse.duckdb`) for fast querying
    * **Parquet Files:** Mirrors each table into compressed Parquet files (`data/parquet/`) for optimized storage
    * **Centralized Configuration:** All settings managed through dedicated configuration modules
* **Manual Faction Coalition Status Integration:**
    * Loads and integrates user-maintained faction coalition/opposition statuses from CSV
    * Enriches parliamentary analysis by providing context on faction alignments
* **Logging:** Comprehensive logging using the `src/utils/logger_setup.py` module

### Frontend - Modular Streamlit UI (Refactored Architecture)
* **Clean Architecture (`src/ui/`):**
    * **Page-based Structure:** Separate modules for different UI sections (`pages/`, `queries/`, `state/`)
    * **Centralized State Management:** Type-safe session state management with proper encapsulation
    * **Query Separation:** Complex SQL queries extracted to dedicated modules with metadata
    * **Component-based UI:** Reusable UI components with clear separation of concerns
* **Self-Service Data Refresh:**
    * Select specific OData tables or refresh all predefined tables
    * Monitor live progress of data fetching with real-time updates
    * Dedicated controls for faction coalition status data refresh
* **Predefined Queries System:**
    * Execute curated SQL queries with proper filtering and parameterization
    * Dynamic query execution with type-safe parameter handling
    * Built-in examples: "Queries + Full Details", "Agenda Items + Full Details", "Bills + Full Details"
    * Results displayed interactively with comprehensive download options
* **Interactive Table Explorer:**
    * Dynamic table browsing with intelligent filter application
    * Auto-detection of filterable columns (KnessetNum, FactionID)
    * Real-time filter application with session state persistence
* **Enhanced Filtering:** Centralized sidebar filters with proper state management
* **Ad-hoc SQL Sandbox:** Advanced users can run custom SQL queries with error handling
* **Data Export:** Multi-format download (CSV, Excel) with proper encoding support
* **Comprehensive Visualizations:**
    * **Predefined Charts:** Over 15 ready-to-use visualizations covering queries, agendas, and advanced analytics
    * **Query Analytics:** Response times by ministry, coalition status analysis with optional date range filtering, performance metrics
    * **Parliamentary Activity:** Calendar heatmaps showing daily activity intensity patterns
    * **Network Analysis:** MK collaboration networks based on shared ministry focus
    * **Hierarchical Views:** Sunburst charts for ministry workload breakdown by query type and status
    * **Timeline Analysis:** Coalition periods, MK tenure, and ministry leadership timelines
    * **Temporal Filtering:** Date range controls for analyzing specific time periods in faction query patterns
    * Interactive engagement with all charts powered by Plotly
* **Interactive Chart Builder:**
    * Dynamically construct custom visualizations (bar charts, scatter plots, etc.) directly from the data tables
    * Users can select tables, columns for axes, color encodings, and faceting to explore data patterns

### Command-Line Interface (CLI)
* **Backend CLI (`src/backend/fetch_table.py`):**
    * Refresh all or specific OData tables.
    * Refresh only the faction coalition status data.
    * Execute ad-hoc SQL queries directly.
    * List available OData tables.
* **Simplified CLI (`src/cli.py` & `scripts/refresh_all.sh`):**
    * A `typer`-based CLI for quick refreshes, primarily used by the `scripts/refresh_all.sh` script for a full data refresh.

## 📂 Project Structure

```plaintext
knesset_refactor/
├── .github/
│   └── workflows/
│       └── ci.yml             # GitHub Actions CI configuration
├── .streamlit/
│   └── config.toml            # Streamlit configuration
├── data/
│   ├── faction_coalition_status.csv # User-managed faction status (create this file)
│   ├── parquet/               # Raw parquet files (auto-generated)
│   ├── warehouse.duckdb       # DuckDB database storage (auto-generated)
│   └── .resume_state.json     # Internal file for resuming downloads (auto-generated)
├── docs/
│   └── KnessetOdataManual.pdf # Official Knesset OData documentation
├── scripts/
│   └── refresh_all.sh         # Shell script for a full data refresh using the Typer CLI
├── src/
│   ├── api/                   # API layer with robust OData client
│   │   ├── odata_client.py    # Async OData client with circuit breaker
│   │   ├── circuit_breaker.py # Circuit breaker pattern implementation
│   │   ├── error_handling.py  # Error categorization and handling
│   │   └── __init__.py
│   ├── backend/               # Legacy compatibility and core utilities
│   │   ├── connection_manager.py # Database connection management
│   │   ├── duckdb_io.py      # DuckDB I/O operations
│   │   ├── fetch_table.py    # Legacy compatibility layer
│   │   ├── tables.py         # Table definitions
│   │   ├── utils.py          # Utility functions
│   │   └── __init__.py
│   ├── config/               # Centralized configuration management
│   │   ├── settings.py       # Application settings
│   │   ├── database.py       # Database configuration
│   │   ├── api.py           # API configuration
│   │   ├── charts.py        # Chart configuration
│   │   └── __init__.py
│   ├── core/                 # Core architecture components
│   │   ├── dependencies.py  # Dependency injection container
│   │   └── __init__.py
│   ├── data/                 # Data layer with clean architecture
│   │   ├── repositories/     # Repository pattern implementations
│   │   ├── services/        # Business logic services
│   │   └── __init__.py
│   ├── ui/                   # Modular UI components
│   │   ├── charts/          # Modular chart system
│   │   │   ├── factory.py   # Chart factory with inheritance
│   │   │   ├── base.py      # Base chart class
│   │   │   ├── comparison.py # Comparison charts
│   │   │   ├── distribution.py # Distribution charts
│   │   │   ├── network.py   # Network analysis charts
│   │   │   ├── timeline.py  # Timeline charts
│   │   │   └── __init__.py
│   │   ├── pages/           # Page-specific UI components
│   │   │   ├── data_refresh_page.py # Main page renderer
│   │   │   ├── plots_page.py # Plots interface renderer
│   │   │   └── __init__.py
│   │   ├── queries/         # Query management system
│   │   │   ├── predefined_queries.py # SQL definitions
│   │   │   ├── query_executor.py # Query execution logic
│   │   │   └── __init__.py
│   │   ├── services/        # UI business logic services
│   │   │   ├── chart_service.py # Chart generation service
│   │   │   └── __init__.py
│   │   ├── state/           # Session state management
│   │   │   ├── session_manager.py # Centralized state management
│   │   │   └── __init__.py
│   │   ├── data_refresh.py  # Streamlined main interface
│   │   ├── sidebar_components.py # Sidebar UI components
│   │   ├── ui_utils.py      # UI utility functions
│   │   ├── plot_generators.py # Legacy compatibility layer
│   │   ├── chart_builder_ui.py # Interactive chart builder
│   │   └── __init__.py
│   ├── utils/
│   │   ├── logger_setup.py  # Logging configuration
│   │   └── __init__.py
│   └── cli.py               # Typer-based CLI with dependency injection
├── tests/
│   ├── conftest.py            # Pytest fixtures and configuration
│   ├── test_fetch_table.py    # Unit tests for data-fetching logic
│   └── test_views.py          # Unit tests for UI predefined queries
├── requirements.txt           # Project dependencies
└── README.md                  # This file
```

## 🛠️ Technologies Used

### Core Technologies
* **Python 3.12+** (Required)
* **DuckDB 1.2.2:** High-performance analytical database for the data warehouse
* **Pandas 2.2.3:** Data manipulation and analysis
* **Parquet:** Efficient columnar storage format

### Architecture & Patterns
* **Clean Architecture:** Layered separation with dependency injection
* **Repository Pattern:** Abstracted data access layer
* **Circuit Breaker Pattern:** Resilient API communication
* **Factory Pattern:** Modular chart generation system
* **Dependency Injection:** Testable, maintainable codebase

### Frontend & Visualization
* **Streamlit 1.44.1:** Modern web UI framework
* **Plotly 5.0+:** Interactive data visualizations
* **Component-based Architecture:** Reusable UI components

### Networking & Reliability
* **aiohttp 3.9.4:** Asynchronous HTTP client with connection pooling
* **backoff 2.2.1:** Intelligent retry strategies
* **Circuit Breaker:** Fault tolerance for API calls

### Development & Testing
* **Pytest 8.3.5:** Comprehensive unit testing framework
* **Type Hints:** Full type safety with runtime validation
* **Typer 0.12+:** Modern CLI framework with dependency injection
* **OpenPyXL 3.1.5:** Excel export functionality
* **tqdm 4.66.1:** Progress visualization

## 📸 Screenshots

### Streamlit Interface
*Coming soon: Screenshots of the data refresh interface, visualizations, and chart builder*

### Sample Visualizations
*Coming soon: Examples of parliamentary activity heatmaps, MK collaboration networks, and query analytics*

## 🚀 Quick Start

Get up and running in 5 minutes with the new modular architecture:

```bash
# 1. Clone and setup
git clone https://github.com/AT020993/knesset_refactor.git
cd knesset_refactor
python -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Download sample data (5-10 minutes)
# The new dependency injection system handles configuration automatically
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Person
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Query

# 4. Launch the refactored interface
streamlit run src/ui/data_refresh.py
```

Open `http://localhost:8501` and explore the new modular UI! 🎉

**New in the refactored version:**
- ⚡ **Faster loading** with modular architecture
- 🧩 **Component-based UI** with better separation of concerns
- 🎯 **Type-safe session management** with centralized state
- 📊 **Extracted SQL queries** for better maintainability

## 🚀 Getting Started

### Prerequisites

* **Python 3.12+** (Required)
* Git

### Installation

1. **Clone the repository:**

    ```bash
    git clone https://github.com/AT020993/knesset_refactor.git
    cd knesset_refactor
    ```

2. **Remove any existing virtual environment:**

    ```bash
    # If you have an old virtual environment, remove it first
    rm -rf .venv
    ```

3. **Create and activate a virtual environment:**

    ```bash
    python -m venv .venv
    # On Windows
    # .venv\Scripts\activate
    # On macOS/Linux
    source .venv/bin/activate
    ```

4. **Install dependencies:**

    ```bash
    pip install --upgrade pip
    pip install -r requirements.txt
    ```

5. **(Optional but Recommended) Create Faction Status CSV:**
    Create a file named `faction_coalition_status.csv` in the `data/` directory. This file is used to track the coalition/opposition status of factions.
    
    **Structure for `data/faction_coalition_status.csv`:**

    ```csv
    KnessetNum,FactionID,FactionName,CoalitionStatus,DateJoinedCoalition,DateLeftCoalition
    25,961,Likud,Coalition,2022-12-29,
    25,954,Yesh Atid,Opposition,,
    ```

## 🖥️ Usage

### Initial Data Setup

Before using the application, you need to download the parliamentary data:

```bash
# Download all essential tables (this may take 15-30 minutes)
PYTHONPATH="./src" python -m backend.fetch_table --all
```

**Note:** If some large tables fail to download, you can fetch them individually:

```bash
# Download specific critical tables
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_PersonToPosition
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Query
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Agenda
```

### Streamlit User Interface

Launch the Streamlit self-service interface:

```bash
streamlit run src/ui/data_refresh.py --server.address localhost --server.port 8501
```

Access the UI via the local URL provided by Streamlit (typically `http://localhost:8501`). Through the UI, you can:

* **Refresh OData tables** and faction statuses
* **Explore tables** with dynamic filters
* **Run predefined analytical queries** with real parliamentary data
* **Explore 15+ predefined visualizations** covering query analytics, parliamentary activity patterns, and collaboration networks
* **Create custom visualizations** using the interactive chart builder
* **Execute custom SQL** queries against the database
* **Download data** in CSV or Excel format

### Command-Line Interface (CLI)

**Show help and available commands:**

```bash
PYTHONPATH="./src" python -m backend.fetch_table --help
```

**Common commands:**

* **Refresh all predefined OData tables and faction statuses:**
    ```bash
    PYTHONPATH="./src" python -m backend.fetch_table --all
    ```
* **Refresh a specific OData table (e.g., KNS_Person):**
    ```bash
    PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Person
    ```
* **Refresh only the faction coalition status data:**
    ```bash
    PYTHONPATH="./src" python -m backend.fetch_table --refresh-faction-status
    ```
* **Execute an SQL query against the warehouse:**
    ```bash
    PYTHONPATH="./src" python -m backend.fetch_table --sql "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main';"
    ```
* **List available OData tables:**
    ```bash
    PYTHONPATH="./src" python -m backend.fetch_table --list-tables
    ```

**Alternative simplified CLI:**

```bash
bash scripts/refresh_all.sh
```

## 📚 Usage Examples

### Common Research Scenarios

#### Scenario 1: Analyzing Parliamentary Questions by Ministry
```bash
# 1. Download query and ministry data
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Query
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_GovMinistry

# 2. Run analysis query
PYTHONPATH="./src" python -m backend.fetch_table --sql "
SELECT 
    m.Name as Ministry, 
    COUNT(*) as Query_Count,
    AVG(DATEDIFF('day', q.StartDate, q.ReplyDate)) as Avg_Response_Days
FROM KNS_Query q 
JOIN KNS_GovMinistry m ON q.GovMinistryID = m.GovMinistryID 
WHERE q.ReplyDate IS NOT NULL 
GROUP BY m.Name 
ORDER BY Query_Count DESC
"
```

#### Scenario 2: Tracking Coalition vs Opposition Activity
```bash
# Launch Streamlit UI
streamlit run src/ui/data_refresh.py

# In the UI:
# 1. Go to "Predefined Queries" → "Queries + Full Details"
# 2. Apply filter: Coalition Status = "Coalition" 
# 3. Set date range for specific Knesset term
# 4. Export results to Excel for further analysis
```

#### Scenario 3: Building Custom Visualizations
```bash
# In Streamlit UI:
# 1. Navigate to "Chart Builder" tab
# 2. Select table: "KNS_Query" 
# 3. X-axis: "StartDate", Y-axis: "Count", Color: "Coalition Status"
# 4. Create time series showing query submission patterns
# 5. Download chart as PNG for presentations
```

### Power User Workflows

#### Daily Data Refresh Automation
```bash
# Add to cron job for daily refresh:
0 6 * * * cd /path/to/knesset_refactor && PYTHONPATH="./src" python -m backend.fetch_table --all >> logs/daily_refresh.log 2>&1
```

#### Research Paper Data Export
```python
# Custom analysis script
import duckdb

# Connect to warehouse
conn = duckdb.connect('data/warehouse.duckdb')

# Complex analytical query
results = conn.execute("""
    SELECT 
        EXTRACT(year FROM q.StartDate) as Year,
        f.Name as Faction,
        COUNT(*) as Questions_Asked,
        COUNT(q.ReplyDate) as Questions_Answered,
        ROUND(COUNT(q.ReplyDate) * 100.0 / COUNT(*), 2) as Answer_Rate
    FROM KNS_Query q
    JOIN KNS_PersonToPosition ptp ON q.PersonID = ptp.PersonID
    JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
    WHERE q.StartDate >= '2020-01-01'
    GROUP BY Year, f.Name
    ORDER BY Year DESC, Questions_Asked DESC
""").fetchdf()

# Export for academic paper
results.to_excel('research_data.xlsx', index=False)
```

## 🧪 Testing

Run unit tests using pytest:

```bash
pytest
```

Test core functionality:

```bash
# Test imports
python -c "import streamlit, duckdb, pandas, plotly, aiohttp; print('✅ All core libraries imported successfully!')"

# Test database
PYTHONPATH="./src" python -m backend.fetch_table --sql "SHOW TABLES;"
```

## 🤖 AI-Powered Development & CI/CD

This project is optimized for AI-assisted development with **automated testing for AI-generated branches**.

### Automated Testing Pipeline

When AI tools (Codex, Jules, etc.) create new branches, our GitHub Actions workflow automatically:

✅ **Comprehensive Testing**
- Runs full pytest suite with 80%+ coverage requirement
- Tests async functionality with proper asyncio handling
- Verifies critical imports and database functionality

✅ **Code Quality Checks**
- Linting with flake8 (syntax errors block merge)
- Code formatting suggestions with Black
- Import sorting with isort
- Type checking with mypy

✅ **Security Scanning**
- Dependency vulnerability checks with Safety
- Code security analysis with Bandit

✅ **Intelligent Reporting**
- Detailed test summaries in GitHub Actions
- Clear merge readiness indicators
- Non-blocking suggestions for improvements

### Workflow Triggers

The CI pipeline runs on:
- 🔄 **All branch pushes** (especially AI-generated branches)
- 🔀 **Pull requests** to main/master
- 📊 **Generates comprehensive test reports**

### AI Development Best Practices

When using AI tools with this repository:

1. **Let AI create feature branches** - CI will automatically test them
2. **Check the Actions tab** for detailed test results  
3. **Look for the green checkmark** before merging
4. **Review the automated summary** for any suggestions

The workflow ensures AI-generated code meets quality standards before integration.

## 🔧 Troubleshooting

### Common Issues and Solutions

**1. ModuleNotFoundError: No module named 'utils'**
```bash
# Ensure PYTHONPATH is set when running CLI commands
PYTHONPATH="./src" python -m backend.fetch_table --help
```

**2. Installation issues**
```bash
# Ensure you're using the correct requirements
pip install --upgrade pip
pip install -r requirements.txt
```

**3. Database connection/serialization errors**
```bash
# Remove old database files and fetch fresh data
rm -rf data/warehouse.duckdb data/parquet/ data/.resume_state.json
PYTHONPATH="./src" python -m backend.fetch_table --all
```

**4. Timestamp conversion errors**
```bash
# If you get "timestamp field value out of range" errors, the data may need cleaning
# This typically happens after version upgrades - re-fetch the problematic tables
```

**5. Missing tables after download**
```bash
# Some large tables may fail during bulk download. Fetch them individually:
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_PersonToPosition
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Query
```

## 📊 Available Data Tables

The system downloads and manages these core tables:

* **KNS_Person** - Members of Knesset (MKs) personal information
* **KNS_Faction** - Political parties and factions
* **KNS_PersonToPosition** - Links people to their positions and factions *(Critical)*
* **KNS_Query** - Parliamentary questions and queries *(Critical)*
* **KNS_Agenda** - Parliamentary agenda items
* **KNS_Committee** - Committee information
* **KNS_CommitteeSession** - Committee meeting records
* **KNS_GovMinistry** - Government ministries
* **KNS_Status** - Various status codes
* **KNS_PlenumSession** - Plenary session records
* **KNS_KnessetDates** - Knesset terms and dates
* **KNS_Bill** - Bills and legislation
* **KNS_Law** - Laws and legal documents
* **KNS_IsraelLaw** - Israeli law references
* **UserFactionCoalitionStatus** - Manual coalition/opposition tracking

## 📋 System Requirements

* **Python:** 3.12+ (required)
* **Memory:** 4GB+ RAM recommended for large table processing
* **Storage:** 2GB+ free space for database and parquet files
* **Network:** Stable internet connection for API data fetching

## 📊 Available Visualizations

The platform includes 15+ predefined visualizations organized into three categories:

### Query Analytics
* **Queries by Time Period** - Track submission patterns over time
* **Query Types Distribution** - Breakdown by query type (regular, urgent, direct)
* **Response Times Analysis** - Box plots showing ministry response times by coalition status
* **Faction Status Analysis** - Query patterns by coalition/opposition membership with optional date range filtering
* **Ministry Performance** - Query distribution and reply rates by ministry
* **Ministry Workload Hierarchy** - Sunburst view of ministry → query type → status

### Agenda Analytics  
* **Agenda Items by Time Period** - Track agenda activity over time
* **Classification Distribution** - Pie charts of agenda item classifications
* **Status Distribution** - Current status of agenda items
* **Faction Activity** - Agenda items by initiating faction
* **Coalition Impact** - Agenda success rates by coalition/opposition status

### Advanced Analytics
* **Parliamentary Activity Heatmap** - Calendar view of daily activity intensity
* **MK Collaboration Network** - Network graph showing MKs with shared ministry focus
* **Coalition Timeline** - Gantt chart of coalition participation periods
* **MK Tenure Timeline** - Service periods across Knessets
* **Ministry Leadership** - Minister appointment timelines

## 🔮 Future Roadmap

### Architecture Improvements
* **Complete Chart Migration:** Finish migrating all chart implementations to the new modular system
* **Advanced Caching:** Redis integration for improved performance
* **Microservices:** Split into independent, scalable services

### Feature Enhancements
* **Advanced Statistical Analysis:** Time series forecasting, sentiment analysis integration
* **Real-time Updates:** WebSocket integration for live data streaming
* **Machine Learning:** Predictive analytics for parliamentary patterns
* **API Gateway:** RESTful API for external integrations

### User Experience
* **Dashboard Customization:** User-configurable dashboard layouts
* **Export Automation:** Scheduled report generation and distribution
* **Mobile Responsiveness:** Progressive Web App (PWA) capabilities
* **User Authentication:** Role-based access control and personalization

### Data & Integration
* **Additional Data Sources:** Integration with complementary parliamentary datasets
* **Data Quality Monitoring:** Automated data validation and quality metrics
* **Performance Optimization:** Query optimization and database tuning
* **Backup & Recovery:** Automated backup strategies and disaster recovery


## 🤝 Contributing

We welcome contributions! Here are some ways you can help:

### Issues & Bug Reports
- 🐛 **Found a bug?** [Open an issue](https://github.com/AT020993/knesset_refactor/issues) with steps to reproduce
- 💡 **Have an idea?** Share feature requests and suggestions
- 📖 **Documentation gaps?** Help improve our docs

### Development Setup
```bash
# Fork the repo, then:
git clone https://github.com/AT020993/knesset_refactor.git
cd knesset_refactor
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run tests
pytest

# Run linting (if available)
# Add your preferred linting commands here
```

### Pull Request Guidelines
- 🧪 **Add tests** for new functionality
- 📝 **Update documentation** for API changes  
- 🏷️ **Follow existing code style** and naming conventions
- ✅ **Ensure all tests pass** before submitting

### Areas Where We Need Help
- 📊 **Chart Migration:** Complete the migration of remaining chart implementations to the new modular system
- 🏗️ **Architecture Enhancement:** Further refactoring of large files (connection_manager.py, chart_renderer.py)
- 🔍 **Performance optimizations** for large dataset handling and query optimization
- 🌐 **API enhancements** for better OData integration and real-time capabilities
- 📱 **UI/UX improvements** using the new component-based architecture
- 🧪 **Test coverage** expansion, especially for the new modular components
- 📚 **Documentation** updates for the new architecture patterns and modules

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

### Third-Party Licenses
- **Knesset OData API**: Data provided by the Israeli Knesset under their terms of service
- **Dependencies**: See `requirements.txt` for all third-party libraries and their respective licenses



## 📚 References

* The official Knesset OData service description can be found in `docs/KnessetOdataManual.pdf`.
* Knesset OData API: `http://knesset.gov.il/Odata/ParliamentInfo.svc`

---

This project aims to continually improve data transparency and accessibility for parliamentary research and analytics.
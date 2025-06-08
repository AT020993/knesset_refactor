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

## 🎯 Project Goals

* **Data Accessibility:** Provide easy access to Israeli parliamentary data for researchers and analysts.
* **Self-Service:** Enable users (including non-technical ones) to independently refresh, explore, and download curated and raw data.
* **Comprehensive Data Management:** Support robust and resumable fetching, efficient storage, and timely updates of parliamentary data.
* **User-Friendly Interface:** Offer an intuitive Streamlit-based interface for data interaction, exploration, and exportation.

## ✨ Key Features

### Backend (`src/backend/fetch_table.py`)
* **Automated OData Fetching:** Retrieves data directly from the official Knesset OData API (`http://knesset.gov.il/Odata/ParliamentInfo.svc`).
* **Robust Downloading:**
    * **Checkpoint-Resume:** For large, cursor-paged tables (e.g., `KNS_CommitteeSession`, `KNS_Bill`), download progress is saved, allowing resumption after interruptions.
    * **Parallel Downloads:** Utilizes `asyncio` for concurrent fetching of multiple pages for skip-based tables, significantly speeding up data acquisition.
    * **Automatic Retries:** Implements backoff strategies for transient network errors.
* **Efficient Data Storage:**
    * **DuckDB Warehouse:** Stores all fetched data in a local DuckDB database (`data/warehouse.duckdb`) for fast querying and analysis.
    * **Parquet Files:** Mirrors each table into compressed Parquet files (`data/parquet/`) for optimized storage and interoperability with other data tools.
* **Manual Faction Coalition Status Integration:**
    * Loads and integrates user-maintained faction coalition/opposition statuses, including joining/leaving dates, from a CSV file (`data/faction_coalition_status.csv`).
    * This data enriches parliamentary analysis by providing context on faction alignments.
* **Logging:** Comprehensive logging using the `src/utils/logger_setup.py` module.

### Frontend - Streamlit UI (`src/ui/data_refresh.py`)
* **Self-Service Data Refresh:**
    * Select specific OData tables or refresh all predefined tables.
    * Monitor live progress of data fetching.
    * Dedicated button to refresh only the faction coalition status data from the CSV.
* **Predefined Queries:**
    * Execute curated SQL queries (e.g., "Queries + Full Details," "Agenda Items + Full Details") for common analytical needs.
    * Results are displayed interactively and can be downloaded.
* **Interactive Table Explorer:**
    * Select and view raw data from any table in the DuckDB warehouse.
    * Apply dynamic filters (e.g., Knesset Number, Faction) to narrow down results.
* **Dynamic Filtering:** Sidebar filters for Knesset Numbers and Factions can be applied to both Predefined Queries and the Table Explorer.
* **Ad-hoc SQL Sandbox:** Advanced users can run custom SQL queries directly against the DuckDB database.
* **Data Export:** Download results from predefined queries and the table explorer in CSV and Excel formats.
* **Table Update Status:** Displays the last updated timestamp for each table in the database.
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
│   ├── backend/
│   │   ├── fetch_table.py     # Core module for fetching & storing data, includes CLI
│   │   ├── tables.py          # Table definitions
│   │   ├── utils.py           # Utility functions
│   │   └── __init__.py
│   ├── ui/
│   │   ├── data_refresh.py    # Main Streamlit interface
│   │   ├── sidebar_components.py # Sidebar UI components
│   │   ├── ui_utils.py        # UI utility functions
│   │   ├── plot_generators.py # Visualization functions
│   │   ├── chart_builder_ui.py # Interactive chart builder
│   │   └── __init__.py
│   ├── utils/
│   │   ├── logger_setup.py    # Logging configuration
│   │   └── __init__.py
│   └── cli.py                 # Typer-based CLI (alternative to fetch_table.py CLI)
├── tests/
│   ├── conftest.py            # Pytest fixtures and configuration
│   ├── test_fetch_table.py    # Unit tests for data-fetching logic
│   └── test_views.py          # Unit tests for UI predefined queries
├── requirements.txt           # Project dependencies
└── README.md                  # This file
```

## 🛠️ Technologies Used

* **Python 3.12+** (Required)
* **DuckDB 1.2.2:** For the data warehouse
* **Pandas 2.2.3:** For data manipulation
* **Parquet:** For efficient columnar storage
* **Streamlit 1.44.1:** For the user interface
* **aiohttp 3.9.4 & backoff 2.2.1:** For robust asynchronous HTTP requests
* **OpenPyXL 3.1.5:** For Excel export functionality
* **Plotly 5.0+:** For generating interactive data visualizations
* **Pytest 8.3.5:** For unit testing
* **tqdm 4.66.1:** For progress bar visualization during data fetching
* **Typer 0.12+:** For `src/cli.py`

## 📸 Screenshots

### Streamlit Interface
*Coming soon: Screenshots of the data refresh interface, visualizations, and chart builder*

### Sample Visualizations
*Coming soon: Examples of parliamentary activity heatmaps, MK collaboration networks, and query analytics*

## 🚀 Quick Start

Get up and running in 5 minutes:

```bash
# 1. Clone and setup
git clone https://github.com/AT020993/knesset_refactor.git
cd knesset_refactor
python -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Download sample data (5-10 minutes)
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Person
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Query

# 4. Launch interface
streamlit run src/ui/data_refresh.py
```

Open `http://localhost:8501` and start exploring! 🎉

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

* Advanced statistical analysis modules
* Scheduling automatic data refreshes
* User authentication and role-based access for the UI
* Integration of additional relevant parliamentary datasets
* Performance optimizations for large dataset handling
* Export functionality for visualization reports


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
- 📊 **New visualizations** for parliamentary data analysis
- 🔍 **Performance optimizations** for large dataset handling  
- 🌐 **API enhancements** for better OData integration
- 📱 **UI/UX improvements** for the Streamlit interface
- 🧪 **Test coverage** expansion

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
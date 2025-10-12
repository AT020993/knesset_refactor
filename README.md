# 🏛️ Knesset OData Explorer

<div align="center">

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![CI Status](https://github.com/AT020993/knesset_refactor/workflows/CI%20-%20Automated%20Testing%20for%20AI-Generated%20Branches/badge.svg)](https://github.com/AT020993/knesset_refactor/actions)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.2.2-yellow.svg)](https://duckdb.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.44.1-red.svg)](https://streamlit.io/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](#-license)

**Comprehensive platform for fetching, analyzing, and visualizing Israeli parliamentary data**

*Democratizing access to parliamentary data for researchers, analysts, and citizens*

[Quick Start](#-quick-start) • [Features](#-key-features) • [Documentation](#-project-structure)

</div>

---

## 🎯 Overview

A complete parliamentary data platform providing:

- 🔄 **Automated fetching** from official Knesset OData API with circuit breaker pattern
- 💾 **Efficient storage** in DuckDB with Parquet backup
- 📊 **18+ interactive visualizations** for parliamentary analysis
- 🖥️ **User-friendly Streamlit interface** with modular architecture
- ⚙️ **Robust CLI tools** for automated workflows

## ✨ Key Features

### Backend Architecture
* **Clean Architecture:** Repository pattern with dependency injection
* **Robust API Client:** Async OData client with circuit breaker and retry logic
* **DuckDB Warehouse:** Fast querying with Parquet backup
* **Resume State Management:** Advanced checkpoint system for interrupted downloads
* **Connection Management:** Monitors and prevents database connection leaks

### Frontend - Streamlit UI
* **Modular Design:** Page-based structure with centralized state management
* **Predefined Queries:** Curated SQL queries with metadata and smart filtering
  * **100% Data Accuracy:** Date-based faction attribution for bills, agendas, and queries
  * **Complete Committee Data:** 74,951/75,051 committee sessions (99.9% coverage)
  * **Bill Timeline Analysis:** Multi-source date resolution with 98.2% coverage
  * **Coalition Status Integration:** Manual faction coalition/opposition tracking
* **Interactive Visualizations:** 18+ charts covering queries, agendas, bills, and network analysis
* **Table Explorer:** Dynamic browsing with intelligent filter application
* **SQL Sandbox:** Custom query execution with error handling
* **Data Export:** Multi-format download (CSV, Excel) with proper encoding

### Command-Line Interface
```bash
PYTHONPATH="./src" python -m backend.fetch_table --all              # Refresh all tables
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Query  # Specific table
bash scripts/refresh_all.sh                                         # Alternative CLI
```

## 🚀 Quick Start

```bash
# 1. Clone and setup
git clone https://github.com/AT020993/knesset_refactor.git
cd knesset_refactor
python -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 3. Download sample data (5-10 minutes)
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Person
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Query

# 4. Launch the interface
streamlit run src/ui/data_refresh.py
```

🎉 **Open `http://localhost:8501`** and explore!

## 📂 Project Structure

```plaintext
knesset_refactor/
├── .github/workflows/           # CI/CD with automated testing
├── data/
│   ├── faction_coalition_status.csv  # User-managed faction status
│   ├── parquet/                 # Raw parquet files (auto-generated)
│   └── warehouse.duckdb         # DuckDB database (auto-generated)
├── src/
│   ├── api/                     # OData client with circuit breaker
│   ├── backend/                 # Legacy compatibility and utilities
│   ├── config/                  # Centralized configuration
│   ├── core/                    # Dependency injection container
│   ├── data/                    # Repository pattern implementations
│   ├── ui/                      # Streamlit interface
│   │   ├── charts/              # Modular chart system
│   │   ├── pages/               # Page components
│   │   ├── queries/             # SQL query definitions
│   │   └── services/            # Business logic services
│   └── utils/                   # Logging and utilities
├── tests/                       # Unit and E2E tests
└── requirements.txt             # Dependencies
```

## 🛠️ Technologies

**Core:** Python 3.12+, DuckDB 1.2.2, Pandas 2.2.3, PyArrow 19.0.1
**Frontend:** Streamlit 1.44.1, Plotly 5.0+
**Networking:** aiohttp 3.9.4 with backoff 2.2.1
**Architecture:** Clean Architecture, Repository Pattern, Circuit Breaker, Factory Pattern, Dependency Injection
**Testing:** Pytest 8.3.5, Playwright (E2E)

## 📖 Usage Guide

### Initial Data Setup
```bash
# Download all essential tables (15-30 minutes)
PYTHONPATH="./src" python -m backend.fetch_table --all

# Or download specific tables
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_PersonToPosition
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_Query
```

### Launch Interface
```bash
streamlit run src/ui/data_refresh.py --server.address localhost --server.port 8501
```

### What You Can Do
| Feature | Description |
|---------|-------------|
| 🔄 **Data Refresh** | Update OData tables and faction statuses |
| 🔍 **Table Explorer** | Browse tables with dynamic filters |
| 📊 **Predefined Queries** | Run analytical queries with coalition status, bill merge tracking |
| 📈 **18+ Visualizations** | Query analytics, activity patterns, bill analysis, collaboration networks |
| 💻 **SQL Sandbox** | Execute custom SQL queries |
| 📥 **Data Export** | Download results in CSV or Excel format |

### Common Scenarios

**Analyzing Parliamentary Questions:**
```bash
PYTHONPATH="./src" python -m backend.fetch_table --sql "
SELECT m.Name as Ministry, COUNT(*) as Query_Count,
       AVG(DATEDIFF('day', q.StartDate, q.ReplyDate)) as Avg_Response_Days
FROM KNS_Query q
JOIN KNS_GovMinistry m ON q.GovMinistryID = m.GovMinistryID
WHERE q.ReplyDate IS NOT NULL
GROUP BY m.Name ORDER BY Query_Count DESC
"
```

**Setting Up Faction Coalition Mapping:**
```bash
# Export all factions to CSV for manual status entry
python -c "
import sys; sys.path.insert(0, 'src'); import csv, duckdb; from config.settings import Settings
with duckdb.connect(str(Settings.DEFAULT_DB_PATH), read_only=True) as con:
    result = con.execute('SELECT KnessetNum, FactionID, Name FROM KNS_Faction WHERE KnessetNum BETWEEN 1 AND 25 ORDER BY KnessetNum DESC').fetchall()
    with open('faction_coalition_mapping.csv', 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f); writer.writerow(['KnessetNum', 'FactionID', 'FactionName', 'CoalitionStatus'])
        for row in result: writer.writerow([row[0], row[1], row[2], ''])
"
# Open in Excel, enter Coalition/Opposition status in Column D
```

## 🧪 Testing

```bash
# Unit tests with coverage
pytest --cov=src --cov-report=term-missing

# E2E tests (requires app running)
pip install -r requirements-dev.txt
playwright install --with-deps
streamlit run src/ui/data_refresh.py  # In one terminal
pytest -m e2e --base-url http://localhost:8501  # In another terminal
```

**E2E Coverage:** Page loading, data refresh controls, predefined queries, sidebar navigation, error handling, responsive design, performance.

## 🤖 AI-Powered Development & CI/CD

Automated testing for AI-generated branches:
- ✅ Full pytest suite with 80%+ coverage
- ✅ E2E testing with Playwright (7/7 passing)
- ✅ Code quality checks (flake8, Black, isort, mypy)
- ✅ Security scanning (Safety, Bandit)

**Workflow triggers:** All branch pushes, pull requests to main/master.

## 🔧 Troubleshooting

**ModuleNotFoundError:**
```bash
PYTHONPATH="./src" python -m backend.fetch_table --help
```

**Database errors:**
```bash
rm -rf data/warehouse.duckdb data/parquet/ data/.resume_state.json
PYTHONPATH="./src" python -m backend.fetch_table --all
```

**Missing tables:**
```bash
PYTHONPATH="./src" python -m backend.fetch_table --table KNS_PersonToPosition
```

## 🏆 Performance Achievements

### Committee Session Data
- **99.9% Complete Dataset:** 74,951/75,051 committee session items
- **5.1x Coverage Improvement:** From 1,992 to 10,232 bills with session data
- **17.6% Bill Coverage:** 1 in 6 bills with verified committee information
- **Session Range:** 1-107 sessions per bill (average 3.6)

### Data Quality
- **100% Faction Attribution Accuracy:** Date-based matching across all charts
- **98.2% Bill Timeline Coverage:** Multi-source date resolution
- **71.4% Committee Resolution:** Historical committee data (Knessets 1-25)

## 📊 Available Data Tables

**Core Tables:** KNS_Person, KNS_Faction, KNS_PersonToPosition, KNS_Query, KNS_Agenda, KNS_Committee, KNS_CommitteeSession, KNS_CmtSessionItem, KNS_GovMinistry, KNS_Status, KNS_PlenumSession, KNS_PlmSessionItem, KNS_KnessetDates, KNS_Bill, KNS_Law, KNS_IsraelLaw, UserFactionCoalitionStatus

## 📊 Available Visualizations

**Query Analytics:** Time periods, types distribution, response times, faction status, ministry performance
**Agenda Analytics:** Time periods, classifications, status, faction activity, coalition impact
**Bills Analytics:** Status distribution, time periods, subtypes, faction activity, coalition status, top initiators
**Advanced Analytics:** Activity heatmap, MK collaboration network, coalition timeline, MK tenure timeline, ministry leadership

## 🤝 Contributing

We welcome contributions!

**Development Setup:**
```bash
git clone https://github.com/AT020993/knesset_refactor.git
cd knesset_refactor
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest  # Run tests
```

**Guidelines:**
- 🧪 Add tests for new functionality
- 📝 Update documentation for API changes
- 🏷️ Follow existing code style
- ✅ Ensure all tests pass

**Areas for Help:**
- Chart migration to modular system
- Performance optimizations
- UI/UX improvements
- Test coverage expansion
- Documentation updates

## 📄 License

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This project is licensed under the **MIT License**.

**Acknowledgments:** Knesset OData API, third-party libraries in [`requirements.txt`](requirements.txt)

---

<div align="center">

**Made with ❤️ for parliamentary transparency and data accessibility**

[⬆️ Back to Top](#️-knesset-odata-explorer) • [🐛 Report Bug](https://github.com/AT020993/knesset_refactor/issues) • [💡 Request Feature](https://github.com/AT020993/knesset_refactor/issues)

</div>

## 📚 References

* Official Knesset OData documentation: `docs/KnessetOdataManual.pdf`
* Knesset OData API: `http://knesset.gov.il/Odata/ParliamentInfo.svc`

---

*Continually improving data transparency and accessibility for parliamentary research and analytics.*

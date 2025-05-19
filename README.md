# Knesset OData Explorer

A comprehensive platform designed to fetch, store, analyze, and visualize parliamentary data from the Israeli Knesset's Open Data (OData) API. This project empowers researchers, analysts, and non-technical users to easily manage and explore Israeli parliamentary data.

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
    * Results are displayedインタラクティブに and can be downloaded.
* **Interactive Table Explorer:**
    * Select and view raw data from any table in the DuckDB warehouse.
    * Apply dynamic filters (e.g., Knesset Number, Faction) to narrow down results.
* **Dynamic Filtering:** Sidebar filters for Knesset Numbers and Factions can be applied to both Predefined Queries and the Table Explorer.
* **Ad-hoc SQL Sandbox:** Advanced users can run custom SQL queries directly against the DuckDB database.
* **Data Export:** Download results from predefined queries and the table explorer in CSV and Excel formats.
* **Table Update Status:** Displays the last updated timestamp for each table in the database.
* **Predefined Visualizations:**
    * Select and view various predefined plots and charts based on the Knesset data (e.g., queries per year, distribution of query types, agenda classifications).
    * Interactive engagement with charts powered by Plotly.
* **Interactive Chart Builder:**
    * Dynamically construct custom visualizations (bar charts, scatter plots, etc.) directly from the data tables.
    * Users can select tables, columns for axes, color encodings, and faceting to explore data patterns.

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
├── data/
│   ├── faction_coalition_status.csv # User-managed faction status (create this file)
│   ├── parquet/               # Raw parquet files (auto-generated)
│   └── warehouse.duckdb       # DuckDB database storage (auto-generated)
│   └── .resume_state.json     # Internal file for resuming downloads (auto-generated)
├── docs/
│   └── KnessetOdataManual.pdf # Official Knesset OData documentation
├── scripts/
│   └── refresh_all.sh         # Shell script for a full data refresh using the Typer CLI
├── src/
│   ├── backend/
│   │   ├── fetch_table.py     # Core module for fetching & storing data, includes argparse CLI
│   │   └── __init__.py
│   ├── ui/
│   │   ├── data_refresh.py    # Streamlit interface for data management
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
````

## 🛠️ Technologies Used

  * **Python 3.12** (as specified in `.github/workflows/ci.yml`)
  * **DuckDB:** For the data warehouse.
  * **Pandas:** For data manipulation.
  * **Parquet:** For efficient columnar storage.
  * **Streamlit:** For the user interface.
  * **aiohttp & backoff:** For robust asynchronous HTTP requests.
  * **OpenPyXL:** For Excel export functionality.
  * **Plotly:** For generating interactive data visualizations.
  * **Pytest:** For unit testing.
  * **tqdm:** For progress bar visualization during data fetching.
  * **Typer:** For `src/cli.py`.

## 🚀 Getting Started

### Prerequisites

  * Python 3.12 or higher.
  * Git.

### Installation

1.  **Clone the repository:**

    ```bash
    git clone <repository-url>
    cd knesset_refactor
    ```

2.  **Create and activate a virtual environment:**

    ```bash
    python -m venv .venv
    # On Windows
    # .venv\Scripts\activate
    # On macOS/Linux
    source .venv/bin/activate
    ```

3.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

4.  **(Optional but Recommended) Create Faction Status CSV:**
    Create a file named `faction_coalition_status.csv` in the `data/` directory. This file is used to track the coalition/opposition status of factions. If it doesn't exist, the faction status features will still run but won't load any custom data.
    **Structure for `data/faction_coalition_status.csv`:**

    ```csv
    KnessetNum,FactionID,FactionName,CoalitionStatus,DateJoinedCoalition,DateLeftCoalition
    25,961,Likud,Coalition,2022-12-29,
    25,954,Yesh Atid,Opposition,,
    ```

      * `KnessetNum` (Integer): The Knesset number.
      * `FactionID` (Integer): The numerical ID of the faction.
      * `FactionName` (Text): The name of the faction (for reference).
      * `CoalitionStatus` (Text): e.g., "Coalition", "Opposition".
      * `DateJoinedCoalition` (Date): YYYY-MM-DD format. Empty if not applicable.
      * `DateLeftCoalition` (Date): YYYY-MM-DD format. Empty if not applicable.

## 🖥️ Usage

### Streamlit User Interface

Launch the Streamlit self-service interface:

```bash
streamlit run src/ui/data_refresh.py
```

Access the UI via the local or network URL provided by Streamlit. Through the UI, you can:

  * Refresh OData tables and faction statuses.
  * Explore tables with filters.
  * Run predefined analytical queries.
  * Execute custom SQL.
  * Download data in CSV or Excel format.

### Command-Line Interface (CLI)

The primary CLI is part of `src/backend/fetch_table.py`. You can use it for various backend operations.

**Show help and available commands:**

```bash
python src/backend/fetch_table.py --help
```

**Common commands:**

  * **Refresh all predefined OData tables and faction statuses:**
    ```bash
    python src/backend/fetch_table.py --all
    ```
  * **Refresh a specific OData table (e.g., KNS\_Person):**
    ```bash
    python src/backend/fetch_table.py --table KNS_Person
    ```
  * **Refresh only the faction coalition status data from `data/faction_coalition_status.csv`:**
    ```bash
    python src/backend/fetch_table.py --refresh-faction-status
    ```
  * **Execute an SQL query against the warehouse:**
    ```bash
    python src/backend/fetch_table.py --sql "SELECT KnessetNum, COUNT(*) FROM KNS_Faction GROUP BY KnessetNum;"
    ```
  * **List available OData tables:**
    ```bash
    python src/backend/fetch_table.py --list-tables
    ```

A convenience script `scripts/refresh_all.sh` is also provided, which uses the simpler Typer-based CLI (`src/cli.py`) to refresh all tables:

```bash
bash scripts/refresh_all.sh
```

## 🧪 Testing

Run unit tests using pytest:

```bash
pytest
```

This will discover and run tests from the `tests/` directory. Test coverage reports can also be generated if `pytest-cov` is configured.

## 參考資料 (References)

  * The official Knesset OData service description can be found in `docs/KnessetOdataManual.pdf`.

## 🔮 Future Roadmap

  * Enhanced data visualization modules within the Streamlit UI.
  * Scheduling automatic data refreshes.
  * User authentication and role-based access for the UI.
  * Integration of additional relevant parliamentary datasets.

## 🤝 Contributing

Contributions are welcome\! Please feel free to submit issues or pull requests to enhance the project's functionality and usability.

## 📄 License

(Specify your project's license here, e.g., MIT, Apache 2.0. If not specified, consider adding one.)

-----

This project aims to continually improve data transparency and accessibility for parliamentary research and analytics.

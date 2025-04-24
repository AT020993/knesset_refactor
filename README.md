# Knesset OData Project

A comprehensive and easy-to-use platform designed to fetch, store, analyze, and visualize parliamentary data from the Israeli Knesset's Open Data (OData) API. This project empowers researchers, analysts, and non-technical users to easily manage and explore parliamentary data without needing direct coding expertise.

## 🎯 Project Goals

- **Data Accessibility:** Provide easy access to Israeli parliamentary data for researchers and analysts.
- **Self-Service:** Enable non-technical users to independently refresh, explore, and download curated data.
- **Comprehensive Data Management:** Support scheduled or manual fetching, storing, and updating of data.
- **User-Friendly Interface:** Offer an intuitive interface for data exploration, visualization, and exportation.

## 📂 Project Structure

```plaintext
knesset_refactor/
├── data/
│   ├── warehouse.duckdb       # DuckDB database storage
│   └── parquet/               # Raw parquet files
│
├── src/
│   ├── backend/
│   │   ├── fetch_table.py     # Core module for fetching & storing data
│   │   └── __init__.py
│   └── ui/
│       ├── data_refresh.py    # Streamlit interface for data management
│       └── __init__.py
│
├── tests/
│   └── test_fetch_table.py    # Unit tests for data-fetching logic
│
├── requirements.txt           # Project dependencies
└── README.md                  # Project overview and documentation
```

## 🚀 Key Features

- **Automatic Data Fetching:** Retrieves data from Knesset OData API.
- **Flexible Data Storage:** Uses DuckDB and Parquet files for efficient data handling.
- **Self-Service Interface:** Streamlit-based user interface allowing:
  - Manual data refresh with progress monitoring.
  - Exporting curated datasets in CSV and Excel formats.
  - Running ad-hoc SQL queries for advanced data exploration.

## 🛠️ Technologies Used

- **Python 3.12** for core backend logic.
- **DuckDB** as a lightweight and performant database engine.
- **Parquet** files for efficient columnar storage of large datasets.
- **Streamlit** for rapid UI development and data visualization.
- **Pandas** for data manipulation and analysis.
- **OpenPyXL** for Excel export support.

## 🖥️ Getting Started

### Prerequisites

Ensure you have Python 3.12 installed.

### Installation

```bash
git clone <repository-url>
cd knesset_refactor
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Running the Application

Launch the Streamlit self-service interface:

```bash
streamlit run src/ui/data_refresh.py
```

Access the UI via the provided local or network URL.

## 📈 Usage

### Fetching & Refreshing Data
- Select tables to refresh via the sidebar controls.
- Monitor live fetch progress directly in the UI.

### Data Export
- Ready-made CSV and Excel exports available directly from the interface.
- Customize or extend exports by editing predefined SQL queries.

### Advanced SQL Queries
- Execute ad-hoc SQL queries in the provided sandbox for detailed analyses.

## 🔮 Future Roadmap

- Enhanced data visualization modules.
- Scheduling automatic data refreshes.
- User authentication and role-based access.
- Additional parliamentary datasets integration.

## 🤝 Contributing

Your contributions are welcome! Please submit issues or pull requests to enhance the project's functionality and usability.

## 📄 License

Specify your project's license here (e.g., MIT, Apache 2.0).

---

This project aims to continually improve data transparency and accessibility for parliamentary research and analytics.


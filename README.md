# Knesset Data Platform

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![CI Status](https://github.com/AT020993/knesset_refactor/workflows/CI%20-%20Automated%20Testing%20for%20AI-Generated%20Branches/badge.svg)](https://github.com/AT020993/knesset_refactor/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A platform for downloading, analyzing, and viewing Israeli parliament (Knesset) data. Built for researchers and analysts who want to explore parliamentary activity without coding.

## What This Does

- Downloads data from the official Knesset database
- Stores it in a local database (DuckDB)
- Shows the data through a web interface (Streamlit)
- Creates charts and graphs for analysis
- Lets you export data to Excel or CSV

## What You Can Analyze

- **Parliamentary Questions**: Who asks what, response times, trending topics
- **Bills**: Tracking from proposal to law, voting patterns, author analysis
- **Committee Activity**: Meeting patterns, member participation
- **Member of Knesset (MK) Data**: Voting records, party changes, activity levels
- **Collaboration Networks**: Who works with whom across party lines

## Quick Start

### Using Streamlit Cloud (Easiest)

1. Visit the deployed app (URL will be added after deployment)
2. Click "Refresh Data" to download the latest Knesset data
3. Start exploring with the charts and queries

### Running Locally

```bash
# 1. Get the code
git clone https://github.com/AT020993/knesset_refactor.git
cd knesset_refactor

# 2. Set up Python environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 3. Install requirements
pip install -r requirements.txt

# 4. Download sample data (5-10 minutes)
PYTHONPATH=./src python -m cli refresh --table KNS_Person

# 5. Start the app
streamlit run src/ui/data_refresh.py
```

Open your browser to `http://localhost:8501`

## Using the Interface

### 1. Data Refresh Page
- Download data from Knesset servers
- Choose specific tables or download everything
- Takes 15-30 minutes for full download
- Only needs to be done once, then refresh as needed

### 2. Table Explorer
- Browse raw data tables
- Filter by Knesset number (1-25)
- Search and sort columns
- Export to Excel/CSV

### 3. Predefined Queries
- Ready-to-use analysis queries
- Includes coalition/opposition status
- Bill tracking with merge detection
- Automatically joins related tables

### 4. Charts & Analysis
Over 20 visualizations including:
- **Query Analytics**: Response times, ministry performance, trends
- **Bill Analytics**: Status tracking, faction activity, success rates
- **Collaboration Networks**: Who works with whom, coalition patterns
- **Activity Heatmaps**: When things happen in parliament

### 5. SQL Sandbox
Write your own queries if you know SQL (optional).

## Project Structure

```
knesset_refactor/
├── data/                    # Database and downloaded data
├── src/
│   ├── api/                # Downloads data from Knesset
│   ├── data/               # Saves and retrieves data
│   ├── ui/                 # Web interface (Streamlit)
│   │   ├── charts/         # Visualization code
│   │   └── queries/        # Predefined SQL queries
│   └── utils/              # Helper functions
└── tests/                  # Automated tests
```

## Data Sources

All data comes from the official [Knesset OData API](http://knesset.gov.il/Odata/ParliamentInfo.svc).

Main tables:
- `KNS_Person` - MK biographical data
- `KNS_Faction` - Political parties/factions
- `KNS_Bill` - Proposed and passed legislation
- `KNS_Query` - Parliamentary questions
- `KNS_Committee` - Committee structure
- `KNS_CommitteeSession` - Committee meeting records

Full documentation: `docs/KnessetOdataManual.pdf`

## Command Line Tools

```bash
# Download all tables
PYTHONPATH=./src python -m cli refresh

# Download specific table
PYTHONPATH=./src python -m cli refresh --table KNS_Query

# Refresh only faction coalition status
PYTHONPATH=./src python -m cli refresh-factions

# Alternative: use script
bash scripts/refresh_all.sh
```

## Requirements

- Python 3.12 or higher
- Internet connection for downloading data
- ~1GB disk space for full dataset

## Testing

```bash
# Run all tests
PYTHONPATH=./src pytest

# With coverage report
PYTHONPATH=./src pytest --cov=src --cov-report=term-missing

# End-to-end tests (requires app running)
pip install -r requirements-dev.txt
playwright install
PYTHONPATH=./src pytest -m e2e --base-url http://localhost:8501
```

## Contributing

Contributions welcome! Areas where help is needed:
- Adding new visualizations
- Improving data processing speed
- Better documentation
- Bug fixes

## License

MIT License - See LICENSE file for details.

## Support

- Check [RESEARCHER_GUIDE.md](RESEARCHER_GUIDE.md) for detailed usage instructions
- See [ARCHITECTURE.md](ARCHITECTURE.md) for technical details
- Open an [issue](https://github.com/AT020993/knesset_refactor/issues) for bugs or questions

---

**Made for parliamentary transparency and open data access**

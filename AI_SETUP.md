# 🤖 AI Development Environment Setup

This document provides setup instructions for AI tools (like Jules AI, Codex, etc.) to work with the Knesset OData Explorer project in an isolated, VM-like environment using Docker.

## 🚀 Quick Start for AI Tools

### Prerequisites
- Docker and Docker Compose installed
- Git access to this repository

### 1. One-Command Setup
```bash
# Clone repository and setup everything
git clone <repository-url>
cd knesset_refactor
chmod +x docker-setup.sh
./docker-setup.sh setup
```

### 2. Access the Environment
```bash
# For development work
./docker-setup.sh up dev

# Get shell access for coding
./docker-setup.sh shell
```

### 3. Access the Application
- **Development UI**: http://localhost:8502
- **Production UI**: http://localhost:8501

## 🐳 Container Services

### Main Services
1. **knesset-app** (Port 8501): Production-ready Streamlit application
2. **knesset-dev** (Port 8502): Development environment with live code mounting
3. **knesset-backup**: Optional automated database backup service

### Key Features
- ✅ **Isolated Environment**: No conflicts with host system
- ✅ **Persistent Data**: Database and logs preserved between runs
- ✅ **Live Development**: Code changes reflected immediately in dev mode
- ✅ **Health Monitoring**: Built-in health checks and auto-restart
- ✅ **Security**: Non-root user execution

## 🛠️ Development Commands

### Container Management
```bash
# Start development environment
./docker-setup.sh up dev

# Start production environment  
./docker-setup.sh up production

# Stop all containers
./docker-setup.sh stop

# View logs
./docker-setup.sh logs

# Clean everything
./docker-setup.sh clean
```

### Development Tasks
```bash
# Get shell access
./docker-setup.sh shell

# Run tests
./docker-setup.sh test

# Inside container - common tasks:
docker-compose exec knesset-dev bash

# Data operations
python -m backend.fetch_table --table KNS_Person
python -m backend.fetch_table --all

# Run tests
pytest

# Check code quality
flake8 src/ tests/
black --check src/ tests/
mypy src/
```

## 📊 Working with Data

### Sample Data Setup
```bash
# Download essential tables for testing
docker-compose exec knesset-dev python -m backend.fetch_table --table KNS_Person
docker-compose exec knesset-dev python -m backend.fetch_table --table KNS_Query
docker-compose exec knesset-dev python -m backend.fetch_table --table KNS_Faction
```

### Database Operations
```bash
# Run SQL queries
docker-compose exec knesset-dev python -m backend.fetch_table --sql "SELECT COUNT(*) FROM KNS_Person"

# Check available tables
docker-compose exec knesset-dev python -m backend.fetch_table --list-tables
```

## 🧪 Testing & Quality

### Running Tests
```bash
# Full test suite
./docker-setup.sh test

# Specific test files
docker-compose exec knesset-dev pytest tests/test_cli.py -v

# With coverage
docker-compose exec knesset-dev pytest --cov=src --cov-report=term-missing
```

### Code Quality Checks
```bash
# Inside container
flake8 src/ tests/ --max-line-length=88
black src/ tests/
isort src/ tests/
mypy src/ --ignore-missing-imports
```

## 📁 Project Structure (Containerized)

```
/app/ (Container working directory)
├── src/                    # Source code (PYTHONPATH=/app/src)
│   ├── api/               # API layer with OData client
│   ├── backend/           # Data fetching and management
│   ├── config/            # Configuration management
│   ├── ui/                # Streamlit UI components
│   └── utils/             # Utility functions
├── tests/                 # Test files
├── data/                  # Persistent data (mounted volume)
│   ├── warehouse.duckdb   # Main database
│   └── parquet/           # Backup parquet files
├── logs/                  # Application logs (mounted volume)
└── requirements.txt       # Python dependencies
```

## 🌐 Environment Variables

The container automatically sets:
```bash
PYTHONPATH=/app/src                    # Python module path
STREAMLIT_CACHE_DISABLED=1            # Disable caching for development
LOG_LEVEL=INFO                         # Logging level
DATABASE_PATH=/app/data/warehouse.duckdb
```

## 🔧 Troubleshooting

### Common Issues

**Container won't start:**
```bash
# Check Docker status
docker ps -a
docker-compose logs

# Rebuild from scratch
./docker-setup.sh clean
./docker-setup.sh build
./docker-setup.sh up dev
```

**Permission issues:**
```bash
# Fix data directory permissions
sudo chown -R $USER:$USER data logs
chmod 755 data logs
```

**Database errors:**
```bash
# Reset database
rm -f data/warehouse.duckdb data/.resume_state.json
./docker-setup.sh setup  # Re-download sample data
```

**Port conflicts:**
```bash
# Check what's using the ports
lsof -i :8501
lsof -i :8502

# Use different ports in docker-compose.yml if needed
```

### Development Tips

1. **Live Development**: Use `knesset-dev` service for code changes
2. **Data Persistence**: Database survives container restarts
3. **Debugging**: Access container logs with `./docker-setup.sh logs`
4. **Testing**: Always run tests before committing changes
5. **Clean State**: Use `./docker-setup.sh clean` for fresh environment

## 🚀 AI-Specific Workflows

### For Code Analysis Tasks
```bash
# Start development environment
./docker-setup.sh up dev

# Access shell for exploration
./docker-setup.sh shell

# Analyze codebase
find src/ -name "*.py" | head -10
grep -r "class.*:" src/
```

### For Feature Development
```bash
# Start with development service
./docker-setup.sh up dev

# Make changes to source code (changes are live-mounted)
# Test changes immediately at http://localhost:8502

# Run tests
./docker-setup.sh test

# Check code quality
docker-compose exec knesset-dev flake8 src/
```

### For Data Analysis
```bash
# Ensure data is available
./docker-setup.sh setup

# Access Streamlit UI for exploration
# Open http://localhost:8502

# Or use CLI for programmatic access
docker-compose exec knesset-dev python -m backend.fetch_table --sql "SELECT * FROM KNS_Person LIMIT 5"
```

## 📚 Additional Resources

- **Main README**: `README.md` - Comprehensive project documentation
- **Architecture**: `ARCHITECTURE.md` - System design and patterns  
- **CI/CD**: `.github/workflows/ci.yml` - Automated testing for AI branches
- **Configuration**: `src/config/` - All configuration files
- **API Documentation**: `docs/KnessetOdataManual.pdf` - Official Knesset API docs

## 🔗 Integration with Existing CI/CD

The Docker setup integrates seamlessly with the existing GitHub Actions CI/CD pipeline:

- ✅ **Automated Testing**: All AI-generated branches are automatically tested
- ✅ **Code Quality**: Linting and formatting checks
- ✅ **Security Scanning**: Dependency and code security analysis
- ✅ **Coverage Reports**: Test coverage monitoring

The AI environment mirrors the CI environment, ensuring consistency between development and testing.
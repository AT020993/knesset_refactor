# OpenAI Codex Agent Configuration

## 🔧 Environment Variables

Configure these environment variables in your Codex workspace:

### Required Variables
```bash
PYTHONPATH=./src                    # Python module path
LOG_LEVEL=DEBUG                     # Enable debug logging
STREAMLIT_CACHE_DISABLED=1          # Disable caching for development
```

### Optional Variables  
```bash
# Database paths
DATABASE_PATH=./data/warehouse.duckdb
PARQUET_PATH=./data/parquet

# API settings
KNESSET_API_BASE_URL=http://knesset.gov.il/Odata/ParliamentInfo.svc
API_TIMEOUT=30
API_MAX_RETRIES=3

# Performance tuning
CONNECTION_POOL_SIZE=8
QUERY_TIMEOUT_SECONDS=60
MAX_ROWS_FOR_CHART_BUILDER=50000

# Development flags
ENABLE_DEBUG_MODE=true
ENABLE_VERBOSE_LOGGING=true
```

## 🔐 Secrets Configuration

For production or secure development, configure these as secrets:

```bash
# API Authentication (if required in future)
KNESSET_API_KEY=<your-api-key>

# Database passwords (if using remote DB)
DATABASE_PASSWORD=<secure-password>

# Encryption keys (for sensitive data)
ENCRYPTION_KEY=<encryption-key>
```

**Note**: Secrets are only available during setup scripts, not during agent execution.

## 🚀 Setup Script Configuration

Set the setup script path to: `scripts/agent-setup.sh`

This script will:
- ✅ Install all Python dependencies
- ✅ Set up development tools (black, mypy, flake8, pytest)
- ✅ Create necessary directories
- ✅ Download sample data for testing
- ✅ Configure environment variables
- ✅ Run verification tests

## 🎯 Recommended Codex Workspace Settings

### Task Configuration
```yaml
# Timeout settings
task_timeout: 30m          # Allow time for data downloads
setup_timeout: 10m         # Setup script can take time

# Resource allocation
memory_limit: 4GB          # DuckDB operations can be memory intensive
cpu_limit: 2               # Parallel operations benefit from multiple cores

# Network access
network_access: true       # Required for Knesset API calls
```

### File Patterns to Watch
Monitor these files for changes:
- `src/**/*.py`            # Source code changes
- `tests/**/*.py`          # Test files  
- `requirements.txt`       # Dependencies
- `*.md`                   # Documentation
- `docker-compose.yml`     # Container config

### Ignore Patterns
Don't monitor these:
- `data/warehouse.duckdb*` # Large database files
- `data/parquet/**`        # Parquet cache files
- `logs/**`                # Log files
- `.venv/**`               # Virtual environment
- `__pycache__/**`         # Python cache

## 🧪 Testing Configuration

### Test Commands
Set these as quick test commands:
```bash
# Quick verification
./scripts/quick-test.sh

# Full test suite
pytest --cov=src --cov-report=term-missing

# Type checking
mypy src/ --ignore-missing-imports

# Code formatting
black src/ tests/ && isort src/ tests/
```

### Test Environment Variables
```bash
TEST_DATABASE_PATH=./data/test_warehouse.duckdb
PYTEST_TIMEOUT=300
PYTEST_WORKERS=auto        # Parallel test execution
```

## 📊 Development Workflow

### 1. Initial Setup
The setup script automatically handles:
- Virtual environment creation
- Dependency installation  
- Sample data download
- Development tool setup

### 2. Code Development
Use these commands frequently:
```bash
# Format code
black src/ tests/
isort src/ tests/

# Type checking
mypy src/

# Run tests
pytest

# Start application
streamlit run src/ui/data_refresh.py
```

### 3. Data Operations
```bash
# Download specific tables
python -m backend.fetch_table --table KNS_Person

# Run custom queries
python -m backend.fetch_table --sql "SELECT COUNT(*) FROM KNS_Person"

# List available tables
python -m backend.fetch_table --list-tables
```

## 🐳 Docker Integration

If you prefer containerized development:
```bash
# Use Docker instead of native setup
./docker-setup.sh up dev
./docker-setup.sh shell
```

The Docker environment includes all the same tools and setup as the native environment.

## 🔍 Debugging and Monitoring

### Log Files
Monitor these for issues:
- `logs/application.log`   # Application logs
- `logs/error.log`         # Error logs  
- `logs/performance.log`   # Performance metrics

### Health Checks
```bash
# Verify database
./scripts/verify-db.sh

# Check imports
python -c "import streamlit, duckdb, pandas; print('OK')"

# Test API connectivity
python -m backend.fetch_table --table KNS_Person --limit 1
```

## 🎯 Success Metrics

Before completing tasks, ensure:
- [ ] All tests pass (`pytest`)
- [ ] Type checking passes (`mypy src/`)
- [ ] Code is formatted (`black`, `isort`)
- [ ] No critical lint issues (`flake8`)
- [ ] Application starts successfully
- [ ] Database operations work
- [ ] No connection leaks detected

## 📚 Additional Resources

- **Full Setup Guide**: `AGENTS.md`
- **Docker Guide**: `AI_SETUP.md`  
- **Project Architecture**: `ARCHITECTURE.md`
- **Contributing**: `README.md`

## 🆘 Troubleshooting

### Common Issues
1. **Import errors**: Check `PYTHONPATH=./src`
2. **Database locks**: Restart and check for hung processes
3. **API timeouts**: Increase `API_TIMEOUT` value
4. **Memory issues**: Monitor usage with large datasets

### Getting Help
- Check `AGENTS.md` for detailed instructions
- Review logs in `logs/` directory
- Run `./scripts/verify-db.sh` for diagnostics
- Use `./scripts/quick-test.sh` for rapid debugging
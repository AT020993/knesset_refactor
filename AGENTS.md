# AI Agent Instructions for Knesset OData Explorer

## 🤖 Setup for OpenAI Codex and Other AI Agents

This project is optimized for AI-assisted development with automated setup scripts and clear workflows.

## 🚀 Quick Start

### Environment Setup
The setup script will automatically:
- Install all Python dependencies 
- Set up development tools (linters, formatters, type checkers)
- Configure the database and data directories
- Download sample data for testing
- Run verification tests

### Development Workflow
1. **Use the setup script first** - It prepares everything you need
2. **Follow the Docker workflow** - Use containerized development when possible
3. **Run tests frequently** - Use `pytest` to verify changes
4. **Use type checking** - Run `mypy src/` before committing
5. **Format code** - Use `black` and `isort` for consistent style

## 🎯 Common Tasks

### Adding New Features
1. **Analyze existing code structure** in `src/` directory
2. **Follow modular architecture** - separate concerns between API, backend, UI layers
3. **Add tests** for new functionality in `tests/` directory
4. **Update documentation** if adding public APIs
5. **Run full test suite** before completing

### Bug Fixes
1. **Reproduce the issue** with existing tests or create new ones
2. **Check logs** in `logs/` directory for error context
3. **Use debugger-friendly environment** - set `LOG_LEVEL=DEBUG`
4. **Verify fix** with both unit tests and integration tests

### Data Operations
1. **Use CLI commands** - `python -m backend.fetch_table --help`
2. **Work with DuckDB** - Database is in `data/warehouse.duckdb`
3. **Check data integrity** after changes with predefined queries
4. **Backup important data** before major changes

## 🧪 Testing Requirements

### Always Run These Before Committing:
```bash
# Type checking
mypy src/ --ignore-missing-imports

# Code formatting  
black src/ tests/
isort src/ tests/

# Linting
flake8 src/ tests/ --max-line-length=88

# Tests with coverage
pytest --cov=src --cov-report=term-missing
```

### Critical Test Coverage:
- All new API endpoints or data operations
- UI components with user interactions
- Error handling and edge cases
- Database operations and migrations

## 🏗️ Architecture Guidelines

### File Organization:
- `src/api/` - External API interactions (Knesset OData)
- `src/backend/` - Data processing and CLI tools
- `src/config/` - Configuration management
- `src/ui/` - Streamlit UI components and pages
- `src/utils/` - Shared utilities and logging

### Coding Standards:
- **Type hints required** for all functions and methods
- **Docstrings required** for public functions
- **Error handling** - Use custom exceptions from `src/api/error_handling.py`
- **Logging** - Use the logger from `src/utils/logger_setup.py`
- **Configuration** - Use settings from `src/config/`

### Performance Considerations:
- **Database connections** - Use connection manager to prevent leaks
- **Async operations** - Use `asyncio` for API calls and I/O
- **Caching** - Leverage Streamlit caching for expensive operations
- **Memory usage** - Be mindful with large datasets

## 🔧 Development Environment

### Recommended Approach: Docker
```bash
# Start development environment
./docker-setup.sh up dev

# Get shell access
./docker-setup.sh shell

# Run tests inside container
./docker-setup.sh test
```

### Alternative: Native Python
```bash
# Run setup script first
./scripts/agent-setup.sh

# Activate environment
source .venv/bin/activate
export PYTHONPATH="./src"

# Start development
streamlit run src/ui/data_refresh.py
```

## 📊 Data Workflow

### Working with Knesset Data:
1. **Understand the schema** - Check `src/backend/tables.py`
2. **Use predefined queries** - See `src/ui/queries/predefined_queries.py`
3. **Test with sample data** first before full downloads
4. **Monitor API limits** - Use circuit breaker patterns

### Database Operations:
```bash
# Download specific tables
python -m backend.fetch_table --table KNS_Person

# Run custom queries
python -m backend.fetch_table --sql "SELECT COUNT(*) FROM KNS_Person"

# List available tables
python -m backend.fetch_table --list-tables
```

## 🚨 Important Notes

### Security:
- **Never commit** database files or API keys
- **Use environment variables** for sensitive configuration
- **Validate inputs** especially for SQL queries and API calls

### Error Handling:
- **Check circuit breaker status** before API operations
- **Use retry logic** for transient failures
- **Log errors appropriately** with context

### Performance:
- **Monitor database size** - DuckDB files can grow large
- **Use pagination** for large result sets
- **Profile slow operations** with logging

## 🔍 Debugging Tips

### Common Issues:
1. **Import errors** - Check `PYTHONPATH` is set to `./src`
2. **Database locks** - Ensure connections are properly closed
3. **API timeouts** - Check network and increase timeout if needed
4. **Memory issues** - Monitor RAM usage with large datasets

### Useful Commands:
```bash
# Check database status
python -c "import duckdb; print(duckdb.connect('data/warehouse.duckdb').execute('SHOW TABLES').fetchall())"

# View logs
tail -f logs/application.log

# Check environment
python -c "import sys; print('\\n'.join(sys.path))"
```

## 📚 Key Files to Understand

- `src/config/settings.py` - Application configuration
- `src/api/odata_client.py` - API client with retry logic
- `src/backend/connection_manager.py` - Database connection handling
- `src/ui/data_refresh.py` - Main Streamlit application
- `tests/conftest.py` - Test configuration and fixtures

## 🎯 Success Criteria

Before completing any task:
- [ ] All tests pass (`pytest`)
- [ ] Code is properly formatted (`black`, `isort`)
- [ ] Type checking passes (`mypy`)
- [ ] No critical linting issues (`flake8`)
- [ ] Documentation is updated if needed
- [ ] Changes are backwards compatible
- [ ] Performance impact is acceptable
#!/bin/bash
set -e

# Minimal setup script for OpenAI Codex - focuses on getting environment ready quickly
# This script prioritizes speed and reliability over comprehensive testing

echo "🚀 Minimal AI Agent Setup - Knesset OData Explorer"
echo "================================================="

# Function to add to bashrc if not already present
add_to_bashrc() {
    local line="$1"
    if ! grep -Fxq "$line" ~/.bashrc 2>/dev/null; then
        echo "$line" >> ~/.bashrc
    fi
}

# Get absolute paths for reliability
CURRENT_DIR=$(pwd)
export PYTHONPATH="$CURRENT_DIR/src:$PYTHONPATH"
export LOG_LEVEL=DEBUG
export STREAMLIT_CACHE_DISABLED=1

echo "📝 Setting environment variables..."
add_to_bashrc "export PYTHONPATH=\"\$(pwd)/src:\$PYTHONPATH\""
add_to_bashrc "export LOG_LEVEL=DEBUG"
add_to_bashrc "export STREAMLIT_CACHE_DISABLED=1"

# Create virtual environment if needed
if [ ! -d ".venv" ]; then
    echo "🐍 Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
echo "⚡ Activating virtual environment..."
source .venv/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet

# Install essential development tools only
echo "🛠️  Installing essential dev tools..."
pip install black isort flake8 mypy pytest --quiet

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data logs backups
chmod 755 data logs backups

# Create minimal .env file
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cat > .env << 'EOF'
PYTHONPATH=./src
LOG_LEVEL=DEBUG
STREAMLIT_CACHE_DISABLED=1
DATABASE_PATH=./data/warehouse.duckdb
PARQUET_PATH=./data/parquet
EOF
fi

# Test critical imports only
echo "🔍 Testing critical imports..."
python3 -c "
import sys
sys.path.insert(0, '$CURRENT_DIR/src')
try:
    import streamlit, duckdb, pandas, plotly
    print('✅ Core imports successful')
    
    from config.settings import Settings
    from utils.logger_setup import setup_logging
    print('✅ Basic project imports successful')
    
except ImportError as e:
    print(f'❌ Import error: {e}')
    exit(1)
"

echo ""
echo "✅ Minimal setup completed successfully!"
echo ""
echo "Quick commands:"
echo "  streamlit run src/ui/data_refresh.py    - Start the application"
echo "  python -m backend.fetch_table --help    - CLI help"
echo "  ./scripts/quick-test.sh                 - Run full tests when ready"
echo ""
echo "🎯 Environment ready for AI development!"
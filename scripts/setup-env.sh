#!/bin/bash
# Quick environment setup for AI agents
# This script can be run multiple times safely

set -e

# Load environment variables if .env exists
if [ -f .env ]; then
    echo "📝 Loading environment variables from .env..."
    export $(grep -v '^#' .env | xargs)
fi

# Set critical environment variables
export PYTHONPATH="$(pwd)/src:$PYTHONPATH"
export LOG_LEVEL="${LOG_LEVEL:-DEBUG}"
export STREAMLIT_CACHE_DISABLED="${STREAMLIT_CACHE_DISABLED:-1}"

echo "✅ Environment configured:"
echo "   PYTHONPATH: $PYTHONPATH"
echo "   LOG_LEVEL: $LOG_LEVEL"
echo "   STREAMLIT_CACHE_DISABLED: $STREAMLIT_CACHE_DISABLED"

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    echo "⚡ Activating virtual environment..."
    source .venv/bin/activate
    echo "✅ Virtual environment activated"
else
    echo "⚠️  No virtual environment found. Run scripts/agent-setup.sh first."
fi

# Quick health check
echo "🔍 Quick health check..."
python3 -c "
import sys
sys.path.insert(0, './src')
try:
    import streamlit, duckdb, pandas
    print('✅ Core imports successful')
except ImportError as e:
    print(f'❌ Import error: {e}')
    print('💡 Run scripts/agent-setup.sh to install dependencies')
    exit(1)
"

echo "🎉 Environment ready for development!"
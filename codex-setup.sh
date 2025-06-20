#!/bin/bash
# Quick setup for Codex environment
pip install mypy flake8 pytest pytest-cov black isort
pip install -r requirements.txt
export PYTHONPATH="$(pwd)/src:$PYTHONPATH"
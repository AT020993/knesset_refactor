#!/usr/bin/env python3
import subprocess
import os

# Change to project directory
project_dir = "/Users/amir/Library/CloudStorage/OneDrive-Personal/Work/Research Assistant/knesset_refactor"
os.chdir(project_dir)

# Run streamlit with virtual environment
subprocess.run([
    f"{project_dir}/.venv/bin/python", 
    "-m", "streamlit", "run", 
    "src/ui/data_refresh.py", 
    "--server.port", "8502"
])
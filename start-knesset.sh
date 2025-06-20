#!/bin/bash
cd "/Users/amir/Library/CloudStorage/OneDrive-Personal/Work/Research Assistant/knesset_refactor"
source .venv/bin/activate
streamlit run src/ui/data_refresh.py --server.port 8502
#!/bin/bash
cd "$(dirname "$0")"
export PATH="$HOME/Library/Python/3.9/bin:$HOME/.local/bin:$PATH"
streamlit run app.py

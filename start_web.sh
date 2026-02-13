#!/bin/bash
cd "$(dirname "$0")"
export PATH="$HOME/Library/Python/3.9/bin:$HOME/.local/bin:$PATH"
uvicorn app_web:app --reload --host 0.0.0.0 --port 8501

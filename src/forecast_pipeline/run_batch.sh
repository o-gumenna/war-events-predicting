#!/bin/bash
# Run batch predictions (sync mode for cron execution)
#
# Usage:
#   ./run_batch.sh                      # Activate venv + run batch_predict.py
#   python batch_predict.py             # Direct Python execution

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BATCH_SCRIPT="${PROJECT_ROOT}/src/forecast_pipeline/batch_predict.py"

# Activate venv if it exists
if [ -f "${PROJECT_ROOT}/venv/bin/activate" ]; then
    source "${PROJECT_ROOT}/venv/bin/activate"
    echo "[$(date -u +'%Y-%m-%d %H:%M:%S')] ✓ Virtual environment activated"
fi

# Run batch prediction
cd "${PROJECT_ROOT}"
python "${BATCH_SCRIPT}"

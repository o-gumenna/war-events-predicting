#!/usr/bin/env bash
# Simple wrapper to run the telegram fetch test on the server.
# Usage: ./scripts/run_telegram_test.sh
set -euo pipefail
cd "$(dirname "$(dirname "$0")")" || exit 1

# Create venv if missing
if [ ! -d "venv" ]; then
  python3 -m venv venv
fi

# Activate
# shellcheck source=/dev/null
source venv/bin/activate

pip install --upgrade pip
# Install requirements if file exists
if [ -f requirements.txt ]; then
  pip install -r requirements.txt || true
fi
# Install minimal deps needed for the test (safe even if already installed)
pip install pandas requests python-dotenv || true

echo "Running telegram fetch test..."
python scripts/test_telegram_fetch.py

rc=$?
if [ $rc -eq 0 ]; then
  echo "TEST PASSED"
else
  echo "TEST FAILED (exit code $rc)"
fi
exit $rc

#!/usr/bin/env bash
# Daily scrape runner script.
# Usage: ./scripts/run_daily.sh [search_url]
#
# Cron example (runs at 06:00 every day, logs to file):
#   0 6 * * * /path/to/portal-inmobiliario-scraper/scripts/run_daily.sh >> /path/to/data/logs/cron.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Activate virtual environment if it exists
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

DATE=$(date +%Y-%m-%d)
LOG_FILE="data/logs/run_${DATE}.log"
mkdir -p data/logs

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Starting daily scrape..." | tee -a "$LOG_FILE"

# Use search URLs file if it exists, otherwise default URL
if [ -f "data/search_urls.txt" ]; then
    ARGS="--search-urls-file data/search_urls.txt"
elif [ -n "${1:-}" ]; then
    ARGS="--search-url $1"
else
    # Sensible default for testing
    ARGS='--search-url https://www.portalinmobiliario.com/venta/departamento/las-condes-metropolitana'
fi

python -m src.cli scrape-all \
    $ARGS \
    --max-pages "${MAX_PAGES:-20}" \
    --max-listings "${MAX_LISTINGS:-500}" \
    --headless \
    --log-level INFO \
    2>&1 | tee -a "$LOG_FILE"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Scrape complete." | tee -a "$LOG_FILE"

# Export
python -m src.cli export --format csv --format json 2>&1 | tee -a "$LOG_FILE"

# Detect new listings
python -m src.cli detect-new 2>&1 | tee -a "$LOG_FILE"

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Done." | tee -a "$LOG_FILE"

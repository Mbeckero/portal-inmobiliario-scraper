#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CRON_CMD="/bin/bash \"$PROJECT_DIR/scripts/run_daily_terrenos.sh\" >> \"$PROJECT_DIR/data/logs/cron.log\" 2>&1"
BEGIN="# BEGIN portal_inmobiliario_daily_terrenos"
END="# END portal_inmobiliario_daily_terrenos"
TMP_FILE="$(mktemp)"
trap 'rm -f "$TMP_FILE"' EXIT

(crontab -l 2>/dev/null || true) | awk -v b="$BEGIN" -v e="$END" '
  $0==b {skip=1; next}
  $0==e {skip=0; next}
  !skip {print}
' > "$TMP_FILE"

{
  echo "$BEGIN"
  echo "TZ=America/Santiago"
  echo "0 9,18 * * * $CRON_CMD"
  echo "$END"
} >> "$TMP_FILE"

crontab "$TMP_FILE"
echo "Cron updated for moved project path: $PROJECT_DIR"

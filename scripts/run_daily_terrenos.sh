#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DOCS_DIR="${HOME}/Documents"
LOG_DIR="$PROJECT_DIR/data/logs"
SEARCH_FILE="$PROJECT_DIR/configs/search_urls.txt"
MIN_AREA_M2="${MIN_AREA_M2:-3000}"
MAX_PAGES="${MAX_PAGES:-3}"
MAX_LISTINGS="${MAX_LISTINGS:-250}"

mkdir -p "$LOG_DIR" "$DOCS_DIR"

# Load env vars from .env safely (supports values with spaces).
if [[ -f "$PROJECT_DIR/.env" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" == \#* || "$line" != *=* ]] && continue
    key="${line%%=*}"
    value="${line#*=}"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    export "$key=$value"
  done < "$PROJECT_DIR/.env"
fi

EMAIL_ENABLED="${EMAIL_ENABLED:-false}"
SMTP_HOST="${SMTP_HOST:-smtp.gmail.com}"
SMTP_PORT="${SMTP_PORT:-587}"
SMTP_USER="${SMTP_USER:-}"
SMTP_PASS="${SMTP_PASS:-}"
EMAIL_FROM="${EMAIL_FROM:-$SMTP_USER}"
EMAIL_TO="${EMAIL_TO:-$SMTP_USER}"
SMTP_STARTTLS="${SMTP_STARTTLS:-true}"

if [[ -x "$PROJECT_DIR/.venv/bin/python" ]]; then
  PY="$PROJECT_DIR/.venv/bin/python"
else
  PY="python3"
fi

cd "$PROJECT_DIR"
RUN_LOG="$LOG_DIR/automation_daily_terrenos.log"

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting scheduled terreno scrape"

  SCRAPE_OUTPUT="$("$PY" -m src.cli scrape-all \
    --search-urls-file "$SEARCH_FILE" \
    --operation-type sale \
    --max-pages "$MAX_PAGES" \
    --max-listings "$MAX_LISTINGS" \
    --headless \
    --min-delay 0.8 \
    --max-delay 1.5 \
    --save-raw \
    --log-level INFO)"

  echo "$SCRAPE_OUTPUT"

  RUN_ID="$(echo "$SCRAPE_OUTPUT" | awk '/Run ID:/ {print $3}' | tail -n1)"
  if [[ -z "$RUN_ID" ]]; then
    echo "Failed to parse Run ID"
    exit 1
  fi

  "$PY" -m src.cli detect-new
  "$PY" -m src.cli export --format csv --format json

  PROJECT_DIR="$PROJECT_DIR" DOCS_DIR="$DOCS_DIR" RUN_ID="$RUN_ID" MIN_AREA_M2="$MIN_AREA_M2" \
  EMAIL_ENABLED="$EMAIL_ENABLED" SMTP_HOST="$SMTP_HOST" SMTP_PORT="$SMTP_PORT" SMTP_USER="$SMTP_USER" SMTP_PASS="$SMTP_PASS" EMAIL_FROM="$EMAIL_FROM" EMAIL_TO="$EMAIL_TO" SMTP_STARTTLS="$SMTP_STARTTLS" "$PY" - << 'PY'
import csv
import json
import os
import sqlite3
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

project_dir = Path(os.environ['PROJECT_DIR'])
docs_dir = Path(os.environ['DOCS_DIR'])
run_id = os.environ['RUN_ID']
min_area = float(os.environ['MIN_AREA_M2'])

email_enabled = os.getenv('EMAIL_ENABLED', 'false').lower() in {'1', 'true', 'yes', 'on'}
smtp_host = os.getenv('SMTP_HOST', '')
smtp_port = int(os.getenv('SMTP_PORT', '587'))
smtp_user = os.getenv('SMTP_USER', '')
smtp_pass = os.getenv('SMTP_PASS', '')
email_from = os.getenv('EMAIL_FROM', '')
email_to = os.getenv('EMAIL_TO', '')
smtp_starttls = os.getenv('SMTP_STARTTLS', 'true').lower() in {'1', 'true', 'yes', 'on'}

# Resolve DB path from DATABASE_URL (sqlite+aiosqlite:///data/scraper.db default)
database_url = os.getenv('DATABASE_URL', 'sqlite+aiosqlite:///data/scraper.db')
if ':///' in database_url:
    db_rel = database_url.split(':///', 1)[1]
else:
    db_rel = database_url

db_path = (project_dir / db_rel).resolve()

con = sqlite3.connect(db_path)
con.row_factory = sqlite3.Row

run = con.execute(
    "SELECT started_at, finished_at FROM scraper_runs WHERE run_id = ?",
    (run_id,),
).fetchone()
if not run:
    raise RuntimeError(f'Run not found in DB: {run_id}')
started_at = run['started_at']
finished_at = run['finished_at'] or datetime.now(timezone.utc).isoformat(sep=' ')

rows = con.execute(
    """
    SELECT l.*
    FROM listing_runs lr
    JOIN listings l ON l.listing_id = lr.listing_id
    WHERE lr.run_id = ?
      AND l.publication_type = 'sale'
      AND (
            LOWER(COALESCE(l.search_url, '')) LIKE '%/venta/terreno/lo-barnechea%'
         OR LOWER(COALESCE(l.search_url, '')) LIKE '%/venta/terreno/las-condes%'
         OR LOWER(COALESCE(l.search_url, '')) LIKE '%/venta/terreno/la-reina%'
      )
      AND COALESCE(l.land_area_m2, l.total_area_m2, l.usable_area_m2, 0) >= ?
      AND l.first_seen_at >= ?
      AND l.first_seen_at <= ?
    ORDER BY COALESCE(l.land_area_m2, l.total_area_m2, l.usable_area_m2, 0) DESC
    """,
    (run_id, min_area, started_at, finished_at),
).fetchall()
records = [dict(r) for r in rows]
con.close()

out_dir = project_dir / 'data/exports' / datetime.now(timezone.utc).strftime('%Y-%m-%d')
out_dir.mkdir(parents=True, exist_ok=True)
json_path = out_dir / f'automation_terrenos_area_gte_{int(min_area)}_{run_id}.json'
csv_path = out_dir / f'automation_terrenos_area_gte_{int(min_area)}_{run_id}.csv'
json_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding='utf-8')
if records:
    with csv_path.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(records[0].keys()))
        w.writeheader()
        w.writerows(records)
else:
    csv_path.write_text('', encoding='utf-8')

generated_at = datetime.now(timezone.utc)
timestamp = generated_at.strftime('%Y%m%d_%H%M%S_UTC')
txt_latest = docs_dir / 'portal_inmobiliario_terrenos_latest.txt'
txt_versioned = docs_dir / f'portal_inmobiliario_terrenos_{timestamp}.txt'

lines = [
    'Portal Inmobiliario Terrenos Report',
    f'Run ID: {run_id}',
    'Criteria: Terrenos en venta en Lo Barnechea, Las Condes, La Reina',
    'Only new listings: yes',
    f'Area filter: >= {int(min_area)} m2',
    f'Total matches: {len(records)}',
    f'Generated at (UTC): {generated_at.isoformat()}',
    f'JSON export: {json_path}',
    f'CSV export: {csv_path}',
    '',
    'Listings:',
]
if not records:
    lines.append('Nada nuevo')
else:
    for i, r in enumerate(records, start=1):
        area = r.get('land_area_m2') or r.get('total_area_m2') or r.get('usable_area_m2') or ''
        price = r.get('price_uf') or r.get('price_clp') or r.get('price') or ''
        currency = r.get('currency') or '-'
        lines.extend([
            f'{i}. {r.get("title") or "-"}',
            f'   Listing ID: {r.get("listing_id") or "-"}',
            f'   Area m2: {area}',
            f'   Price: {price} {currency}',
            f'   Commune/Region: {r.get("commune") or "-"} / {r.get("region") or "-"}',
            f'   URL: {r.get("listing_url") or "-"}',
            '',
        ])

content = '\n'.join(lines).strip() + '\n'
txt_latest.write_text(content, encoding='utf-8')
txt_versioned.write_text(content, encoding='utf-8')

print(f'Filtered export rows: {len(records)}')
print(f'TXT latest: {txt_latest}')
print(f'TXT versioned: {txt_versioned}')

if email_enabled:
    missing = [k for k, v in {
        'SMTP_HOST': smtp_host,
        'SMTP_USER': smtp_user,
        'SMTP_PASS': smtp_pass,
        'EMAIL_FROM': email_from,
        'EMAIL_TO': email_to,
    }.items() if not v]
    if missing:
        print(f'Email skipped: missing config {", ".join(missing)}')
    else:
        msg = EmailMessage()
        msg['Subject'] = f'[Portal Inmobiliario] Terrenos nuevos >= {int(min_area)} m2 | {len(records)} resultados'
        msg['From'] = email_from
        msg['To'] = email_to
        msg.set_content(content)
        msg.add_attachment(content.encode('utf-8'), maintype='text', subtype='plain', filename=txt_versioned.name)
        try:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as server:
                if smtp_starttls:
                    server.starttls()
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
            print(f'Email sent to: {email_to}')
        except Exception as exc:
            print(f'Email error: {exc}')
else:
    print('Email disabled (EMAIL_ENABLED=false)')
PY

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Scheduled terreno scrape finished"
} >> "$RUN_LOG" 2>&1

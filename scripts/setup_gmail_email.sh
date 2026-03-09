#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_DIR/.env"
EXAMPLE_FILE="$PROJECT_DIR/.env.example"

[[ -f "$ENV_FILE" ]] || cp "$EXAMPLE_FILE" "$ENV_FILE"

read -rp "Gmail address (sender): " GMAIL
read -rsp "Gmail App Password (16 chars): " APP_PASS
echo
read -rp "Recipient email (Enter = same sender): " EMAIL_TO
[[ -z "$EMAIL_TO" ]] && EMAIL_TO="$GMAIL"
APP_PASS_CLEAN="${APP_PASS// /}"

ENV_FILE="$ENV_FILE" GMAIL="$GMAIL" EMAIL_TO="$EMAIL_TO" APP_PASS_CLEAN="$APP_PASS_CLEAN" python3 - << 'PY'
import os
from pathlib import Path

env_file = Path(os.environ['ENV_FILE'])
updates = {
    'EMAIL_ENABLED': 'true',
    'SMTP_HOST': 'smtp.gmail.com',
    'SMTP_PORT': '587',
    'SMTP_USER': os.environ['GMAIL'],
    'SMTP_PASS': os.environ['APP_PASS_CLEAN'],
    'EMAIL_FROM': os.environ['GMAIL'],
    'EMAIL_TO': os.environ['EMAIL_TO'],
    'SMTP_STARTTLS': 'true',
}

text = env_file.read_text(encoding='utf-8') if env_file.exists() else ''
lines = text.splitlines()
out, seen = [], set()
for line in lines:
    if not line or line.lstrip().startswith('#') or '=' not in line:
        out.append(line)
        continue
    k, _, _ = line.partition('=')
    k = k.strip()
    if k in updates:
        out.append(f"{k}={updates[k]}")
        seen.add(k)
    else:
        out.append(line)
for k, v in updates.items():
    if k not in seen:
        out.append(f"{k}={v}")

env_file.write_text('\n'.join(out).rstrip() + '\n', encoding='utf-8')
print(f'Updated {env_file}')
PY

echo "Gmail config saved. Test with: bash '$PROJECT_DIR/scripts/run_daily_terrenos.sh'"

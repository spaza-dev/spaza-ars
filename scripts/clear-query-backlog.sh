#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

DB="/var/lib/ars/ars.db"
WORK_DIR="/var/lib/ars/work"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

if [[ ! -f "${DB}" ]]; then
  echo "database not found: ${DB}" >&2
  exit 1
fi

echo "Stopping ARS..."
systemctl stop ars.service

echo "Backing up database..."
sqlite3 "${DB}" ".backup '/var/lib/ars/ars.db.bak-${STAMP}'"

echo "Clearing query backlog..."
sqlite3 "${DB}" <<SQL
BEGIN;
UPDATE jobs
   SET status = 'failed',
       error = 'backlog cleared ${STAMP}',
       updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
 WHERE kind = 'query' AND status IN ('pending', 'running');
COMMIT;
SQL

if [[ -d "${WORK_DIR}" ]]; then
  echo "Removing stale chunk files..."
  find "${WORK_DIR}" -type f -name 'chunk-*.wav' -delete
fi

echo "Starting ARS..."
systemctl start ars.service

echo "Remaining query jobs:"
sqlite3 "${DB}" \
  "SELECT status, COUNT(*) FROM jobs WHERE kind = 'query' GROUP BY status;"

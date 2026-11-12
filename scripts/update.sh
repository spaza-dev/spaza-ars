#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

APP_DIR="/opt/ars"
STATE_DIR="/var/lib/ars"
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_DIR="/var/backups/ars/${STAMP}"

mkdir -p "${BACKUP_DIR}"
cp -a "${APP_DIR}/ars" "${BACKUP_DIR}/ars" 2>/dev/null || true
cp -a "${STATE_DIR}/ars.db" "${BACKUP_DIR}/ars.db" 2>/dev/null || true

echo "Building new release..."
(cd "${SOURCE_DIR}" && cargo build --release)
install -m 0755 "${SOURCE_DIR}/target/release/ars" "${APP_DIR}/ars.new"

echo "Restarting services..."
systemctl stop ars.service
mv "${APP_DIR}/ars.new" "${APP_DIR}/ars"
systemctl start olaf-rest.service
systemctl start ars.service

sleep 3
if curl -fsS http://127.0.0.1:8080/healthz >/dev/null; then
  echo "Update succeeded."
  exit 0
fi

echo "Health check failed; rolling back." >&2
systemctl stop ars.service
cp -a "${BACKUP_DIR}/ars" "${APP_DIR}/ars" 2>/dev/null || true
systemctl start ars.service
exit 1

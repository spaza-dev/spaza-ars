#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

APP_USER="${APP_USER:-ars}"
APP_DIR="/opt/ars"
ETC_DIR="/etc/ars"
STATE_DIR="/var/lib/ars"
LOG_DIR="/var/log/ars"
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Installing dependencies..."
apt-get update
apt-get install -y ffmpeg curl ca-certificates sqlite3 python3 python3-pip git build-essential
if ! command -v yt-dlp >/dev/null 2>&1; then
  python3 -m pip install --break-system-packages -U yt-dlp || pip3 install -U yt-dlp
fi

if ! id "${APP_USER}" >/dev/null 2>&1; then
  useradd --system --create-home --home-dir "${STATE_DIR}" --shell /usr/sbin/nologin "${APP_USER}"
fi
install -d -o "${APP_USER}" -g "${APP_USER}" "${APP_DIR}" "${ETC_DIR}" "${STATE_DIR}" "${LOG_DIR}"
install -d -o "${APP_USER}" -g "${APP_USER}" "${STATE_DIR}/.olaf" "${STATE_DIR}/work" "${STATE_DIR}/olaf-db"

if ! command -v olaf >/dev/null 2>&1; then
  echo "Olaf is not installed. Building Olaf from source..."
  if ! command -v zig >/dev/null 2>&1; then
    echo "zig is required to build Olaf. Install zig or set OLAF_BIN to a prebuilt binary." >&2
    exit 1
  fi
  tmp="$(mktemp -d)"
  git clone --depth 1 https://github.com/JorenSix/Olaf "${tmp}/Olaf"
  (cd "${tmp}/Olaf" && make && make install)
  rm -rf "${tmp}"
fi

if command -v cargo >/dev/null 2>&1; then
  echo "Building ars..."
  (cd "${SOURCE_DIR}" && cargo build --release)
  install -m 0755 "${SOURCE_DIR}/target/release/ars" "${APP_DIR}/ars"
else
  echo "cargo not found; place a prebuilt ars at ${APP_DIR}/ars" >&2
  exit 1
fi

if [[ ! -f "${ETC_DIR}/config.toml" ]]; then
  install -m 0640 -o "${APP_USER}" -g "${APP_USER}" "${SOURCE_DIR}/config.example.toml" "${ETC_DIR}/config.toml"
fi

if [[ ! -f "${ETC_DIR}/ars.env" ]]; then
  token="$(openssl rand -hex 32 2>/dev/null || head -c 32 /dev/urandom | xxd -p -c 64)"
  secret="$(openssl rand -hex 32 2>/dev/null || head -c 32 /dev/urandom | xxd -p -c 64)"
  cat > "${ETC_DIR}/ars.env" <<EOF
ARS_API_TOKEN=${token}
ARS_WEBHOOK_SIGNING_SECRET=${secret}
RUST_LOG=info
EOF
  chown "${APP_USER}:${APP_USER}" "${ETC_DIR}/ars.env"
  chmod 0600 "${ETC_DIR}/ars.env"
fi

cat > "${STATE_DIR}/.olaf/olaf_config.json" <<EOF
{
  "db_folder": "${STATE_DIR}/olaf-db",
  "cache_folder": "${STATE_DIR}/cache",
  "rest_append_db_path_with_addr": false,
  "rest_max_body_mb": 512,
  "rest_workers": 4
}
EOF
chown -R "${APP_USER}:${APP_USER}" "${STATE_DIR}"

install -m 0644 "${SOURCE_DIR}/olaf-rest.service" /etc/systemd/system/olaf-rest.service
install -m 0644 "${SOURCE_DIR}/ars.service" /etc/systemd/system/ars.service

systemctl daemon-reload
systemctl enable --now olaf-rest.service
systemctl enable --now ars.service

sleep 2
curl -fsS http://127.0.0.1:8080/healthz >/dev/null
curl -fsS http://127.0.0.1:8920/api/healthz >/dev/null || true

echo "Installed. API token:"
grep ARS_API_TOKEN "${ETC_DIR}/ars.env"
echo "Configure Cloudflare Tunnel to point to http://127.0.0.1:8080."

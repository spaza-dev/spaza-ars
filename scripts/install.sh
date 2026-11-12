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

export DEBIAN_FRONTEND=noninteractive

echo "Installing dependencies..."
apt-get update
apt-get install -y ffmpeg curl ca-certificates sqlite3 python3 python3-pip git build-essential openssl xz-utils

if ! command -v yt-dlp >/dev/null 2>&1; then
  python3 -m pip install --break-system-packages -U yt-dlp 2>/dev/null || pip3 install -U yt-dlp
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "Installing Rust toolchain..."
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
  export PATH="${HOME}/.cargo/bin:${PATH}"
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is still unavailable after rustup install" >&2
  exit 1
fi

if ! command -v zig >/dev/null 2>&1 && ! command -v olaf >/dev/null 2>&1; then
  echo "Installing Zig to build Olaf..."
  arch="$(uname -m)"
  case "${arch}" in
    x86_64|amd64) zig_arch="x86_64-linux" ;;
    aarch64|arm64) zig_arch="aarch64-linux" ;;
    *) echo "Unsupported architecture for Zig: ${arch}" >&2; exit 1 ;;
  esac
  curl -fsSL https://ziglang.org/download/index.json -o /tmp/zig-index.json
  read -r zig_version zig_url < <(python3 - "${zig_arch}" <<'PY'
import json, sys
arch = sys.argv[1]
with open('/tmp/zig-index.json') as fh:
    data = json.load(fh)
versions = [key for key in data if key != 'master']
versions.sort(key=lambda value: tuple(int(part) for part in value.split('.')))
version = versions[-1]
print(version, data[version][arch]['tarball'])
PY
)
  curl -fsSL "${zig_url}" -o /tmp/zig.tar.xz
  rm -rf "/opt/zig-${zig_version}"
  tar -C /opt -xf /tmp/zig.tar.xz
  zig_dir="$(find /opt -maxdepth 1 -type d -name "zig-*${zig_version}*" | head -n1)"
  if [[ -z "${zig_dir}" ]]; then
    echo "Could not locate extracted Zig directory" >&2
    exit 1
  fi
  ln -sf "${zig_dir}/zig" /usr/local/bin/zig
fi

if ! id "${APP_USER}" >/dev/null 2>&1; then
  useradd --system --create-home --home-dir "${STATE_DIR}" --shell /usr/sbin/nologin "${APP_USER}"
fi
install -d -o "${APP_USER}" -g "${APP_USER}" "${APP_DIR}" "${ETC_DIR}" "${STATE_DIR}" "${LOG_DIR}"
install -d -o "${APP_USER}" -g "${APP_USER}" "${STATE_DIR}/.olaf" "${STATE_DIR}/work" "${STATE_DIR}/cache" "${STATE_DIR}/olaf-db"

if ! command -v olaf >/dev/null 2>&1; then
  echo "Building Olaf from source..."
  tmp="$(mktemp -d)"
  git clone --depth 1 https://github.com/JorenSix/Olaf "${tmp}/Olaf"
  (cd "${tmp}/Olaf" && make && make install)
  rm -rf "${tmp}"
fi

echo "Building ARS..."
(cd "${SOURCE_DIR}" && cargo build --release)
install -m 0755 "${SOURCE_DIR}/target/release/ars" "${APP_DIR}/ars"

if [[ ! -f "${ETC_DIR}/config.toml" ]]; then
  install -m 0640 -o "${APP_USER}" -g "${APP_USER}" "${SOURCE_DIR}/config.example.toml" "${ETC_DIR}/config.toml"
fi

if [[ ! -f "${ETC_DIR}/ars.env" ]]; then
  token="$(openssl rand -hex 32 2>/dev/null || head -c 32 /dev/urandom | od -An -N32 -tx1 | tr -d ' \n')"
  secret="$(openssl rand -hex 32 2>/dev/null || head -c 32 /dev/urandom | od -An -N32 -tx1 | tr -d ' \n')"
  cat > "${ETC_DIR}/ars.env" <<ENV
ARS_API_TOKEN=${token}
ARS_WEBHOOK_SIGNING_SECRET=${secret}
RUST_LOG=info
ENV
  chown "${APP_USER}:${APP_USER}" "${ETC_DIR}/ars.env"
  chmod 0600 "${ETC_DIR}/ars.env"
fi

cat > "${STATE_DIR}/.olaf/olaf_config.json" <<JSON
{
  "db_folder": "${STATE_DIR}/olaf-db",
  "cache_folder": "${STATE_DIR}/cache",
  "rest_append_db_path_with_addr": false,
  "rest_max_body_mb": 512,
  "rest_workers": 4
}
JSON
chown -R "${APP_USER}:${APP_USER}" "${STATE_DIR}"

install -m 0644 "${SOURCE_DIR}/olaf-rest.service" /etc/systemd/system/olaf-rest.service
install -m 0644 "${SOURCE_DIR}/ars.service" /etc/systemd/system/ars.service

systemctl daemon-reload
systemctl enable --now olaf-rest.service
systemctl enable --now ars.service

for _ in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! curl -fsS http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
  echo "ARS health check failed. Recent logs:" >&2
  journalctl -u ars.service -n 50 --no-pager >&2 || true
  exit 1
fi
curl -fsS http://127.0.0.1:8920/api/healthz >/dev/null 2>&1 || true

echo "Installed. API token:"
grep ARS_API_TOKEN "${ETC_DIR}/ars.env"
echo "Configure Cloudflare Tunnel to point to http://127.0.0.1:8080."

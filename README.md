# Airplay Recognition Service (ARS)

Rust service that exposes a provider-compatible REST API backed by an Olaf
REST sidecar. It manages streams, catalog indexing, detections, webhooks and
optional evidence clips.

## Architecture

- Axum HTTP API with bearer auth and Swagger UI.
- SQLite for streams, callbacks, jobs, detections, aggregates, webhooks and evidence.
- Olaf REST sidecar on `127.0.0.1:8920` for fingerprint store/query.
- ffmpeg for stream capture and clip extraction.
- yt-dlp for YouTube/Twitch stream resolution.
- S3-compatible evidence upload using SigV4 PUT.
- Cloudflare Tunnel provides public TLS; no Caddy is required.

## Run locally

```bash
cp config.example.toml config.toml
cargo run -- --config config.toml
```

Start Olaf sidecar separately:

```bash
olaf rest serve --listen 127.0.0.1:8920
```

## Install

```bash
sudo bash scripts/install.sh
```

The installer installs ffmpeg, yt-dlp, Rust, Zig (when Olaf needs to be built),
and Olaf if missing. It creates `/opt/ars`, `/etc/ars` and
`/var/lib/ars`, installs `olaf-rest.service` and
`ars.service`, and generates the API token.

## Update

```bash
sudo bash scripts/update.sh
```

## API

- `GET /healthz`
- `GET /readyz`
- `GET /version`
- `GET /docs`
- `GET /openapi.json`
- `/v1/streams`
- `/v1/callbacks`
- `/v1/catalog/items`
- `/v1/query`
- `/v1/jobs/{id}`
- `/v1/detections`
- `/v1/detections/aggregate`

All `/v1/*` endpoints require `Authorization: Bearer <ARS_API_TOKEN>`.

## Stream fields

Streams support `catalogs` as a JSON array, `evidence_enabled`, per-stream
aggregation window, chunk duration and overlap.

## Webhooks

Outbound callbacks support:

- `Authorization: Bearer <callback token>`
- `X-Olaf-Signature: sha256=<hmac>`

Both can be enabled per callback (`auth_type = "both"`).

## Evidence

When `evidence_enabled` is set on a stream, the service uploads the matched
audio chunk to the configured S3 bucket and includes the evidence object in the
webhook payload.

## Limitations

- Olaf delete support depends on the pinned Olaf version. Catalog deletion
  removes the local item only in this build.
- Chunk overlap is stored as configuration but capture currently uses ffmpeg
  segment boundaries.
- Installer requires Zig to build Olaf if `olaf` is not already installed.

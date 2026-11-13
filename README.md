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



## API documentation

- Swagger UI: `https://airplay-ars.spaza.co.ke/docs`
- OpenAPI JSON: `https://airplay-ars.spaza.co.ke/openapi.json`

The OpenAPI document is generated from the route annotations and uses
`ARS_PUBLIC_BASE_URL` / `server.public_base_url` as its server URL.

All `/v1/*` routes use bearer authentication. Click **Authorize** in Swagger
UI and paste the ARS API token.

## Console integration

Console environment:

```env
RECOGNITION_PROVIDER=olaf
OLAF_API_BASE=https://airplay-ars.spaza.co.ke
OLAF_API_TOKEN=<ARS_API_TOKEN>
OLAF_CALLBACK_TOKEN=<random callback bearer token>
OLAF_WEBHOOK_SIGNING_SECRET=<ARS_WEBHOOK_SIGNING_SECRET>
```

After deploying the console, open **Settings → General → Sync callback URL**
so ARS registers the console callback URL and callback credentials.

## Catalog indexing

`POST /v1/catalog/items/json` accepts a stored object reference and is
idempotent on `catalog + external_id`:

```json
{
  "catalog": "private",
  "external_id": "12345",
  "name": "Song A",
  "type": "song",
  "object": {
    "bucket": "airplay-audd-catalog",
    "key": "catalog/123/song.mp3"
  },
  "force": false
}
```

- `200` when the item is already indexed and `force` is false.
- `202` when an index job is queued or already running.
- `force=true` re-indexes an existing item.

Index jobs read the object directly from S3/R2 using the `[evidence]`
credentials, enforce a timeout and maximum size, then forward the audio to
Olaf `/api/store`. When `ARS_CATALOG_S3_BUCKET` / `[index] catalog_bucket` is
set, the request bucket must match it; otherwise the request bucket is used,
falling back to `evidence.bucket` when the request omits a bucket. Set
`ARS_CATALOG_S3_PREFIX` / `[index] catalog_prefix` to restrict keys to one
prefix. Temporary files are removed after success or failure. Index jobs are
single-attempt.

The same idempotency rules apply to the multipart `POST /v1/catalog/items`
endpoint.

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

# Airplay Recognition Service (ARS) Implementation Plan

## 1. Goals

Build a standalone Rust service that:

- Uses an Olaf REST sidecar for fingerprint store/query.
- Manages streams, callbacks, catalog items and jobs in SQLite.
- Captures multiple stream types with ffmpeg and yt-dlp.
- Aggregates matches and sends authenticated webhooks.
- Optionally clips evidence audio and uploads it to S3.
- Exposes a richer REST API with an adapter-compatible subset.
- Runs under systemd and is published via Cloudflare Tunnel.

## 2. Components

- `api`: Axum routes, auth, Swagger UI.
- `db`: SQLite schema and repository functions.
- `engine`: Olaf REST client.
- `capture`: stream supervisor, ffmpeg/yt-dlp integration.
- `workers`: query, index, aggregation, cleanup, webhook workers.
- `evidence`: SigV4 S3 PUT upload.
- `webhook`: outbound delivery with bearer/HMAC auth and retry.

## 3. Data model

Tables: `streams`, `callbacks`, `catalog_items`, `jobs`, `detections`,
`detection_aggregates`, `webhook_outbox`, `evidence`, `settings`,
`schema_migrations`.

Streams store `catalogs` as a JSON array and support per-stream aggregation
window and evidence settings.

## 4. API

Provider-compatible subset:

- Streams CRUD/start/stop
- Callback registration
- Catalog item indexing
- Query
- Jobs
- Detections/aggregates
- Health/version/docs

## 5. Stream capture

- Direct URLs: ffmpeg.
- YouTube/Twitch: yt-dlp resolves the live URL, then ffmpeg.
- ffmpeg writes time-stamped WAV chunks.
- Completed chunks enqueue query jobs.
- Supervisor restarts failed captures with backoff.
- `max_streams` bounds concurrency.

## 6. Query and aggregation

- Query worker calls Olaf `/api/query`.
- Matches resolve to catalog items by Olaf identifier.
- Stream `catalogs` filters which catalog items are accepted.
- Detections are inserted, then aggregated per stream/catalog item after the
  aggregation window.
- Aggregate creation enqueues webhooks and deletes detections immediately.
- Aggregates are retained for 24 hours.

## 7. Indexing

- Multipart file or URL.
- Catalog item created locally with a stable fingerprint identifier.
- Single index worker calls Olaf `/api/store`.
- Status is exposed via catalog item and job endpoints.

## 8. Evidence

- Enabled per stream.
- Matched audio chunk is uploaded to S3 with SigV4.
- Evidence metadata is included in the webhook.
- S3 credentials are owned by the service.

## 9. Webhooks

- Transactional outbox.
- Retries with exponential backoff.
- Bearer token and/or HMAC signature.
- Idempotency key per aggregate.

## 10. Operations

- systemd units for `olaf-rest` and `ars`.
- Installer installs ffmpeg, yt-dlp, Olaf and the service.
- Updater builds, backs up, restarts and rolls back on health failure.
- Cloudflare Tunnel points to `127.0.0.1:8080`.

## 11. Phases

1. Schema, config, auth, health, docs.
2. Olaf engine, index/query workers.
3. Stream supervisor and capture.
4. Detections, aggregation, webhooks.
5. Evidence upload.
6. Installer/updater/systemd.
7. Console provider adapter integration.

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS callbacks (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  auth_type TEXT NOT NULL DEFAULT 'bearer',
  token TEXT,
  signing_secret TEXT,
  event_types TEXT NOT NULL DEFAULT '["matches.aggregated","stream.status"]',
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS streams (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  source_type TEXT NOT NULL DEFAULT 'radio',
  external_id TEXT,
  catalogs TEXT NOT NULL DEFAULT '[]',
  enabled INTEGER NOT NULL DEFAULT 1,
  evidence_enabled INTEGER NOT NULL DEFAULT 0,
  evidence_pre_seconds INTEGER NOT NULL DEFAULT 10,
  evidence_post_seconds INTEGER NOT NULL DEFAULT 20,
  callback_id TEXT,
  chunk_seconds INTEGER NOT NULL DEFAULT 10,
  overlap_seconds INTEGER NOT NULL DEFAULT 2,
  aggregation_window_seconds INTEGER,
  status TEXT NOT NULL DEFAULT 'stopped',
  last_error TEXT,
  last_audio_at TEXT,
  last_match_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_streams_enabled ON streams(enabled);

CREATE TABLE IF NOT EXISTS catalog_items (
  id TEXT PRIMARY KEY,
  catalog TEXT NOT NULL DEFAULT 'private',
  external_id TEXT,
  name TEXT NOT NULL,
  type TEXT NOT NULL DEFAULT 'song',
  metadata_json TEXT NOT NULL DEFAULT '{}',
  fingerprint_id TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  indexed_at TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_catalog_items_catalog ON catalog_items(catalog);
CREATE INDEX IF NOT EXISTS idx_catalog_items_fingerprint ON catalog_items(fingerprint_id);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  priority INTEGER NOT NULL DEFAULT 0,
  payload_json TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 10,
  next_attempt_at TEXT,
  locked_at TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_due ON jobs(status, next_attempt_at, created_at);

CREATE TABLE IF NOT EXISTS detections (
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL,
  catalog_item_id TEXT NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  confidence REAL,
  match_count INTEGER,
  evidence_id TEXT,
  aggregated_at TEXT,
  raw_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_detections_stream ON detections(stream_id, created_at);
CREATE INDEX IF NOT EXISTS idx_detections_pending ON detections(aggregated_at);

CREATE TABLE IF NOT EXISTS detection_aggregates (
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL,
  window_start TEXT NOT NULL,
  window_end TEXT NOT NULL,
  match_count INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_aggregates_created ON detection_aggregates(created_at);

CREATE TABLE IF NOT EXISTS webhook_outbox (
  id TEXT PRIMARY KEY,
  callback_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL,
  sent_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_due ON webhook_outbox(status, next_attempt_at);

CREATE TABLE IF NOT EXISTS evidence (
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL,
  detection_id TEXT,
  storage TEXT NOT NULL DEFAULT 's3',
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  content_type TEXT NOT NULL,
  size_bytes INTEGER,
  duration_seconds REAL,
  status TEXT NOT NULL DEFAULT 'pending',
  error TEXT,
  uploaded_at TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_migrations(version, applied_at)
VALUES (1, datetime('now'));

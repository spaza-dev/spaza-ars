ALTER TABLE streams ADD COLUMN callback_mode TEXT NOT NULL DEFAULT 'after';

CREATE TABLE IF NOT EXISTS stream_match_state (
  stream_id TEXT NOT NULL,
  catalog_item_id TEXT NOT NULL,
  started_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  callback_sent_at TEXT,
  PRIMARY KEY (stream_id, catalog_item_id)
);

CREATE INDEX IF NOT EXISTS idx_stream_match_state_seen
  ON stream_match_state(last_seen_at);
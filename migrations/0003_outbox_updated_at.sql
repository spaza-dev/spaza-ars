ALTER TABLE webhook_outbox ADD COLUMN updated_at TEXT;

UPDATE webhook_outbox SET updated_at = created_at WHERE updated_at IS NULL;

ALTER TABLE catalog_items ADD COLUMN last_job_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_items_catalog_external
  ON catalog_items(catalog, external_id)
  WHERE external_id IS NOT NULL;

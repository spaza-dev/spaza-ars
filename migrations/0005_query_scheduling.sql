CREATE INDEX IF NOT EXISTS idx_jobs_query_stream_due
  ON jobs(json_extract(payload_json, '$.stream_id'), status, next_attempt_at, created_at)
  WHERE kind = 'query';

CREATE INDEX IF NOT EXISTS idx_jobs_query_path_active
  ON jobs(json_extract(payload_json, '$.path'))
  WHERE kind = 'query' AND status IN ('pending', 'running');

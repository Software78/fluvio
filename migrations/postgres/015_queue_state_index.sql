CREATE INDEX IF NOT EXISTS idx_fluvio_jobs_queue_state
  ON fluvio_jobs (queue, state);

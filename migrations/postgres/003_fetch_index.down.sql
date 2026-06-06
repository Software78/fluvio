DROP INDEX IF EXISTS idx_fluvio_jobs_fetch;
CREATE INDEX idx_fluvio_jobs_fetch
  ON fluvio_jobs (queue, priority ASC, scheduled_at ASC)
  WHERE state IN ('pending', 'scheduled');

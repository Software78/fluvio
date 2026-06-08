CREATE TABLE fluvio_periodic_jobs (
  kind         TEXT PRIMARY KEY,
  cron         TEXT NOT NULL,
  args         JSONB NOT NULL DEFAULT '{}',
  queue        TEXT NOT NULL DEFAULT 'default',
  max_attempts SMALLINT NOT NULL DEFAULT 3,
  next_run_at  TIMESTAMPTZ NOT NULL,
  last_run_at  TIMESTAMPTZ,
  paused       BOOLEAN NOT NULL DEFAULT false,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_fluvio_periodic_jobs_due
  ON fluvio_periodic_jobs (next_run_at)
  WHERE paused = false;

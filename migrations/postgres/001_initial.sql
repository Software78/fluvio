CREATE TYPE fluvio_job_state AS ENUM (
  'pending',
  'running',
  'completed',
  'failed',
  'dead',
  'scheduled',
  'cancelled'
);

CREATE TABLE fluvio_jobs (
  id             BIGSERIAL PRIMARY KEY,
  queue          TEXT        NOT NULL DEFAULT 'default',
  kind           TEXT        NOT NULL,
  args           JSONB       NOT NULL DEFAULT '{}',
  state          fluvio_job_state NOT NULL DEFAULT 'pending',
  priority       SMALLINT    NOT NULL DEFAULT 1,
  attempt        SMALLINT    NOT NULL DEFAULT 0,
  max_attempts   SMALLINT    NOT NULL DEFAULT 3,
  attempted_by   TEXT[]      NOT NULL DEFAULT '{}',
  scheduled_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempted_at   TIMESTAMPTZ,
  finalized_at   TIMESTAMPTZ,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  error_trace    JSONB,
  tags           TEXT[]      NOT NULL DEFAULT '{}',
  unique_key     TEXT,
  metadata       JSONB       NOT NULL DEFAULT '{}',
  workflow_id      TEXT,
  workflow_task_id TEXT,
  batch_id         TEXT,
  sequence_id      TEXT,
  sequence_pos     INT,
  encrypted        BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX idx_fluvio_jobs_fetch
  ON fluvio_jobs (queue, priority ASC, scheduled_at ASC)
  WHERE state IN ('pending', 'scheduled');

CREATE UNIQUE INDEX idx_fluvio_jobs_unique_key
  ON fluvio_jobs (unique_key)
  WHERE unique_key IS NOT NULL
    AND state NOT IN ('completed', 'dead', 'cancelled');

CREATE INDEX idx_fluvio_jobs_scheduled
  ON fluvio_jobs (scheduled_at)
  WHERE state = 'scheduled';

CREATE INDEX idx_fluvio_jobs_running_since
  ON fluvio_jobs (attempted_at)
  WHERE state = 'running';

CREATE TABLE fluvio_migrations (
  version    TEXT        PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

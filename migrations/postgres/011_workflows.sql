CREATE TABLE fluvio_workflows (
  id         TEXT PRIMARY KEY,
  state      TEXT NOT NULL DEFAULT 'running', -- running|completed|failed|cancelled
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata   JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE fluvio_workflow_tasks (
  workflow_id   TEXT NOT NULL REFERENCES fluvio_workflows(id) ON DELETE CASCADE,
  task_id       TEXT NOT NULL,
  state         TEXT NOT NULL DEFAULT 'waiting', -- waiting|pending|running|completed|failed|cancelled
  depends_on    TEXT[] NOT NULL DEFAULT '{}',
  job_id        BIGINT REFERENCES fluvio_jobs(id),
  queue         TEXT NOT NULL DEFAULT 'default',
  kind          TEXT NOT NULL,
  args          JSONB NOT NULL DEFAULT '{}',
  priority      SMALLINT NOT NULL DEFAULT 1,
  max_attempts  SMALLINT NOT NULL DEFAULT 3,
  tags          TEXT[] NOT NULL DEFAULT '{}',
  metadata      JSONB NOT NULL DEFAULT '{}',
  unique_key    TEXT,
  PRIMARY KEY (workflow_id, task_id)
);

CREATE INDEX idx_fluvio_workflow_tasks_job ON fluvio_workflow_tasks (job_id);

ALTER TABLE fluvio_jobs ADD COLUMN IF NOT EXISTS workflow_id TEXT;
ALTER TABLE fluvio_jobs ADD COLUMN IF NOT EXISTS workflow_task_id TEXT;

CREATE TABLE fluvio_workers (
  worker_id   TEXT        PRIMARY KEY,
  queues      JSONB       NOT NULL DEFAULT '{}',
  started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_fluvio_workers_last_seen ON fluvio_workers (last_seen);

CREATE TABLE fluvio_dead_jobs (
  id           BIGINT     PRIMARY KEY,
  queue        TEXT       NOT NULL,
  kind         TEXT       NOT NULL,
  args         JSONB      NOT NULL,
  error_trace  JSONB,
  metadata     JSONB      NOT NULL DEFAULT '{}',
  tags         TEXT[]     NOT NULL DEFAULT '{}',
  died_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  replayed_at  TIMESTAMPTZ
);


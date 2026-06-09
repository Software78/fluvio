CREATE TABLE fluvio_sequences (
  id         TEXT        PRIMARY KEY,
  kind       TEXT        NOT NULL,
  total      INT         NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_fluvio_jobs_sequence
  ON fluvio_jobs (sequence_id, sequence_pos)
  WHERE sequence_id IS NOT NULL;

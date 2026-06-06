CREATE TABLE fluvio_leader (
  id          TEXT        PRIMARY KEY DEFAULT 'singleton',
  elected_by  TEXT        NOT NULL,
  elected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE fluvio_queue_meta (
  queue      TEXT        PRIMARY KEY,
  paused     BOOLEAN     NOT NULL DEFAULT false,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

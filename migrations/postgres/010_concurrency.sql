CREATE TABLE fluvio_concurrency_slots (
  kind            TEXT NOT NULL,
  partition_key   TEXT NOT NULL DEFAULT '',
  running         INT  NOT NULL DEFAULT 0,
  max_concurrent  INT  NOT NULL,
  PRIMARY KEY (kind, partition_key)
);

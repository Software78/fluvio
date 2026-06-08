ALTER TABLE fluvio_jobs
  ADD COLUMN IF NOT EXISTS concurrency_slot_key TEXT;

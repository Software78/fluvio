package postgres

const fetchJobsSQL = `
WITH candidates AS (
  SELECT id
  FROM fluvio_jobs
  WHERE state = 'pending'
    AND scheduled_at <= now()
    AND queue = ANY($1::text[])
  ORDER BY priority ASC, scheduled_at ASC
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
UPDATE fluvio_jobs
SET
  state        = 'running',
  attempt      = attempt + 1,
  attempted_at = now(),
  attempted_by = array_append(attempted_by, $3::text)
WHERE id IN (SELECT id FROM candidates)
RETURNING id, queue, kind, args, state, priority, attempt, max_attempts,
  attempted_by, scheduled_at, attempted_at, finalized_at, created_at,
  error_trace, tags, unique_key, metadata, workflow_id, workflow_task_id, encrypted
`

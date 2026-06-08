package postgres

const fetchJobsSQL = `
WITH candidates AS (
  SELECT j.id
  FROM fluvio_jobs j
  LEFT JOIN fluvio_concurrency_slots cs
    ON cs.kind = j.kind AND cs.partition_key = ''
  WHERE j.state = 'pending'
    AND j.scheduled_at <= now()
    AND j.queue = ANY($1::text[])
    AND (cs.max_concurrent IS NULL OR cs.running < cs.max_concurrent)
  ORDER BY j.priority ASC, j.scheduled_at ASC
  LIMIT $2
  FOR UPDATE OF j SKIP LOCKED
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
  error_trace, tags, unique_key, metadata, workflow_id, workflow_task_id, encrypted,
  concurrency_slot_key
`

package postgres

const fetchJobsSQL = `
WITH candidates AS (
  SELECT j.id, j.kind, j.priority, j.scheduled_at
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
),
ranked AS (
  SELECT c.*,
    ROW_NUMBER() OVER (PARTITION BY c.kind ORDER BY c.priority ASC, c.scheduled_at ASC) AS rn
  FROM candidates c
),
eligible AS (
  SELECT r.id, r.kind
  FROM ranked r
  LEFT JOIN fluvio_concurrency_slots cs
    ON cs.kind = r.kind AND cs.partition_key = ''
  WHERE cs.max_concurrent IS NULL
    OR NOT (r.kind = ANY($4::text[]))
    OR (r.kind = ANY($4::text[]) AND r.rn <= cs.max_concurrent - cs.running)
),
slot_bump AS (
  UPDATE fluvio_concurrency_slots cs
  SET running = running + sub.n
  FROM (
    SELECT e.kind, COUNT(*)::int AS n
    FROM eligible e
    WHERE e.kind = ANY($4::text[])
    GROUP BY e.kind
  ) sub
  WHERE cs.kind = sub.kind AND cs.partition_key = ''
  RETURNING cs.kind
),
claimed AS (
  UPDATE fluvio_jobs j
  SET
    state        = 'running',
    attempt      = attempt + 1,
    attempted_at = now(),
    attempted_by = array_append(attempted_by, $3::text),
    concurrency_slot_key = CASE
      WHEN e.kind = ANY($4::text[]) THEN ''::text
      ELSE NULL
    END
  FROM eligible e
  WHERE j.id = e.id
  RETURNING j.id, j.queue, j.kind, j.args, j.state, j.priority, j.attempt, j.max_attempts,
    j.attempted_by, j.scheduled_at, j.attempted_at, j.finalized_at, j.created_at,
    j.error_trace, j.tags, j.unique_key, j.metadata, j.workflow_id, j.workflow_task_id, j.encrypted,
    j.concurrency_slot_key
),
_ AS (
  UPDATE fluvio_workflow_tasks t
  SET state = 'running'
  FROM claimed c
  WHERE t.job_id = c.id AND t.state = 'pending'
)
SELECT id, queue, kind, args, state, priority, attempt, max_attempts,
  attempted_by, scheduled_at, attempted_at, finalized_at, created_at,
  error_trace, tags, unique_key, metadata, workflow_id, workflow_task_id, encrypted,
  concurrency_slot_key
FROM claimed
`

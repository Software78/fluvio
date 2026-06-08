package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
)

var errLeaderLost = errors.New("fluvio/postgres: leader lease lost")

const leaderLockID int64 = 0x666c7576696f

const jobColumns = `id, queue, kind, args, state, priority, attempt, max_attempts,
	attempted_by, scheduled_at, attempted_at, finalized_at, created_at,
	error_trace, tags, unique_key, metadata, workflow_id, workflow_task_id, encrypted`

const deadJobColumns = `id, queue, kind, args, error_trace, metadata, tags, died_at`

func scanJob(row pgx.Row) (*driver.Job, error) {
	var j driver.Job
	var args, metadata, errorTrace []byte
	var uniqueKey, workflowID, workflowTaskID *string
	err := row.Scan(
		&j.ID, &j.Queue, &j.Kind, &args, &j.State, &j.Priority, &j.Attempt, &j.MaxAttempts,
		&j.AttemptedBy, &j.ScheduledAt, &j.AttemptedAt, &j.FinalizedAt, &j.CreatedAt,
		&errorTrace, &j.Tags, &uniqueKey, &metadata, &workflowID, &workflowTaskID, &j.Encrypted,
	)
	if err != nil {
		return nil, err
	}
	j.Args = args
	j.Metadata = metadata
	j.ErrorTrace = errorTrace
	j.UniqueKey = uniqueKey
	j.WorkflowID = workflowID
	j.WorkflowTaskID = workflowTaskID
	return &j, nil
}

func scanJobs(rows pgx.Rows) ([]*driver.Job, error) {
	var jobs []*driver.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func scanDeadJob(row pgx.Row) (*driver.Job, error) {
	var j driver.Job
	var args, metadata, errorTrace []byte
	var diedAt time.Time
	err := row.Scan(
		&j.ID, &j.Queue, &j.Kind, &args, &errorTrace, &metadata, &j.Tags, &diedAt,
	)
	if err != nil {
		return nil, err
	}
	j.State = "dead"
	j.Args = args
	j.Metadata = metadata
	j.ErrorTrace = errorTrace
	j.CreatedAt = diedAt
	j.FinalizedAt = &diedAt
	return &j, nil
}

func scanDeadJobs(rows pgx.Rows) ([]*driver.Job, error) {
	var jobs []*driver.Job
	for rows.Next() {
		j, err := scanDeadJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func normalizeEnqueueParams(p driver.EnqueueParams) (driver.EnqueueParams, error) {
	if p.Queue == "" {
		p.Queue = driver.QueueDefault
	}
	if p.Priority == 0 {
		p.Priority = 1
	}
	if p.MaxAttempts < 0 {
		return p, fmt.Errorf("%w: max_attempts must be at least 1", fluvio.ErrInvalidConfig)
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 3
	}
	if len(p.Args) == 0 {
		p.Args = []byte("{}")
	}
	if len(p.Metadata) == 0 {
		p.Metadata = []byte("{}")
	}
	if p.Tags == nil {
		p.Tags = []string{}
	}
	return p, nil
}

func initialState(p driver.EnqueueParams) string {
	if p.ScheduledAt != nil && p.ScheduledAt.After(time.Now().UTC()) {
		return "scheduled"
	}
	return "pending"
}

var validJobStates = map[string]struct{}{
	"pending": {}, "running": {}, "completed": {}, "failed": {},
	"dead": {}, "scheduled": {}, "cancelled": {},
}

func validateJobState(state string) error {
	if state == "" {
		return nil
	}
	if _, ok := validJobStates[state]; !ok {
		return fmt.Errorf("%w: %q", fluvio.ErrInvalidJobState, state)
	}
	return nil
}

func (d *Driver) Enqueue(ctx context.Context, p driver.EnqueueParams) (*driver.Job, error) {
	p, err := normalizeEnqueueParams(p)
	if err != nil {
		return nil, err
	}
	return d.enqueueWithQuerier(ctx, d.pool, p)
}

func (d *Driver) EnqueueTx(ctx context.Context, tx driver.Tx, p driver.EnqueueParams) (*driver.Job, error) {
	pgxTx, ok := tx.(pgx.Tx)
	if !ok {
		return nil, errors.New("fluvio/postgres: tx must be pgx.Tx")
	}
	p, err := normalizeEnqueueParams(p)
	if err != nil {
		return nil, err
	}
	return d.enqueueWithQuerier(ctx, pgxTx, p)
}

func (d *Driver) enqueueWithQuerier(ctx context.Context, q pgxQuerier, p driver.EnqueueParams) (*driver.Job, error) {
	if p.MaxAttempts < 1 {
		return nil, fmt.Errorf("%w: max_attempts must be at least 1", fluvio.ErrInvalidConfig)
	}
	state := initialState(p)
	scheduledAt := time.Now().UTC()
	if p.ScheduledAt != nil {
		scheduledAt = *p.ScheduledAt
	}

	row := q.QueryRow(ctx, `
		INSERT INTO fluvio_jobs (
			queue, kind, args, state, priority, max_attempts,
			scheduled_at, unique_key, tags, metadata, workflow_id, workflow_task_id, encrypted
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING `+jobColumns,
		p.Queue, p.Kind, p.Args, state, p.Priority, p.MaxAttempts,
		scheduledAt, p.UniqueKey, p.Tags, p.Metadata, p.WorkflowID, p.WorkflowTaskID, p.Encrypted,
	)
	job, err := scanJob(row)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, fluvio.ErrUniqueConflict
		}
		return nil, err
	}
	return job, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

type pgxQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (d *Driver) Fetch(ctx context.Context, queues []string, workerID string, maxJobs int) ([]*driver.Job, error) {
	if len(queues) == 0 {
		return nil, nil
	}

	activeQueues, err := d.filterPausedQueues(ctx, queues)
	if err != nil {
		return nil, err
	}
	if len(activeQueues) == 0 {
		return nil, nil
	}

	rows, err := d.pool.Query(ctx, fetchJobsSQL, activeQueues, maxJobs, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs, err := scanJobs(rows)
	if err != nil {
		return nil, err
	}

	var out []*driver.Job
	for _, job := range jobs {
		if !d.isGlobalConcurrencyKind(job.Kind) {
			out = append(out, job)
			continue
		}
		acquired, err := d.AcquireConcurrencySlot(ctx, job.Kind, "")
		if err != nil {
			return nil, err
		}
		if !acquired {
			_ = d.Nack(ctx, job.ID, fluvio.ErrConcurrencySlotUnavailable, time.Now().UTC().Add(5*time.Second))
			continue
		}
		out = append(out, job)
	}
	return out, nil
}

func (d *Driver) filterPausedQueues(ctx context.Context, queues []string) ([]string, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT q FROM unnest($1::text[]) AS q
		WHERE NOT EXISTS (
			SELECT 1 FROM fluvio_queue_meta m
			WHERE m.queue = q AND m.paused = true
		)
	`, queues)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var active []string
	for rows.Next() {
		var q string
		if err := rows.Scan(&q); err != nil {
			return nil, err
		}
		active = append(active, q)
	}
	return active, rows.Err()
}

func (d *Driver) Ack(ctx context.Context, jobID int64) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var kind string
	var workflowID, workflowTaskID *string
	err = tx.QueryRow(ctx, `
		UPDATE fluvio_jobs
		SET state = 'completed', finalized_at = now()
		WHERE id = $1 AND state = 'running'
		RETURNING kind, workflow_id, workflow_task_id
	`, jobID).Scan(&kind, &workflowID, &workflowTaskID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fluvio.ErrJobNotFound
		}
		return err
	}

	if workflowID != nil && workflowTaskID != nil {
		if err := d.completeWorkflowTaskTx(ctx, tx, *workflowID, *workflowTaskID); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	if d.isGlobalConcurrencyKind(kind) {
		if err := d.ReleaseConcurrencySlot(ctx, kind, ""); err != nil {
			return err
		}
	}
	return nil
}

type errorTraceEntry struct {
	Attempt int16     `json:"attempt"`
	Error   string    `json:"error"`
	At      time.Time `json:"at"`
}

func (d *Driver) Nack(ctx context.Context, jobID int64, jobErr error, nextAttemptAt time.Time) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var queue, kind string
	var args, metadata []byte
	var tags []string
	var attempt, maxAttempts int16
	var errorTrace []byte
	var workflowID, workflowTaskID *string
	err = tx.QueryRow(ctx, `
		SELECT queue, kind, args, attempt, max_attempts,
			COALESCE(error_trace, '[]'::jsonb),
			metadata, tags, workflow_id, workflow_task_id
		FROM fluvio_jobs WHERE id = $1 AND state = 'running'
		FOR UPDATE
	`, jobID).Scan(
		&queue, &kind, &args, &attempt, &maxAttempts,
		&errorTrace, &metadata, &tags, &workflowID, &workflowTaskID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fluvio.ErrJobNotFound
		}
		return err
	}

	var trace []errorTraceEntry
	_ = json.Unmarshal(errorTrace, &trace)
	trace = append(trace, errorTraceEntry{
		Attempt: attempt,
		Error:   jobErr.Error(),
		At:      time.Now().UTC(),
	})
	newTrace, _ := json.Marshal(trace)

	var tag pgconn.CommandTag
	if attempt >= maxAttempts {
		if _, err := tx.Exec(ctx, `
			INSERT INTO fluvio_dead_jobs (id, queue, kind, args, error_trace, metadata, tags)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, jobID, queue, kind, args, newTrace, metadata, tags); err != nil {
			return err
		}
		tag, err = tx.Exec(ctx, `
			UPDATE fluvio_jobs
			SET state = 'dead', finalized_at = now(), error_trace = $2
			WHERE id = $1
		`, jobID, newTrace)
	} else {
		tag, err = tx.Exec(ctx, `
			UPDATE fluvio_jobs
			SET state = 'scheduled', scheduled_at = $2, error_trace = $3
			WHERE id = $1
		`, jobID, nextAttemptAt, newTrace)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fluvio.ErrJobNotFound
	}

	terminal := attempt >= maxAttempts
	if terminal && workflowID != nil && workflowTaskID != nil {
		if err := d.failWorkflowTaskTx(ctx, tx, *workflowID, *workflowTaskID); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// Do not release when the job never acquired a slot (Fetch-time concurrency rejection).
	if d.isGlobalConcurrencyKind(kind) && !errors.Is(jobErr, fluvio.ErrConcurrencySlotUnavailable) {
		return d.ReleaseConcurrencySlot(ctx, kind, "")
	}
	return nil
}

func (d *Driver) Cancel(ctx context.Context, jobID int64) error {
	tag, err := d.pool.Exec(ctx, `
		UPDATE fluvio_jobs
		SET state = 'cancelled', finalized_at = now()
		WHERE id = $1 AND state IN ('pending', 'scheduled')
	`, jobID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fluvio.ErrJobNotFound
	}
	return nil
}

func (d *Driver) GetJob(ctx context.Context, jobID int64) (*driver.Job, error) {
	row := d.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM fluvio_jobs WHERE id = $1`, jobID)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fluvio.ErrJobNotFound
		}
		return nil, err
	}
	return job, nil
}

func (d *Driver) ListJobs(ctx context.Context, p driver.ListJobsParams) ([]*driver.Job, error) {
	if err := validateJobState(p.State); err != nil {
		return nil, err
	}
	limit := p.Limit
	if limit <= 0 {
		limit = 50
	}
	query := `SELECT ` + jobColumns + ` FROM fluvio_jobs WHERE 1=1`
	args := []any{}
	argN := 1
	if p.Queue != "" {
		query += fmt.Sprintf(" AND queue = $%d", argN)
		args = append(args, p.Queue)
		argN++
	}
	if p.State != "" {
		query += fmt.Sprintf(" AND state = $%d", argN)
		args = append(args, p.State)
		argN++
	}
	if p.Kind != "" {
		query += fmt.Sprintf(" AND kind = $%d", argN)
		args = append(args, p.Kind)
		argN++
	}
	query += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d OFFSET $%d", argN, argN+1)
	args = append(args, limit, p.Offset)

	rows, err := d.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

func (d *Driver) ListDead(ctx context.Context, limit, offset int) ([]*driver.Job, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := d.pool.Query(ctx, `
		SELECT `+deadJobColumns+`
		FROM fluvio_dead_jobs
		ORDER BY died_at DESC
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeadJobs(rows)
}

func (d *Driver) ReplayDead(ctx context.Context, jobID int64) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var queue, kind string
	var args, metadata []byte
	var tags []string
	err = tx.QueryRow(ctx, `
		SELECT queue, kind, args, metadata, tags
		FROM fluvio_dead_jobs
		WHERE id = $1 AND replayed_at IS NULL
		FOR UPDATE
	`, jobID).Scan(&queue, &kind, &args, &metadata, &tags)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fluvio.ErrJobNotFound
		}
		return err
	}

	var priority, maxAttempts int16
	err = tx.QueryRow(ctx, `
		SELECT priority, max_attempts
		FROM fluvio_jobs
		WHERE id = $1
	`, jobID).Scan(&priority, &maxAttempts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			priority = 1
			maxAttempts = 3
		} else {
			return err
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO fluvio_jobs (
			queue, kind, args, state, priority, max_attempts,
			scheduled_at, tags, metadata
		) VALUES ($1, $2, $3, 'pending', $4, $5, now(), $6, $7)
	`, queue, kind, args, priority, maxAttempts, tags, metadata)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE fluvio_dead_jobs SET replayed_at = now() WHERE id = $1
	`, jobID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fluvio.ErrJobNotFound
	}
	return tx.Commit(ctx)
}

func (d *Driver) PurgeDead(ctx context.Context, before time.Time) (int64, error) {
	tag, err := d.pool.Exec(ctx, `
		DELETE FROM fluvio_dead_jobs
		WHERE died_at < $1
	`, before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (d *Driver) TickScheduled(ctx context.Context, now time.Time) (int64, error) {
	tag, err := d.pool.Exec(ctx, `
		UPDATE fluvio_jobs
		SET state = 'pending'
		WHERE state = 'scheduled' AND scheduled_at <= $1
	`, now)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (d *Driver) UniqueJobExists(ctx context.Context, uniqueKey string) (bool, error) {
	var exists bool
	err := d.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM fluvio_jobs
			WHERE unique_key = $1
			  AND state NOT IN ('completed', 'dead', 'cancelled')
		)
	`, uniqueKey).Scan(&exists)
	return exists, err
}

func (d *Driver) PauseQueue(ctx context.Context, queue string) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO fluvio_queue_meta (queue, paused, updated_at)
		VALUES ($1, true, now())
		ON CONFLICT (queue) DO UPDATE SET paused = true, updated_at = now()
	`, queue)
	return err
}

func (d *Driver) ResumeQueue(ctx context.Context, queue string) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO fluvio_queue_meta (queue, paused, updated_at)
		VALUES ($1, false, now())
		ON CONFLICT (queue) DO UPDATE SET paused = false, updated_at = now()
	`, queue)
	return err
}

func (d *Driver) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	var paused bool
	err := d.pool.QueryRow(ctx, `
		SELECT COALESCE(
			(SELECT paused FROM fluvio_queue_meta WHERE queue = $1),
			false
		)
	`, queue).Scan(&paused)
	return paused, err
}

func (d *Driver) QueueStats(ctx context.Context, queue string) (*driver.QueueStats, error) {
	row := d.pool.QueryRow(ctx, `
		SELECT
			$1::text AS queue,
			COUNT(*) FILTER (WHERE state = 'pending') AS pending,
			COUNT(*) FILTER (WHERE state = 'running') AS running,
			COUNT(*) FILTER (WHERE state = 'scheduled') AS scheduled,
			COUNT(*) FILTER (WHERE state = 'dead') AS dead,
			COUNT(*) FILTER (WHERE state = 'completed') AS completed,
			COUNT(*) FILTER (WHERE state = 'failed') AS failed,
			COALESCE((SELECT paused FROM fluvio_queue_meta WHERE queue = $1), false) AS paused
		FROM fluvio_jobs WHERE queue = $1
	`, queue)

	stats := &driver.QueueStats{}
	err := row.Scan(
		&stats.Queue, &stats.Pending, &stats.Running, &stats.Scheduled,
		&stats.Dead, &stats.Completed, &stats.Failed, &stats.Paused,
	)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (d *Driver) ListQueues(ctx context.Context) ([]*driver.QueueStats, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT
			q.queue,
			COUNT(j.id) FILTER (WHERE j.state = 'pending') AS pending,
			COUNT(j.id) FILTER (WHERE j.state = 'running') AS running,
			COUNT(j.id) FILTER (WHERE j.state = 'scheduled') AS scheduled,
			COUNT(j.id) FILTER (WHERE j.state = 'dead') AS dead,
			COUNT(j.id) FILTER (WHERE j.state = 'completed') AS completed,
			COUNT(j.id) FILTER (WHERE j.state = 'failed') AS failed,
			COALESCE(m.paused, false) AS paused
		FROM (
			SELECT DISTINCT queue FROM fluvio_jobs
			UNION
			SELECT queue FROM fluvio_queue_meta
		) q(queue)
		LEFT JOIN fluvio_jobs j ON j.queue = q.queue
		LEFT JOIN fluvio_queue_meta m ON m.queue = q.queue
		GROUP BY q.queue, m.paused
		ORDER BY q.queue
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []*driver.QueueStats
	for rows.Next() {
		s := &driver.QueueStats{}
		if err := rows.Scan(
			&s.Queue, &s.Pending, &s.Running, &s.Scheduled,
			&s.Dead, &s.Completed, &s.Failed, &s.Paused,
		); err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	return stats, rows.Err()
}

func (d *Driver) TryAcquireLeader(ctx context.Context) (bool, error) {
	d.leaderMu.Lock()
	defer d.leaderMu.Unlock()

	if d.useLease {
		return d.tryAcquireLease(ctx)
	}
	if d.leaderConn == nil {
		conn, err := d.pool.Acquire(ctx)
		if err != nil {
			return false, err
		}
		d.leaderConn = conn
	}
	var acquired bool
	err := d.leaderConn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderLockID).Scan(&acquired)
	return acquired, err
}

func (d *Driver) RenewLeader(ctx context.Context) error {
	if !d.useLease {
		return nil
	}
	tag, err := d.pool.Exec(ctx, `
		UPDATE fluvio_leader
		SET expires_at = now() + interval '60 seconds'
		WHERE id = 'singleton' AND elected_by = $1
	`, d.leaderID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errLeaderLost
	}
	return nil
}

func (d *Driver) ReleaseLeader(ctx context.Context) error {
	d.leaderMu.Lock()
	defer d.leaderMu.Unlock()

	if d.useLease {
		_, err := d.pool.Exec(ctx, `DELETE FROM fluvio_leader WHERE elected_by = $1`, d.leaderID)
		return err
	}
	if d.leaderConn == nil {
		return nil
	}
	_, err := d.leaderConn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, leaderLockID)
	d.leaderConn.Release()
	d.leaderConn = nil
	return err
}

func (d *Driver) tryAcquireLease(ctx context.Context) (bool, error) {
	expiry := time.Now().Add(60 * time.Second)
	tag, err := d.pool.Exec(ctx, `
		INSERT INTO fluvio_leader (id, elected_by, expires_at)
		VALUES ('singleton', $1, $2)
		ON CONFLICT (id) DO UPDATE SET
			elected_by = EXCLUDED.elected_by,
			elected_at = now(),
			expires_at = EXCLUDED.expires_at
		WHERE fluvio_leader.expires_at < now()
	`, d.leaderID, expiry)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() > 0 {
		d.leaseExpiry = expiry
		return true, nil
	}
	var electedBy string
	err = d.pool.QueryRow(ctx, `
		SELECT elected_by FROM fluvio_leader WHERE id = 'singleton' AND expires_at > now()
	`).Scan(&electedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return electedBy == d.leaderID, nil
}

func (d *Driver) StuckJobs(ctx context.Context, timeout time.Duration) ([]*driver.Job, error) {
	interval := fmt.Sprintf("%d seconds", int64(timeout.Seconds()))
	rows, err := d.pool.Query(ctx, `
		SELECT `+jobColumns+`
		FROM fluvio_jobs
		WHERE state = 'running' AND attempted_at < now() - $1::interval
	`, interval)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

func (d *Driver) UpsertWorker(ctx context.Context, workerID string, queues map[string]int) error {
	if queues == nil {
		queues = map[string]int{}
	}
	queuesJSON, err := json.Marshal(queues)
	if err != nil {
		return err
	}
	_, err = d.pool.Exec(ctx, `
		INSERT INTO fluvio_workers (worker_id, queues, started_at, last_seen)
		VALUES ($1, $2, now(), now())
		ON CONFLICT (worker_id) DO UPDATE SET
			queues = EXCLUDED.queues,
			last_seen = now()
	`, workerID, queuesJSON)
	return err
}

func (d *Driver) RemoveWorker(ctx context.Context, workerID string) error {
	_, err := d.pool.Exec(ctx, `DELETE FROM fluvio_workers WHERE worker_id = $1`, workerID)
	return err
}

func (d *Driver) ListWorkers(ctx context.Context, staleAfter time.Duration) ([]*driver.WorkerInstance, error) {
	interval := fmt.Sprintf("%d seconds", int64(staleAfter.Seconds()))
	rows, err := d.pool.Query(ctx, `
		SELECT worker_id, queues, started_at, last_seen
		FROM fluvio_workers
		WHERE last_seen > now() - $1::interval
		ORDER BY worker_id
	`, interval)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workers []*driver.WorkerInstance
	for rows.Next() {
		var w driver.WorkerInstance
		var queuesJSON []byte
		if err := rows.Scan(&w.ID, &queuesJSON, &w.StartedAt, &w.LastSeen); err != nil {
			return nil, err
		}
		w.Queues = map[string]int{}
		if len(queuesJSON) > 0 {
			_ = json.Unmarshal(queuesJSON, &w.Queues)
		}
		workers = append(workers, &w)
	}
	return workers, rows.Err()
}

func (d *Driver) EnqueueMany(ctx context.Context, params []driver.EnqueueParams) ([]*driver.Job, error) {
	if len(params) == 0 {
		return nil, nil
	}

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	jobs := make([]*driver.Job, 0, len(params))
	for _, p := range params {
		p, err := normalizeEnqueueParams(p)
		if err != nil {
			return nil, err
		}
		job, err := d.enqueueWithQuerier(ctx, tx, p)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return jobs, nil
}

const periodicJobColumns = `kind, cron, queue, max_attempts, args, next_run_at, last_run_at, paused`

func scanPeriodicJob(row pgx.Row) (*driver.PeriodicJob, error) {
	var j driver.PeriodicJob
	var args []byte
	err := row.Scan(
		&j.Kind, &j.Cron, &j.Queue, &j.MaxAttempts, &args,
		&j.NextRunAt, &j.LastRunAt, &j.Paused,
	)
	if err != nil {
		return nil, err
	}
	j.Args = args
	return &j, nil
}

func scanPeriodicJobs(rows pgx.Rows) ([]*driver.PeriodicJob, error) {
	var jobs []*driver.PeriodicJob
	for rows.Next() {
		j, err := scanPeriodicJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (d *Driver) BeginTx(ctx context.Context) (driver.Tx, error) {
	return d.pool.Begin(ctx)
}

func (d *Driver) CommitTx(ctx context.Context, tx driver.Tx) error {
	pgxTx, ok := tx.(pgx.Tx)
	if !ok {
		return errors.New("fluvio/postgres: tx must be pgx.Tx")
	}
	return pgxTx.Commit(ctx)
}

func (d *Driver) RollbackTx(ctx context.Context, tx driver.Tx) error {
	pgxTx, ok := tx.(pgx.Tx)
	if !ok {
		return errors.New("fluvio/postgres: tx must be pgx.Tx")
	}
	return pgxTx.Rollback(ctx)
}

func (d *Driver) UpsertPeriodicJob(ctx context.Context, kind, cron, queue string, maxAttempts int16, args []byte) error {
	if queue == "" {
		queue = driver.QueueDefault
	}
	if maxAttempts == 0 {
		maxAttempts = 3
	}
	if len(args) == 0 {
		args = []byte("{}")
	}
	_, err := d.pool.Exec(ctx, `
		INSERT INTO fluvio_periodic_jobs (kind, cron, args, queue, max_attempts, next_run_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (kind) DO UPDATE SET
			cron = EXCLUDED.cron,
			args = EXCLUDED.args,
			queue = EXCLUDED.queue,
			max_attempts = EXCLUDED.max_attempts
	`, kind, cron, args, queue, maxAttempts)
	return err
}

func (d *Driver) DuePeriodicJobs(ctx context.Context, now time.Time) ([]*driver.PeriodicJob, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT `+periodicJobColumns+`
		FROM fluvio_periodic_jobs
		WHERE next_run_at <= $1 AND paused = false
		FOR UPDATE SKIP LOCKED
	`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPeriodicJobs(rows)
}

func (d *Driver) UpdatePeriodicJobNextRun(ctx context.Context, kind string, nextRun time.Time) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE fluvio_periodic_jobs
		SET next_run_at = $2
		WHERE kind = $1
	`, kind, nextRun)
	return err
}

func (d *Driver) UpdatePeriodicJobNextRunTx(ctx context.Context, tx driver.Tx, kind string, nextRun time.Time) (bool, error) {
	pgxTx, ok := tx.(pgx.Tx)
	if !ok {
		return false, errors.New("fluvio/postgres: tx must be pgx.Tx")
	}
	tag, err := pgxTx.Exec(ctx, `
		UPDATE fluvio_periodic_jobs
		SET next_run_at = $2, last_run_at = now()
		WHERE kind = $1 AND next_run_at <= now() AND paused = false
	`, kind, nextRun)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (d *Driver) ListPeriodicJobs(ctx context.Context) ([]*driver.PeriodicJob, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT `+periodicJobColumns+`
		FROM fluvio_periodic_jobs
		ORDER BY kind
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPeriodicJobs(rows)
}

func (d *Driver) PausePeriodicJob(ctx context.Context, kind string) error {
	tag, err := d.pool.Exec(ctx, `
		UPDATE fluvio_periodic_jobs SET paused = true WHERE kind = $1
	`, kind)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fluvio.ErrJobNotFound
	}
	return nil
}

func (d *Driver) ResumePeriodicJob(ctx context.Context, kind string) error {
	tag, err := d.pool.Exec(ctx, `
		UPDATE fluvio_periodic_jobs SET paused = false WHERE kind = $1
	`, kind)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fluvio.ErrJobNotFound
	}
	return nil
}

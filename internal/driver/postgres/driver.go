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

const leaderLockID int64 = 0x666c7576696f

const jobColumns = `id, queue, kind, args, state, priority, attempt, max_attempts,
	attempted_by, scheduled_at, attempted_at, finalized_at, created_at,
	error_trace, tags, unique_key, metadata`

func scanJob(row pgx.Row) (*driver.Job, error) {
	var j driver.Job
	var args, metadata, errorTrace []byte
	var uniqueKey *string
	err := row.Scan(
		&j.ID, &j.Queue, &j.Kind, &args, &j.State, &j.Priority, &j.Attempt, &j.MaxAttempts,
		&j.AttemptedBy, &j.ScheduledAt, &j.AttemptedAt, &j.FinalizedAt, &j.CreatedAt,
		&errorTrace, &j.Tags, &uniqueKey, &metadata,
	)
	if err != nil {
		return nil, err
	}
	j.Args = args
	j.Metadata = metadata
	j.ErrorTrace = errorTrace
	j.UniqueKey = uniqueKey
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

func normalizeEnqueueParams(p driver.EnqueueParams) driver.EnqueueParams {
	if p.Queue == "" {
		p.Queue = driver.QueueDefault
	}
	if p.Priority == 0 {
		p.Priority = 1
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
	return p
}

func initialState(p driver.EnqueueParams) string {
	if p.ScheduledAt != nil && p.ScheduledAt.After(time.Now()) {
		return "scheduled"
	}
	return "pending"
}

func (d *Driver) Enqueue(ctx context.Context, p driver.EnqueueParams) (*driver.Job, error) {
	return d.enqueueWithQuerier(ctx, d.pool, normalizeEnqueueParams(p))
}

func (d *Driver) EnqueueTx(ctx context.Context, tx driver.Tx, p driver.EnqueueParams) (*driver.Job, error) {
	pgxTx, ok := tx.(pgx.Tx)
	if !ok {
		return nil, errors.New("fluvio/postgres: tx must be pgx.Tx")
	}
	return d.enqueueWithQuerier(ctx, pgxTx, normalizeEnqueueParams(p))
}

func (d *Driver) enqueueWithQuerier(ctx context.Context, q pgxQuerier, p driver.EnqueueParams) (*driver.Job, error) {
	state := initialState(p)
	scheduledAt := time.Now()
	if p.ScheduledAt != nil {
		scheduledAt = *p.ScheduledAt
	}

	row := q.QueryRow(ctx, `
		INSERT INTO fluvio_jobs (
			queue, kind, args, state, priority, max_attempts,
			scheduled_at, unique_key, tags, metadata
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING `+jobColumns,
		p.Queue, p.Kind, p.Args, state, p.Priority, p.MaxAttempts,
		scheduledAt, p.UniqueKey, p.Tags, p.Metadata,
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
	return scanJobs(rows)
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
	tag, err := d.pool.Exec(ctx, `
		UPDATE fluvio_jobs
		SET state = 'completed', finalized_at = now()
		WHERE id = $1 AND state = 'running'
	`, jobID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fluvio.ErrJobNotFound
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

	var attempt, maxAttempts int16
	var errorTrace []byte
	err = tx.QueryRow(ctx, `
		SELECT attempt, max_attempts, COALESCE(error_trace, '[]'::jsonb)
		FROM fluvio_jobs WHERE id = $1 AND state = 'running'
		FOR UPDATE
	`, jobID).Scan(&attempt, &maxAttempts, &errorTrace)
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

	if attempt >= maxAttempts {
		_, err = tx.Exec(ctx, `
			UPDATE fluvio_jobs
			SET state = 'dead', finalized_at = now(), error_trace = $2
			WHERE id = $1
		`, jobID, newTrace)
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE fluvio_jobs
			SET state = 'scheduled', scheduled_at = $2, error_trace = $3
			WHERE id = $1
		`, jobID, nextAttemptAt, newTrace)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
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
	_, err := d.pool.Exec(ctx, `
		UPDATE fluvio_leader
		SET expires_at = now() + interval '60 seconds'
		WHERE id = 'singleton' AND elected_by = $1
	`, d.leaderID)
	return err
}

func (d *Driver) ReleaseLeader(ctx context.Context) error {
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
	rows, err := d.pool.Query(ctx, `
		SELECT `+jobColumns+`
		FROM fluvio_jobs
		WHERE state = 'running' AND attempted_at < now() - $1::interval
	`, timeout.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
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
		job, err := d.enqueueWithQuerier(ctx, tx, normalizeEnqueueParams(p))
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

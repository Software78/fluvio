//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
	"github.com/software78/fluvio/internal/driver/postgres"
)

func setupPostgres(t *testing.T) (*pgxpool.Pool, *postgres.Driver) {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("fluvio"),
		tcpostgres.WithUsername("fluvio"),
		tcpostgres.WithPassword("fluvio"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

	d := postgres.New(pool, postgres.Config{})
	require.NoError(t, d.Migrate(ctx))
	return pool, d
}

func TestMigrateUpDownStatus(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	status, err := d.MigrationStatus(ctx)
	require.NoError(t, err)
	require.Len(t, status, 12)

	require.NoError(t, d.MigrateDown(ctx, 1))
	status, err = d.MigrationStatus(ctx)
	require.NoError(t, err)
	require.Len(t, status, 11)

	require.NoError(t, d.Migrate(ctx))
	status, err = d.MigrationStatus(ctx)
	require.NoError(t, err)
	require.Len(t, status, 12)
}

func TestEnqueueFetchAck(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	job, err := d.Enqueue(ctx, driver.EnqueueParams{
		Kind: "send_email",
		Args: []byte(`{"to":"a@example.com"}`),
	})
	require.NoError(t, err)
	require.Equal(t, "pending", job.State)

	jobs, err := d.Fetch(ctx, []string{"default"}, "worker-1", 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, "running", jobs[0].State)
	require.Equal(t, int16(1), jobs[0].Attempt)

	require.NoError(t, d.Ack(ctx, jobs[0].ID))
	got, err := d.GetJob(ctx, jobs[0].ID)
	require.NoError(t, err)
	require.Equal(t, "completed", got.State)
}

func TestEnqueueTx(t *testing.T) {
	pool, d := setupPostgres(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)

	job, err := d.EnqueueTx(ctx, tx, driver.EnqueueParams{
		Kind: "tx_job",
		Args: []byte(`{}`),
	})
	require.NoError(t, err)

	_, err = d.GetJob(ctx, job.ID)
	require.Error(t, err)

	require.NoError(t, tx.Commit(ctx))

	got, err := d.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, "tx_job", got.Kind)
}

func TestNackRetryAndDead(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	job, err := d.Enqueue(ctx, driver.EnqueueParams{
		Kind:        "fail_job",
		Args:        []byte(`{}`),
		MaxAttempts: 2,
	})
	require.NoError(t, err)

	jobs, err := d.Fetch(ctx, []string{"default"}, "w1", 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	require.NoError(t, d.Nack(ctx, jobs[0].ID, errTest, time.Now()))

	n, err := d.TickScheduled(ctx, time.Now())
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	jobs, err = d.Fetch(ctx, []string{"default"}, "w1", 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	require.NoError(t, d.Nack(ctx, jobs[0].ID, errTest, time.Now()))

	got, err := d.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, "dead", got.State)

	var deadCount int
	err = d.Pool().QueryRow(ctx, `
		SELECT COUNT(*)
		FROM fluvio_dead_jobs
		WHERE id = $1
	`, job.ID).Scan(&deadCount)
	require.NoError(t, err)
	require.Equal(t, 1, deadCount)
}

var errTest = testError{}

type testError struct{}

func (testError) Error() string { return "test error" }

func TestUniqueConflict(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()
	key := "unique:1"

	_, err := d.Enqueue(ctx, driver.EnqueueParams{
		Kind:      "unique_job",
		Args:      []byte(`{}`),
		UniqueKey: &key,
	})
	require.NoError(t, err)

	_, err = d.Enqueue(ctx, driver.EnqueueParams{
		Kind:      "unique_job",
		Args:      []byte(`{}`),
		UniqueKey: &key,
	})
	require.ErrorIs(t, err, fluvio.ErrUniqueConflict)
}

func TestMaxAttemptsValidation(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	_, err := d.Enqueue(ctx, driver.EnqueueParams{
		Kind:        "bad",
		Args:        []byte(`{}`),
		MaxAttempts: -1,
	})
	require.ErrorIs(t, err, fluvio.ErrInvalidConfig)

	job, err := d.Enqueue(ctx, driver.EnqueueParams{
		Kind:        "defaulted",
		Args:        []byte(`{}`),
		MaxAttempts: 0,
	})
	require.NoError(t, err)
	require.Equal(t, int16(3), job.MaxAttempts)
}

func TestPauseQueue(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	_, err := d.Enqueue(ctx, driver.EnqueueParams{Kind: "k", Args: []byte(`{}`)})
	require.NoError(t, err)
	require.NoError(t, d.PauseQueue(ctx, "default"))

	jobs, err := d.Fetch(ctx, []string{"default"}, "w1", 10)
	require.ErrorIs(t, err, driver.ErrQueuesPaused)
	require.Nil(t, jobs)

	require.NoError(t, d.ResumeQueue(ctx, "default"))
	jobs, err = d.Fetch(ctx, []string{"default"}, "w1", 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
}

func TestEnqueueMany(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	params := make([]driver.EnqueueParams, 100)
	for i := range params {
		params[i] = driver.EnqueueParams{Kind: "bulk", Args: []byte(`{}`)}
	}
	jobs, err := d.EnqueueMany(ctx, params)
	require.NoError(t, err)
	require.Len(t, jobs, 100)
}

func TestPollOnlySubscribeDisabled(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()
	d.ConfigureNotify(true, 0)

	_, err := d.Subscribe(ctx, []string{"default"})
	require.Error(t, err)
}

func TestTickScheduledNotify(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	sub, err := d.Subscribe(ctx, []string{"default"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	dueTime := time.Now().Add(5 * time.Second)
	job, err := d.Enqueue(ctx, driver.EnqueueParams{
		Kind:        "due",
		Args:        []byte(`{}`),
		ScheduledAt: &dueTime,
	})
	require.NoError(t, err)
	require.Equal(t, "scheduled", job.State)

	n, err := d.TickScheduled(ctx, dueTime.Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	select {
	case <-sub.Wake():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected NOTIFY after TickScheduled")
	}
}

func TestTickScheduled(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	future := time.Now().Add(time.Hour)
	job, err := d.Enqueue(ctx, driver.EnqueueParams{
		Kind:        "later",
		Args:        []byte(`{}`),
		ScheduledAt: &future,
	})
	require.NoError(t, err)
	require.Equal(t, "scheduled", job.State)

	n, err := d.TickScheduled(ctx, time.Now())
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	dueTime := time.Now().Add(5 * time.Second)
	_, err = d.Enqueue(ctx, driver.EnqueueParams{
		Kind:        "due",
		Args:        []byte(`{}`),
		ScheduledAt: &dueTime,
	})
	require.NoError(t, err)

	n, err = d.TickScheduled(ctx, dueTime.Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}

func TestQueueStats(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	_, err := d.Enqueue(ctx, driver.EnqueueParams{Kind: "k", Args: []byte(`{}`)})
	require.NoError(t, err)
	stats, err := d.QueueStats(ctx, "default")
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.Pending)
}

func TestWorkerRegistry(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	require.NoError(t, d.UpsertWorker(ctx, "worker-a", map[string]int{"default": 10}))
	require.NoError(t, d.UpsertWorker(ctx, "worker-b", map[string]int{"default": 5, "critical": 2}))

	workers, err := d.ListWorkers(ctx, 90*time.Second)
	require.NoError(t, err)
	require.Len(t, workers, 2)

	byID := map[string]*driver.WorkerInstance{}
	for _, w := range workers {
		byID[w.ID] = w
	}
	require.Equal(t, 10, byID["worker-a"].Queues["default"])
	require.Equal(t, 5, byID["worker-b"].Queues["default"])
	require.Equal(t, 2, byID["worker-b"].Queues["critical"])

	require.NoError(t, d.RemoveWorker(ctx, "worker-a"))
	workers, err = d.ListWorkers(ctx, 90*time.Second)
	require.NoError(t, err)
	require.Len(t, workers, 1)
	require.Equal(t, "worker-b", workers[0].ID)

	// Upsert again preserves started_at on conflict.
	firstSeen := workers[0].StartedAt
	require.NoError(t, d.UpsertWorker(ctx, "worker-b", map[string]int{"default": 8}))
	workers, err = d.ListWorkers(ctx, 90*time.Second)
	require.NoError(t, err)
	require.Len(t, workers, 1)
	require.Equal(t, firstSeen, workers[0].StartedAt)
	require.Equal(t, 8, workers[0].Queues["default"])
}

func TestPeriodicJobs(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	nextRun := time.Date(2030, 1, 15, 9, 0, 0, 0, time.UTC)
	require.NoError(t, d.UpsertPeriodicJob(ctx, "daily-report", "0 9 * * *", "default", 3, []byte(`{"format":"pdf"}`), nextRun))

	jobs, err := d.ListPeriodicJobs(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, "daily-report", jobs[0].Kind)
	require.Equal(t, "0 9 * * *", jobs[0].Cron)
	require.Equal(t, "default", jobs[0].Queue)
	require.Equal(t, int16(3), jobs[0].MaxAttempts)
	require.JSONEq(t, `{"format":"pdf"}`, string(jobs[0].Args))
	require.False(t, jobs[0].Paused)
	require.Equal(t, nextRun, jobs[0].NextRunAt)

	updatedNextRun := time.Date(2030, 2, 1, 10, 0, 0, 0, time.UTC)
	require.NoError(t, d.UpsertPeriodicJob(ctx, "daily-report", "0 10 * * *", "critical", 5, []byte(`{"format":"csv"}`), updatedNextRun))
	jobs, err = d.ListPeriodicJobs(ctx)
	require.NoError(t, err)
	require.Equal(t, "0 10 * * *", jobs[0].Cron)
	require.Equal(t, "critical", jobs[0].Queue)
	require.Equal(t, int16(5), jobs[0].MaxAttempts)
	require.Equal(t, updatedNextRun, jobs[0].NextRunAt)

	require.NoError(t, d.PausePeriodicJob(ctx, "daily-report"))
	jobs, err = d.ListPeriodicJobs(ctx)
	require.NoError(t, err)
	require.True(t, jobs[0].Paused)

	past := time.Now().Add(-time.Hour)
	due, err := d.DuePeriodicJobs(ctx, time.Now())
	require.NoError(t, err)
	require.Empty(t, due)

	require.NoError(t, d.ResumePeriodicJob(ctx, "daily-report"))
	require.NoError(t, d.UpdatePeriodicJobNextRun(ctx, "daily-report", past))

	due, err = d.DuePeriodicJobs(ctx, time.Now())
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.Equal(t, "daily-report", due[0].Kind)

	future := time.Now().Add(time.Hour)
	tx, err := d.BeginTx(ctx)
	require.NoError(t, err)
	claimed, err := d.UpdatePeriodicJobNextRunTx(ctx, tx, "daily-report", future)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, d.CommitTx(ctx, tx))

	jobs, err = d.ListPeriodicJobs(ctx)
	require.NoError(t, err)
	require.True(t, jobs[0].NextRunAt.After(time.Now()))
	require.NotNil(t, jobs[0].LastRunAt)

	// Second claim should fail (next_run_at is in the future).
	tx, err = d.BeginTx(ctx)
	require.NoError(t, err)
	claimed, err = d.UpdatePeriodicJobNextRunTx(ctx, tx, "daily-report", future)
	require.NoError(t, err)
	require.False(t, claimed)
	require.NoError(t, d.RollbackTx(ctx, tx))
}

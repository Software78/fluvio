//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
	"github.com/software78/fluvio/internal/driver/postgres"
	"github.com/software78/fluvio/migrations"
)

func setupPostgresBench(b *testing.B) (*pgxpool.Pool, *postgres.Driver) {
	b.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("fluvio"),
		tcpostgres.WithUsername("fluvio"),
		tcpostgres.WithPassword("fluvio"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(b, err)
	b.Cleanup(func() { _ = container.Terminate(ctx) })

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(b, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(b, err)
	b.Cleanup(func() { pool.Close() })

	d := postgres.New(pool, postgres.Config{})
	require.NoError(b, d.Migrate(ctx))
	return pool, d
}

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

// countPostgresUpMigrations returns the number of up (.sql, not .down.sql) migration
// files embedded under migrations/postgres. Migration numbers 006, 007, and 009 are
// intentionally skipped and have no files, so they are not counted; see
// migrations/postgres/README.md.
func countPostgresUpMigrations(t *testing.T) int {
	t.Helper()
	entries, err := migrations.Postgres.ReadDir(migrations.PostgresDir)
	require.NoError(t, err)
	n := 0
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".down.sql") {
			continue
		}
		if strings.HasSuffix(name, ".sql") {
			n++
		}
	}
	return n
}

func TestMigrateUpDownStatus(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()
	expected := countPostgresUpMigrations(t)

	status, err := d.MigrationStatus(ctx)
	require.NoError(t, err)
	require.Len(t, status, expected)

	require.NoError(t, d.MigrateDown(ctx, 1))
	status, err = d.MigrationStatus(ctx)
	require.NoError(t, err)
	require.Len(t, status, expected-1)

	require.NoError(t, d.Migrate(ctx))
	status, err = d.MigrationStatus(ctx)
	require.NoError(t, err)
	require.Len(t, status, expected)
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

func TestNackErrorTraceTrimming(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	job, err := d.Enqueue(ctx, driver.EnqueueParams{
		Kind:        "trace_trim_job",
		Args:        []byte(`{}`),
		MaxAttempts: 50,
	})
	require.NoError(t, err)

	for i := 0; i < 30; i++ {
		_, err = d.TickScheduled(ctx, time.Now())
		require.NoError(t, err)

		jobs, err := d.Fetch(ctx, []string{"default"}, "w1", 1)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.Equal(t, job.ID, jobs[0].ID)

		require.NoError(t, d.Nack(ctx, jobs[0].ID, errTest, time.Now()))
	}

	var traceLen int
	err = d.Pool().QueryRow(ctx, `
		SELECT jsonb_array_length(COALESCE(error_trace, '[]'::jsonb))
		FROM fluvio_jobs WHERE id = $1
	`, job.ID).Scan(&traceLen)
	require.NoError(t, err)
	require.LessOrEqual(t, traceLen, 25)
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

func BenchmarkEnqueueMany(b *testing.B) {
	pool, d := setupPostgresBench(b)
	ctx := context.Background()

	const n = 1000
	params := make([]driver.EnqueueParams, n)
	for i := range params {
		params[i] = driver.EnqueueParams{Kind: "bulk_bench", Args: []byte(`{}`)}
	}

	b.Run("CopyFromStaging", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			jobs, err := d.EnqueueMany(ctx, params)
			require.NoError(b, err)
			require.Len(b, jobs, n, "EnqueueMany must return all inserted jobs via staging COPY + INSERT RETURNING")
			_, err = pool.Exec(ctx, `DELETE FROM fluvio_jobs`)
			require.NoError(b, err)
		}
	})

	b.Run("InsertLoop", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, err := d.EnqueueManyLoop(ctx, params)
			require.NoError(b, err)
			_, err = pool.Exec(ctx, `DELETE FROM fluvio_jobs`)
			require.NoError(b, err)
		}
	})
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
	require.True(t, jobs[0].NextRunAt.Equal(nextRun))

	updatedNextRun := time.Date(2030, 2, 1, 10, 0, 0, 0, time.UTC)
	require.NoError(t, d.UpsertPeriodicJob(ctx, "daily-report", "0 10 * * *", "critical", 5, []byte(`{"format":"csv"}`), updatedNextRun))
	jobs, err = d.ListPeriodicJobs(ctx)
	require.NoError(t, err)
	require.Equal(t, "0 10 * * *", jobs[0].Cron)
	require.Equal(t, "critical", jobs[0].Queue)
	require.Equal(t, int16(5), jobs[0].MaxAttempts)
	require.True(t, jobs[0].NextRunAt.Equal(updatedNextRun))

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

func assertSlotInvariant(t *testing.T, pool *pgxpool.Pool, kind string) {
	t.Helper()
	ctx := context.Background()

	var running int
	err := pool.QueryRow(ctx, `
		SELECT running FROM fluvio_concurrency_slots
		WHERE kind = $1 AND partition_key = ''
	`, kind).Scan(&running)
	require.NoError(t, err)

	var held int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM fluvio_jobs
		WHERE kind = $1 AND state = 'running' AND concurrency_slot_key IS NOT NULL
	`, kind).Scan(&held)
	require.NoError(t, err)
	require.Equal(t, held, running, "slot running count must match jobs holding slots")
}

func TestFetchMidCrashSlotConsistency(t *testing.T) {
	pool, d := setupPostgres(t)
	ctx := context.Background()
	const kind = "crash_job"

	require.NoError(t, d.SetConcurrencyLimit(ctx, driver.ConcurrencyLimit{
		Kind:          kind,
		MaxConcurrent: 1,
	}))
	_, err := d.Enqueue(ctx, driver.EnqueueParams{Kind: kind, Args: []byte(`{}`)})
	require.NoError(t, err)

	standalone, err := pgx.Connect(ctx, pool.Config().ConnString())
	require.NoError(t, err)
	defer standalone.Close(ctx)

	pid := standalone.PgConn().PID()
	globalKinds := []string{kind}
	errCh := make(chan error, 1)
	go func() {
		_, qerr := standalone.Query(ctx, postgres.FetchJobsSQLWithDelay(3),
			[]string{"default"}, 1, "crash-worker", globalKinds)
		errCh <- qerr
	}()

	time.Sleep(200 * time.Millisecond)
	_, err = pool.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid)
	require.NoError(t, err)

	select {
	case err = <-errCh:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for fetch to fail")
	}

	stuck, err := d.StuckJobs(ctx, time.Minute)
	require.NoError(t, err)
	for _, job := range stuck {
		require.NoError(t, d.Nack(ctx, job.ID, errTest, time.Now()))
	}

	assertSlotInvariant(t, pool, kind)

	var state string
	require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM fluvio_jobs WHERE kind = $1`, kind).Scan(&state))
	require.Equal(t, "pending", state)

	jobs, err := d.Fetch(ctx, []string{"default"}, "crash-worker", 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assertSlotInvariant(t, pool, kind)

	_, err = pool.Exec(ctx, `UPDATE fluvio_jobs SET attempted_at = now() - interval '1 hour' WHERE id = $1`, jobs[0].ID)
	require.NoError(t, err)

	stuck, err = d.StuckJobs(ctx, time.Minute)
	require.NoError(t, err)
	require.Len(t, stuck, 1)
	require.NoError(t, d.Nack(ctx, stuck[0].ID, errTest, time.Now()))

	assertSlotInvariant(t, pool, kind)
}

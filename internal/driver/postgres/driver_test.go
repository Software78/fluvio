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
	require.Len(t, status, 2)

	require.NoError(t, d.MigrateDown(ctx, 1))
	status, err = d.MigrationStatus(ctx)
	require.NoError(t, err)
	require.Len(t, status, 1)

	require.NoError(t, d.Migrate(ctx))
	status, err = d.MigrationStatus(ctx)
	require.NoError(t, err)
	require.Len(t, status, 2)
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

	require.NoError(t, d.Nack(ctx, jobs[0].ID, errTest, time.Now().Add(time.Second)))

	jobs, err = d.Fetch(ctx, []string{"default"}, "w1", 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	require.NoError(t, d.Nack(ctx, jobs[0].ID, errTest, time.Now()))

	got, err := d.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, "dead", got.State)
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

func TestPauseQueue(t *testing.T) {
	_, d := setupPostgres(t)
	ctx := context.Background()

	_, err := d.Enqueue(ctx, driver.EnqueueParams{Kind: "k", Args: []byte(`{}`)})
	require.NoError(t, err)
	require.NoError(t, d.PauseQueue(ctx, "default"))

	jobs, err := d.Fetch(ctx, []string{"default"}, "w1", 10)
	require.NoError(t, err)
	require.Empty(t, jobs)

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

	due := time.Now().Add(-time.Second)
	_, err = d.Enqueue(ctx, driver.EnqueueParams{
		Kind:        "due",
		Args:        []byte(`{}`),
		ScheduledAt: &due,
	})
	require.NoError(t, err)

	n, err = d.TickScheduled(ctx, time.Now())
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

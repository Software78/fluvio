//go:build integration

package scheduler_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/software78/fluvio/internal/driver/postgres"
	"github.com/software78/fluvio/internal/scheduler"
)

func setupPeriodicIntegration(t *testing.T) (*pgxpool.Pool, *postgres.Driver) {
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

func TestPeriodicRegisterPersistsRow(t *testing.T) {
	pool, d := setupPeriodicIntegration(t)
	ctx := context.Background()

	p := scheduler.NewPeriodic(d, slog.Default(), time.Minute)
	require.NoError(t, p.Register(ctx, "0 9 * * *", "daily-report", []byte(`{"format":"pdf"}`), "critical", 5))

	var kind, cron, queue string
	var maxAttempts int16
	var args []byte
	err := pool.QueryRow(ctx, `
		SELECT kind, cron, queue, max_attempts, args
		FROM fluvio_periodic_jobs WHERE kind = $1
	`, "daily-report").Scan(&kind, &cron, &queue, &maxAttempts, &args)
	require.NoError(t, err)
	require.Equal(t, "daily-report", kind)
	require.Equal(t, "0 9 * * *", cron)
	require.Equal(t, "critical", queue)
	require.Equal(t, int16(5), maxAttempts)
	require.JSONEq(t, `{"format":"pdf"}`, string(args))
}

func TestPeriodicTickEnqueuesAndAdvances(t *testing.T) {
	pool, d := setupPeriodicIntegration(t)
	ctx := context.Background()

	p := scheduler.NewPeriodic(d, slog.Default(), time.Minute)
	require.NoError(t, p.Register(ctx, "* * * * *", "heartbeat", []byte(`{}`), "default", 3))

	past := time.Now().Add(-time.Hour)
	_, err := pool.Exec(ctx, `UPDATE fluvio_periodic_jobs SET next_run_at = $1 WHERE kind = $2`, past, "heartbeat")
	require.NoError(t, err)

	now := time.Now().UTC()
	p.Tick(ctx, now)

	var jobCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM fluvio_jobs WHERE kind = $1`, "heartbeat").Scan(&jobCount)
	require.NoError(t, err)
	require.Equal(t, 1, jobCount)

	var nextRunAt time.Time
	err = pool.QueryRow(ctx, `SELECT next_run_at FROM fluvio_periodic_jobs WHERE kind = $1`, "heartbeat").Scan(&nextRunAt)
	require.NoError(t, err)
	require.True(t, nextRunAt.After(now))
}

func TestPeriodicTickNoDoubleEnqueue(t *testing.T) {
	pool, d := setupPeriodicIntegration(t)
	ctx := context.Background()

	p := scheduler.NewPeriodic(d, slog.Default(), time.Minute)
	require.NoError(t, p.Register(ctx, "* * * * *", "once", []byte(`{}`), "default", 3))

	past := time.Now().Add(-time.Hour)
	_, err := pool.Exec(ctx, `UPDATE fluvio_periodic_jobs SET next_run_at = $1 WHERE kind = $2`, past, "once")
	require.NoError(t, err)

	now := time.Now().UTC()
	p.Tick(ctx, now)
	p.Tick(ctx, now)

	var jobCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM fluvio_jobs WHERE kind = $1`, "once").Scan(&jobCount)
	require.NoError(t, err)
	require.Equal(t, 1, jobCount)
}

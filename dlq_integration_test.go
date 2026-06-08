//go:build integration

package fluvio_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	fluvio "github.com/software78/fluvio"
	"github.com/software78/fluvio/postgres"
)

func setupDLQIntegration(t *testing.T) (*pgxpool.Pool, *fluvio.Client) {
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

	workers := fluvio.NewWorkers()
	fluvio.AddWorker(workers, FailWorker{})

	client, err := fluvio.NewClient(d, &fluvio.Config{
		Queues: map[string]fluvio.QueueConfig{
			fluvio.QueueDefault: {MaxWorkers: 1},
		},
		Workers: workers,
	})
	require.NoError(t, err)

	return pool, client
}

func TestDLQExhaustReplayPurge(t *testing.T) {
	pool, client := setupDLQIntegration(t)
	ctx := context.Background()

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { client.Stop() })

	row, err := client.Enqueue(ctx, FailArgs{}, fluvio.WithMaxAttempts(1))
	require.NoError(t, err)
	jobID := row.ID

	require.Eventually(t, func() bool {
		job, err := client.GetJob(ctx, jobID)
		if err != nil {
			return false
		}
		if job.State != fluvio.JobStateDead {
			return false
		}
		var deadCount int
		err = pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM fluvio_dead_jobs
			WHERE id = $1
		`, jobID).Scan(&deadCount)
		return err == nil && deadCount == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Stop workers so the replayed job stays in 'pending' for inspection.
	client.Stop()

	require.NoError(t, client.ReplayDeadJob(ctx, jobID))

	var newJobID int64
	var attempt int16
	var maxAttempts int16
	err = pool.QueryRow(ctx, `
		SELECT id, attempt, max_attempts
		FROM fluvio_jobs
		WHERE state = 'pending'
		  AND kind = $1
		ORDER BY id DESC
		LIMIT 1
	`, "fail").Scan(&newJobID, &attempt, &maxAttempts)
	require.NoError(t, err)
	require.NotEqual(t, jobID, newJobID)
	require.Equal(t, int16(0), attempt)
	require.Equal(t, int16(1), maxAttempts)

	// Purge dead jobs before now; we expect the original dead row is older than 'before'.
	time.Sleep(20 * time.Millisecond)
	before := time.Now()
	purged, err := client.PurgeDeadJobs(ctx, before)
	require.NoError(t, err)
	require.Equal(t, int64(1), purged)

	var deadCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM fluvio_dead_jobs
		WHERE id = $1
	`, jobID).Scan(&deadCount)
	require.NoError(t, err)
	require.Equal(t, 0, deadCount)
}

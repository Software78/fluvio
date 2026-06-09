//go:build integration

package fluvio_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	fluvio "github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
	"github.com/software78/fluvio/postgres"
)

type seqStepArgs struct {
	Step int `json:"step"`
}

func (a seqStepArgs) Kind() string { return "seq-step" }

type seqStepWorker struct{ fluvio.WorkerDefaults[seqStepArgs] }

func (seqStepWorker) Work(ctx context.Context, job *fluvio.Job[seqStepArgs]) error {
	return nil
}

type seqJob struct {
	ID          int64
	SequencePos int
	State       string
}

func setupSequenceIntegration(t *testing.T) (*pgxpool.Pool, *fluvio.Client, driver.Driver) {
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
	fluvio.AddWorker(workers, seqStepWorker{})

	client, err := fluvio.NewClient(d, &fluvio.Config{
		Queues: map[string]fluvio.QueueConfig{
			fluvio.QueueDefault: {MaxWorkers: 4},
		},
		Workers: workers,
	})
	require.NoError(t, err)

	return pool, client, d
}

func jobsForSequence(t *testing.T, pool *pgxpool.Pool, seqID string) []seqJob {
	t.Helper()
	ctx := context.Background()
	rows, err := pool.Query(ctx, `
		SELECT id, sequence_pos, state
		FROM fluvio_jobs
		WHERE sequence_id = $1
		ORDER BY sequence_pos
	`, seqID)
	require.NoError(t, err)
	defer rows.Close()

	var jobs []seqJob
	for rows.Next() {
		var j seqJob
		require.NoError(t, rows.Scan(&j.ID, &j.SequencePos, &j.State))
		jobs = append(jobs, j)
	}
	require.NoError(t, rows.Err())
	return jobs
}

func fetchOne(t *testing.T, ctx context.Context, d driver.Driver) *driver.Job {
	t.Helper()
	jobs, err := d.Fetch(ctx, []string{fluvio.QueueDefault}, "seq-test", 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	return jobs[0]
}

func sequencePosForJob(t *testing.T, pool *pgxpool.Pool, jobID int64) int {
	t.Helper()
	var pos int
	err := pool.QueryRow(context.Background(), `
		SELECT sequence_pos FROM fluvio_jobs WHERE id = $1
	`, jobID).Scan(&pos)
	require.NoError(t, err)
	return pos
}

func fetchAndAckStep(t *testing.T, ctx context.Context, pool *pgxpool.Pool, d driver.Driver, pos int) {
	t.Helper()
	job := fetchOne(t, ctx, d)
	require.Equal(t, pos, sequencePosForJob(t, pool, job.ID))
	require.NoError(t, d.Ack(ctx, job.ID))
}

func TestSequenceLifecycle(t *testing.T) {
	pool, client, d := setupSequenceIntegration(t)
	ctx := context.Background()

	seqID, err := client.EnqueueSequence(ctx, []fluvio.EnqueueItem{
		{Args: seqStepArgs{Step: 0}},
		{Args: seqStepArgs{Step: 1}},
		{Args: seqStepArgs{Step: 2}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, seqID)

	jobs := jobsForSequence(t, pool, seqID)
	require.Len(t, jobs, 3)
	require.Equal(t, "pending", jobs[0].State)
	require.Equal(t, "scheduled", jobs[1].State)
	require.Equal(t, "scheduled", jobs[2].State)

	job := fetchOne(t, ctx, d)
	require.Equal(t, 0, sequencePosForJob(t, pool, job.ID))

	noJobs, err := d.Fetch(ctx, []string{fluvio.QueueDefault}, "seq-test", 1)
	require.NoError(t, err)
	require.Empty(t, noJobs)

	require.NoError(t, d.Ack(ctx, job.ID))
	jobs = jobsForSequence(t, pool, seqID)
	require.Equal(t, "completed", jobs[0].State)
	require.Equal(t, "pending", jobs[1].State)
	require.Equal(t, "scheduled", jobs[2].State)

	fetchAndAckStep(t, ctx, pool, d, 1)
	jobs = jobsForSequence(t, pool, seqID)
	require.Equal(t, "completed", jobs[1].State)
	require.Equal(t, "pending", jobs[2].State)

	fetchAndAckStep(t, ctx, pool, d, 2)
	jobs = jobsForSequence(t, pool, seqID)
	require.Equal(t, "completed", jobs[0].State)
	require.Equal(t, "completed", jobs[1].State)
	require.Equal(t, "completed", jobs[2].State)

	noJobs, err = d.Fetch(ctx, []string{fluvio.QueueDefault}, "seq-test", 1)
	require.NoError(t, err)
	require.Empty(t, noJobs)
}

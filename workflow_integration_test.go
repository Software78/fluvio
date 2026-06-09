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
	"github.com/software78/fluvio/internal/driver"
	"github.com/software78/fluvio/postgres"
)

type wfTaskArgs struct {
	Task string `json:"task"`
}

func (a wfTaskArgs) Kind() string { return "wf-task" }

func setupWorkflowIntegration(t *testing.T) (*pgxpool.Pool, *fluvio.Client, driver.Driver) {
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
	fluvio.AddWorker(workers, &wfTaskWorker{})
	fluvio.AddWorker(workers, wfFailWorker{})
	fluvio.AddWorker(workers, wfSlowWorker{})

	client, err := fluvio.NewClient(d, &fluvio.Config{
		Queues: map[string]fluvio.QueueConfig{
			fluvio.QueueDefault: {MaxWorkers: 4},
		},
		Workers: workers,
	})
	require.NoError(t, err)

	return pool, client, d
}

type wfTaskWorker struct{ fluvio.WorkerDefaults[wfTaskArgs] }

func (wfTaskWorker) Work(ctx context.Context, job *fluvio.Job[wfTaskArgs]) error {
	return nil
}

type wfFailArgs struct{}

func (wfFailArgs) Kind() string { return "wf-fail" }

type wfFailWorker struct{ fluvio.WorkerDefaults[wfFailArgs] }

func (wfFailWorker) Work(ctx context.Context, job *fluvio.Job[wfFailArgs]) error {
	return context.Canceled
}

type wfSlowArgs struct {
	Task string `json:"task"`
}

func (wfSlowArgs) Kind() string { return "wf-slow" }

type wfSlowWorker struct{ fluvio.WorkerDefaults[wfSlowArgs] }

func (wfSlowWorker) Work(ctx context.Context, _ *fluvio.Job[wfSlowArgs]) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return nil
	}
}

func taskState(wf *driver.WorkflowState, taskID string) string {
	for _, t := range wf.Tasks {
		if t.TaskID == taskID {
			return t.State
		}
	}
	return ""
}

func jobIDForTask(wf *driver.WorkflowState, taskID string) int64 {
	for _, t := range wf.Tasks {
		if t.TaskID == taskID && t.JobID != nil {
			return *t.JobID
		}
	}
	return 0
}

func fetchAndAck(t *testing.T, ctx context.Context, d driver.Driver, jobID int64) {
	t.Helper()
	jobs, err := d.Fetch(ctx, []string{fluvio.QueueDefault}, "wf-test", 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, jobID, jobs[0].ID)
	require.NoError(t, d.Ack(ctx, jobID, nil))
}

func diamondWorkflow() *fluvio.Workflow {
	return fluvio.NewWorkflow().
		Task("A", wfTaskArgs{Task: "A"}).
		Task("B", wfTaskArgs{Task: "B"}, fluvio.WithDependsOn("A")).
		Task("C", wfTaskArgs{Task: "C"}, fluvio.WithDependsOn("A")).
		Task("D", wfTaskArgs{Task: "D"}, fluvio.WithDependsOn("B", "C"))
}

func TestWorkflowDiamondDAG(t *testing.T) {
	_, client, d := setupWorkflowIntegration(t)
	ctx := context.Background()

	wfID, err := client.EnqueueWorkflow(ctx, diamondWorkflow())
	require.NoError(t, err)

	wf, err := client.GetWorkflow(ctx, wfID)
	require.NoError(t, err)
	require.Equal(t, "running", wf.State)
	require.Equal(t, "pending", taskState(wf, "A"))
	require.NotZero(t, jobIDForTask(wf, "A"))
	require.Equal(t, "waiting", taskState(wf, "B"))
	require.Equal(t, "waiting", taskState(wf, "C"))
	require.Equal(t, "waiting", taskState(wf, "D"))

	fetchAndAck(t, ctx, d, jobIDForTask(wf, "A"))
	wf, err = client.GetWorkflow(ctx, wfID)
	require.NoError(t, err)
	require.Equal(t, "pending", taskState(wf, "B"))
	require.Equal(t, "pending", taskState(wf, "C"))
	require.Equal(t, "waiting", taskState(wf, "D"))

	fetchAndAck(t, ctx, d, jobIDForTask(wf, "B"))
	wf, err = client.GetWorkflow(ctx, wfID)
	require.NoError(t, err)
	require.Equal(t, "waiting", taskState(wf, "D"))

	fetchAndAck(t, ctx, d, jobIDForTask(wf, "C"))
	wf, err = client.GetWorkflow(ctx, wfID)
	require.NoError(t, err)
	require.Equal(t, "pending", taskState(wf, "D"))

	fetchAndAck(t, ctx, d, jobIDForTask(wf, "D"))
	wf, err = client.GetWorkflow(ctx, wfID)
	require.NoError(t, err)
	require.Equal(t, "completed", wf.State)
}

func TestWorkflowFailCancelsDependents(t *testing.T) {
	_, client, _ := setupWorkflowIntegration(t)
	ctx := context.Background()

	wf := fluvio.NewWorkflow().
		Task("A", wfFailArgs{}, fluvio.WithTaskEnqueueOptions(fluvio.WithMaxAttempts(1))).
		Task("B", wfTaskArgs{Task: "B"}, fluvio.WithDependsOn("A")).
		Task("C", wfTaskArgs{Task: "C"}, fluvio.WithDependsOn("A")).
		Task("D", wfTaskArgs{Task: "D"}, fluvio.WithDependsOn("B", "C"))

	wfID, err := client.EnqueueWorkflow(ctx, wf)
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { client.Stop() })

	require.Eventually(t, func() bool {
		wfState, err := client.GetWorkflow(ctx, wfID)
		if err != nil {
			return false
		}
		return wfState.State == "failed" &&
			taskState(wfState, "B") == "cancelled" &&
			taskState(wfState, "C") == "cancelled" &&
			taskState(wfState, "D") == "cancelled"
	}, 10*time.Second, 100*time.Millisecond)
}

func TestWorkflowFailCancelsParallelSiblings(t *testing.T) {
	_, client, _ := setupWorkflowIntegration(t)
	ctx := context.Background()

	wf := fluvio.NewWorkflow().
		Task("A", wfFailArgs{}, fluvio.WithTaskEnqueueOptions(fluvio.WithMaxAttempts(1))).
		Task("B", wfSlowArgs{Task: "B"})

	wfID, err := client.EnqueueWorkflow(ctx, wf)
	require.NoError(t, err)

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { client.Stop() })

	var slowJobID int64
	require.Eventually(t, func() bool {
		wfState, err := client.GetWorkflow(ctx, wfID)
		if err != nil {
			return false
		}
		if wfState.State != "failed" {
			return false
		}
		slowJobID = jobIDForTask(wfState, "B")
		if slowJobID == 0 {
			return false
		}
		bState := taskState(wfState, "B")
		return bState == "running" || bState == "completed"
	}, 10*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		wfState, err := client.GetWorkflow(ctx, wfID)
		if err != nil {
			return false
		}
		if wfState.State != "failed" || taskState(wfState, "B") != "completed" {
			return false
		}
		job, err := client.GetJob(ctx, slowJobID)
		if err != nil {
			return false
		}
		return job.State == fluvio.JobStateCompleted || job.State == fluvio.JobStateDead
	}, 15*time.Second, 100*time.Millisecond)
}

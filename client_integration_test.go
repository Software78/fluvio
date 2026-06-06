//go:build integration

package fluvio_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	fluvio "github.com/software78/fluvio"
	"github.com/software78/fluvio/postgres"
)

type HelloArgs struct {
	Name string `json:"name"`
}

func (HelloArgs) Kind() string { return "hello" }

type HelloWorker struct {
	fluvio.WorkerDefaults[HelloArgs]
	done chan int64
}

func (w *HelloWorker) Work(ctx context.Context, job *fluvio.Job[HelloArgs]) error {
	w.done <- job.ID
	return nil
}

func setupIntegration(t *testing.T) (*pgxpool.Pool, *fluvio.Client, *HelloWorker) {
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
	hw := &HelloWorker{done: make(chan int64, 4)}
	fluvio.AddWorker(workers, hw)

	client, err := fluvio.NewClient(d, &fluvio.Config{
		Queues: map[string]fluvio.QueueConfig{
			fluvio.QueueDefault: {MaxWorkers: 5},
		},
		Workers: workers,
	})
	require.NoError(t, err)
	return pool, client, hw
}

func TestClientLifecycle(t *testing.T) {
	_, client, hw := setupIntegration(t)
	ctx := context.Background()

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { client.Stop() })

	row, err := client.Enqueue(ctx, HelloArgs{Name: "world"})
	require.NoError(t, err)

	select {
	case id := <-hw.done:
		require.Equal(t, row.ID, id)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for job")
	}

	job, err := client.GetJob(ctx, row.ID)
	require.NoError(t, err)
	require.Equal(t, fluvio.JobStateCompleted, job.State)
}

func TestTransactionalEnqueue(t *testing.T) {
	pool, client, _ := setupIntegration(t)
	ctx := context.Background()
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { client.Stop() })

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)

	row, err := client.EnqueueTx(ctx, tx, HelloArgs{Name: "tx"})
	require.NoError(t, err)

	_, err = client.GetJob(ctx, row.ID)
	require.ErrorIs(t, err, fluvio.ErrJobNotFound)

	require.NoError(t, tx.Commit(ctx))

	job, err := client.GetJob(ctx, row.ID)
	require.NoError(t, err)
	require.Equal(t, "hello", job.Kind)
}

func TestMiddlewareAndErrorHandler(t *testing.T) {
	pool, _, _ := setupIntegration(t)
	ctx := context.Background()

	var mu sync.Mutex
	var middlewareOrder []string
	var errorHandled atomic.Bool

	workers := fluvio.NewWorkers()
	fluvio.AddWorker(workers, &FailWorker{})

	d := postgres.New(pool, postgres.Config{})
	client, err := fluvio.NewClient(d, &fluvio.Config{
		Queues:  map[string]fluvio.QueueConfig{fluvio.QueueDefault: {MaxWorkers: 2}},
		Workers: workers,
		Middleware: []fluvio.JobMiddleware{
			func(next func(context.Context) error) func(context.Context) error {
				return func(ctx context.Context) error {
					mu.Lock()
					middlewareOrder = append(middlewareOrder, "mw1")
					mu.Unlock()
					return next(ctx)
				}
			},
		},
		ErrorHandler: func(ctx context.Context, job fluvio.JobRow, err error) {
			errorHandled.Store(true)
		},
	})
	require.NoError(t, err)
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { client.Stop() })

	_, err = client.Enqueue(ctx, FailArgs{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(middlewareOrder) == 1 && errorHandled.Load()
	}, 5*time.Second, 100*time.Millisecond)
}

func TestUniqueEnqueue(t *testing.T) {
	_, client, _ := setupIntegration(t)
	ctx := context.Background()

	key := "user:1"
	_, err := client.Enqueue(ctx, HelloArgs{Name: "a"}, fluvio.WithUniqueKey(key))
	require.NoError(t, err)

	_, err = client.Enqueue(ctx, HelloArgs{Name: "b"}, fluvio.WithUniqueKey(key))
	require.True(t, errors.Is(err, fluvio.ErrUniqueConflict))
}

type FailArgs struct{}

func (FailArgs) Kind() string { return "fail" }

type FailWorker struct{ fluvio.WorkerDefaults[FailArgs] }

func (FailWorker) Work(ctx context.Context, job *fluvio.Job[FailArgs]) error {
	return errFail
}

var errFail = failErr{}

type failErr struct{}

func (failErr) Error() string { return "fail" }

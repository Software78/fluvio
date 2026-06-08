//go:build integration

package fluvio_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	fluvio "github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
	"github.com/software78/fluvio/postgres"
)

type SlowJobArgs struct{}

func (SlowJobArgs) Kind() string { return "slow_job" }

type SlowWorker struct {
	fluvio.WorkerDefaults[SlowJobArgs]

	done           chan struct{}
	running        atomic.Int32
	peakConcurrent atomic.Int32
}

func (w *SlowWorker) Work(ctx context.Context, _ *fluvio.Job[SlowJobArgs]) error {
	cur := w.running.Add(1)
	defer w.running.Add(-1)

	for {
		peak := w.peakConcurrent.Load()
		if cur <= peak {
			break
		}
		if w.peakConcurrent.CompareAndSwap(peak, cur) {
			break
		}
	}

	time.Sleep(200 * time.Millisecond)
	w.done <- struct{}{}
	return nil
}

func setupConcurrencyIntegration(t *testing.T) (*pgxpool.Pool, *fluvio.Client, *SlowWorker) {
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
	sw := &SlowWorker{done: make(chan struct{}, 8)}
	fluvio.AddWorker(workers, sw)

	client, err := fluvio.NewClient(d, &fluvio.Config{
		Queues: map[string]fluvio.QueueConfig{
			fluvio.QueueDefault: {MaxWorkers: 6},
		},
		Workers: workers,
	})
	require.NoError(t, err)

	require.NoError(t, client.SetConcurrencyLimit(ctx, fluvio.ConcurrencyLimitConfig{
		Kind:          "slow_job",
		MaxConcurrent: 2,
	}))

	return pool, client, sw
}

func TestConcurrencyLimitPeak(t *testing.T) {
	_, client, sw := setupConcurrencyIntegration(t)
	ctx := context.Background()

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { client.Stop() })

	const jobCount = 6
	for i := 0; i < jobCount; i++ {
		_, err := client.Enqueue(ctx, SlowJobArgs{})
		require.NoError(t, err)
	}

	for i := 0; i < jobCount; i++ {
		select {
		case <-sw.done:
		case <-time.After(30 * time.Second):
			t.Fatal("timeout waiting for slow_job")
		}
	}

	require.LessOrEqual(t, int(sw.peakConcurrent.Load()), 2,
		"peak concurrent slow_job executions must not exceed limit")
}

func TestConcurrentFetchNoConcurrencyOvershoot(t *testing.T) {
	pool, client, _ := setupConcurrencyIntegration(t)
	ctx := context.Background()

	d := postgres.New(pool, postgres.Config{})
	require.NoError(t, d.SetConcurrencyLimit(ctx, driver.ConcurrencyLimit{
		Kind:          "slow_job",
		MaxConcurrent: 2,
	}))

	for i := 0; i < 10; i++ {
		_, err := client.Enqueue(ctx, SlowJobArgs{})
		require.NoError(t, err)
	}

	for i := 0; i < 2; i++ {
		jobs, err := d.Fetch(ctx, []string{fluvio.QueueDefault}, "pre-fill", 1)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
	}

	var peakRunning atomic.Int32
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				var count int
				err := pool.QueryRow(ctx, `
					SELECT COUNT(*) FROM fluvio_jobs
					WHERE kind = 'slow_job' AND state = 'running'
				`).Scan(&count)
				if err == nil {
					for {
						old := peakRunning.Load()
						if int32(count) <= old {
							break
						}
						if peakRunning.CompareAndSwap(old, int32(count)) {
							break
						}
					}
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, err := d.Fetch(ctx, []string{fluvio.QueueDefault}, fmt.Sprintf("w-%d", id), 5)
				require.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()
	close(stop)

	require.LessOrEqual(t, int(peakRunning.Load()), 2,
		"peak running slow_job count must not exceed concurrency limit during concurrent fetch")
}

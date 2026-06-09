package fluvio_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	fluvio "github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
)

type concurrencyRegisterCall struct {
	kind          string
	maxConcurrent int
	partitioned   bool
}

type concurrencyRecordingDriver struct {
	driver.NoopDriver
	mu    sync.Mutex
	calls []concurrencyRegisterCall
}

func (d *concurrencyRecordingDriver) RegisterConcurrencyLimit(kind string, maxConcurrent int, partitioned bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, concurrencyRegisterCall{kind, maxConcurrent, partitioned})
}

func (d *concurrencyRecordingDriver) registerCalls() []concurrencyRegisterCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]concurrencyRegisterCall, len(d.calls))
	copy(out, d.calls)
	return out
}

func (d *concurrencyRecordingDriver) SetConcurrencyLimit(_ context.Context, limit driver.ConcurrencyLimit) error {
	d.RegisterConcurrencyLimit(limit.Kind, limit.MaxConcurrent, false)
	return nil
}

func newConcurrencyTestClient(t *testing.T, d driver.Driver) *fluvio.Client {
	t.Helper()
	client, err := fluvio.NewClient(d, &fluvio.Config{Workers: fluvio.NewWorkers()})
	require.NoError(t, err)
	return client
}

func TestSetConcurrencyLimitRegistersInMemoryLimit(t *testing.T) {
	t.Parallel()

	d := &concurrencyRecordingDriver{}
	client := newConcurrencyTestClient(t, d)

	err := client.SetConcurrencyLimit(context.Background(), fluvio.ConcurrencyLimitConfig{
		Kind:          "slow_job",
		MaxConcurrent: 2,
	})
	require.NoError(t, err)

	calls := d.registerCalls()
	require.Len(t, calls, 2, "SetConcurrencyLimit persists via driver then registers in-memory limit")
	require.Equal(t, concurrencyRegisterCall{"slow_job", 2, false}, calls[0])
	require.Equal(t, concurrencyRegisterCall{"slow_job", 2, false}, calls[1])
}

type stopLifecycleArgs struct{}

func (stopLifecycleArgs) Kind() string { return "stop_lifecycle" }

type stopLifecycleWorker struct {
	fluvio.WorkerDefaults[stopLifecycleArgs]
	started chan struct{}
}

func (w *stopLifecycleWorker) Work(ctx context.Context, _ *fluvio.Job[stopLifecycleArgs]) error {
	select {
	case w.started <- struct{}{}:
	default:
	}
	time.Sleep(500 * time.Millisecond)
	return nil
}

type stopLifecycleDriver struct {
	driver.NoopDriver
	mu      sync.Mutex
	pending []*driver.Job
	nextID  int64
	ackCh   chan int64
}

func (d *stopLifecycleDriver) Enqueue(_ context.Context, p driver.EnqueueParams) (*driver.Job, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nextID++
	job := &driver.Job{
		ID:          d.nextID,
		Queue:       p.Queue,
		Kind:        p.Kind,
		Args:        p.Args,
		State:       "available",
		MaxAttempts: p.MaxAttempts,
	}
	if job.Queue == "" {
		job.Queue = driver.QueueDefault
	}
	if job.MaxAttempts == 0 {
		job.MaxAttempts = 25
	}
	d.pending = append(d.pending, job)
	return job, nil
}

func (d *stopLifecycleDriver) Fetch(ctx context.Context, _ []string, _ string, max int) ([]*driver.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.pending) == 0 {
		return nil, nil
	}
	n := max
	if n > len(d.pending) {
		n = len(d.pending)
	}
	out := make([]*driver.Job, n)
	copy(out, d.pending[:n])
	d.pending = d.pending[n:]
	return out, nil
}

func (d *stopLifecycleDriver) Ack(_ context.Context, id int64, _ []byte) error {
	if d.ackCh != nil {
		select {
		case d.ackCh <- id:
		default:
		}
	}
	return nil
}

type blockingStopArgs struct{}

func (blockingStopArgs) Kind() string { return "blocking_stop" }

type blockingStopWorker struct {
	fluvio.WorkerDefaults[blockingStopArgs]
	started chan struct{}
	done    chan struct{}
}

func (w *blockingStopWorker) Work(ctx context.Context, _ *fluvio.Job[blockingStopArgs]) error {
	close(w.started)
	<-ctx.Done()
	close(w.done)
	return ctx.Err()
}

func TestStopCancelsContextAwareWorker(t *testing.T) {
	t.Parallel()

	d := &stopLifecycleDriver{}
	bw := &blockingStopWorker{
		started: make(chan struct{}),
		done:    make(chan struct{}),
	}
	workers := fluvio.NewWorkers()
	fluvio.AddWorker(workers, bw)

	client, err := fluvio.NewClient(d, &fluvio.Config{
		Queues: map[string]fluvio.QueueConfig{
			fluvio.QueueDefault: {MaxWorkers: 1},
		},
		Workers:       workers,
		FetchInterval: 5 * time.Millisecond,
		PollOnly:      true,
	})
	require.NoError(t, err)

	ctx := context.Background()
	_, err = client.Enqueue(ctx, blockingStopArgs{})
	require.NoError(t, err)
	require.NoError(t, client.Start(ctx))

	<-bw.started

	stopDone := make(chan struct{})
	go func() {
		client.Stop()
		close(stopDone)
	}()

	select {
	case <-bw.done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker did not unblock within 500ms of Stop")
	}

	select {
	case <-stopDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stop did not return within 500ms")
	}
}

func TestClientStopContextTimeout(t *testing.T) {
	t.Parallel()

	d := &stopLifecycleDriver{ackCh: make(chan int64, 1)}
	started := make(chan struct{}, 1)
	workers := fluvio.NewWorkers()
	fluvio.AddWorker(workers, &stopLifecycleWorker{started: started})

	client, err := fluvio.NewClient(d, &fluvio.Config{
		Queues: map[string]fluvio.QueueConfig{
			fluvio.QueueDefault: {MaxWorkers: 1},
		},
		Workers:       workers,
		FetchInterval: 5 * time.Millisecond,
		PollOnly:      true,
	})
	require.NoError(t, err)

	ctx := context.Background()
	_, err = client.Enqueue(ctx, stopLifecycleArgs{})
	require.NoError(t, err)
	require.NoError(t, client.Start(ctx))

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow job did not start")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = client.StopContext(stopCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case <-d.ackCh:
	case <-time.After(2 * time.Second):
		t.Fatal("job was not acked after stop timeout")
	}
}

func TestSetConcurrencyLimitRegistersPartitionedLimit(t *testing.T) {
	t.Parallel()

	d := &concurrencyRecordingDriver{}
	client := newConcurrencyTestClient(t, d)

	partitionFn := func([]byte) string { return "tenant-a" }
	err := client.SetConcurrencyLimit(context.Background(), fluvio.ConcurrencyLimitConfig{
		Kind:           "tenant_job",
		MaxConcurrent:  3,
		PartitionKeyFn: partitionFn,
	})
	require.NoError(t, err)

	calls := d.registerCalls()
	require.Len(t, calls, 2)
	require.Equal(t, concurrencyRegisterCall{"tenant_job", 3, false}, calls[0])
	require.Equal(t, concurrencyRegisterCall{"tenant_job", 3, true}, calls[1])
}

package executor_test

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/software78/fluvio/internal/driver"
	"github.com/software78/fluvio/internal/executor"
)

type fakeDriver struct {
	mu         sync.Mutex
	fetch      []*driver.Job
	fetchCount atomic.Int32
}

func (f *fakeDriver) Fetch(_ context.Context, _ []string, _ string, max int) ([]*driver.Job, error) {
	f.fetchCount.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.fetch) == 0 {
		return nil, nil
	}
	n := max
	if n > len(f.fetch) {
		n = len(f.fetch)
	}
	out := f.fetch[:n]
	f.fetch = f.fetch[n:]
	return out, nil
}

func (f *fakeDriver) Ack(context.Context, int64) error                    { return nil }
func (f *fakeDriver) Nack(context.Context, int64, error, time.Time) error { return nil }
func (f *fakeDriver) Enqueue(context.Context, driver.EnqueueParams) (*driver.Job, error) {
	return nil, nil
}
func (f *fakeDriver) EnqueueTx(context.Context, driver.Tx, driver.EnqueueParams) (*driver.Job, error) {
	return nil, nil
}
func (f *fakeDriver) EnqueueMany(context.Context, []driver.EnqueueParams) ([]*driver.Job, error) {
	return nil, nil
}
func (f *fakeDriver) Cancel(context.Context, int64) error                { return nil }
func (f *fakeDriver) GetJob(context.Context, int64) (*driver.Job, error) { return nil, nil }
func (f *fakeDriver) ListJobs(context.Context, driver.ListJobsParams) ([]*driver.Job, error) {
	return nil, nil
}
func (f *fakeDriver) ListDead(context.Context, int, int) ([]*driver.Job, error) { return nil, nil }
func (f *fakeDriver) ReplayDead(context.Context, int64) error                   { return nil }
func (f *fakeDriver) PurgeDead(context.Context, time.Time) (int64, error)       { return 0, nil }
func (f *fakeDriver) TickScheduled(context.Context, time.Time) (int64, error)   { return 0, nil }
func (f *fakeDriver) UpsertPeriodicJob(context.Context, string, string, string, int16, []byte, time.Time) error {
	return nil
}
func (f *fakeDriver) DuePeriodicJobs(context.Context, time.Time) ([]*driver.PeriodicJob, error) {
	return nil, nil
}
func (f *fakeDriver) UpdatePeriodicJobNextRun(context.Context, string, time.Time) error { return nil }
func (f *fakeDriver) UpdatePeriodicJobNextRunTx(context.Context, driver.Tx, string, time.Time) (bool, error) {
	return false, nil
}
func (f *fakeDriver) ListPeriodicJobs(context.Context) ([]*driver.PeriodicJob, error) { return nil, nil }
func (f *fakeDriver) PausePeriodicJob(context.Context, string) error                    { return nil }
func (f *fakeDriver) ResumePeriodicJob(context.Context, string) error                   { return nil }
func (f *fakeDriver) BeginTx(context.Context) (driver.Tx, error)                      { return nil, nil }
func (f *fakeDriver) CommitTx(context.Context, driver.Tx) error                         { return nil }
func (f *fakeDriver) RollbackTx(context.Context, driver.Tx) error                      { return nil }
func (f *fakeDriver) UniqueJobExists(context.Context, string) (bool, error)     { return false, nil }
func (f *fakeDriver) PauseQueue(context.Context, string) error                  { return nil }
func (f *fakeDriver) ResumeQueue(context.Context, string) error                 { return nil }
func (f *fakeDriver) IsQueuePaused(context.Context, string) (bool, error)       { return false, nil }
func (f *fakeDriver) QueueStats(context.Context, string) (*driver.QueueStats, error) {
	return nil, nil
}
func (f *fakeDriver) ListQueues(context.Context) ([]*driver.QueueStats, error) { return nil, nil }
func (f *fakeDriver) TryAcquireLeader(context.Context) (bool, error)           { return false, nil }
func (f *fakeDriver) VerifyLeader(context.Context) error                         { return nil }
func (f *fakeDriver) ReleaseLeader(context.Context) error                      { return nil }
func (f *fakeDriver) StuckJobs(context.Context, time.Duration) ([]*driver.Job, error) {
	return nil, nil
}
func (f *fakeDriver) UpsertWorker(context.Context, string, map[string]int) error { return nil }
func (f *fakeDriver) RemoveWorker(context.Context, string) error                 { return nil }
func (f *fakeDriver) ListWorkers(context.Context, time.Duration) ([]*driver.WorkerInstance, error) {
	return nil, nil
}
func (f *fakeDriver) Migrate(context.Context) error                     { return nil }
func (f *fakeDriver) MigrateDown(context.Context, int) error            { return nil }
func (f *fakeDriver) MigrationStatus(context.Context) ([]string, error) { return nil, nil }
func (f *fakeDriver) SetConcurrencyLimit(context.Context, driver.ConcurrencyLimit) error {
	return nil
}
func (f *fakeDriver) AcquireConcurrencySlot(context.Context, string, string) (bool, error) {
	return true, nil
}
func (f *fakeDriver) AcquireConcurrencySlotForJob(context.Context, int64, string, string) (bool, error) {
	return true, nil
}
func (f *fakeDriver) ReleaseConcurrencySlot(context.Context, string, string) error { return nil }
func (f *fakeDriver) SetConcurrencySlotKey(context.Context, int64, string) error   { return nil }
func (f *fakeDriver) CreateWorkflow(context.Context, *driver.WorkflowRecord) error { return nil }
func (f *fakeDriver) CompleteWorkflowTask(context.Context, driver.Tx, string, string) error {
	return nil
}
func (f *fakeDriver) FailWorkflowTask(context.Context, string, string) error { return nil }
func (f *fakeDriver) GetWorkflow(context.Context, string) (*driver.WorkflowState, error) {
	return nil, nil
}
func (f *fakeDriver) ListWorkflows(context.Context, int, int) ([]*driver.WorkflowState, error) {
	return nil, nil
}
func (f *fakeDriver) Close() error { return nil }

func TestExecutorRespectsMaxWorkers(t *testing.T) {
	exec := executor.New(2, slog.Default())
	block := make(chan struct{})
	var running int
	var mu sync.Mutex

	handler := func(ctx context.Context, job *driver.Job) error {
		mu.Lock()
		running++
		mu.Unlock()
		<-block
		return nil
	}

	for i := 0; i < 4; i++ {
		exec.Dispatch(context.Background(), &driver.Job{ID: int64(i + 1)}, handler)
	}

	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	require.Equal(t, 2, running)
	mu.Unlock()
	close(block)
	exec.Stop()
}

func TestExecutorStopWhileWaitingForSlot(t *testing.T) {
	exec := executor.New(1, slog.Default())
	block := make(chan struct{})
	started := make(chan struct{})

	handler := func(ctx context.Context, job *driver.Job) error {
		select {
		case <-started:
		default:
			close(started)
		}
		<-block
		return nil
	}

	exec.Dispatch(context.Background(), &driver.Job{ID: 1}, handler)
	<-started

	for i := 0; i < 5; i++ {
		exec.Dispatch(context.Background(), &driver.Job{ID: int64(i + 2)}, handler)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(block)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, exec.StopContext(ctx))
}

func TestExecutorStopContextTimeout(t *testing.T) {
	exec := executor.New(1, slog.Default())
	block := make(chan struct{})

	exec.Dispatch(context.Background(), &driver.Job{ID: 1}, func(ctx context.Context, job *driver.Job) error {
		<-block
		return nil
	})
	time.Sleep(30 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := exec.StopContext(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	close(block)
	_ = exec.StopContext(context.Background())
}

func TestExecutorStopDoesNotRunHandlerAfterClose(t *testing.T) {
	exec := executor.New(1, slog.Default())
	block := make(chan struct{})
	handlerRan := make(chan struct{}, 1)

	handler := func(ctx context.Context, job *driver.Job) error {
		select {
		case handlerRan <- struct{}{}:
		default:
		}
		<-block
		return nil
	}

	exec.Dispatch(context.Background(), &driver.Job{ID: 1}, handler)
	select {
	case <-handlerRan:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	go func() {
		_ = exec.StopContext(context.Background())
		close(block)
	}()

	select {
	case <-handlerRan:
		// first run is expected
	case <-time.After(time.Second):
	}

	// Dispatch after stop must not run handler.
	for i := 0; i < 10; i++ {
		exec.Dispatch(context.Background(), &driver.Job{ID: int64(i + 2)}, handler)
	}
	time.Sleep(50 * time.Millisecond)
	select {
	case <-handlerRan:
		t.Fatal("handler ran after executor stopped")
	default:
	}
}

func TestFetchLoopBackoff(t *testing.T) {
	fd := &fakeDriver{}
	exec := executor.New(5, slog.Default())
	loop := executor.NewFetchLoop(fd, []string{"default"}, "w1", 10*time.Millisecond, exec, func(ctx context.Context, job *driver.Job) error {
		return nil
	}, slog.Default(), nil)
	loop.Start()
	time.Sleep(100 * time.Millisecond)
	loop.Stop()
	exec.Stop()
}

func TestFetchLoopWake(t *testing.T) {
	fd := &fakeDriver{
		fetch: []*driver.Job{{ID: 1, Queue: "default", Kind: "noop"}},
	}
	exec := executor.New(5, slog.Default())
	wake := make(chan struct{}, 1)
	loop := executor.NewFetchLoop(fd, []string{"default"}, "w1", time.Second, exec, func(ctx context.Context, job *driver.Job) error {
		return nil
	}, slog.Default(), wake)
	loop.Start()
	t.Cleanup(func() {
		loop.Stop()
		exec.Stop()
	})

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(1), fd.fetchCount.Load())

	fd.mu.Lock()
	fd.fetch = []*driver.Job{{ID: 2, Queue: "default", Kind: "noop"}}
	fd.mu.Unlock()

	wake <- struct{}{}
	require.Eventually(t, func() bool {
		return fd.fetchCount.Load() >= 2
	}, time.Second, 10*time.Millisecond)
}

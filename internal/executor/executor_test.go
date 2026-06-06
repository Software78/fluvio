package executor_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/software78/fluvio/internal/driver"
	"github.com/software78/fluvio/internal/executor"
)

type fakeDriver struct {
	mu    sync.Mutex
	fetch []*driver.Job
}

func (f *fakeDriver) Fetch(_ context.Context, _ []string, _ string, max int) ([]*driver.Job, error) {
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

func (f *fakeDriver) Ack(context.Context, int64) error   { return nil }
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
func (f *fakeDriver) Cancel(context.Context, int64) error { return nil }
func (f *fakeDriver) GetJob(context.Context, int64) (*driver.Job, error) { return nil, nil }
func (f *fakeDriver) ListJobs(context.Context, driver.ListJobsParams) ([]*driver.Job, error) {
	return nil, nil
}
func (f *fakeDriver) TickScheduled(context.Context, time.Time) (int64, error) { return 0, nil }
func (f *fakeDriver) UniqueJobExists(context.Context, string) (bool, error) { return false, nil }
func (f *fakeDriver) PauseQueue(context.Context, string) error { return nil }
func (f *fakeDriver) ResumeQueue(context.Context, string) error { return nil }
func (f *fakeDriver) IsQueuePaused(context.Context, string) (bool, error) { return false, nil }
func (f *fakeDriver) QueueStats(context.Context, string) (*driver.QueueStats, error) {
	return nil, nil
}
func (f *fakeDriver) ListQueues(context.Context) ([]*driver.QueueStats, error) { return nil, nil }
func (f *fakeDriver) TryAcquireLeader(context.Context) (bool, error) { return false, nil }
func (f *fakeDriver) RenewLeader(context.Context) error { return nil }
func (f *fakeDriver) ReleaseLeader(context.Context) error { return nil }
func (f *fakeDriver) StuckJobs(context.Context, time.Duration) ([]*driver.Job, error) {
	return nil, nil
}
func (f *fakeDriver) UpsertWorker(context.Context, string, map[string]int) error { return nil }
func (f *fakeDriver) RemoveWorker(context.Context, string) error { return nil }
func (f *fakeDriver) ListWorkers(context.Context, time.Duration) ([]*driver.WorkerInstance, error) {
	return nil, nil
}
func (f *fakeDriver) Migrate(context.Context) error { return nil }
func (f *fakeDriver) MigrateDown(context.Context, int) error { return nil }
func (f *fakeDriver) MigrationStatus(context.Context) ([]string, error) { return nil, nil }
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

func TestFetchLoopBackoff(t *testing.T) {
	fd := &fakeDriver{}
	exec := executor.New(5, slog.Default())
	loop := executor.NewFetchLoop(fd, []string{"default"}, "w1", 10*time.Millisecond, exec, func(ctx context.Context, job *driver.Job) error {
		return nil
	}, slog.Default())
	loop.Start()
	time.Sleep(100 * time.Millisecond)
	loop.Stop()
	exec.Stop()
}

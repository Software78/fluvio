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
	driver.NoopDriver
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

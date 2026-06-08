package executor

import (
	"context"
	"log/slog"
	"sync"

	"github.com/software78/fluvio/internal/driver"
)

// JobHandler is the stable job-dispatch callback signature used by FetchLoop and Client.handleJob.
// Changing this signature requires updating all call sites; it is not interface-assertable.
type JobHandler func(ctx context.Context, job *driver.Job) error

type Executor struct {
	sem      chan struct{}
	max      int
	logger   *slog.Logger
	wg       sync.WaitGroup
	mu       sync.Mutex
	closed   bool
	stopCh   chan struct{}
	stopOnce sync.Once
}

func New(maxWorkers int, logger *slog.Logger) *Executor {
	if maxWorkers <= 0 {
		maxWorkers = 1
	}
	return &Executor{
		sem:    make(chan struct{}, maxWorkers),
		max:    maxWorkers,
		logger: logger,
		stopCh: make(chan struct{}),
	}
}

func (e *Executor) MaxWorkers() int { return e.max }

func (e *Executor) AvailableSlots() int {
	return e.max - len(e.sem)
}

func (e *Executor) Dispatch(ctx context.Context, job *driver.Job, handler JobHandler) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.wg.Add(1)
	e.mu.Unlock()

	go func() {
		defer e.wg.Done()
		select {
		case e.sem <- struct{}{}:
		case <-e.stopCh:
			return
		}
		defer func() { <-e.sem }()
		_ = handler(ctx, job)
	}()
}

func (e *Executor) Stop() {
	_ = e.StopContext(context.Background())
}

func (e *Executor) StopContext(ctx context.Context) error {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	e.stopOnce.Do(func() { close(e.stopCh) })

	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

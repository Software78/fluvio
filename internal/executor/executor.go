package executor

import (
	"context"
	"log/slog"
	"sync"

	"github.com/software78/fluvio/internal/driver"
)

type JobHandler func(ctx context.Context, job *driver.Job) error

type Executor struct {
	sem     chan struct{}
	max     int
	logger  *slog.Logger
	wg      sync.WaitGroup
	mu      sync.Mutex
	closed  bool
}

func New(maxWorkers int, logger *slog.Logger) *Executor {
	if maxWorkers <= 0 {
		maxWorkers = 1
	}
	return &Executor{
		sem:    make(chan struct{}, maxWorkers),
		max:    maxWorkers,
		logger: logger,
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

	e.sem <- struct{}{}
	go func() {
		defer func() {
			<-e.sem
			e.wg.Done()
		}()

		_ = handler(ctx, job)
	}()
}

func (e *Executor) Stop() {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	e.wg.Wait()
}

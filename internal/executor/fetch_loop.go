package executor

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/software78/fluvio/internal/driver"
)

type FetchLoop struct {
	ctx        context.Context
	driver     driver.FetchDriver
	queues     []string
	workerID   string
	interval   time.Duration
	executor   *Executor
	handler    JobHandler
	logger     *slog.Logger
	wake       <-chan struct{}
	stopCh     chan struct{}
	stopOnce   sync.Once
	doneCh     chan struct{}
	backoff    time.Duration
	maxBackoff time.Duration
}

func NewFetchLoop(
	ctx context.Context,
	d driver.FetchDriver,
	queues []string,
	workerID string,
	interval time.Duration,
	exec *Executor,
	handler JobHandler,
	logger *slog.Logger,
	wake <-chan struct{},
) *FetchLoop {
	if ctx == nil {
		ctx = context.Background()
	}
	return &FetchLoop{
		ctx:        ctx,
		driver:     d,
		queues:     queues,
		workerID:   workerID,
		interval:   interval,
		executor:   exec,
		handler:    handler,
		logger:     logger,
		wake:       wake,
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
		maxBackoff:  5 * time.Second,
	}
}

func (f *FetchLoop) Start() {
	go f.run()
}

func (f *FetchLoop) Stop() {
	f.stopOnce.Do(func() { close(f.stopCh) })
	<-f.doneCh
}

func (f *FetchLoop) run() {
	defer close(f.doneCh)
	sleep := f.interval
	for {
		if f.tick(&sleep) {
			return
		}
		if stop, wake := f.wait(sleep); stop {
			return
		} else if wake {
			sleep = f.interval
		}
	}
}

func (f *FetchLoop) wait(sleep time.Duration) (stop, wake bool) {
	if f.wake == nil {
		select {
		case <-f.stopCh:
			return true, false
		case <-time.After(sleep):
			return false, false
		}
	}
	select {
	case <-f.stopCh:
		return true, false
	case <-f.wake:
		f.backoff = 0
		return false, true
	case <-time.After(sleep):
		return false, false
	}
}

func (f *FetchLoop) tick(sleep *time.Duration) (stop bool) {
	slots := f.executor.AvailableSlots()
	if slots <= 0 {
		*sleep = f.interval
		return false
	}

	jobs, err := f.driver.Fetch(f.ctx, f.queues, f.workerID, slots)
	if errors.Is(err, driver.ErrQueuesPaused) {
		f.backoff = 0
		*sleep = f.interval
		return false
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return true
		}
		f.logger.Error("fetch failed", "error", err)
		*sleep = f.interval
		return false
	}

	if len(jobs) == 0 {
		if f.backoff == 0 {
			f.backoff = f.interval
		} else if f.backoff < f.maxBackoff {
			f.backoff *= 2
			if f.backoff > f.maxBackoff {
				f.backoff = f.maxBackoff
			}
		}
		*sleep = f.backoff
		return false
	}

	f.backoff = 0
	*sleep = f.interval

	for _, job := range jobs {
		f.executor.Dispatch(f.ctx, job, f.handler)
	}
	return false
}

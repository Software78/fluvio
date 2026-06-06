package executor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/software78/fluvio/internal/driver"
)

type FetchLoop struct {
	driver        driver.Driver
	queues        []string
	workerID      string
	interval      time.Duration
	executor      *Executor
	handler       JobHandler
	logger        *slog.Logger
	stopCh        chan struct{}
	doneCh        chan struct{}
	mu            sync.Mutex
	backoff       time.Duration
	maxBackoff    time.Duration
}

func NewFetchLoop(
	d driver.Driver,
	queues []string,
	workerID string,
	interval time.Duration,
	exec *Executor,
	handler JobHandler,
	logger *slog.Logger,
) *FetchLoop {
	return &FetchLoop{
		driver:     d,
		queues:     queues,
		workerID:   workerID,
		interval:   interval,
		executor:   exec,
		handler:    handler,
		logger:     logger,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		maxBackoff: 5 * time.Second,
	}
}

func (f *FetchLoop) Start() {
	go f.run()
}

func (f *FetchLoop) Stop() {
	close(f.stopCh)
	<-f.doneCh
}

func (f *FetchLoop) run() {
	defer close(f.doneCh)
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for {
		select {
		case <-f.stopCh:
			return
		case <-ticker.C:
			if f.tick() {
				return
			}
		}
	}
}

func (f *FetchLoop) tick() (stop bool) {
	slots := f.executor.AvailableSlots()
	if slots <= 0 {
		return false
	}

	ctx := context.Background()
	jobs, err := f.driver.Fetch(ctx, f.queues, f.workerID, slots)
	if err != nil {
		f.logger.Error("fetch failed", "error", err)
		return false
	}

	if len(jobs) == 0 {
		f.mu.Lock()
		if f.backoff == 0 {
			f.backoff = f.interval
		} else if f.backoff < f.maxBackoff {
			f.backoff *= 2
			if f.backoff > f.maxBackoff {
				f.backoff = f.maxBackoff
			}
		}
		sleep := f.backoff
		f.mu.Unlock()

		select {
		case <-f.stopCh:
			return true
		case <-time.After(sleep):
		}
		return false
	}

	f.mu.Lock()
	f.backoff = 0
	f.mu.Unlock()

	for _, job := range jobs {
		f.executor.Dispatch(context.Background(), job, f.handler)
	}
	return false
}

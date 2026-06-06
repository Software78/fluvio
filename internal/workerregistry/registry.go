package workerregistry

import (
	"context"
	"log/slog"
	"time"

	"github.com/software78/fluvio/internal/driver"
)

// Registry heartbeats a processing client into the fleet worker table.
type Registry struct {
	driver   driver.Driver
	workerID string
	queues   map[string]int
	interval time.Duration
	logger   *slog.Logger
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func New(d driver.Driver, workerID string, queues map[string]int, interval time.Duration, logger *slog.Logger) *Registry {
	if queues == nil {
		queues = map[string]int{}
	}
	return &Registry{
		driver:   d,
		workerID: workerID,
		queues:   queues,
		interval: interval,
		logger:   logger,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

func (r *Registry) Start() {
	go r.run()
}

func (r *Registry) Stop() {
	close(r.stopCh)
	<-r.doneCh
	_ = r.driver.RemoveWorker(context.Background(), r.workerID)
}

func (r *Registry) run() {
	defer close(r.doneCh)

	ctx := context.Background()
	if err := r.driver.UpsertWorker(ctx, r.workerID, r.queues); err != nil {
		r.logger.Error("worker registry upsert failed", "error", err)
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			if err := r.driver.UpsertWorker(ctx, r.workerID, r.queues); err != nil {
				r.logger.Error("worker registry heartbeat failed", "error", err)
			}
		}
	}
}

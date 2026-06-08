package workerregistry

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/software78/fluvio/internal/driver"
)

const heartbeatTimeout = 5 * time.Second

// Registry heartbeats a processing client into the fleet worker table.
type Registry struct {
	driver   driver.Driver
	workerID string
	queues   map[string]int
	interval time.Duration
	logger   *slog.Logger
	stopCh   chan struct{}
	stopOnce sync.Once
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
	r.stopOnce.Do(func() { close(r.stopCh) })
	<-r.doneCh
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cancel()
	_ = r.driver.RemoveWorker(ctx, r.workerID)
}

func (r *Registry) upsert() error {
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cancel()
	return r.driver.UpsertWorker(ctx, r.workerID, r.queues)
}

func (r *Registry) run() {
	defer close(r.doneCh)

	if err := r.upsert(); err != nil {
		r.logger.Error("worker registry upsert failed", "error", err)
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			if err := r.upsert(); err != nil {
				r.logger.Error("worker registry heartbeat failed", "error", err)
			}
		}
	}
}

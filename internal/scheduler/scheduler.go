package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/software78/fluvio/internal/driver"
)

type Scheduler struct {
	driver       driver.Driver
	logger       *slog.Logger
	interval     time.Duration
	startupDelay time.Duration
	stopCh       chan struct{}
	stopOnce     sync.Once
	doneCh       chan struct{}
}

func New(d driver.Driver, logger *slog.Logger, interval, startupDelay time.Duration) *Scheduler {
	if interval == 0 {
		interval = 5 * time.Second
	}
	return &Scheduler{
		driver:       d,
		logger:       logger,
		interval:     interval,
		startupDelay: startupDelay,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
}

func (s *Scheduler) Start() {
	go s.run()
}

func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	<-s.doneCh
}

func (s *Scheduler) run() {
	defer close(s.doneCh)
	if s.startupDelay > 0 {
		select {
		case <-s.stopCh:
			return
		case <-time.After(s.startupDelay):
		}
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			n, err := s.driver.TickScheduled(context.Background(), time.Now().UTC())
			if err != nil {
				s.logger.Error("tick scheduled failed", "error", err)
			} else if n > 0 {
				s.logger.Debug("moved scheduled jobs to pending", "count", n)
			}
		}
	}
}

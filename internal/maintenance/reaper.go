package maintenance

import (
	"context"
	"log/slog"
	"time"

	"github.com/software78/fluvio/internal/driver"
)

type NackFunc func(ctx context.Context, job *driver.Job, err error, nextAttemptAt time.Time) error

type Reaper struct {
	driver   driver.Driver
	logger   *slog.Logger
	timeout  time.Duration
	interval time.Duration
	nack     NackFunc
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func NewReaper(d driver.Driver, logger *slog.Logger, timeout, interval time.Duration, nack NackFunc) *Reaper {
	if interval == 0 {
		interval = 60 * time.Second
	}
	return &Reaper{
		driver:   d,
		logger:   logger,
		timeout:  timeout,
		interval: interval,
		nack:     nack,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

func (r *Reaper) Start() {
	go r.run()
}

func (r *Reaper) Stop() {
	close(r.stopCh)
	<-r.doneCh
}

func (r *Reaper) run() {
	defer close(r.doneCh)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.reap()
		}
	}
}

func (r *Reaper) reap() {
	ctx := context.Background()
	jobs, err := r.driver.StuckJobs(ctx, r.timeout)
	if err != nil {
		r.logger.Error("stuck jobs query failed", "error", err)
		return
	}
	for _, job := range jobs {
		err := r.nack(ctx, job, errStuckJob, time.Now())
		if err != nil {
			r.logger.Error("reap nack failed", "job_id", job.ID, "error", err)
		} else {
			r.logger.Info("reaped stuck job", "job_id", job.ID)
		}
	}
}

var errStuckJob = stuckJobError{}

type stuckJobError struct{}

func (stuckJobError) Error() string { return "job timed out" }

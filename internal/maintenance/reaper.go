package maintenance

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/software78/fluvio/internal/driver"
)

type NackFunc func(ctx context.Context, job *driver.Job, err error, nextAttemptAt time.Time) error

type retryDelayFunc func(attempt int16, maxDelay time.Duration) time.Duration

type Reaper struct {
	driver        driver.MaintenanceDriver
	logger        *slog.Logger
	timeout       time.Duration
	interval      time.Duration
	startupDelay  time.Duration
	maxRetryDelay time.Duration
	retryDelay    retryDelayFunc
	nack          NackFunc
	stopCh        chan struct{}
	stopOnce      sync.Once
	doneCh        chan struct{}
}

func NewReaper(
	d driver.MaintenanceDriver,
	logger *slog.Logger,
	timeout, interval, startupDelay, maxRetryDelay time.Duration,
	retryDelay retryDelayFunc,
	nack NackFunc,
) *Reaper {
	if interval == 0 {
		interval = 60 * time.Second
	}
	return &Reaper{
		driver:        d,
		logger:        logger,
		timeout:       timeout,
		interval:      interval,
		startupDelay:  startupDelay,
		maxRetryDelay: maxRetryDelay,
		retryDelay:    retryDelay,
		nack:          nack,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

func (r *Reaper) Start() {
	go r.run()
}

func (r *Reaper) Stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
	<-r.doneCh
}

func (r *Reaper) run() {
	defer close(r.doneCh)
	if r.startupDelay > 0 {
		select {
		case <-r.stopCh:
			return
		case <-time.After(r.startupDelay):
		}
	}
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
		delay := r.retryDelay(job.Attempt, r.maxRetryDelay)
		nextAt := time.Now().UTC().Add(delay)
		err := r.nack(ctx, job, errStuckJob, nextAt)
		if err != nil {
			r.logger.Error("reap nack failed", "job_id", job.ID, "error", err)
		} else {
			r.logger.Info("reaped stuck job", "job_id", job.ID, "retry_at", nextAt)
		}
	}
}

var errStuckJob = stuckJobError{}

type stuckJobError struct{}

func (stuckJobError) Error() string { return "job timed out" }

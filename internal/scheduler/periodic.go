package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/software78/fluvio/internal/driver"
)

type Periodic struct {
	driver       driver.Driver
	logger       *slog.Logger
	interval     time.Duration
	startupDelay time.Duration
	parser       cron.Parser
	schedules    sync.Map // kind -> cron.Schedule
	stopCh       chan struct{}
	stopOnce     sync.Once
	doneCh       chan struct{}
}

func NewPeriodic(d driver.Driver, logger *slog.Logger, interval, startupDelay time.Duration) *Periodic {
	if interval == 0 {
		interval = 30 * time.Second
	}
	return &Periodic{
		driver:       d,
		logger:       logger,
		interval:     interval,
		startupDelay: startupDelay,
		parser:       cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
}

func (p *Periodic) Register(ctx context.Context, cronExpr, kind string, args []byte, queue string, maxAttempts int16) error {
	schedule, err := p.parser.Parse(cronExpr)
	if err != nil {
		return err
	}
	nextRun := schedule.Next(time.Now().UTC())
	if err := p.driver.UpsertPeriodicJob(ctx, kind, cronExpr, queue, maxAttempts, args, nextRun); err != nil {
		return err
	}
	p.schedules.Store(kind, schedule)
	return nil
}

func (p *Periodic) Start() {
	go p.run()
}

func (p *Periodic) Stop() {
	p.stopOnce.Do(func() { close(p.stopCh) })
	<-p.doneCh
}

func (p *Periodic) run() {
	defer close(p.doneCh)
	if p.startupDelay > 0 {
		select {
		case <-p.stopCh:
			return
		case <-time.After(p.startupDelay):
		}
	}
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case now := <-ticker.C:
			p.Tick(context.Background(), now.UTC())
		}
	}
}

func (p *Periodic) scheduleFor(kind, cronExpr string) (cron.Schedule, error) {
	if s, ok := p.schedules.Load(kind); ok {
		return s.(cron.Schedule), nil
	}
	schedule, err := p.parser.Parse(cronExpr)
	if err != nil {
		return nil, err
	}
	p.schedules.Store(kind, schedule)
	return schedule, nil
}

// Tick processes due periodic jobs. Exported for integration tests.
func (p *Periodic) Tick(ctx context.Context, now time.Time) {
	due, err := p.driver.DuePeriodicJobs(ctx, now)
	if err != nil {
		p.logger.Error("due periodic jobs failed", "error", err)
		return
	}

	for _, job := range due {
		schedule, err := p.scheduleFor(job.Kind, job.Cron)
		if err != nil {
			p.logger.Error("parse periodic cron failed", "kind", job.Kind, "error", err)
			continue
		}

		nextRun := schedule.Next(now)
		tx, err := p.driver.BeginTx(ctx)
		if err != nil {
			p.logger.Error("begin tx failed", "kind", job.Kind, "error", err)
			continue
		}

		claimed, err := p.driver.UpdatePeriodicJobNextRunTx(ctx, tx, job.Kind, nextRun)
		if err != nil {
			_ = p.driver.RollbackTx(ctx, tx)
			p.logger.Error("update periodic next run failed", "kind", job.Kind, "error", err)
			continue
		}
		if !claimed {
			_ = p.driver.RollbackTx(ctx, tx)
			continue
		}

		_, err = p.driver.EnqueueTx(ctx, tx, driver.EnqueueParams{
			Queue:       job.Queue,
			Kind:        job.Kind,
			Args:        job.Args,
			MaxAttempts: job.MaxAttempts,
		})
		if err != nil {
			_ = p.driver.RollbackTx(ctx, tx)
			p.logger.Error("periodic enqueue failed", "kind", job.Kind, "error", err)
			continue
		}

		if err := p.driver.CommitTx(ctx, tx); err != nil {
			p.logger.Error("commit periodic tx failed", "kind", job.Kind, "error", err)
		}
	}
}

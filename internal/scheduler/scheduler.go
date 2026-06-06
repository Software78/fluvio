package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/software78/fluvio/internal/driver"
)

type Scheduler struct {
	driver   driver.Driver
	logger   *slog.Logger
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func New(d driver.Driver, logger *slog.Logger, interval time.Duration) *Scheduler {
	if interval == 0 {
		interval = 5 * time.Second
	}
	return &Scheduler{
		driver:   d,
		logger:   logger,
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

func (s *Scheduler) Start() {
	go s.run()
}

func (s *Scheduler) Stop() {
	close(s.stopCh)
	<-s.doneCh
}

func (s *Scheduler) run() {
	defer close(s.doneCh)
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

type PeriodicJob struct {
	Cron     string
	Kind     string
	Args     []byte
	schedule cron.Schedule
}

type Periodic struct {
	driver   driver.Driver
	logger   *slog.Logger
	interval time.Duration
	jobs     []PeriodicJob
	parser   cron.Parser
	stopCh   chan struct{}
	doneCh   chan struct{}
	mu       sync.RWMutex
}

func NewPeriodic(d driver.Driver, logger *slog.Logger, interval time.Duration) *Periodic {
	if interval == 0 {
		interval = 30 * time.Second
	}
	return &Periodic{
		driver:   d,
		logger:   logger,
		interval: interval,
		parser:   cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

func (p *Periodic) Register(cronExpr, kind string, args []byte) error {
	schedule, err := p.parser.Parse(cronExpr)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.jobs = append(p.jobs, PeriodicJob{Cron: cronExpr, Kind: kind, Args: args, schedule: schedule})
	return nil
}

func (p *Periodic) Start() {
	go p.run()
}

func (p *Periodic) Stop() {
	close(p.stopCh)
	<-p.doneCh
}

func (p *Periodic) run() {
	defer close(p.doneCh)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	lastRun := make(map[string]time.Time)

	for {
		select {
		case <-p.stopCh:
			return
		case now := <-ticker.C:
			p.tick(now, lastRun)
		}
	}
}

func periodicJobKey(kind, cronExpr string) string {
	return kind + "\x00" + cronExpr
}

func (p *Periodic) tick(now time.Time, lastRun map[string]time.Time) {
	p.mu.RLock()
	jobs := append([]PeriodicJob(nil), p.jobs...)
	p.mu.RUnlock()

	for _, job := range jobs {
		key := periodicJobKey(job.Kind, job.Cron)
		prev, ok := lastRun[key]
		if !ok {
			prev = now.Add(-time.Minute)
		}
		next := job.schedule.Next(prev)
		if !next.After(now) {
			_, err := p.driver.Enqueue(context.Background(), driver.EnqueueParams{
				Kind: job.Kind,
				Args: job.Args,
			})
			if err != nil {
				p.logger.Error("periodic enqueue failed", "kind", job.Kind, "error", err)
			} else {
				lastRun[key] = now
			}
		}
	}
}

package fluvio

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/software78/fluvio/internal/driver"
	"github.com/software78/fluvio/internal/executor"
	"github.com/software78/fluvio/internal/leader"
	"github.com/software78/fluvio/internal/maintenance"
	"github.com/software78/fluvio/internal/scheduler"
)

// Client is the main Fluvio job queue client.
type Client struct {
	driver    driver.Driver
	cfg       Config
	mu        sync.Mutex
	running   bool
	stopCh    chan struct{}

	fetchLoops []*executor.FetchLoop
	exec       *executor.Executor
	elector    *leader.Elector
	sched      *scheduler.Scheduler
	periodic   *scheduler.Periodic
	reaper     *maintenance.Reaper

	leaderMu       sync.Mutex
	leaderServices bool
}

// NewClient creates a client. Workers are required for kind mapping even on insert-only clients.
func NewClient(d driver.Driver, cfg *Config) (*Client, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: driver is required", ErrInvalidConfig)
	}
	if cfg == nil {
		cfg = &Config{}
	}
	cfg.applyDefaults()
	if cfg.Workers == nil {
		return nil, fmt.Errorf("%w: workers registry is required", ErrInvalidConfig)
	}
	return &Client{
		driver:   d,
		cfg:      *cfg,
		periodic: scheduler.NewPeriodic(d, cfg.Logger, 30*time.Second),
	}, nil
}

func (c *Client) Driver() driver.Driver { return c.driver }

func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return ErrClientRunning
	}

	totalWorkers := 0
	queues := make([]string, 0, len(c.cfg.Queues))
	for name, qc := range c.cfg.Queues {
		queues = append(queues, name)
		totalWorkers += qc.MaxWorkers
	}
	if totalWorkers == 0 {
		totalWorkers = 10
	}

	c.exec = executor.New(totalWorkers, c.cfg.Logger)
	c.stopCh = make(chan struct{})

	if len(queues) > 0 {
		loop := executor.NewFetchLoop(
			c.driver,
			queues,
			c.cfg.WorkerID,
			c.cfg.FetchInterval,
			c.exec,
			c.handleJob,
			c.cfg.Logger,
		)
		c.fetchLoops = append(c.fetchLoops, loop)
		loop.Start()
	}

	c.sched = scheduler.New(c.driver, c.cfg.Logger, 5*time.Second)
	c.reaper = maintenance.NewReaper(
		c.driver,
		c.cfg.Logger,
		c.cfg.JobTimeout,
		60*time.Second,
		c.nackJob,
	)

	c.elector = leader.NewElector(c.driver, c.cfg.Logger, leader.LeaderCallbacks{
		OnAcquire: c.startLeaderServices,
		OnLoss:    c.stopLeaderServices,
	})
	c.elector.Start()

	c.running = true
	return nil
}

func (c *Client) Stop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	c.running = false
	c.mu.Unlock()

	close(c.stopCh)
	c.stopLeaderServices()

	if c.elector != nil {
		c.elector.Stop()
	}
	for _, loop := range c.fetchLoops {
		loop.Stop()
	}
	if c.exec != nil {
		c.exec.Stop()
	}
}

func (c *Client) startLeaderServices() {
	c.leaderMu.Lock()
	defer c.leaderMu.Unlock()
	if c.leaderServices {
		return
	}
	c.sched.Start()
	c.periodic.Start()
	c.reaper.Start()
	c.leaderServices = true
}

func (c *Client) stopLeaderServices() {
	c.leaderMu.Lock()
	defer c.leaderMu.Unlock()
	if !c.leaderServices {
		return
	}
	c.sched.Stop()
	c.periodic.Stop()
	c.reaper.Stop()
	c.leaderServices = false
}

func (c *Client) AddPeriodicJob(cronExpr string, args JobArgs) error {
	data, err := json.Marshal(args)
	if err != nil {
		return err
	}
	return c.periodic.Register(cronExpr, args.Kind(), data)
}

func (c *Client) Enqueue(ctx context.Context, args JobArgs, opts ...EnqueueOption) (*JobRow, error) {
	o := applyEnqueueOptions(opts)
	data, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	job, err := c.driver.Enqueue(ctx, driver.EnqueueParams{
		Queue:       o.queue,
		Kind:        args.Kind(),
		Args:        data,
		Priority:    o.priority,
		MaxAttempts: o.maxAttempts,
		ScheduledAt: o.scheduledAt,
		UniqueKey:   o.uniqueKey,
		Tags:        o.tags,
		Metadata:    o.metadata,
	})
	if err != nil {
		return nil, err
	}
	row := driverJobToRow(job)
	return &row, nil
}

func (c *Client) EnqueueTx(ctx context.Context, tx pgx.Tx, args JobArgs, opts ...EnqueueOption) (*JobRow, error) {
	o := applyEnqueueOptions(opts)
	data, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	job, err := c.driver.EnqueueTx(ctx, tx, driver.EnqueueParams{
		Queue:       o.queue,
		Kind:        args.Kind(),
		Args:        data,
		Priority:    o.priority,
		MaxAttempts: o.maxAttempts,
		ScheduledAt: o.scheduledAt,
		UniqueKey:   o.uniqueKey,
		Tags:        o.tags,
		Metadata:    o.metadata,
	})
	if err != nil {
		return nil, err
	}
	row := driverJobToRow(job)
	return &row, nil
}

func (c *Client) EnqueueMany(ctx context.Context, argsList []JobArgs, opts ...EnqueueOption) ([]JobRow, error) {
	if len(argsList) == 0 {
		return nil, nil
	}
	o := applyEnqueueOptions(opts)
	params := make([]driver.EnqueueParams, len(argsList))
	for i, args := range argsList {
		data, err := json.Marshal(args)
		if err != nil {
			return nil, err
		}
		params[i] = driver.EnqueueParams{
			Queue:       o.queue,
			Kind:        args.Kind(),
			Args:        data,
			Priority:    o.priority,
			MaxAttempts: o.maxAttempts,
			ScheduledAt: o.scheduledAt,
			UniqueKey:   o.uniqueKey,
			Tags:        o.tags,
			Metadata:    o.metadata,
		}
	}
	jobs, err := c.driver.EnqueueMany(ctx, params)
	if err != nil {
		return nil, err
	}
	rows := make([]JobRow, len(jobs))
	for i, job := range jobs {
		rows[i] = driverJobToRow(job)
	}
	return rows, nil
}

func (c *Client) GetJob(ctx context.Context, id int64) (*JobRow, error) {
	job, err := c.driver.GetJob(ctx, id)
	if err != nil {
		return nil, err
	}
	row := driverJobToRow(job)
	return &row, nil
}

func (c *Client) ListJobs(ctx context.Context, queue, state, kind string, limit, offset int) ([]JobRow, error) {
	jobs, err := c.driver.ListJobs(ctx, driver.ListJobsParams{
		Queue: queue, State: state, Kind: kind, Limit: limit, Offset: offset,
	})
	if err != nil {
		return nil, err
	}
	rows := make([]JobRow, len(jobs))
	for i, job := range jobs {
		rows[i] = driverJobToRow(job)
	}
	return rows, nil
}

func (c *Client) PauseQueue(ctx context.Context, queue string) error {
	return c.driver.PauseQueue(ctx, queue)
}

func (c *Client) ResumeQueue(ctx context.Context, queue string) error {
	return c.driver.ResumeQueue(ctx, queue)
}

func (c *Client) QueueStats(ctx context.Context, queue string) (*driver.QueueStats, error) {
	return c.driver.QueueStats(ctx, queue)
}

func (c *Client) ListQueues(ctx context.Context) ([]*driver.QueueStats, error) {
	return c.driver.ListQueues(ctx)
}

func (c *Client) Migrate(ctx context.Context) error {
	return c.driver.Migrate(ctx)
}

func (c *Client) handleJob(ctx context.Context, dJob *driver.Job) error {
	w, ok := c.cfg.Workers.get(dJob.Kind)
	if !ok {
		err := fmt.Errorf("%w: %s", ErrWorkerNotFound, dJob.Kind)
		_ = c.nackJob(ctx, dJob, err, time.Now())
		return err
	}

	wrap := &driverJobWrapper{
		id:          dJob.ID,
		queue:       dJob.Queue,
		kind:        dJob.Kind,
		args:        dJob.Args,
		attempt:     dJob.Attempt,
		maxAttempts: dJob.MaxAttempts,
	}

	run := func(ctx context.Context) error {
		return w.work(ctx, wrap)
	}
	run = chainMiddleware(c.cfg.Middleware, run)

	workCtx := ctx
	timeout := w.timeout(c.cfg.JobTimeout)
	if timeout > 0 {
		var cancel context.CancelFunc
		workCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	err := func() (retErr error) {
		defer func() {
			if r := recover(); r != nil {
				retErr = fmt.Errorf("panic: %v", r)
			}
		}()
		return run(workCtx)
	}()
	if err != nil {
		row := driverJobToRow(dJob)
		if c.cfg.ErrorHandler != nil {
			c.cfg.ErrorHandler(ctx, row, err)
		}
		nextAt := time.Now().Add(w.nextAttempt(wrap, err, c.cfg.MaxRetryDelay))
		return c.nackJob(ctx, dJob, err, nextAt)
	}
	return c.driver.Ack(ctx, dJob.ID)
}

func (c *Client) nackJob(ctx context.Context, dJob *driver.Job, err error, nextAt time.Time) error {
	return c.driver.Nack(ctx, dJob.ID, err, nextAt)
}

func driverJobToRow(job *driver.Job) JobRow {
	return JobRow{
		ID:          job.ID,
		Queue:       job.Queue,
		Kind:        job.Kind,
		Args:        json.RawMessage(job.Args),
		State:       JobState(job.State),
		Priority:    job.Priority,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		ScheduledAt: job.ScheduledAt,
		CreatedAt:   job.CreatedAt,
		ErrorTrace:  json.RawMessage(job.ErrorTrace),
		Tags:        job.Tags,
	}
}
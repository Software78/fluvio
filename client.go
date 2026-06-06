package fluvio

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/software78/fluvio/internal/driver"
	"github.com/software78/fluvio/internal/executor"
	"github.com/software78/fluvio/internal/leader"
	"github.com/software78/fluvio/internal/maintenance"
	"github.com/software78/fluvio/internal/scheduler"
)

type queueRunner struct {
	loop *executor.FetchLoop
	exec *executor.Executor
}

type pendingPeriodic struct {
	cronExpr string
	kind     string
	data     []byte
}

// Client is the main Fluvio job queue client.
type Client struct {
	driver    driver.Driver
	cfg       Config
	mu        sync.Mutex
	running   bool
	stopCh    chan struct{}

	queueRunners []*queueRunner
	elector      *leader.Elector
	sched        *scheduler.Scheduler
	periodic     *scheduler.Periodic
	reaper       *maintenance.Reaper

	pendingPeriodic []pendingPeriodic

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
		driver: d,
		cfg:    *cfg,
	}, nil
}

func (c *Client) Driver() driver.Driver { return c.driver }

func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return ErrClientRunning
	}

	c.queueRunners = nil
	for name, qc := range c.cfg.Queues {
		if qc.MaxWorkers <= 0 {
			continue
		}
		exec := executor.New(qc.MaxWorkers, c.cfg.Logger)
		loop := executor.NewFetchLoop(
			c.driver,
			[]string{name},
			c.cfg.WorkerID,
			c.cfg.FetchInterval,
			exec,
			c.handleJob,
			c.cfg.Logger,
		)
		c.queueRunners = append(c.queueRunners, &queueRunner{loop: loop, exec: exec})
	}

	c.stopCh = make(chan struct{})

	c.periodic = scheduler.NewPeriodic(c.driver, c.cfg.Logger, c.cfg.PeriodicInterval)
	for _, reg := range c.pendingPeriodic {
		if err := c.periodic.Register(reg.cronExpr, reg.kind, reg.data); err != nil {
			return err
		}
	}
	c.pendingPeriodic = nil

	for _, qr := range c.queueRunners {
		qr.loop.Start()
	}

	c.sched = scheduler.New(c.driver, c.cfg.Logger, 5*time.Second)
	c.reaper = maintenance.NewReaper(
		c.driver,
		c.cfg.Logger,
		c.cfg.JobTimeout,
		60*time.Second,
		c.cfg.MaxRetryDelay,
		DefaultRetryDelay,
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

// Stop shuts down the client and waits for in-flight jobs to finish.
func (c *Client) Stop() {
	_ = c.StopContext(context.Background())
}

// StopContext shuts down the client. If ctx expires before in-flight jobs finish,
// shutdown continues but returns ctx.Err().
func (c *Client) StopContext(ctx context.Context) error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = false
	c.mu.Unlock()

	close(c.stopCh)
	c.stopLeaderServices()

	if c.elector != nil {
		c.elector.Stop()
	}
	for _, qr := range c.queueRunners {
		qr.loop.Stop()
	}

	var stopErr error
	for _, qr := range c.queueRunners {
		if err := qr.exec.StopContext(ctx); err != nil && stopErr == nil {
			stopErr = err
		}
	}
	return stopErr
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
	if c.periodic != nil {
		return c.periodic.Register(cronExpr, args.Kind(), data)
	}
	c.pendingPeriodic = append(c.pendingPeriodic, pendingPeriodic{
		cronExpr: cronExpr,
		kind:     args.Kind(),
		data:     data,
	})
	return nil
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

func (c *Client) EnqueueTx(ctx context.Context, tx Tx, args JobArgs, opts ...EnqueueOption) (*JobRow, error) {
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

// EnqueueMany inserts multiple jobs in a single transaction. All jobs commit
// together or none do; the first error (including ErrUniqueConflict) rolls back
// the entire batch.
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
	if !ValidJobState(state) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidJobState, state)
	}
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

// UniqueJobExists reports whether an active job with the given unique key exists.
func (c *Client) UniqueJobExists(ctx context.Context, uniqueKey string) (bool, error) {
	return c.driver.UniqueJobExists(ctx, uniqueKey)
}

func (c *Client) Migrate(ctx context.Context) error {
	return c.driver.Migrate(ctx)
}

// handleJob runs a fetched job. ctx comes from the fetch loop (context.Background);
// it is not cancelled on client shutdown — StopContext waits on the executor
// WaitGroup instead. Per-job timeouts use context.WithTimeout derived from ctx.
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
		delay, nextErr := w.nextAttempt(wrap, err, c.cfg.MaxRetryDelay)
		if nextErr != nil {
			c.cfg.Logger.Warn("failed to decode job args for retry delay; using default backoff",
				"job_id", dJob.ID, "kind", dJob.Kind, "error", nextErr)
			delay = DefaultRetryDelay(dJob.Attempt, c.cfg.MaxRetryDelay)
		}
		nextAt := time.Now().Add(delay)
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
		AttemptedBy: job.AttemptedBy,
		ScheduledAt: job.ScheduledAt,
		AttemptedAt: job.AttemptedAt,
		FinalizedAt: job.FinalizedAt,
		CreatedAt:   job.CreatedAt,
		ErrorTrace:  json.RawMessage(job.ErrorTrace),
		Tags:        job.Tags,
		UniqueKey:   job.UniqueKey,
		Metadata:    json.RawMessage(job.Metadata),
	}
}

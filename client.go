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
	"github.com/software78/fluvio/internal/workerregistry"
)

type queueRunner struct {
	loop           *executor.FetchLoop
	exec           *executor.Executor
	shutdownCancel context.CancelFunc
}

type pendingPeriodic struct {
	cronExpr    string
	kind        string
	data        []byte
	queue       string
	maxAttempts int16
}

// Verify Client satisfies the apiClient interface used by fluviui.
// This assertion lives here rather than in fluviui to avoid an import cycle.
// Uncomment if fluviui's apiClient is ever exported:
// var _ fluviui.apiClient = (*Client)(nil)

// Client is the main Fluvio job queue client.
type Client struct {
	driver       driver.Driver
	cfg          Config
	mu           sync.Mutex
	running      bool
	stopCh       chan struct{}
	clientCtx    context.Context
	clientCancel context.CancelFunc

	queueRunners []*queueRunner
	registry     *workerregistry.Registry
	elector      *leader.Elector
	sched        *scheduler.Scheduler
	periodic     *scheduler.Periodic
	reaper       *maintenance.Reaper

	pendingPeriodic []pendingPeriodic

	leaderMu       sync.Mutex
	leaderServices bool

	concurrencyMu   sync.RWMutex
	partitionKeyFns map[string]func(args []byte) string // in-memory only; not persisted

	wakeSub JobWakeSubscription
}

// ConcurrencyLimitConfig configures per-kind concurrency caps.
type ConcurrencyLimitConfig struct {
	Kind          string
	MaxConcurrent int
	// PartitionKeyFn extracts a partition string from raw job args JSON.
	// Optional; never serialised — each worker process must call SetConcurrencyLimit on startup.
	PartitionKeyFn func(args []byte) string
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

	if nc, ok := c.driver.(NotifyConfigurer); ok {
		nc.ConfigureNotify(c.cfg.PollOnly, c.cfg.NotifyDebounce)
	}

	var wake <-chan struct{}
	if !c.cfg.PollOnly {
		if sub, ok := c.driver.(JobSubscriber); ok {
			queueNames := make([]string, 0, len(c.cfg.Queues))
			for name, qc := range c.cfg.Queues {
				if qc.MaxWorkers > 0 {
					queueNames = append(queueNames, name)
				}
			}
			if len(queueNames) > 0 {
				subscriber, err := sub.Subscribe(ctx, queueNames)
				if err != nil {
					return err
				}
				c.wakeSub = subscriber
				wake = subscriber.Wake()
			}
		}
	}

	c.queueRunners = nil
	for name, qc := range c.cfg.Queues {
		if qc.MaxWorkers <= 0 {
			continue
		}
		exec := executor.New(qc.MaxWorkers, c.cfg.Logger)
		shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
		loop := executor.NewFetchLoop(
			shutdownCtx,
			c.driver,
			[]string{name},
			c.cfg.WorkerID,
			c.cfg.FetchInterval,
			exec,
			c.handleJob,
			c.cfg.Logger,
			wake,
			
		)
		c.queueRunners = append(c.queueRunners, &queueRunner{
			loop:           loop,
			exec:           exec,
			shutdownCancel: shutdownCancel,
		})
	}

	c.stopCh = make(chan struct{})

	c.periodic = scheduler.NewPeriodic(c.driver, c.cfg.Logger, c.cfg.PeriodicInterval, c.cfg.LeaderServicesStartupDelay)
	for _, reg := range c.pendingPeriodic {
		if err := c.periodic.Register(context.Background(), reg.cronExpr, reg.kind, reg.data, reg.queue, reg.maxAttempts); err != nil {
			return err
		}
	}
	c.pendingPeriodic = nil

	for _, qr := range c.queueRunners {
		qr.loop.Start()
	}

	if queues := processingQueues(c.cfg.Queues); len(queues) > 0 {
		c.registry = workerregistry.New(
			c.driver,
			c.cfg.WorkerID,
			queues,
			c.cfg.WorkerHeartbeatInterval,
			c.cfg.Logger,
		)
		c.registry.Start()
	}

	c.sched = scheduler.New(c.driver, c.cfg.Logger, 5*time.Second, c.cfg.LeaderServicesStartupDelay)
	c.reaper = maintenance.NewReaper(
		c.driver,
		c.cfg.Logger,
		c.cfg.JobTimeout,
		60*time.Second,
		c.cfg.LeaderServicesStartupDelay,
		c.cfg.MaxRetryDelay,
		DefaultRetryDelay,
		c.nackJob,
	)

	c.elector = leader.NewElector(c.driver, c.cfg.Logger, leader.LeaderCallbacks{
		OnAcquire: c.startLeaderServices,
		OnLoss:    c.stopLeaderServices,
	})
	c.elector.Start()

	c.cfg.Workers.markStarted()
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

	if c.registry != nil {
		c.registry.Stop()
		c.registry = nil
	}

	if c.elector != nil {
		c.elector.Stop()
	}
	if c.wakeSub != nil {
		_ = c.wakeSub.Close()
		c.wakeSub = nil
	}
	for _, qr := range c.queueRunners {
		if qr.shutdownCancel != nil {
			qr.shutdownCancel()
		}
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

func (c *Client) AddPeriodicJob(cronExpr string, args JobArgs, opts ...EnqueueOption) error {
	o := applyEnqueueOptions(opts)
	data, err := json.Marshal(args)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.periodic != nil {
		return c.periodic.Register(context.Background(), cronExpr, args.Kind(), data, o.queue, o.maxAttempts)
	}
	c.pendingPeriodic = append(c.pendingPeriodic, pendingPeriodic{
		cronExpr:    cronExpr,
		kind:        args.Kind(),
		data:        data,
		queue:       o.queue,
		maxAttempts: o.maxAttempts,
	})
	return nil
}

func (c *Client) ListPeriodicJobs(ctx context.Context) ([]driver.PeriodicJob, error) {
	jobs, err := c.driver.ListPeriodicJobs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]driver.PeriodicJob, len(jobs))
	for i, j := range jobs {
		out[i] = *j
	}
	return out, nil
}

func (c *Client) PausePeriodicJob(ctx context.Context, kind string) error {
	return c.driver.PausePeriodicJob(ctx, kind)
}

func (c *Client) ResumePeriodicJob(ctx context.Context, kind string) error {
	return c.driver.ResumePeriodicJob(ctx, kind)
}

func (c *Client) prepareJobArgs(data []byte, encrypted bool) ([]byte, bool, error) {
	if !encrypted {
		return data, false, nil
	}
	if c.cfg.KeyProvider == nil {
		return nil, false, ErrNoKeyProvider
	}
	ciphertext, err := c.cfg.KeyProvider.Encrypt(data)
	if err != nil {
		return nil, false, err
	}
	stored, err := json.Marshal(ciphertext)
	if err != nil {
		return nil, false, err
	}
	return stored, true, nil
}

func (c *Client) Enqueue(ctx context.Context, args JobArgs, opts ...EnqueueOption) (*JobRow, error) {
	o := applyEnqueueOptions(opts)
	data, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	argsData, encrypted, err := c.prepareJobArgs(data, o.encrypted)
	if err != nil {
		return nil, err
	}
	job, err := c.driver.Enqueue(ctx, driver.EnqueueParams{
		Queue:       o.queue,
		Kind:        args.Kind(),
		Args:        argsData,
		Priority:    o.priority,
		MaxAttempts: o.maxAttempts,
		ScheduledAt: o.scheduledAt,
		UniqueKey:   o.uniqueKey,
		Tags:        o.tags,
		Metadata:    o.metadata,
		Encrypted:   encrypted,
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
	argsData, encrypted, err := c.prepareJobArgs(data, o.encrypted)
	if err != nil {
		return nil, err
	}
	job, err := c.driver.EnqueueTx(ctx, tx, driver.EnqueueParams{
		Queue:       o.queue,
		Kind:        args.Kind(),
		Args:        argsData,
		Priority:    o.priority,
		MaxAttempts: o.maxAttempts,
		ScheduledAt: o.scheduledAt,
		UniqueKey:   o.uniqueKey,
		Tags:        o.tags,
		Metadata:    o.metadata,
		Encrypted:   encrypted,
	})
	if err != nil {
		return nil, err
	}
	row := driverJobToRow(job)
	return &row, nil
}

// EnqueueItem is one job in an EnqueueMany batch with per-item options.
type EnqueueItem struct {
	Args JobArgs
	Opts []EnqueueOption
}

// EnqueueMany inserts multiple jobs in a single transaction. All jobs commit
// together or none do; the first error (including ErrUniqueConflict) rolls back
// the entire batch.
func (c *Client) EnqueueMany(ctx context.Context, items []EnqueueItem) ([]JobRow, error) {
	if len(items) == 0 {
		return nil, nil
	}
	params := make([]driver.EnqueueParams, len(items))
	for i, item := range items {
		o := applyEnqueueOptions(item.Opts)
		if o.uniqueKey != nil {
			return nil, fmt.Errorf("%w: WithUniqueKey is not supported in EnqueueMany; enqueue jobs individually", ErrInvalidConfig)
		}
		data, err := json.Marshal(item.Args)
		if err != nil {
			return nil, err
		}
		argsData, encrypted, err := c.prepareJobArgs(data, o.encrypted)
		if err != nil {
			return nil, err
		}
		params[i] = driver.EnqueueParams{
			Queue:       o.queue,
			Kind:        item.Args.Kind(),
			Args:        argsData,
			Priority:    o.priority,
			MaxAttempts: o.maxAttempts,
			ScheduledAt: o.scheduledAt,
			UniqueKey:   o.uniqueKey,
			Tags:        o.tags,
			Metadata:    o.metadata,
			Encrypted:   encrypted,
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

// EnqueueSequence inserts an ordered chain of jobs atomically. Step 0 is
// immediately fetchable; later steps become pending one at a time as each
// preceding step is acked.
func (c *Client) EnqueueSequence(ctx context.Context, items []EnqueueItem) (string, error) {
	if len(items) == 0 {
		return "", fmt.Errorf("%w: sequence must have at least one step", ErrInvalidConfig)
	}
	params := make([]driver.EnqueueParams, len(items))
	for i, item := range items {
		o := applyEnqueueOptions(item.Opts)
		if o.uniqueKey != nil {
			return "", fmt.Errorf("%w: WithUniqueKey is not supported in EnqueueSequence", ErrInvalidConfig)
		}
		data, err := json.Marshal(item.Args)
		if err != nil {
			return "", err
		}
		argsData, encrypted, err := c.prepareJobArgs(data, o.encrypted)
		if err != nil {
			return "", err
		}
		params[i] = driver.EnqueueParams{
			Queue:       o.queue,
			Kind:        item.Args.Kind(),
			Args:        argsData,
			Priority:    o.priority,
			MaxAttempts: o.maxAttempts,
			ScheduledAt: o.scheduledAt,
			UniqueKey:   o.uniqueKey,
			Tags:        o.tags,
			Metadata:    o.metadata,
			Encrypted:   encrypted,
		}
	}
	return c.driver.EnqueueSequence(ctx, params)
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

func (c *Client) ListDeadJobs(ctx context.Context, limit, offset int) ([]JobRow, error) {
	jobs, err := c.driver.ListDead(ctx, limit, offset)
	if err != nil {
		return nil, err
	}
	rows := make([]JobRow, len(jobs))
	for i, job := range jobs {
		rows[i] = driverJobToRow(job)
	}
	return rows, nil
}

func (c *Client) ReplayDeadJob(ctx context.Context, jobID int64) error {
	return c.driver.ReplayDead(ctx, jobID)
}

func (c *Client) PurgeDeadJobs(ctx context.Context, before time.Time) (int64, error) {
	return c.driver.PurgeDead(ctx, before)
}

// Cancel cancels a pending or scheduled job.
func (c *Client) Cancel(ctx context.Context, jobID int64) error {
	return c.driver.Cancel(ctx, jobID)
}

// RunJobNow promotes a scheduled job to pending immediately.
func (c *Client) RunJobNow(ctx context.Context, jobID int64) error {
	return c.driver.RunJobNow(ctx, jobID)
}

// EnqueueRawParams configures a raw-args enqueue for admin tooling.
type EnqueueRawParams struct {
	Kind        string
	Queue       string
	Args        json.RawMessage
	Priority    int16
	MaxAttempts int16
	ScheduledAt *time.Time
	UniqueKey   *string
	Tags        []string
	Metadata    json.RawMessage
	Encrypted   bool
}

// EnqueueRaw enqueues a job with raw JSON args (no typed JobArgs).
func (c *Client) EnqueueRaw(ctx context.Context, p EnqueueRawParams) (*JobRow, error) {
	if p.Kind == "" {
		return nil, fmt.Errorf("%w: kind is required", ErrInvalidConfig)
	}
	args := []byte(p.Args)
	if len(args) == 0 {
		args = []byte("{}")
	}
	metadata := []byte(p.Metadata)
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}
	argsData, encrypted, err := c.prepareJobArgs(args, p.Encrypted)
	if err != nil {
		return nil, err
	}
	queue := p.Queue
	if queue == "" {
		queue = QueueDefault
	}
	job, err := c.driver.Enqueue(ctx, driver.EnqueueParams{
		Queue:       queue,
		Kind:        p.Kind,
		Args:        argsData,
		Priority:    p.Priority,
		MaxAttempts: p.MaxAttempts,
		ScheduledAt: p.ScheduledAt,
		UniqueKey:   p.UniqueKey,
		Tags:        p.Tags,
		Metadata:    metadata,
		Encrypted:   encrypted,
	})
	if err != nil {
		return nil, err
	}
	row := driverJobToRow(job)
	return &row, nil
}

// AddPeriodicJobRaw registers a cron schedule with raw JSON args.
func (c *Client) AddPeriodicJobRaw(ctx context.Context, cronExpr, kind, queue string, args []byte, maxAttempts int16) error {
	if kind == "" {
		return fmt.Errorf("%w: kind is required", ErrInvalidConfig)
	}
	if len(args) == 0 {
		args = []byte("{}")
	}
	if queue == "" {
		queue = QueueDefault
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.periodic != nil {
		return c.periodic.Register(ctx, cronExpr, kind, args, queue, maxAttempts)
	}
	c.pendingPeriodic = append(c.pendingPeriodic, pendingPeriodic{
		cronExpr:    cronExpr,
		kind:        kind,
		data:        args,
		queue:       queue,
		maxAttempts: maxAttempts,
	})
	return nil
}

// ListConcurrencySlots returns all concurrency slot rows from the database.
func (c *Client) ListConcurrencySlots(ctx context.Context) ([]driver.ConcurrencySlot, error) {
	slots, err := c.driver.ListConcurrencySlots(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]driver.ConcurrencySlot, len(slots))
	for i, s := range slots {
		out[i] = *s
	}
	return out, nil
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

// ListWorkers returns live processing clients registered in the fleet.
func (c *Client) ListWorkers(ctx context.Context) ([]WorkerInstance, error) {
	workers, err := c.driver.ListWorkers(ctx, c.cfg.WorkerTTL)
	if err != nil {
		return nil, err
	}
	out := make([]WorkerInstance, len(workers))
	for i, w := range workers {
		out[i] = *w
	}
	return out, nil
}

// QueueWorkerCapacity returns the number of live worker instances and total
// concurrent job capacity for the given queue across the fleet.
func (c *Client) QueueWorkerCapacity(ctx context.Context, queue string) (instances, maxConcurrent int, err error) {
	workers, err := c.ListWorkers(ctx)
	if err != nil {
		return 0, 0, err
	}
	for _, w := range workers {
		if cap, ok := w.Queues[queue]; ok && cap > 0 {
			instances++
			maxConcurrent += cap
		}
	}
	return instances, maxConcurrent, nil
}

// UniqueJobExists reports whether an active job with the given unique key exists.
func (c *Client) UniqueJobExists(ctx context.Context, uniqueKey string) (bool, error) {
	return c.driver.UniqueJobExists(ctx, uniqueKey)
}

func (c *Client) Migrate(ctx context.Context) error {
	return c.driver.Migrate(ctx)
}

// SetConcurrencyLimit configures a per-kind concurrency cap. PartitionKeyFn is held
// in memory only and is not persisted; each worker process must call this on startup.
func (c *Client) SetConcurrencyLimit(ctx context.Context, cfg ConcurrencyLimitConfig) error {
	if err := c.driver.SetConcurrencyLimit(ctx, driver.ConcurrencyLimit{
		Kind:          cfg.Kind,
		MaxConcurrent: cfg.MaxConcurrent,
	}); err != nil {
		return err
	}

	c.concurrencyMu.Lock()
	if c.partitionKeyFns == nil {
		c.partitionKeyFns = make(map[string]func(args []byte) string)
	}
	if cfg.PartitionKeyFn != nil {
		c.partitionKeyFns[cfg.Kind] = cfg.PartitionKeyFn
	} else {
		delete(c.partitionKeyFns, cfg.Kind)
	}
	c.concurrencyMu.Unlock()

	c.driver.RegisterConcurrencyLimit(cfg.Kind, cfg.MaxConcurrent, cfg.PartitionKeyFn != nil)
	return nil
}

func (c *Client) partitionKeyFn(kind string) func(args []byte) string {
	c.concurrencyMu.RLock()
	defer c.concurrencyMu.RUnlock()
	return c.partitionKeyFns[kind]
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

	args := dJob.Args
	if dJob.Encrypted {
		if c.cfg.KeyProvider == nil {
			return c.nackJob(ctx, dJob, ErrNoKeyProvider, time.Now().Add(5*time.Second))
		}
		var ciphertext []byte
		if err := json.Unmarshal(dJob.Args, &ciphertext); err != nil {
			return c.nackJob(ctx, dJob, err, time.Now().Add(5*time.Second))
		}
		plaintext, err := c.cfg.KeyProvider.Decrypt(ciphertext)
		if err != nil {
			return c.nackJob(ctx, dJob, err, time.Now().Add(5*time.Second))
		}
		args = plaintext
	}

	// Partitioned limits acquire here; global limits acquire atomically in driver Fetch SQL. All releases in Ack/Nack.
	if fn := c.partitionKeyFn(dJob.Kind); fn != nil {
		partitionKey := fn(args)
		acquired, err := c.driver.AcquireConcurrencySlotForJob(ctx, dJob.ID, dJob.Kind, partitionKey)
		if err != nil {
			return err
		}
		if !acquired {
			return c.nackJob(ctx, dJob, ErrConcurrencySlotUnavailable, time.Now().Add(5*time.Second))
		}
	}

	maxWorkers := 0
	if qc, ok := c.cfg.Queues[dJob.Queue]; ok {
		maxWorkers = qc.MaxWorkers
	}
	wrap := &driverJobWrapper{
		id:          dJob.ID,
		queue:       dJob.Queue,
		kind:        dJob.Kind,
		args:        args,
		attempt:     dJob.Attempt,
		maxAttempts: dJob.MaxAttempts,
		attemptedBy: dJob.AttemptedBy,
		workerID:    c.cfg.WorkerID,
		maxWorkers:  maxWorkers,
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

func processingQueues(queues map[string]QueueConfig) map[string]int {
	out := make(map[string]int)
	for name, qc := range queues {
		if qc.MaxWorkers > 0 {
			out[name] = qc.MaxWorkers
		}
	}
	return out
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
		DiedAt:      job.DiedAt,
		ErrorTrace:  json.RawMessage(job.ErrorTrace),
		Tags:        job.Tags,
		UniqueKey:   job.UniqueKey,
		Metadata:    json.RawMessage(job.Metadata),
	}
}

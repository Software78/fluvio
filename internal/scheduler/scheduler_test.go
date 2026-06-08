package scheduler_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/software78/fluvio/internal/driver"
	"github.com/software78/fluvio/internal/scheduler"
)

type fakeTx struct{}

type recordingDriver struct {
	mu           sync.Mutex
	enqueued     []string
	periodicJobs map[string]*driver.PeriodicJob
}

func newRecordingDriver() *recordingDriver {
	return &recordingDriver{periodicJobs: make(map[string]*driver.PeriodicJob)}
}

func (d *recordingDriver) Enqueue(_ context.Context, p driver.EnqueueParams) (*driver.Job, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.enqueued = append(d.enqueued, p.Kind)
	return &driver.Job{Kind: p.Kind}, nil
}

func (d *recordingDriver) EnqueueTx(_ context.Context, _ driver.Tx, p driver.EnqueueParams) (*driver.Job, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.enqueued = append(d.enqueued, p.Kind)
	return &driver.Job{Kind: p.Kind}, nil
}

func (d *recordingDriver) enqueuedKinds() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.enqueued...)
}

func (d *recordingDriver) UpsertPeriodicJob(_ context.Context, kind, cron, queue string, maxAttempts int16, args []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if queue == "" {
		queue = driver.QueueDefault
	}
	if maxAttempts == 0 {
		maxAttempts = 3
	}
	if len(args) == 0 {
		args = []byte("{}")
	}
	if existing, ok := d.periodicJobs[kind]; ok {
		existing.Cron = cron
		existing.Queue = queue
		existing.MaxAttempts = maxAttempts
		existing.Args = args
	} else {
		d.periodicJobs[kind] = &driver.PeriodicJob{
			Kind:        kind,
			Cron:        cron,
			Queue:       queue,
			MaxAttempts: maxAttempts,
			Args:        args,
			NextRunAt:   time.Now().UTC(),
		}
	}
	return nil
}

func (d *recordingDriver) UpdatePeriodicJobNextRun(_ context.Context, kind string, nextRun time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if j, ok := d.periodicJobs[kind]; ok {
		j.NextRunAt = nextRun
	}
	return nil
}

func (d *recordingDriver) DuePeriodicJobs(_ context.Context, now time.Time) ([]*driver.PeriodicJob, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var due []*driver.PeriodicJob
	for _, j := range d.periodicJobs {
		if !j.Paused && !j.NextRunAt.After(now) {
			dup := *j
			due = append(due, &dup)
		}
	}
	return due, nil
}

func (d *recordingDriver) UpdatePeriodicJobNextRunTx(_ context.Context, _ driver.Tx, kind string, nextRun time.Time) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	j, ok := d.periodicJobs[kind]
	if !ok || j.Paused || j.NextRunAt.After(time.Now().UTC()) {
		return false, nil
	}
	now := time.Now().UTC()
	j.NextRunAt = nextRun
	j.LastRunAt = &now
	return true, nil
}

func (d *recordingDriver) BeginTx(context.Context) (driver.Tx, error)  { return fakeTx{}, nil }
func (d *recordingDriver) CommitTx(context.Context, driver.Tx) error   { return nil }
func (d *recordingDriver) RollbackTx(context.Context, driver.Tx) error { return nil }

func (d *recordingDriver) ListPeriodicJobs(context.Context) ([]*driver.PeriodicJob, error) {
	return nil, nil
}
func (d *recordingDriver) PausePeriodicJob(context.Context, string) error  { return nil }
func (d *recordingDriver) ResumePeriodicJob(context.Context, string) error { return nil }

func (d *recordingDriver) Ack(context.Context, int64) error                    { return nil }
func (d *recordingDriver) Nack(context.Context, int64, error, time.Time) error { return nil }
func (d *recordingDriver) EnqueueMany(context.Context, []driver.EnqueueParams) ([]*driver.Job, error) {
	return nil, nil
}
func (d *recordingDriver) Fetch(context.Context, []string, string, int) ([]*driver.Job, error) {
	return nil, nil
}
func (d *recordingDriver) Cancel(context.Context, int64) error                { return nil }
func (d *recordingDriver) GetJob(context.Context, int64) (*driver.Job, error) { return nil, nil }
func (d *recordingDriver) ListJobs(context.Context, driver.ListJobsParams) ([]*driver.Job, error) {
	return nil, nil
}
func (d *recordingDriver) ListDead(context.Context, int, int) ([]*driver.Job, error) { return nil, nil }
func (d *recordingDriver) ReplayDead(context.Context, int64) error                   { return nil }
func (d *recordingDriver) PurgeDead(context.Context, time.Time) (int64, error)       { return 0, nil }
func (d *recordingDriver) TickScheduled(context.Context, time.Time) (int64, error)   { return 0, nil }
func (d *recordingDriver) UniqueJobExists(context.Context, string) (bool, error)     { return false, nil }
func (d *recordingDriver) PauseQueue(context.Context, string) error                  { return nil }
func (d *recordingDriver) ResumeQueue(context.Context, string) error                 { return nil }
func (d *recordingDriver) IsQueuePaused(context.Context, string) (bool, error)       { return false, nil }
func (d *recordingDriver) QueueStats(context.Context, string) (*driver.QueueStats, error) {
	return nil, nil
}
func (d *recordingDriver) ListQueues(context.Context) ([]*driver.QueueStats, error) { return nil, nil }
func (d *recordingDriver) TryAcquireLeader(context.Context) (bool, error)           { return false, nil }
func (d *recordingDriver) VerifyLeader(context.Context) error                         { return nil }
func (d *recordingDriver) ReleaseLeader(context.Context) error                      { return nil }
func (d *recordingDriver) StuckJobs(context.Context, time.Duration) ([]*driver.Job, error) {
	return nil, nil
}
func (d *recordingDriver) UpsertWorker(context.Context, string, map[string]int) error { return nil }
func (d *recordingDriver) RemoveWorker(context.Context, string) error                 { return nil }
func (d *recordingDriver) ListWorkers(context.Context, time.Duration) ([]*driver.WorkerInstance, error) {
	return nil, nil
}
func (d *recordingDriver) Migrate(context.Context) error                     { return nil }
func (d *recordingDriver) MigrateDown(context.Context, int) error            { return nil }
func (d *recordingDriver) MigrationStatus(context.Context) ([]string, error) { return nil, nil }
func (d *recordingDriver) SetConcurrencyLimit(context.Context, driver.ConcurrencyLimit) error {
	return nil
}
func (d *recordingDriver) AcquireConcurrencySlot(context.Context, string, string) (bool, error) {
	return true, nil
}
func (d *recordingDriver) ReleaseConcurrencySlot(context.Context, string, string) error { return nil }
func (d *recordingDriver) SetConcurrencySlotKey(context.Context, int64, string) error   { return nil }
func (d *recordingDriver) CreateWorkflow(context.Context, *driver.WorkflowRecord) error { return nil }
func (d *recordingDriver) CompleteWorkflowTask(context.Context, driver.Tx, string, string) error {
	return nil
}
func (d *recordingDriver) FailWorkflowTask(context.Context, string, string) error { return nil }
func (d *recordingDriver) GetWorkflow(context.Context, string) (*driver.WorkflowState, error) {
	return nil, nil
}
func (d *recordingDriver) ListWorkflows(context.Context, int, int) ([]*driver.WorkflowState, error) {
	return nil, nil
}
func (d *recordingDriver) Close() error { return nil }

func TestPeriodicDistinctKindsWithColon(t *testing.T) {
	rd := newRecordingDriver()
	p := scheduler.NewPeriodic(rd, slog.Default(), time.Millisecond, 0)
	ctx := context.Background()
	require.NoError(t, p.Register(ctx, "* * * * *", "foo:bar", []byte(`{}`), "default", 3))
	require.NoError(t, p.Register(ctx, "* * * * *", "foo", []byte(`{}`), "default", 3))

	// Force jobs due and tick manually.
	rd.mu.Lock()
	rd.periodicJobs["foo:bar"].NextRunAt = time.Now().Add(-time.Minute)
	rd.periodicJobs["foo"].NextRunAt = time.Now().Add(-time.Minute)
	rd.mu.Unlock()

	p.Tick(ctx, time.Now().UTC())

	kinds := rd.enqueuedKinds()
	require.Len(t, kinds, 2)
	require.ElementsMatch(t, []string{"foo:bar", "foo"}, kinds)
}

func TestPeriodicRegisterRejectsInvalidCron(t *testing.T) {
	p := scheduler.NewPeriodic(newRecordingDriver(), slog.Default(), time.Second, 0)
	require.Error(t, p.Register(context.Background(), "not a cron", "k", nil, "default", 3))
}

func TestPeriodicTickNoDoubleEnqueueUnit(t *testing.T) {
	rd := newRecordingDriver()
	p := scheduler.NewPeriodic(rd, slog.Default(), time.Second, 0)
	ctx := context.Background()
	require.NoError(t, p.Register(ctx, "* * * * *", "tick-once", []byte(`{}`), "default", 3))

	rd.mu.Lock()
	rd.periodicJobs["tick-once"].NextRunAt = time.Now().Add(-time.Minute)
	rd.mu.Unlock()

	now := time.Now().UTC()
	p.Tick(ctx, now)
	p.Tick(ctx, now)

	require.Len(t, rd.enqueuedKinds(), 1)
}

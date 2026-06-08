package maintenance_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/software78/fluvio/internal/driver"
	"github.com/software78/fluvio/internal/maintenance"
)

type reaperDriver struct {
	jobs []*driver.Job
}

func (d *reaperDriver) StuckJobs(context.Context, time.Duration) ([]*driver.Job, error) {
	return d.jobs, nil
}

func (d *reaperDriver) Enqueue(context.Context, driver.EnqueueParams) (*driver.Job, error) {
	return nil, nil
}
func (d *reaperDriver) EnqueueTx(context.Context, driver.Tx, driver.EnqueueParams) (*driver.Job, error) {
	return nil, nil
}
func (d *reaperDriver) EnqueueMany(context.Context, []driver.EnqueueParams) ([]*driver.Job, error) {
	return nil, nil
}
func (d *reaperDriver) Fetch(context.Context, []string, string, int) ([]*driver.Job, error) {
	return nil, nil
}
func (d *reaperDriver) Ack(context.Context, int64) error                    { return nil }
func (d *reaperDriver) Nack(context.Context, int64, error, time.Time) error { return nil }
func (d *reaperDriver) Cancel(context.Context, int64) error                 { return nil }
func (d *reaperDriver) GetJob(context.Context, int64) (*driver.Job, error)  { return nil, nil }
func (d *reaperDriver) ListJobs(context.Context, driver.ListJobsParams) ([]*driver.Job, error) {
	return nil, nil
}
func (d *reaperDriver) ListDead(context.Context, int, int) ([]*driver.Job, error) { return nil, nil }
func (d *reaperDriver) ReplayDead(context.Context, int64) error                   { return nil }
func (d *reaperDriver) PurgeDead(context.Context, time.Time) (int64, error)       { return 0, nil }
func (d *reaperDriver) TickScheduled(context.Context, time.Time) (int64, error)   { return 0, nil }
func (d *reaperDriver) UpsertPeriodicJob(context.Context, string, string, string, int16, []byte) error {
	return nil
}
func (d *reaperDriver) DuePeriodicJobs(context.Context, time.Time) ([]*driver.PeriodicJob, error) {
	return nil, nil
}
func (d *reaperDriver) UpdatePeriodicJobNextRun(context.Context, string, time.Time) error { return nil }
func (d *reaperDriver) UpdatePeriodicJobNextRunTx(context.Context, driver.Tx, string, time.Time) (bool, error) {
	return false, nil
}
func (d *reaperDriver) ListPeriodicJobs(context.Context) ([]*driver.PeriodicJob, error) { return nil, nil }
func (d *reaperDriver) PausePeriodicJob(context.Context, string) error                    { return nil }
func (d *reaperDriver) ResumePeriodicJob(context.Context, string) error                   { return nil }
func (d *reaperDriver) BeginTx(context.Context) (driver.Tx, error)                      { return nil, nil }
func (d *reaperDriver) CommitTx(context.Context, driver.Tx) error                         { return nil }
func (d *reaperDriver) RollbackTx(context.Context, driver.Tx) error                      { return nil }
func (d *reaperDriver) UniqueJobExists(context.Context, string) (bool, error)     { return false, nil }
func (d *reaperDriver) PauseQueue(context.Context, string) error                  { return nil }
func (d *reaperDriver) ResumeQueue(context.Context, string) error                 { return nil }
func (d *reaperDriver) IsQueuePaused(context.Context, string) (bool, error)       { return false, nil }
func (d *reaperDriver) QueueStats(context.Context, string) (*driver.QueueStats, error) {
	return nil, nil
}
func (d *reaperDriver) ListQueues(context.Context) ([]*driver.QueueStats, error)   { return nil, nil }
func (d *reaperDriver) TryAcquireLeader(context.Context) (bool, error)             { return false, nil }
func (d *reaperDriver) RenewLeader(context.Context) error                          { return nil }
func (d *reaperDriver) ReleaseLeader(context.Context) error                        { return nil }
func (d *reaperDriver) UpsertWorker(context.Context, string, map[string]int) error { return nil }
func (d *reaperDriver) RemoveWorker(context.Context, string) error                 { return nil }
func (d *reaperDriver) ListWorkers(context.Context, time.Duration) ([]*driver.WorkerInstance, error) {
	return nil, nil
}
func (d *reaperDriver) Migrate(context.Context) error                     { return nil }
func (d *reaperDriver) MigrateDown(context.Context, int) error            { return nil }
func (d *reaperDriver) MigrationStatus(context.Context) ([]string, error) { return nil, nil }
func (d *reaperDriver) SetConcurrencyLimit(context.Context, driver.ConcurrencyLimit) error {
	return nil
}
func (d *reaperDriver) AcquireConcurrencySlot(context.Context, string, string) (bool, error) {
	return true, nil
}
func (d *reaperDriver) ReleaseConcurrencySlot(context.Context, string, string) error { return nil }
func (d *reaperDriver) CreateWorkflow(context.Context, *driver.WorkflowRecord) error { return nil }
func (d *reaperDriver) CompleteWorkflowTask(context.Context, driver.Tx, string, string) error {
	return nil
}
func (d *reaperDriver) FailWorkflowTask(context.Context, string, string) error { return nil }
func (d *reaperDriver) GetWorkflow(context.Context, string) (*driver.WorkflowState, error) {
	return nil, nil
}
func (d *reaperDriver) ListWorkflows(context.Context, int, int) ([]*driver.WorkflowState, error) {
	return nil, nil
}
func (d *reaperDriver) Close() error { return nil }

func TestReaperAppliesRetryBackoff(t *testing.T) {
	rd := &reaperDriver{jobs: []*driver.Job{{ID: 1, Attempt: 2}}}

	var mu sync.Mutex
	var nextAt time.Time
	nack := func(_ context.Context, job *driver.Job, _ error, at time.Time) error {
		mu.Lock()
		nextAt = at
		mu.Unlock()
		return nil
	}

	retryDelay := func(attempt int16, maxDelay time.Duration) time.Duration {
		return time.Duration(attempt) * time.Minute
	}

	r := maintenance.NewReaper(rd, slog.Default(), time.Minute, time.Millisecond, 24*time.Hour, retryDelay, nack)
	r.Start()
	time.Sleep(20 * time.Millisecond)
	r.Stop()

	mu.Lock()
	defer mu.Unlock()
	require.False(t, nextAt.IsZero())
	require.Greater(t, time.Until(nextAt), time.Minute)
}

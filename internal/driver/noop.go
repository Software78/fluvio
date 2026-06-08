package driver

import (
	"context"
	"time"
)

// NoopDriver implements Driver with zero-value returns and nil errors.
// Embed it in test mocks and override only the methods under test.
type NoopDriver struct{}

func (NoopDriver) Enqueue(context.Context, EnqueueParams) (*Job, error) {
	return nil, nil
}

func (NoopDriver) EnqueueTx(context.Context, Tx, EnqueueParams) (*Job, error) {
	return nil, nil
}

func (NoopDriver) EnqueueMany(context.Context, []EnqueueParams) ([]*Job, error) {
	return nil, nil
}

func (NoopDriver) Fetch(context.Context, []string, string, int) ([]*Job, error) {
	return nil, nil
}

func (NoopDriver) Ack(context.Context, int64) error { return nil }

func (NoopDriver) Nack(context.Context, int64, error, time.Time) error { return nil }

func (NoopDriver) Cancel(context.Context, int64) error { return nil }

func (NoopDriver) GetJob(context.Context, int64) (*Job, error) {
	return nil, nil
}

func (NoopDriver) ListJobs(context.Context, ListJobsParams) ([]*Job, error) {
	return nil, nil
}

func (NoopDriver) ListDead(context.Context, int, int) ([]*Job, error) {
	return nil, nil
}

func (NoopDriver) ReplayDead(context.Context, int64) error { return nil }

func (NoopDriver) PurgeDead(context.Context, time.Time) (int64, error) {
	return 0, nil
}

func (NoopDriver) TickScheduled(context.Context, time.Time) (int64, error) {
	return 0, nil
}

func (NoopDriver) UpsertPeriodicJob(context.Context, string, string, string, int16, []byte, time.Time) error {
	return nil
}

func (NoopDriver) DuePeriodicJobs(context.Context, time.Time) ([]*PeriodicJob, error) {
	return nil, nil
}

func (NoopDriver) UpdatePeriodicJobNextRun(context.Context, string, time.Time) error {
	return nil
}

func (NoopDriver) UpdatePeriodicJobNextRunTx(context.Context, Tx, string, time.Time) (bool, error) {
	return false, nil
}

func (NoopDriver) ListPeriodicJobs(context.Context) ([]*PeriodicJob, error) {
	return nil, nil
}

func (NoopDriver) PausePeriodicJob(context.Context, string) error { return nil }

func (NoopDriver) ResumePeriodicJob(context.Context, string) error { return nil }

func (NoopDriver) BeginTx(context.Context) (Tx, error) {
	return nil, nil
}

func (NoopDriver) CommitTx(context.Context, Tx) error { return nil }

func (NoopDriver) RollbackTx(context.Context, Tx) error { return nil }

func (NoopDriver) UniqueJobExists(context.Context, string) (bool, error) {
	return false, nil
}

func (NoopDriver) PauseQueue(context.Context, string) error { return nil }

func (NoopDriver) ResumeQueue(context.Context, string) error { return nil }

func (NoopDriver) IsQueuePaused(context.Context, string) (bool, error) {
	return false, nil
}

func (NoopDriver) QueueStats(context.Context, string) (*QueueStats, error) {
	return nil, nil
}

func (NoopDriver) ListQueues(context.Context) ([]*QueueStats, error) {
	return nil, nil
}

func (NoopDriver) TryAcquireLeader(context.Context) (bool, error) {
	return false, nil
}

func (NoopDriver) VerifyLeader(context.Context) error { return nil }

func (NoopDriver) ReleaseLeader(context.Context) error { return nil }

func (NoopDriver) StuckJobs(context.Context, time.Duration) ([]*Job, error) {
	return nil, nil
}

func (NoopDriver) UpsertWorker(context.Context, string, map[string]int) error {
	return nil
}

func (NoopDriver) RemoveWorker(context.Context, string) error { return nil }

func (NoopDriver) ListWorkers(context.Context, time.Duration) ([]*WorkerInstance, error) {
	return nil, nil
}

func (NoopDriver) Migrate(context.Context) error { return nil }

func (NoopDriver) MigrateDown(context.Context, int) error { return nil }

func (NoopDriver) MigrationStatus(context.Context) ([]string, error) {
	return nil, nil
}

func (NoopDriver) Close() error { return nil }

func (NoopDriver) SetConcurrencyLimit(context.Context, ConcurrencyLimit) error {
	return nil
}

func (NoopDriver) RegisterConcurrencyLimit(string, int, bool) {}

func (NoopDriver) AcquireConcurrencySlot(context.Context, string, string) (bool, error) {
	return true, nil
}

func (NoopDriver) AcquireConcurrencySlotForJob(context.Context, int64, string, string) (bool, error) {
	return true, nil
}

func (NoopDriver) ReleaseConcurrencySlot(context.Context, string, string) error {
	return nil
}

func (NoopDriver) SetConcurrencySlotKey(context.Context, int64, string) error {
	return nil
}

func (NoopDriver) CreateWorkflow(context.Context, *WorkflowRecord) error {
	return nil
}

func (NoopDriver) CompleteWorkflowTask(context.Context, Tx, string, string) error {
	return nil
}

func (NoopDriver) FailWorkflowTask(context.Context, string, string) error {
	return nil
}

func (NoopDriver) GetWorkflow(context.Context, string) (*WorkflowState, error) {
	return nil, nil
}

func (NoopDriver) ListWorkflows(context.Context, int, int) ([]*WorkflowState, error) {
	return nil, nil
}

var _ Driver = NoopDriver{}

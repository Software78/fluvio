package fluvio_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	fluvio "github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
)

type concurrencyRegisterCall struct {
	kind          string
	maxConcurrent int
	partitioned   bool
}

// concurrencyRecordingDriver implements driver.Driver but does not embed the old
// optional concurrencyRegistrar interface — registration is required via Driver.
type concurrencyRecordingDriver struct {
	mu    sync.Mutex
	calls []concurrencyRegisterCall
}

func (d *concurrencyRecordingDriver) RegisterConcurrencyLimit(kind string, maxConcurrent int, partitioned bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, concurrencyRegisterCall{kind, maxConcurrent, partitioned})
}

func (d *concurrencyRecordingDriver) registerCalls() []concurrencyRegisterCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]concurrencyRegisterCall, len(d.calls))
	copy(out, d.calls)
	return out
}

func (d *concurrencyRecordingDriver) SetConcurrencyLimit(_ context.Context, limit driver.ConcurrencyLimit) error {
	d.RegisterConcurrencyLimit(limit.Kind, limit.MaxConcurrent, false)
	return nil
}

func (d *concurrencyRecordingDriver) Fetch(context.Context, []string, string, int) ([]*driver.Job, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) Ack(context.Context, int64) error                    { return nil }
func (d *concurrencyRecordingDriver) Nack(context.Context, int64, error, time.Time) error { return nil }
func (d *concurrencyRecordingDriver) Enqueue(context.Context, driver.EnqueueParams) (*driver.Job, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) EnqueueTx(context.Context, driver.Tx, driver.EnqueueParams) (*driver.Job, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) EnqueueMany(context.Context, []driver.EnqueueParams) ([]*driver.Job, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) Cancel(context.Context, int64) error { return nil }
func (d *concurrencyRecordingDriver) GetJob(context.Context, int64) (*driver.Job, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) ListJobs(context.Context, driver.ListJobsParams) ([]*driver.Job, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) ListDead(context.Context, int, int) ([]*driver.Job, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) ReplayDead(context.Context, int64) error             { return nil }
func (d *concurrencyRecordingDriver) PurgeDead(context.Context, time.Time) (int64, error) { return 0, nil }
func (d *concurrencyRecordingDriver) TickScheduled(context.Context, time.Time) (int64, error) {
	return 0, nil
}
func (d *concurrencyRecordingDriver) UpsertPeriodicJob(context.Context, string, string, string, int16, []byte, time.Time) error {
	return nil
}
func (d *concurrencyRecordingDriver) DuePeriodicJobs(context.Context, time.Time) ([]*driver.PeriodicJob, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) UpdatePeriodicJobNextRun(context.Context, string, time.Time) error {
	return nil
}
func (d *concurrencyRecordingDriver) UpdatePeriodicJobNextRunTx(context.Context, driver.Tx, string, time.Time) (bool, error) {
	return false, nil
}
func (d *concurrencyRecordingDriver) ListPeriodicJobs(context.Context) ([]*driver.PeriodicJob, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) PausePeriodicJob(context.Context, string) error  { return nil }
func (d *concurrencyRecordingDriver) ResumePeriodicJob(context.Context, string) error { return nil }
func (d *concurrencyRecordingDriver) BeginTx(context.Context) (driver.Tx, error)      { return nil, nil }
func (d *concurrencyRecordingDriver) CommitTx(context.Context, driver.Tx) error         { return nil }
func (d *concurrencyRecordingDriver) RollbackTx(context.Context, driver.Tx) error       { return nil }
func (d *concurrencyRecordingDriver) UniqueJobExists(context.Context, string) (bool, error) {
	return false, nil
}
func (d *concurrencyRecordingDriver) PauseQueue(context.Context, string) error            { return nil }
func (d *concurrencyRecordingDriver) ResumeQueue(context.Context, string) error           { return nil }
func (d *concurrencyRecordingDriver) IsQueuePaused(context.Context, string) (bool, error) { return false, nil }
func (d *concurrencyRecordingDriver) QueueStats(context.Context, string) (*driver.QueueStats, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) ListQueues(context.Context) ([]*driver.QueueStats, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) TryAcquireLeader(context.Context) (bool, error) { return false, nil }
func (d *concurrencyRecordingDriver) VerifyLeader(context.Context) error           { return nil }
func (d *concurrencyRecordingDriver) ReleaseLeader(context.Context) error          { return nil }
func (d *concurrencyRecordingDriver) StuckJobs(context.Context, time.Duration) ([]*driver.Job, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) UpsertWorker(context.Context, string, map[string]int) error {
	return nil
}
func (d *concurrencyRecordingDriver) RemoveWorker(context.Context, string) error { return nil }
func (d *concurrencyRecordingDriver) ListWorkers(context.Context, time.Duration) ([]*driver.WorkerInstance, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) Migrate(context.Context) error                     { return nil }
func (d *concurrencyRecordingDriver) MigrateDown(context.Context, int) error            { return nil }
func (d *concurrencyRecordingDriver) MigrationStatus(context.Context) ([]string, error) { return nil, nil }
func (d *concurrencyRecordingDriver) AcquireConcurrencySlot(context.Context, string, string) (bool, error) {
	return true, nil
}
func (d *concurrencyRecordingDriver) AcquireConcurrencySlotForJob(context.Context, int64, string, string) (bool, error) {
	return true, nil
}
func (d *concurrencyRecordingDriver) ReleaseConcurrencySlot(context.Context, string, string) error {
	return nil
}
func (d *concurrencyRecordingDriver) SetConcurrencySlotKey(context.Context, int64, string) error {
	return nil
}
func (d *concurrencyRecordingDriver) CreateWorkflow(context.Context, *driver.WorkflowRecord) error {
	return nil
}
func (d *concurrencyRecordingDriver) CompleteWorkflowTask(context.Context, driver.Tx, string, string) error {
	return nil
}
func (d *concurrencyRecordingDriver) FailWorkflowTask(context.Context, string, string) error { return nil }
func (d *concurrencyRecordingDriver) GetWorkflow(context.Context, string) (*driver.WorkflowState, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) ListWorkflows(context.Context, int, int) ([]*driver.WorkflowState, error) {
	return nil, nil
}
func (d *concurrencyRecordingDriver) Close() error { return nil }

func newConcurrencyTestClient(t *testing.T, d driver.Driver) *fluvio.Client {
	t.Helper()
	client, err := fluvio.NewClient(d, &fluvio.Config{Workers: fluvio.NewWorkers()})
	require.NoError(t, err)
	return client
}

func TestSetConcurrencyLimitRegistersInMemoryLimit(t *testing.T) {
	t.Parallel()

	d := &concurrencyRecordingDriver{}
	client := newConcurrencyTestClient(t, d)

	err := client.SetConcurrencyLimit(context.Background(), fluvio.ConcurrencyLimitConfig{
		Kind:          "slow_job",
		MaxConcurrent: 2,
	})
	require.NoError(t, err)

	calls := d.registerCalls()
	require.Len(t, calls, 2, "SetConcurrencyLimit persists via driver then registers in-memory limit")
	require.Equal(t, concurrencyRegisterCall{"slow_job", 2, false}, calls[0])
	require.Equal(t, concurrencyRegisterCall{"slow_job", 2, false}, calls[1])
}

func TestSetConcurrencyLimitRegistersPartitionedLimit(t *testing.T) {
	t.Parallel()

	d := &concurrencyRecordingDriver{}
	client := newConcurrencyTestClient(t, d)

	partitionFn := func([]byte) string { return "tenant-a" }
	err := client.SetConcurrencyLimit(context.Background(), fluvio.ConcurrencyLimitConfig{
		Kind:           "tenant_job",
		MaxConcurrent:  3,
		PartitionKeyFn: partitionFn,
	})
	require.NoError(t, err)

	calls := d.registerCalls()
	require.Len(t, calls, 2)
	require.Equal(t, concurrencyRegisterCall{"tenant_job", 3, false}, calls[0])
	require.Equal(t, concurrencyRegisterCall{"tenant_job", 3, true}, calls[1])
}

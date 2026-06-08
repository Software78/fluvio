package leader

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/software78/fluvio/internal/driver"
)

var errLeaderLost = errors.New("leader lost")

type mockDriver struct {
	mu          sync.Mutex
	leader      bool
	renewCalls  int
	renewFailAt int
}

func (m *mockDriver) TryAcquireLeader(context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.leader {
		return false, nil
	}
	m.leader = true
	return true, nil
}

func (m *mockDriver) RenewLeader(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.renewCalls++
	if m.renewFailAt > 0 && m.renewCalls >= m.renewFailAt {
		m.leader = false
		return errLeaderLost
	}
	return nil
}

func (m *mockDriver) ReleaseLeader(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leader = false
	return nil
}

func (m *mockDriver) setRenewFailAt(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.renewFailAt = n
	m.renewCalls = 0
}

func (m *mockDriver) Enqueue(context.Context, driver.EnqueueParams) (*driver.Job, error) {
	return nil, nil
}
func (m *mockDriver) EnqueueTx(context.Context, driver.Tx, driver.EnqueueParams) (*driver.Job, error) {
	return nil, nil
}
func (m *mockDriver) EnqueueMany(context.Context, []driver.EnqueueParams) ([]*driver.Job, error) {
	return nil, nil
}
func (m *mockDriver) Fetch(context.Context, []string, string, int) ([]*driver.Job, error) {
	return nil, nil
}
func (m *mockDriver) Ack(context.Context, int64) error                    { return nil }
func (m *mockDriver) Nack(context.Context, int64, error, time.Time) error { return nil }
func (m *mockDriver) Cancel(context.Context, int64) error                 { return nil }
func (m *mockDriver) GetJob(context.Context, int64) (*driver.Job, error)  { return nil, nil }
func (m *mockDriver) ListJobs(context.Context, driver.ListJobsParams) ([]*driver.Job, error) {
	return nil, nil
}
func (m *mockDriver) ListDead(context.Context, int, int) ([]*driver.Job, error) { return nil, nil }
func (m *mockDriver) ReplayDead(context.Context, int64) error                   { return nil }
func (m *mockDriver) PurgeDead(context.Context, time.Time) (int64, error)       { return 0, nil }
func (m *mockDriver) TickScheduled(context.Context, time.Time) (int64, error)   { return 0, nil }
func (m *mockDriver) UpsertPeriodicJob(context.Context, string, string, string, int16, []byte) error {
	return nil
}
func (m *mockDriver) DuePeriodicJobs(context.Context, time.Time) ([]*driver.PeriodicJob, error) {
	return nil, nil
}
func (m *mockDriver) UpdatePeriodicJobNextRun(context.Context, string, time.Time) error { return nil }
func (m *mockDriver) UpdatePeriodicJobNextRunTx(context.Context, driver.Tx, string, time.Time) (bool, error) {
	return false, nil
}
func (m *mockDriver) ListPeriodicJobs(context.Context) ([]*driver.PeriodicJob, error) { return nil, nil }
func (m *mockDriver) PausePeriodicJob(context.Context, string) error                    { return nil }
func (m *mockDriver) ResumePeriodicJob(context.Context, string) error                   { return nil }
func (m *mockDriver) BeginTx(context.Context) (driver.Tx, error)                      { return nil, nil }
func (m *mockDriver) CommitTx(context.Context, driver.Tx) error                         { return nil }
func (m *mockDriver) RollbackTx(context.Context, driver.Tx) error                      { return nil }
func (m *mockDriver) UniqueJobExists(context.Context, string) (bool, error)     { return false, nil }
func (m *mockDriver) PauseQueue(context.Context, string) error                  { return nil }
func (m *mockDriver) ResumeQueue(context.Context, string) error                 { return nil }
func (m *mockDriver) IsQueuePaused(context.Context, string) (bool, error)       { return false, nil }
func (m *mockDriver) QueueStats(context.Context, string) (*driver.QueueStats, error) {
	return nil, nil
}
func (m *mockDriver) ListQueues(context.Context) ([]*driver.QueueStats, error) { return nil, nil }
func (m *mockDriver) StuckJobs(context.Context, time.Duration) ([]*driver.Job, error) {
	return nil, nil
}
func (m *mockDriver) UpsertWorker(context.Context, string, map[string]int) error { return nil }
func (m *mockDriver) RemoveWorker(context.Context, string) error                 { return nil }
func (m *mockDriver) ListWorkers(context.Context, time.Duration) ([]*driver.WorkerInstance, error) {
	return nil, nil
}
func (m *mockDriver) Migrate(context.Context) error                     { return nil }
func (m *mockDriver) MigrateDown(context.Context, int) error            { return nil }
func (m *mockDriver) MigrationStatus(context.Context) ([]string, error) { return nil, nil }
func (m *mockDriver) SetConcurrencyLimit(context.Context, driver.ConcurrencyLimit) error {
	return nil
}
func (m *mockDriver) AcquireConcurrencySlot(context.Context, string, string) (bool, error) {
	return true, nil
}
func (m *mockDriver) ReleaseConcurrencySlot(context.Context, string, string) error { return nil }
func (m *mockDriver) CreateWorkflow(context.Context, *driver.WorkflowRecord) error { return nil }
func (m *mockDriver) CompleteWorkflowTask(context.Context, driver.Tx, string, string) error {
	return nil
}
func (m *mockDriver) FailWorkflowTask(context.Context, string, string) error { return nil }
func (m *mockDriver) GetWorkflow(context.Context, string) (*driver.WorkflowState, error) {
	return nil, nil
}
func (m *mockDriver) ListWorkflows(context.Context, int, int) ([]*driver.WorkflowState, error) {
	return nil, nil
}
func (m *mockDriver) Close() error { return nil }

func TestElectorSingleRenewLoopOnReacquire(t *testing.T) {
	md := &mockDriver{}
	var mu sync.Mutex
	acquires := 0

	e := NewElector(md, slog.Default(), LeaderCallbacks{
		OnAcquire: func() {
			mu.Lock()
			acquires++
			mu.Unlock()
		},
	})
	e.interval = 20 * time.Millisecond
	e.renew = 20 * time.Millisecond
	e.Start()

	time.Sleep(50 * time.Millisecond)

	md.setRenewFailAt(1)
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	totalAcquires := acquires
	mu.Unlock()
	require.GreaterOrEqual(t, totalAcquires, 2)

	e.Stop()
}

func TestElectorRenewFailureTriggersLoss(t *testing.T) {
	md := &mockDriver{}
	lossCh := make(chan struct{}, 1)

	e := NewElector(md, slog.Default(), LeaderCallbacks{
		OnLoss: func() { lossCh <- struct{}{} },
	})
	e.interval = 20 * time.Millisecond
	e.renew = 20 * time.Millisecond
	e.Start()
	time.Sleep(50 * time.Millisecond)

	md.setRenewFailAt(1)
	select {
	case <-lossCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected OnLoss after renew failure")
	}

	e.Stop()
}

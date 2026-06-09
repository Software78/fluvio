package fluviui

import (
	"context"
	"time"

	"github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
)

// stubAPIClient provides no-op defaults for apiClient methods not under test.
type stubAPIClient struct{}

func (stubAPIClient) ListDeadJobs(ctx context.Context, limit, offset int) ([]fluvio.JobRow, error) {
	return nil, nil
}
func (stubAPIClient) ReplayDeadJob(ctx context.Context, jobID int64) error { return nil }
func (stubAPIClient) PurgeDeadJobs(ctx context.Context, before time.Time) (int64, error) {
	return 0, nil
}
func (stubAPIClient) EnqueueRaw(ctx context.Context, p fluvio.EnqueueRawParams) (*fluvio.JobRow, error) {
	return &fluvio.JobRow{Kind: p.Kind, Queue: p.Queue}, nil
}
func (stubAPIClient) Cancel(ctx context.Context, jobID int64) error    { return nil }
func (stubAPIClient) RunJobNow(ctx context.Context, jobID int64) error { return nil }
func (stubAPIClient) QueueStats(ctx context.Context, queue string) (*driver.QueueStats, error) {
	return &driver.QueueStats{Queue: queue}, nil
}
func (stubAPIClient) QueueWorkerCapacity(ctx context.Context, queue string) (int, int, error) {
	return 0, 0, nil
}
func (stubAPIClient) ListPeriodicJobs(ctx context.Context) ([]driver.PeriodicJob, error) {
	return nil, nil
}
func (stubAPIClient) AddPeriodicJobRaw(ctx context.Context, cronExpr, kind, queue string, args []byte, maxAttempts int16) error {
	return nil
}
func (stubAPIClient) PausePeriodicJob(ctx context.Context, kind string) error  { return nil }
func (stubAPIClient) ResumePeriodicJob(ctx context.Context, kind string) error { return nil }
func (stubAPIClient) ListWorkflows(ctx context.Context, limit, offset int) ([]*driver.WorkflowState, error) {
	return nil, nil
}
func (stubAPIClient) GetWorkflow(ctx context.Context, workflowID string) (*driver.WorkflowState, error) {
	return nil, fluvio.ErrWorkflowNotFound
}
func (stubAPIClient) ListConcurrencySlots(ctx context.Context) ([]driver.ConcurrencySlot, error) {
	return nil, nil
}
func (stubAPIClient) SetConcurrencyLimit(ctx context.Context, cfg fluvio.ConcurrencyLimitConfig) error {
	return nil
}

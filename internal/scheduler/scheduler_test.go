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

type recordingDriver struct {
	mu       sync.Mutex
	enqueued []string
}

func (d *recordingDriver) Enqueue(_ context.Context, p driver.EnqueueParams) (*driver.Job, error) {
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

func (d *recordingDriver) Ack(context.Context, int64) error   { return nil }
func (d *recordingDriver) Nack(context.Context, int64, error, time.Time) error { return nil }
func (d *recordingDriver) EnqueueTx(context.Context, driver.Tx, driver.EnqueueParams) (*driver.Job, error) {
	return nil, nil
}
func (d *recordingDriver) EnqueueMany(context.Context, []driver.EnqueueParams) ([]*driver.Job, error) {
	return nil, nil
}
func (d *recordingDriver) Fetch(context.Context, []string, string, int) ([]*driver.Job, error) {
	return nil, nil
}
func (d *recordingDriver) Cancel(context.Context, int64) error { return nil }
func (d *recordingDriver) GetJob(context.Context, int64) (*driver.Job, error) { return nil, nil }
func (d *recordingDriver) ListJobs(context.Context, driver.ListJobsParams) ([]*driver.Job, error) {
	return nil, nil
}
func (d *recordingDriver) TickScheduled(context.Context, time.Time) (int64, error) { return 0, nil }
func (d *recordingDriver) UniqueJobExists(context.Context, string) (bool, error) { return false, nil }
func (d *recordingDriver) PauseQueue(context.Context, string) error { return nil }
func (d *recordingDriver) ResumeQueue(context.Context, string) error { return nil }
func (d *recordingDriver) IsQueuePaused(context.Context, string) (bool, error) { return false, nil }
func (d *recordingDriver) QueueStats(context.Context, string) (*driver.QueueStats, error) {
	return nil, nil
}
func (d *recordingDriver) ListQueues(context.Context) ([]*driver.QueueStats, error) { return nil, nil }
func (d *recordingDriver) TryAcquireLeader(context.Context) (bool, error) { return false, nil }
func (d *recordingDriver) RenewLeader(context.Context) error { return nil }
func (d *recordingDriver) ReleaseLeader(context.Context) error { return nil }
func (d *recordingDriver) StuckJobs(context.Context, time.Duration) ([]*driver.Job, error) {
	return nil, nil
}
func (d *recordingDriver) Migrate(context.Context) error { return nil }
func (d *recordingDriver) MigrateDown(context.Context, int) error { return nil }
func (d *recordingDriver) MigrationStatus(context.Context) ([]string, error) { return nil, nil }
func (d *recordingDriver) Close() error { return nil }

func TestPeriodicDistinctKindsWithColon(t *testing.T) {
	rd := &recordingDriver{}
	p := scheduler.NewPeriodic(rd, slog.Default(), time.Millisecond)
	require.NoError(t, p.Register("* * * * *", "foo:bar", []byte(`{}`)))
	require.NoError(t, p.Register("* * * * *", "foo", []byte(`{}`)))

	p.Start()
	time.Sleep(50 * time.Millisecond)
	p.Stop()

	kinds := rd.enqueuedKinds()
	require.Len(t, kinds, 2)
	require.ElementsMatch(t, []string{"foo:bar", "foo"}, kinds)
}

func TestPeriodicRegisterRejectsInvalidCron(t *testing.T) {
	p := scheduler.NewPeriodic(&recordingDriver{}, slog.Default(), time.Second)
	require.Error(t, p.Register("not a cron", "k", nil))
}

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
	driver.NoopDriver
	jobs []*driver.Job
}

func (d *reaperDriver) StuckJobs(context.Context, time.Duration) ([]*driver.Job, error) {
	return d.jobs, nil
}

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

	r := maintenance.NewReaper(rd, slog.Default(), time.Minute, time.Millisecond, 0, 24*time.Hour, retryDelay, nack)
	r.Start()
	time.Sleep(20 * time.Millisecond)
	r.Stop()

	mu.Lock()
	defer mu.Unlock()
	require.False(t, nextAt.IsZero())
	require.Greater(t, time.Until(nextAt), time.Minute)
}

func TestReaperStopTwice(t *testing.T) {
	r := maintenance.NewReaper(&reaperDriver{}, slog.Default(), time.Minute, time.Millisecond, 0, 24*time.Hour,
		func(int16, time.Duration) time.Duration { return time.Second },
		func(context.Context, *driver.Job, error, time.Time) error { return nil },
	)
	r.Start()
	time.Sleep(5 * time.Millisecond)
	r.Stop()
	r.Stop()
}

func TestReaperRestartAfterStop(t *testing.T) {
	r := maintenance.NewReaper(&reaperDriver{}, slog.Default(), time.Minute, time.Millisecond, 0, 24*time.Hour,
		func(int16, time.Duration) time.Duration { return time.Second },
		func(context.Context, *driver.Job, error, time.Time) error { return nil },
	)
	r.Start()
	time.Sleep(5 * time.Millisecond)
	r.Stop()
	r.Start()
	time.Sleep(5 * time.Millisecond)
	r.Stop()
}

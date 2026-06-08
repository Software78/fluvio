package fluvio_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	fluvio "github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
)

type concurrencyRegisterCall struct {
	kind          string
	maxConcurrent int
	partitioned   bool
}

type concurrencyRecordingDriver struct {
	driver.NoopDriver
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

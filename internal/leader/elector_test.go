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
	driver.NoopDriver
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

func (m *mockDriver) VerifyLeader(context.Context) error {
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

func TestElectorStopDuringRenewFailure(t *testing.T) {
	md := &mockDriver{}
	e := NewElector(md, slog.Default(), LeaderCallbacks{})
	e.interval = 20 * time.Millisecond
	e.renew = 20 * time.Millisecond
	e.Start()
	time.Sleep(50 * time.Millisecond)

	md.setRenewFailAt(1)

	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked during renew failure (deadlock)")
	}
}

func TestElectorStopTwice(t *testing.T) {
	md := &mockDriver{}
	e := NewElector(md, slog.Default(), LeaderCallbacks{})
	e.interval = 20 * time.Millisecond
	e.renew = 20 * time.Millisecond
	e.Start()
	time.Sleep(30 * time.Millisecond)
	e.Stop()
	e.Stop()
}

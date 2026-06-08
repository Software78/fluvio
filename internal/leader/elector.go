package leader

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/software78/fluvio/internal/driver"
)

type LeaderCallbacks struct {
	OnAcquire func()
	OnLoss    func()
}

type Elector struct {
	driver      driver.Driver
	logger      *slog.Logger
	callbacks   LeaderCallbacks
	interval    time.Duration
	renew       time.Duration
	stopCh      chan struct{}
	doneCh      chan struct{}
	mu          sync.Mutex
	isLeader    bool
	renewMu     sync.Mutex
	renewCancel context.CancelFunc
	renewWG     sync.WaitGroup
}

func NewElector(d driver.Driver, logger *slog.Logger, callbacks LeaderCallbacks) *Elector {
	return &Elector{
		driver:    d,
		logger:    logger,
		callbacks: callbacks,
		interval:  5 * time.Second,
		renew:     30 * time.Second,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

func (e *Elector) Start() {
	go e.run()
}

func (e *Elector) Stop() {
	close(e.stopCh)
	e.stopRenewLoop()
	<-e.doneCh
	_ = e.driver.ReleaseLeader(context.Background())
}

func (e *Elector) run() {
	defer close(e.doneCh)
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		default:
		}

		e.mu.Lock()
		leader := e.isLeader
		e.mu.Unlock()

		if !leader {
			acquired, err := e.driver.TryAcquireLeader(context.Background())
			if err != nil {
				e.logger.Error("leader election failed", "error", err)
			} else if acquired {
				e.mu.Lock()
				e.isLeader = true
				e.mu.Unlock()
				e.logger.Info("acquired leader lock")
				if e.callbacks.OnAcquire != nil {
					e.callbacks.OnAcquire()
				}
				e.startRenewLoop()
			}
		}

		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
		}
	}
}

func (e *Elector) startRenewLoop() {
	e.renewMu.Lock()
	defer e.renewMu.Unlock()
	e.stopRenewLoopLocked()
	ctx, cancel := context.WithCancel(context.Background())
	e.renewCancel = cancel
	e.renewWG.Add(1)
	go func() {
		defer e.renewWG.Done()
		e.renewLoop(ctx)
	}()
}

func (e *Elector) cancelRenewLoop() {
	e.renewMu.Lock()
	defer e.renewMu.Unlock()
	if e.renewCancel != nil {
		e.renewCancel()
	}
}

func (e *Elector) stopRenewLoop() {
	e.renewMu.Lock()
	defer e.renewMu.Unlock()
	e.stopRenewLoopLocked()
}

func (e *Elector) stopRenewLoopLocked() {
	if e.renewCancel != nil {
		e.renewCancel()
		e.renewWG.Wait()
		e.renewCancel = nil
	}
}

func (e *Elector) renewLoop(ctx context.Context) {
	ticker := time.NewTicker(e.renew)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.mu.Lock()
			if !e.isLeader {
				e.mu.Unlock()
				return
			}
			e.mu.Unlock()

			if err := e.driver.VerifyLeader(context.Background()); err != nil {
				e.logger.Error("leader verify failed", "error", err)
				e.handleLoss()
				return
			}
		}
	}
}

func (e *Elector) handleLoss() {
	e.mu.Lock()
	if !e.isLeader {
		e.mu.Unlock()
		return
	}
	e.isLeader = false
	e.mu.Unlock()

	e.cancelRenewLoop()

	e.logger.Warn("lost leader lock")
	if e.callbacks.OnLoss != nil {
		e.callbacks.OnLoss()
	}
}

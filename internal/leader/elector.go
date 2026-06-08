package leader

import (
	"context"
	"log/slog"
	"math/rand"
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
	stopOnce    sync.Once
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
	e.stopOnce.Do(func() { close(e.stopCh) })
	e.drainRenewLoop()
	<-e.doneCh
	_ = e.driver.ReleaseLeader(context.Background())
}

func (e *Elector) run() {
	defer close(e.doneCh)

	if e.interval > 0 {
		jitter := time.Duration(rand.Int63n(int64(e.interval)))
		select {
		case <-e.stopCh:
			return
		case <-time.After(jitter):
		}
	}

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
	e.drainRenewLoop()
	e.renewMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	e.renewCancel = cancel
	e.renewWG.Add(1)
	e.renewMu.Unlock()
	go func() {
		defer e.renewWG.Done()
		e.renewLoop(ctx)
	}()
}

func (e *Elector) cancelRenewLoop() {
	e.renewMu.Lock()
	cancel := e.renewCancel
	e.renewMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (e *Elector) drainRenewLoop() {
	e.renewMu.Lock()
	cancel := e.renewCancel
	e.renewCancel = nil
	e.renewMu.Unlock()
	if cancel != nil {
		cancel()
		e.renewWG.Wait()
	}
}

type leaseExpiryReader interface {
	LeaderLeaseExpiry() time.Time
}

func (e *Elector) renewWait() time.Duration {
	wait := e.renew
	if r, ok := e.driver.(leaseExpiryReader); ok {
		if until := time.Until(r.LeaderLeaseExpiry()); until > 0 && until < e.renew {
			wait = until / 2
			if wait < time.Second {
				wait = time.Second
			}
		}
	}
	return wait
}

func (e *Elector) renewLoop(ctx context.Context) {
	for {
		select {
		case <-e.stopCh:
			return
		case <-ctx.Done():
			return
		case <-time.After(e.renewWait()):
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

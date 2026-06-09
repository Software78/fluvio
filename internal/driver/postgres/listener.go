package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/software78/fluvio"
)

// Subscriber listens for PostgreSQL NOTIFY events and signals workers to fetch.
type Subscriber struct {
	pool   *pgxpool.Pool
	conn   *pgxpool.Conn
	queues []string
	wakeCh chan struct{}
	stopCh chan struct{}
	doneCh chan struct{}
	logger *slog.Logger
	mu     sync.Mutex
}

func (d *Driver) Subscribe(ctx context.Context, queues []string) (fluvio.JobWakeSubscription, error) {
	if d.pollOnly {
		return nil, fmt.Errorf("fluvio/postgres: subscribe disabled in poll-only mode")
	}
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	s := &Subscriber{
		pool:   d.pool,
		conn:   conn,
		queues: append([]string(nil), queues...),
		wakeCh: make(chan struct{}, 1),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
		logger: slog.Default(),
	}
	if err := s.listenAll(ctx); err != nil {
		conn.Release()
		return nil, err
	}
	go s.run()
	return s, nil
}

func (s *Subscriber) Wake() <-chan struct{} {
	return s.wakeCh
}

func (s *Subscriber) Close() error {
	close(s.stopCh)
	<-s.doneCh
	return nil
}

func (s *Subscriber) listenAll(ctx context.Context) error {
	channels := make([]string, 0, len(s.queues)+1)
	channels = append(channels, notifyControlChannel)
	seen := map[string]struct{}{notifyControlChannel: {}}
	for _, q := range s.queues {
		ch := queueNotifyChannel(q)
		if _, ok := seen[ch]; ok {
			continue
		}
		seen[ch] = struct{}{}
		channels = append(channels, ch)
	}
	for _, ch := range channels {
		if _, err := s.conn.Exec(ctx, `LISTEN "`+ch+`"`); err != nil {
			return err
		}
	}
	return nil
}

func (s *Subscriber) run() {
	defer close(s.doneCh)
	defer s.conn.Release()

	backoff := 100 * time.Millisecond
	for {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			select {
			case <-s.stopCh:
				cancel()
			case <-ctx.Done():
			}
		}()

		notification, err := s.conn.Conn().WaitForNotification(ctx)
		cancel()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
			}
			s.logger.Error("listen notification failed", "error", err)
			time.Sleep(backoff)
			if backoff < 5*time.Second {
				backoff *= 2
			}
			reconnectCtx, reconnectCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := s.reconnect(reconnectCtx); err != nil {
				s.logger.Error("listen reconnect failed", "error", err)
			}
			reconnectCancel()
			continue
		}
		backoff = 100 * time.Millisecond
		_ = notification
		s.signalWake()
	}
}

func (s *Subscriber) reconnect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn.Release()
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	s.conn = conn
	return s.listenAll(ctx)
}

func (s *Subscriber) signalWake() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

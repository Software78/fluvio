package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/software78/fluvio/internal/driver"
)

const (
	notifyPrefix        = "fluvio."
	notifyControlChannel = notifyPrefix + "control"
)

type execQuerier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type notifyLimiter struct {
	mu       sync.Mutex
	last     map[string]time.Time
	cooldown time.Duration
}

func newNotifyLimiter(cooldown time.Duration) *notifyLimiter {
	if cooldown <= 0 {
		cooldown = 100 * time.Millisecond
	}
	return &notifyLimiter{
		last:     make(map[string]time.Time),
		cooldown: cooldown,
	}
}

func (l *notifyLimiter) Allow(channel string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if last, ok := l.last[channel]; ok && now.Sub(last) < l.cooldown {
		return false
	}
	l.last[channel] = now
	return true
}

func queueNotifyChannel(queue string) string {
	if queue == "" {
		queue = driver.QueueDefault
	}
	return notifyPrefix + queue
}

func (d *Driver) ConfigureNotify(pollOnly bool, debounce time.Duration) {
	d.pollOnly = pollOnly
	if debounce <= 0 {
		debounce = 100 * time.Millisecond
	}
	d.notifyLimiter = newNotifyLimiter(debounce)
}

func (d *Driver) maybeNotifyChannel(ctx context.Context, q execQuerier, channel string) error {
	if d.pollOnly || d.notifyLimiter == nil || !d.notifyLimiter.Allow(channel) {
		return nil
	}
	_, err := q.Exec(ctx, `SELECT pg_notify($1, '')`, channel)
	return err
}

func (d *Driver) maybeNotifyQueue(ctx context.Context, q execQuerier, queue string) error {
	return d.maybeNotifyChannel(ctx, q, queueNotifyChannel(queue))
}

func (d *Driver) maybeNotifyControl(ctx context.Context, q execQuerier) error {
	return d.maybeNotifyChannel(ctx, q, notifyControlChannel)
}

func (d *Driver) maybeNotifyQueues(ctx context.Context, q execQuerier, queues []string) error {
	seen := make(map[string]struct{}, len(queues))
	for _, queue := range queues {
		ch := queueNotifyChannel(queue)
		if _, ok := seen[ch]; ok {
			continue
		}
		seen[ch] = struct{}{}
		if err := d.maybeNotifyChannel(ctx, q, ch); err != nil {
			return err
		}
	}
	return nil
}

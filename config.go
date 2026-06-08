package fluvio

import (
	"context"
	"log/slog"
	"fmt"
	"os"
	"time"
)

// Config configures a Fluvio client.
type Config struct {
	Queues           map[string]QueueConfig
	Workers          *Workers
	FetchInterval    time.Duration
	JobTimeout       time.Duration
	MaxRetryDelay    time.Duration
	PeriodicInterval time.Duration
	Middleware       []JobMiddleware
	Logger           *slog.Logger
	ErrorHandler            func(ctx context.Context, job JobRow, err error)
	WorkerID                string
	WorkerHeartbeatInterval time.Duration
	WorkerTTL                   time.Duration
	LeaderServicesStartupDelay  time.Duration // delay before first scheduler/reaper/periodic tick after leader election
	KeyProvider                 KeyProvider   // if non-nil, encryption is available
	PollOnly                    bool          // disable LISTEN/NOTIFY; use with PgBouncer transaction pooling
	NotifyDebounce              time.Duration // minimum interval between NOTIFY per queue channel
}

type QueueConfig struct {
	// MaxWorkers is the maximum number of concurrent jobs for this queue.
	// Set to 0 to disable processing while still allowing enqueue.
	MaxWorkers int
}

func (c *Config) applyDefaults() {
	if c.FetchInterval == 0 {
		c.FetchInterval = 500 * time.Millisecond
	}
	if c.JobTimeout == 0 {
		c.JobTimeout = 30 * time.Minute
	}
	if c.MaxRetryDelay == 0 {
		c.MaxRetryDelay = 24 * time.Hour
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.WorkerID == "" {
		c.WorkerID = DefaultWorkerID()
	}
	if c.PeriodicInterval == 0 {
		c.PeriodicInterval = 30 * time.Second
	}
	if c.WorkerHeartbeatInterval == 0 {
		c.WorkerHeartbeatInterval = 30 * time.Second
	}
	if c.WorkerTTL == 0 {
		c.WorkerTTL = 90 * time.Second
	}
	if c.NotifyDebounce == 0 {
		c.NotifyDebounce = 100 * time.Millisecond
	}
}

// DefaultWorkerID returns a unique identifier based on hostname and process ID.
func DefaultWorkerID() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "fluvio"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

// EnqueueOption configures a single enqueue operation.
type EnqueueOption func(*enqueueOptions)

type enqueueOptions struct {
	queue       string
	priority    int16
	maxAttempts int16
	scheduledAt *time.Time
	uniqueKey   *string
	tags        []string
	metadata    []byte
	encrypted   bool
}

func WithQueue(queue string) EnqueueOption {
	return func(o *enqueueOptions) { o.queue = queue }
}

func WithPriority(priority int16) EnqueueOption {
	return func(o *enqueueOptions) { o.priority = priority }
}

func WithMaxAttempts(maxAttempts int16) EnqueueOption {
	return func(o *enqueueOptions) { o.maxAttempts = maxAttempts }
}

func WithScheduledAt(t time.Time) EnqueueOption {
	return func(o *enqueueOptions) { o.scheduledAt = &t }
}

func WithUniqueKey(key string) EnqueueOption {
	return func(o *enqueueOptions) { o.uniqueKey = &key }
}

func WithTags(tags ...string) EnqueueOption {
	return func(o *enqueueOptions) { o.tags = tags }
}

func WithMetadata(metadata []byte) EnqueueOption {
	return func(o *enqueueOptions) { o.metadata = metadata }
}

// WithEncryption marks this enqueue as encrypted. Requires Config.KeyProvider;
// returns ErrNoKeyProvider at enqueue time when no KeyProvider is configured.
func WithEncryption() EnqueueOption {
	return func(o *enqueueOptions) { o.encrypted = true }
}

func applyEnqueueOptions(opts []EnqueueOption) enqueueOptions {
	o := enqueueOptions{
		queue: QueueDefault,
	}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

package fluvio

import (
	"context"
	"log/slog"
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
	ErrorHandler     func(ctx context.Context, job JobRow, err error)
	WorkerID         string
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
		c.WorkerID = "fluvio-worker"
	}
	if c.PeriodicInterval == 0 {
		c.PeriodicInterval = 30 * time.Second
	}
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

func applyEnqueueOptions(opts []EnqueueOption) enqueueOptions {
	o := enqueueOptions{
		queue: QueueDefault,
	}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

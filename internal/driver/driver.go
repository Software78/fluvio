package driver

import (
	"context"
	"time"
)

const QueueDefault = "default"

// Job is the canonical job representation returned by all driver methods.
type Job struct {
	ID          int64
	Queue       string
	Kind        string
	Args        []byte
	State       string
	Priority    int16
	Attempt     int16
	MaxAttempts int16
	AttemptedBy []string
	ScheduledAt time.Time
	AttemptedAt *time.Time
	FinalizedAt *time.Time
	CreatedAt   time.Time
	ErrorTrace  []byte
	Tags        []string
	UniqueKey   *string
	Metadata    []byte
}

type EnqueueParams struct {
	Queue       string
	Kind        string
	Args        []byte
	Priority    int16
	MaxAttempts int16
	ScheduledAt *time.Time
	UniqueKey   *string
	Tags        []string
	Metadata    []byte
}

type QueueStats struct {
	Queue     string
	Pending   int64
	Running   int64
	Scheduled int64
	Dead      int64
	Completed int64
	Failed    int64
	Paused    bool
}

type ListJobsParams struct {
	Queue  string
	State  string
	Kind   string
	Limit  int
	Offset int
}

// Tx is an opaque transaction handle. Drivers cast it to their concrete type.
type Tx interface{}

// Driver is the OSS driver interface.
type Driver interface {
	Enqueue(ctx context.Context, p EnqueueParams) (*Job, error)
	EnqueueTx(ctx context.Context, tx Tx, p EnqueueParams) (*Job, error)
	EnqueueMany(ctx context.Context, params []EnqueueParams) ([]*Job, error)
	Fetch(ctx context.Context, queues []string, workerID string, maxJobs int) ([]*Job, error)
	Ack(ctx context.Context, jobID int64) error
	Nack(ctx context.Context, jobID int64, jobErr error, nextAttemptAt time.Time) error
	Cancel(ctx context.Context, jobID int64) error
	GetJob(ctx context.Context, jobID int64) (*Job, error)
	ListJobs(ctx context.Context, p ListJobsParams) ([]*Job, error)

	TickScheduled(ctx context.Context, now time.Time) (int64, error)

	UniqueJobExists(ctx context.Context, uniqueKey string) (bool, error)

	PauseQueue(ctx context.Context, queue string) error
	ResumeQueue(ctx context.Context, queue string) error
	IsQueuePaused(ctx context.Context, queue string) (bool, error)
	QueueStats(ctx context.Context, queue string) (*QueueStats, error)
	ListQueues(ctx context.Context) ([]*QueueStats, error)

	TryAcquireLeader(ctx context.Context) (bool, error)
	RenewLeader(ctx context.Context) error
	ReleaseLeader(ctx context.Context) error

	StuckJobs(ctx context.Context, timeout time.Duration) ([]*Job, error)

	Migrate(ctx context.Context) error
	MigrateDown(ctx context.Context, steps int) error
	MigrationStatus(ctx context.Context) ([]string, error)
	Close() error
}

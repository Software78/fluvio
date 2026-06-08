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
	DiedAt      *time.Time
	ErrorTrace  []byte
	Tags           []string
	UniqueKey      *string
	Metadata       []byte
	WorkflowID         *string
	WorkflowTaskID     *string
	Encrypted          bool
	ConcurrencySlotKey *string // nil = no slot; non-nil = held slot key ("" = global)
}

type EnqueueParams struct {
	Queue       string
	Kind        string
	Args        []byte
	Priority    int16
	MaxAttempts int16
	ScheduledAt *time.Time
	UniqueKey      *string
	Tags           []string
	Metadata       []byte
	WorkflowID     *string
	WorkflowTaskID *string
	Encrypted      bool
}

type QueueStats struct {
	Queue     string
	Pending   int64
	Running   int64
	Scheduled int64
	Dead      int64
	Completed int64
	Failed    int64
	Cancelled int64
	Paused    bool
}

type ListJobsParams struct {
	Queue  string
	State  string
	Kind   string
	Limit  int
	Offset int
}

// WorkerInstance represents a live processing client registered in the fleet.
type WorkerInstance struct {
	ID        string
	Queues    map[string]int // queue -> max_workers
	StartedAt time.Time
	LastSeen  time.Time
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
	ListDead(ctx context.Context, limit, offset int) ([]*Job, error)
	ReplayDead(ctx context.Context, jobID int64) error
	PurgeDead(ctx context.Context, before time.Time) (int64, error)

	TickScheduled(ctx context.Context, now time.Time) (int64, error)

	UpsertPeriodicJob(ctx context.Context, kind, cron, queue string, maxAttempts int16, args []byte, nextRun time.Time) error
	DuePeriodicJobs(ctx context.Context, now time.Time) ([]*PeriodicJob, error)
	UpdatePeriodicJobNextRun(ctx context.Context, kind string, nextRun time.Time) error
	UpdatePeriodicJobNextRunTx(ctx context.Context, tx Tx, kind string, nextRun time.Time) (bool, error)
	ListPeriodicJobs(ctx context.Context) ([]*PeriodicJob, error)
	PausePeriodicJob(ctx context.Context, kind string) error
	ResumePeriodicJob(ctx context.Context, kind string) error

	BeginTx(ctx context.Context) (Tx, error)
	CommitTx(ctx context.Context, tx Tx) error
	RollbackTx(ctx context.Context, tx Tx) error

	UniqueJobExists(ctx context.Context, uniqueKey string) (bool, error)

	PauseQueue(ctx context.Context, queue string) error
	ResumeQueue(ctx context.Context, queue string) error
	IsQueuePaused(ctx context.Context, queue string) (bool, error)
	QueueStats(ctx context.Context, queue string) (*QueueStats, error)
	ListQueues(ctx context.Context) ([]*QueueStats, error)

	TryAcquireLeader(ctx context.Context) (bool, error)
	VerifyLeader(ctx context.Context) error
	ReleaseLeader(ctx context.Context) error

	StuckJobs(ctx context.Context, timeout time.Duration) ([]*Job, error)

	UpsertWorker(ctx context.Context, workerID string, queues map[string]int) error
	RemoveWorker(ctx context.Context, workerID string) error
	ListWorkers(ctx context.Context, staleAfter time.Duration) ([]*WorkerInstance, error)

	Migrate(ctx context.Context) error
	MigrateDown(ctx context.Context, steps int) error
	MigrationStatus(ctx context.Context) ([]string, error)
	Close() error

	SetConcurrencyLimit(ctx context.Context, limit ConcurrencyLimit) error
	// RegisterConcurrencyLimit records an in-memory limit for Fetch-time global enforcement.
	// partitioned is true when the client uses a PartitionKeyFn (per-partition slots).
	RegisterConcurrencyLimit(kind string, maxConcurrent int, partitioned bool)
	AcquireConcurrencySlot(ctx context.Context, kind, partitionKey string) (acquired bool, err error)
	AcquireConcurrencySlotForJob(ctx context.Context, jobID int64, kind, partitionKey string) (acquired bool, err error)
	ReleaseConcurrencySlot(ctx context.Context, kind, partitionKey string) error
	SetConcurrencySlotKey(ctx context.Context, jobID int64, partitionKey string) error

	CreateWorkflow(ctx context.Context, w *WorkflowRecord) error
	CompleteWorkflowTask(ctx context.Context, tx Tx, workflowID, taskID string) error
	FailWorkflowTask(ctx context.Context, workflowID, taskID string) error
	GetWorkflow(ctx context.Context, workflowID string) (*WorkflowState, error)
	ListWorkflows(ctx context.Context, limit, offset int) ([]*WorkflowState, error)
}

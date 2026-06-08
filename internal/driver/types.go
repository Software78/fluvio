package driver

import (
	"time"
)

// WorkflowRecord is the input to CreateWorkflow.
type WorkflowRecord struct {
	ID       string
	Tasks    []WorkflowTask
	Metadata []byte
}

// WorkflowTask describes a single task in a workflow DAG.
type WorkflowTask struct {
	TaskID        string
	DependsOn     []string
	EnqueueParams EnqueueParams
}

// WorkflowState is the current state of a workflow and its tasks.
type WorkflowState struct {
	ID        string
	State     string
	Tasks     []WorkflowTaskState
	CreatedAt time.Time
}

// WorkflowTaskState is the runtime state of one workflow task.
type WorkflowTaskState struct {
	TaskID    string
	State     string
	DependsOn []string
	JobID     *int64
}

// ConcurrencyLimit configures per-kind concurrency caps stored in the database.
type ConcurrencyLimit struct {
	Kind          string
	MaxConcurrent int
	// PartitionKeyFn extracts a partition string from raw job args JSON.
	// If nil, all jobs of this Kind share one global slot.
	// NOT serialised — held in memory by the Client only.
	PartitionKeyFn func(args []byte) string
}

// PeriodicJob is a durable cron schedule stored in the database.
type PeriodicJob struct {
	Kind, Cron, Queue string
	MaxAttempts       int16
	Args              []byte
	NextRunAt         time.Time
	LastRunAt         *time.Time
	Paused            bool
}

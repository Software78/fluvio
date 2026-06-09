package fluvio

import (
	"encoding/json"
	"time"

	"github.com/software78/fluvio/internal/driver"
)

const QueueDefault = driver.QueueDefault

// JobState represents the lifecycle state of a job.
type JobState string

const (
	JobStatePending   JobState = "pending"
	JobStateRunning   JobState = "running"
	JobStateCompleted JobState = "completed"
	JobStateFailed    JobState = "failed"
	JobStateDead      JobState = "dead"
	JobStateScheduled JobState = "scheduled"
	JobStateCancelled JobState = "cancelled"
)

// JobArgs must be implemented by all job argument types.
type JobArgs interface {
	Kind() string
}

const maxJobLogEntries = 100

// JobLogEntry is a structured log line attached by a worker during execution.
type JobLogEntry struct {
	At      time.Time       `json:"at"`
	Level   string          `json:"level"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Job wraps a queued job with typed arguments.
type Job[T JobArgs] struct {
	ID            int64
	Queue         string
	Kind          string
	Args          T
	State         JobState
	Priority      int16
	Attempt       int16
	MaxAttempts   int16
	AttemptedBy   []string
	WorkerID      string // ID of the processing instance that claimed this job.
	MaxWorkers    int    // Local concurrency cap for this job's queue on this instance.
	ScheduledAt   time.Time
	AttemptedAt   *time.Time
	FinalizedAt   *time.Time
	CreatedAt     time.Time
	ErrorTrace    json.RawMessage
	Logs          json.RawMessage
	Tags          []string
	UniqueKey     *string
	Metadata      json.RawMessage
	logBuf        *[]JobLogEntry
	driverJob     *driver.Job
}

// ValidJobState reports whether s is a known job state filter value.
func ValidJobState(s string) bool {
	if s == "" {
		return true
	}
	switch JobState(s) {
	case JobStatePending, JobStateRunning, JobStateCompleted, JobStateFailed,
		JobStateDead, JobStateScheduled, JobStateCancelled:
		return true
	default:
		return false
	}
}

func jobFromDriver[T JobArgs](d *driver.Job, args T) *Job[T] {
	return &Job[T]{
		ID:          d.ID,
		Queue:       d.Queue,
		Kind:        d.Kind,
		Args:        args,
		State:       JobState(d.State),
		Priority:    d.Priority,
		Attempt:     d.Attempt,
		MaxAttempts: d.MaxAttempts,
		AttemptedBy: d.AttemptedBy,
		ScheduledAt: d.ScheduledAt,
		AttemptedAt: d.AttemptedAt,
		FinalizedAt: d.FinalizedAt,
		CreatedAt:   d.CreatedAt,
		ErrorTrace:  json.RawMessage(d.ErrorTrace),
		Logs:        json.RawMessage(d.Logs),
		Tags:        d.Tags,
		UniqueKey:   d.UniqueKey,
		Metadata:    json.RawMessage(d.Metadata),
		driverJob:   d,
	}
}

// Log appends a structured log entry. data is optional and marshaled as JSON.
func (j *Job[T]) Log(level, message string, data map[string]any) {
	if j.logBuf == nil {
		return
	}
	entry := JobLogEntry{
		At:      time.Now().UTC(),
		Level:   level,
		Message: message,
	}
	if data != nil {
		b, err := json.Marshal(data)
		if err == nil {
			entry.Data = b
		}
	}
	buf := append(*j.logBuf, entry)
	if len(buf) > maxJobLogEntries {
		buf = buf[len(buf)-maxJobLogEntries:]
	}
	*j.logBuf = buf
}

func (j *Job[T]) Info(message string, data map[string]any)  { j.Log("info", message, data) }
func (j *Job[T]) Warn(message string, data map[string]any)  { j.Log("warn", message, data) }
func (j *Job[T]) Error(message string, data map[string]any) { j.Log("error", message, data) }
func (j *Job[T]) Debug(message string, data map[string]any) { j.Log("debug", message, data) }

// JobRow is a non-generic view of a job for error handlers and inspection APIs.
type JobRow struct {
	ID          int64
	Queue       string
	Kind        string
	Args        json.RawMessage
	State       JobState
	Priority    int16
	Attempt     int16
	MaxAttempts int16
	AttemptedBy []string
	ScheduledAt time.Time
	AttemptedAt *time.Time
	FinalizedAt *time.Time
	CreatedAt   time.Time
	DiedAt      *time.Time // set for dead jobs from the DLQ
	ErrorTrace  json.RawMessage
	Logs        json.RawMessage
	Tags        []string
	UniqueKey   *string
	Metadata    json.RawMessage
}

// ClaimedBy returns the worker ID that claimed the current attempt, or "" if none.
func (j *Job[T]) ClaimedBy() string {
	if len(j.AttemptedBy) == 0 {
		return ""
	}
	return j.AttemptedBy[len(j.AttemptedBy)-1]
}

func (j *Job[T]) Row() JobRow {
	args, _ := json.Marshal(j.Args)
	return JobRow{
		ID:          j.ID,
		Queue:       j.Queue,
		Kind:        j.Kind,
		Args:        args,
		State:       j.State,
		Priority:    j.Priority,
		Attempt:     j.Attempt,
		MaxAttempts: j.MaxAttempts,
		AttemptedBy: j.AttemptedBy,
		ScheduledAt: j.ScheduledAt,
		AttemptedAt: j.AttemptedAt,
		FinalizedAt: j.FinalizedAt,
		CreatedAt:   j.CreatedAt,
		ErrorTrace:  j.ErrorTrace,
		Logs:        j.Logs,
		Tags:        j.Tags,
		UniqueKey:   j.UniqueKey,
		Metadata:    j.Metadata,
	}
}

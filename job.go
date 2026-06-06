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
	ScheduledAt   time.Time
	AttemptedAt   *time.Time
	FinalizedAt   *time.Time
	CreatedAt     time.Time
	ErrorTrace    json.RawMessage
	Tags          []string
	UniqueKey     *string
	Metadata      json.RawMessage
	driverJob     *driver.Job
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
		Tags:        d.Tags,
		UniqueKey:   d.UniqueKey,
		Metadata:    json.RawMessage(d.Metadata),
		driverJob:   d,
	}
}

// JobRow is a non-generic view of a job for error handlers and UI.
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
	ErrorTrace  json.RawMessage
	Tags        []string
	UniqueKey   *string
	Metadata    json.RawMessage
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
		Tags:        j.Tags,
		UniqueKey:   j.UniqueKey,
		Metadata:    j.Metadata,
	}
}

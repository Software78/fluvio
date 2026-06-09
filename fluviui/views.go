package fluviui

import (
	"encoding/json"
	"time"

	"github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
)

// JobRowView is a snake_case JSON view of a job row.
type JobRowView struct {
	ID          int64           `json:"id"`
	Queue       string          `json:"queue"`
	Kind        string          `json:"kind"`
	Args        json.RawMessage `json:"args"`
	State       fluvio.JobState `json:"state"`
	Priority    int16           `json:"priority"`
	Attempt     int16           `json:"attempt"`
	MaxAttempts int16           `json:"max_attempts"`
	AttemptedBy []string        `json:"attempted_by"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	AttemptedAt *time.Time      `json:"attempted_at"`
	FinalizedAt *time.Time      `json:"finalized_at"`
	CreatedAt   time.Time       `json:"created_at"`
	DiedAt      *time.Time      `json:"died_at,omitempty"`
	ErrorTrace  json.RawMessage `json:"error_trace,omitempty"`
	Logs        json.RawMessage `json:"logs,omitempty"`
	Tags        []string        `json:"tags"`
	UniqueKey   *string         `json:"unique_key"`
	Metadata    json.RawMessage `json:"metadata"`
}

func jobRowToView(row fluvio.JobRow) JobRowView {
	tags := row.Tags
	if tags == nil {
		tags = []string{}
	}
	attemptedBy := row.AttemptedBy
	if attemptedBy == nil {
		attemptedBy = []string{}
	}
	return JobRowView{
		ID: row.ID, Queue: row.Queue, Kind: row.Kind, Args: row.Args,
		State: row.State, Priority: row.Priority, Attempt: row.Attempt,
		MaxAttempts: row.MaxAttempts, AttemptedBy: attemptedBy,
		ScheduledAt: row.ScheduledAt, AttemptedAt: row.AttemptedAt,
		FinalizedAt: row.FinalizedAt, CreatedAt: row.CreatedAt,
		DiedAt: row.DiedAt, ErrorTrace: row.ErrorTrace, Logs: row.Logs, Tags: tags,
		UniqueKey: row.UniqueKey, Metadata: row.Metadata,
	}
}

func jobRowsToViews(rows []fluvio.JobRow) []JobRowView {
	out := make([]JobRowView, len(rows))
	for i, row := range rows {
		out[i] = jobRowToView(row)
	}
	return out
}

// PeriodicJobView is a snake_case JSON view of a periodic job.
type PeriodicJobView struct {
	Kind        string          `json:"kind"`
	Cron        string          `json:"cron"`
	Queue       string          `json:"queue"`
	MaxAttempts int16           `json:"max_attempts"`
	Args        json.RawMessage `json:"args"`
	NextRunAt   time.Time       `json:"next_run_at"`
	LastRunAt   *time.Time      `json:"last_run_at"`
	Paused      bool            `json:"paused"`
}

func periodicJobToView(j driver.PeriodicJob) PeriodicJobView {
	args := j.Args
	if len(args) == 0 {
		args = []byte("{}")
	}
	return PeriodicJobView{
		Kind: j.Kind, Cron: j.Cron, Queue: j.Queue,
		MaxAttempts: j.MaxAttempts, Args: args,
		NextRunAt: j.NextRunAt, LastRunAt: j.LastRunAt, Paused: j.Paused,
	}
}

// WorkflowTaskView is a snake_case JSON view of a workflow task.
type WorkflowTaskView struct {
	TaskID    string   `json:"task_id"`
	State     string   `json:"state"`
	DependsOn []string `json:"depends_on"`
	JobID     *int64   `json:"job_id"`
}

// WorkflowView is a snake_case JSON view of a workflow.
type WorkflowView struct {
	ID        string             `json:"id"`
	State     string             `json:"state"`
	Tasks     []WorkflowTaskView `json:"tasks"`
	CreatedAt time.Time          `json:"created_at"`
}

func workflowToView(w *driver.WorkflowState) WorkflowView {
	tasks := make([]WorkflowTaskView, len(w.Tasks))
	for i, t := range w.Tasks {
		deps := t.DependsOn
		if deps == nil {
			deps = []string{}
		}
		tasks[i] = WorkflowTaskView{
			TaskID: t.TaskID, State: t.State,
			DependsOn: deps, JobID: t.JobID,
		}
	}
	return WorkflowView{
		ID: w.ID, State: w.State, Tasks: tasks, CreatedAt: w.CreatedAt,
	}
}

// ConcurrencySlotView is a snake_case JSON view of a concurrency slot.
type ConcurrencySlotView struct {
	Kind          string `json:"kind"`
	PartitionKey  string `json:"partition_key"`
	Running       int    `json:"running"`
	MaxConcurrent int    `json:"max_concurrent"`
}

func concurrencySlotToView(s driver.ConcurrencySlot) ConcurrencySlotView {
	return ConcurrencySlotView{
		Kind: s.Kind, PartitionKey: s.PartitionKey,
		Running: s.Running, MaxConcurrent: s.MaxConcurrent,
	}
}

// QueueDetailView combines queue stats with fleet worker capacity.
type QueueDetailView struct {
	QueueStatsView
	WorkerInstances int `json:"worker_instances"`
	WorkerCapacity  int `json:"worker_capacity"`
}

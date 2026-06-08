package fluvio

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/software78/fluvio/internal/driver"
)

// Workflow is a DAG of jobs built with the fluent Task API.
// When a task fails terminally, sibling tasks in waiting, pending, or running
// state are cancelled and their queued jobs are cancelled. An in-flight running
// job may still finish its current execution, but no downstream tasks are enqueued.
type Workflow struct {
	id       string
	tasks    []workflowTaskDef
	metadata []byte
}

type workflowTaskDef struct {
	taskID    string
	args      JobArgs
	dependsOn []string
	opts      []EnqueueOption
}

// WorkflowOption configures a workflow task definition.
type WorkflowOption func(*workflowTaskDef)

// NewWorkflow creates a workflow with a generated ID.
func NewWorkflow() *Workflow {
	return &Workflow{
		id:       generateWorkflowID(),
		metadata: []byte("{}"),
	}
}

// Task adds a task to the workflow. Returns w for chaining.
func (w *Workflow) Task(taskID string, args JobArgs, opts ...WorkflowOption) *Workflow {
	def := workflowTaskDef{
		taskID: taskID,
		args:   args,
	}
	for _, opt := range opts {
		opt(&def)
	}
	w.tasks = append(w.tasks, def)
	return w
}

// WithDependsOn sets task dependencies.
func WithDependsOn(taskIDs ...string) WorkflowOption {
	return func(t *workflowTaskDef) {
		t.dependsOn = append(t.dependsOn, taskIDs...)
	}
}

// WithTaskEnqueueOptions applies enqueue options to a workflow task.
func WithTaskEnqueueOptions(opts ...EnqueueOption) WorkflowOption {
	return func(t *workflowTaskDef) {
		t.opts = append(t.opts, opts...)
	}
}

func generateWorkflowID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func validateWorkflowDAG(tasks []workflowTaskDef) error {
	if len(tasks) == 0 {
		return fmt.Errorf("%w: workflow must have at least one task", ErrInvalidWorkflow)
	}

	ids := make(map[string]struct{}, len(tasks))
	for _, t := range tasks {
		if t.taskID == "" {
			return fmt.Errorf("%w: task id is required", ErrInvalidWorkflow)
		}
		if _, dup := ids[t.taskID]; dup {
			return fmt.Errorf("%w: duplicate task id %q", ErrInvalidWorkflow, t.taskID)
		}
		ids[t.taskID] = struct{}{}
	}

	for _, t := range tasks {
		for _, dep := range t.dependsOn {
			if _, ok := ids[dep]; !ok {
				return fmt.Errorf("%w: unknown dependency %q for task %q", ErrInvalidWorkflow, dep, t.taskID)
			}
		}
	}

	taskByID := make(map[string]workflowTaskDef, len(tasks))
	for _, t := range tasks {
		taskByID[t.taskID] = t
	}

	visited := make(map[string]int, len(tasks)) // 0=unvisited, 1=visiting, 2=done
	var visit func(taskID string) error
	visit = func(taskID string) error {
		switch visited[taskID] {
		case 1:
			return ErrWorkflowCycle
		case 2:
			return nil
		}
		visited[taskID] = 1
		t := taskByID[taskID]
		for _, dep := range t.dependsOn {
			if err := visit(dep); err != nil {
				return err
			}
		}
		visited[taskID] = 2
		return nil
	}

	for id := range ids {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) EnqueueWorkflow(ctx context.Context, wf *Workflow) (string, error) {
	if wf == nil {
		return "", fmt.Errorf("%w: workflow is required", ErrInvalidWorkflow)
	}
	if err := validateWorkflowDAG(wf.tasks); err != nil {
		return "", err
	}

	record := driver.WorkflowRecord{
		ID:       wf.id,
		Metadata: wf.metadata,
		Tasks:    make([]driver.WorkflowTask, 0, len(wf.tasks)),
	}
	if len(record.Metadata) == 0 {
		record.Metadata = []byte("{}")
	}

	for _, t := range wf.tasks {
		o := applyEnqueueOptions(t.opts)
		data, err := json.Marshal(t.args)
		if err != nil {
			return "", err
		}
		dependsOn := t.dependsOn
		if dependsOn == nil {
			dependsOn = []string{}
		}
		record.Tasks = append(record.Tasks, driver.WorkflowTask{
			TaskID:    t.taskID,
			DependsOn: dependsOn,
			EnqueueParams: driver.EnqueueParams{
				Queue:       o.queue,
				Kind:        t.args.Kind(),
				Args:        data,
				Priority:    o.priority,
				MaxAttempts: o.maxAttempts,
				ScheduledAt: o.scheduledAt,
				UniqueKey:   o.uniqueKey,
				Tags:        o.tags,
				Metadata:    o.metadata,
			},
		})
	}

	if err := c.driver.CreateWorkflow(ctx, &record); err != nil {
		return "", err
	}
	return wf.id, nil
}

func (c *Client) GetWorkflow(ctx context.Context, workflowID string) (*driver.WorkflowState, error) {
	return c.driver.GetWorkflow(ctx, workflowID)
}

func (c *Client) ListWorkflows(ctx context.Context, limit, offset int) ([]*driver.WorkflowState, error) {
	return c.driver.ListWorkflows(ctx, limit, offset)
}

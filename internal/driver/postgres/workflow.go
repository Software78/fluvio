package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
)

func (d *Driver) CreateWorkflow(ctx context.Context, w *driver.WorkflowRecord) error {
	if w == nil || w.ID == "" {
		return fmt.Errorf("%w: workflow id is required", fluvio.ErrInvalidWorkflow)
	}
	if len(w.Tasks) == 0 {
		return fmt.Errorf("%w: workflow must have at least one task", fluvio.ErrInvalidWorkflow)
	}

	metadata := w.Metadata
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO fluvio_workflows (id, metadata)
		VALUES ($1, $2)
	`, w.ID, metadata); err != nil {
		return err
	}

	for _, task := range w.Tasks {
		p, err := normalizeEnqueueParams(task.EnqueueParams)
		if err != nil {
			return err
		}
		dependsOn := task.DependsOn
		if dependsOn == nil {
			dependsOn = []string{}
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO fluvio_workflow_tasks (
				workflow_id, task_id, state, depends_on,
				queue, kind, args, priority, max_attempts, tags, metadata, unique_key
			) VALUES ($1, $2, 'waiting', $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`, w.ID, task.TaskID, dependsOn,
			p.Queue, p.Kind, p.Args, p.Priority, p.MaxAttempts, p.Tags, p.Metadata, p.UniqueKey,
		); err != nil {
			return err
		}
	}

	rows, err := tx.Query(ctx, `
		SELECT task_id FROM fluvio_workflow_tasks
		WHERE workflow_id = $1 AND cardinality(depends_on) = 0
	`, w.ID)
	if err != nil {
		return err
	}
	var roots []string
	for rows.Next() {
		var taskID string
		if err := rows.Scan(&taskID); err != nil {
			rows.Close()
			return err
		}
		roots = append(roots, taskID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, taskID := range roots {
		if err := d.enqueueWorkflowTaskTx(ctx, tx, w.ID, taskID); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (d *Driver) CompleteWorkflowTask(ctx context.Context, tx driver.Tx, workflowID, taskID string) error {
	pgxTx, ok := tx.(pgx.Tx)
	if !ok {
		return errors.New("fluvio/postgres: tx must be pgx.Tx")
	}
	return d.completeWorkflowTaskTx(ctx, pgxTx, workflowID, taskID)
}

func (d *Driver) completeWorkflowTaskTx(ctx context.Context, tx pgx.Tx, workflowID, taskID string) error {
	tag, err := tx.Exec(ctx, `
		UPDATE fluvio_workflow_tasks
		SET state = 'completed'
		WHERE workflow_id = $1 AND task_id = $2
	`, workflowID, taskID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fluvio.ErrWorkflowNotFound
	}

	rows, err := tx.Query(ctx, `
		SELECT t.task_id FROM fluvio_workflow_tasks t
		WHERE t.workflow_id = $1 AND t.state = 'waiting'
		  AND NOT EXISTS (
		    SELECT 1 FROM unnest(t.depends_on) AS dep(task_id)
		    JOIN fluvio_workflow_tasks d
		      ON d.workflow_id = t.workflow_id AND d.task_id = dep.task_id
		    WHERE d.state <> 'completed'
		  )
	`, workflowID)
	if err != nil {
		return err
	}
	var unblocked []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		unblocked = append(unblocked, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, id := range unblocked {
		if err := d.enqueueWorkflowTaskTx(ctx, tx, workflowID, id); err != nil {
			return err
		}
	}

	var remaining int
	err = tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM fluvio_workflow_tasks
		WHERE workflow_id = $1 AND state <> 'completed'
	`, workflowID).Scan(&remaining)
	if err != nil {
		return err
	}
	if remaining == 0 {
		_, err = tx.Exec(ctx, `
			UPDATE fluvio_workflows SET state = 'completed' WHERE id = $1
		`, workflowID)
		return err
	}
	return nil
}

func (d *Driver) FailWorkflowTask(ctx context.Context, workflowID, taskID string) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := d.failWorkflowTaskTx(ctx, tx, workflowID, taskID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (d *Driver) failWorkflowTaskTx(ctx context.Context, tx pgx.Tx, workflowID, taskID string) error {
	tag, err := tx.Exec(ctx, `
		UPDATE fluvio_workflow_tasks
		SET state = 'failed'
		WHERE workflow_id = $1 AND task_id = $2
	`, workflowID, taskID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fluvio.ErrWorkflowNotFound
	}
	if _, err := tx.Exec(ctx, `
		UPDATE fluvio_workflow_tasks
		SET state = 'cancelled'
		WHERE workflow_id = $1 AND state = 'waiting'
	`, workflowID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE fluvio_workflows SET state = 'failed' WHERE id = $1
	`, workflowID)
	return err
}

func (d *Driver) enqueueWorkflowTaskTx(ctx context.Context, tx pgx.Tx, workflowID, taskID string) error {
	var queue, kind string
	var args, metadata []byte
	var priority, maxAttempts int16
	var tags []string
	var uniqueKey *string
	err := tx.QueryRow(ctx, `
		SELECT queue, kind, args, priority, max_attempts, tags, metadata, unique_key
		FROM fluvio_workflow_tasks
		WHERE workflow_id = $1 AND task_id = $2
		FOR UPDATE
	`, workflowID, taskID).Scan(
		&queue, &kind, &args, &priority, &maxAttempts, &tags, &metadata, &uniqueKey,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fluvio.ErrWorkflowNotFound
		}
		return err
	}

	wfID := workflowID
	taskIDCopy := taskID
	job, err := d.enqueueWithQuerier(ctx, tx, driver.EnqueueParams{
		Queue:          queue,
		Kind:           kind,
		Args:           args,
		Priority:       priority,
		MaxAttempts:    maxAttempts,
		Tags:           tags,
		Metadata:       metadata,
		UniqueKey:      uniqueKey,
		WorkflowID:     &wfID,
		WorkflowTaskID: &taskIDCopy,
	})
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		UPDATE fluvio_workflow_tasks
		SET state = 'pending', job_id = $3
		WHERE workflow_id = $1 AND task_id = $2
	`, workflowID, taskID, job.ID)
	return err
}

func (d *Driver) GetWorkflow(ctx context.Context, workflowID string) (*driver.WorkflowState, error) {
	var state driver.WorkflowState
	err := d.pool.QueryRow(ctx, `
		SELECT id, state, created_at FROM fluvio_workflows WHERE id = $1
	`, workflowID).Scan(&state.ID, &state.State, &state.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fluvio.ErrWorkflowNotFound
		}
		return nil, err
	}

	tasks, err := d.loadWorkflowTasks(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	state.Tasks = tasks
	return &state, nil
}

func (d *Driver) ListWorkflows(ctx context.Context, limit, offset int) ([]*driver.WorkflowState, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := d.pool.Query(ctx, `
		SELECT id, state, created_at
		FROM fluvio_workflows
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*driver.WorkflowState
	for rows.Next() {
		var ws driver.WorkflowState
		if err := rows.Scan(&ws.ID, &ws.State, &ws.CreatedAt); err != nil {
			return nil, err
		}
		tasks, err := d.loadWorkflowTasks(ctx, ws.ID)
		if err != nil {
			return nil, err
		}
		ws.Tasks = tasks
		out = append(out, &ws)
	}
	return out, rows.Err()
}

func (d *Driver) loadWorkflowTasks(ctx context.Context, workflowID string) ([]driver.WorkflowTaskState, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT task_id, state, depends_on, job_id
		FROM fluvio_workflow_tasks
		WHERE workflow_id = $1
		ORDER BY task_id
	`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []driver.WorkflowTaskState
	for rows.Next() {
		var t driver.WorkflowTaskState
		if err := rows.Scan(&t.TaskID, &t.State, &t.DependsOn, &t.JobID); err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

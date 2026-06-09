package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
)

var sequenceHoldScheduledAt = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

func generateSequenceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func (d *Driver) EnqueueSequence(ctx context.Context, params []driver.EnqueueParams) (string, error) {
	if len(params) == 0 {
		return "", fmt.Errorf("%w: sequence must have at least one step", fluvio.ErrInvalidConfig)
	}

	normalized := make([]driver.EnqueueParams, len(params))
	for i, p := range params {
		n, err := normalizeEnqueueParams(p)
		if err != nil {
			return "", err
		}
		normalized[i] = n
	}

	seqID := generateSequenceID()
	kind := normalized[0].Kind

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO fluvio_sequences (id, kind, total)
		VALUES ($1, $2, $3)
	`, seqID, kind, len(normalized)); err != nil {
		return "", err
	}

	now := time.Now().UTC()
	var notifyQueue string
	for i, p := range normalized {
		state := "scheduled"
		scheduledAt := sequenceHoldScheduledAt
		if i == 0 {
			state = "pending"
			scheduledAt = now
			notifyQueue = p.Queue
		}

		_, err := tx.Exec(ctx, `
			INSERT INTO fluvio_jobs (
				queue, kind, args, state, priority, max_attempts,
				scheduled_at, unique_key, tags, metadata, encrypted,
				sequence_id, sequence_pos
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		`, p.Queue, p.Kind, p.Args, state, p.Priority, p.MaxAttempts,
			scheduledAt, p.UniqueKey, p.Tags, p.Metadata, p.Encrypted,
			seqID, i,
		)
		if err != nil {
			if isUniqueViolation(err) {
				return "", fluvio.ErrUniqueConflict
			}
			return "", err
		}
	}

	if notifyQueue != "" {
		if err := d.maybeNotifyQueue(ctx, tx, notifyQueue); err != nil {
			return "", err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return seqID, nil
}

func (d *Driver) AdvanceSequence(ctx context.Context, tx driver.Tx, completedJobID int64) error {
	pgxTx, ok := tx.(pgx.Tx)
	if !ok {
		return errors.New("fluvio/postgres: tx must be pgx.Tx")
	}

	var sequenceID *string
	var sequencePos *int
	err := pgxTx.QueryRow(ctx, `
		SELECT sequence_id, sequence_pos
		FROM fluvio_jobs
		WHERE id = $1 AND sequence_id IS NOT NULL
	`, completedJobID).Scan(&sequenceID, &sequencePos)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if sequenceID == nil || sequencePos == nil {
		return nil
	}
	return d.advanceSequenceTx(ctx, pgxTx, *sequenceID, *sequencePos+1)
}

func (d *Driver) advanceSequenceTx(ctx context.Context, tx pgx.Tx, sequenceID string, nextPos int) error {
	var queue string
	err := tx.QueryRow(ctx, `
		UPDATE fluvio_jobs
		SET state = 'pending', scheduled_at = now()
		WHERE sequence_id = $1
			AND sequence_pos = $2
			AND state = 'scheduled'
		RETURNING queue
	`, sequenceID, nextPos).Scan(&queue)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	return d.maybeNotifyQueue(ctx, tx, queue)
}

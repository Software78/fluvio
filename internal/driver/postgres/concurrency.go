package postgres

import (
	"context"
	"fmt"

	"github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
)

// RegisterConcurrencyLimit records an in-memory concurrency limit for a job kind.
// partitioned is true when the client uses a PartitionKeyFn (per-partition slots).
func (d *Driver) RegisterConcurrencyLimit(kind string, maxConcurrent int, partitioned bool) {
	d.concurrencyMu.Lock()
	defer d.concurrencyMu.Unlock()
	if d.concurrencyLimits == nil {
		d.concurrencyLimits = make(map[string]concurrencyKindConfig)
	}
	d.concurrencyLimits[kind] = concurrencyKindConfig{
		maxConcurrent: maxConcurrent,
		partitioned:   partitioned,
	}
}

func (d *Driver) isGlobalConcurrencyKind(kind string) bool {
	d.concurrencyMu.RLock()
	defer d.concurrencyMu.RUnlock()
	cfg, ok := d.concurrencyLimits[kind]
	return ok && !cfg.partitioned
}

func (d *Driver) SetConcurrencyLimit(ctx context.Context, limit driver.ConcurrencyLimit) error {
	if limit.Kind == "" {
		return fmt.Errorf("%w: kind is required", fluvio.ErrInvalidConfig)
	}
	if limit.MaxConcurrent < 1 {
		return fmt.Errorf("%w: max_concurrent must be at least 1", fluvio.ErrInvalidConfig)
	}
	_, err := d.pool.Exec(ctx, `
		INSERT INTO fluvio_concurrency_slots (kind, partition_key, running, max_concurrent)
		VALUES ($1, '', 0, $2)
		ON CONFLICT (kind, partition_key) DO UPDATE SET max_concurrent = EXCLUDED.max_concurrent
	`, limit.Kind, limit.MaxConcurrent)
	if err != nil {
		return err
	}
	// Default to global (non-partitioned); the client may override via RegisterConcurrencyLimit.
	d.RegisterConcurrencyLimit(limit.Kind, limit.MaxConcurrent, false)
	return nil
}

func (d *Driver) AcquireConcurrencySlot(ctx context.Context, kind, partitionKey string) (bool, error) {
	if partitionKey != "" {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO fluvio_concurrency_slots (kind, partition_key, running, max_concurrent)
			SELECT $1, $2, 0, max_concurrent
			FROM fluvio_concurrency_slots
			WHERE kind = $1 AND partition_key = ''
			ON CONFLICT (kind, partition_key) DO NOTHING
		`, kind, partitionKey)
		if err != nil {
			return false, err
		}
	}

	tag, err := d.pool.Exec(ctx, `
		UPDATE fluvio_concurrency_slots
		SET running = running + 1
		WHERE kind = $1 AND partition_key = $2 AND running < max_concurrent
	`, kind, partitionKey)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (d *Driver) ReleaseConcurrencySlot(ctx context.Context, kind, partitionKey string) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE fluvio_concurrency_slots
		SET running = GREATEST(running - 1, 0)
		WHERE kind = $1 AND partition_key = $2
	`, kind, partitionKey)
	return err
}

package postgres

import (
	"context"

	"github.com/software78/fluvio/internal/driver"
)

func (d *Driver) EnqueueManyLoop(ctx context.Context, params []driver.EnqueueParams) ([]*driver.Job, error) {
	return d.enqueueManyLoop(ctx, params)
}

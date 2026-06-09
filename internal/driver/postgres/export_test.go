package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/software78/fluvio/internal/driver"
)

// EnqueueManyLoop exposes enqueueManyLoop for benchmarks. See enqueueManyLoop
// for notes on UniqueKey handling.
func (d *Driver) EnqueueManyLoop(ctx context.Context, params []driver.EnqueueParams) ([]*driver.Job, error) {
	return d.enqueueManyLoop(ctx, params)
}

// FetchJobsSQL exposes fetchJobsSQL for integration tests.
func FetchJobsSQL() string {
	return fetchJobsSQL
}

// FetchJobsSQLWithDelay prepends a pg_sleep delay CTE so tests can kill the connection mid-fetch.
func FetchJobsSQLWithDelay(seconds float64) string {
	return fmt.Sprintf("WITH delay AS (SELECT pg_sleep(%f)), ", seconds) + strings.TrimPrefix(fetchJobsSQL, "WITH ")
}

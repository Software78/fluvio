// Package postgres provides the PostgreSQL driver for Fluvio.
// It wraps the internal implementation so callers can construct a driver
// from an existing pgxpool.Pool without importing internal packages.
package postgres

import (
	"github.com/jackc/pgx/v5/pgxpool"

	fluvio "github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
	internal "github.com/software78/fluvio/internal/driver/postgres"
)

type Config = internal.Config

// Driver is the PostgreSQL driver type.
type Driver = internal.Driver

var (
	_ driver.Driver            = (*Driver)(nil)
	_ fluvio.JobSubscriber     = (*Driver)(nil)
	_ fluvio.NotifyConfigurer = (*Driver)(nil)
)

// New creates a PostgreSQL-backed driver.
func New(pool *pgxpool.Pool, cfg Config) *internal.Driver {
	return internal.New(pool, cfg)
}

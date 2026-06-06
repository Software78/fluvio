// Package postgres provides the PostgreSQL driver for Fluvio.
package postgres

import (
	"github.com/jackc/pgx/v5/pgxpool"

	internal "github.com/software78/fluvio/internal/driver/postgres"
)

type Config = internal.Config

// Driver is the PostgreSQL driver type.
type Driver = internal.Driver

// New creates a PostgreSQL-backed driver.
func New(pool *pgxpool.Pool, cfg Config) *internal.Driver {
	return internal.New(pool, cfg)
}

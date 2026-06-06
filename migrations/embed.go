package migrations

import "embed"

//go:embed postgres/*.sql
var Postgres embed.FS

const PostgresDir = "postgres"

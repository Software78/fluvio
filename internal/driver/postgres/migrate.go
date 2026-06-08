package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/software78/fluvio/migrations"
)

type Config struct {
	UseLeaseTable bool // Use lease table instead of session advisory lock (PgBouncer-compatible).
	LeaderID      string // Instance identifier for lease-table leader election.
	PollOnly      bool          // Disable LISTEN/NOTIFY (required for PgBouncer transaction pooling).
	NotifyDebounce time.Duration // Minimum interval between NOTIFY per channel (default 100ms).
}

const migrationLockID int64 = 0x666c7576696f6d // "fluviom"

type concurrencyKindConfig struct {
	maxConcurrent int
	partitioned   bool
}

// Driver implements driver.Driver for PostgreSQL.
// TryAcquireLeader, VerifyLeader, and ReleaseLeader must not be called concurrently.
type Driver struct {
	pool        *pgxpool.Pool
	leaderConn  *pgxpool.Conn
	leaderMu    sync.Mutex
	useLease    bool
	leaderID    string
	leaseExpiry time.Time

	concurrencyMu     sync.RWMutex
	concurrencyLimits map[string]concurrencyKindConfig

	pollOnly       bool
	notifyLimiter  *notifyLimiter
}

// New creates a Postgres driver from a connection pool.
func New(pool *pgxpool.Pool, cfg Config) *Driver {
	if cfg.LeaderID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "fluvio"
		}
		cfg.LeaderID = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}
	debounce := cfg.NotifyDebounce
	if debounce <= 0 {
		debounce = 100 * time.Millisecond
	}
	return &Driver{
		pool:          pool,
		useLease:      cfg.UseLeaseTable,
		leaderID:      cfg.LeaderID,
		pollOnly:      cfg.PollOnly,
		notifyLimiter: newNotifyLimiter(debounce),
	}
}

func (d *Driver) Pool() *pgxpool.Pool {
	return d.pool
}

func (d *Driver) Close() error {
	d.leaderMu.Lock()
	defer d.leaderMu.Unlock()
	if d.leaderConn != nil {
		d.leaderConn.Release()
		d.leaderConn = nil
	}
	d.pool.Close()
	return nil
}

func (d *Driver) Migrate(ctx context.Context) error {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockID) }()

	applied, err := d.appliedMigrations(ctx)
	if err != nil {
		return err
	}

	ups, err := d.listUpMigrations()
	if err != nil {
		return err
	}

	for _, version := range ups {
		if applied[version] {
			continue
		}
		sql, err := migrations.Postgres.ReadFile(filepath.Join(migrations.PostgresDir, version+".sql"))
		if err != nil {
			return fmt.Errorf("read migration %s: %w", version, err)
		}
		tx, err := d.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO fluvio_migrations (version) VALUES ($1)`,
			version,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (d *Driver) MigrateDown(ctx context.Context, steps int) error {
	if steps <= 0 {
		return nil
	}

	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockID) }()

	appliedList, err := d.MigrationStatus(ctx)
	if err != nil {
		return err
	}
	if len(appliedList) == 0 {
		return nil
	}

	for i := 0; i < steps && len(appliedList) > 0; i++ {
		version := appliedList[len(appliedList)-1]
		downFile := filepath.Join(migrations.PostgresDir, version+".down.sql")
		sql, err := migrations.Postgres.ReadFile(downFile)
		if err != nil {
			return fmt.Errorf("read down migration %s: %w", version, err)
		}
		tx, err := d.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("rollback migration %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM fluvio_migrations WHERE version = $1`,
			version,
		); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		appliedList = appliedList[:len(appliedList)-1]
	}
	return nil
}

func (d *Driver) MigrationStatus(ctx context.Context) ([]string, error) {
	var exists bool
	err := d.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_name = 'fluvio_migrations'
		)
	`).Scan(&exists)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	rows, err := d.pool.Query(ctx, `SELECT version FROM fluvio_migrations ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var versions []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}
	return versions, rows.Err()
}

func (d *Driver) appliedMigrations(ctx context.Context) (map[string]bool, error) {
	status, err := d.MigrationStatus(ctx)
	if err != nil {
		return nil, err
	}
	m := make(map[string]bool, len(status))
	for _, v := range status {
		m[v] = true
	}
	return m, nil
}

func (d *Driver) listUpMigrations() ([]string, error) {
	entries, err := migrations.Postgres.ReadDir(migrations.PostgresDir)
	if err != nil {
		return nil, err
	}
	var versions []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".down.sql") {
			continue
		}
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		versions = append(versions, strings.TrimSuffix(name, ".sql"))
	}
	sort.Strings(versions)
	if err := validateMigrationSequence(versions); err != nil {
		return nil, err
	}
	return versions, nil
}

func validateMigrationSequence(versions []string) error {
	seen := make(map[int]string, len(versions))
	var nums []int
	for _, v := range versions {
		n, err := migrationNumber(v)
		if err != nil {
			return fmt.Errorf("invalid migration version %q: %w", v, err)
		}
		if prev, ok := seen[n]; ok {
			return fmt.Errorf("duplicate migration number %03d: %q and %q", n, prev, v)
		}
		seen[n] = v
		nums = append(nums, n)
	}
	sort.Ints(nums)
	for i := 1; i < len(nums); i++ {
		if nums[i] <= nums[i-1] {
			return fmt.Errorf("migration numbers must be strictly increasing (found %03d after %03d)", nums[i], nums[i-1])
		}
	}
	return nil
}

func migrationNumber(version string) (int, error) {
	prefix, _, ok := strings.Cut(version, "_")
	if !ok {
		return 0, fmt.Errorf("expected NNN_name format")
	}
	return strconv.Atoi(prefix)
}

# Fluvio — Postgres Job Queue: Engineering Handoff

> A River-compatible, Postgres-backed job queue for Go with open-source core and Pro features.
> Driver interface is written for future multi-backend support (Redis, MySQL, SQLite) but only the Postgres driver ships in v1.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Repository Layout](#2-repository-layout)
3. [Database Schema](#3-database-schema)
4. [Driver Interface](#4-driver-interface)
5. [Postgres Driver Implementation](#5-postgres-driver-implementation)
6. [Core Engine](#6-core-engine)
7. [Public Client API](#7-public-client-api)
8. [Pro Features](#8-pro-features)
9. [Migrations CLI](#9-migrations-cli)
10. [Testing Strategy](#10-testing-strategy)
11. [Build Order](#11-build-order)
12. [Open Questions & Decisions](#12-open-questions--decisions)

---

## 1. Project Overview

### What this is

A production-grade background job queue for Go applications backed by PostgreSQL. Jobs are persisted transactionally — you can enqueue a job in the same database transaction as your business logic, guaranteeing the job fires if and only if the transaction commits. No Redis. No separate message broker. Just Postgres.

### OSS vs Pro split

| Layer | OSS (free) | Pro (paid) |
|---|---|---|
| Core fetch/work/retry | ✅ | ✅ |
| Multiple queues | ✅ | ✅ |
| Unique jobs | ✅ | ✅ |
| Periodic jobs (in-memory cron) | ✅ | ✅ |
| Workflows (DAG) | ❌ | ✅ |
| Sequences (ordered chains) | ❌ | ✅ |
| Concurrency limits (partitioned) | ❌ | ✅ |
| Dead letter queue | ❌ | ✅ |
| Durable periodic jobs | ❌ | ✅ |
| Encrypted job args | ❌ | ✅ |
| Ephemeral jobs | ❌ | ✅ |

### Key design principles

- **Transactional enqueue** — enqueue inside a `pgx` or `database/sql` tx; job is only visible after commit.
- **`SKIP LOCKED` fetch** — zero coordination overhead between competing workers; Postgres handles it natively.
- **Generics for type safety** — `Worker[T JobArgs]` gives strongly typed args with no `json.Unmarshal` boilerplate in job handlers.
- **Driver abstraction** — `driver.Driver` interface is defined from day one; only one implementation ships in v1, but the abstraction keeps the codebase honest and the future Redis driver is a drop-in.
- **Leader-based maintenance** — scheduled sweeps, periodic job ticks, and stuck job reaping run only on the elected leader, using Postgres advisory locks.

---

## 2. Repository Layout

```
fluvio/
├── go.mod                          module github.com/you/fluvio
├── go.sum
├── docs/
│   └── project-brief.md            Design notes and implementation plan
│
├── client.go                       Client, NewClient, Start, Stop
├── worker.go                       Worker[T] interface, WorkerDefaults[T]
├── job.go                          Job[T], JobArgs interface, JobState
├── config.go                       Config, QueueConfig, RetryPolicy
├── errors.go                       ErrJobNotFound, ErrUniqueConflict, etc.
├── middleware.go                   JobMiddleware, chain execution
│
├── internal/
│   ├── driver/
│   │   ├── driver.go               Driver interface (backend-agnostic)
│   │   ├── types.go                shared Job, EnqueueParams, QueueStats structs
│   │   └── postgres/
│   │       ├── driver.go           pgx implementation of Driver
│   │       ├── queries.go          raw SQL constants (no ORM)
│   │       ├── tx.go               transaction adapter
│   │       └── migrate.go          embedded migration runner
│   │
│   ├── executor/
│   │   ├── executor.go             goroutine pool, semaphore, panic recovery
│   │   └── fetch_loop.go           ticker, Fetch → dispatch, backpressure
│   │
│   ├── scheduler/
│   │   ├── scheduler.go            sweeps scheduled→pending (leader-only)
│   │   └── periodic.go             in-memory cron runner (leader-only)
│   │
│   ├── leader/
│   │   └── elector.go              Postgres advisory lock election + renewal
│   │
│   └── maintenance/
│       └── reaper.go               detects stuck running jobs, moves to failed
│
├── pro/
│   ├── workflow/
│   │   ├── workflow.go             Workflow, Task, DAG builder API
│   │   └── engine.go               completion detection, fan-in trigger
│   ├── sequence/
│   │   └── sequence.go             Sequence builder, next-job enqueue on ack
│   ├── concurrency/
│   │   └── limits.go               ConcurrencyLimit, partition key fn, slot counting
│   ├── dlq/
│   │   └── dlq.go                  DLQ list/replay/purge
│   ├── encrypt/
│   │   └── encrypt.go              KeyProvider interface, AES-256-GCM impl
│   └── ephemeral/
│       └── ephemeral.go            auto-delete completed jobs
│
├── migrations/
│   └── postgres/
│       ├── 001_initial.sql         core jobs table + indexes
│       ├── 002_leader.sql          leader election table
│       ├── 003_periodic.sql        durable periodic jobs table
│       ├── 004_workflows.sql       workflows + workflow_deps tables
│       ├── 005_sequences.sql       sequences table
│       └── 006_dlq.sql             dead letter table
│
└── cmd/
    └── fluvio/
        └── main.go                 CLI: migrate up/down, inspect, replay
```

---

## 3. Database Schema

### 3.1 Core jobs table

```sql
CREATE TYPE fluvio_job_state AS ENUM (
  'pending',
  'running',
  'completed',
  'failed',
  'dead',
  'scheduled',
  'cancelled'
);

CREATE TABLE fluvio_jobs (
  id             BIGSERIAL PRIMARY KEY,
  queue          TEXT        NOT NULL DEFAULT 'default',
  kind           TEXT        NOT NULL,
  args           JSONB       NOT NULL DEFAULT '{}',
  state          fluvio_job_state NOT NULL DEFAULT 'pending',
  priority       SMALLINT    NOT NULL DEFAULT 1,   -- lower = higher priority
  attempt        SMALLINT    NOT NULL DEFAULT 0,
  max_attempts   SMALLINT    NOT NULL DEFAULT 3,
  attempted_by   TEXT[]      NOT NULL DEFAULT '{}',
  scheduled_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempted_at   TIMESTAMPTZ,
  finalized_at   TIMESTAMPTZ,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  error_trace    JSONB,       -- [{attempt, error, at}]
  tags           TEXT[]      NOT NULL DEFAULT '{}',
  unique_key     TEXT,        -- nullable; drives unique index
  metadata       JSONB       NOT NULL DEFAULT '{}',

  -- Pro fields (NULL in OSS mode; driver reads/writes these)
  workflow_id      TEXT,
  workflow_task_id TEXT,
  batch_id         TEXT,
  sequence_id      TEXT,
  sequence_pos     INT,
  encrypted        BOOLEAN NOT NULL DEFAULT false
);

-- Primary fetch index: only pending/scheduled rows are eligible
CREATE INDEX idx_fluvio_jobs_fetch
  ON fluvio_jobs (queue, priority ASC, scheduled_at ASC)
  WHERE state IN ('pending', 'scheduled');

-- Unique jobs: key must be unique across non-terminal states
CREATE UNIQUE INDEX idx_fluvio_jobs_unique_key
  ON fluvio_jobs (unique_key)
  WHERE unique_key IS NOT NULL
    AND state NOT IN ('completed', 'dead', 'cancelled');

-- Scheduled sweep: find due scheduled jobs
CREATE INDEX idx_fluvio_jobs_scheduled
  ON fluvio_jobs (scheduled_at)
  WHERE state = 'scheduled';

-- Stuck job detection
CREATE INDEX idx_fluvio_jobs_running_since
  ON fluvio_jobs (attempted_at)
  WHERE state = 'running';
```

### 3.2 Leader election

```sql
CREATE TABLE fluvio_leader (
  id          TEXT        PRIMARY KEY DEFAULT 'singleton',
  elected_by  TEXT        NOT NULL,   -- hostname:pid or UUID
  elected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at  TIMESTAMPTZ NOT NULL
);
```

Advisory lock (`pg_try_advisory_lock`) is preferred for leader election — no table row needed. The table above is a fallback for environments where advisory locks are unavailable (e.g., PgBouncer in transaction mode).

### 3.3 Durable periodic jobs (Pro)

```sql
CREATE TABLE fluvio_periodic_jobs (
  kind          TEXT        PRIMARY KEY,
  cron          TEXT        NOT NULL,
  args          JSONB       NOT NULL DEFAULT '{}',
  next_run_at   TIMESTAMPTZ NOT NULL,
  last_run_at   TIMESTAMPTZ,
  paused        BOOLEAN     NOT NULL DEFAULT false,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### 3.4 Workflows (Pro)

```sql
CREATE TABLE fluvio_workflows (
  id          TEXT        PRIMARY KEY,
  state       TEXT        NOT NULL DEFAULT 'running', -- running|completed|failed|cancelled
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata    JSONB       NOT NULL DEFAULT '{}'
);

CREATE TABLE fluvio_workflow_deps (
  workflow_id  TEXT NOT NULL REFERENCES fluvio_workflows(id) ON DELETE CASCADE,
  task_id      TEXT NOT NULL,    -- unique within this workflow
  depends_on   TEXT NOT NULL,    -- upstream task_id
  PRIMARY KEY (workflow_id, task_id, depends_on)
);
```

### 3.5 Sequences (Pro)

```sql
CREATE TABLE fluvio_sequences (
  id          TEXT        PRIMARY KEY,
  kind        TEXT        NOT NULL,
  total       INT         NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

The `sequence_id` + `sequence_pos` columns on `fluvio_jobs` are sufficient to drive execution; this table is metadata for inspection APIs.

### 3.6 Dead letter queue (Pro)

```sql
CREATE TABLE fluvio_dead_jobs (
  id           BIGINT      PRIMARY KEY, -- same ID as original
  queue        TEXT        NOT NULL,
  kind         TEXT        NOT NULL,
  args         JSONB       NOT NULL,
  error_trace  JSONB,
  died_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  replayed_at  TIMESTAMPTZ
);
```

---

## 4. Driver Interface

Defined in `internal/driver/driver.go`. Every backend must satisfy this. The Postgres driver in v1 is the only implementation; future drivers (Redis, MySQL) implement the same interface.

```go
package driver

import (
    "context"
    "time"
)

// Job is the canonical job representation returned by all driver methods.
type Job struct {
    ID            int64
    Queue         string
    Kind          string
    Args          []byte // raw JSON
    State         string
    Priority      int16
    Attempt       int16
    MaxAttempts   int16
    AttemptedBy   []string
    ScheduledAt   time.Time
    AttemptedAt   *time.Time
    FinalizedAt   *time.Time
    CreatedAt     time.Time
    ErrorTrace    []byte // raw JSON: [{attempt, error, at}]
    Tags          []string
    UniqueKey     *string
    Metadata      []byte

    // Pro fields
    WorkflowID     *string
    WorkflowTaskID *string
    BatchID        *string
    SequenceID     *string
    SequencePos    *int
    Encrypted      bool
}

type EnqueueParams struct {
    Queue       string
    Kind        string
    Args        []byte
    Priority    int16
    MaxAttempts int16
    ScheduledAt *time.Time // nil = now
    UniqueKey   *string
    Tags        []string
    Metadata    []byte

    // Pro fields
    WorkflowID     *string
    WorkflowTaskID *string
    BatchID        *string
    SequenceID     *string
    SequencePos    *int
}

type QueueStats struct {
    Queue     string
    Pending   int64
    Running   int64
    Scheduled int64
    Dead      int64
}

// Tx is an opaque transaction handle. Drivers cast it to their concrete type.
type Tx interface{}

// Driver is the core interface. All backends implement this.
type Driver interface {
    // --- Job lifecycle ---

    // Enqueue inserts a job. Returns ErrUniqueConflict if unique_key already exists.
    Enqueue(ctx context.Context, p EnqueueParams) (*Job, error)

    // EnqueueTx inserts a job inside the caller's transaction.
    EnqueueTx(ctx context.Context, tx Tx, p EnqueueParams) (*Job, error)

    // EnqueueMany bulk-inserts jobs using COPY FROM for efficiency.
    EnqueueMany(ctx context.Context, params []EnqueueParams) ([]*Job, error)

    // Fetch claims up to maxJobs jobs from the given queues using SKIP LOCKED.
    Fetch(ctx context.Context, queues []string, workerID string, maxJobs int) ([]*Job, error)

    // Ack marks a job completed.
    Ack(ctx context.Context, jobID int64) error

    // Nack records a failed attempt. If attempt < max_attempts, reschedules at nextAttemptAt.
    // If attempt == max_attempts, moves to 'dead'.
    Nack(ctx context.Context, jobID int64, jobErr error, nextAttemptAt time.Time) error

    // Cancel moves a pending/scheduled job to 'cancelled'. No-op if already terminal.
    Cancel(ctx context.Context, jobID int64) error

    // GetJob retrieves a single job by ID.
    GetJob(ctx context.Context, jobID int64) (*Job, error)

    // --- Scheduling ---

    // TickScheduled moves jobs with scheduled_at <= now from 'scheduled' → 'pending'.
    // Returns number of jobs moved. Called by the scheduler on the leader only.
    TickScheduled(ctx context.Context, now time.Time) (int64, error)

    // --- Unique jobs ---

    // UniqueJobExists returns true if an active job with this unique_key exists.
    UniqueJobExists(ctx context.Context, uniqueKey string) (bool, error)

    // --- Queue management ---

    // PauseQueue prevents new jobs from being fetched from a queue.
    PauseQueue(ctx context.Context, queue string) error

    // ResumeQueue re-enables fetching from a paused queue.
    ResumeQueue(ctx context.Context, queue string) error

    // QueueStats returns counts by state for a queue.
    QueueStats(ctx context.Context, queue string) (*QueueStats, error)

    // ListQueues returns stats for all known queues.
    ListQueues(ctx context.Context) ([]*QueueStats, error)

    // --- Leader election ---

    // TryAcquireLeader attempts to elect this instance as leader.
    // Uses pg_try_advisory_lock. Returns true if acquired.
    TryAcquireLeader(ctx context.Context) (bool, error)

    // RenewLeader refreshes the advisory lock session (no-op for session locks;
    // updates expiry if table-based fallback is used).
    RenewLeader(ctx context.Context) error

    // ReleaseLeader releases the advisory lock.
    ReleaseLeader(ctx context.Context) error

    // --- Maintenance ---

    // StuckJobs returns jobs that have been in 'running' state beyond the timeout.
    StuckJobs(ctx context.Context, timeout time.Duration) ([]*Job, error)

    // --- Pro: DLQ ---

    // ListDead returns dead jobs, newest first.
    ListDead(ctx context.Context, limit, offset int) ([]*Job, error)

    // ReplayDead re-enqueues a dead job as a fresh pending job.
    ReplayDead(ctx context.Context, jobID int64) error

    // PurgeDead permanently deletes dead jobs finalized before the given time.
    PurgeDead(ctx context.Context, before time.Time) (int64, error)

    // --- Pro: Durable periodic jobs ---

    // UpsertPeriodicJob inserts or updates a periodic job schedule.
    UpsertPeriodicJob(ctx context.Context, kind, cron string, args []byte) error

    // DuePeriodicJobs returns periodic jobs whose next_run_at <= now.
    DuePeriodicJobs(ctx context.Context, now time.Time) ([]*PeriodicJob, error)

    // UpdatePeriodicJobNextRun advances next_run_at after a run.
    UpdatePeriodicJobNextRun(ctx context.Context, kind string, nextRun time.Time) error

    // --- Pro: Sequences ---

    // EnqueueSequence inserts all jobs in a sequence atomically.
    // Only the first job (pos=0) is set to 'pending'; the rest are 'scheduled' with no scheduled_at.
    EnqueueSequence(ctx context.Context, params []EnqueueParams) error

    // AdvanceSequence is called on job completion: enqueues the next job in the sequence.
    AdvanceSequence(ctx context.Context, completedJobID int64) error

    // --- Pro: Workflows ---

    // CreateWorkflow persists a workflow and its dependency graph.
    CreateWorkflow(ctx context.Context, w *WorkflowRecord) error

    // CompleteWorkflowTask marks a task done and enqueues newly unblocked tasks.
    CompleteWorkflowTask(ctx context.Context, workflowID, taskID string) error

    // FailWorkflowTask handles task failure per the workflow's failure policy.
    FailWorkflowTask(ctx context.Context, workflowID, taskID string, policy FailurePolicy) error

    // --- Migrations ---

    // Migrate runs all pending migration files in order.
    Migrate(ctx context.Context) error

    // MigrateDown rolls back the last N migrations.
    MigrateDown(ctx context.Context, steps int) error

    // Close releases the underlying connection pool.
    Close() error
}

type PeriodicJob struct {
    Kind       string
    Cron       string
    Args       []byte
    NextRunAt  time.Time
    LastRunAt  *time.Time
    Paused     bool
}

type WorkflowRecord struct {
    ID       string
    Tasks    []WorkflowTask
    Metadata []byte
}

type WorkflowTask struct {
    ID          string
    DependsOn   []string
    EnqueueParams EnqueueParams
}

type FailurePolicy int

const (
    FailWorkflow FailurePolicy = iota
    SkipTask
    RetryTask
)
```

---

## 5. Postgres Driver Implementation

### 5.1 Fetch query — the critical path

```sql
-- internal/driver/postgres/queries.go: fetchJobsSQL
WITH candidates AS (
  SELECT id
  FROM fluvio_jobs
  WHERE state = 'pending'
    AND scheduled_at <= now()
    AND queue = ANY($1::text[])
  ORDER BY priority ASC, scheduled_at ASC
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
UPDATE fluvio_jobs
SET
  state        = 'running',
  attempt      = attempt + 1,
  attempted_at = now(),
  attempted_by = array_append(attempted_by, $3::text)
WHERE id IN (SELECT id FROM candidates)
RETURNING *;
```

`$1` = `[]string` queue names, `$2` = `int` max jobs, `$3` = `string` worker ID.

The `WITH ... FOR UPDATE SKIP LOCKED` + `UPDATE` in one statement is atomic. No two workers can claim the same row.

### 5.2 Bulk insert with COPY FROM

```go
// EnqueueMany uses pgx's CopyFrom for O(1) round-trips regardless of batch size.
func (d *PostgresDriver) EnqueueMany(ctx context.Context, params []EnqueueParams) ([]*Job, error) {
    rows := make([][]any, len(params))
    for i, p := range params {
        rows[i] = []any{p.Queue, p.Kind, p.Args, p.Priority, p.MaxAttempts, scheduledAt(p), p.UniqueKey, p.Tags}
    }
    _, err := d.pool.CopyFrom(ctx,
        pgx.Identifier{"fluvio_jobs"},
        []string{"queue", "kind", "args", "priority", "max_attempts", "scheduled_at", "unique_key", "tags"},
        pgx.CopyFromRows(rows),
    )
    // ... return inserted jobs
}
```

### 5.3 Leader election

```go
// Prefer session-level advisory locks.
// Lock ID is a stable int64 derived from fnv.New64a("fluvio:leader").
const leaderLockID int64 = 0x666c7576696f // "fluvio" in hex, for example

func (d *Driver) TryAcquireLeader(ctx context.Context) (bool, error) {
    if d.leaderConn == nil {
        conn, err := d.pool.Acquire(ctx)
        if err != nil {
            return false, err
        }
        d.leaderConn = conn
    }
    var acquired bool
    err := d.leaderConn.QueryRow(ctx,
        "SELECT pg_try_advisory_lock($1)", leaderLockID,
    ).Scan(&acquired)
    return acquired, err
}

func (d *Driver) ReleaseLeader(ctx context.Context) error {
    if d.leaderConn == nil {
        return nil
    }
    _, err := d.leaderConn.Exec(ctx, "SELECT pg_advisory_unlock($1)", leaderLockID)
    d.leaderConn.Release()
    d.leaderConn = nil
    return err
}
```

**Important**: advisory locks are session-scoped. The driver holds a dedicated `*pgxpool.Conn` (via `pool.Acquire`) for the leader lock's lifetime — not a transient pool query. `ReleaseLeader` is called on elector shutdown regardless of whether leadership was acquired, so the dedicated connection is always returned to the pool.

### 5.4 Transaction adapter

The driver's `EnqueueTx` accepts a `driver.Tx` (opaque interface). The Postgres driver casts it to `pgx.Tx`:

```go
func (d *PostgresDriver) EnqueueTx(ctx context.Context, tx driver.Tx, p EnqueueParams) (*Job, error) {
    pgTx, ok := tx.(pgx.Tx)
    if !ok {
        return nil, errors.New("fluvio/postgres: tx must be pgx.Tx")
    }
    return d.enqueueWithQuerier(ctx, pgTx, p)
}
```

When the `database/sql` driver is added later, it casts to `*sql.Tx` instead.

---

## 6. Core Engine

### 6.1 Executor (goroutine pool)

```
internal/executor/executor.go
```

- Maintains a semaphore (`chan struct{}`) of size `MaxWorkers`.
- Each fetched job acquires a slot, runs the worker's `Work()` in a goroutine, and releases the slot on return.
- Panic recovery: catches panics in `Work()`, converts to an error, calls `Nack`.
- Respects context cancellation for graceful shutdown.

### 6.2 Fetch loop

```
internal/executor/fetch_loop.go
```

```
wait for FetchInterval, NOTIFY wakeup, or stop
  → if available_slots > 0:
      Fetch(queues, workerID, available_slots)
      → dispatch each job to executor
  → if zero jobs returned: apply backoff (up to 5s)
  → if available_slots == 0: skip fetch, wait for slot release
```

Backoff prevents thundering herd when queues are empty. PostgreSQL LISTEN/NOTIFY wakes idle workers immediately when jobs become pending; `FetchInterval` is the fallback poll. Set `PollOnly: true` when LISTEN is unavailable (e.g. PgBouncer transaction pooling).

### 6.3 Retry / backoff

Default exponential backoff in `WorkerDefaults`:

```go
func (w WorkerDefaults[T]) NextAttempt(job *Job[T], _ error) time.Duration {
    // attempt 1 → 1s, 2 → 4s, 3 → 16s, 4 → 64s ... capped at 24h
    base := time.Duration(math.Pow(4, float64(job.Attempt))) * time.Second
    if base > 24*time.Hour {
        base = 24 * time.Hour
    }
    return base
}
```

Workers override this by implementing `NextAttempt` on their struct.

### 6.4 Leader election loop

```
internal/leader/elector.go
```

```
on Start():
  attempt TryAcquireLeader every 5s until acquired
  once acquired:
    start scheduler, periodic runner, reaper (all leader-only goroutines)
    renew lock every 30s (keep-alive)
  on lock loss (connection drop):
    stop leader-only goroutines
    re-enter election loop
```

### 6.5 Stuck job reaper

```
internal/maintenance/reaper.go
```

Runs on leader only. Every 60s:

```sql
SELECT id FROM fluvio_jobs
WHERE state = 'running'
  AND attempted_at < now() - $1::interval
```

`$1` = job timeout (default 30 minutes, configurable per worker via `Timeout()` method). Reaped jobs are `Nack`'d as if they failed. On final attempt, they move to `dead`.

---

## 7. Public Client API

### 7.1 Defining a job

```go
// Args must implement JobArgs (a single Kind() string method).
type SendEmailArgs struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}

func (SendEmailArgs) Kind() string { return "send_email" }

// Worker embeds WorkerDefaults for sane defaults.
type SendEmailWorker struct {
    fluvio.WorkerDefaults[SendEmailArgs]
    mailer *Mailer
}

func (w *SendEmailWorker) Work(ctx context.Context, job *fluvio.Job[SendEmailArgs]) error {
    return w.mailer.Send(ctx, job.Args.To, job.Args.Subject)
}
```

### 7.2 Starting a client

```go
pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))

workers := fluvio.NewWorkers()
fluvio.AddWorker(workers, &SendEmailWorker{mailer: mailer})

client, err := fluvio.NewClient(postgres.New(pool), &fluvio.Config{
    Queues: map[string]fluvio.QueueConfig{
        fluvio.QueueDefault: {MaxWorkers: 50},
        "critical":          {MaxWorkers: 10},
    },
    Workers: workers,
})

if err := client.Start(ctx); err != nil {
    log.Fatal(err)
}
defer client.Stop()
```

### 7.3 Enqueueing jobs

```go
// Simple enqueue
_, err = client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"})

// Transactional enqueue (job fires iff tx commits)
tx, _ := pool.Begin(ctx)
_, err = client.EnqueueTx(ctx, tx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"})
err = updateUserRecord(ctx, tx, userID)
tx.Commit(ctx)

// With options
_, err = client.Enqueue(ctx,
    SendEmailArgs{To: "user@example.com", Subject: "Reminder"},
    fluvio.WithScheduledAt(time.Now().Add(24*time.Hour)),
    fluvio.WithPriority(2),
    fluvio.WithMaxAttempts(5),
    fluvio.WithUniqueKey("email:welcome:"+userID),
    fluvio.WithTags("email", "onboarding"),
    fluvio.WithQueue("critical"),
)

// Bulk insert
_, err = client.EnqueueMany(ctx, []SendEmailArgs{...})
```

### 7.4 Config reference

```go
type Config struct {
    // Queues to work and their per-queue settings.
    // Omit to run an insert-only client (no workers).
    Queues map[string]QueueConfig

    // Workers registry. Required even for insert-only clients
    // (so Kind → Worker mapping is known).
    Workers *Workers

    // FetchInterval controls how often workers poll for new jobs.
    // Default: 500ms
    FetchInterval time.Duration

    // JobTimeout is the default maximum duration a job can run.
    // Per-worker Timeout() overrides this.
    // Default: 30 minutes
    JobTimeout time.Duration

    // MaxRetryDelay caps the exponential backoff.
    // Default: 24 hours
    MaxRetryDelay time.Duration

    // Middleware is a list of JobMiddleware applied to every job.
    Middleware []JobMiddleware

    // Logger. Defaults to slog.Default().
    Logger *slog.Logger

    // ErrorHandler is called on every job error (for metrics, alerting).
    ErrorHandler func(ctx context.Context, job *JobRow, err error)
}

type QueueConfig struct {
    MaxWorkers int
}
```

---

## 8. Pro Features

All Pro features live under `pro/` and are gated at the import level. They share the same `driver.Driver` — no separate database connection needed.

### 8.1 Workflows

```go
import "github.com/you/fluvio/pro/workflow"

wf := workflow.New("billing-pipeline").
    Task("charge", ChargeArgs{...}).
    Task("send-receipt", SendReceiptArgs{...}, workflow.DependsOn("charge")).
    Task("update-crm", UpdateCRMArgs{...}, workflow.DependsOn("charge")).
    Task("close-order", CloseOrderArgs{...}, workflow.DependsOn("send-receipt", "update-crm"))

_, err = client.EnqueueWorkflow(ctx, wf)
```

**Engine logic** (`pro/workflow/engine.go`):

When a workflow task job is `Ack`'d, `CompleteWorkflowTask` is called. It:
1. Marks the task as complete in `fluvio_workflow_deps`.
2. Queries for tasks whose all dependencies are now complete.
3. Enqueues those tasks atomically in the same transaction as the ack.

Fan-out: multiple tasks with the same `DependsOn` — they all become pending simultaneously.  
Fan-in: a task with multiple `DependsOn` entries — only enqueued once all are complete.

Workflow-level failure: if a task fails with `FailWorkflow` policy, all non-started tasks are cancelled and the workflow is marked `failed`.

### 8.2 Sequences

```go
import "github.com/you/fluvio/pro/sequence"

seq := sequence.New("user-onboarding").
    Step(CreateProfileArgs{UserID: 42}).
    Step(SendWelcomeEmailArgs{UserID: 42}).
    Step(AssignTrialArgs{UserID: 42})

_, err = client.EnqueueSequence(ctx, seq)
```

- All steps are inserted atomically. Step 0 is `pending`; steps 1+ are in state `sequence_waiting` (a virtual state stored in `metadata`, not a new enum value).
- On step N's `Ack`, `AdvanceSequence` enqueues step N+1 atomically.
- Different sequences with the same kind run fully in parallel with each other.

### 8.3 Concurrency limits

```go
import "github.com/you/fluvio/pro/concurrency"

// Globally: no more than 5 concurrent PDF render jobs
client.SetConcurrencyLimit(concurrency.Limit{
    Kind:    "render_pdf",
    MaxConcurrent: 5,
})

// Partitioned: no more than 2 jobs per tenant_id
client.SetConcurrencyLimit(concurrency.Limit{
    Kind:    "process_invoice",
    MaxConcurrent: 2,
    PartitionKey: func(args json.RawMessage) string {
        var a struct{ TenantID string `json:"tenant_id"` }
        json.Unmarshal(args, &a)
        return a.TenantID
    },
})
```

Implementation: a `fluvio_concurrency_slots` table with `(kind, partition_key, count)`. Fetch logic wraps the normal `SKIP LOCKED` query with a join/check against this table. Count increments on claim, decrements on `Ack`/`Nack`. Postgres row-level locking keeps this atomic.

### 8.4 Dead letter queue

```go
import "github.com/you/fluvio/pro/dlq"

d := dlq.New(client)

// Inspect
jobs, err := d.List(ctx, dlq.ListParams{Queue: "default", Limit: 50})

// Replay a single job
err = d.Replay(ctx, jobID)

// Replay all dead jobs for a kind
count, err = d.ReplayByKind(ctx, "send_email")

// Purge old dead jobs
count, err = d.Purge(ctx, time.Now().Add(-30*24*time.Hour))
```

### 8.5 Encrypted jobs

```go
import "github.com/you/fluvio/pro/encrypt"

// Use a static key (development)
enc := encrypt.NewAESGCM([]byte(os.Getenv("JOB_ENCRYPTION_KEY")))

// Wrap client to encrypt all job args at insert, decrypt at fetch
client = client.WithEncryption(enc)
```

`KeyProvider` interface allows plugging in AWS KMS, HashiCorp Vault, etc.

```go
type KeyProvider interface {
    Encrypt(plaintext []byte) (ciphertext []byte, err error)
    Decrypt(ciphertext []byte) (plaintext []byte, err error)
}
```

The `encrypted` boolean column on `fluvio_jobs` tells the driver whether to attempt decryption.

### 8.6 Ephemeral jobs

```go
// Job args opt into ephemeral mode
type CacheWarmArgs struct {
    Key string `json:"key"`
}

func (CacheWarmArgs) Kind() string    { return "cache_warm" }
func (CacheWarmArgs) Ephemeral() bool { return true } // implements EphemeralJob interface
```

On `Ack`, if the job implements `EphemeralJob`, the row is immediately `DELETE`'d rather than updated to `completed`. Keeps the jobs table clean for high-volume, low-importance work.

### 8.7 Durable periodic jobs

```go
// Register at startup (survives restart, no double-fire on leader transition)
client.AddDurablePeriodicJob("0 * * * *", SendDailyReportArgs{})
client.AddDurablePeriodicJob("*/5 * * * *", FlushCacheArgs{})
```

- Schedules persisted in `fluvio_periodic_jobs`.
- Leader's periodic runner queries `next_run_at <= now()` every 30s.
- Unique key on `(kind, next_run_at)` prevents double-enqueue during leader transition.
- `last_run_at` and `next_run_at` updated atomically with the enqueue.

---

## 9. Migrations CLI

```bash
# Apply all pending migrations
fluvio migrate up --dsn "postgres://..."

# Roll back last N migrations
fluvio migrate down --steps 1 --dsn "postgres://..."

# Show current migration state
fluvio migrate status --dsn "postgres://..."
```

Migrations are embedded in the binary via `//go:embed migrations/postgres/*.sql` and run in filename-order. State tracked in `fluvio_migrations (version TEXT, applied_at TIMESTAMPTZ)`.

The migration runner is also callable programmatically:

```go
driver.Migrate(ctx) // called once in your app's startup
```

---

## 10. Testing Strategy

### Unit tests

- Driver methods: use `testcontainers-go` to spin up a real Postgres container per test suite. Do not mock the database.
- Executor: use a fake driver that records calls; test backpressure, panic recovery, graceful shutdown.
- Workflow engine: table-driven tests with in-memory job state; verify correct fan-in/fan-out.

### Integration tests

- Full client lifecycle: `NewClient` → `Start` → enqueue → worker executes → `Stop`.
- Transactional enqueue: verify job is invisible before commit, visible after.
- Unique jobs: verify second enqueue returns `ErrUniqueConflict`.
- Periodic jobs: mock `time.Now()` via a clock interface; verify tick causes enqueue.
- Sequences: enqueue 3-step sequence, ack each, verify order.
- Concurrency limits: enqueue 10 jobs with limit=3, verify ≤3 run simultaneously.

### Race detector

All tests run with `-race`. The fetch loop, executor semaphore, and leader elector are the primary concurrency surfaces.

```makefile
test:
    go test -race ./...

test-integration:
    go test -race -tags integration ./...
```

---

## 11. Build Order

Ship in this order. Each phase is independently useful and testable.

| Phase | Deliverable | Est. effort |
|---|---|---|
| **1** | Schema v1 + migration runner | 2 days |
| **2** | Postgres driver: `Enqueue`, `EnqueueTx`, `Fetch`, `Ack`, `Nack` | 3 days |
| **3** | Executor + fetch loop (single queue, in-process workers) | 3 days |
| **4** | Public `Client` API, `Worker[T]`, `JobArgs` generics | 2 days |
| **5** | Leader election + scheduled job sweep + stuck job reaper | 2 days |
| **6** | `EnqueueMany` (COPY FROM), bulk opts, priority | 1 day |
| **7** | Unique jobs + `ErrUniqueConflict` | 1 day |
| **8** | Queue pause/resume + `QueueStats` | 1 day |
| **9** | Middleware chain + error handler hook | 1 day |
| **10 (Pro)** | Dead letter queue | 2 days |
| **11 (Pro)** | Durable periodic jobs | 2 days |
| **12 (Pro)** | Sequences | 3 days |
| **13 (Pro)** | Concurrency limits | 3 days |
| **14 (Pro)** | Workflows (DAG engine) | 5 days |
| **15 (Pro)** | Encrypted jobs + `KeyProvider` interface | 2 days |
| **16 (Pro)** | Ephemeral jobs | 1 day |
| **17** | CLI (`migrate up/down/status`) | 1 day |

**OSS v1.0 gate**: phases 1–9 complete, all tests green, README with getting started guide.  
**Pro v1.0 gate**: phases 10–16 complete.

---

## 12. Open Questions & Decisions

These should be resolved before or during phase 1.

| # | Question | Options | Recommendation |
|---|---|---|---|
| 1 | **Module name** | `fluvio`, `torrent`, `conduit`, `gqueue` | Pick before publishing to avoid renaming pain |
| 2 | **pgx v4 vs v5** | v4 is more widely used; v5 is current | Ship v5; expose `riverdatabasesql`-style adapter for v4 users |
| 3 | **Advisory lock vs lease table** | Advisory lock = simpler; lease table = PgBouncer-compatible | Default advisory lock, env flag for lease table fallback |
| 4 | **`database/sql` support** | pgx only, or also `database/sql`? | pgx only in v1; `database/sql` driver in v1.1 (lets Bun/GORM users do transactional enqueue) |
| 5 | **Job args encoding** | JSON (JSONB column) only, or also msgpack? | JSONB — inspectable in Postgres, no perf concern at job scale |
| 6 | **LISTEN/NOTIFY** | Poll-only v1, or add pub/sub for instant pickup? | **Shipped** — hybrid NOTIFY + poll; `PollOnly` for PgBouncer |
| 7 | **Pro licensing** | Source-available, private Go module, binary-only | Private Go module (same as River Pro); easiest to enforce, no WASM/binary hassle |
| 8 | **Sequence waiting state** | New enum value vs metadata field | Metadata field — avoids migration on all envs when Pro is added; cast to `scheduled` with far-future `scheduled_at` is another option |
| 9 | **Error trace format** | Array of objects vs flat string | JSONB array `[{attempt, error, at, worker}]` — queryable and inspection-friendly |
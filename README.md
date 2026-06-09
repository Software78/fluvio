# Fluvio

[![codecov](https://codecov.io/gh/Software78/fluvio/graph/badge.svg)](https://codecov.io/gh/Software78/fluvio)

A production-grade background job queue for Go backed by your relational database. Enqueue jobs in the same database transaction as your business logic — no Redis, no separate broker.

## Features

| Feature | Status |
|---------|--------|
| Transactional enqueue | Yes |
| Fetch/work/retry with SKIP LOCKED | Yes |
| Multiple queues | Yes |
| Unique jobs | Yes |
| Scheduled jobs | Yes |
| Durable periodic (cron) | Yes |
| Workflows (DAG) | Yes |
| Concurrency limits | Yes |
| Dead letter queue | Yes |
| Encrypted args | Yes |
| Sequences | Planned |

## Database support

Fluvio is built on a `driver.Driver` interface so storage backends are swappable. Only PostgreSQL ships today; other relational drivers are on the roadmap.

| Database | Status | Notes |
|----------|--------|-------|
| PostgreSQL | Supported | Production-ready via `github.com/software78/fluvio/postgres` (pgx) |
| MySQL / MariaDB | Planned | `FOR UPDATE SKIP LOCKED` (MySQL 8.0+); lease-table leader election |
| SQLite | Planned | Single-node and local dev; file-based migrations |
| SQL Server | Planned | `READPAST` / row-lock patterns for job fetch |
| CockroachDB | Planned | Postgres-compatible dialect; validate `SKIP LOCKED` and advisory locks |

Each driver will implement the same interface — enqueue, fetch, ack/nack, migrations, leader election, and the advanced features above — with SQL adapted to the target engine. Transactional enqueue will use that database's native transaction type (e.g. `pgx.Tx`, `*sql.Tx`).

Contributions for new drivers are welcome; open an issue before starting large backend work so schema and locking semantics stay aligned.

## Quick start

### 1. Run migrations

```bash
export DATABASE_URL="postgres://user:pass@localhost:5432/mydb?sslmode=disable"
go run ./cmd/fluvio migrate up --dsn "$DATABASE_URL"
```

### 2. Define a worker and start the client

```go
package main

import (
    "context"
    "log"
    "os"

    "github.com/jackc/pgx/v5/pgxpool"
    fluvio "github.com/software78/fluvio"
    "github.com/software78/fluvio/postgres"
)

type SendEmailArgs struct {
    To string `json:"to"`
}

func (SendEmailArgs) Kind() string { return "send_email" }

type SendEmailWorker struct {
    fluvio.WorkerDefaults[SendEmailArgs]
}

func (w *SendEmailWorker) Work(ctx context.Context, job *fluvio.Job[SendEmailArgs]) error {
    log.Printf("send email to %s", job.Args.To)
    return nil
}

func main() {
    ctx := context.Background()
    pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Fatal(err)
    }

    workers := fluvio.NewWorkers()
    fluvio.AddWorker(workers, &SendEmailWorker{})

    client, err := fluvio.NewClient(postgres.New(pool, postgres.Config{}), &fluvio.Config{
        Queues:  map[string]fluvio.QueueConfig{fluvio.QueueDefault: {MaxWorkers: 10}},
        Workers: workers,
    })
    if err != nil {
        log.Fatal(err)
    }
    if err := client.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer client.Stop()

    _, err = client.Enqueue(ctx, SendEmailArgs{To: "user@example.com"})
    if err != nil {
        log.Fatal(err)
    }

    select {}
}
```

### 3. Transactional enqueue

```go
tx, _ := pool.Begin(ctx)
_, err = client.EnqueueTx(ctx, tx, SendEmailArgs{To: "user@example.com"})
// ... other writes in the same tx ...
tx.Commit(ctx) // job visible only after commit
```

### Bulk enqueue

`EnqueueMany` runs all inserts in one transaction. If any row fails (including a unique-key conflict), the entire batch is rolled back and no jobs are inserted. Each item can specify its own enqueue options (queue, priority, tags, and so on).

```go
rows, err := client.EnqueueMany(ctx, []fluvio.EnqueueItem{
    {Args: SendEmailArgs{To: "a@example.com"}, Opts: []fluvio.EnqueueOption{fluvio.WithPriority(1)}},
    {Args: SendEmailArgs{To: "b@example.com"}, Opts: []fluvio.EnqueueOption{fluvio.WithQueue("critical")}},
})
```

Use `UniqueJobExists` to check for an active job with a given unique key before enqueueing.

### 4. Web API

Mount the REST API and SSE stream for [Fluvio UI](https://github.com/Software78/fluvio_ui) or another admin tool:

```go
import "github.com/software78/fluvio/fluviui"

// Production — restrict origin and require authentication
mux.Handle("/fluvio/", fluviui.Handler(client,
    fluviui.WithAllowedOrigin("https://your-ui.example.com"),
    fluviui.WithMiddleware(func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            user, pass, ok := r.BasicAuth()
            if !ok || user != "admin" || pass != os.Getenv("FLUVIO_UI_PASSWORD") {
                w.Header().Set("WWW-Authenticate", `Basic realm="fluvio"`)
                http.Error(w, "unauthorized", http.StatusUnauthorized)
                return
            }
            next.ServeHTTP(w, r)
        })
    }),
))

// Development — allow all origins (default)
mux.Handle("/fluvio/", fluviui.Handler(client))
```

Endpoints:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/fluvio/api/queues` | List queue stats |
| GET | `/fluvio/api/queues/{name}` | Queue stats plus fleet worker capacity |
| POST | `/fluvio/api/queues/{name}/pause` | Pause a queue |
| POST | `/fluvio/api/queues/{name}/resume` | Resume a queue |
| GET | `/fluvio/api/jobs` | List jobs (`queue`, `state`, `kind`, `limit`, `offset` query params) |
| POST | `/fluvio/api/jobs` | Enqueue a job (`kind`, `queue`, `args`, optional schedule/tags) |
| GET | `/fluvio/api/jobs/{id}` | Get job details |
| POST | `/fluvio/api/jobs/{id}/cancel` | Cancel a pending or scheduled job |
| POST | `/fluvio/api/jobs/{id}/retry` | Run a scheduled job now |
| GET | `/fluvio/api/dead` | List dead-letter jobs (`limit`, `offset`) |
| POST | `/fluvio/api/dead/{id}/replay` | Replay one dead job |
| POST | `/fluvio/api/dead/replay` | Bulk replay (`{ "ids": [1,2,3] }`) |
| POST | `/fluvio/api/dead/purge` | Purge dead jobs before timestamp (`{ "before": "..." }`) |
| GET | `/fluvio/api/periodic` | List periodic (cron) jobs |
| POST | `/fluvio/api/periodic` | Register a periodic job |
| POST | `/fluvio/api/periodic/{kind}/pause` | Pause a periodic job |
| POST | `/fluvio/api/periodic/{kind}/resume` | Resume a periodic job |
| GET | `/fluvio/api/workflows` | List workflows (`limit`, `offset`) |
| GET | `/fluvio/api/workflows/{id}` | Get workflow detail |
| GET | `/fluvio/api/concurrency` | List concurrency slots |
| PUT | `/fluvio/api/concurrency/{kind}` | Set per-kind concurrency limit |
| GET | `/fluvio/api/workers` | List live worker instances |
| GET | `/fluvio/api/events` | SSE stream of queue stats (every 5s) |

`GET /fluvio/api/jobs` and `GET /fluvio/api/dead` return a paginated object: `{ "jobs": [...], "limit": 50, "offset": 0, "has_more": false }`. Default `limit` is 50 (max 100). Use `has_more` to fetch the next page with a higher `offset`. Job objects use snake_case JSON field names.

## Advanced features

### Durable periodic jobs

Register cron schedules that survive restarts. The leader-elected instance enqueues jobs on each tick.

```go
client.AddPeriodicJob("0 9 * * *", DailyReportArgs{Format: "pdf"})
```

List, pause, and resume periodic jobs with `ListPeriodicJobs`, `PausePeriodicJob`, and `ResumePeriodicJob`.

### Workflows (DAG)

Chain jobs with dependencies. Root tasks enqueue immediately; downstream tasks enqueue when their dependencies complete. A failed task cancels dependents.

```go
wfID, err := client.EnqueueWorkflow(ctx, fluvio.NewWorkflow().
    Task("A", TaskAArgs{}).
    Task("B", TaskBArgs{}, fluvio.WithDependsOn("A")).
    Task("C", TaskCArgs{}, fluvio.WithDependsOn("A")).
    Task("D", TaskDArgs{}, fluvio.WithDependsOn("B", "C")))

state, err := client.GetWorkflow(ctx, wfID)
```

Use `WithTaskEnqueueOptions` to pass enqueue options (queue, max attempts, etc.) to individual tasks.

### Concurrency limits

Cap how many jobs of a given kind run at once across the fleet. Limits are stored in the database; the leader enforces them before fetch.

```go
client.SetConcurrencyLimit(ctx, fluvio.ConcurrencyLimitConfig{
    Kind:          "send_email",
    MaxConcurrent: 5,
})
```

For per-tenant limits, provide a `PartitionKeyFn` that extracts a partition from raw args JSON. `PartitionKeyFn` is held in memory only — each worker process must call `SetConcurrencyLimit` on startup.

### Custom retry backoff

Override `NextAttempt` on your worker to customize the retry schedule. Call `DefaultRetryDelayForJob` to fall back to the built-in exponential schedule for some error types:

```go
func (w *MyWorker) NextAttempt(job *fluvio.Job[MyArgs], err error) time.Duration {
	if errors.Is(err, ErrRateLimited) {
		return 60 * time.Second // fixed delay for rate limit errors
	}
	return fluvio.DefaultRetryDelayForJob(job, w.cfg.MaxRetryDelay)
}
```

### Dead letter queue

Jobs that exhaust retries move to `dead` state and are copied to `fluvio_dead_jobs`. Inspect, replay, or purge them:

```go
dead, err := client.ListDeadJobs(ctx, 50, 0)
err = client.ReplayDeadJob(ctx, jobID)
n, err := client.PurgeDeadJobs(ctx, time.Now().Add(-30*24*time.Hour))
```

### Encrypted args

Encrypt job arguments at rest with AES-256-GCM or a custom `KeyProvider` (KMS, Vault, etc.):

```go
key, _ := hex.DecodeString(os.Getenv("FLUVIO_ENCRYPTION_KEY")) // 32 bytes
kp, _ := fluvio.NewAESGCMKeyProvider(key)

client, _ := fluvio.NewClient(d, &fluvio.Config{
    KeyProvider: kp,
    // ...
})

client.Enqueue(ctx, SensitiveArgs{Token: "secret"}, fluvio.WithEncryption())
```

Workers decrypt args automatically before `Work` is called.

## CLI

```bash
fluvio migrate up --dsn "$DATABASE_URL"
fluvio migrate down --steps 1 --dsn "$DATABASE_URL"
fluvio migrate status --dsn "$DATABASE_URL"
fluvio inspect --dsn "$DATABASE_URL"
```

Build the CLI:

```bash
make build   # outputs bin/fluvio
```

## Configuration

### Client (`fluvio.Config`)

| Field | Default | Description |
|-------|---------|-------------|
| `FetchInterval` | 500ms | Fallback poll interval when LISTEN/NOTIFY is enabled; primary poll interval when `PollOnly` is true |
| `PollOnly` | false | Disable LISTEN/NOTIFY wakeup (required with PgBouncer transaction pooling) |
| `NotifyDebounce` | 100ms | Minimum interval between PostgreSQL NOTIFY calls per queue channel |
| `JobTimeout` | 30m | Max running time before reaper nacks |
| `MaxRetryDelay` | 24h | Cap on exponential backoff |
| `PeriodicInterval` | 30s | Tick interval for durable cron jobs |
| `WorkerID` | `{hostname}-{pid}` | Instance identifier stored in `attempted_by` and the fleet registry |
| `WorkerHeartbeatInterval` | 30s | How often processing clients heartbeat to the fleet registry |
| `WorkerTTL` | 90s | Staleness threshold when listing live workers via `ListWorkers` |
| `KeyProvider` | nil | Enables `WithEncryption()` when set |
| `LeaderServicesStartupDelay` | 0 | Delay before first scheduler/reaper/periodic tick after leader election (recommend 15s in production) |

Set `WorkerID` explicitly in production so job pickup and fleet visibility are stable across restarts (e.g. `"api-worker-" + os.Getenv("HOSTNAME")`). Use `fluvio.DefaultWorkerID()` for the built-in default.

Each `Job` passed to `Work()` includes `WorkerID` (this instance), `MaxWorkers` (local queue concurrency), and `AttemptedBy` (claim history). Use `job.ClaimedBy()` for the worker that claimed the current attempt.

### Job logs

Workers can attach structured log entries during execution with `job.Info`, `job.Warn`, `job.Error`, or `job.Debug`. On successful completion, entries are persisted to `fluvio_jobs.logs` as a JSONB array (`level`, `message`, `at`, optional `data`). Failed attempts do not persist logs.

```go
func (w *SendEmailWorker) Work(ctx context.Context, job *fluvio.Job[SendEmailArgs]) error {
    // ... send email ...
    job.Info("email sent", map[string]any{"to": job.Args.To, "message_id": msgID})
    return nil
}
```

Inspect persisted logs with `client.GetJob` or the fluviui API (`GET /fluvio/api/jobs/{id}` returns a `logs` field on completed jobs).

Processing clients with at least one queue where `MaxWorkers > 0` register in the `fluvio_workers` table. Query the fleet with `client.ListWorkers(ctx)` or `client.QueueWorkerCapacity(ctx, queue)`.

Per-queue `MaxWorkers` controls concurrency — each queue gets its own fetch loop capped at that limit. Set `MaxWorkers` to 0 to disable processing for a queue. Omit queues entirely for insert-only clients.

Leader election (Postgres advisory lock) runs scheduled sweeps, durable periodic jobs, and the stuck-job reaper on one instance. For production HA, prefer `UseLeaseTable: true` on the Postgres driver; advisory-lock mode requires a stable dedicated connection.

### Job wakeup (PostgreSQL)

The Postgres driver uses **LISTEN/NOTIFY** for low-latency job pickup. When a pending job is enqueued (or promoted from `scheduled`), workers are woken immediately; `FetchInterval` remains as a fallback poll. Set `PollOnly: true` on the client (or `postgres.Config`) when using PgBouncer in transaction pooling mode, where LISTEN is not supported.

### Schema notes

The `fluvio_jobs` columns `batch_id`, `sequence_id`, and `sequence_pos` are reserved for a future sequences feature and are not currently used by the library.

### fluviui

The `fluviui` HTTP handlers are unauthenticated. Deploy behind a reverse proxy or use `fluviui.WithMiddleware` to add authentication.

Job list and detail responses include `logs` (JSON array) for completed jobs that recorded execution logs. [Fluvio UI](https://github.com/Software78/fluvio_ui) consumes this field from `GET /fluvio/api/jobs` and `GET /fluvio/api/jobs/{id}`.

### Postgres driver (`postgres.Config`)

| Field | Default | Description |
|-------|---------|-------------|
| `UseLeaseTable` | false | Use lease table instead of advisory lock (PgBouncer-compatible) |
| `LeaderID` | `{hostname}-{pid}` | Instance identifier for lease-table leader election |

## Development

```bash
make test              # unit tests with -race
make test-integration  # requires Docker
make coverage          # unit + integration profiles, summary + coverage.html
```

## License

Apache 2.0 — see [LICENSE](LICENSE).

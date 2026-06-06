# Fluvio

A production-grade background job queue for Go backed by PostgreSQL. Enqueue jobs in the same database transaction as your business logic — no Redis, no separate broker.

## Features (OSS)

| Feature | OSS | Pro (forthcoming) |
|---------|-----|-------------------|
| Transactional enqueue | Yes | Yes |
| Fetch/work/retry with SKIP LOCKED | Yes | Yes |
| Multiple queues | Yes | Yes |
| Unique jobs | Yes | Yes |
| Scheduled jobs | Yes | Yes |
| In-memory periodic (cron) | Yes | Yes |
| Workflows (DAG) | — | Pro |
| Sequences | — | Pro |
| Concurrency limits | — | Pro |
| Dead letter queue | — | Pro |
| Durable periodic | — | Pro |
| Encrypted args | — | Pro |

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

`EnqueueMany` runs all inserts in one transaction. If any row fails (including a unique-key conflict), the entire batch is rolled back and no jobs are inserted.

Use `UniqueJobExists` to check for an active job with a given unique key before enqueueing.

### 4. Web API

Mount the REST API and SSE stream for a separate UI or admin tool:

```go
import "github.com/software78/fluvio/fluviui"

// Production — restrict origin
mux.Handle("/fluvio/", fluviui.Handler(client,
    fluviui.WithAllowedOrigin("https://your-ui.example.com"),
))

// Development — allow all origins (default)
mux.Handle("/fluvio/", fluviui.Handler(client))
```

Endpoints:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/fluvio/api/queues` | List queue stats |
| POST | `/fluvio/api/queues/{name}/pause` | Pause a queue |
| POST | `/fluvio/api/queues/{name}/resume` | Resume a queue |
| GET | `/fluvio/api/jobs` | List jobs (`queue`, `state`, `kind`, `limit`, `offset` query params) |
| GET | `/fluvio/api/jobs/{id}` | Get job details |
| GET | `/fluvio/api/events` | SSE stream of queue stats (every 5s) |

`GET /fluvio/api/jobs` returns a paginated object: `{ "jobs": [...], "limit": 50, "offset": 0, "has_more": false }`. Default `limit` is 50 (max 100). Use `has_more` to fetch the next page with a higher `offset`.

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
| `FetchInterval` | 500ms | Poll interval for new jobs |
| `JobTimeout` | 30m | Max running time before reaper nacks |
| `MaxRetryDelay` | 24h | Cap on exponential backoff |
| `PeriodicInterval` | 30s | Tick interval for in-memory cron jobs |
| `WorkerID` | `{hostname}-{pid}` | Instance identifier stored in `attempted_by` and the fleet registry |
| `WorkerHeartbeatInterval` | 30s | How often processing clients heartbeat to the fleet registry |
| `WorkerTTL` | 90s | Staleness threshold when listing live workers via `ListWorkers` |

Set `WorkerID` explicitly in production so job pickup and fleet visibility are stable across restarts (e.g. `"api-worker-" + os.Getenv("HOSTNAME")`). Use `fluvio.DefaultWorkerID()` for the built-in default.

Each `Job` passed to `Work()` includes `WorkerID` (this instance), `MaxWorkers` (local queue concurrency), and `AttemptedBy` (claim history). Use `job.ClaimedBy()` for the worker that claimed the current attempt.

Processing clients with at least one queue where `MaxWorkers > 0` register in the `fluvio_workers` table. Query the fleet with `client.ListWorkers(ctx)` or `client.QueueWorkerCapacity(ctx, queue)`.

Per-queue `MaxWorkers` controls concurrency — each queue gets its own fetch loop capped at that limit. Set `MaxWorkers` to 0 to disable processing for a queue. Omit queues entirely for insert-only clients.

Leader election (Postgres advisory lock) runs scheduled sweeps, in-memory periodic jobs, and the stuck-job reaper on one instance.

### Postgres driver (`postgres.Config`)

| Field | Default | Description |
|-------|---------|-------------|
| `UseLeaseTable` | false | Use lease table instead of advisory lock (PgBouncer-compatible) |
| `LeaderID` | `{hostname}-{pid}` | Instance identifier for lease-table leader election |

## Development

```bash
make test              # unit tests with -race
make test-integration  # requires Docker
```

## License

Apache 2.0 — see [LICENSE](LICENSE).

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
| Web UI (inspect) | Yes | Yes |
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

### 4. Web UI

```go
import "github.com/software78/fluvio/fluviui"

adapter := &fluviui.ClientAdapter{Client: client}
http.Handle("/fluvio/", fluviui.Handler(adapter, "/fluvio/"))
```

Open `/fluvio/` for dashboard, job list, and queue management.

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
| `WorkerID` | `{hostname}-{pid}` | Identifier stored in `attempted_by` |

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

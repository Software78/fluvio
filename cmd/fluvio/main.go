package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/software78/fluvio/postgres"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "migrate":
		migrateCmd(os.Args[2:])
	case "inspect":
		inspectCmd(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Fluvio — Postgres job queue

Usage:
  fluvio migrate up --dsn URL
  fluvio migrate down --steps N --dsn URL
  fluvio migrate status --dsn URL
  fluvio inspect --dsn URL

`)
}

func openDriver(dsn string) (*postgres.Driver, func()) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	d := postgres.New(pool, postgres.Config{})
	return d, func() { d.Close() }
}

func migrateCmd(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "migrate requires subcommand: up, down, status")
		os.Exit(1)
	}
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	dsn := fs.String("dsn", os.Getenv("DATABASE_URL"), "Postgres connection string")
	steps := fs.Int("steps", 1, "Number of migrations to roll back")
	_ = fs.Parse(args[1:])

	d, cleanup := openDriver(*dsn)
	defer cleanup()
	ctx := context.Background()

	switch args[0] {
	case "up":
		if err := d.Migrate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "migrate up: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("migrations applied")
	case "down":
		if err := d.MigrateDown(ctx, *steps); err != nil {
			fmt.Fprintf(os.Stderr, "migrate down: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("rolled back %d migration(s)\n", *steps)
	case "status":
		versions, err := d.MigrationStatus(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "migrate status: %v\n", err)
			os.Exit(1)
		}
		if len(versions) == 0 {
			fmt.Println("no migrations applied")
			return
		}
		for _, v := range versions {
			fmt.Println(v)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown migrate subcommand")
		os.Exit(1)
	}
}

func inspectCmd(args []string) {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	dsn := fs.String("dsn", os.Getenv("DATABASE_URL"), "Postgres connection string")
	_ = fs.Parse(args)

	d, cleanup := openDriver(*dsn)
	defer cleanup()
	ctx := context.Background()

	stats, err := d.ListQueues(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "inspect: %v\n", err)
		os.Exit(1)
	}
	for _, s := range stats {
		fmt.Printf("%s: pending=%d running=%d scheduled=%d dead=%d paused=%v\n",
			s.Queue, s.Pending, s.Running, s.Scheduled, s.Dead, s.Paused)
	}

	workers, err := d.ListWorkers(ctx, 90*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "inspect workers: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("\nworkers (%d live):\n", len(workers))
	capacity := map[string]int{}
	for _, w := range workers {
		fmt.Printf("  %s: queues=%v started=%s last_seen=%s\n",
			w.ID, w.Queues, w.StartedAt.Format(time.RFC3339), w.LastSeen.Format(time.RFC3339))
		for queue, max := range w.Queues {
			capacity[queue] += max
		}
	}
	if len(capacity) > 0 {
		fmt.Println("\nfleet capacity:")
		for queue, total := range capacity {
			fmt.Printf("  %s: max_concurrent=%d\n", queue, total)
		}
	}
}

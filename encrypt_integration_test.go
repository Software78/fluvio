//go:build integration

package fluvio_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	fluvio "github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
	"github.com/software78/fluvio/postgres"
)

func setupEncryptionDB(t *testing.T) (*pgxpool.Pool, driver.Driver) {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("fluvio"),
		tcpostgres.WithUsername("fluvio"),
		tcpostgres.WithPassword("fluvio"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

	d := postgres.New(pool, postgres.Config{})
	require.NoError(t, d.Migrate(ctx))
	return pool, d
}

func newEncryptionClient(t *testing.T, d driver.Driver, keyProvider fluvio.KeyProvider) (*fluvio.Client, *HelloWorker) {
	t.Helper()
	workers := fluvio.NewWorkers()
	hw := &HelloWorker{done: make(chan int64, 4)}
	fluvio.AddWorker(workers, hw)

	cfg := &fluvio.Config{
		Queues: map[string]fluvio.QueueConfig{
			fluvio.QueueDefault: {MaxWorkers: 5},
		},
		Workers: workers,
	}
	if keyProvider != nil {
		cfg.KeyProvider = keyProvider
	}

	client, err := fluvio.NewClient(d, cfg)
	require.NoError(t, err)
	return client, hw
}

func TestEncryptionRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	kp, err := fluvio.NewAESGCMKeyProvider(key)
	require.NoError(t, err)

	pool, d := setupEncryptionDB(t)
	client, hw := newEncryptionClient(t, d, kp)
	ctx := context.Background()

	row, err := client.Enqueue(ctx, HelloArgs{Name: "secret"}, fluvio.WithEncryption())
	require.NoError(t, err)

	var argsText string
	var encrypted bool
	err = pool.QueryRow(ctx,
		`SELECT args::text, encrypted FROM fluvio_jobs WHERE id = $1`, row.ID,
	).Scan(&argsText, &encrypted)
	require.NoError(t, err)
	require.True(t, encrypted)
	require.NotEqual(t, `{"name":"secret"}`, argsText)

	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { client.Stop() })

	select {
	case id := <-hw.done:
		require.Equal(t, row.ID, id)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for encrypted job")
	}

	job := hw.lastJob.Load()
	require.NotNil(t, job)
	require.Equal(t, "secret", job.Args.Name)
}

func TestEncryptionNoKeyProviderNack(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	kp, err := fluvio.NewAESGCMKeyProvider(key)
	require.NoError(t, err)

	_, d := setupEncryptionDB(t)
	ctx := context.Background()

	encWorkers := fluvio.NewWorkers()
	fluvio.AddWorker(encWorkers, &HelloWorker{done: make(chan int64, 1)})
	encClient, err := fluvio.NewClient(d, &fluvio.Config{
		Queues: map[string]fluvio.QueueConfig{
			fluvio.QueueDefault: {MaxWorkers: 5},
		},
		Workers:     encWorkers,
		KeyProvider: kp,
	})
	require.NoError(t, err)

	row, err := encClient.Enqueue(ctx, HelloArgs{Name: "secret"}, fluvio.WithEncryption())
	require.NoError(t, err)

	noKeyClient, _ := newEncryptionClient(t, d, nil)
	require.NoError(t, noKeyClient.Start(ctx))
	t.Cleanup(func() { noKeyClient.Stop() })

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job, err := noKeyClient.GetJob(ctx, row.ID)
		require.NoError(t, err)
		if job.State == fluvio.JobStateScheduled {
			require.Contains(t, string(job.ErrorTrace), fluvio.ErrNoKeyProvider.Error())
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("expected job %d to be nacked with ErrNoKeyProvider", row.ID)
}

func TestEncryptionEnqueueWithoutKeyProvider(t *testing.T) {
	_, d := setupEncryptionDB(t)
	client, _ := newEncryptionClient(t, d, nil)
	ctx := context.Background()

	_, err := client.Enqueue(ctx, HelloArgs{Name: "secret"}, fluvio.WithEncryption())
	require.ErrorIs(t, err, fluvio.ErrNoKeyProvider)
}

func TestAESGCMKeyProviderRejectsInvalidKey(t *testing.T) {
	_, err := fluvio.NewAESGCMKeyProvider([]byte("too-short"))
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "32 bytes"))
}

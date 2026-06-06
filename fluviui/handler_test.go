package fluviui

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
)

type mockClient struct{}

func (mockClient) ListQueues(ctx context.Context) ([]*driver.QueueStats, error) {
	return []*driver.QueueStats{{
		Queue: "default", Pending: 3, Running: 1,
	}}, nil
}

func (mockClient) ListJobs(ctx context.Context, queue, state, kind string, limit, offset int) ([]fluvio.JobRow, error) {
	return []fluvio.JobRow{{ID: 1, Queue: "default", Kind: "hello", State: fluvio.JobStatePending}}, nil
}

func (mockClient) GetJob(ctx context.Context, id int64) (*fluvio.JobRow, error) {
	return &fluvio.JobRow{ID: id, Kind: "hello"}, nil
}

func (mockClient) PauseQueue(ctx context.Context, queue string) error  { return nil }
func (mockClient) ResumeQueue(ctx context.Context, queue string) error { return nil }

type errorClient struct{}

func (errorClient) ListQueues(ctx context.Context) ([]*driver.QueueStats, error) {
	return nil, errors.New("pq: connection refused to 10.0.0.1:5432")
}

func (errorClient) ListJobs(ctx context.Context, queue, state, kind string, limit, offset int) ([]fluvio.JobRow, error) {
	return nil, nil
}

func (errorClient) GetJob(ctx context.Context, id int64) (*fluvio.JobRow, error) {
	return nil, fluvio.ErrJobNotFound
}

func (errorClient) PauseQueue(ctx context.Context, queue string) error  { return nil }
func (errorClient) ResumeQueue(ctx context.Context, queue string) error { return nil }

func TestHandlerAPIQueues(t *testing.T) {
	h := handlerFor(mockClient{})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/fluvio/api/queues")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var stats []*QueueStatsView
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&stats))
	require.Len(t, stats, 1)
	require.Equal(t, int64(3), stats[0].Pending)
}

func TestHandlerAPIErrorSanitized(t *testing.T) {
	h := handlerFor(errorClient{})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/fluvio/api/queues")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "internal server error", body["error"])
	require.NotContains(t, body["error"], "connection refused")
}

func TestCORSDefaultOrigin(t *testing.T) {
	h := handlerFor(mockClient{})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/fluvio/api/queues")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
	require.Equal(t, "GET, POST, OPTIONS", resp.Header.Get("Access-Control-Allow-Methods"))
	require.Equal(t, "Content-Type", resp.Header.Get("Access-Control-Allow-Headers"))
}

func TestCORSWithAllowedOrigin(t *testing.T) {
	h := handlerFor(mockClient{}, WithAllowedOrigin("https://ui.example.com"))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/fluvio/api/queues")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, "https://ui.example.com", resp.Header.Get("Access-Control-Allow-Origin"))
}

func TestCORSOptionsPreflight(t *testing.T) {
	h := handlerFor(mockClient{}, WithAllowedOrigin("https://ui.example.com"))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	req, err := http.NewRequest(http.MethodOptions, srv.URL+"/fluvio/api/queues", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.Equal(t, "https://ui.example.com", resp.Header.Get("Access-Control-Allow-Origin"))
	require.Equal(t, "GET, POST, OPTIONS", resp.Header.Get("Access-Control-Allow-Methods"))
	require.Equal(t, "Content-Type", resp.Header.Get("Access-Control-Allow-Headers"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Empty(t, body)
}

func TestCORSOnSSEEndpoint(t *testing.T) {
	h := handlerFor(mockClient{})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/fluvio/api/events", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
	cancel()
}

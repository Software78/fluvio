package fluviui

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
)

type mockClient struct {
	stubAPIClient
}

func (mockClient) ListQueues(ctx context.Context) ([]*driver.QueueStats, error) {
	return []*driver.QueueStats{{
		Queue: "default", Pending: 3, Running: 1,
	}}, nil
}

func (mockClient) ListJobs(ctx context.Context, queue, state, kind string, limit, offset int) ([]fluvio.JobRow, error) {
	_ = ctx
	_ = queue
	_ = state
	_ = kind
	return []fluvio.JobRow{{ID: 1, Queue: "default", Kind: "hello", State: fluvio.JobStatePending}}, nil
}

func (mockClient) GetJob(ctx context.Context, id int64) (*fluvio.JobRow, error) {
	return &fluvio.JobRow{ID: id, Kind: "hello"}, nil
}

func (mockClient) PauseQueue(ctx context.Context, queue string) error  { return nil }
func (mockClient) ResumeQueue(ctx context.Context, queue string) error { return nil }
func (mockClient) ListWorkers(ctx context.Context) ([]fluvio.WorkerInstance, error) {
	return nil, nil
}

type errorClient struct {
	stubAPIClient
}

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
func (errorClient) ListWorkers(ctx context.Context) ([]fluvio.WorkerInstance, error) {
	return nil, nil
}

type pagingClient struct {
	stubAPIClient
	lastLimit  int
	lastOffset int
}

func (c *pagingClient) ListQueues(ctx context.Context) ([]*driver.QueueStats, error) {
	return nil, nil
}

func (c *pagingClient) ListJobs(ctx context.Context, queue, state, kind string, limit, offset int) ([]fluvio.JobRow, error) {
	_ = ctx
	_ = queue
	_ = state
	_ = kind
	c.lastLimit = limit
	c.lastOffset = offset
	jobs := make([]fluvio.JobRow, limit)
	for i := range jobs {
		jobs[i] = fluvio.JobRow{ID: int64(offset + i + 1)}
	}
	return jobs, nil
}

func (c *pagingClient) GetJob(ctx context.Context, id int64) (*fluvio.JobRow, error) {
	return nil, fluvio.ErrJobNotFound
}

func (c *pagingClient) PauseQueue(ctx context.Context, queue string) error  { return nil }
func (c *pagingClient) ResumeQueue(ctx context.Context, queue string) error { return nil }
func (c *pagingClient) ListWorkers(ctx context.Context) ([]fluvio.WorkerInstance, error) {
	return nil, nil
}

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

func TestHandlerAPIJobsPagination(t *testing.T) {
	client := &pagingClient{}
	h := handlerFor(client)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/fluvio/api/jobs?limit=2&offset=10")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, 3, client.lastLimit)
	require.Equal(t, 10, client.lastOffset)

	var page JobsPage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&page))
	require.Equal(t, 2, page.Limit)
	require.Equal(t, 10, page.Offset)
	require.True(t, page.HasMore)
	require.Len(t, page.Jobs, 2)
	require.Equal(t, int64(11), page.Jobs[0].ID)
}

func TestHandlerAPIJobsPaginationDefaults(t *testing.T) {
	client := &pagingClient{}
	h := handlerFor(client)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/fluvio/api/jobs")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, defaultJobsLimit+1, client.lastLimit)
	require.Equal(t, 0, client.lastOffset)

	var page JobsPage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&page))
	require.Equal(t, defaultJobsLimit, page.Limit)
	require.Equal(t, 0, page.Offset)
	require.True(t, page.HasMore)
}

func TestHandlerAPIJobsInvalidLimit(t *testing.T) {
	h := handlerFor(mockClient{})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/fluvio/api/jobs?limit=0")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "invalid request", body["error"])
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

func TestJobRowViewIncludesLogs(t *testing.T) {
	logs := json.RawMessage(`[{"level":"info","message":"ok"}]`)
	view := jobRowToView(fluvio.JobRow{
		ID: 1, Queue: "default", Kind: "hello", State: fluvio.JobStateCompleted, Logs: logs,
	})
	b, err := json.Marshal(view)
	require.NoError(t, err)
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(b, &m))
	require.JSONEq(t, string(logs), string(m["logs"]))
}

type logsJobClient struct {
	mockClient
}

func (logsJobClient) GetJob(ctx context.Context, id int64) (*fluvio.JobRow, error) {
	_ = ctx
	return &fluvio.JobRow{
		ID:    id,
		Kind:  "hello",
		State: fluvio.JobStateCompleted,
		Logs:  json.RawMessage(`[{"level":"info","message":"done","data":{"rows":3}}]`),
	}, nil
}

func TestHandlerAPIJobDetailLogs(t *testing.T) {
	h := handlerFor(logsJobClient{})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/fluvio/api/jobs/42")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var view JobRowView
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&view))
	require.Equal(t, int64(42), view.ID)
	require.Equal(t, fluvio.JobStateCompleted, view.State)
	require.JSONEq(t, `[{"level":"info","message":"done","data":{"rows":3}}]`, string(view.Logs))
}

func TestCORSDefaultOrigin(t *testing.T) {
	h := handlerFor(mockClient{})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/fluvio/api/queues")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
	require.Equal(t, "GET, POST, PUT, OPTIONS", resp.Header.Get("Access-Control-Allow-Methods"))
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
	require.Equal(t, "GET, POST, PUT, OPTIONS", resp.Header.Get("Access-Control-Allow-Methods"))
	require.Equal(t, "Content-Type", resp.Header.Get("Access-Control-Allow-Headers"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Empty(t, body)
}

func TestJobDetailRejectsInvalidID(t *testing.T) {
	h := handlerFor(mockClient{})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/fluvio/api/jobs/123abc")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
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

func TestSSEFirstEventIsStats(t *testing.T) {
	h := handlerFor(mockClient{})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/fluvio/api/events", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	scanner := bufio.NewScanner(resp.Body)
	var lines []string
	for scanner.Scan() {
		line := scanner.Text()
		lines = append(lines, line)
		if len(lines) >= 2 {
			break
		}
	}
	require.GreaterOrEqual(t, len(lines), 1)
	require.Equal(t, "event: stats", lines[0])
}

func TestSSEStreamKeepalive(t *testing.T) {
	h := handlerFor(mockClient{}, WithKeepaliveInterval(50*time.Millisecond))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/fluvio/api/events", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	done := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(resp.Body)
		done <- b
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	body := string(<-done)
	require.GreaterOrEqual(t, strings.Count(body, "event: stats"), 1)
	require.Contains(t, body, ": keepalive")
}

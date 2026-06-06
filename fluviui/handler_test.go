package fluviui_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/software78/fluvio"
	"github.com/software78/fluvio/fluviui"
)

type mockInspector struct{}

func (mockInspector) ListQueues(ctx context.Context) ([]*fluviui.QueueStatsView, error) {
	return []*fluviui.QueueStatsView{{
		Queue: "default", Pending: 3, Running: 1,
	}}, nil
}

func (mockInspector) ListJobs(ctx context.Context, queue, state, kind string, limit, offset int) ([]fluvio.JobRow, error) {
	return []fluvio.JobRow{{ID: 1, Queue: "default", Kind: "hello", State: fluvio.JobStatePending}}, nil
}

func (mockInspector) GetJob(ctx context.Context, id int64) (*fluvio.JobRow, error) {
	return &fluvio.JobRow{ID: id, Kind: "hello"}, nil
}

func (mockInspector) PauseQueue(ctx context.Context, queue string) error  { return nil }
func (mockInspector) ResumeQueue(ctx context.Context, queue string) error { return nil }

func TestHandlerAPIQueues(t *testing.T) {
	h := fluviui.Handler(mockInspector{}, "/fluvio/")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/fluvio/api/queues")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var stats []*fluviui.QueueStatsView
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&stats))
	require.Len(t, stats, 1)
	require.Equal(t, int64(3), stats[0].Pending)
}

func TestHandlerAPIErrorSanitized(t *testing.T) {
	h := fluviui.Handler(errorInspector{}, "/fluvio/")
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

type errorInspector struct{}

func (errorInspector) ListQueues(ctx context.Context) ([]*fluviui.QueueStatsView, error) {
	return nil, errors.New("pq: connection refused to 10.0.0.1:5432")
}

func (errorInspector) ListJobs(ctx context.Context, queue, state, kind string, limit, offset int) ([]fluvio.JobRow, error) {
	return nil, nil
}

func (errorInspector) GetJob(ctx context.Context, id int64) (*fluvio.JobRow, error) {
	return nil, fluvio.ErrJobNotFound
}

func (errorInspector) PauseQueue(ctx context.Context, queue string) error  { return nil }
func (errorInspector) ResumeQueue(ctx context.Context, queue string) error { return nil }

func TestHandlerDashboard(t *testing.T) {
	h := fluviui.Handler(mockInspector{}, "/fluvio/")
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/fluvio/")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

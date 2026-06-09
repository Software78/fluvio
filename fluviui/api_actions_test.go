package fluviui

import (
	"context"
	"encoding/json"
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

type actionClient struct {
	stubAPIClient
	deadJobs       []fluvio.JobRow
	replayedIDs    []int64
	purgedBefore   time.Time
	enqueued       []fluvio.EnqueueRawParams
	cancelledID    int64
	retriedID      int64
	periodicJobs   []driver.PeriodicJob
	addedPeriodic  bool
	pausedKind     string
	resumedKind    string
	workflows      []*driver.WorkflowState
	concurrency    []driver.ConcurrencySlot
	setConcurrency fluvio.ConcurrencyLimitConfig
	replayErrID    int64
}

func (c *actionClient) ListDeadJobs(ctx context.Context, limit, offset int) ([]fluvio.JobRow, error) {
	_ = ctx
	start := offset
	if start > len(c.deadJobs) {
		return nil, nil
	}
	end := start + limit
	if end > len(c.deadJobs) {
		end = len(c.deadJobs)
	}
	return c.deadJobs[start:end], nil
}

func (c *actionClient) ReplayDeadJob(ctx context.Context, jobID int64) error {
	_ = ctx
	if jobID == c.replayErrID {
		return fluvio.ErrJobNotFound
	}
	c.replayedIDs = append(c.replayedIDs, jobID)
	return nil
}

func (c *actionClient) PurgeDeadJobs(ctx context.Context, before time.Time) (int64, error) {
	_ = ctx
	c.purgedBefore = before
	return 3, nil
}

func (c *actionClient) EnqueueRaw(ctx context.Context, p fluvio.EnqueueRawParams) (*fluvio.JobRow, error) {
	_ = ctx
	c.enqueued = append(c.enqueued, p)
	return &fluvio.JobRow{ID: 99, Kind: p.Kind, Queue: p.Queue, State: fluvio.JobStatePending}, nil
}

func (c *actionClient) Cancel(ctx context.Context, jobID int64) error {
	_ = ctx
	c.cancelledID = jobID
	return nil
}

func (c *actionClient) RunJobNow(ctx context.Context, jobID int64) error {
	_ = ctx
	c.retriedID = jobID
	return nil
}

func (c *actionClient) ListPeriodicJobs(ctx context.Context) ([]driver.PeriodicJob, error) {
	_ = ctx
	return c.periodicJobs, nil
}

func (c *actionClient) AddPeriodicJobRaw(ctx context.Context, cronExpr, kind, queue string, args []byte, maxAttempts int16) error {
	_ = ctx
	c.addedPeriodic = true
	return nil
}

func (c *actionClient) PausePeriodicJob(ctx context.Context, kind string) error {
	_ = ctx
	c.pausedKind = kind
	return nil
}

func (c *actionClient) ResumePeriodicJob(ctx context.Context, kind string) error {
	_ = ctx
	c.resumedKind = kind
	return nil
}

func (c *actionClient) ListWorkflows(ctx context.Context, limit, offset int) ([]*driver.WorkflowState, error) {
	_ = ctx
	return c.workflows, nil
}

func (c *actionClient) GetWorkflow(ctx context.Context, workflowID string) (*driver.WorkflowState, error) {
	_ = ctx
	for _, w := range c.workflows {
		if w.ID == workflowID {
			return w, nil
		}
	}
	return nil, fluvio.ErrWorkflowNotFound
}

func (c *actionClient) ListConcurrencySlots(ctx context.Context) ([]driver.ConcurrencySlot, error) {
	_ = ctx
	return c.concurrency, nil
}

func (c *actionClient) SetConcurrencyLimit(ctx context.Context, cfg fluvio.ConcurrencyLimitConfig) error {
	_ = ctx
	c.setConcurrency = cfg
	return nil
}

func (c *actionClient) QueueStats(ctx context.Context, queue string) (*driver.QueueStats, error) {
	_ = ctx
	return &driver.QueueStats{Queue: queue, Pending: 5}, nil
}

func (c *actionClient) QueueWorkerCapacity(ctx context.Context, queue string) (int, int, error) {
	_ = ctx
	return 2, 10, nil
}

func (c *actionClient) ListQueues(ctx context.Context) ([]*driver.QueueStats, error) {
	return nil, nil
}

func (c *actionClient) ListJobs(ctx context.Context, queue, state, kind string, limit, offset int) ([]fluvio.JobRow, error) {
	return nil, nil
}

func (c *actionClient) GetJob(ctx context.Context, id int64) (*fluvio.JobRow, error) {
	return &fluvio.JobRow{ID: id, Kind: "test", State: fluvio.JobStatePending}, nil
}

func (c *actionClient) PauseQueue(ctx context.Context, queue string) error  { return nil }
func (c *actionClient) ResumeQueue(ctx context.Context, queue string) error { return nil }
func (c *actionClient) ListWorkers(ctx context.Context) ([]fluvio.WorkerInstance, error) {
	return nil, nil
}

func newActionTestServer(t *testing.T, client *actionClient) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handlerFor(client))
	t.Cleanup(srv.Close)
	return srv
}

func TestDeadListAndReplay(t *testing.T) {
	client := &actionClient{
		deadJobs: []fluvio.JobRow{{ID: 1, Kind: "fail", State: fluvio.JobStateDead}},
	}
	srv := newActionTestServer(t, client)

	resp, err := http.Get(srv.URL + "/fluvio/api/dead")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var page JobsPage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&page))
	require.Len(t, page.Jobs, 1)
	require.Equal(t, int64(1), page.Jobs[0].ID)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/fluvio/api/dead/1/replay", nil)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	require.Equal(t, []int64{1}, client.replayedIDs)
}

func TestDeadBulkReplayPartialFailure(t *testing.T) {
	client := &actionClient{replayErrID: 2}
	srv := newActionTestServer(t, client)

	body := `{"ids":[1,2,3]}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/fluvio/api/dead/replay", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Equal(t, float64(2), result["replayed"])
	errs, ok := result["errors"].([]any)
	require.True(t, ok)
	require.Len(t, errs, 1)
}

func TestDeadPurge(t *testing.T) {
	client := &actionClient{}
	srv := newActionTestServer(t, client)

	body := `{"before":"2024-06-01T00:00:00Z"}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/fluvio/api/dead/purge", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]int64
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Equal(t, int64(3), result["purged"])
	require.False(t, client.purgedBefore.IsZero())
}

func TestJobEnqueueCancelRetry(t *testing.T) {
	client := &actionClient{}
	srv := newActionTestServer(t, client)

	body := `{"kind":"send_email","queue":"mail","args":{"to":"a@b.c"}}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/fluvio/api/jobs", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Len(t, client.enqueued, 1)
	require.Equal(t, "send_email", client.enqueued[0].Kind)

	req2, err := http.NewRequest(http.MethodPost, srv.URL+"/fluvio/api/jobs/42/cancel", nil)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	require.Equal(t, int64(42), client.cancelledID)

	req3, err := http.NewRequest(http.MethodPost, srv.URL+"/fluvio/api/jobs/7/retry", nil)
	require.NoError(t, err)
	resp3, err := http.DefaultClient.Do(req3)
	require.NoError(t, err)
	defer resp3.Body.Close()
	require.Equal(t, http.StatusOK, resp3.StatusCode)
	require.Equal(t, int64(7), client.retriedID)
}

func TestJobDetailSnakeCase(t *testing.T) {
	client := &actionClient{}
	srv := newActionTestServer(t, client)

	resp, err := http.Get(srv.URL + "/fluvio/api/jobs/5")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"max_attempts"`)
	require.NotContains(t, string(raw), `"MaxAttempts"`)
}

func TestPeriodicEndpoints(t *testing.T) {
	client := &actionClient{
		periodicJobs: []driver.PeriodicJob{{Kind: "daily", Cron: "0 9 * * *", Queue: "default"}},
	}
	srv := newActionTestServer(t, client)

	resp, err := http.Get(srv.URL + "/fluvio/api/periodic")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var jobs []PeriodicJobView
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&jobs))
	require.Len(t, jobs, 1)
	require.Equal(t, "daily", jobs[0].Kind)

	addBody := `{"cron":"0 10 * * *","kind":"hourly","args":{}}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/fluvio/api/periodic", strings.NewReader(addBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusCreated, resp2.StatusCode)
	require.True(t, client.addedPeriodic)

	req3, err := http.NewRequest(http.MethodPost, srv.URL+"/fluvio/api/periodic/daily/pause", nil)
	require.NoError(t, err)
	resp3, err := http.DefaultClient.Do(req3)
	require.NoError(t, err)
	defer resp3.Body.Close()
	require.Equal(t, http.StatusOK, resp3.StatusCode)
	require.Equal(t, "daily", client.pausedKind)

	req4, err := http.NewRequest(http.MethodPost, srv.URL+"/fluvio/api/periodic/daily/resume", nil)
	require.NoError(t, err)
	resp4, err := http.DefaultClient.Do(req4)
	require.NoError(t, err)
	defer resp4.Body.Close()
	require.Equal(t, http.StatusOK, resp4.StatusCode)
	require.Equal(t, "daily", client.resumedKind)
}

func TestWorkflowEndpoints(t *testing.T) {
	client := &actionClient{
		workflows: []*driver.WorkflowState{{
			ID: "wf-1", State: "running",
			Tasks: []driver.WorkflowTaskState{{TaskID: "A", State: "completed"}},
		}},
	}
	srv := newActionTestServer(t, client)

	resp, err := http.Get(srv.URL + "/fluvio/api/workflows")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var page WorkflowsPage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&page))
	require.Len(t, page.Workflows, 1)

	resp2, err := http.Get(srv.URL + "/fluvio/api/workflows/wf-1")
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	var wf WorkflowView
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&wf))
	require.Equal(t, "wf-1", wf.ID)
}

func TestQueueDetailAndConcurrency(t *testing.T) {
	client := &actionClient{
		concurrency: []driver.ConcurrencySlot{{Kind: "email", MaxConcurrent: 5}},
	}
	srv := newActionTestServer(t, client)

	resp, err := http.Get(srv.URL + "/fluvio/api/queues/mail")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var detail QueueDetailView
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&detail))
	require.Equal(t, "mail", detail.Queue)
	require.Equal(t, 2, detail.WorkerInstances)
	require.Equal(t, 10, detail.WorkerCapacity)

	resp2, err := http.Get(srv.URL + "/fluvio/api/concurrency")
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	var slots []ConcurrencySlotView
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&slots))
	require.Len(t, slots, 1)

	body := `{"max_concurrent":8}`
	req, err := http.NewRequest(http.MethodPut, srv.URL+"/fluvio/api/concurrency/email", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp3, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp3.Body.Close()
	require.Equal(t, http.StatusOK, resp3.StatusCode)
	require.Equal(t, "email", client.setConcurrency.Kind)
	require.Equal(t, 8, client.setConcurrency.MaxConcurrent)
}

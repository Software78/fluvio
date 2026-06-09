package fluviui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/software78/fluvio"
	"github.com/software78/fluvio/internal/driver"
)

const (
	defaultJobsLimit = 50
	maxJobsLimit     = 100
)

type apiClient interface {
	ListQueues(ctx context.Context) ([]*driver.QueueStats, error)
	ListJobs(ctx context.Context, queue, state, kind string, limit, offset int) ([]fluvio.JobRow, error)
	GetJob(ctx context.Context, id int64) (*fluvio.JobRow, error)
	ListDeadJobs(ctx context.Context, limit, offset int) ([]fluvio.JobRow, error)
	ReplayDeadJob(ctx context.Context, jobID int64) error
	PurgeDeadJobs(ctx context.Context, before time.Time) (int64, error)
	EnqueueRaw(ctx context.Context, p fluvio.EnqueueRawParams) (*fluvio.JobRow, error)
	Cancel(ctx context.Context, jobID int64) error
	RunJobNow(ctx context.Context, jobID int64) error
	PauseQueue(ctx context.Context, queue string) error
	ResumeQueue(ctx context.Context, queue string) error
	QueueStats(ctx context.Context, queue string) (*driver.QueueStats, error)
	QueueWorkerCapacity(ctx context.Context, queue string) (instances, maxConcurrent int, err error)
	ListWorkers(ctx context.Context) ([]fluvio.WorkerInstance, error)
	ListPeriodicJobs(ctx context.Context) ([]driver.PeriodicJob, error)
	AddPeriodicJobRaw(ctx context.Context, cronExpr, kind, queue string, args []byte, maxAttempts int16) error
	PausePeriodicJob(ctx context.Context, kind string) error
	ResumePeriodicJob(ctx context.Context, kind string) error
	ListWorkflows(ctx context.Context, limit, offset int) ([]*driver.WorkflowState, error)
	GetWorkflow(ctx context.Context, workflowID string) (*driver.WorkflowState, error)
	ListConcurrencySlots(ctx context.Context) ([]driver.ConcurrencySlot, error)
	SetConcurrencyLimit(ctx context.Context, cfg fluvio.ConcurrencyLimitConfig) error
}

// QueueStatsView mirrors driver stats for JSON API responses.
type QueueStatsView struct {
	Queue     string `json:"queue"`
	Pending   int64  `json:"pending"`
	Running   int64  `json:"running"`
	Scheduled int64  `json:"scheduled"`
	Dead      int64  `json:"dead"`
	Completed int64  `json:"completed"`
	Failed    int64  `json:"failed"`
	Cancelled int64  `json:"cancelled"`
	Paused    bool   `json:"paused"`
}

// WorkerView mirrors fleet registry entries for JSON API responses.
type WorkerView struct {
	ID        string         `json:"id"`
	Queues    map[string]int `json:"queues"`
	StartedAt time.Time      `json:"started_at"`
	LastSeen  time.Time      `json:"last_seen"`
}

// JobsPage is a paginated jobs list response.
type JobsPage struct {
	Jobs    []JobRowView `json:"jobs"`
	Limit   int          `json:"limit"`
	Offset  int          `json:"offset"`
	HasMore bool         `json:"has_more"`
}

func parseJobsPagination(q url.Values) (limit, offset int, err error) {
	limit = defaultJobsLimit
	offset = 0

	if s := q.Get("limit"); s != "" {
		limit, err = strconv.Atoi(s)
		if err != nil || limit < 1 || limit > maxJobsLimit {
			return 0, 0, fmt.Errorf("%w: limit must be between 1 and %d", fluvio.ErrInvalidConfig, maxJobsLimit)
		}
	}
	if s := q.Get("offset"); s != "" {
		offset, err = strconv.Atoi(s)
		if err != nil || offset < 0 {
			return 0, 0, fmt.Errorf("%w: offset must be >= 0", fluvio.ErrInvalidConfig)
		}
	}
	return limit, offset, nil
}

func listQueuesView(ctx context.Context, client apiClient) ([]*QueueStatsView, error) {
	stats, err := client.ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*QueueStatsView, len(stats))
	for i, s := range stats {
		out[i] = &QueueStatsView{
			Queue: s.Queue, Pending: s.Pending, Running: s.Running,
			Scheduled: s.Scheduled, Dead: s.Dead, Completed: s.Completed,
			Failed: s.Failed, Cancelled: s.Cancelled, Paused: s.Paused,
		}
	}
	return out, nil
}

func queuesHandler(client apiClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stats, err := listQueuesView(r.Context(), client)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, stats)
	})
}

func workersHandler(client apiClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		workers, err := client.ListWorkers(r.Context())
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}
		out := make([]WorkerView, len(workers))
		for i, w := range workers {
			out[i] = WorkerView{
				ID: w.ID, Queues: w.Queues,
				StartedAt: w.StartedAt, LastSeen: w.LastSeen,
			}
		}
		writeJSON(w, http.StatusOK, out)
	})
}

func jobsHandler(client apiClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit, offset, err := parseJobsPagination(r.URL.Query())
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, err)
			return
		}

		jobs, err := client.ListJobs(r.Context(),
			r.URL.Query().Get("queue"),
			r.URL.Query().Get("state"),
			r.URL.Query().Get("kind"),
			limit+1, offset,
		)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, fluvio.ErrInvalidJobState) {
				status = http.StatusBadRequest
			}
			writeAPIError(w, status, err)
			return
		}

		hasMore := len(jobs) > limit
		if hasMore {
			jobs = jobs[:limit]
		}
		if jobs == nil {
			jobs = []fluvio.JobRow{}
		}

		writeJSON(w, http.StatusOK, JobsPage{
			Jobs:    jobRowsToViews(jobs),
			Limit:   limit,
			Offset:  offset,
			HasMore: hasMore,
		})
	})
}

func queueActionHandler(client apiClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/fluvio/api/queues/")
		path = strings.TrimSuffix(path, "/")
		switch {
		case path == "":
			http.NotFound(w, r)
		case strings.HasSuffix(path, "/pause") && r.Method == http.MethodPost:
			queue := strings.TrimSuffix(path, "/pause")
			if err := client.PauseQueue(r.Context(), queue); err != nil {
				writeAPIError(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		case strings.HasSuffix(path, "/resume") && r.Method == http.MethodPost:
			queue := strings.TrimSuffix(path, "/resume")
			if err := client.ResumeQueue(r.Context(), queue); err != nil {
				writeAPIError(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		case r.Method == http.MethodGet:
			queueDetailHandler(client, path)(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

func queueDetailHandler(client apiClient, queue string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := client.QueueStats(r.Context(), queue)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}
		instances, capacity, err := client.QueueWorkerCapacity(r.Context(), queue)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, QueueDetailView{
			QueueStatsView: QueueStatsView{
				Queue: stats.Queue, Pending: stats.Pending, Running: stats.Running,
				Scheduled: stats.Scheduled, Dead: stats.Dead, Completed: stats.Completed,
				Failed: stats.Failed, Cancelled: stats.Cancelled, Paused: stats.Paused,
			},
			WorkerInstances: instances,
			WorkerCapacity:  capacity,
		})
	}
}

func sseHandler(client apiClient, cfg config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ctx := r.Context()
		statsTicker := time.NewTicker(5 * time.Second)
		keepaliveTicker := time.NewTicker(cfg.keepaliveInterval)
		defer statsTicker.Stop()
		defer keepaliveTicker.Stop()

		writeEvent := func() {
			stats, err := listQueuesView(ctx, client)
			if err != nil {
				return
			}
			data, _ := json.Marshal(stats)
			fmt.Fprintf(w, "event: stats\ndata: %s\n\n", data)
			flusher.Flush()
		}

		writeEvent()
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTicker.C:
				writeEvent()
			case <-keepaliveTicker.C:
				fmt.Fprintf(w, ": keepalive\n\n")
				flusher.Flush()
			}
		}
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeAPIError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": apiErrorMessage(status, err)})
}

func apiStatusForError(err error) int {
	switch {
	case errors.Is(err, fluvio.ErrJobNotFound):
		return http.StatusNotFound
	case errors.Is(err, fluvio.ErrWorkflowNotFound):
		return http.StatusNotFound
	case errors.Is(err, fluvio.ErrInvalidJobState):
		return http.StatusBadRequest
	case errors.Is(err, fluvio.ErrInvalidConfig):
		return http.StatusBadRequest
	case errors.Is(err, fluvio.ErrUniqueConflict):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func apiErrorMessage(status int, err error) string {
	if err == nil {
		return "unknown error"
	}
	switch {
	case errors.Is(err, fluvio.ErrJobNotFound):
		return "job not found"
	case errors.Is(err, fluvio.ErrWorkflowNotFound):
		return "workflow not found"
	case errors.Is(err, fluvio.ErrInvalidJobState):
		return "invalid job state"
	case errors.Is(err, fluvio.ErrInvalidConfig):
		return "invalid request"
	case errors.Is(err, fluvio.ErrUniqueConflict):
		return "job with unique key already exists"
	default:
		if status == http.StatusNotFound {
			return "not found"
		}
		return "internal server error"
	}
}

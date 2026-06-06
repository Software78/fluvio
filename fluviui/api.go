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
	PauseQueue(ctx context.Context, queue string) error
	ResumeQueue(ctx context.Context, queue string) error
	ListWorkers(ctx context.Context) ([]fluvio.WorkerInstance, error)
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
	Jobs    []fluvio.JobRow `json:"jobs"`
	Limit   int             `json:"limit"`
	Offset  int             `json:"offset"`
	HasMore bool            `json:"has_more"`
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
			Failed: s.Failed, Paused: s.Paused,
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
			Jobs:    jobs,
			Limit:   limit,
			Offset:  offset,
			HasMore: hasMore,
		})
	})
}

func jobDetailHandler(client apiClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idStr := strings.TrimPrefix(r.URL.Path, "/fluvio/api/jobs/")
		var id int64
		if _, err := fmt.Sscanf(idStr, "%d", &id); err != nil {
			http.NotFound(w, r)
			return
		}
		job, err := client.GetJob(r.Context(), id)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, fluvio.ErrJobNotFound) {
				status = http.StatusNotFound
			}
			writeAPIError(w, status, err)
			return
		}
		writeJSON(w, http.StatusOK, job)
	})
}

func queueActionHandler(client apiClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/fluvio/api/queues/")
		switch {
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
		default:
			http.NotFound(w, r)
		}
	})
}

func sseHandler(client apiClient) http.Handler {
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
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

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
			case <-ticker.C:
				writeEvent()
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

func apiErrorMessage(status int, err error) string {
	if err == nil {
		return "unknown error"
	}
	switch {
	case errors.Is(err, fluvio.ErrJobNotFound):
		return "job not found"
	case errors.Is(err, fluvio.ErrInvalidJobState):
		return "invalid job state"
	case errors.Is(err, fluvio.ErrInvalidConfig):
		return "invalid request"
	default:
		if status == http.StatusNotFound {
			return "not found"
		}
		return "internal server error"
	}
}

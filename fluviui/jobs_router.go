package fluviui

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/software78/fluvio"
)

func jobsRouter(client apiClient) http.Handler {
	listHandler := jobsHandler(client)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fluvio/api/jobs" {
			switch r.Method {
			case http.MethodGet:
				listHandler.ServeHTTP(w, r)
			case http.MethodPost:
				enqueueJobHandler(client)(w, r)
			default:
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			}
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/fluvio/api/jobs/")
		path = strings.Trim(path, "/")
		if path == "" {
			http.NotFound(w, r)
			return
		}

		switch {
		case strings.HasSuffix(path, "/cancel") && r.Method == http.MethodPost:
			idStr := strings.TrimSuffix(path, "/cancel")
			jobCancelHandler(client, idStr)(w, r)
		case strings.HasSuffix(path, "/retry") && r.Method == http.MethodPost:
			idStr := strings.TrimSuffix(path, "/retry")
			jobRetryHandler(client, idStr)(w, r)
		case r.Method == http.MethodGet:
			id, err := strconv.ParseInt(path, 10, 64)
			if err != nil || id <= 0 {
				http.NotFound(w, r)
				return
			}
			jobDetailHandler(client, id)(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

func jobDetailHandler(client apiClient, id int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		job, err := client.GetJob(r.Context(), id)
		if err != nil {
			writeAPIError(w, apiStatusForError(err), err)
			return
		}
		writeJSON(w, http.StatusOK, jobRowToView(*job))
	}
}

func jobCancelHandler(client apiClient, idStr string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil || id <= 0 {
			http.NotFound(w, r)
			return
		}
		if err := client.Cancel(r.Context(), id); err != nil {
			writeAPIError(w, apiStatusForError(err), err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}
}

func jobRetryHandler(client apiClient, idStr string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil || id <= 0 {
			http.NotFound(w, r)
			return
		}
		if err := client.RunJobNow(r.Context(), id); err != nil {
			writeAPIError(w, apiStatusForError(err), err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}
}

type enqueueJobRequest struct {
	Kind        string          `json:"kind"`
	Queue       string          `json:"queue"`
	Args        json.RawMessage `json:"args"`
	Priority    int16           `json:"priority"`
	MaxAttempts int16           `json:"max_attempts"`
	ScheduledAt *time.Time      `json:"scheduled_at"`
	Tags        []string        `json:"tags"`
	UniqueKey   *string         `json:"unique_key"`
	Metadata    json.RawMessage `json:"metadata"`
	Encrypted   bool            `json:"encrypted"`
}

func enqueueJobHandler(client apiClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req enqueueJobRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, fluvio.ErrInvalidConfig)
			return
		}
		if req.Kind == "" {
			writeAPIError(w, http.StatusBadRequest, fluvio.ErrInvalidConfig)
			return
		}

		job, err := client.EnqueueRaw(r.Context(), fluvio.EnqueueRawParams{
			Kind: req.Kind, Queue: req.Queue, Args: req.Args,
			Priority: req.Priority, MaxAttempts: req.MaxAttempts,
			ScheduledAt: req.ScheduledAt, Tags: req.Tags,
			UniqueKey: req.UniqueKey, Metadata: req.Metadata,
			Encrypted: req.Encrypted,
		})
		if err != nil {
			writeAPIError(w, apiStatusForError(err), err)
			return
		}
		writeJSON(w, http.StatusCreated, jobRowToView(*job))
	}
}

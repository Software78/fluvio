package fluviui

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/software78/fluvio"
)

func periodicHandler(client apiClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/fluvio/api/periodic")
		path = strings.Trim(path, "/")

		switch {
		case path == "" && r.Method == http.MethodGet:
			periodicListHandler(client)(w, r)
		case path == "" && r.Method == http.MethodPost:
			periodicAddHandler(client)(w, r)
		case strings.HasSuffix(path, "/pause") && r.Method == http.MethodPost:
			kind := strings.TrimSuffix(path, "/pause")
			periodicPauseHandler(client, kind)(w, r)
		case strings.HasSuffix(path, "/resume") && r.Method == http.MethodPost:
			kind := strings.TrimSuffix(path, "/resume")
			periodicResumeHandler(client, kind)(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

func periodicListHandler(client apiClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jobs, err := client.ListPeriodicJobs(r.Context())
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}
		out := make([]PeriodicJobView, len(jobs))
		for i, j := range jobs {
			out[i] = periodicJobToView(j)
		}
		writeJSON(w, http.StatusOK, out)
	}
}

type addPeriodicRequest struct {
	Cron        string          `json:"cron"`
	Kind        string          `json:"kind"`
	Queue       string          `json:"queue"`
	Args        json.RawMessage `json:"args"`
	MaxAttempts int16           `json:"max_attempts"`
}

func periodicAddHandler(client apiClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req addPeriodicRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, fluvio.ErrInvalidConfig)
			return
		}
		if req.Cron == "" || req.Kind == "" {
			writeAPIError(w, http.StatusBadRequest, fluvio.ErrInvalidConfig)
			return
		}

		args := []byte(req.Args)
		if err := client.AddPeriodicJobRaw(r.Context(), req.Cron, req.Kind, req.Queue, args, req.MaxAttempts); err != nil {
			writeAPIError(w, apiStatusForError(err), err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]bool{"ok": true})
	}
}

func periodicPauseHandler(client apiClient, kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if kind == "" {
			http.NotFound(w, r)
			return
		}
		if err := client.PausePeriodicJob(r.Context(), kind); err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}
}

func periodicResumeHandler(client apiClient, kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if kind == "" {
			http.NotFound(w, r)
			return
		}
		if err := client.ResumePeriodicJob(r.Context(), kind); err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}
}

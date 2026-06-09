package fluviui

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/software78/fluvio"
)

func deadHandler(client apiClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/fluvio/api/dead")
		path = strings.Trim(path, "/")

		switch {
		case path == "" && r.Method == http.MethodGet:
			deadListHandler(client)(w, r)
		case path == "replay" && r.Method == http.MethodPost:
			deadBulkReplayHandler(client)(w, r)
		case path == "purge" && r.Method == http.MethodPost:
			deadPurgeHandler(client)(w, r)
		case strings.HasSuffix(path, "/replay") && r.Method == http.MethodPost:
			idStr := strings.TrimSuffix(path, "/replay")
			deadReplayHandler(client, idStr)(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

func deadListHandler(client apiClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit, offset, err := parseJobsPagination(r.URL.Query())
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, err)
			return
		}

		jobs, err := client.ListDeadJobs(r.Context(), limit+1, offset)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
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
	}
}

func deadReplayHandler(client apiClient, idStr string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil || id <= 0 {
			http.NotFound(w, r)
			return
		}
		if err := client.ReplayDeadJob(r.Context(), id); err != nil {
			writeAPIError(w, apiStatusForError(err), err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}
}

type bulkReplayRequest struct {
	IDs []int64 `json:"ids"`
}

type bulkReplayError struct {
	ID    int64  `json:"id"`
	Error string `json:"error"`
}

func deadBulkReplayHandler(client apiClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req bulkReplayRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, fluvio.ErrInvalidConfig)
			return
		}
		if len(req.IDs) == 0 {
			writeAPIError(w, http.StatusBadRequest, fluvio.ErrInvalidConfig)
			return
		}

		var replayed int
		var errs []bulkReplayError
		for _, id := range req.IDs {
			if err := client.ReplayDeadJob(r.Context(), id); err != nil {
				errs = append(errs, bulkReplayError{ID: id, Error: apiErrorMessage(apiStatusForError(err), err)})
				continue
			}
			replayed++
		}

		resp := map[string]any{"replayed": replayed}
		if len(errs) > 0 {
			resp["errors"] = errs
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

type purgeDeadRequest struct {
	Before time.Time `json:"before"`
}

func deadPurgeHandler(client apiClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req purgeDeadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, fluvio.ErrInvalidConfig)
			return
		}
		if req.Before.IsZero() {
			writeAPIError(w, http.StatusBadRequest, fluvio.ErrInvalidConfig)
			return
		}

		n, err := client.PurgeDeadJobs(r.Context(), req.Before)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]int64{"purged": n})
	}
}

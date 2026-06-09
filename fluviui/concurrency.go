package fluviui

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/software78/fluvio"
)

func concurrencyHandler(client apiClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/fluvio/api/concurrency")
		path = strings.Trim(path, "/")

		switch {
		case path == "" && r.Method == http.MethodGet:
			concurrencyListHandler(client)(w, r)
		case path != "" && r.Method == http.MethodPut:
			concurrencySetHandler(client, path)(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

func concurrencyListHandler(client apiClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slots, err := client.ListConcurrencySlots(r.Context())
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}
		out := make([]ConcurrencySlotView, len(slots))
		for i, s := range slots {
			out[i] = concurrencySlotToView(s)
		}
		writeJSON(w, http.StatusOK, out)
	}
}

type setConcurrencyRequest struct {
	MaxConcurrent int `json:"max_concurrent"`
}

func concurrencySetHandler(client apiClient, kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if kind == "" {
			http.NotFound(w, r)
			return
		}
		var req setConcurrencyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, fluvio.ErrInvalidConfig)
			return
		}
		if req.MaxConcurrent < 1 {
			writeAPIError(w, http.StatusBadRequest, fluvio.ErrInvalidConfig)
			return
		}
		if err := client.SetConcurrencyLimit(r.Context(), fluvio.ConcurrencyLimitConfig{
			Kind: kind, MaxConcurrent: req.MaxConcurrent,
		}); err != nil {
			writeAPIError(w, apiStatusForError(err), err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}
}

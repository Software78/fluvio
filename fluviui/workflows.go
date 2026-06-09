package fluviui

import (
	"net/http"
	"strings"
)

type WorkflowsPage struct {
	Workflows []WorkflowView `json:"workflows"`
	Limit     int            `json:"limit"`
	Offset    int            `json:"offset"`
	HasMore   bool           `json:"has_more"`
}

func workflowsHandler(client apiClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/fluvio/api/workflows")
		path = strings.Trim(path, "/")

		switch {
		case path == "" && r.Method == http.MethodGet:
			workflowsListHandler(client)(w, r)
		case path != "" && r.Method == http.MethodGet:
			workflowDetailHandler(client, path)(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

func workflowsListHandler(client apiClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit, offset, err := parseJobsPagination(r.URL.Query())
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, err)
			return
		}

		workflows, err := client.ListWorkflows(r.Context(), limit+1, offset)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, err)
			return
		}

		hasMore := len(workflows) > limit
		if hasMore {
			workflows = workflows[:limit]
		}

		views := make([]WorkflowView, len(workflows))
		for i, wf := range workflows {
			views[i] = workflowToView(wf)
		}

		writeJSON(w, http.StatusOK, WorkflowsPage{
			Workflows: views,
			Limit:     limit,
			Offset:    offset,
			HasMore:   hasMore,
		})
	}
}

func workflowDetailHandler(client apiClient, id string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		wf, err := client.GetWorkflow(r.Context(), id)
		if err != nil {
			writeAPIError(w, apiStatusForError(err), err)
			return
		}
		writeJSON(w, http.StatusOK, workflowToView(wf))
	}
}

package fluviui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

	"github.com/software78/fluvio"
)

var pageTemplate = template.Must(template.New("page").Parse(layoutHTML))

// Inspector is the subset of Client methods used by the UI.
type Inspector interface {
	ListQueues(ctx context.Context) ([]*QueueStatsView, error)
	ListJobs(ctx context.Context, queue, state, kind string, limit, offset int) ([]fluvio.JobRow, error)
	GetJob(ctx context.Context, id int64) (*fluvio.JobRow, error)
	PauseQueue(ctx context.Context, queue string) error
	ResumeQueue(ctx context.Context, queue string) error
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

// ClientAdapter wraps fluvio.Client for the UI.
type ClientAdapter struct {
	Client *fluvio.Client
}

func (a *ClientAdapter) ListQueues(ctx context.Context) ([]*QueueStatsView, error) {
	stats, err := a.Client.ListQueues(ctx)
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

func (a *ClientAdapter) ListJobs(ctx context.Context, queue, state, kind string, limit, offset int) ([]fluvio.JobRow, error) {
	return a.Client.ListJobs(ctx, queue, state, kind, limit, offset)
}

func (a *ClientAdapter) GetJob(ctx context.Context, id int64) (*fluvio.JobRow, error) {
	return a.Client.GetJob(ctx, id)
}

func (a *ClientAdapter) PauseQueue(ctx context.Context, queue string) error {
	return a.Client.PauseQueue(ctx, queue)
}

func (a *ClientAdapter) ResumeQueue(ctx context.Context, queue string) error {
	return a.Client.ResumeQueue(ctx, queue)
}

// Handler mounts the Fluvio web UI and API under prefix (default /fluvio/).
func Handler(inspector Inspector, prefix string) http.Handler {
	if prefix == "" {
		prefix = "/fluvio/"
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	mux := http.NewServeMux()
	ui := &server{inspector: inspector}
	mux.Handle(prefix, http.StripPrefix(strings.TrimSuffix(prefix, "/"), ui))
	return mux
}

type server struct {
	inspector Inspector
}

type pageData struct {
	Title string
	Page  string
	Queue string
	State string
	Kind  string
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path == "" {
		path = "/"
	}
	if path == "/" {
		switch r.URL.Query().Get("page") {
		case "jobs":
			s.renderPage(w, pageData{Title: "Jobs", Page: "jobs", Queue: r.URL.Query().Get("queue"), State: r.URL.Query().Get("state"), Kind: r.URL.Query().Get("kind")})
		case "queues":
			s.renderPage(w, pageData{Title: "Queues", Page: "queues"})
		default:
			s.renderPage(w, pageData{Title: "Dashboard", Page: "dashboard"})
		}
		return
	}
	switch {
	case path == "/api/queues":
		s.apiQueues(w, r)
	case path == "/api/jobs":
		s.apiJobs(w, r)
	case strings.HasPrefix(path, "/api/jobs/"):
		id := strings.TrimPrefix(path, "/api/jobs/")
		s.apiJob(w, r, id)
	case strings.HasSuffix(path, "/pause") && r.Method == http.MethodPost:
		queue := strings.TrimSuffix(strings.TrimPrefix(path, "/api/queues/"), "/pause")
		s.apiPause(w, r, queue, true)
	case strings.HasSuffix(path, "/resume") && r.Method == http.MethodPost:
		queue := strings.TrimSuffix(strings.TrimPrefix(path, "/api/queues/"), "/resume")
		s.apiPause(w, r, queue, false)
	case path == "/api/events":
		s.apiEvents(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *server) renderPage(w http.ResponseWriter, data pageData) {
	var content string
	switch data.Page {
	case "jobs":
		content = jobsContent
	case "queues":
		content = queuesContent
	default:
		content = dashboardContent
	}
	tpl := pageTemplate
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, struct {
		pageData
		Content template.HTML
	}{data, template.HTML(content)}); err != nil {
		http.Error(w, "failed to render page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

func (s *server) apiQueues(w http.ResponseWriter, r *http.Request) {
	stats, err := s.inspector.ListQueues(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *server) apiJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.inspector.ListJobs(r.Context(),
		r.URL.Query().Get("queue"),
		r.URL.Query().Get("state"),
		r.URL.Query().Get("kind"),
		50, 0,
	)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *server) apiJob(w http.ResponseWriter, r *http.Request, idStr string) {
	var id int64
	if _, err := fmt.Sscanf(idStr, "%d", &id); err != nil {
		http.NotFound(w, r)
		return
	}
	job, err := s.inspector.GetJob(r.Context(), id)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, fluvio.ErrJobNotFound) {
			status = http.StatusNotFound
		}
		writeAPIError(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *server) apiPause(w http.ResponseWriter, r *http.Request, queue string, pause bool) {
	var err error
	if pause {
		err = s.inspector.PauseQueue(r.Context(), queue)
	} else {
		err = s.inspector.ResumeQueue(r.Context(), queue)
	}
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) apiEvents(w http.ResponseWriter, r *http.Request) {
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
		stats, err := s.inspector.ListQueues(ctx)
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

const layoutHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{{.Title}} · Fluvio</title>
  <script src="https://unpkg.com/htmx.org@2.0.4"></script>
  <style>
    body { font-family: system-ui, sans-serif; margin: 0; background: #0f172a; color: #e2e8f0; }
    nav { background: #1e293b; padding: 1rem 2rem; display: flex; gap: 1.5rem; }
    nav a { color: #94a3b8; text-decoration: none; }
    nav a.active { color: #38bdf8; font-weight: 600; }
    main { padding: 2rem; max-width: 1100px; margin: 0 auto; }
    table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
    th, td { text-align: left; padding: 0.5rem 0.75rem; border-bottom: 1px solid #334155; }
    th { color: #94a3b8; font-size: 0.85rem; }
    .card { background: #1e293b; border-radius: 8px; padding: 1rem 1.25rem; margin-bottom: 1rem; }
    .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 1rem; }
    .stat { background: #334155; border-radius: 6px; padding: 1rem; }
    .stat label { display: block; font-size: 0.75rem; color: #94a3b8; }
    .stat span { font-size: 1.5rem; font-weight: 700; }
    button { background: #2563eb; color: white; border: none; padding: 0.35rem 0.75rem; border-radius: 4px; cursor: pointer; }
    button.secondary { background: #475569; }
    input, select { background: #334155; border: 1px solid #475569; color: #e2e8f0; padding: 0.35rem 0.5rem; border-radius: 4px; margin-right: 0.5rem; }
  </style>
</head>
<body>
  <nav>
    <strong style="color:#38bdf8">Fluvio</strong>
    <a href="./?page=dashboard" class="{{if eq .Page "dashboard"}}active{{end}}">Dashboard</a>
    <a href="./?page=jobs" class="{{if eq .Page "jobs"}}active{{end}}">Jobs</a>
    <a href="./?page=queues" class="{{if eq .Page "queues"}}active{{end}}">Queues</a>
  </nav>
  <main><h1>{{.Title}}</h1>{{.Content}}</main>
</body>
</html>`

const dashboardContent = `
<div id="stats" hx-get="api/queues" hx-trigger="load, every 5s" hx-swap="innerHTML"><p>Loading stats…</p></div>
<script>
document.body.addEventListener('htmx:afterOnLoad', function(evt) {
  if (evt.detail.target.id !== 'stats') return;
  try {
    var stats = JSON.parse(evt.detail.xhr.responseText);
    var html = '<div class="stat-grid">';
    stats.forEach(function(s) {
      html += '<div class="stat"><label>' + s.queue + (s.paused ? ' (paused)' : '') + '</label>';
      html += '<span>' + s.pending + ' pending</span></div>';
    });
    html += '</div>';
    evt.detail.target.innerHTML = html || '<p>No queues yet.</p>';
  } catch(e) {}
});
</script>`

const jobsContent = `
<form hx-get="api/jobs" hx-target="#job-list" hx-trigger="submit, load">
  <label>Queue <input name="queue"></label>
  <label>State <select name="state"><option value="">all</option><option>pending</option><option>running</option><option>completed</option><option>scheduled</option><option>dead</option></select></label>
  <label>Kind <input name="kind"></label>
  <button type="submit">Filter</button>
</form>
<div id="job-list" class="card"><p>Loading…</p></div>
<script>
document.body.addEventListener('htmx:afterOnLoad', function(evt) {
  if (evt.detail.target.id !== 'job-list') return;
  try {
    var jobs = JSON.parse(evt.detail.xhr.responseText);
    if (!jobs.length) { evt.detail.target.innerHTML = '<p>No jobs found.</p>'; return; }
    var html = '<table><tr><th>ID</th><th>Queue</th><th>Kind</th><th>State</th><th>Attempt</th></tr>';
    jobs.forEach(function(j) {
      html += '<tr><td>' + j.ID + '</td><td>' + j.Queue + '</td><td>' + j.Kind + '</td><td>' + j.State + '</td><td>' + j.Attempt + '/' + j.MaxAttempts + '</td></tr>';
    });
    html += '</table>';
    evt.detail.target.innerHTML = html;
  } catch(e) {}
});
</script>`

const queuesContent = `
<div id="queue-list" hx-get="api/queues" hx-trigger="load, every 5s"><p>Loading…</p></div>
<script>
document.body.addEventListener('htmx:afterOnLoad', function(evt) {
  if (evt.detail.target.id !== 'queue-list') return;
  try {
    var stats = JSON.parse(evt.detail.xhr.responseText);
    var html = '<table><tr><th>Queue</th><th>Pending</th><th>Running</th><th>Scheduled</th><th>Dead</th><th>Actions</th></tr>';
    stats.forEach(function(s) {
      html += '<tr><td>' + s.queue + '</td><td>' + s.pending + '</td><td>' + s.running + '</td><td>' + s.scheduled + '</td><td>' + s.dead + '</td><td>';
      if (s.paused) {
        html += '<button hx-post="api/queues/' + s.queue + '/resume" hx-swap="none">Resume</button>';
      } else {
        html += '<button class="secondary" hx-post="api/queues/' + s.queue + '/pause" hx-swap="none">Pause</button>';
      }
      html += '</td></tr>';
    });
    html += '</table>';
    evt.detail.target.innerHTML = html || '<p>No queues yet.</p>';
  } catch(e) {}
});
</script>`

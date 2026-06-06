package fluviui

import (
	"net/http"

	"github.com/software78/fluvio"
)

type config struct {
	allowedOrigin string
}

type Option func(*config)

// WithAllowedOrigin sets the value of the Access-Control-Allow-Origin header.
// Defaults to "*" if not set.
func WithAllowedOrigin(origin string) Option {
	return func(c *config) {
		c.allowedOrigin = origin
	}
}

func defaultConfig() config {
	return config{allowedOrigin: "*"}
}

func corsMiddleware(cfg config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", cfg.allowedOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Handler mounts the Fluvio REST API and SSE stream.
func Handler(client *fluvio.Client, opts ...Option) http.Handler {
	return handlerFor(client, opts...)
}

func handlerFor(client apiClient, opts ...Option) http.Handler {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}

	mux := http.NewServeMux()
	cm := corsMiddleware(cfg)

	mux.Handle("/fluvio/api/events", cm(sseHandler(client)))
	mux.Handle("/fluvio/api/workers", cm(workersHandler(client)))
	mux.Handle("/fluvio/api/jobs", cm(jobsHandler(client)))
	mux.Handle("/fluvio/api/jobs/", cm(jobDetailHandler(client)))
	mux.Handle("/fluvio/api/queues", cm(queuesHandler(client)))
	mux.Handle("/fluvio/api/queues/", cm(queueActionHandler(client)))

	return mux
}

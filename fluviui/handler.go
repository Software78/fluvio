// Package fluviui provides an HTTP API for queue monitoring and control.
// Use WithMiddleware to add authentication (for example basic auth or bearer tokens).
package fluviui

import (
	"net/http"
	"time"

	"github.com/software78/fluvio"
)

type config struct {
	allowedOrigin     string
	keepaliveInterval time.Duration
	middleware        func(http.Handler) http.Handler
}

type Option func(*config)

// WithAllowedOrigin sets the value of the Access-Control-Allow-Origin header.
// Defaults to "*" if not set.
func WithAllowedOrigin(origin string) Option {
	return func(c *config) {
		c.allowedOrigin = origin
	}
}

// WithMiddleware wraps all API handlers (e.g. for authentication).
func WithMiddleware(mw func(http.Handler) http.Handler) Option {
	return func(c *config) {
		c.middleware = mw
	}
}

// WithKeepaliveInterval sets the SSE keepalive ticker interval.
// Defaults to 15s if not set.
func WithKeepaliveInterval(d time.Duration) Option {
	return func(c *config) {
		c.keepaliveInterval = d
	}
}

func defaultConfig() config {
	return config{
		allowedOrigin:     "*",
		keepaliveInterval: 15 * time.Second,
	}
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
	wrap := func(h http.Handler) http.Handler {
		h = cm(h)
		if cfg.middleware != nil {
			h = cfg.middleware(h)
		}
		return h
	}

	mux.Handle("/fluvio/api/events", wrap(sseHandler(client, cfg)))
	mux.Handle("/fluvio/api/workers", wrap(workersHandler(client)))
	mux.Handle("/fluvio/api/jobs", wrap(jobsHandler(client)))
	mux.Handle("/fluvio/api/jobs/", wrap(jobDetailHandler(client)))
	mux.Handle("/fluvio/api/queues", wrap(queuesHandler(client)))
	mux.Handle("/fluvio/api/queues/", wrap(queueActionHandler(client)))

	return mux
}

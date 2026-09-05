// Package fluviui provides an HTTP API for queue monitoring and control.
// Use WithMiddleware to add authentication (for example basic auth or bearer tokens).
package fluviui

import (
	"net/http"
	"strings"
	"time"

	"github.com/software78/fluvio"
)

type config struct {
	// allowedOrigins is nil/empty for the wide-open default ("*"). A single
	// literal "*" entry also means "*". Otherwise each entry is either an
	// exact origin ("https://ui.example.com") or a single-level wildcard
	// subdomain pattern ("https://*.sandbox.example.com").
	allowedOrigins    []string
	allowedHeaders    []string
	keepaliveInterval time.Duration
	middleware        func(http.Handler) http.Handler
}

type Option func(*config)

// WithAllowedOrigin restricts cross-origin requests to a single origin.
//
// Deprecated: use WithAllowedOrigins, which supports multiple origins and
// wildcard subdomain patterns (e.g. "https://*.sandbox.example.com") — useful
// when the same fluviui deployment is reached from several environments
// (local dev, per-branch previews, staging) without needing a redeploy per
// origin.
func WithAllowedOrigin(origin string) Option {
	return WithAllowedOrigins(origin)
}

// WithAllowedOrigins restricts cross-origin requests to the given allowlist.
// Each entry may be:
//
//   - an exact origin, e.g. "https://ui.example.com"
//   - a single-level wildcard subdomain pattern, e.g.
//     "https://*.sandbox.example.com" (matches "https://pr-42.sandbox.example.com"
//     but not "https://sandbox.example.com" or nested subdomains)
//   - "*" to allow any origin (the default when this option is never set)
//
// Unlike a single fixed Access-Control-Allow-Origin string, the incoming
// request's Origin header is checked against this allowlist and, on a match,
// that exact origin is reflected back (with `Vary: Origin`). This lets one
// server safely serve several UI origins at once, and means a misconfigured
// or unexpected origin gets no CORS headers at all instead of a header that
// silently doesn't match what the browser sent.
func WithAllowedOrigins(origins ...string) Option {
	return func(c *config) {
		c.allowedOrigins = origins
	}
}

// WithAllowedHeaders overrides the request headers permitted by
// Access-Control-Allow-Headers. Defaults to "Content-Type, Authorization" --
// the Authorization default matters whenever WithMiddleware enforces Basic
// or Bearer auth on a cross-origin deployment: without it in the allowlist,
// browsers refuse to send the Authorization header at all, and the preflight
// for every authenticated endpoint fails before the real request is sent.
func WithAllowedHeaders(headers ...string) Option {
	return func(c *config) {
		c.allowedHeaders = headers
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
		allowedHeaders:    []string{"Content-Type", "Authorization"},
		keepaliveInterval: 15 * time.Second,
	}
}

// resolveOrigin decides what, if anything, to put in Access-Control-Allow-Origin
// for the given request. It returns the value to send and whether any CORS
// header should be sent at all.
func resolveOrigin(allowedOrigins []string, requestOrigin string) (value string, send bool) {
	if len(allowedOrigins) == 0 {
		return "*", true
	}
	for _, pattern := range allowedOrigins {
		if pattern == "*" {
			return "*", true
		}
		if pattern == requestOrigin {
			return requestOrigin, true
		}
		if requestOrigin != "" && originMatchesWildcard(pattern, requestOrigin) {
			return requestOrigin, true
		}
	}
	return "", false
}

// originMatchesWildcard reports whether origin matches a pattern of the form
// "<scheme>://*.<suffix>" against exactly one subdomain level, e.g. pattern
// "https://*.sandbox.example.com" matches origin "https://pr-42.sandbox.example.com".
func originMatchesWildcard(pattern, origin string) bool {
	scheme, patternHost, ok := strings.Cut(pattern, "://")
	if !ok || !strings.HasPrefix(patternHost, "*.") {
		return false
	}
	suffix := strings.TrimPrefix(patternHost, "*")
	originScheme, originHost, ok := strings.Cut(origin, "://")
	if !ok || originScheme != scheme {
		return false
	}
	if !strings.HasSuffix(originHost, suffix) {
		return false
	}
	sub := strings.TrimSuffix(originHost, suffix)
	return sub != "" && !strings.Contains(sub, ".")
}

func corsMiddleware(cfg config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if value, send := resolveOrigin(cfg.allowedOrigins, r.Header.Get("Origin")); send {
				w.Header().Set("Access-Control-Allow-Origin", value)
				if value != "*" {
					w.Header().Add("Vary", "Origin")
				}
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", strings.Join(cfg.allowedHeaders, ", "))
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
		// CORS headers must be set before auth runs, not after: if
		// cfg.middleware (e.g. Basic Auth) is the outer wrapper, an
		// unauthenticated request's 401 response never reaches corsMiddleware
		// at all, so it comes back with no Access-Control-Allow-Origin and
		// the browser reports an opaque CORS failure instead of a readable
		// 401. Wrapping CORS outermost means it always runs first and its
		// headers survive regardless of what the inner middleware/handler
		// decides to do with the response.
		if cfg.middleware != nil {
			h = cfg.middleware(h)
		}
		h = cm(h)
		return h
	}

	mux.Handle("/fluvio/api/events", wrap(sseHandler(client, cfg)))
	mux.Handle("/fluvio/api/workers", wrap(workersHandler(client)))
	mux.Handle("/fluvio/api/dead/", wrap(deadHandler(client)))
	mux.Handle("/fluvio/api/dead", wrap(deadHandler(client)))
	mux.Handle("/fluvio/api/jobs/", wrap(jobsRouter(client)))
	mux.Handle("/fluvio/api/jobs", wrap(jobsRouter(client)))
	mux.Handle("/fluvio/api/periodic/", wrap(periodicHandler(client)))
	mux.Handle("/fluvio/api/periodic", wrap(periodicHandler(client)))
	mux.Handle("/fluvio/api/workflows/", wrap(workflowsHandler(client)))
	mux.Handle("/fluvio/api/workflows", wrap(workflowsHandler(client)))
	mux.Handle("/fluvio/api/concurrency/", wrap(concurrencyHandler(client)))
	mux.Handle("/fluvio/api/concurrency", wrap(concurrencyHandler(client)))
	mux.Handle("/fluvio/api/queues", wrap(queuesHandler(client)))
	mux.Handle("/fluvio/api/queues/", wrap(queueActionHandler(client)))

	return mux
}

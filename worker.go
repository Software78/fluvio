package fluvio

import (
	"context"
	"encoding/json"
	"math"
	"sync"
	"time"
)

// Worker processes jobs of a specific argument type.
type Worker[T JobArgs] interface {
	Work(ctx context.Context, job *Job[T]) error
}

// WorkerDefaults provides default Worker behavior.
type WorkerDefaults[T JobArgs] struct{}

func (WorkerDefaults[T]) Timeout() time.Duration { return 0 }

// NextAttempt returns the delay before the next retry attempt.
// Attempt is the number of attempts made including the current one
// (incremented by Fetch before the worker runs).
func (WorkerDefaults[T]) NextAttempt(job *Job[T], _ error) time.Duration {
	return DefaultRetryDelayForJob(job, 24*time.Hour)
}

type timeoutWorker[T JobArgs] interface {
	Timeout() time.Duration
}

type nextAttemptWorker[T JobArgs] interface {
	NextAttempt(job *Job[T], err error) time.Duration
}

func workerTimeout[T JobArgs](w Worker[T], defaultTimeout time.Duration) time.Duration {
	if tw, ok := w.(timeoutWorker[T]); ok {
		if d := tw.Timeout(); d > 0 {
			return d
		}
	}
	return defaultTimeout
}

func workerNextAttempt[T JobArgs](w Worker[T], job *Job[T], err error, maxDelay time.Duration) time.Duration {
	var d time.Duration
	if na, ok := w.(nextAttemptWorker[T]); ok {
		d = na.NextAttempt(job, err)
	} else {
		d = WorkerDefaults[T]{}.NextAttempt(job, err)
	}
	if d > maxDelay {
		d = maxDelay
	}
	return d
}

// DefaultRetryDelay returns the built-in exponential backoff for a given attempt count.
// attempt should be the value of job.Attempt, which is already incremented by Fetch
// before the worker runs — so attempt=1 on the first failure, attempt=2 on the second, etc.
// The delay sequence is approximately: 4s, 16s, 64s, 256s, capped at maxDelay.
func DefaultRetryDelay(attempt int16, maxDelay time.Duration) time.Duration {
	if maxDelay <= 0 {
		maxDelay = 24 * time.Hour
	}
	base := time.Duration(math.Pow(4, float64(attempt))) * time.Second
	if base > 24*time.Hour {
		base = 24 * time.Hour
	}
	if base > maxDelay {
		base = maxDelay
	}
	if base < time.Second {
		base = time.Second
	}
	return base
}

// DefaultRetryDelayForJob returns the default exponential backoff for the given job.
// Use this inside a custom NextAttempt implementation to fall back to the default schedule.
func DefaultRetryDelayForJob[T JobArgs](job *Job[T], maxDelay time.Duration) time.Duration {
	return DefaultRetryDelay(job.Attempt, maxDelay)
}

type Workers struct {
	mu      sync.RWMutex
	started bool
	byKind  map[string]anyWorker
}

type anyWorker interface {
	kind() string
	work(ctx context.Context, dJob *driverJobWrapper) error
	timeout(defaultTimeout time.Duration) time.Duration
	nextAttempt(dJob *driverJobWrapper, err error, maxDelay time.Duration) (time.Duration, error)
}

type driverJobWrapper struct {
	id          int64
	queue       string
	kind        string
	args        []byte
	attempt     int16
	maxAttempts int16
	attemptedBy []string
	workerID    string
	maxWorkers  int
	logBuf      []JobLogEntry
}

type typedWorker[T JobArgs] struct {
	kindName string
	worker   Worker[T]
}

func (tw *typedWorker[T]) kind() string { return tw.kindName }

func (tw *typedWorker[T]) work(ctx context.Context, d *driverJobWrapper) error {
	var args T
	if err := json.Unmarshal(d.args, &args); err != nil {
		return err
	}
	job := &Job[T]{
		ID:          d.id,
		Queue:       d.queue,
		Kind:        d.kind,
		Args:        args,
		Attempt:     d.attempt,
		MaxAttempts: d.maxAttempts,
		AttemptedBy: d.attemptedBy,
		WorkerID:    d.workerID,
		MaxWorkers:  d.maxWorkers,
		logBuf:      &d.logBuf,
	}
	return tw.worker.Work(ctx, job)
}

func (tw *typedWorker[T]) timeout(defaultTimeout time.Duration) time.Duration {
	return workerTimeout(tw.worker, defaultTimeout)
}

func (tw *typedWorker[T]) nextAttempt(d *driverJobWrapper, err error, maxDelay time.Duration) (time.Duration, error) {
	var args T
	if err := json.Unmarshal(d.args, &args); err != nil {
		return 0, err
	}
	job := &Job[T]{
		ID:          d.id,
		Attempt:     d.attempt,
		MaxAttempts: d.maxAttempts,
		Args:        args,
	}
	return workerNextAttempt(tw.worker, job, err, maxDelay), nil
}

func NewWorkers() *Workers {
	return &Workers{byKind: make(map[string]anyWorker)}
}

// AddWorker registers a worker for a job kind. Call before NewClient/Start;
// concurrent AddWorker after Start is not supported.
func AddWorker[T JobArgs](w *Workers, worker Worker[T]) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		panic("fluvio: AddWorker after Client.Start is not supported")
	}
	var zero T
	kind := zero.Kind()
	w.byKind[kind] = &typedWorker[T]{kindName: kind, worker: worker}
}

func (w *Workers) markStarted() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.started = true
}

func (w *Workers) get(kind string) (anyWorker, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	aw, ok := w.byKind[kind]
	return aw, ok
}

func (w *Workers) kinds() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	kinds := make([]string, 0, len(w.byKind))
	for k := range w.byKind {
		kinds = append(kinds, k)
	}
	return kinds
}

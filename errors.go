package fluvio

import "errors"

var (
	ErrJobNotFound     = errors.New("fluvio: job not found")
	ErrUniqueConflict  = errors.New("fluvio: job with unique key already exists")
	ErrQueuePaused     = errors.New("fluvio: queue is paused")
	ErrWorkerNotFound  = errors.New("fluvio: no worker registered for job kind")
	ErrClientStopped   = errors.New("fluvio: client is stopped")
	ErrClientRunning   = errors.New("fluvio: client is already running")
	ErrInvalidConfig   = errors.New("fluvio: invalid config")
)

package driver

import "errors"

// ErrQueuesPaused is returned by Fetch when every requested queue is paused.
var ErrQueuesPaused = errors.New("fluvio: all requested queues are paused")

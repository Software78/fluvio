package fluvio

import (
	"context"
	"time"
)

// JobWakeSubscription receives job availability signals from a LISTEN/NOTIFY-enabled driver.
type JobWakeSubscription interface {
	Wake() <-chan struct{}
	Close() error
}

// JobSubscriber is implemented by drivers that support PostgreSQL LISTEN/NOTIFY wakeup.
type JobSubscriber interface {
	Subscribe(ctx context.Context, queues []string) (JobWakeSubscription, error)
}

// NotifyConfigurer updates driver-side NOTIFY settings from client configuration.
type NotifyConfigurer interface {
	ConfigureNotify(pollOnly bool, debounce time.Duration)
}

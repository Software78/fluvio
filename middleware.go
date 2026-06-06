package fluvio

import "context"

// JobMiddleware wraps job execution.
type JobMiddleware func(next func(context.Context) error) func(context.Context) error

func chainMiddleware(mw []JobMiddleware, final func(context.Context) error) func(context.Context) error {
	fn := final
	for i := len(mw) - 1; i >= 0; i-- {
		fn = mw[i](fn)
	}
	return fn
}

package fluvio_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMiddlewareChainOrder(t *testing.T) {
	var order []string
	mw1 := func(next func(context.Context) error) func(context.Context) error {
		return func(ctx context.Context) error {
			order = append(order, "1")
			return next(ctx)
		}
	}
	mw2 := func(next func(context.Context) error) func(context.Context) error {
		return func(ctx context.Context) error {
			order = append(order, "2")
			return next(ctx)
		}
	}

	fn := mw1(mw2(func(ctx context.Context) error {
		order = append(order, "work")
		return nil
	}))

	require.NoError(t, fn(context.Background()))
	require.Equal(t, []string{"1", "2", "work"}, order)
}

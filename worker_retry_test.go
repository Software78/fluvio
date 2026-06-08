package fluvio_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	fluvio "github.com/software78/fluvio"
)

func TestDefaultRetryDelaySequence(t *testing.T) {
	cases := []struct {
		attempt int16
		wantMin time.Duration
		wantMax time.Duration
	}{
		{1, 3 * time.Second, 5 * time.Second},   // ~4s
		{2, 14 * time.Second, 18 * time.Second}, // ~16s
		{3, 60 * time.Second, 70 * time.Second}, // ~64s
	}
	for _, tc := range cases {
		d := fluvio.DefaultRetryDelay(tc.attempt, 24*time.Hour)
		require.GreaterOrEqual(t, d, tc.wantMin, "attempt %d", tc.attempt)
		require.LessOrEqual(t, d, tc.wantMax, "attempt %d", tc.attempt)
	}
}

func TestDefaultRetryDelayRespectsMaxDelay(t *testing.T) {
	max := 10 * time.Second
	d := fluvio.DefaultRetryDelay(10, max)
	require.Equal(t, max, d)
}

func TestDefaultRetryDelayForJobMatchesDefaultRetryDelay(t *testing.T) {
	job := &fluvio.Job[testArgs]{Attempt: 3}
	want := fluvio.DefaultRetryDelay(3, 24*time.Hour)
	got := fluvio.DefaultRetryDelayForJob(job, 24*time.Hour)
	require.Equal(t, want, got)
}

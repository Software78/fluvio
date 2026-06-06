package fluvio_test

import (
	"testing"

	fluvio "github.com/software78/fluvio"
)

func TestJobClaimedBy(t *testing.T) {
	t.Parallel()

	job := &fluvio.Job[testArgs]{AttemptedBy: nil}
	if got := job.ClaimedBy(); got != "" {
		t.Fatalf("ClaimedBy() = %q, want empty", got)
	}

	job.AttemptedBy = []string{"worker-a", "worker-b"}
	if got := job.ClaimedBy(); got != "worker-b" {
		t.Fatalf("ClaimedBy() = %q, want worker-b", got)
	}
}

type testArgs struct{}

func (testArgs) Kind() string { return "test" }

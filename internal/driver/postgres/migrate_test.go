package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateMigrationSequenceGaps(t *testing.T) {
	// Gaps are allowed
	require.NoError(t, validateMigrationSequence([]string{
		"001_initial", "002_leader", "003_fetch_index",
		"004_workers", "005_dlq",
		// 006, 007 intentionally skipped
		"008_periodic_durable",
		// 009 intentionally skipped
		"010_concurrency", "011_workflows",
	}))
}

func TestValidateMigrationSequenceDuplicate(t *testing.T) {
	err := validateMigrationSequence([]string{"001_a", "001_b"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate")
}

func TestValidateMigrationSequenceInvalidFormat(t *testing.T) {
	err := validateMigrationSequence([]string{"no_prefix"})
	require.Error(t, err)
}

func TestValidateMigrationSequenceEmpty(t *testing.T) {
	require.NoError(t, validateMigrationSequence(nil))
}

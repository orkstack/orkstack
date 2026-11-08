//go:build integration

package orker_test

import (
	"os"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
)

// TestMain runs this package's integration tests in an object-storage bucket
// of their own (internal/testsupport/s3bucket). Integration tests run as
// concurrent processes — within a proof, and across proofs on one host — so
// a bucket is a resource each process owns: the keys it writes, and any
// bucket-wide setting such as a lifecycle rule, are never another process's
// (AGENTS.md §11.1).
//
// The binary's other role is a real Orker for a test to SIGKILL
// (container_removal_integration_test.go): a process of its own, with no test
// of its own for a ledger to list as skipped.
func TestMain(m *testing.M) {
	if spec := os.Getenv(heldOrkerVar); spec != "" {
		os.Exit(runHeldOrker(spec))
	}
	os.Exit(s3bucket.Main(m, "orker"))
}

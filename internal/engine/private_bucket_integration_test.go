//go:build integration

package engine_test

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
func TestMain(m *testing.M) {
	os.Exit(s3bucket.Main(m, "engine"))
}

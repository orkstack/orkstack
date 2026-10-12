//go:build integration

package main

import (
	"os"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// TestMain runs this package's integration tests in an object-storage bucket
// of their own (internal/testsupport/s3bucket), so an engine boot here reads
// only the namespaces and Orkflows these tests wrote — not every earlier
// run's leftovers in the shared development bucket.
//
// The binaries testbin built for this process are removed when it ends.
func TestMain(m *testing.M) {
	code := s3bucket.Main(m, "ork")
	testbin.Cleanup()
	os.Exit(code)
}

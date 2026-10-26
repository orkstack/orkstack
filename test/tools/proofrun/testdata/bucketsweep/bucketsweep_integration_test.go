//go:build integration

package bucketsweep

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
)

// TestMain ends the way s3bucket.Main ends a package whose tests ran: it names
// a bucket and hands it to s3bucket.Finish with the sweeper's file
// (ORK_TEST_BUCKET_LEFT_FILE). There is no object store here, so the remover
// Finish would call on the test's own slot only leaves a marker in
// BUCKETSWEEP_DIR: a marker in that directory is a bucket removed by the
// process instead of left for the sweep.
func TestMain(m *testing.M) {
	// A listing runs no test and needs no bucket, as in s3bucket.Main.
	if !flag.Parsed() {
		flag.Parse()
	}
	if f := flag.Lookup("test.list"); f != nil && f.Value.String() != "" {
		os.Exit(m.Run())
	}
	code := m.Run()
	bucket := s3bucket.Name("bucketsweep", os.Getpid(), time.Now().UnixNano())
	s3bucket.Finish(context.Background(), code, bucket, os.Getenv(s3bucket.EnvLeftFile),
		func(context.Context) error {
			return os.WriteFile(filepath.Join(os.Getenv("BUCKETSWEEP_DIR"), "removed-on-the-slot-"+bucket), nil, 0o644)
		}, os.Stderr)
	os.Exit(code)
}

// TestPasses passes unless BUCKETSWEEP_FAIL=1.
func TestPasses(t *testing.T) {
	if os.Getenv("BUCKETSWEEP_FAIL") == "1" {
		t.Fatal("this test fails")
	}
}

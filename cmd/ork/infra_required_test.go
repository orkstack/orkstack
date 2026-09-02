//go:build integration

package main

import (
	"os"
	"strconv"
	"testing"
)

// infraAbsent reports that a prerequisite this test cannot run without — the
// Temporal cluster, MinIO, the docker daemon — is not there.
//
// By default it skips, so `go test -tags=integration ./...` still works on a
// machine with nothing started. Set ORK_REQUIRE_INFRA and it fails instead.
//
// WHY THE SWITCH EXISTS. `go test` exits 0 for a package whose every test
// called t.Skip, so a harness that reads only the exit code cannot tell
// "proved" from "declined to run" — and one was once caught printing PASS
// on a machine with no cluster. A harness therefore probes the
// infrastructure itself and exports ORK_REQUIRE_INFRA=1 once it is up
// (`make test-integration` does). From then on a skip is not a fact about
// the environment; it is a test opting out, and it turns the run red.
//
// Only the reachability of infrastructure goes through here. A host
// capability that starting the infrastructure would not fix — no directory
// the docker daemon can bind-mount, no route to a public registry — stays an
// ordinary t.Skip, because failing there would report a fault in the tree
// that is not in the tree.
func infraAbsent(t *testing.T, format string, args ...any) {
	t.Helper()
	if required, _ := strconv.ParseBool(os.Getenv("ORK_REQUIRE_INFRA")); required {
		t.Fatalf("ORK_REQUIRE_INFRA is set, so an absent prerequisite is a failure: "+format, args...)
	}
	t.Skipf(format, args...)
}

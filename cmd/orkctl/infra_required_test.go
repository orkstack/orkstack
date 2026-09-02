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
// EVERY prerequisite goes through here, and the container tests are why the
// rule had to be stated that way. Their first version reasoned that a host
// capability starting the infrastructure would not fix — no route to a public
// registry, no directory the docker daemon can bind-mount — should stay an
// ordinary t.Skip, so as not to report a fault in the tree that is not in the
// tree. That is exactly the hole the switch exists to close: with
// ORK_REQUIRE_INFRA set and an image that cannot be pulled, the required test
// printed `--- SKIP`, the package exited 0, and the proof reported PASS having
// measured nothing. Which prerequisite was missing is the PROOF SCRIPT's
// question, and it answers it the same way for all of them: it probes before
// it requires, and reports 77 (INCOMPLETE) for what it could not find. Past
// that point nothing may opt out.
func infraAbsent(t *testing.T, format string, args ...any) {
	t.Helper()
	if required, _ := strconv.ParseBool(os.Getenv("ORK_REQUIRE_INFRA")); required {
		t.Fatalf("ORK_REQUIRE_INFRA is set, so an absent prerequisite is a failure: "+format, args...)
	}
	t.Skipf(format, args...)
}

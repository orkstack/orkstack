//go:build integration

package dsl_test

import (
	"os"
	"strconv"
	"testing"
)

// infraAbsent reports that a prerequisite this test cannot run without — the
// local registry, the docker daemon, the pinned cosign image — is not there.
//
// By default it skips, so `go test -tags=integration ./...` still works on a
// machine with nothing started. Set ORK_REQUIRE_INFRA and it fails instead:
// a harness that probed the infrastructure and found it up exports
// ORK_REQUIRE_INFRA=1 (`make test-integration` does), and from then on a skip
// is a test opting out, which turns the run red (internal/engine's
// infra_required_test.go says why at length).
func infraAbsent(t *testing.T, format string, args ...any) {
	t.Helper()
	if required, _ := strconv.ParseBool(os.Getenv("ORK_REQUIRE_INFRA")); required {
		t.Fatalf("ORK_REQUIRE_INFRA is set, so an absent prerequisite is a failure: "+format, args...)
	}
	t.Skipf(format, args...)
}

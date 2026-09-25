//go:build integration

package api_test

import (
	"os"
	"strconv"
	"testing"
)

// infraAbsent reports that a prerequisite this test cannot run without — the
// Temporal cluster, MinIO — is not there. By default it skips; with
// ORK_REQUIRE_INFRA set (make test-integration, scripts/proof.sh) it fails,
// because a harness that already probed the infrastructure cannot tell a skip
// from a pass by exit code alone. See pkg/artifacts/infra_required_test.go.
func infraAbsent(t *testing.T, format string, args ...any) {
	t.Helper()
	if required, _ := strconv.ParseBool(os.Getenv("ORK_REQUIRE_INFRA")); required {
		t.Fatalf("ORK_REQUIRE_INFRA is set, so an absent prerequisite is a failure: "+format, args...)
	}
	t.Skipf(format, args...)
}

//go:build integration

package main

import (
	"os"
	"strconv"
	"testing"
)

// infraAbsent reports that infrastructure this test cannot run without (the
// Temporal cluster, the object store) is not there: a skip by default, a
// failure under ORK_REQUIRE_INFRA, which the harness exports once it has
// probed the infrastructure itself. cmd/ork's copy says why.
func infraAbsent(t *testing.T, format string, args ...any) {
	t.Helper()
	if required, _ := strconv.ParseBool(os.Getenv("ORK_REQUIRE_INFRA")); required {
		t.Fatalf("ORK_REQUIRE_INFRA is set, so an absent prerequisite is a failure: "+format, args...)
	}
	t.Skipf(format, args...)
}

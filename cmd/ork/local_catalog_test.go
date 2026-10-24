//go:build integration

package main

import (
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
)

// localActivityCatalogEnv is localcatalog.CatalogEnv with this package's
// prerequisite convention: an absent registry or a missing activity image
// skips, and fails under ORK_REQUIRE_INFRA (see infraAbsent).
func localActivityCatalogEnv(t *testing.T) string {
	t.Helper()
	env, err := localcatalog.CatalogEnv()
	if err != nil {
		infraAbsent(t, "local activity catalog unavailable: %v", err)
		return ""
	}
	return env
}

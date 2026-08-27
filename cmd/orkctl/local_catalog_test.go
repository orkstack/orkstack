//go:build integration

package main

import (
	"context"
	"os"
	"os/exec"
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

// requireContainerCanReach proves a container can actually reach the test's
// own HTTP origin before a run depends on it.
//
// Since v0.0.5 the http activity's request is made inside a container, so a
// host address the container cannot route to is a missing prerequisite of the
// MACHINE (a docker network without host.docker.internal), not a defect: it
// skips by default and fails under ORK_REQUIRE_INFRA.
func requireContainerCanReach(ctx context.Context, t *testing.T, url string) {
	t.Helper()
	image := os.Getenv("ORK_TEST_CONTAINER_IMAGE")
	if image == "" {
		image = "alpine:3.20"
	}
	probe := exec.CommandContext(ctx, "docker", "run", "--rm", image,
		"wget", "-q", "-O", "/dev/null", url)
	if out, err := probe.CombinedOutput(); err != nil {
		infraAbsent(t, "a container cannot reach the test origin %s (%v): %s", url, err, out)
	}
}

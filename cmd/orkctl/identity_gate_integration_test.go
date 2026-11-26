//go:build integration

package main

import (
	"context"
	"debug/buildinfo"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	ork "github.com/orkstack/orkstack/internal/buildinfo"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
)

// TestIntegration_OnlyATestsOwnBinariesOpenTheIdentityGate: no binary of a
// proof carries a tag for tests, and neither daemon says it does; the binary a
// test asks for with the gate open carries the tag, says so first in its log
// and says so in its --version.
func TestIntegration_OnlyATestsOwnBinariesOpenTheIdentityGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	carried := func(path string) []string {
		t.Helper()
		info, err := buildinfo.ReadFile(path)
		must.OKf(t, err, "the build information of %s", path)
		return ork.TestTags(info)
	}
	paths := []string{testbin.Path(t, "./cmd/ork"), testbin.Path(t, "./cmd/orker"), testbin.Path(t, "./cmd/orkctl"), testbin.Path(t, "./test/tools/devtoken")}
	if dir := os.Getenv("ORK_TEST_BIN_DIR"); dir != "" {
		// Every binary the proof built, and not only these.
		all, err := filepath.Glob(filepath.Join(dir, "*"))
		must.OK(t, err)
		for _, path := range all {
			if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
				paths = append(paths, path)
			}
		}
	}
	for _, path := range paths {
		if tags := carried(path); len(tags) != 0 {
			t.Errorf("%s carries %v", path, tags)
		}
	}
	const opened = "task identity is open by build tag"
	for _, pkg := range []string{"./cmd/ork", "./cmd/orker"} {
		version, log, code := runBinary(ctx, t, testbin.Path(t, pkg), repoRoot(t), "--version")
		if code != 0 || strings.Contains(version, "test build") || strings.Contains(log, opened) {
			t.Errorf("%s --version: exit %d, %s%s", pkg, code, version, log)
		}
		gated := testbin.GatedPath(t, pkg)
		if tags := carried(gated); !slices.Equal(tags, []string{testbin.IdentityGateTag}) {
			t.Errorf("the gated %s carries %v", pkg, tags)
		}
		version, log, code = runBinary(ctx, t, gated, repoRoot(t), "--version")
		first, _, _ := strings.Cut(log, "\n")
		if code != 0 || !strings.Contains(version, testbin.IdentityGateTag+", never to be deployed") || !strings.Contains(first, opened) {
			t.Errorf("the gated %s --version: exit %d, %s%s", pkg, code, version, log)
		}
	}
	t.Logf("EVIDENCE %d binaries of the run carry no tag for tests, and the gated ork and orker say they do", len(paths))
}

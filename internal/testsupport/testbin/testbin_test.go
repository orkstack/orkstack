package testbin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestPathPrefersTheProofsBinaries(t *testing.T) {
	shared := t.TempDir()
	want := filepath.Join(shared, "orkctl")
	must.OK(t, os.WriteFile(want, []byte("#!/bin/sh\n"), 0o755))
	t.Setenv(EnvDir, shared)
	if got := Path(t, "./cmd/orkctl"); got != want {
		t.Errorf("Path = %s, want the proof's %s", got, want)
	}
}

func TestPathRefusesAnythingButCommandsAndTools(t *testing.T) {
	for _, pkg := range []string{"./internal/engine", "cmd/ork", "../cmd/ork"} {
		if _, err := resolve(pkg); err == nil {
			t.Errorf("resolve(%q) accepted it", pkg)
		}
	}
}

func TestPathBuildsOncePerProcessWithoutTheProofsBinaries(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a binary")
	}
	t.Setenv(EnvDir, "")
	first, err := resolve("./test/tools/deaf-poller")
	must.OK(t, err)
	second, err := resolve("./test/tools/deaf-poller")
	if err != nil || second != first {
		t.Fatalf("second build: %s, %v; want the first, %s", second, err, first)
	}
	if root, _ := repoRoot(); filepath.Dir(first) == filepath.Join(root, "bin") {
		t.Fatalf("built into the repository's bin/: %s", first)
	}
	Cleanup()
	if _, err := os.Stat(first); !os.IsNotExist(err) {
		t.Errorf("Cleanup left %s", first)
	}
}

// TestOrkerEnv_GivesAnOrkerNoStopGraceUnlessTheTestNamesOne: an Orker
// defaults to a day's stop grace, so a test that stops one while it runs a
// task would wait it out; OrkerEnv adds a zero grace, and a grace the test
// (or the proof) already named wins.
func TestOrkerEnv_GivesAnOrkerNoStopGraceUnlessTheTestNamesOne(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
		want []string
	}{
		{"none named", []string{"A=1"}, []string{"A=1", "ORK_ORKER_STOP_GRACE=0s"}},
		{"empty", nil, []string{"ORK_ORKER_STOP_GRACE=0s"}},
		{"the test names one", []string{"ORK_ORKER_STOP_GRACE=10m", "A=1"}, []string{"ORK_ORKER_STOP_GRACE=10m", "A=1"}},
		{"an empty one is still named", []string{"ORK_ORKER_STOP_GRACE="}, []string{"ORK_ORKER_STOP_GRACE="}},
	} {
		in := append([]string(nil), tc.env...)
		got := OrkerEnv(in)
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("%s: OrkerEnv(%q) = %q, want %q", tc.name, tc.env, got, tc.want)
		}
		if strings.Join(in, "|") != strings.Join(tc.env, "|") {
			t.Errorf("%s: OrkerEnv changed its argument to %q", tc.name, in)
		}
	}
}

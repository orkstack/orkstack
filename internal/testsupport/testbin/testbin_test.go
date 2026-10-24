package testbin

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPathPrefersTheProofsBinaries(t *testing.T) {
	shared := t.TempDir()
	want := filepath.Join(shared, "orkctl")
	if err := os.WriteFile(want, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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

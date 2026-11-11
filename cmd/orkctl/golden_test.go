package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// update rewrites the golden files: go test ./cmd/orkctl -run Golden -update
var update = flag.Bool("update", false, "rewrite testdata golden files")

// requireGolden compares human output against a committed golden file.
func requireGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		must.OKf(t, os.MkdirAll("testdata", 0o755), "create testdata")
		must.OKf(t, os.WriteFile(path, []byte(got), 0o644), "write golden %s", path)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run with -update to create it)", path, err)
	}
	if got != string(want) {
		t.Fatalf("output does not match %s\n--- want ---\n%s\n--- got ---\n%s", path, want, got)
	}
}

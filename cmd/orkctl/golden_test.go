package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

// update rewrites the golden files: go test ./cmd/orkctl -run Golden -update
var update = flag.Bool("update", false, "rewrite testdata golden files")

// requireGolden compares human output against a committed golden file.
func requireGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatalf("create testdata: %v", err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden %s: %v", path, err)
		}
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

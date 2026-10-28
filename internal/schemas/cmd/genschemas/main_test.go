package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGenschemas_WritesTheCommittedSchemas: the command writes exactly the
// files docs/schemas/ and the UI's generated copy hold, and names each one it
// wrote.
func TestGenschemas_WritesTheCommittedSchemas(t *testing.T) {
	const root = "../../../.."
	dest := t.TempDir()
	var out bytes.Buffer
	if err := generate(root, dest, &out); err != nil {
		t.Fatal(err)
	}
	written := strings.Fields(out.String())
	if len(written) != 5 {
		t.Fatalf("wrote %v, want the three schemas and the UI's copies of the two kinds'", written)
	}
	for _, path := range written {
		got, err := os.ReadFile(filepath.Join(dest, path))
		if err != nil {
			t.Fatal(err)
		}
		want, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s differs from the committed file (%v): run go run ./internal/schemas/cmd/genschemas", path, err)
		}
	}
}

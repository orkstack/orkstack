package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestGenschemas_WritesTheCommittedSchemas: the command writes exactly the
// files docs/schemas/ and the UI's generated copy hold and the interface
// LABEL of every Dockerfile built beside an interface.ork.yaml, and names
// each one it wrote.
func TestGenschemas_WritesTheCommittedSchemas(t *testing.T) {
	const root = "../../../.."
	dest := t.TempDir()
	var out bytes.Buffer
	must.OK(t, generate(root, dest, &out))
	written := strings.Fields(out.String())
	if len(written) != 14 {
		t.Fatalf("wrote %v, want the four schemas, the UI's copies of the two kinds' and the eight Dockerfiles", written)
	}
	for _, path := range written {
		got, err := os.ReadFile(filepath.Join(dest, path))
		must.OK(t, err)
		want, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s differs from the committed file (%v): run go run ./internal/schemas/cmd/genschemas", path, err)
		}
	}
}

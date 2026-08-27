package orker

import (
	"encoding/base64"
	"os"
	"syscall"
	"testing"
)

// mustWrite writes a file a test depends on, failing the test rather than
// returning an error nobody checks.
func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// mustFIFO creates a named pipe, so the output scan can be shown refusing one.
//
// A FIFO is the entry type most likely to hang a naive scanner: opening it
// blocks until a writer appears. §3.5 refuses it on the STAT, before any open,
// which is what this exercises.
func mustFIFO(t *testing.T, path string) {
	t.Helper()
	if err := syscall.Mkfifo(path, 0o644); err != nil {
		t.Skipf("this filesystem cannot hold a FIFO: %v", err)
	}
}

// decodeRawURL reverses EncodeArtifactName.
func decodeRawURL(encoded string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	return string(raw), err
}

package proof_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

func TestWrite_LandsUnderTheProofDirectory(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(proof.EnvDir, dir)

	proof.Write(t, "engine", "transcript.txt", []byte("observed\n"))
	proof.WriteJSON(t, "engine", "sample.json", map[string]int{"events": 3})

	got, err := os.ReadFile(filepath.Join(dir, "engine", "transcript.txt"))
	if err != nil {
		t.Fatalf("transcript was not written: %v", err)
	}
	if string(got) != "observed\n" {
		t.Errorf("transcript = %q, want %q", got, "observed\n")
	}
	if _, err := os.Stat(filepath.Join(dir, "engine", "sample.json")); err != nil {
		t.Errorf("sample was not written: %v", err)
	}
}

func TestWrite_WithoutADirectoryOnlyLogs(t *testing.T) {
	t.Setenv(proof.EnvDir, "")
	if proof.Dir() != "" {
		t.Fatalf("Dir() = %q, want empty", proof.Dir())
	}
	// Must not panic, fail, or write anywhere.
	proof.Write(t, "engine", "transcript.txt", []byte("observed\n"))
}

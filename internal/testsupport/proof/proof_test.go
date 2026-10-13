package proof_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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

func TestOrkerIDIsUniqueToTheProcessAndNamesTheProof(t *testing.T) {
	t.Setenv(proof.EnvID, "")
	pid := strconv.Itoa(os.Getpid())
	if got := proof.OrkerID("orker-test"); got != "orker-test-"+pid {
		t.Errorf("OrkerID = %q", got)
	}
	t.Setenv(proof.EnvID, "pr-0123456789ab")
	if got := proof.OrkerID("orker-test"); got != "orker-test-pr-0123456789ab-"+pid {
		t.Errorf("OrkerID under a proof = %q", got)
	}
}

// scripts/proof-clean.sh reads the owning pid out of a work root's name with
// -p([0-9]+)-[0-9]+$; a root it cannot read that way it never removes.
func TestWorkRootPatternNamesTheOwningProcess(t *testing.T) {
	root, err := os.MkdirTemp(t.TempDir(), proof.WorkRootPattern("orkctl-TestIntegration_X-sub"))
	if err != nil {
		t.Fatal(err)
	}
	want := regexp.MustCompile(`^orkctl-TestIntegration_X-sub-p` + strconv.Itoa(os.Getpid()) + `-[0-9]+$`)
	if base := filepath.Base(root); !want.MatchString(base) {
		t.Errorf("work root %q does not match %s", base, want)
	}
}

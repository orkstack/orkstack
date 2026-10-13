// Package proof records the evidence an integration test produces — a CLI
// transcript, a measurement, a sample of a wire format — where `make proof`
// (scripts/proof.sh) collects it.
//
// Every writer goes through here so one variable, ORK_PROOF_DIR, decides where
// evidence lands: <ORK_PROOF_DIR>/<group>/<name>. With the variable unset the
// body still reaches the test log, so a plain `go test -tags=integration`
// carries the same evidence inside its own output and never writes into the
// tree. Writing is best effort: a proof that cannot be written is reported in
// the log and never fails the test that produced it, because the behavior
// under test has already been observed by then.
package proof

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// EnvDir names the directory evidence is written under.
const EnvDir = "ORK_PROOF_DIR"

// EnvID names the proof this process runs under. scripts/proof.sh sets it
// (`pr-<hex>`), and it is what scripts/proof-stop.sh finds a stopped proof's
// leftovers by.
const EnvID = "ORK_PROOF_ID"

// OrkerID returns an Orker identity for base that is unique to this process:
// base, the proof's id when a proof runs it, and the pid.
//
// An Orker identity is HOST-wide, not process-wide: Boot takes an exclusive
// lock on it in the host's temp directory and reaps every container labelled
// with it. Tests run as concurrent processes (AGENTS.md §11.1), so a literal
// identity would make a second process's Orker refuse to boot — or, without
// the lock, reap the first one's containers.
func OrkerID(base string) string {
	if id := os.Getenv(EnvID); id != "" {
		return fmt.Sprintf("%s-%s-%d", base, id, os.Getpid())
	}
	return fmt.Sprintf("%s-%d", base, os.Getpid())
}

// WorkRootPattern is the os.MkdirTemp pattern for a test's own Orker work
// root under the shared parent (ORK_ORKER_WORKDIR or the repository's
// .data/work): label, then this process's pid, then MkdirTemp's numeric
// suffix — <label>-p<pid>-<digits>. Never the parent itself: an Orker's Boot
// reaps every pin, spool and stage directory under its root, so two Orkers on
// one root delete each other's in-flight work (AGENTS.md §11.1). The pid is
// what lets scripts/proof-clean.sh tell a leftover root from one a running
// test still uses; it removes only a root whose process is gone.
func WorkRootPattern(label string) string {
	return fmt.Sprintf("%s-p%d-*", label, os.Getpid())
}

// Dir reports the evidence directory, or "" when no run asked for one.
func Dir() string {
	return os.Getenv(EnvDir)
}

// Write records body as <ORK_PROOF_DIR>/<group>/<name>, and logs it either way.
func Write(t testing.TB, group, name string, body []byte) {
	t.Helper()
	t.Logf("proof %s/%s:\n%s", group, name, body)
	dir := Dir()
	if dir == "" {
		t.Logf("%s is unset: %s/%s was logged, not written", EnvDir, group, name)
		return
	}
	target := filepath.Join(dir, group, name)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Logf("proof: cannot create %s: %v", filepath.Dir(target), err)
		return
	}
	if err := os.WriteFile(target, body, 0o644); err != nil {
		t.Logf("proof: cannot write %s: %v", target, err)
		return
	}
	t.Logf("proof written: %s", target)
}

// WriteJSON records value, pretty-printed, as <ORK_PROOF_DIR>/<group>/<name>.
func WriteJSON(t testing.TB, group, name string, value any) {
	t.Helper()
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Logf("proof: cannot encode %s/%s: %v", group, name, err)
		return
	}
	Write(t, group, name, append(body, '\n'))
}

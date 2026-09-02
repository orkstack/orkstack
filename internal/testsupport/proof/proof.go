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
	"os"
	"path/filepath"
	"testing"
)

// EnvDir names the directory evidence is written under.
const EnvDir = "ORK_PROOF_DIR"

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

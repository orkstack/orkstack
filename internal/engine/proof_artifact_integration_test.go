//go:build integration

package engine_test

import (
	"os"
	"path/filepath"
	"testing"
)

// EnvProofDir names where a run publishes its proof artifacts.
//
// A proof artifact is published only when a proof run asks for it. The
// integration tests here measure things worth recording, and proof/M10.2 is a
// committed directory: a test that wrote into it on every run would leave the
// tree dirty after the repository's own gate (`go test -tags=integration -p 1
// ./... -count=1`), and a reader could not tell this run's measurement from
// yesterday's — the staleness rule AGENTS.md §6 exists for.
//
// So the destination is an argument. proof/M10.2/reproduce.sh sets it; a plain
// `go test` does not, and the body then goes to the test log, where the same
// evidence survives inside test.log.
const EnvProofDir = "ORK_PROOF_DIR"

// publishProofArtifact logs an artifact and writes it out when, and only when,
// a destination was given.
func publishProofArtifact(t *testing.T, name, body string) {
	t.Helper()
	t.Logf("%s\n%s", name, body)

	dir := os.Getenv(EnvProofDir)
	if dir == "" {
		t.Logf("%s is unset: %s was logged, not published. Set it to record the artifact.",
			EnvProofDir, name)
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Logf("cannot create %s: %v", dir, err)
		return
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Logf("cannot write %s: %v", path, err)
		return
	}
	t.Logf("wrote %s", path)
}

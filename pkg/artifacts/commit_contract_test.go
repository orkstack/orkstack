package artifacts_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/commitcontract"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestCommitStore_EveryLocalBackendShowsTheOneContract runs the shared
// CommitStore contract — the one the real S3 backend runs in the integration
// suite — against the local and in-memory fakes, so a backend cannot drift on
// "a second Create is refused" or on "Get returns the exact bytes Create
// wrote".
func TestCommitStore_EveryLocalBackendShowsTheOneContract(t *testing.T) {
	root := t.TempDir()
	store, err := artifacts.NewLocalCommitStore(root)
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := artifacts.CommitRoot(artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: root})
	if err != nil {
		t.Fatal(err)
	}
	key, err := artifacts.CommitKey("default", "run-contract-local", "contract")
	if err != nil {
		t.Fatal(err)
	}
	commitcontract.Run(t, store, strings.TrimSuffix(prefix, "/")+"/"+key)

	commitcontract.Run(t, artifacts.NewMemoryCommitStore(), "mem://commits/contract/run-contract-memory/commit.json")
}

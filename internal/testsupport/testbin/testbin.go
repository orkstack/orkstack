// Package testbin gives integration tests the OrkStack binaries they run:
// `ork`, `orker`, `orkctl` and the test tools under test/tools.
//
// A proof builds every binary its suites run ONCE, from the tree it is
// proving, into its own directory, and exports that directory as
// ORK_TEST_BIN_DIR (scripts/proof.sh). Path returns the binary from there.
// Without the variable — a plain `go test -tags=integration` — Path builds
// the binary once per test process into a directory of its own, which
// Cleanup removes when the process's TestMain ends. Nothing here ever writes
// to the repository's bin/, which every worktree's dev stack and every
// concurrent proof would otherwise share (AGENTS.md §11.1).
//
// Path takes a LITERAL package path ("./cmd/ork", "./test/tools/cosign-sign"):
// test/tools/proofrun reads those literals to know which binaries a suite
// runs, which is how a feature proof knows that an orkctl integration test
// exercises internal/engine although it never imports it. A Go binary built
// any other way in a test is refused by that scan.
package testbin

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// EnvDir names the directory a proof built the binaries into.
const EnvDir = "ORK_TEST_BIN_DIR"

var (
	mu    sync.Mutex
	dir   string // this process's own build directory, when it needed one
	built = map[string]*build{}
)

type build struct {
	once sync.Once
	path string
	err  error
}

// Path returns the path of the binary built from pkg, a "./cmd/<name>" or
// "./test/tools/<name>" path relative to the repository root.
func Path(t testing.TB, pkg string) string {
	t.Helper()
	p, err := resolve(pkg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func resolve(pkg string) (string, error) {
	if !strings.HasPrefix(pkg, "./cmd/") && !strings.HasPrefix(pkg, "./test/tools/") {
		return "", fmt.Errorf("testbin: %q is not a ./cmd/<name> or ./test/tools/<name> package", pkg)
	}
	name := path.Base(pkg)
	if shared := os.Getenv(EnvDir); shared != "" {
		candidate := filepath.Join(shared, name)
		if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
			return candidate, nil
		}
	}
	mu.Lock()
	b := built[pkg]
	if b == nil {
		b = &build{}
		built[pkg] = b
	}
	mu.Unlock()
	b.once.Do(func() { b.path, b.err = compile(pkg, name) })
	return b.path, b.err
}

func compile(pkg, name string) (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	mu.Lock()
	if dir == "" {
		d, err := os.MkdirTemp("", "ork-testbin-")
		if err != nil {
			mu.Unlock()
			return "", fmt.Errorf("testbin: %w", err)
		}
		dir = d
	}
	out := filepath.Join(dir, name)
	mu.Unlock()
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Dir = root
	if b, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("testbin: go build %s: %v\n%s", pkg, err, b)
	}
	return out, nil
}

// Cleanup removes the directory this process built binaries into, if any.
// TestMain calls it after m.Run.
func Cleanup() {
	mu.Lock()
	defer mu.Unlock()
	if dir != "" {
		_ = os.RemoveAll(dir)
		dir = ""
	}
	built = map[string]*build{}
}

// repoRoot walks up from the working directory to the module root.
func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for d := wd; ; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			return d, nil
		}
		if filepath.Dir(d) == d {
			return "", fmt.Errorf("testbin: no go.mod above %s", wd)
		}
	}
}

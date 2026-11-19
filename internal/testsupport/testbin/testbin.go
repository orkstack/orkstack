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
// Path takes a package path relative to the module root ("./cmd/ork",
// "./test/tools/cosign-sign"). A test builds a binary only through Path
// (AGENTS.md §11.1).
package testbin

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
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

// StaticPath returns the path of a statically linked build of pkg
// (CGO_ENABLED=0), for a test that copies the binary into an image with no C
// library, such as the orker image's distroless base. It is always built by
// this process, never taken from a proof's ORK_TEST_BIN_DIR, whose binaries
// are the host's own linkage. A test that runs the binary on the host and in
// an image runs this one in both, so the two are one build.
func StaticPath(t testing.TB, pkg string) string {
	t.Helper()
	if err := checkPackage(pkg); err != nil {
		t.Fatal(err)
	}
	p, err := once("static:"+pkg, func() (string, error) { return compile(pkg, path.Base(pkg)+"-static", true) })
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func checkPackage(pkg string) error {
	if !strings.HasPrefix(pkg, "./cmd/") && !strings.HasPrefix(pkg, "./test/tools/") {
		return fmt.Errorf("testbin: %q is not a ./cmd/<name> or ./test/tools/<name> package", pkg)
	}
	return nil
}

// once builds key at most once per process.
func once(key string, compileIt func() (string, error)) (string, error) {
	mu.Lock()
	b := built[key]
	if b == nil {
		b = &build{}
		built[key] = b
	}
	mu.Unlock()
	b.once.Do(func() { b.path, b.err = compileIt() })
	return b.path, b.err
}

func resolve(pkg string) (string, error) {
	if err := checkPackage(pkg); err != nil {
		return "", err
	}
	name := path.Base(pkg)
	if shared := os.Getenv(EnvDir); shared != "" {
		candidate := filepath.Join(shared, name)
		if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
			return candidate, nil
		}
	}
	return once(pkg, func() (string, error) { return compile(pkg, name, false) })
}

func compile(pkg, name string, static bool) (string, error) {
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
	cmd.Env = GoBuildEnv(os.Environ())
	if static {
		cmd.Env = append(cmd.Env, "CGO_ENABLED=0")
	}
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

// StopGraceVar is the Orker's stop grace setting (internal/orker.StopGraceVar).
const StopGraceVar = "ORK_ORKER_STOP_GRACE"

// OrkerEnv is env for an Orker a test starts: env with a zero stop grace
// added unless it already names one (the test's own, or the proof's). An
// Orker defaults to a day's grace, so a test that stops one while it runs a
// task would otherwise wait it out; a proof sets the same zero grace for
// every Orker (test/tools/proofrun), and OrkerEnv makes a by-hand `go test`
// behave as a proof does. TestDocs_OrkerSpawnsSetAStopGrace holds every
// test that starts ./cmd/orker to it. env is not changed.
func OrkerEnv(env []string) []string {
	for _, kv := range env {
		if strings.HasPrefix(kv, StopGraceVar+"=") {
			return env
		}
	}
	return append(append(make([]string, 0, len(env)+1), env...), StopGraceVar+"=0s")
}

// GoBuildEnv is env for a go build of a binary under test: env without the
// variables a developer's shell or an outer make may set that change which
// binary the build makes (its target, its flags, its experiments). The binary
// a test runs is the host's ordinary build, never one the shell chose.
func GoBuildEnv(env []string) []string {
	return without(env, "GOFLAGS", "GOOS", "GOARCH", "GOEXPERIMENT", "CGO_ENABLED", "GO386", "GOAMD64", "GOARM", "GOARM64", "GOWASM")
}

// StackEnv is env for an ork, orker or orkctl a test starts: env without the
// settings a developer's shell may hold for its own deployment and the proof
// never sets, which would reconfigure the process under test. What the proof
// and CI hand a stack on purpose (its Temporal address and namespace, the
// object store, the container CLI, the work root) passes through, and a
// variable the test itself appends after StackEnv still wins.
func StackEnv(env []string) []string {
	return without(env, "ORK_CONFIG_FILE", "ORK_ADMIN_ADDR", "ORK_PPROF_ENABLED",
		"ORK_TEMPORAL_TLS_ENABLED", "ORK_TEMPORAL_TLS_CA_FILE", "ORK_TEMPORAL_TLS_CERT_FILE",
		"ORK_TEMPORAL_TLS_KEY_FILE", "ORK_TEMPORAL_TLS_SERVER_NAME", "ORK_TEMPORAL_API_KEY_FILE")
}

// without is env less every entry whose name is one of names; env is not
// changed.
func without(env []string, names ...string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		if !slices.Contains(names, name) {
			out = append(out, kv)
		}
	}
	return out
}

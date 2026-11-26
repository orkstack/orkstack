package dist_test

import (
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/dist"
	"golang.org/x/tools/go/packages"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// repoRoot is the module root: this package lives at internal/dist.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	must.OK(t, err)
	return root
}

// goTool finds the go command this test run was started with, so a cross
// build uses the same toolchain as the test binary: `go test` puts its own
// GOROOT/bin first on the PATH of the test binary it runs.
func goTool(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("go")
	must.OKf(t, err, "no go command to build with")
	return path
}

// buildSlots bounds the go builds this package runs at once, of every test:
// more compilers and linkers at once would not finish sooner on one host. A
// slot is held only around one go command, never while waiting on another
// test's build, so no wait can hold one.
var buildSlots = make(chan struct{}, 3)

// inSlot runs build while it holds a build slot.
func inSlot(build func() error) error {
	buildSlots <- struct{}{}
	defer func() { <-buildSlots }()
	return build()
}

// compiled are the compile configurations warmCompile compiled, by GOOS/GOARCH.
var compiled sync.Map // string -> *sharedBuild

// warmCompile compiles every release binary of target's GOOS and GOARCH in one
// go build, once for the whole run, before any test of the configuration builds
// one: the go command shares no compile between two processes running at once,
// so the builds the tests start together would each compile the whole
// dependency graph. After it, each of their builds only links. Nothing inside
// the once may stop the test that runs it, or every other would read it done.
func warmCompile(t *testing.T, target dist.Target) {
	t.Helper()
	root, goBin := repoRoot(t), goTool(t)
	key := target.GOOS + "-" + target.GOARCH
	entry, _ := compiled.LoadOrStore(key, &sharedBuild{})
	b := entry.(*sharedBuild)
	b.once.Do(func() {
		var mains []string
		for _, other := range dist.Targets() {
			if other.GOOS == target.GOOS && other.GOARCH == target.GOARCH {
				mains = append(mains, other.Package())
			}
		}
		args := []string{"build", "-trimpath", "-buildvcs=false", "-tags", dist.BuildTags}
		if len(mains) == 1 {
			args = append(args, "-o", os.DevNull) // one main package is linked; more are compiled only
		}
		args = append(args, mains...)
		b.err = inSlot(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, goBin, args...)
			cmd.Dir = root
			cmd.Env = append(os.Environ(), target.BuildEnv()...)
			if output, err := cmd.CombinedOutput(); err != nil {
				return fmt.Errorf("go %s: %w\n%s", strings.Join(args, " "), err, output)
			}
			return nil
		})
	})
	must.OKf(t, b.err, "compile %s", key)
}

// TestTargets_AreTheDecidedMatrix: the release publishes orkctl for linux,
// darwin and windows on amd64 and arm64, and ork and orker for linux on
// amd64 and arm64. The list is written out here rather than derived from the
// implementation, so adding or dropping a platform is a decision this test
// shows.
func TestTargets_AreTheDecidedMatrix(t *testing.T) {
	t.Parallel()
	want := []string{
		"ork linux amd64",
		"ork linux arm64",
		"orkctl darwin amd64",
		"orkctl darwin arm64",
		"orkctl linux amd64",
		"orkctl linux arm64",
		"orkctl windows amd64",
		"orkctl windows arm64",
		"orker linux amd64",
		"orker linux arm64",
	}
	var got []string
	for _, target := range dist.Targets() {
		got = append(got, target.Binary+" "+target.GOOS+" "+target.GOARCH)
	}
	slices.Sort(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("release matrix\n got: %q\nwant: %q", got, want)
	}
}

// TestTargets_ReturnsACopy: a caller that edits the list it was given does
// not change what the next caller builds.
func TestTargets_ReturnsACopy(t *testing.T) {
	t.Parallel()
	first := dist.Targets()
	first[0].GOOS = "plan9"
	if again := dist.Targets(); again[0].GOOS == "plan9" {
		t.Fatal("Targets returned the package's own slice")
	}
}

// TestTargets_BuildArguments: every release build is static, reproducible and
// stamped, whatever the target.
func TestTargets_BuildArguments(t *testing.T) {
	t.Parallel()
	for _, target := range dist.Targets() {
		args := target.BuildArgs("v1.2.3", "abc123", "a2V5", "out/"+target.Binary)
		ldflags := "-X main.version=v1.2.3 -X main.commit=abc123"
		if target.Binary == dist.Orker {
			// Only the orker verifies its probe image with the release key.
			ldflags += " -X main.releaseKey=a2V5"
		}
		want := []string{
			"build", "-trimpath", "-buildvcs=false", "-tags", "timetzdata",
			"-ldflags", ldflags,
			"-o", "out/" + target.Binary, "./cmd/" + target.Binary,
		}
		if !reflect.DeepEqual(args, want) {
			t.Errorf("%s: go %q, want go %q", target, args, want)
		}
		env := target.BuildEnv()
		// The variables that change a build's bytes are fixed, so the
		// caller's own (a GOFLAGS of -race, a GOAMD64 of v3) cannot reach a
		// release binary.
		for _, wantVar := range []string{
			"CGO_ENABLED=0", "GOOS=" + target.GOOS, "GOARCH=" + target.GOARCH,
			"GOFLAGS=", "GOEXPERIMENT=", "GOAMD64=v1", "GOARM64=v8.0",
		} {
			if !slices.Contains(env, wantVar) {
				t.Errorf("%s: build environment %q lacks %s", target, env, wantVar)
			}
		}
	}
}

// fullyBuilt is the architecture of each release OS whose targets are built
// in full; the others of that OS are type-checked (archCheck). What a build of
// our own code depends on is the OS: it is the proof that orkctl compiles for
// Windows, which the Unix-only parts of internal/orker once prevented.
var fullyBuilt = map[string]string{"linux": "amd64", "darwin": "arm64", "windows": "amd64"}

// TestTargets_EveryBinaryCrossCompiles: each release target builds, from this
// tree, with the flags a release uses: in full for one architecture of each OS
// (fullyBuilt), and for the other, every package of its graph, of every
// module and the standard library, type-checks for it from source. That one is
// not compiled or linked, so what only its compiler or linker would refuse (an
// assembly function an architecture lacks) is left to release.yml's make dist,
// which builds and links every target before anything is published. A build is
// slow, so -short skips it; the proof's unit tier does not run -short.
func TestTargets_EveryBinaryCrossCompiles(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-compiles every release target; -short skips it, the proof runs it")
	}
	t.Parallel()
	root := repoRoot(t)
	goBin := goTool(t)
	for _, target := range dist.Targets() {
		full, ok := fullyBuilt[target.GOOS]
		if !ok {
			t.Fatalf("%s: no architecture of %s is built in full", target, target.GOOS)
		}
		t.Run(target.String(), func(t *testing.T) {
			t.Parallel()
			if target.GOOS == runtime.GOOS && target.GOARCH == runtime.GOARCH {
				// This host's targets are built as a release builds them once,
				// for every test that runs them.
				sharedReleaseBuild(t, target, testCommit)
				return
			}
			if target.GOARCH != full {
				archCheck(t, target)
				return
			}
			warmCompile(t, target)
			var output bytes.Buffer
			err := inSlot(func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
				defer cancel()
				cmd := exec.CommandContext(ctx, goBin, target.BuildArgs("v0.0.0-test", "0000000000000000000000000000000000000000", "", os.DevNull)...)
				cmd.Dir = root
				cmd.Env = append(os.Environ(), target.BuildEnv()...)
				cmd.Stdout, cmd.Stderr = &output, &output
				return cmd.Run()
			})
			if err != nil {
				t.Fatalf("%s does not build: %v\n%s", target, err, output.String())
			}
		})
	}
}

// archChecks runs one archCheck at a time: each holds every package of a
// binary's graph, parsed and typed, which is the most memory a test here takes.
var archChecks sync.Mutex

// archCheck type-checks target's binary for its GOOS and GOARCH from source,
// with every package of its graph: a type that differs by architecture
// (syscall.Stat_t's fields), a symbol one lacks, a file only one has. It
// compiles nothing, so it costs seconds where a compile costs minutes.
func archCheck(t *testing.T, target dist.Target) {
	t.Helper()
	root := repoRoot(t)
	archChecks.Lock()
	defer archChecks.Unlock()
	var pkgs []*packages.Package
	err := inSlot(func() error {
		var err error
		pkgs, err = packages.Load(&packages.Config{
			Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps | packages.NeedSyntax | packages.NeedTypes,
			Dir:  root,
			// Its own go list, never a driver the caller's environment names.
			Env:        append(os.Environ(), append(target.BuildEnv(), "GOPACKAGESDRIVER=off")...),
			BuildFlags: []string{"-tags", dist.BuildTags},
			ParseFile: func(fset *token.FileSet, name string, src []byte) (*ast.File, error) {
				return parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
			},
		}, target.Package())
		return err
	})
	must.OKf(t, err, "load %s", target)
	seen := map[string]bool{}
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		seen[p.PkgPath] = true
		for _, e := range p.Errors {
			t.Errorf("%s: %s does not type-check for it: %v", target, p.PkgPath, e)
		}
	})
	if len(pkgs) != 1 || pkgs[0].Types == nil || !pkgs[0].Types.Complete() || !seen["runtime"] || !seen["syscall"] {
		t.Fatalf("%s: its own package was not type-checked with what it imports (%d packages), so this test proves nothing", target, len(seen))
	}
	t.Logf("%s: %d packages type-checked", target, len(seen))
}

// TestTargets_FullyBuiltAreReleaseTargets: the architecture built in full for
// each OS is one the release publishes, so a full build is always a release's.
func TestTargets_FullyBuiltAreReleaseTargets(t *testing.T) {
	t.Parallel()
	published := map[string]bool{}
	for _, target := range dist.Targets() {
		published[target.GOOS+"/"+target.GOARCH] = true
	}
	for goos, goarch := range fullyBuilt {
		if !published[goos+"/"+goarch] {
			t.Errorf("%s/%s is built in full, and the release publishes no target for it", goos, goarch)
		}
	}
}

// TestTargets_ReleaseBuildsEmbedTheTimeZoneDatabase: a scratch image has no
// zoneinfo, and schedules and automation documents load named time zones, so
// every release binary must link time/tzdata. The same build without the tag
// does not, which is what makes the first half mean something.
func TestTargets_ReleaseBuildsEmbedTheTimeZoneDatabase(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	goBin := goTool(t)
	deps := func(t *testing.T, target dist.Target, tags string) []string {
		t.Helper()
		args := []string{"list", "-deps", "-f", "{{.ImportPath}}"}
		if tags != "" {
			args = append(args, "-tags", tags)
		}
		args = append(args, target.Package())
		cmd := exec.Command(goBin, args...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), target.BuildEnv()...)
		out, err := cmd.Output()
		must.OKf(t, err, "go %s", strings.Join(args, " "))
		return strings.Fields(string(out))
	}
	tagsOf := func(t *testing.T, target dist.Target) string {
		t.Helper()
		args := target.BuildArgs("v0.0.0-test", "0000000000000000000000000000000000000000", "", "x")
		i := slices.Index(args, "-tags")
		if i < 0 || i+1 >= len(args) {
			t.Fatalf("%s: build arguments %q carry no -tags", target, args)
		}
		return args[i+1]
	}
	// One target per binary is enough: the tag is the same for all of them.
	seen := map[string]bool{}
	for _, target := range dist.Targets() {
		if seen[target.Binary] {
			continue
		}
		seen[target.Binary] = true
		t.Run(target.Binary, func(t *testing.T) {
			t.Parallel()
			if slices.Contains(deps(t, target, ""), "time/tzdata") {
				t.Fatalf("%s links time/tzdata without the tag, so this test proves nothing", target.Binary)
			}
			if !slices.Contains(deps(t, target, tagsOf(t, target)), "time/tzdata") {
				t.Fatalf("a release build of %s does not link time/tzdata (build tags %q)", target.Binary, tagsOf(t, target))
			}
		})
	}
}

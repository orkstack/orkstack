package dist_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/dist"
)

// repoRoot is the module root: this package lives at internal/dist.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// goTool finds the go command this test run was started with, so a cross
// build uses the same toolchain as the test binary.
func goTool(t *testing.T) string {
	t.Helper()
	path := filepath.Join(runtime.GOROOT(), "bin", "go")
	if _, err := os.Stat(path); err != nil {
		var lookErr error
		if path, lookErr = exec.LookPath("go"); lookErr != nil {
			t.Fatalf("no go command to build with: %v", lookErr)
		}
	}
	return path
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
		args := target.BuildArgs("v1.2.3", "out/"+target.Binary)
		want := []string{
			"build", "-trimpath", "-buildvcs=false", "-tags", "timetzdata",
			"-ldflags", "-X main.version=v1.2.3",
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

// TestTargets_EveryBinaryCrossCompiles: each release target builds, from this
// tree, with the flags a release uses. It is the proof that orkctl compiles
// for Windows, which the Unix-only parts of internal/orker once prevented. A
// build is slow, so -short skips it; the proof's unit tier does not run
// -short.
func TestTargets_EveryBinaryCrossCompiles(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-compiles every release target; -short skips it, the proof runs it")
	}
	t.Parallel()
	root := repoRoot(t)
	goBin := goTool(t)
	// Ten linkers at once would not finish sooner on one host.
	slots := make(chan struct{}, 3)
	for _, target := range dist.Targets() {
		t.Run(target.String(), func(t *testing.T) {
			t.Parallel()
			slots <- struct{}{}
			defer func() { <-slots }()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, goBin, target.BuildArgs("v0.0.0-test", os.DevNull)...)
			cmd.Dir = root
			cmd.Env = append(os.Environ(), target.BuildEnv()...)
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			if err := cmd.Run(); err != nil {
				t.Fatalf("%s does not build: %v\n%s", target, err, output.String())
			}
		})
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
		if err != nil {
			t.Fatalf("go %s: %v", strings.Join(args, " "), err)
		}
		return strings.Fields(string(out))
	}
	tagsOf := func(t *testing.T, target dist.Target) string {
		t.Helper()
		args := target.BuildArgs("v0.0.0-test", "x")
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

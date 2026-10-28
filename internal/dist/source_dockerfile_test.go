package dist_test

import (
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dist"
	"github.com/orkstack/orkstack/internal/dsl"
)

// deploy/source/Dockerfile builds the quickstart's platform images from a
// clone (compose.yaml). These tests hold it to the release's images: every
// stage deploy/Dockerfile has is here on the same pinned base, doing the same
// thing, and the only difference is where the binaries come from, compiled
// here instead of copied from `make dist`'s output.

const (
	sourceDockerfilePath   = "deploy/source/Dockerfile"
	sourceDockerignorePath = "deploy/source/Dockerfile.dockerignore"
	// binariesStage is the one stage the release has no counterpart for.
	binariesStage = "binaries"
)

func sourceStages(t *testing.T) []dfStage {
	t.Helper()
	return parseDockerfile(t, sourceDockerfilePath, readRepoFile(t, sourceDockerfilePath))
}

// stageShape is what a stage does, without what only a release carries (its
// LABELs, and the ARGs they and the binaries' paths read), and with a COPY of
// one of the binaries reduced to its destination: the release copies `make
// dist`'s file and the source build its own compilation, to the same path.
func stageShape(s dfStage) []string {
	var out []string
	for _, ins := range s.body {
		switch {
		case ins.cmd == "LABEL", ins.cmd == "ARG":
			continue
		case ins.cmd == "COPY" && (ins.flags["from"] == binariesStage || strings.HasPrefix(ins.args, "dist/bin/")):
			_, dest := copyOperands(ins)
			out = append(out, "COPY <binary> "+dest)
		default:
			var flags []string
			for _, name := range slices.Sorted(maps.Keys(ins.flags)) {
				flags = append(flags, "--"+name+"="+ins.flags[name])
			}
			out = append(out, strings.TrimSpace(ins.cmd+" "+strings.Join(flags, " ")+" "+ins.args))
		}
	}
	return out
}

func TestSourceDockerfile_StagesAreTheReleasesOwn(t *testing.T) {
	t.Parallel()
	release, source := dockerfileStages(t), sourceStages(t)
	byName := map[string]dfStage{}
	for _, s := range source {
		byName[s.name] = s
	}
	for _, want := range release {
		got, ok := byName[want.name]
		if !ok {
			t.Errorf("%s has no stage %s, which %s builds", sourceDockerfilePath, want.name, dockerfilePath)
			continue
		}
		if got.image != want.image || got.platform != want.platform {
			t.Errorf("stage %s is FROM %s (platform %q) here, but FROM %s (platform %q) in %s",
				want.name, got.image, got.platform, want.image, want.platform, dockerfilePath)
		}
		if g, w := stageShape(got), stageShape(want); !slices.Equal(g, w) {
			t.Errorf("stage %s differs from %s's:\n  here    %q\n  release %q", want.name, dockerfilePath, g, w)
		}
		delete(byName, want.name)
	}
	if extra := slices.Sorted(maps.Keys(byName)); !slices.Equal(extra, []string{binariesStage}) {
		t.Errorf("%s's stages beyond the release's are %v, want only %s", sourceDockerfilePath, extra, binariesStage)
	}
	// The shape is not blind to a change.
	ork := stageNamed(t, release, "ork")
	changed := ork
	changed.body = append(slices.Clone(ork.body), dfInstruction{cmd: "USER", args: "root"})
	if slices.Equal(stageShape(changed), stageShape(ork)) {
		t.Error("stageShape does not see an added USER root")
	}
}

func TestSourceDockerfile_EveryStageIsDigestPinned(t *testing.T) {
	t.Parallel()
	expectBasesPinned(t, sourceDockerfilePath, sourceStages(t))
}

// TestSourceDockerfile_CompilesTheReleaseCommandsStatically: the binaries
// stage builds, once and on the build platform, exactly the commands the
// release images carry, as static binaries the distroless bases run, with the
// Go the workflows pin and never one it would download.
func TestSourceDockerfile_CompilesTheReleaseCommandsStatically(t *testing.T) {
	t.Parallel()
	stages := sourceStages(t)
	binaries := stageNamed(t, stages, binariesStage)
	if binaries.platform != "$BUILDPLATFORM" {
		t.Errorf("the %s stage runs on platform %q, want $BUILDPLATFORM: Go cross-compiles, so nothing runs under emulation",
			binariesStage, binaries.platform)
	}
	ref, err := dsl.ParseReference(binaries.image)
	if err != nil {
		t.Fatalf("%s:%d: %v", sourceDockerfilePath, binaries.line, err)
	}
	if version, _, _ := strings.Cut(ref.Tag, "-"); ref.Repository != "library/golang" || version != proofPin(t, "go-version") {
		t.Errorf("%s:%d: the %s stage is on %s, want golang at the workflows' go-version %s",
			sourceDockerfilePath, binaries.line, binariesStage, binaries.image, proofPin(t, "go-version"))
	}
	runs := binaries.all("RUN")
	if len(runs) != 1 {
		t.Fatalf("the %s stage has %d RUN steps, want one go build", binariesStage, len(runs))
	}
	build := runs[0].args
	for _, want := range []string{
		"CGO_ENABLED=0", "GOOS=linux", "GOARCH=$TARGETARCH", "GOTOOLCHAIN=local", "go build", "-trimpath", "-buildvcs=false",
		"-tags " + dist.BuildTags, "-o /out/",
	} {
		if !strings.Contains(build, want) {
			t.Errorf("the go build %q does not say %s", build, want)
		}
	}
	// What the runtime stages copy from it is what it builds, and is every
	// linux binary the release ships.
	var copied []string
	for _, s := range stages {
		for _, ins := range s.all("COPY") {
			if ins.flags["from"] != binariesStage {
				continue
			}
			sources, _ := copyOperands(ins)
			for _, source := range sources {
				binary := strings.TrimPrefix(source, "/out/")
				copied = append(copied, binary)
				if !strings.Contains(build, "./cmd/"+binary) {
					t.Errorf("stage %s copies %s, which the go build does not build (./cmd/%s)", s.name, source, binary)
				}
			}
		}
	}
	var shipped []string
	for _, target := range dist.Targets() {
		if target.GOOS == "linux" && !slices.Contains(shipped, target.Binary) {
			shipped = append(shipped, target.Binary)
		}
	}
	if !slices.Equal(sorted(copied), sorted(shipped)) {
		t.Errorf("the runtime stages copy %v from the build, want the release's linux binaries %v", sorted(copied), sorted(shipped))
	}
	// No other stage runs on the target platform either.
	for _, s := range stages {
		if len(s.all("RUN")) != 0 && s.platform != "$BUILDPLATFORM" {
			t.Errorf("stage %s runs a step on the target platform (--platform=%q)", s.name, s.platform)
		}
	}
}

// TestSourceDockerignore_AdmitsWhatTheBuildReadsAndNothingElse: the context
// holds the Go module's sources, the UI's and the examples, and never a
// secret, the git directory or what a build or a test left behind.
func TestSourceDockerignore_AdmitsWhatTheBuildReadsAndNothingElse(t *testing.T) {
	t.Parallel()
	rules := parseDockerignore(readRepoFile(t, sourceDockerignorePath))
	if len(rules) == 0 || rules[0].negate || strings.Join(rules[0].segments, "/") != "*" {
		t.Fatalf("%s must start with * so that nothing is in the context unless it is admitted", sourceDockerignorePath)
	}
	root := repoRoot(t)
	// Every Go file the commands compile.
	goFiles := 0
	for _, dir := range []string{"cmd", "gen/go", "internal", "pkg"} {
		err := filepath.WalkDir(filepath.Join(root, filepath.FromSlash(dir)), func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && d.Name() == "testdata" {
				return fs.SkipDir
			}
			if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			goFiles++
			if rules.excluded(filepath.ToSlash(rel)) {
				t.Errorf("%s leaves out %s, which the go build compiles", sourceDockerignorePath, filepath.ToSlash(rel))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if goFiles == 0 {
		t.Fatal("found no Go file to check")
	}
	for _, file := range []string{
		"LICENSE", "go.mod", "go.sum", "ui/package.json", "ui/bun.lock", "ui/bunfig.toml", "ui/src/lib/server/examples.ts",
		"examples/each-fan-out.ork.yaml",
	} {
		if rules.excluded(file) {
			t.Errorf("%s leaves out %s, which the Dockerfile copies", sourceDockerignorePath, file)
		}
	}
	for _, file := range []string{
		".git/HEAD", ".env", "compose.yaml", ".claude/settings.json", "proof/x/proof.json", ".data/x", "bin/ork",
		"dist/bin/linux-amd64/ork", "docs/README.md", "images/activity-shell/Dockerfile", "site/package.json",
		"scripts/quickstart-smoke.sh", "test/fixtures/bash/Dockerfile", "deploy/Dockerfile",
		"cmd/ork/main_test.go", "internal/dist/compose_test.go", "internal/engine/testdata/history/x.json",
		"gen/ts/x.ts", "examples/README.md", "examples/custom-image/main.go",
		"ui/node_modules/svelte/package.json", "ui/.svelte-kit/tsconfig.json", "ui/build/index.js",
		"ui/e2e/harness.ts", "ui/test/preload.ts", "ui/test-results/x", "ui/playwright-report/index.html",
	} {
		if !rules.excluded(file) {
			t.Errorf("%s admits %s, which no build step reads", sourceDockerignorePath, file)
		}
	}
	// A new top-level path stays out of the context until someone admits it.
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		switch e.Name() {
		case "LICENSE", "go.mod", "go.sum", "cmd", "gen", "internal", "pkg", "examples", "ui":
			continue
		}
		file := e.Name()
		if e.IsDir() {
			file += "/file"
		}
		if !rules.excluded(file) {
			t.Errorf("%s admits the repository's %s, which no build step reads", sourceDockerignorePath, e.Name())
		}
	}
}

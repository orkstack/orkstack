package dist_test

import (
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"regexp"
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
// here, with the web UI built here and compiled into ork, instead of copied
// from `make dist`'s output. Its ui target is also what `make ui` exports
// for an ork built outside Docker, the release's included.

const (
	sourceDockerfilePath   = "deploy/source/Dockerfile"
	sourceDockerignorePath = "deploy/source/Dockerfile.dockerignore"
	// The stages the release has no counterpart for: the one that compiles
	// the commands, the one that builds the UI, and the UI's build alone.
	binariesStage = "binaries"
	uiBuildStage  = "ui-build"
	uiStage       = "ui"
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
	if extra, want := slices.Sorted(maps.Keys(byName)), sorted([]string{binariesStage, uiBuildStage, uiStage}); !slices.Equal(extra, want) {
		t.Errorf("%s's stages beyond the release's are %v, want only %v", sourceDockerfilePath, extra, want)
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

// TestSourceDockerfile_OrkEmbedsTheUIStagesBuild: the ui stage is the UI's
// build and nothing else, at its root; the binaries stage copies it into the
// directory internal/webui embeds before it compiles, so the quickstart's ork
// serves the real UI and never the placeholder; and the build's version is
// the commit's time whenever one is passed.
func TestSourceDockerfile_OrkEmbedsTheUIStagesBuild(t *testing.T) {
	t.Parallel()
	stages := sourceStages(t)
	build := stageNamed(t, stages, uiBuildStage)
	var workdir string
	epoch, buildStep := -1, -1
	for i, ins := range build.body {
		switch {
		case ins.cmd == "WORKDIR":
			workdir = ins.args
		case ins.cmd == "ARG" && ins.args == "SOURCE_DATE_EPOCH":
			epoch = i
		case ins.cmd == "RUN" && strings.Contains(ins.args, "bun run build"):
			buildStep = i
		}
	}
	if buildStep < 0 || epoch < 0 || epoch > buildStep {
		t.Errorf("the %s stage does not declare ARG SOURCE_DATE_EPOCH before `bun run build` (ARG at %d, build at %d): "+
			"the UI's version would be the clock's, and one commit would build different bytes", uiBuildStage, epoch, buildStep)
	}

	ui := stageNamed(t, stages, uiStage)
	if ui.image != "scratch" || len(ui.body) != 1 || ui.body[0].cmd != "COPY" || ui.body[0].flags["from"] != uiBuildStage {
		t.Fatalf("the %s stage is FROM %s with %d instruction(s); want FROM scratch and one COPY --from=%s of the build",
			uiStage, ui.image, len(ui.body), uiBuildStage)
	}
	// adapter-static writes build/ beside the UI's package.json.
	if sources, dest := copyOperands(ui.body[0]); len(sources) != 1 || path.Clean(sources[0]) != path.Join(workdir, "build") || dest != "/" {
		t.Errorf("the %s stage copies %v to %s, want %s, the UI's build, to /", uiStage, sources, dest, path.Join(workdir, "build"))
	}

	embed := regexp.MustCompile(`(?m)^//go:embed all:(\S+)$`).FindSubmatch(readRepoFile(t, "internal/webui/webui.go"))
	if embed == nil {
		t.Fatal("internal/webui/webui.go no longer embeds its UI build with //go:embed all:<dir>; update this test with it")
	}
	if want := "internal/webui/" + string(embed[1]); path.Dir(filepath.ToSlash(dist.UIIndex)) != want {
		t.Errorf("internal/webui embeds %s, but dist.UIIndex is %s", want, dist.UIIndex)
	}
	binaries := stageNamed(t, stages, binariesStage)
	var dir string
	copied, compiled := -1, -1
	for i, ins := range binaries.body {
		switch ins.cmd {
		case "WORKDIR":
			dir = ins.args
		case "RUN":
			compiled = i
		case "COPY":
			if ins.flags["from"] != uiStage {
				continue
			}
			sources, dest := copyOperands(ins)
			if !path.IsAbs(dest) {
				dest = path.Join(dir, dest)
			}
			if want := path.Join(dir, "internal/webui", string(embed[1])); len(sources) != 1 || sources[0] != "/" || dest != want {
				t.Errorf("the %s stage copies %v from %s to %s, want / to %s, where internal/webui embeds it",
					binariesStage, sources, uiStage, dest, want)
			}
			copied = i
		}
	}
	if copied < 0 || compiled < 0 || copied > compiled {
		t.Errorf("the %s stage does not copy the %s stage's build in before it compiles (copy at %d, go build at %d): "+
			"the quickstart's ork would serve the placeholder", binariesStage, uiStage, copied, compiled)
	}
}

// TestSourceDockerfile_UIBuildReadsTheExamples: the UI's gallery reads
// examples/ at build time, from outside ui/, so the ui-build stage copies
// them to where the UI's glob resolves.
func TestSourceDockerfile_UIBuildReadsTheExamples(t *testing.T) {
	t.Parallel()
	build := stageNamed(t, sourceStages(t), uiBuildStage)
	var workdir, examplesDest string
	var copiesUI bool
	buildStep := -1
	for i, ins := range build.body {
		switch ins.cmd {
		case "WORKDIR":
			workdir = ins.args
		case "COPY":
			sources, dest := copyOperands(ins)
			if len(sources) == 1 && sources[0] == "examples/" {
				examplesDest = path.Clean(dest)
				if buildStep >= 0 {
					t.Errorf("%s copies examples/ after it builds", uiBuildStage)
				}
			}
			if len(sources) == 1 && sources[0] == "ui/" {
				copiesUI = true
			}
		case "RUN":
			if strings.Contains(ins.args, "bun run build") {
				buildStep = i
			}
		}
	}
	if buildStep < 0 || !copiesUI || examplesDest == "" {
		t.Fatalf("%s must COPY ui/ and examples/ and then run `bun run build` (ui %v, examples %q, build step %d)",
			uiBuildStage, copiesUI, examplesDest, buildStep)
	}
	source := readRepoFile(t, "ui/src/lib/server/examples.ts")
	glob := regexp.MustCompile(`import\.meta\.glob\('([^']+)'`).FindSubmatch(source)
	if glob == nil {
		t.Fatal("ui/src/lib/server/examples.ts no longer globs the examples; update this test with it")
	}
	// Vite resolves the glob against the file that holds it.
	resolved := path.Dir(path.Join(workdir, "src/lib/server", string(glob[1])))
	if resolved != examplesDest {
		t.Errorf("the UI globs %s from %s, so it reads %s, but %s copies the examples to %s: "+
			"the gallery would be empty", glob[1], "src/lib/server/examples.ts", resolved, uiBuildStage, examplesDest)
	}
}

func TestSourceDockerfile_InstallsFromTheLockfile(t *testing.T) {
	t.Parallel()
	installs := 0
	for _, s := range sourceStages(t) {
		for _, run := range s.all("RUN") {
			if strings.Contains(run.args, "bun install") {
				installs++
				if !strings.Contains(run.args, "--frozen-lockfile") {
					t.Errorf("stage %s: %q can change the dependencies; install with --frozen-lockfile", s.name, run.args)
				}
			}
		}
	}
	if installs == 0 {
		t.Error("no stage installs the UI's dependencies")
	}
}

// makeRule returns a Makefile rule's prerequisites and its recipe, the
// tab-indented lines under it joined, continuation lines included.
func makeRule(t *testing.T, makefile, target string) (prerequisites []string, recipe string) {
	t.Helper()
	lines := strings.Split(makefile, "\n")
	header := regexp.MustCompile(`^` + regexp.QuoteMeta(target) + `:([^=].*)?$`)
	for i, line := range lines {
		m := header.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		deps, _, _ := strings.Cut(m[1], "##")
		var body []string
		for _, next := range lines[i+1:] {
			if !strings.HasPrefix(next, "\t") {
				break
			}
			body = append(body, strings.TrimSpace(next))
		}
		return strings.Fields(deps), strings.Join(body, "\n")
	}
	t.Fatalf("the Makefile has no rule for %s", target)
	return nil, ""
}

// TestMakeUI_ExportsTheSourceDockerfilesUIStage: `make ui` builds the UI as
// the quickstart's ork does, in the source Dockerfile's ui stage on the
// pinned bun, stamped with the commit's time, and writes it where
// internal/webui embeds it; `make dist` runs it first, so a release's ork is
// compiled with it (and dist.Build refuses a tree without it).
func TestMakeUI_ExportsTheSourceDockerfilesUIStage(t *testing.T) {
	t.Parallel()
	makefile := string(readRepoFile(t, "Makefile"))
	_, recipe := makeRule(t, makefile, "ui")
	for _, want := range []string{
		"docker buildx build", "-f " + sourceDockerfilePath, "--target " + uiStage, "type=local,dest=",
		"--build-arg SOURCE_DATE_EPOCH=$(SOURCE_DATE_EPOCH)", path.Dir(dist.UIIndex), "index.html", ".gitkeep",
	} {
		if !strings.Contains(recipe, want) {
			t.Errorf("make ui does not say %q:\n%s", want, recipe)
		}
	}
	if deps, _ := makeRule(t, makefile, "dist"); !slices.Contains(deps, "ui") {
		t.Errorf("make dist depends on %v, not ui: a release's ork would be compiled without the UI", deps)
	}
}

// TestSourceDockerignore_AdmitsWhatTheBuildReadsAndNothingElse: the context
// holds the Go module's sources, the UI's and the examples, and never a
// secret, the git directory or what a build or a test left behind, a UI
// build included.
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
		"examples/each-fan-out.ork.yaml", "internal/webui/placeholder/index.html",
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
		"ui/node_modules/svelte/package.json", "ui/.svelte-kit/tsconfig.json", "ui/build/index.html",
		"ui/e2e/harness.ts", "ui/test/preload.ts", "ui/test-results/x", "ui/playwright-report/index.html",
		// A UI build `make ui` left in the checkout: ork embeds the ui stage's.
		"internal/webui/dist/index.html", "internal/webui/dist/_app/immutable/entry/start.js",
		"internal/webui/dist/.gitkeep",
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

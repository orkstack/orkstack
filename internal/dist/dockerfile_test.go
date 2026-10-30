package dist_test

import (
	"encoding/json"
	"maps"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dist"
	"github.com/orkstack/orkstack/internal/dsl"
)

// The release images are described by deploy/Dockerfile, one target each. These
// tests hold that file to the release contract (docs/DEPLOY.md, "Running the
// published images"): what it publishes, what runs as root, what it copies and
// how it is pinned. They read the committed Dockerfile, so an edit that breaks
// the contract fails here, in the unit gate, and not in a release.

const (
	dockerfilePath   = "deploy/Dockerfile"
	dockerignorePath = "deploy/Dockerfile.dockerignore"
	// sourceRepository is the repository the images say they were built from;
	// ghcr.io links a package to a repository by this label.
	sourceRepository = "https://github.com/orkstack/orkstack"
)

// dfInstruction is one Dockerfile instruction, continuation lines joined.
type dfInstruction struct {
	line  int
	cmd   string            // upper-case: COPY, RUN, USER
	flags map[string]string // leading --name=value flags
	args  string            // the rest, trimmed
}

// dfStage is one FROM and the instructions after it.
type dfStage struct {
	name     string
	image    string // as written after FROM
	platform string // the --platform flag, "" when there is none
	line     int
	body     []dfInstruction
}

// logicalLines joins a Dockerfile's continuation lines and drops comments and
// blank lines, remembering the line each instruction starts on.
func logicalLines(text string) (lines []string, numbers []int) {
	var (
		current strings.Builder
		start   int
	)
	for i, raw := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(raw)
		if strings.HasPrefix(trimmed, "#") || trimmed == "" {
			continue
		}
		if current.Len() == 0 {
			start = i + 1
		}
		if strings.HasSuffix(trimmed, `\`) {
			current.WriteString(strings.TrimSuffix(trimmed, `\`))
			current.WriteByte(' ')
			continue
		}
		current.WriteString(trimmed)
		lines, numbers = append(lines, current.String()), append(numbers, start)
		current.Reset()
	}
	return lines, numbers
}

// parseInstruction splits a logical line into command, flags and arguments.
func parseInstruction(line string, number int) dfInstruction {
	cmd, rest, _ := strings.Cut(line, " ")
	ins := dfInstruction{line: number, cmd: strings.ToUpper(cmd), flags: map[string]string{}}
	rest = strings.TrimSpace(rest)
	for strings.HasPrefix(rest, "--") {
		flag, after, _ := strings.Cut(rest, " ")
		name, value, _ := strings.Cut(strings.TrimPrefix(flag, "--"), "=")
		ins.flags[name] = value
		rest = strings.TrimSpace(after)
	}
	ins.args = rest
	return ins
}

func parseDockerfile(t *testing.T, name string, data []byte) []dfStage {
	t.Helper()
	var stages []dfStage
	lines, numbers := logicalLines(string(data))
	for i, line := range lines {
		ins := parseInstruction(line, numbers[i])
		if ins.cmd == "FROM" {
			fields := strings.Fields(ins.args)
			stage := dfStage{line: ins.line, platform: ins.flags["platform"]}
			switch {
			case len(fields) == 1:
				stage.image = fields[0]
			case len(fields) == 3 && strings.EqualFold(fields[1], "AS"):
				stage.image, stage.name = fields[0], fields[2]
			default:
				t.Fatalf("%s:%d: cannot read %q", name, ins.line, line)
			}
			stages = append(stages, stage)
			continue
		}
		if len(stages) == 0 {
			continue // ARG before the first FROM
		}
		stages[len(stages)-1].body = append(stages[len(stages)-1].body, ins)
	}
	if len(stages) == 0 {
		t.Fatalf("%s has no FROM", name)
	}
	return stages
}

func dockerfileStages(t *testing.T) []dfStage {
	t.Helper()
	return parseDockerfile(t, dockerfilePath, readRepoFile(t, dockerfilePath))
}

func stageNamed(t *testing.T, stages []dfStage, name string) dfStage {
	t.Helper()
	for _, s := range stages {
		if s.name == name {
			return s
		}
	}
	t.Fatalf("no stage %q among the Dockerfile's %d", name, len(stages))
	return dfStage{}
}

func (s dfStage) all(cmd string) []dfInstruction {
	var out []dfInstruction
	for _, ins := range s.body {
		if ins.cmd == cmd {
			out = append(out, ins)
		}
	}
	return out
}

// stageSources is the set of stages another stage builds on or copies from:
// the helpers. Every other stage is a target a release builds.
func stageSources(stages []dfStage) map[string]bool {
	used := map[string]bool{}
	for _, s := range stages {
		used[s.image] = true
		for _, ins := range s.all("COPY") {
			if from := ins.flags["from"]; from != "" {
				used[from] = true
			}
		}
	}
	return used
}

// publishedStages are the stages no other stage uses, by name.
func publishedStages(t *testing.T, stages []dfStage) []dfStage {
	t.Helper()
	used := stageSources(stages)
	var published []dfStage
	for _, s := range stages {
		if s.name == "" {
			t.Fatalf("%s:%d: a stage without a name cannot be a --target", dockerfilePath, s.line)
		}
		if !used[s.name] {
			published = append(published, s)
		}
	}
	return published
}

// externalImages are the stages whose base is an image pull: not an earlier
// stage, and not scratch, Docker's empty base, which pulls nothing.
func externalImages(stages []dfStage) []dfStage {
	earlier := map[string]bool{}
	var out []dfStage
	for _, s := range stages {
		if !earlier[s.image] && s.image != "scratch" {
			out = append(out, s)
		}
		if s.name != "" {
			earlier[s.name] = true
		}
	}
	return out
}

// entrypointOf is the argv of a stage's one exec-form ENTRYPOINT.
func entrypointOf(t *testing.T, s dfStage) []string {
	t.Helper()
	entrypoints := s.all("ENTRYPOINT")
	if len(entrypoints) != 1 {
		t.Fatalf("target %s has %d ENTRYPOINT instructions, want 1", s.name, len(entrypoints))
	}
	argv, ok := jsonArray(entrypoints[0].args)
	if !ok {
		t.Fatalf("target %s: ENTRYPOINT %s is not a JSON array", s.name, entrypoints[0].args)
	}
	return argv
}

func jsonArray(args string) ([]string, bool) {
	var out []string
	if !strings.HasPrefix(args, "[") || json.Unmarshal([]byte(args), &out) != nil {
		return nil, false
	}
	return out, true
}

// parseLabels reads the key="value" pairs of LABEL instructions.
var labelPair = regexp.MustCompile(`([A-Za-z0-9._-]+)=(?:"([^"]*)"|(\S+))`)

func stageLabels(s dfStage) map[string]string {
	labels := map[string]string{}
	for _, ins := range s.all("LABEL") {
		for _, m := range labelPair.FindAllStringSubmatch(ins.args, -1) {
			labels[m[1]] = m[2] + m[3]
		}
	}
	return labels
}

func sorted(values []string) []string {
	out := slices.Clone(values)
	sort.Strings(out)
	return out
}

// linuxArchitectures are the architectures the release builds linux binaries
// for, in the words Docker's TARGETARCH uses.
func linuxArchitectures() []string {
	var arches []string
	for _, target := range dist.Targets() {
		if target.GOOS == "linux" && !slices.Contains(arches, target.GOARCH) {
			arches = append(arches, target.GOARCH)
		}
	}
	return arches
}

func TestReleaseDockerfile_TargetsAreTheThreePublishedImages(t *testing.T) {
	t.Parallel()
	stages := dockerfileStages(t)
	seen := map[string]bool{}
	for _, s := range stages {
		if seen[s.name] {
			t.Errorf("%s: two stages are named %q", dockerfilePath, s.name)
		}
		seen[s.name] = true
	}
	var names []string
	for _, s := range publishedStages(t, stages) {
		names = append(names, s.name)
		if len(s.all("ENTRYPOINT")) != 1 {
			t.Errorf("target %s has %d ENTRYPOINT instructions, want 1", s.name, len(s.all("ENTRYPOINT")))
		}
	}
	if got, want := sorted(names), sorted(dist.ServiceImages()); !slices.Equal(got, want) {
		t.Errorf("the stages no other stage uses are %q, want the published images %q "+
			"(every other stage must be a source another stage copies from)", got, want)
	}
}

func TestReleaseDockerfile_EveryStageIsDigestPinned(t *testing.T) {
	t.Parallel()
	expectBasesPinned(t, dockerfilePath, dockerfileStages(t))
}

// expectBasesPinned fails for every stage of a Dockerfile whose base is an
// image pull not pinned by digest. The digest is the pin; the tag is kept so a
// reader and Dependabot know which series the digest was taken from.
// Distroless publishes no versioned tag, so :latest and :nonroot are those
// labels.
func expectBasesPinned(t *testing.T, name string, stages []dfStage) {
	t.Helper()
	pinned := regexp.MustCompile(`^[^\s@]+:[A-Za-z0-9_][A-Za-z0-9._-]{0,127}@sha256:[0-9a-f]{64}$`)
	bases := externalImages(stages)
	if len(bases) == 0 {
		t.Fatalf("%s: no stage pulls an image", name)
	}
	for _, s := range bases {
		if !pinned.MatchString(s.image) {
			t.Errorf("%s:%d: FROM %s is not name:tag@sha256:<64 hex>; a base is pinned by the digest of its "+
				"multi-arch index, with its tag kept for the reader", name, s.line, s.image)
			continue
		}
		ref, err := dsl.ParseReference(s.image)
		if err != nil || ref.Tag == "" || !dsl.ValidDigest(ref.Digest) {
			t.Errorf("%s:%d: FROM %s does not parse as a tagged, digest-pinned reference (%v)", name, s.line, s.image, err)
		}
	}
	// The pin regexp must not be vacuous.
	for _, bad := range []string{
		"alpine", "alpine:3.20", "alpine:latest", "alpine@sha256:" + strings.Repeat("a", 64),
		"alpine:3.20@sha256:" + strings.Repeat("A", 64), "alpine:3.20@sha256:" + strings.Repeat("a", 63),
		"alpine:3.20@sha512:" + strings.Repeat("a", 64),
	} {
		if pinned.MatchString(bad) {
			t.Errorf("the pin pattern accepts %q", bad)
		}
	}
}

// effectiveUser is the user a stage's image runs as: its last USER, else its
// base's default, which is root unless the base's tag says nonroot.
func effectiveUser(s dfStage) string {
	if users := s.all("USER"); len(users) > 0 {
		user, _, _ := strings.Cut(users[len(users)-1].args, ":")
		return user
	}
	if ref, err := dsl.ParseReference(s.image); err == nil && ref.Tag == "nonroot" {
		return "nonroot"
	}
	return "root"
}

func TestReleaseDockerfile_OnlyTheOrkerRunsAsRoot(t *testing.T) {
	t.Parallel()
	stages := dockerfileStages(t)
	want := map[string]string{"ork": "nonroot", "orkctl": "nonroot", "orker": "root"}
	published := publishedStages(t, stages)
	for _, s := range published {
		got, known := want[s.name]
		if !known {
			t.Errorf("target %s is not one the test knows a user for", s.name)
			continue
		}
		if user := effectiveUser(s); user != got {
			t.Errorf("target %s runs as %q, want %q", s.name, user, got)
		}
	}
	// ork and orkctl take the base's own nonroot user, not a USER of theirs.
	for _, name := range []string{"ork", "orkctl"} {
		s := stageNamed(t, stages, name)
		if ref, err := dsl.ParseReference(s.image); err != nil || ref.Tag != "nonroot" {
			t.Errorf("target %s is built on %s; want a base tagged :nonroot", name, s.image)
		}
		if len(s.all("USER")) != 0 {
			t.Errorf("target %s sets USER; its base's nonroot user is the contract", name)
		}
	}
	// The orker says it runs as root: that is a stated fact
	// (docs/DEPLOY.md), not the base image's accident.
	orker := stageNamed(t, stages, "orker").all("USER")
	if len(orker) != 1 || orker[0].args != "root" {
		t.Errorf("the orker target must state USER root exactly once: %v", orker)
	}
	roots := 0
	for _, s := range published {
		if u := effectiveUser(s); u == "root" || u == "0" {
			roots++
		}
	}
	if roots != 1 {
		t.Errorf("%d published images run as root, want only the orker", roots)
	}
}

func TestReleaseDockerfile_EntrypointsAreExecForm(t *testing.T) {
	t.Parallel()
	for _, s := range publishedStages(t, dockerfileStages(t)) {
		entrypoints := s.all("ENTRYPOINT")
		if len(entrypoints) != 1 {
			t.Errorf("target %s: %d ENTRYPOINT instructions", s.name, len(entrypoints))
			continue
		}
		argv, ok := jsonArray(entrypoints[0].args)
		if !ok || len(argv) == 0 || argv[0] == "" {
			t.Errorf("target %s: ENTRYPOINT %s is not a JSON array; a shell form would run the process "+
				"under sh -c, which does not forward SIGTERM (and a distroless image has no sh)", s.name, entrypoints[0].args)
		}
		for _, cmd := range s.all("CMD") {
			if _, ok := jsonArray(cmd.args); !ok {
				t.Errorf("target %s: CMD %s is not a JSON array", s.name, cmd.args)
			}
		}
	}
	for _, shell := range []string{`ENTRYPOINT /usr/local/bin/ork`, `ENTRYPOINT ["ork" "x"]`, `ENTRYPOINT []`} {
		if argv, ok := jsonArray(strings.TrimPrefix(shell, "ENTRYPOINT ")); ok && len(argv) > 0 {
			t.Errorf("the exec-form check accepts %q", shell)
		}
	}
}

func TestReleaseDockerfile_NoRunStepRunsOnTheTargetPlatform(t *testing.T) {
	t.Parallel()
	published := map[string]bool{}
	stages := dockerfileStages(t)
	for _, s := range publishedStages(t, stages) {
		published[s.name] = true
	}
	for _, s := range stages {
		runs := s.all("RUN")
		switch {
		case published[s.name] && (len(runs) != 0 || s.platform != ""):
			t.Errorf("target %s has RUN steps or a pinned platform (%q); a published image is assembled from "+
				"COPY alone, so no emulated step ever runs", s.name, s.platform)
		case len(runs) != 0 && s.platform != "$BUILDPLATFORM":
			t.Errorf("stage %s runs %d step(s) on the target platform (--platform=%q); a RUN under emulation is "+
				"slow and unreproducible, so it belongs on $BUILDPLATFORM", s.name, len(runs), s.platform)
		}
	}
}

// proofPin reads the single value of a setup action's version input from
// proof.yml.
func proofPin(t *testing.T, key string) string {
	t.Helper()
	pattern := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(key) + `:\s*"?([0-9][^"\s]*)"?\s*$`)
	values := map[string]bool{}
	for _, m := range pattern.FindAllSubmatch(readRepoFile(t, ".github/workflows/proof.yml"), -1) {
		values[string(m[1])] = true
	}
	if len(values) != 1 {
		t.Fatalf("proof.yml pins %s to %d different values (%v), want exactly one", key, len(values), values)
	}
	for value := range values {
		return value
	}
	return ""
}

// TestDockerfiles_ToolchainsMatchTheWorkflowPins: the release images carry
// no UI toolchain and no UI runtime, because `make dist` builds the UI first
// (make ui) and compiles it into ork, whose image copies that binary. The
// source build makes the UI with the bun the workflows build it with, and no
// image of either runs node: the UI is a static build that ork serves.
func TestDockerfiles_ToolchainsMatchTheWorkflowPins(t *testing.T) {
	t.Parallel()
	bun := proofPin(t, "bun-version")
	for _, file := range []struct {
		path   string
		stages []dfStage
		bun    int // the distinct bun images the file may pull
	}{
		{dockerfilePath, dockerfileStages(t), 0},
		{sourceDockerfilePath, sourceStages(t), 1},
	} {
		bunImages := map[string]bool{}
		for _, s := range externalImages(file.stages) {
			ref, err := dsl.ParseReference(s.image)
			if err != nil {
				t.Fatalf("%s:%d: %v", file.path, s.line, err)
			}
			switch ref.Repository {
			case "oven/bun":
				bunImages[s.image] = true
				if ref.Tag != bun {
					t.Errorf("%s:%d: the bun image is %s, but the workflows build the UI with bun %s (proof.yml bun-version)",
						file.path, s.line, ref.Tag, bun)
				}
			case "library/node":
				t.Errorf("%s:%d: stage %s runs %s; the UI is a static build that ork serves, so no image runs node",
					file.path, s.line, s.name, s.image)
			}
		}
		if len(bunImages) != file.bun {
			t.Errorf("%s pulls %d distinct bun image(s) %v, want %d", file.path, len(bunImages), slices.Sorted(maps.Keys(bunImages)), file.bun)
		}
	}
}

// copySources returns the source operands of a COPY, and its destination.
func copyOperands(ins dfInstruction) (sources []string, dest string) {
	fields := strings.Fields(ins.args)
	if len(fields) < 2 {
		return nil, ""
	}
	return fields[:len(fields)-1], fields[len(fields)-1]
}

// builtLinuxBinaries is what `make dist` writes for the images to copy: every
// path under the release directory's bin/, as dist.Build lays them out.
func builtLinuxBinaries(t *testing.T) []string {
	t.Helper()
	out := t.TempDir()
	buildInto(t, fakeRoot(t), out, &fakeBuilder{})
	var built []string
	err := filepath.WalkDir(filepath.Join(out, "bin"), func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(out, p)
		built = append(built, "dist/"+filepath.ToSlash(rel)) // the Makefile's DIST
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(built)
	return built
}

func TestReleaseDockerfile_CopiesTheDistBinaries(t *testing.T) {
	t.Parallel()
	stages := dockerfileStages(t)
	var copied []string
	for _, name := range []string{dist.Ork, dist.Orker, dist.Orkctl} {
		s := stageNamed(t, stages, name)
		if len(s.all("ARG")) == 0 || !strings.HasPrefix(s.all("ARG")[0].args, "TARGETARCH") {
			t.Errorf("target %s does not declare ARG TARGETARCH first, so ${TARGETARCH} is empty in its COPY", name)
		}
		var binaries int
		for _, ins := range s.all("COPY") {
			if ins.flags["from"] != "" {
				continue
			}
			sources, dest := copyOperands(ins)
			if len(sources) != 1 || !strings.HasPrefix(sources[0], "dist/bin/") {
				continue
			}
			binaries++
			if want := "dist/bin/linux-${TARGETARCH}/" + name; sources[0] != want {
				t.Errorf("target %s copies %s, want %s", name, sources[0], want)
			}
			if dest != "/usr/local/bin/"+name {
				t.Errorf("target %s copies the binary to %s, want /usr/local/bin/%s", name, dest, name)
			}
			if ins.flags["chmod"] != "0755" {
				t.Errorf("target %s copies the binary with --chmod=%q, want 0755", name, ins.flags["chmod"])
			}
			for _, arch := range linuxArchitectures() {
				copied = append(copied, strings.ReplaceAll(sources[0], "${TARGETARCH}", arch))
			}
			if entry := entrypointOf(t, s); len(entry) != 1 || entry[0] != dest {
				t.Errorf("target %s: ENTRYPOINT %v is not the binary it copied (%s)", name, entry, dest)
			}
		}
		if binaries != 1 {
			t.Errorf("target %s copies %d binaries from dist/bin, want exactly its own", name, binaries)
		}
		if len(s.all("RUN")) != 0 {
			t.Errorf("target %s builds something; the binaries are built once by `make dist`", name)
		}
	}
	// The images copy exactly the binaries `make dist` writes: no path is
	// misspelled, and none that dist writes is left out.
	if built := builtLinuxBinaries(t); !slices.Equal(sorted(copied), built) {
		t.Errorf("the images copy %q, but dist.Build writes %q", sorted(copied), built)
	}

	// Every published image carries the licence.
	for _, s := range publishedStages(t, stages) {
		found := false
		for _, ins := range s.all("COPY") {
			if sources, dest := copyOperands(ins); ins.flags["from"] == "" && len(sources) == 1 &&
				sources[0] == dist.LicenseFile && dest == "/usr/share/doc/orkstack/LICENSE" {
				found = true
			}
		}
		if !found {
			t.Errorf("target %s does not copy LICENSE to /usr/share/doc/orkstack/LICENSE", s.name)
		}
	}
}

func TestReleaseDockerfile_ImagesCarryTheSourceLabel(t *testing.T) {
	t.Parallel()
	module := regexp.MustCompile(`(?m)^module\s+(\S+)`).FindSubmatch(readRepoFile(t, "go.mod"))
	if module == nil || "https://"+string(module[1]) != sourceRepository {
		t.Fatalf("go.mod's module %q is not the repository %s the images name", module, sourceRepository)
	}
	for _, s := range publishedStages(t, dockerfileStages(t)) {
		var declared bool
		for _, arg := range s.all("ARG") {
			declared = declared || strings.HasPrefix(arg.args, "VERSION")
		}
		if !declared {
			t.Errorf("target %s does not declare ARG VERSION, so its version label would be empty", s.name)
		}
		labels := stageLabels(s)
		want := map[string]string{
			"org.opencontainers.image.source":   sourceRepository,
			"org.opencontainers.image.licenses": "MIT",
			"org.opencontainers.image.version":  "${VERSION}",
			"org.opencontainers.image.title":    s.name,
		}
		for key, value := range want {
			if labels[key] != value {
				t.Errorf("target %s: label %s = %q, want %q", s.name, key, labels[key], value)
			}
		}
		if labels["org.opencontainers.image.description"] == "" {
			t.Errorf("target %s has no org.opencontainers.image.description", s.name)
		}
	}
}

// ignoreRule is one line of a .dockerignore: a path pattern, possibly
// negated. A pattern is matched against whole paths and, as Docker does,
// against each parent directory of a path.
type ignoreRule struct {
	segments []string
	negate   bool
}

type ignoreRules []ignoreRule

func parseDockerignore(data []byte) ignoreRules {
	var rules ignoreRules
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		rule := ignoreRule{negate: strings.HasPrefix(line, "!")}
		pattern := path.Clean(strings.TrimPrefix(strings.TrimSpace(strings.TrimPrefix(line, "!")), "/"))
		if pattern == "." {
			continue
		}
		rule.segments = strings.Split(pattern, "/")
		rules = append(rules, rule)
	}
	return rules
}

func matchSegments(pattern, name []string) bool {
	if len(pattern) == 0 {
		return len(name) == 0
	}
	if pattern[0] == "**" {
		for skip := 0; skip <= len(name); skip++ {
			if matchSegments(pattern[1:], name[skip:]) {
				return true
			}
		}
		return false
	}
	if len(name) == 0 {
		return false
	}
	if ok, err := path.Match(pattern[0], name[0]); err != nil || !ok {
		return false
	}
	return matchSegments(pattern[1:], name[1:])
}

func (r ignoreRule) matchesOrParentMatches(file string) bool {
	parts := strings.Split(file, "/")
	for n := 1; n <= len(parts); n++ {
		if matchSegments(r.segments, parts[:n]) {
			return true
		}
	}
	return false
}

// excluded reports whether the build context leaves file out: the last rule
// that matches it decides.
func (rules ignoreRules) excluded(file string) bool {
	excluded := false
	for _, rule := range rules {
		if rule.matchesOrParentMatches(file) {
			excluded = !rule.negate
		}
	}
	return excluded
}

func TestDockerignoreMatcher(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		ignore   string
		file     string
		excluded bool
	}{
		{"nothing listed", "", "a/b", false},
		{"star leaves nothing", "*\n", "a", true},
		{"star covers a nested file through its parent", "*\n", "a/b/c", true},
		{"a later exception wins", "*\n!a\n", "a", false},
		{"an exception for a directory admits its files", "*\n!a/\n", "a/b/c", false},
		{"a later rule beats an earlier exception", "*\n!a\na/b\n", "a/b/c", true},
		{"a sibling stays out", "*\n!a\n", "b", true},
		{"a segment glob", "*\n!d/linux-*/x\n", "d/linux-amd64/x", false},
		{"and only the whole name", "*\n!d/linux-*/x\n", "d/linux-amd64/xy", true},
		{"and only that depth", "*\n!d/linux-*/x\n", "d/linux-amd64/sub/x", true},
		{"a star does not cross a slash", "*\n!e/*.y\n", "e/sub/f.y", true},
		{"a double star does", "*\n!e/**/*.y\n", "e/sub/deeper/f.y", false},
		{"a leading slash is the root", "/a\n", "a/b", true},
		{"a comment is not a rule", "# a\n", "a", false},
		{"a dot file matches a star", "*\n", ".env", true},
	} {
		if got := parseDockerignore([]byte(tc.ignore)).excluded(tc.file); got != tc.excluded {
			t.Errorf("%s: excluded(%q) = %v, want %v", tc.name, tc.file, got, tc.excluded)
		}
	}
}

func TestReleaseDockerignore_AdmitsOnlyWhatTheBuildCopies(t *testing.T) {
	t.Parallel()
	rules := parseDockerignore(readRepoFile(t, dockerignorePath))
	if len(rules) == 0 || rules[0].negate || strings.Join(rules[0].segments, "/") != "*" {
		t.Fatalf("%s must start with * so that nothing is in the context unless it is admitted", dockerignorePath)
	}

	// Everything the Dockerfile copies from the context is admitted.
	var admitted []string
	for _, s := range dockerfileStages(t) {
		for _, ins := range s.all("COPY") {
			if ins.flags["from"] != "" {
				continue
			}
			sources, _ := copyOperands(ins)
			for _, source := range sources {
				for _, arch := range linuxArchitectures() {
					source := strings.ReplaceAll(source, "${TARGETARCH}", arch)
					if strings.HasSuffix(source, "/") {
						t.Fatalf("the test cannot expand COPY source %q; teach it, or the ignore file is unchecked", source)
					}
					admitted = append(admitted, source)
				}
			}
		}
	}
	for _, file := range slices.Compact(sorted(admitted)) {
		if rules.excluded(file) {
			t.Errorf("%s leaves out %s, which the Dockerfile copies", dockerignorePath, file)
		}
	}
	for _, file := range []string{
		"dist/bin/linux-amd64/ork", "dist/bin/linux-arm64/orker", "dist/bin/linux-arm64/orkctl", "LICENSE",
	} {
		if rules.excluded(file) {
			t.Errorf("%s leaves out %s", dockerignorePath, file)
		}
	}

	// And nothing else is: not the source tree, not a secret, not a built
	// archive beside the binaries, not the UI, whose build is inside ork.
	for _, file := range []string{
		".git/HEAD", ".env", "compose.yaml", ".claude/settings.json", "proof/x/proof.json", ".data/x",
		"go.mod", "cmd/ork/main.go", "internal/dist/archive.go", "docs/README.md", "images/shell/Dockerfile",
		"site/package.json", "scripts/quickstart-smoke.sh", "deploy/source/Dockerfile", "test/fixtures/bash/Dockerfile",
		"dist/orkctl-linux-amd64.tar.gz", "dist/SHA256SUMS", "dist/meta/images.tsv", "dist/layouts/ork/index.json",
		"dist/bin/windows-amd64/orkctl.exe", "dist/bin/linux-amd64/orkctl.exe", "dist/bin/linux-amd64/extra",
		"examples/README.md", "examples/each-fan-out.ork.yaml", "examples/automation/examples.ork.yaml",
		"examples/custom-image/main.go", "ui/package.json", "ui/bun.lock", "ui/src/lib/server/examples.ts",
		"ui/node_modules/svelte/package.json", "ui/.svelte-kit/tsconfig.json", "ui/build/index.html",
		"ui/e2e/harness.ts", "ui/test/preload.ts", "ui/test-results/x", "ui/playwright-report/index.html",
		"internal/webui/dist/index.html",
	} {
		if !rules.excluded(file) {
			t.Errorf("%s admits %s, which no build step copies", dockerignorePath, file)
		}
	}

	// A new top-level path stays out of the context until someone admits it.
	entries, err := os.ReadDir(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		switch e.Name() {
		case "LICENSE", "dist":
			continue
		}
		file := e.Name()
		if e.IsDir() {
			file += "/file"
		}
		if !rules.excluded(file) {
			t.Errorf("%s admits the repository's %s, which no build step copies", dockerignorePath, e.Name())
		}
	}
}

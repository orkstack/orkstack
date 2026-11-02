package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The proto contract is checked against the first release on every push and
// pull request: ci.yml's proto job checks out every tag and runs `make
// proto-breaking`, which runs buf breaking from the image deploy/buf-image.txt
// pins by digest, with no network, against the tree of the tag PROTO_BASE
// (v0.1.0), under buf.yaml's breaking rules.

// makesProtoBreaking matches a shell line that makes the proto-breaking
// target: `make`, any flags or variables, then the target as a word.
var makesProtoBreaking = regexp.MustCompile(`(?m)^\s*make(\s+\S+)*?\s+proto-breaking(\s|$)`)

// protoBreakingProblems is the rule for ci.yml's proto job.
func protoBreakingProblems(w *wfFile) []string {
	job, ok := w.Jobs["proto"]
	if !ok {
		return []string{"ci.yml has no proto job"}
	}
	var p []string
	if job.If != "" {
		p = append(p, "ci.yml's proto job is conditional: "+job.If)
	}
	if job.ContinueOnError != nil {
		p = append(p, "ci.yml's proto job continues on error, so a breaking change would not turn it red")
	}
	for i, s := range job.Steps {
		if s.ContinueOnError != nil {
			p = append(p, fmt.Sprintf("ci.yml's proto job %s continues on error", s.label(i)))
		}
	}
	checkout := stepIndex(job.Steps, isCheckout)
	if checkout < 0 || withString(job.Steps[checkout], "fetch-depth") != "0" {
		p = append(p, "the proto job does not check out with fetch-depth: 0, so it would see no tag to compare with")
	}
	// The step runs the target as a command, with whatever flags or variables
	// a contributor would give it; `make proto` (lint only) is not it.
	if at := stepIndex(job.Steps, func(s wfStep) bool { return makesProtoBreaking.MatchString(s.Run) }); at < 0 {
		p = append(p, "the proto job never runs make proto-breaking")
	} else if at < checkout {
		p = append(p, "the proto job runs make proto-breaking before its checkout")
	}
	return p
}

func TestWorkflows_CIChecksTheProtoContractAgainstTheFirstRelease(t *testing.T) {
	expectEachMutationRefused(t, "ci.yml", protoBreakingProblems, []mutation{
		{"no proto job", func(w *wfFile) { delete(w.Jobs, "proto") }},
		{"only by hand", func(w *wfFile) { w.Jobs["proto"].If = "${{ github.event_name == 'workflow_dispatch' }}" }},
		{"a shallow checkout", func(w *wfFile) {
			for i := range w.Jobs["proto"].Steps {
				if isCheckout(w.Jobs["proto"].Steps[i]) {
					w.Jobs["proto"].Steps[i].With["fetch-depth"] = 1
				}
			}
		}},
		{"a job that continues on error", func(w *wfFile) { w.Jobs["proto"].ContinueOnError = true }},
		{"the check continues on error", func(w *wfFile) {
			for i := range w.Jobs["proto"].Steps {
				if strings.Contains(w.Jobs["proto"].Steps[i].Run, "make proto-breaking") {
					w.Jobs["proto"].Steps[i].ContinueOnError = true
				}
			}
		}},
		{"lint only", func(w *wfFile) {
			for i := range w.Jobs["proto"].Steps {
				w.Jobs["proto"].Steps[i].Run = strings.ReplaceAll(w.Jobs["proto"].Steps[i].Run, "make proto-breaking", "make proto")
			}
		}},
	})
}

// bufPinned is the only shape deploy/buf-image.txt may name: one exact buf
// release, by its multi-platform index digest.
var bufPinned = regexp.MustCompile(`^bufbuild/buf:([0-9]+\.[0-9]+\.[0-9]+)@sha256:[0-9a-f]{64}$`)

// bufPin is the image deploy/buf-image.txt names: its first line that is not
// blank or a comment.
func bufPin(t *testing.T) string {
	t.Helper()
	for _, line := range strings.Split(readRepoFileForPins(t, "deploy/buf-image.txt"), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			return line
		}
	}
	return ""
}

// protoBreakingTag is the release the repository's `make proto-breaking`
// compares with, as its own dry run names it.
func protoBreakingTag(t *testing.T) string {
	t.Helper()
	res := runMake(t, "", mustRepoRoot(), nil, "-n", "-s", "proto-breaking")
	m := regexp.MustCompile(`git archive "refs/tags/(\S+)"`).FindStringSubmatch(res.out)
	if res.code != 0 || m == nil {
		t.Fatalf("make -n proto-breaking (exit %d) names no tag it compares with:\n%s", res.code, res.out)
	}
	return m[1]
}

// protoBreakingFixture is a throwaway repository standing for the project
// when `make proto-breaking` runs: the release tag v0.1.0 holds one proto
// file and a buf.yaml, and the working tree has since changed the buf.yaml
// and added another proto file. deploy/buf-image.txt is the repository's own.
func protoBreakingFixture(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo, "-c", "init.defaultBranch=main", "-c", "commit.gpgsign=false",
			"-c", "user.name=test", "-c", "user.email=test@example.com"}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	put := func(rel, body string) {
		t.Helper()
		write(t, filepath.Join(repo, filepath.FromSlash(rel)), body)
	}
	git("init", "-q")
	put("deploy/buf-image.txt", readRepoFileForPins(t, "deploy/buf-image.txt"))
	put("buf.yaml", "version: v2\n# at the release\n")
	put("proto/orkstack/v1/a.proto", "// at the release\n")
	git("add", "-A")
	git("commit", "-q", "-m", "release")
	git("tag", "v0.1.0")
	put("buf.yaml", "version: v2\n# in the working tree\n")
	put("proto/orkstack/v1/new.proto", "// not in the release\n")
	return repo
}

// bufDocker is a docker that records its arguments, one to a line and ended
// by a line of dashes, and what the directory mounted on /base holds, and then
// exits with $FAKE_DOCKER_EXIT.
const bufDocker = `for a in "$@"; do printf '%s\n' "$a"; done >>"$FAKE_LOG"
echo '---' >>"$FAKE_LOG"
for a in "$@"; do
	case "$a" in
	*:/base:ro) src="${a%:/base:ro}"; (cd "$src" && find . -type f | sort | sed 's/^/base-file /' && sed 's/^/base-buf.yaml /' buf.yaml) >>"$FAKE_LOG" ;;
	esac
done
exit "${FAKE_DOCKER_EXIT:-0}"
`

// TestMakefile_ProtoBreakingRunsThePinnedBufAgainstTheFirstRelease: the target
// CI runs compares the working tree with the release tag's own tree (what git
// exports at the tag, not what is checked out), through the pinned buf image,
// with no network; a breaking change fails it, and so does a clone without the
// tag, before any docker runs. The real recipe runs, with a docker that
// records how it was asked to run buf.
func TestMakefile_ProtoBreakingRunsThePinnedBufAgainstTheFirstRelease(t *testing.T) {
	pin := bufPin(t)
	if !bufPinned.MatchString(pin) {
		t.Fatalf("deploy/buf-image.txt names %q, not bufbuild/buf:X.Y.Z@sha256:<64 hex>", pin)
	}
	if !strings.Contains(readRepoFileForPins(t, "buf.yaml"), "breaking:") {
		t.Error("buf.yaml names no breaking rules")
	}

	// The release the repository compares with is a release tag, and a clone
	// that has tags at all has that one.
	tag := protoBreakingTag(t)
	if !regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(tag) {
		t.Errorf("make proto-breaking compares with %q, not a release tag", tag)
	}
	if tags, err := exec.Command("git", "-C", mustRepoRoot(), "tag", "-l").Output(); err == nil && strings.TrimSpace(string(tags)) != "" &&
		!slices.Contains(strings.Fields(string(tags)), tag) {
		t.Errorf("make proto-breaking compares with %s, which this clone does not have (it has %s)", tag, strings.Join(strings.Fields(string(tags)), " "))
	}

	run := func(t *testing.T, env []string, args ...string) (res makeResult, log string) {
		t.Helper()
		repo := protoBreakingFixture(t)
		bin := t.TempDir()
		stubTool(t, bin, "docker", bufDocker)
		logPath := filepath.Join(t.TempDir(), "docker.log")
		res = runMake(t, "", repo, append([]string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "GOBIN=" + bin, "FAKE_LOG=" + logPath}, env...),
			append([]string{"proto-breaking"}, args...)...)
		b, _ := os.ReadFile(logPath)
		return res, string(b)
	}

	t.Run("compares with the tag's tree through the pinned image, offline", func(t *testing.T) {
		res, log := run(t, nil)
		if res.code != 0 {
			t.Fatalf("exit %d:\n%s", res.code, res.out)
		}
		args := strings.Split(strings.SplitN(log, "---\n", 2)[0], "\n")
		args = args[:len(args)-1]
		if n := len(args); n < 4 || !slices.Equal(args[n-4:], []string{pin, "breaking", "--against", "/base"}) {
			t.Errorf("docker was asked %q, want it to end with the pinned image and `breaking --against /base`", args)
		}
		if i := slices.Index(args, "--network"); i < 0 || args[i+1] != "none" {
			t.Errorf("buf was run with a network: %q", args)
		}
		if !slices.ContainsFunc(args, func(a string) bool { return strings.HasSuffix(a, ":/work:ro") }) {
			t.Errorf("the working tree is not mounted read-only: %q", args)
		}
		base := map[string]bool{}
		for _, l := range strings.Split(log, "\n") {
			if f, ok := strings.CutPrefix(l, "base-file "); ok {
				base[f] = true
			}
		}
		if !base["./proto/orkstack/v1/a.proto"] || !base["./buf.yaml"] || base["./proto/orkstack/v1/new.proto"] {
			t.Errorf("the base holds %v, want the tag's tree: its buf.yaml and proto, not the working tree's added file", base)
		}
		if !strings.Contains(log, "base-buf.yaml # at the release") || strings.Contains(log, "in the working tree") {
			t.Errorf("the base's buf.yaml is not the tag's:\n%s", log)
		}
	})

	t.Run("a breaking change fails it", func(t *testing.T) {
		res, _ := run(t, []string{"FAKE_DOCKER_EXIT=1"})
		if res.code == 0 {
			t.Errorf("buf found a breaking change and make exited 0:\n%s", res.out)
		}
		if strings.Contains(res.out, "keeps "+tag) {
			t.Errorf("a failed check said the contract is kept:\n%s", res.out)
		}
	})

	t.Run("a clone without the tag fails before docker runs", func(t *testing.T) {
		res, log := run(t, nil, "PROTO_BASE=v9.9.9")
		if res.code == 0 || !strings.Contains(res.out, "v9.9.9") {
			t.Errorf("a missing tag: exit %d, output %q; want a failure naming it", res.code, res.out)
		}
		if log != "" {
			t.Errorf("docker ran without a base to compare with:\n%s", log)
		}
	})
}

// A deploy/buf-image.txt that names no image fails the target, rather than
// running docker with an empty image.
func TestMakefile_ProtoBreakingFailsWhenNoImageIsPinned(t *testing.T) {
	repo := protoBreakingFixture(t)
	write(t, filepath.Join(repo, "deploy", "buf-image.txt"), "# no image\n\n")
	bin := t.TempDir()
	stubTool(t, bin, "docker", bufDocker)
	logPath := filepath.Join(t.TempDir(), "docker.log")
	res := runMake(t, "", repo, []string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "GOBIN=" + bin, "FAKE_LOG=" + logPath}, "proto-breaking")
	if res.code == 0 {
		t.Errorf("exit 0 with no image pinned:\n%s", res.out)
	}
	if _, err := os.Stat(logPath); err == nil {
		t.Error("docker ran with no image pinned")
	}
}

// toolsFixture is a throwaway directory standing for the repository when
// `make tools` runs: the two files whose pins it compares, and a go that
// records `go install`.
func toolsFixture(t *testing.T, imagePin, packageJSON string) (dir string, run func(env ...string) (makeResult, string)) {
	t.Helper()
	dir = t.TempDir()
	write(t, filepath.Join(dir, "deploy", "buf-image.txt"), imagePin)
	write(t, filepath.Join(dir, "ui", "package.json"), packageJSON)
	bin := t.TempDir()
	logPath := filepath.Join(bin, "go.log")
	stubTool(t, bin, "go", `[ "$1" = env ] && exit 0
echo "$*" >>"`+logPath+`"
`)
	return dir, func(env ...string) (makeResult, string) {
		res := runMake(t, "", dir, append([]string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "GOBIN=" + bin, "HOME=" + dir}, env...), "tools")
		b, _ := os.ReadFile(logPath)
		return res, string(b)
	}
}

// TestMakefile_ToolsInstallsTheBufTheImagePins: the buf `make tools` puts on
// PATH, which `make proto` lints and generates with, is the release
// deploy/buf-image.txt pins for `make proto-breaking`, never @latest; and it
// refuses, installing nothing, when the UI's package.json pins another buf
// (the buf `make tsgen` runs). The real recipe runs, over the repository's own
// two files and over variants of them, with a go that records its installs.
func TestMakefile_ToolsInstallsTheBufTheImagePins(t *testing.T) {
	m := bufPinned.FindStringSubmatch(bufPin(t))
	if m == nil {
		t.Fatalf("deploy/buf-image.txt names %q, not bufbuild/buf:X.Y.Z@sha256:<64 hex>", bufPin(t))
	}
	version := m[1]
	image, pkg := readRepoFileForPins(t, "deploy/buf-image.txt"), readRepoFileForPins(t, "ui/package.json")
	if !strings.Contains(pkg, `"@bufbuild/buf": "`+version+`"`) {
		t.Fatalf("ui/package.json does not pin @bufbuild/buf %s, the release deploy/buf-image.txt pins", version)
	}

	t.Run("the repository's own pins agree and buf is installed at the image's release", func(t *testing.T) {
		_, run := toolsFixture(t, image, pkg)
		res, installs := run()
		if res.code != 0 {
			t.Fatalf("make tools refused the repository's own pins (exit %d):\n%s", res.code, res.out)
		}
		if want := "install github.com/bufbuild/buf/cmd/buf@v" + version + "\n"; installs != want {
			t.Errorf("go was asked %q, want exactly %q", installs, want)
		}
	})
	t.Run("a UI that pins another buf is refused, with nothing installed", func(t *testing.T) {
		_, run := toolsFixture(t, image, strings.Replace(pkg, `"@bufbuild/buf": "`+version+`"`, `"@bufbuild/buf": "0.0.1"`, 1))
		res, installs := run()
		if res.code == 0 {
			t.Errorf("make tools accepted a UI pinning another buf:\n%s", res.out)
		}
		for _, v := range []string{"0.0.1", version} {
			if !strings.Contains(res.out, v) {
				t.Errorf("the refusal does not name %s:\n%s", v, res.out)
			}
		}
		if installs != "" {
			t.Errorf("go installed %q after a refusal", installs)
		}
	})
	t.Run("an image file naming no buf is refused, with nothing installed", func(t *testing.T) {
		_, run := toolsFixture(t, "# no image\n", pkg)
		res, installs := run()
		if res.code == 0 || installs != "" {
			t.Errorf("exit %d, installs %q; want a refusal that installs nothing:\n%s", res.code, installs, res.out)
		}
	})
}

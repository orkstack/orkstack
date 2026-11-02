package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// Static analysis and the vulnerability scan (AGENTS.md §11.1).
//
// staticcheck is part of `make lint`, so `make check`, `make merge-check` and
// the lint gate of every proof run it, with its default checks, twice: as
// built and with the integration tag the integration suites are vetted with.
// govulncheck is not: it reads the vulnerability database over the network, and
// a network failure must never fail a proof, so `make vuln` is a job of ci.yml
// and of nightly.yml and appears in no proof gate. The scan's other two lanes,
// `make vuln-js` (bun audit over the UI's and the site's lockfiles) and
// `make vuln-images` (the pinned Grype over the release images), read advisory
// databases over the network too, and are jobs of nightly.yml alone. Each rule
// here returns its problems, so a companion test shows that it bites on a
// weakened copy.

// The rules run the Makefile: its dry runs (`make -n`) name the commands a
// target would run once its variables are expanded, and `make vuln-js` and
// `make vuln-images` run for real with a bun and a docker that record how they
// were called (the scan's network is theirs to stand in for). Each rule takes
// the path of the Makefile to run, so that a weakened copy can be held to it.

// makeDryRun is what `make -n -s target` prints with mk, and whether the
// target exists.
func makeDryRun(t *testing.T, mk, target string, vars ...string) (string, bool) {
	t.Helper()
	// A recipe line that names $(MAKE) runs under -n, and the image scan's makes its
	// database directory: a directory of the test's own, not the checkout's.
	vars = append(vars, "VULN_IMAGES_DIR="+t.TempDir())
	res := runMake(t, mk, mustRepoRoot(), nil, append([]string{"-n", "-s", target}, vars...)...)
	if res.code != 0 && strings.Contains(res.out, "No rule to make target") {
		return "", false
	}
	if res.code != 0 {
		t.Fatalf("make -n %s: exit %d\n%s", target, res.code, res.out)
	}
	return res.out, true
}

// hasCommandLine reports whether out has a line that is exactly command.
func hasCommandLine(out, command string) bool {
	for _, l := range strings.Split(out, "\n") {
		if strings.TrimSpace(l) == command {
			return true
		}
	}
	return false
}

// lintProblems holds `make lint` and `make vuln` to the rules above, by the
// commands they print.
func lintProblems(t *testing.T, mk string) []string {
	t.Helper()
	var p []string
	lint, ok := makeDryRun(t, mk, "lint")
	if !ok {
		return []string{"the Makefile has no lint target"}
	}
	for _, want := range []string{"go vet ./...", "go tool staticcheck ./...", "go tool staticcheck -tags=integration ./..."} {
		if !hasCommandLine(lint, want) {
			p = append(p, fmt.Sprintf("make lint does not run %q", want))
		}
	}
	// A pinned tool is run through `go tool`, never from a PATH install or a
	// floating `go run pkg@latest`.
	if strings.Contains(lint, "@latest") || strings.Contains(lint, "go run honnef.co") {
		p = append(p, "make lint runs staticcheck other than through the go.mod tool directive")
	}
	if strings.Contains(lint, "staticcheck.conf") || strings.Contains(lint, "-checks") {
		p = append(p, "make lint changes staticcheck's check set; it runs the default checks")
	}
	if vuln, ok := makeDryRun(t, mk, "vuln"); !ok || !hasCommandLine(vuln, "go tool govulncheck ./...") {
		p = append(p, "make vuln does not run `go tool govulncheck ./...`")
	}
	return p
}

// fastPathProblems: nothing a proof or the fast path runs may reach the
// network database, whatever the targets it is made of print.
func fastPathProblems(t *testing.T, mk string) []string {
	t.Helper()
	var p []string
	for _, target := range []string{"lint", "check", "merge-check", "test", "build"} {
		out, ok := makeDryRun(t, mk, target)
		if !ok {
			continue
		}
		for _, banned := range []string{"govulncheck", "bun audit", "grype"} {
			if strings.Contains(out, banned) {
				p = append(p, fmt.Sprintf("make %s runs the vulnerability scan (%s), which needs the network", target, banned))
			}
		}
	}
	return p
}

// stubBunScript records `<dir> <args>` of each call to $FAKE_BUN_LOG, and
// fails the audits $FAKE_BUN_FAIL names: `<dir>:high` for the audit that fails
// on a high or critical advisory, `<dir>:all` for the listing of every one.
const stubBunScript = `echo "$(basename "$PWD") $*" >>"$FAKE_BUN_LOG"
mode=all
case " $* " in *" --audit-level=high "*) mode=high ;; esac
case " $FAKE_BUN_FAIL " in *" $(basename "$PWD"):$mode "*) echo "bun audit: advisories" >&2; exit 1 ;; esac
exit 0
`

// vulnJSProblems runs `make vuln-js` with a bun that records its calls: it
// audits both the UI's and the site's lockfiles, lists every advisory, fails
// on a high or critical one of either, with nothing left out of the audit
// (no --production, --omit or --ignore), and a listing that fails alone does
// not fail it.
func vulnJSProblems(t *testing.T, mk string) []string {
	t.Helper()
	run := func(fail string) (makeResult, []string) {
		bin, log := t.TempDir(), filepath.Join(t.TempDir(), "bun.log")
		stubTool(t, bin, "bun", stubBunScript)
		res := runMake(t, mk, mustRepoRoot(), []string{"GOBIN=" + bin, "PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
			"FAKE_BUN_LOG=" + log, "FAKE_BUN_FAIL=" + fail}, "vuln-js")
		b, _ := os.ReadFile(log)
		var calls []string
		for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if l != "" {
				calls = append(calls, l)
			}
		}
		return res, calls
	}
	res, calls := run("")
	if res.code != 0 {
		return []string{fmt.Sprintf("make vuln-js fails with no advisory (exit %d): %s", res.code, lastLine(res.out))}
	}
	var p []string
	want := []string{"site audit", "site audit --audit-level=high", "ui audit", "ui audit --audit-level=high"}
	if got := sorted(calls); !slices.Equal(got, want) {
		p = append(p, fmt.Sprintf("make vuln-js ran bun as %q, want %q: both lockfiles, every advisory listed, high and critical failing, nothing left out", got, want))
	}
	for _, dir := range []string{"ui", "site"} {
		res, _ := run(dir + ":high")
		switch named := namedAfterIn(res.out); {
		case res.code == 0:
			p = append(p, fmt.Sprintf("make vuln-js passes a high or critical advisory in %s/", dir))
		case !slices.Equal(named, []string{dir}):
			p = append(p, fmt.Sprintf("make vuln-js fails a high advisory in %s/ naming %q, want it alone", dir, named))
		}
	}
	if res, _ := run("ui:all site:all"); res.code != 0 {
		p = append(p, "make vuln-js fails when only the listing of every advisory fails, not the audit that fails the run")
	}
	return p
}

// lastLine is the last line of a target's output that make did not write: what
// the recipe said as it failed.
func lastLine(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if !strings.HasPrefix(lines[i], "make: ***") {
			return lines[i]
		}
	}
	return ""
}

// namedAfterIn is the words a failing scan target names after "in:" in its
// last line, up to a parenthesis: the lanes or images that failed.
func namedAfterIn(out string) []string {
	_, after, ok := strings.Cut(lastLine(out), " in:")
	if !ok {
		return nil
	}
	after, _, _ = strings.Cut(after, " (")
	return strings.Fields(after)
}

// stubScanDocker records each call, one argument to a line and ended by a line
// of dashes, to $FAKE_LOG, and fails the scan of the layout
// $FAKE_DOCKER_FAIL names.
const stubScanDocker = `for a in "$@"; do printf '%s\n' "$a"; last="$a"; done >>"$FAKE_LOG"
echo '---' >>"$FAKE_LOG"
case "$last" in *"/layouts/${FAKE_DOCKER_FAIL:-no-such-layout}") exit 1 ;; esac
exit 0
`

// scanCalls parses stubScanDocker's log into the arguments of each call.
func scanCalls(log string) [][]string {
	var calls [][]string
	for _, block := range strings.Split(log, "---\n") {
		if block = strings.TrimSuffix(block, "\n"); block != "" {
			calls = append(calls, strings.Split(block, "\n"))
		}
	}
	return calls
}

// makeVar is the value of a Makefile variable under mk.
func makeVar(t *testing.T, mk, name string) []string {
	t.Helper()
	res := runMake(t, mk, mustRepoRoot(), nil, "-s", "--no-print-directory", "--eval=print-%: ; @echo $($*)", "print-"+name)
	if res.code != 0 {
		t.Fatalf("printing %s: exit %d\n%s", name, res.code, res.out)
	}
	return strings.Fields(res.out)
}

// scannedImages is what `make vuln-images` should scan under mk: the release
// images and every activity images/Makefile builds.
func scannedImages(t *testing.T, mk string) []string {
	t.Helper()
	want := makeVar(t, mk, "RELEASE_IMAGES")
	for _, a := range makeVar(t, mk, "VULN_ACTIVITIES") {
		want = append(want, "activity-"+a)
	}
	return sorted(want)
}

// vulnImagesProblems runs `make vuln-images` over layouts already built
// (LAYOUTS=) with a docker that records its calls: it scans every release
// image and every activity once, each by the pinned Grype and the policy file,
// with no capability, no new privilege and a read-only root, keeping Grype's
// database and scratch under VULN_IMAGES_DIR; it fails on a failed scan, after
// scanning the rest, and on an images/Makefile with no activity. Its build
// step, which needs the network, is read from the dry run.
func vulnImagesProblems(t *testing.T, mk string) []string {
	t.Helper()
	root := mustRepoRoot()
	pin := ""
	for _, line := range strings.Split(readRepoFile(t, "deploy/grype-image.txt"), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			pin = line
			break
		}
	}
	run := func(fail string, extra ...string) (makeResult, [][]string, string, string) {
		bin, work := t.TempDir(), t.TempDir()
		log := filepath.Join(work, "docker.log")
		layouts := filepath.Join(work, "layouts")
		if err := os.MkdirAll(layouts, 0o755); err != nil {
			t.Fatal(err)
		}
		stubTool(t, bin, "docker", stubScanDocker)
		args := append([]string{"vuln-images", "LAYOUTS=" + layouts, "VULN_IMAGES_DIR=" + filepath.Join(work, "scan")}, extra...)
		res := runMake(t, mk, root, []string{"GOBIN=" + bin, "PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
			"FAKE_LOG=" + log, "FAKE_DOCKER_FAIL=" + fail}, args...)
		b, _ := os.ReadFile(log)
		return res, scanCalls(string(b)), layouts, filepath.Join(work, "scan")
	}
	res, calls, layouts, scan := run("")
	if res.code != 0 {
		return []string{fmt.Sprintf("make vuln-images fails with no vulnerability (exit %d): %s", res.code, lastLine(res.out))}
	}
	var p []string
	var scanned []string
	for _, c := range calls {
		scanned = append(scanned, strings.TrimPrefix(c[len(c)-1], "oci-dir:/layouts/"))
		for _, need := range [][]string{{"--cap-drop=ALL"}, {"--security-opt", "no-new-privileges"}, {"--read-only"}, {"-c", "/grype.yaml"},
			{"--platform", "linux/amd64"}, {"-e", "GRYPE_DB_CACHE_DIR=/db"}, {"-e", "TMPDIR=/scratch"},
			{"-v", scan + "/db:/db"}, {"-v", scan + "/tmp:/scratch"}, {"-v", layouts + ":/layouts:ro"}, {"-v", root + "/deploy/grype.yaml:/grype.yaml:ro"}} {
			found := false
			for i, a := range c {
				if a == need[0] && (len(need) == 1 || (i+1 < len(c) && c[i+1] == need[1])) {
					found = true
				}
			}
			if !found {
				p = append(p, fmt.Sprintf("make vuln-images scans %s without %s", c[len(c)-1], strings.Join(need, " ")))
			}
		}
		if i := slices.Index(c, "-c"); i < 1 || c[i-1] != pin {
			p = append(p, fmt.Sprintf("make vuln-images scans %s with something other than the Grype image deploy/grype-image.txt pins", c[len(c)-1]))
		}
	}
	if want := scannedImages(t, mk); !slices.Equal(sorted(scanned), want) {
		p = append(p, fmt.Sprintf("make vuln-images scanned %q, want each of %q once: the release images and every activity", sorted(scanned), want))
	}

	// A scan that fails fails the target, after the rest were scanned.
	failing := "activity-" + makeVar(t, mk, "VULN_ACTIVITIES")[0]
	if res, calls, _, _ := run(failing); res.code == 0 {
		p = append(p, "make vuln-images passes a scan that failed")
	} else {
		if named := namedAfterIn(res.out); !slices.Equal(named, []string{failing}) {
			p = append(p, fmt.Sprintf("make vuln-images names %q for the scan that failed, want %q alone", named, failing))
		}
		if len(calls) != len(scanned) {
			p = append(p, fmt.Sprintf("make vuln-images stopped after %d of %d scans when one failed", len(calls), len(scanned)))
		}
	}
	// An images/Makefile that names no activity is a scan of nothing.
	if res, calls, _, _ := run("", "VULN_ACTIVITIES="); res.code == 0 || len(calls) != 0 {
		p = append(p, fmt.Sprintf("make vuln-images passes when it has no activity to scan (exit %d, %d scans)", res.code, len(calls)))
	}

	// What needs the network is read from the dry run: it builds what a
	// release publishes, for the platform scanned, and keeps Grype's database,
	// about 3 GB, in the checkout and never under a /tmp that may be a tmpfs.
	dry, ok := makeDryRun(t, mk, "vuln-images")
	switch {
	case !ok:
		p = append(p, "the Makefile has no vuln-images target")
	case !regexp.MustCompile(`(?m)release-images\b.*VERSION=v0\.0\.0\b.*PLATFORMS=linux/amd64|release-images\b.*PLATFORMS=linux/amd64.*VERSION=v0\.0\.0\b`).MatchString(dry):
		p = append(p, "make vuln-images does not build the images a release publishes (release-images, v0.0.0, linux/amd64)")
	}
	if dir := makeVar(t, mk, "VULN_IMAGES_DIR"); len(dir) != 1 || !strings.HasPrefix(dir[0], root+"/") {
		p = append(p, fmt.Sprintf("make vuln-images keeps Grype's database in %q, outside the checkout %s", dir, root))
	}
	return p
}

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// staticAnalysisProblems is every rule of the Makefile, run over the Makefile
// at mk, by the kinds named (all of them when none is).
func staticAnalysisProblems(t *testing.T, mk string, kinds ...string) []string {
	t.Helper()
	has := func(k string) bool { return len(kinds) == 0 || slices.Contains(kinds, k) }
	var p []string
	if has("lint") {
		p = append(p, lintProblems(t, mk)...)
	}
	if has("fastpath") {
		p = append(p, fastPathProblems(t, mk)...)
	}
	if has("js") {
		p = append(p, vulnJSProblems(t, mk)...)
	}
	if has("images") {
		p = append(p, vulnImagesProblems(t, mk)...)
	}
	return p
}

func TestMakefile_LintRunsStaticcheckAndNothingOnTheNetwork(t *testing.T) {
	root := mustRepoRoot()
	mk := readRepoFile(t, "Makefile")
	if p := staticAnalysisProblems(t, filepath.Join(root, "Makefile")); len(p) > 0 {
		t.Fatalf("Makefile:\n  %s", strings.Join(p, "\n  "))
	}
	// Each weakening of the Makefile is held to the rules of the kind it
	// weakens: a copy of the Makefile is written and run.
	for _, c := range []struct{ name, kind, from, to string }{
		{"the build-tagged run dropped", "lint", "\tgo tool staticcheck -tags=integration ./...\n", ""},
		{"the plain run dropped", "lint", "\tgo tool staticcheck ./...\n", ""},
		{"an unpinned staticcheck", "lint", "\tgo tool staticcheck ./...\n", "\tgo run honnef.co/go/tools/cmd/staticcheck@latest ./...\n"},
		{"a narrowed check set", "lint", "\tgo tool staticcheck ./...\n", "\tgo tool staticcheck -checks=SA4006 ./...\n"},
		{"the vulnerability scan inside lint", "fastpath", "\tgo tool staticcheck ./...\n", "\tgo tool staticcheck ./...\n\tgo tool govulncheck ./...\n"},
		{"the vulnerability scan dropped", "lint", "\tgo tool govulncheck ./...\n", "\ttrue\n"},
		{"merge-check running the scan", "fastpath", "merge-check: build lint test ", "merge-check: build lint test vuln "},
		{"merge-check running the image scan", "fastpath", "merge-check: build lint test ", "merge-check: build lint test vuln-images "},
		{"the JavaScript audit dropped", "js", "\n.PHONY: vuln-js\nvuln-js:", "\n.PHONY: vuln-jsx\nvuln-jsx:"},
		{"the site not audited", "js", "for d in ui site; do", "for d in ui; do"},
		{"the audit failing on nothing", "js", "(cd $$d && bun audit --audit-level=high) || failed=", "(cd $$d && bun audit --audit-level=high) || true; failed="},
		{"the audit failing only on critical", "js", "bun audit --audit-level=high", "bun audit --audit-level=critical"},
		{"the audit without dev dependencies", "js", "(cd $$d && bun audit --audit-level=high)", "(cd $$d && bun audit --audit-level=high --production)"},
		{"the image scan dropped", "images", "\n.PHONY: vuln-images\nvuln-images:", "\n.PHONY: vuln-imagez\nvuln-imagez:"},
		{"an unpinned scanner", "images", "grype=\"$$(grep -v '^#' deploy/grype-image.txt | head -1)\"", "grype=anchore/grype:latest"},
		{"the scan without its policy", "images", `"$$grype" -c /grype.yaml `, `"$$grype" `},
		{"a failed image passed", "images", `|| failed="$$failed $$t"`, "|| true"},
		{"layouts that are not the release's", "images", "$(MAKE) release-images VERSION=v0.0.0", "$(MAKE) -C images oci VERSION=v0.0.0"},
		{"the audit exiting 0", "js", `names the fixed versions)" >&2; exit 1; fi`, `names the fixed versions)" >&2; exit 0; fi`},
		{"the image scan exiting 0", "images", `a scan that could not run, in:$$failed" >&2; exit 1; fi`, `a scan that could not run, in:$$failed" >&2; fi`},
		{"an empty activity list passing", "images", `[ -n '$(strip $(VULN_ACTIVITIES))' ] || {`, `true || {`},
		{"the database under /tmp", "images", "VULN_IMAGES_DIR ?= $(CURDIR)/.data/vuln-images", "VULN_IMAGES_DIR ?= /tmp/vuln-images"},
		{"the database dropped from the checkout", "images", "VULN_IMAGES_DIR ?= $(CURDIR)/.data/vuln-images", "VULN_IMAGES_DIR ?= $(TMPDIR)/vuln-images"},
		{"the scanner's scratch under /tmp", "images", "-v '$(VULN_IMAGES_DIR)/tmp:/scratch'", "-v /tmp:/scratch"},
		{"the scanner's database under /tmp", "images", "-v '$(VULN_IMAGES_DIR)/db:/db'", "-v /tmp/grype-db:/db"},
		{"a scanner with its capabilities", "images", "--cap-drop=ALL --security-opt no-new-privileges --read-only ", ""},
	} {
		if !strings.Contains(mk, c.from) {
			t.Fatalf("the mutation %q finds nothing to change in the Makefile", c.name)
		}
		weakened := filepath.Join(t.TempDir(), "Makefile")
		// A target's prerequisites ride its first line, which its recipe starts with.
		if err := os.WriteFile(weakened, []byte(strings.Replace(mk, c.from, c.to, 1)), 0o644); err != nil {
			t.Fatal(err)
		}
		if len(staticAnalysisProblems(t, weakened, c.kind)) == 0 {
			t.Errorf("the rule accepts the weakening %q", c.name)
		}
	}
}

// scanJobProblems: a lane of the vulnerability scan is a job of its own, named
// for its make target, that runs `make <job>`, with nothing that lets an
// advisory pass.
func scanJobProblems(file, name string) func(*wfFile) []string {
	return func(w *wfFile) []string {
		job, ok := w.Jobs[name]
		if !ok {
			return []string{file + " has no " + name + " job"}
		}
		var p []string
		if job.ContinueOnError != nil {
			p = append(p, fmt.Sprintf("%s: jobs.%s continues on error, so an advisory would not fail the run", file, name))
		}
		ran := false
		for i, s := range job.Steps {
			if s.ContinueOnError != nil {
				p = append(p, fmt.Sprintf("%s: jobs.%s %s continues on error", file, name, s.label(i)))
			}
			if strings.TrimSpace(s.Run) == "make "+name {
				ran = true
				if s.If != "" {
					p = append(p, fmt.Sprintf("%s: jobs.%s %s is conditional, so the scan can be skipped", file, name, s.label(i)))
				}
			}
		}
		if !ran {
			p = append(p, fmt.Sprintf("%s: jobs.%s does not run `make %s`", file, name, name))
		}
		return p
	}
}

// scanJobMutations are the weakenings every scan job's rule refuses.
func scanJobMutations(name string) []mutation {
	eachRun := func(w *wfFile, edit func(s *wfStep)) {
		for i := range w.Jobs[name].Steps {
			if w.Jobs[name].Steps[i].Run != "" {
				edit(&w.Jobs[name].Steps[i])
			}
		}
	}
	return []mutation{
		{"no " + name + " job", func(w *wfFile) { delete(w.Jobs, name) }},
		{"a job that continues on error", func(w *wfFile) { w.Jobs[name].ContinueOnError = true }},
		{"a step that continues on error", func(w *wfFile) { eachRun(w, func(s *wfStep) { s.ContinueOnError = true }) }},
		{"a skippable scan step", func(w *wfFile) { eachRun(w, func(s *wfStep) { s.If = "false" }) }},
		{"a job that runs something else", func(w *wfFile) { eachRun(w, func(s *wfStep) { s.Run = "make lint" }) }},
	}
}

func TestWorkflows_TheVulnerabilityScanRunsInCIAndNightly(t *testing.T) {
	for _, file := range []string{"ci.yml", "nightly.yml"} {
		expectEachMutationRefused(t, file, scanJobProblems(file, "vuln"), scanJobMutations("vuln"))
	}
}

// TestWorkflows_TheNightlyScansJavaScriptAndImages: the vulnerability scan's
// JavaScript and image lanes are jobs of nightly.yml that run every night the
// nightly does, beside the proof and whatever its result, like `vuln`.
func TestWorkflows_TheNightlyScansJavaScriptAndImages(t *testing.T) {
	var releaseBuildKit string
	for _, job := range loadWorkflow(t, "release.yml").Jobs {
		for _, s := range job.Steps {
			if strings.HasPrefix(s.Uses, "docker/setup-buildx-action@") {
				releaseBuildKit = withString(s, "driver-opts")
			}
		}
	}
	if !strings.Contains(releaseBuildKit, "@sha256:") {
		t.Fatalf("release.yml runs BuildKit by driver-opts %q, not by digest", releaseBuildKit)
	}
	buildx := func(w *wfFile) *wfStep {
		for i := range w.Jobs["vuln-images"].Steps {
			if strings.HasPrefix(w.Jobs["vuln-images"].Steps[i].Uses, "docker/setup-buildx-action@") {
				return &w.Jobs["vuln-images"].Steps[i]
			}
		}
		return &wfStep{}
	}
	for _, name := range []string{"vuln-js", "vuln-images"} {
		rule := func(w *wfFile) []string {
			p := scanJobProblems("nightly.yml", name)(w)
			job, ok := w.Jobs[name]
			if !ok {
				return p
			}
			if len(job.needs()) > 0 {
				p = append(p, fmt.Sprintf("jobs.%s.needs = %v; it runs beside the proof, whatever the proof's result", name, job.needs()))
			}
			if nightly, ok := w.Jobs["nightly"]; !ok || normalizeIf(job.If) != normalizeIf(nightly.If) {
				p = append(p, fmt.Sprintf("jobs.%s.if = %q, want the nightly's own, so it runs every night the nightly does", name, job.If))
			}
			if name == "vuln-images" {
				// The builder is release.yml's, pinned by digest: Dependabot
				// sees neither, so one can never move without the other.
				var have string
				for _, s := range job.Steps {
					if strings.HasPrefix(s.Uses, "docker/setup-buildx-action@") {
						have = withString(s, "driver-opts")
					}
				}
				if have == "" || have != releaseBuildKit {
					p = append(p, fmt.Sprintf("jobs.vuln-images runs BuildKit by driver-opts %q, and release.yml by %q", have, releaseBuildKit))
				}
			}
			return p
		}
		cases := append(scanJobMutations(name),
			mutation{"waiting for the proof", func(w *wfFile) { w.Jobs[name].Needs = "nightly" }},
			mutation{"by dispatch only", func(w *wfFile) { w.Jobs[name].If = "${{ github.event_name == 'workflow_dispatch' }}" }},
		)
		if name == "vuln-images" {
			cases = append(cases,
				mutation{"the default BuildKit", func(w *wfFile) { delete(buildx(w).With, "driver-opts") }},
				mutation{"BuildKit by tag", func(w *wfFile) { buildx(w).With["driver-opts"] = "image=moby/buildkit:buildx-stable-1" }},
			)
		}
		expectEachMutationRefused(t, "nightly.yml", rule, cases)
	}
}

// TestVulnImages_ScansEveryImageAReleasePublishes: make vuln-images scans the
// release images and every activity images/Makefile builds, as its
// print-activities names them (the scan itself is run by
// TestMakefile_LintRunsStaticcheckAndNothingOnTheNetwork's rule), and
// images/Makefile prints, so builds, every activity directory, so neither a
// new activity nor a renamed one can narrow the scan in silence.
func TestVulnImages_ScansEveryImageAReleasePublishes(t *testing.T) {
	root := mustRepoRoot()
	res := runMake(t, filepath.Join(root, "images", "Makefile"), filepath.Join(root, "images"), nil, "-s", "--no-print-directory", "print-activities")
	if res.code != 0 {
		t.Fatalf("make print-activities: exit %d\n%s", res.code, res.out)
	}
	var built []string
	for _, a := range strings.Fields(res.out) {
		built = append(built, "activity-"+a)
	}
	dockerfiles, err := filepath.Glob(filepath.Join(root, "images", "activity-*", "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	var dirs []string
	for _, d := range dockerfiles {
		dirs = append(dirs, filepath.Base(filepath.Dir(d)))
	}
	if len(built) == 0 || !slices.Equal(sorted(built), sorted(dirs)) {
		t.Errorf("images/ holds the activities %q, and images/Makefile prints, builds, and make vuln-images scans, %q", dirs, built)
	}
	// What the scan is made of, expanded: the release images, then those.
	scan := scannedImages(t, filepath.Join(root, "Makefile"))
	for _, b := range built {
		if !slices.Contains(scan, b) {
			t.Errorf("make vuln-images does not scan %s: it scans %q", b, scan)
		}
	}
	if len(scan) <= len(built) {
		t.Errorf("make vuln-images scans %q, none of them a release image", scan)
	}
}

// grypeConfig is deploy/grype.yaml as the policy tests read it, with no key
// beyond these: Grype would ignore a misspelt one, and an ignore entry's
// include-aliases, namespace, fix-state, match-type, VEX keys, or a package's
// language or upstream-name would widen or bypass the entry.
type grypeConfig struct {
	OnlyFixed         bool   `yaml:"only-fixed"`
	FailOn            string `yaml:"fail-on-severity"`
	CheckForAppUpdate *bool  `yaml:"check-for-app-update"`
	DB                struct {
		RequireUpdateCheck bool `yaml:"require-update-check"`
	} `yaml:"db"`
	Ignore []grypeIgnore `yaml:"ignore"`
}

type grypeIgnore struct {
	Vulnerability string             `yaml:"vulnerability"`
	Reason        string             `yaml:"reason"`
	Package       grypeIgnorePackage `yaml:"package"`
}

type grypeIgnorePackage struct {
	Name     string `yaml:"name"`
	Version  string `yaml:"version"`
	Type     string `yaml:"type"`
	Location string `yaml:"location"`
}

// setsAside reports whether the entry has a set-aside's shape: every Go module
// of the binary at one location, and nothing else. Every other entry is a
// waiver.
func (ig grypeIgnore) setsAside() bool {
	return ig == grypeIgnore{Package: grypeIgnorePackage{Type: "go-module", Location: ig.Package.Location}}
}

func decodeGrypeConfig(src string) (grypeConfig, error) {
	var cfg grypeConfig
	dec := yaml.NewDecoder(strings.NewReader(src))
	dec.KnownFields(true)
	err := dec.Decode(&cfg)
	return cfg, err
}

// TestVulnImages_TheScannerAndItsPolicyArePinned: deploy/grype-image.txt names
// one Grype release by its index digest, and deploy/grype.yaml, read with no
// unknown key, fails on a fixed vulnerability of high or critical severity and
// sets aside exactly the Go modules of OrkStack's own binaries, at the paths
// deploy/Dockerfile copies them to: govulncheck judges those, by reachability,
// and every other binary an image carries is the scan's. Its waivers are
// TestVulnImages_EveryWaiverIsScopedReasonedAndDated's.
func TestVulnImages_TheScannerAndItsPolicyArePinned(t *testing.T) {
	var pin string
	for _, line := range strings.Split(readRepoFile(t, "deploy/grype-image.txt"), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			pin = line
			break
		}
	}
	if !regexp.MustCompile(`^ghcr\.io/anchore/grype:v[0-9]+\.[0-9]+\.[0-9]+@sha256:[0-9a-f]{64}$`).MatchString(pin) {
		t.Errorf("deploy/grype-image.txt names %q, not ghcr.io/anchore/grype:vX.Y.Z@sha256:<64 hex>", pin)
	}
	var own []string
	for _, m := range regexp.MustCompile(`(?m)^COPY --chmod=0755 dist/bin/linux-\$\{TARGETARCH\}/[a-z]+ (\S+)$`).FindAllStringSubmatch(readRepoFile(t, "deploy/Dockerfile"), -1) {
		own = append(own, m[1])
	}
	if len(own) == 0 {
		t.Fatal("deploy/Dockerfile copies no OrkStack binary: the policy check is vacuous")
	}
	policy := func(src string) []string {
		cfg, err := decodeGrypeConfig(src)
		if err != nil {
			return []string{"deploy/grype.yaml: " + err.Error()}
		}
		var p []string
		if !cfg.OnlyFixed || cfg.FailOn != "high" {
			p = append(p, fmt.Sprintf("deploy/grype.yaml has only-fixed %v and fail-on-severity %q, want true and \"high\"", cfg.OnlyFixed, cfg.FailOn))
		}
		if cfg.CheckForAppUpdate == nil || *cfg.CheckForAppUpdate {
			p = append(p, "deploy/grype.yaml lets the pinned Grype look for a newer release of itself")
		}
		if !cfg.DB.RequireUpdateCheck {
			p = append(p, "deploy/grype.yaml lets a scan judge the images by a database it could not check for an update")
		}
		var ignored []string
		for _, ig := range cfg.Ignore {
			if ig.setsAside() {
				ignored = append(ignored, ig.Package.Location)
			}
		}
		sort.Strings(ignored)
		want := append([]string(nil), own...)
		sort.Strings(want)
		if strings.Join(ignored, " ") != strings.Join(want, " ") {
			p = append(p, fmt.Sprintf("deploy/grype.yaml sets aside the Go modules at %q, and deploy/Dockerfile copies OrkStack's binaries to %q", ignored, want))
		}
		return p
	}
	src := readRepoFile(t, "deploy/grype.yaml")
	if p := policy(src); len(p) > 0 {
		t.Fatalf("\n  %s", strings.Join(p, "\n  "))
	}
	for _, c := range []struct{ name, from, to string }{
		{"failing only on critical", "fail-on-severity: high", "fail-on-severity: critical"},
		{"a misspelt key Grype would ignore", "fail-on-severity: high", "fail-on: high"},
		{"no fail at all", "fail-on-severity: high\n", ""},
		{"the docker CLI set aside", "location: /usr/local/bin/orker\n", "location: /usr/local/bin/docker\n"},
		{"every Go module set aside", "      location: /usr/local/bin/ork\n", ""},
		{"an OS package set aside", "type: go-module\n      location: /usr/local/bin/orkctl", "type: apk\n      location: /usr/local/bin/orkctl"},
		{"a set-aside narrowed to one module", "type: go-module\n      location: /usr/local/bin/orkctl", "name: golang.org/x/net\n      type: go-module\n      location: /usr/local/bin/orkctl"},
		{"a self-update check", "check-for-app-update: false", "check-for-app-update: true"},
		{"a database never checked", "require-update-check: true", "require-update-check: false"},
	} {
		if !strings.Contains(src, c.from) {
			t.Fatalf("the mutation %q finds nothing to change in deploy/grype.yaml", c.name)
		}
		if len(policy(strings.Replace(src, c.from, c.to, 1))) == 0 {
			t.Errorf("the rule accepts the weakening %q", c.name)
		}
	}
}

// grypeWaiverProblems: every entry of deploy/grype.yaml's ignore list that is
// not a set-aside is a waiver, approved by the user, and a waiver sets aside
// one vulnerability in one package of one binary, for a stated reason, until a
// stated date. It names the vulnerability by its GHSA or CVE id and the
// package by its exact name, version, type and location: Grype reads a name
// holding a regular-expression character as a pattern and a location as a
// glob, either of which would widen it. Its reason opens "review-by
// YYYY-MM-DD: " and says why; the date is at most 30 days after today, and
// once it has passed the waiver is refused, as an expired quarantine entry
// stops excusing.
func grypeWaiverProblems(src string, today time.Time) []string {
	cfg, err := decodeGrypeConfig(src)
	if err != nil {
		return []string{"deploy/grype.yaml: " + err.Error()}
	}
	advisory := regexp.MustCompile(`^(GHSA-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}|CVE-[0-9]{4}-[0-9]{4,})$`)
	review := regexp.MustCompile(`^review-by ([0-9]{4}-[0-9]{2}-[0-9]{2}): \S`)
	var p []string
	for _, ig := range cfg.Ignore {
		if ig.setsAside() {
			continue
		}
		pk := ig.Package
		where := fmt.Sprintf("deploy/grype.yaml's waiver of %q in %q", ig.Vulnerability, pk.Name)
		if !advisory.MatchString(ig.Vulnerability) {
			p = append(p, where+" names no GHSA or CVE id")
		}
		if pk.Name == "" || pk.Version == "" || pk.Type == "" || pk.Location == "" {
			p = append(p, fmt.Sprintf("%s does not name one package (name %q, version %q, type %q, location %q)", where, pk.Name, pk.Version, pk.Type, pk.Location))
		}
		if strings.ContainsAny(pk.Name, `^$*+?[](){}|\`) || strings.ContainsAny(pk.Location, `*?[]{}\`) {
			p = append(p, fmt.Sprintf("%s names the package by a pattern (name %q, location %q), not exactly", where, pk.Name, pk.Location))
		}
		if pk.Location != "" && !strings.HasPrefix(pk.Location, "/") {
			p = append(p, fmt.Sprintf("%s: location %q is not a path in the image", where, pk.Location))
		}
		m := review.FindStringSubmatch(ig.Reason)
		if m == nil {
			p = append(p, fmt.Sprintf("%s has the reason %q, which does not open \"review-by YYYY-MM-DD: \" and say why", where, ig.Reason))
			continue
		}
		by, err := time.Parse(time.DateOnly, m[1])
		switch {
		case err != nil:
			p = append(p, fmt.Sprintf("%s: review-by %q is not a date", where, m[1]))
		case today.After(by):
			p = append(p, fmt.Sprintf("%s was to be reviewed by %s, which has passed: remove it, or renew it with the user's approval", where, m[1]))
		case by.Sub(today) > 30*24*time.Hour:
			p = append(p, fmt.Sprintf("%s: review-by %s is more than 30 days after today", where, m[1]))
		}
	}
	return p
}

func TestVulnImages_EveryWaiverIsScopedReasonedAndDated(t *testing.T) {
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if p := grypeWaiverProblems(readRepoFile(t, "deploy/grype.yaml"), today); len(p) > 0 {
		t.Fatalf("\n  %s", strings.Join(p, "\n  "))
	}
	day := func(n int) string { return today.AddDate(0, 0, n).Format(time.DateOnly) }
	waiver := "ignore:\n" +
		"  - vulnerability: GHSA-abcd-efgh-ijkl\n" +
		"    reason: \"review-by " + day(10) + ": the fix is not released yet\"\n" +
		"    package:\n" +
		"      name: example.com/mod/v2\n" +
		"      version: v2.1.0\n" +
		"      type: go-module\n" +
		"      location: /usr/local/bin/tool\n"
	for _, c := range []struct{ name, from, to string }{
		{"the waiver as written", "", ""},
		{"a review the day it is due", day(10), day(0)},
		{"a review 30 days ahead", day(10), day(30)},
		{"a CVE id", "GHSA-abcd-efgh-ijkl", "CVE-2026-12345"},
	} {
		if p := grypeWaiverProblems(strings.Replace(waiver, c.from, c.to, 1), today); len(p) > 0 {
			t.Errorf("the rule refuses %s:\n  %s", c.name, strings.Join(p, "\n  "))
		}
	}
	for _, c := range []struct{ name, from, to string }{
		{"no reason", "    reason: \"review-by " + day(10) + ": the fix is not released yet\"\n", ""},
		{"a reason with no review date", "review-by " + day(10) + ": ", ""},
		{"a review date with no reason", ": the fix is not released yet", ":"},
		{"a review date that is no date", "review-by " + day(10), "review-by 2026-02-30"},
		{"a review date that has passed", day(10), day(-1)},
		{"a review date more than 30 days ahead", day(10), day(31)},
		{"no vulnerability", "  - vulnerability: GHSA-abcd-efgh-ijkl\n    reason", "  - reason"},
		{"a vulnerability that is no advisory id", "GHSA-abcd-efgh-ijkl", "go-jose"},
		{"no package", "    package:\n      name: example.com/mod/v2\n      version: v2.1.0\n      type: go-module\n      location: /usr/local/bin/tool\n", ""},
		{"no package name", "      name: example.com/mod/v2\n", ""},
		{"no package version", "      version: v2.1.0\n", ""},
		{"no package type", "      type: go-module\n", ""},
		{"no package location", "      location: /usr/local/bin/tool\n", ""},
		{"a package name Grype reads as a pattern", "name: example.com/mod/v2", "name: example.com/.*"},
		{"a location Grype reads as a glob", "location: /usr/local/bin/tool", "location: /usr/local/bin/*"},
		{"a relative location", "location: /usr/local/bin/tool", "location: usr/local/bin/tool"},
		{"the vulnerability's aliases too", "    package:\n", "    include-aliases: true\n    package:\n"},
		{"a whole namespace", "    package:\n", "    namespace: github:language:go\n    package:\n"},
	} {
		if !strings.Contains(waiver, c.from) {
			t.Fatalf("the mutation %q finds nothing to change in the waiver", c.name)
		}
		if len(grypeWaiverProblems(strings.Replace(waiver, c.from, c.to, 1), today)) == 0 {
			t.Errorf("the rule accepts the weakening %q", c.name)
		}
	}
}

// proofScanProblems: no proof gate reads the vulnerability database.
func proofScanProblems(files map[string]string) []string {
	var p []string
	for name, text := range files {
		for _, banned := range []string{"govulncheck", "make vuln", "bun audit", "grype"} {
			if strings.Contains(text, banned) {
				p = append(p, fmt.Sprintf("%s mentions %q: a network failure would fail a proof", name, banned))
			}
		}
	}
	return p
}

func TestProofs_NeverRunTheVulnerabilityScan(t *testing.T) {
	files := map[string]string{
		"proof.yml": workflowFile(t, "proof.yml"),
	}
	scripts, err := filepath.Glob(filepath.Join("..", "..", "..", "scripts", "*.sh"))
	if err != nil || len(scripts) == 0 {
		t.Fatalf("scripts/*.sh: %v (%d files)", err, len(scripts))
	}
	for _, s := range scripts {
		b, err := os.ReadFile(s)
		if err != nil {
			t.Fatal(err)
		}
		files["scripts/"+filepath.Base(s)] = string(b)
	}
	if p := proofScanProblems(files); len(p) > 0 {
		t.Fatalf("\n  %s", strings.Join(p, "\n  "))
	}
	files["scripts/proof.sh"] += "\nstep vuln vuln.log make vuln\n"
	if len(proofScanProblems(files)) == 0 {
		t.Error("the rule accepts a proof gate that runs `make vuln`")
	}
}

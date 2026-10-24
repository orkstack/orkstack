package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// The site gate is scripts/proof.sh's and scripts/proof-part.sh's own code, so
// these tests run the real scripts. They run in a throwaway git repository
// that holds copies of the two scripts and a stub `proofrun` (a program that
// answers the few subcommands the script calls before and after its gates),
// with a fake `bun` that records how it was run and a fake `docker` that is
// never there: no container, no Temporal and no real suite is started, and
// every gate the test does not name is left out with PROOF_GATES or a stub.

// scriptTools are the commands the proof scripts use on the paths these tests
// exercise. They are linked into a PATH of their own, so a test can leave bun
// out of it whatever the host has installed.
var scriptTools = []string{
	"bash", "sh", "env", "dirname", "basename", "ps", "tr", "setsid", "git", "go", "mktemp", "date",
	"sha256sum", "cut", "sed", "awk", "grep", "mkdir", "mv", "cp", "chmod", "rm", "cat", "find", "sort",
	"head", "tail", "wc", "hostname", "nproc", "uname", "seq", "sleep", "kill", "xargs", "tee", "ls",
	"readlink", "id", "test", "true", "false",
}

// stubProofrun answers what scripts/proof.sh asks of proofrun on the paths
// under test: no telemetry path is touched, and the gate files are none.
const stubProofrun = `package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "touches" {
		if strings.Contains(strings.Join(os.Args[2:], " "), "-gate-files") {
			fmt.Println("[]")
			return
		}
		os.Exit(1)
	}
}
`

// fakeBun logs its working directory and arguments to $BUN_LOG, prints them,
// and fails "run check" when $BUN_FAIL_CHECK is set.
const fakeBun = `#!/bin/sh
printf '%s\t%s\n' "$(pwd)" "$*" >>"$BUN_LOG"
echo "fake bun: $*"
if [ "$*" = "run check" ] && [ -n "$BUN_FAIL_CHECK" ]; then
	echo "fake bun: check failed" >&2
	exit 1
fi
`

// siteScriptRepo is a throwaway repository for running the proof scripts.
type siteScriptRepo struct {
	t     *testing.T
	root  string
	tools string // a PATH directory of the scriptTools alone
	fakes string // a PATH directory of the fake docker and make
	bin   string // a PATH directory of the fake bun, when a test wants one
	log   string // the fake bun's log
}

func newSiteScriptRepo(t *testing.T) *siteScriptRepo {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("the proof scripts read /proc and need setsid")
	}
	if testing.Short() {
		t.Skip("builds a stub binary and runs the proof scripts")
	}
	// Every test owns its repository, its PATH and its processes, and shares
	// nothing with another: they run beside each other.
	t.Parallel()
	r := &siteScriptRepo{t: t, root: t.TempDir()}
	r.root, _ = filepath.EvalSymlinks(r.root)
	r.tools = filepath.Join(r.root, ".tools")
	r.fakes = filepath.Join(r.root, ".fakes")
	r.bin = filepath.Join(r.root, ".bun")
	r.log = filepath.Join(r.root, "bun.log")
	for _, d := range []string{r.tools, r.fakes} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range scriptTools {
		if p, err := exec.LookPath(name); err == nil {
			if err := os.Symlink(p, filepath.Join(r.tools, name)); err != nil && !os.IsExist(err) {
				t.Fatal(err)
			}
		}
	}
	for _, name := range []string{"git", "go", "setsid", "bash"} {
		if _, err := os.Lstat(filepath.Join(r.tools, name)); err != nil {
			t.Fatalf("%s is not on PATH, and the proof scripts need it", name)
		}
	}
	// docker is never there; make would only lint.
	r.write(filepath.Join(r.fakes, "docker"), "#!/bin/sh\nexit 1\n", 0o755)
	r.write(filepath.Join(r.fakes, "make"), "#!/bin/sh\nexit 0\n", 0o755)

	root := mustRepoRoot()
	for _, f := range []string{"scripts/proof.sh", "scripts/proof-part.sh"} {
		b, err := os.ReadFile(filepath.Join(root, f))
		if err != nil {
			t.Fatal(err)
		}
		r.write(filepath.Join(r.root, f), string(b), 0o755)
	}
	r.write(filepath.Join(r.root, "go.mod"), "module stub\n", 0o644)
	r.write(filepath.Join(r.root, "test/tools/proofrun/main.go"), stubProofrun, 0o644)
	r.write(filepath.Join(r.root, ".gitignore"), "proof/\n.tools/\n.fakes/\n.bun/\n.tmp/\nbun.log\n", 0o644)
	if err := os.MkdirAll(filepath.Join(r.root, ".tmp"), 0o755); err != nil {
		t.Fatal(err)
	}
	r.git("init", "-q", "-b", "main")
	r.git("add", "-A")
	r.git("-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false", "commit", "-q", "-m", "stub")
	return r
}

func (r *siteScriptRepo) write(path, text string, mode os.FileMode) {
	r.t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), mode); err != nil {
		r.t.Fatal(err)
	}
}

// hermeticGit is the environment that keeps the host's git configuration
// (signing, hooks) out of the throwaway repository.
func hermeticGit() []string {
	return []string{"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null"}
}

func (r *siteScriptRepo) git(args ...string) string {
	r.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = r.root
	cmd.Env = append(os.Environ(), hermeticGit()...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		r.t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// withBun puts the fake bun on the scripts' PATH, withNode a node beside it
// (the site's scripts run on node; the fake is never started, only looked for).
func (r *siteScriptRepo) withBun() { r.write(filepath.Join(r.bin, "bun"), fakeBun, 0o755) }
func (r *siteScriptRepo) withNode() {
	r.write(filepath.Join(r.bin, "node"), "#!/bin/sh\nexit 0\n", 0o755)
}

// withNodeModules creates the dependency directories the gates check for.
func (r *siteScriptRepo) withNodeModules(dirs ...string) {
	r.t.Helper()
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(r.root, d, "node_modules"), 0o755); err != nil {
			r.t.Fatal(err)
		}
	}
}

// run runs scripts/proof.sh with args and returns its exit status and output.
func (r *siteScriptRepo) run(env []string, args ...string) (int, string) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", append([]string{filepath.Join(r.root, "scripts", "proof.sh")}, args...)...)
	cmd.Dir = r.root
	path := r.bin + ":" + r.fakes + ":" + r.tools
	goroot, err := exec.Command("go", "env", "GOROOT").Output()
	if err != nil {
		r.t.Fatal(err)
	}
	cmd.Env = append(os.Environ(), hermeticGit()...)
	cmd.Env = append(cmd.Env, "PATH="+path, "GOROOT="+strings.TrimSpace(string(goroot)), "GOTOOLCHAIN=local",
		"GOFLAGS=", "GOWORK=off", "TMPDIR="+filepath.Join(r.root, ".tmp"), "BUN_LOG="+r.log, "PROOF_SESSION=", "PROOF_GATES=", "PROOF_FAIL_FAST=1")
	cmd.Env = append(cmd.Env, env...)
	cmd.WaitDelay = 20 * time.Second
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			r.t.Fatalf("running proof.sh: %v\n%s", err, out)
		}
		code = ee.ExitCode()
	}
	return code, string(out)
}

// gates is the proof directory's gates.tsv, one record per line.
func (r *siteScriptRepo) gates(name string) []string {
	r.t.Helper()
	b, err := os.ReadFile(filepath.Join(r.root, "proof", name, "gates.tsv"))
	if err != nil {
		r.t.Fatalf("no gates.tsv for proof %s: %v", name, err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

// bunRuns is what the fake bun recorded: working directory and arguments.
func (r *siteScriptRepo) bunRuns() []string {
	b, err := os.ReadFile(r.log)
	if err != nil {
		return nil
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func (r *siteScriptRepo) wroteNoProof() bool {
	_, err := os.Stat(filepath.Join(r.root, "proof"))
	return os.IsNotExist(err)
}

// The site gate runs `bun run check && bun run build` in site/, logs to
// site.log, and is recorded in gates.tsv like every other gate.
func TestProofScript_TheSiteGateChecksAndBuildsTheSiteInItsDirectory(t *testing.T) {
	r := newSiteScriptRepo(t)
	r.withBun()
	r.withNode()
	r.withNodeModules("site")
	code, out := r.run([]string{"PROOF_GATES=site"}, "--tier", "full", "site-pass")
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	gates := r.gates("site-pass")
	if len(gates) != 1 || !strings.HasPrefix(gates[0], "site\tpass\t") || !strings.Contains(gates[0], "\tsite.log\t") ||
		!strings.Contains(gates[0], "bun run check && bun run build") {
		t.Errorf("gates.tsv = %q, want one passing site gate logged to site.log", gates)
	}
	want := []string{filepath.Join(r.root, "site") + "\trun check", filepath.Join(r.root, "site") + "\trun build"}
	if got := r.bunRuns(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("bun ran\n%s\nwant, in order and in site/\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	log, err := os.ReadFile(filepath.Join(r.root, "proof", "site-pass", "site.log"))
	if err != nil || !strings.Contains(string(log), "fake bun: run check") || !strings.Contains(string(log), "fake bun: run build") {
		t.Errorf("site.log = %q (%v): the gate's output belongs in it", log, err)
	}
}

// `check` failing is the gate failing, and the build never runs after it.
func TestProofScript_AFailingSiteCheckFailsTheGateAndSkipsTheBuild(t *testing.T) {
	r := newSiteScriptRepo(t)
	r.withBun()
	r.withNode()
	r.withNodeModules("site")
	_, out := r.run([]string{"PROOF_GATES=site", "BUN_FAIL_CHECK=1"}, "--tier", "full", "site-fail")
	gates := r.gates("site-fail")
	if len(gates) != 1 || !strings.HasPrefix(gates[0], "site\tfail\t") {
		t.Errorf("gates.tsv = %q, want one failing site gate", gates)
	}
	if got := r.bunRuns(); len(got) != 1 || !strings.HasSuffix(got[0], "\trun check") {
		t.Errorf("bun ran %v: a failing check must stop the gate before the build", got)
	}
	if !strings.Contains(out, "gate site failed") || !strings.Contains(out, "site: FAIL") {
		t.Errorf("the proof does not say the site gate failed:\n%s", out)
	}
}

// The site gate's precondition is checked before anything is written, like the
// UI gates': bun on PATH, node on PATH (the site's scripts and astro run on it)
// and site/node_modules, and a missing one is exit 77.
func TestProofScript_AMissingSitePrerequisiteIsExit77AndWritesNothing(t *testing.T) {
	for _, c := range []struct {
		name      string
		bun, node bool
		modules   []string
		want      string
		absent    string
	}{
		{"no bun", false, true, []string{"site"}, "bun on PATH", "node on PATH"},
		{"no node", true, false, []string{"site"}, "node on PATH (the site's scripts and astro run on it)", "bun on PATH"},
		{"no site/node_modules", true, true, []string{"ui"}, "site/node_modules (cd site && bun install --frozen-lockfile)", "ui/node_modules"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newSiteScriptRepo(t)
			if c.bun {
				r.withBun()
			}
			if c.node {
				r.withNode()
			}
			r.withNodeModules(c.modules...)
			code, out := r.run([]string{"PROOF_GATES=site"}, "--tier", "full", "site-missing")
			if code != 77 {
				t.Fatalf("exit %d, want 77:\n%s", code, out)
			}
			if !strings.Contains(out, "prerequisites absent, nothing measured, nothing written") || !strings.Contains(out, c.want) {
				t.Errorf("output lacks %q:\n%s", c.want, out)
			}
			if c.absent != "" && strings.Contains(out, c.absent) {
				t.Errorf("output names %q, which is present:\n%s", c.absent, out)
			}
			if !r.wroteNoProof() {
				t.Error("a refused proof left a proof directory behind")
			}
			if len(r.bunRuns()) != 0 {
				t.Errorf("bun ran for a refused proof: %v", r.bunRuns())
			}
		})
	}
}

// The full and release tiers always run the site gate; the nightly tier, which
// runs the deferred tests and nothing else, does not. Each tier is asked
// without PROOF_GATES, on a host with bun but without site/node_modules and
// without Docker, and says what it is missing: nothing is started.
func TestProofScript_TheFullAndReleaseTiersAlwaysRunTheSiteGateAndNightlyNever(t *testing.T) {
	for _, c := range []struct {
		tier string
		site bool
	}{{"full", true}, {"release", true}, {"nightly", false}} {
		t.Run(c.tier, func(t *testing.T) {
			r := newSiteScriptRepo(t)
			r.withBun()
			r.withNode()
			r.withNodeModules("ui")
			code, out := r.run(nil, "--tier", c.tier, "site-tier")
			if code != 77 || !strings.Contains(out, "a docker daemon") {
				t.Fatalf("exit %d, want 77 with the stack gates' prerequisites listed (the positive control):\n%s", code, out)
			}
			if got := strings.Contains(out, "site/node_modules"); got != c.site {
				t.Errorf("the %s tier names site/node_modules = %v, want %v:\n%s", c.tier, got, c.site, out)
			}
			if !c.site && strings.Contains(out, "node") {
				t.Errorf("the %s tier needs neither bun, node nor the site, and says:\n%s", c.tier, out)
			}
			if !r.wroteNoProof() {
				t.Error("a refused proof left a proof directory behind")
			}
		})
	}
}

// PROOF_GATES validates its names against the script's gate list, so `site` is
// a gate the script knows; an unknown one is still refused.
func TestProofScript_SiteIsAGateTheScriptKnows(t *testing.T) {
	r := newSiteScriptRepo(t)
	r.withBun()
	r.withNodeModules("site")
	code, out := r.run([]string{"PROOF_GATES=sit"}, "--tier", "full", "site-unknown")
	if code != 2 || !strings.Contains(out, "PROOF_GATES names an unknown gate 'sit'") || !strings.Contains(out, " site ") {
		t.Errorf("exit %d, want 2 naming the known gates, site among them:\n%s", code, out)
	}
}

// The CI fast part records the site gate beside the UI gates, and refuses
// (exit 77) without bun or site/node_modules.
func TestProofPart_TheFastPartRecordsTheSiteGate(t *testing.T) {
	r := newSiteScriptRepo(t)
	r.withBun()
	r.withNode()
	r.withNodeModules("ui", "site")
	base := r.git("rev-parse", "HEAD")
	code, out := r.run(nil, "--part", "fast", "--tier", "full", "--proof-id", "pr-site", "--attempt", "1", "--base-commit", base, "site-part")
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	var names, results []string
	siteLine := ""
	for _, line := range r.gates("site-part-part-fast-a1") {
		f := strings.Split(line, "\t")
		names, results = append(names, f[0]), append(results, f[1])
		if f[0] == "site" {
			siteLine = line
		}
	}
	if got, want := strings.Join(names, " "), "lint build unit vet-integration ui-check ui-unit site"; got != want {
		t.Errorf("the fast part records gates %q, want %q", got, want)
	}
	for i, res := range results {
		if res != "pass" {
			t.Errorf("gate %s: %s", names[i], res)
		}
	}
	if !strings.HasPrefix(siteLine, "site\tpass\t") || !strings.Contains(siteLine, "\tsite.log\tbun run check && bun run build") {
		t.Errorf("the site gate is recorded as %q", siteLine)
	}
	var siteRuns []string
	for _, run := range r.bunRuns() {
		if strings.HasPrefix(run, filepath.Join(r.root, "site")+"\t") {
			siteRuns = append(siteRuns, strings.TrimPrefix(run, filepath.Join(r.root, "site")+"\t"))
		}
	}
	if strings.Join(siteRuns, ",") != "run check,run build" {
		t.Errorf("bun ran %v in site/, want run check then run build", siteRuns)
	}
}

// A failing site gate is recorded failing, and fails the part.
func TestProofPart_AFailingSiteGateFailsTheFastPart(t *testing.T) {
	r := newSiteScriptRepo(t)
	r.withBun()
	r.withNode()
	r.withNodeModules("ui", "site")
	code, _ := r.run([]string{"BUN_FAIL_CHECK=1"}, "--part", "fast", "--tier", "full", "--proof-id", "pr-site", "--attempt", "1",
		"--base-commit", r.git("rev-parse", "HEAD"), "site-part")
	if code == 0 {
		t.Error("a fast part whose bun run check fails exited 0")
	}
	got := r.gates("site-part-part-fast-a1")
	if !strings.Contains(strings.Join(got, "\n"), "site\tfail\t") {
		t.Errorf("gates = %q: the failing site gate is not recorded", got)
	}
	// The part still records the gates that passed: a failing site gate does not
	// hide the lint, build and unit results the merger reads.
	if !strings.Contains(strings.Join(got, "\n"), "unit\tpass\t") {
		t.Errorf("gates = %q: the passing gates were lost", got)
	}
}

func TestProofPart_TheFastPartNeedsBunNodeAndSiteNodeModules(t *testing.T) {
	for _, c := range []struct {
		name      string
		bun, node bool
		modules   []string
		want      string
	}{
		{"no bun", false, true, []string{"ui", "site"}, "bun on PATH"},
		{"no node", true, false, []string{"ui", "site"}, "node on PATH (the site's scripts and astro run on it)"},
		{"no site/node_modules", true, true, []string{"ui"}, "site/node_modules (cd site && bun install --frozen-lockfile)"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newSiteScriptRepo(t)
			if c.bun {
				r.withBun()
			}
			if c.node {
				r.withNode()
			}
			r.withNodeModules(c.modules...)
			code, out := r.run(nil, "--part", "fast", "--tier", "full", "--proof-id", "pr-site", "--attempt", "1",
				"--base-commit", r.git("rev-parse", "HEAD"), "site-part")
			if code != 77 || !strings.Contains(out, "prerequisites absent") || !strings.Contains(out, c.want) {
				t.Errorf("exit %d, want 77 naming %q:\n%s", code, c.want, out)
			}
			if !r.wroteNoProof() {
				t.Error("a refused part left a proof directory behind")
			}
		})
	}
}

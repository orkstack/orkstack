package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
)

// A local proof with more than one Playwright worker starts one browser server
// for its gate through a short-lived `bun e2e/browser-server.ts start`, which
// labelled the container with its OWN pid as the owner and exited. The next
// proof's sweep then read the owner as gone and stopped the browser server of
// a proof still running: on 2026-10-30 at 20:54 local, proof
// upgrade-ui-site-3-feature's start (its scripts/proof.sh sweep, temporal.log:
// "stopped stale funny_feistel (its proof, pid 114342, is gone)") stopped
// container 44afe947702a, the browser server of proof
// api-hardening-feature, 89 specs into its gate; 94 specs failed after it.
//
// The recorded records: that proof's proof.pid ("3902900 3902900 18609085
// pr-048ac8024ee0"), its Temporal's labels, and the browser server's
// (orkstack.proof=pr-048ac8024ee0, orkstack.e2e=browser-server, owner the
// starter's pid 114342, which had exited).

const (
	incidentHost  = "sauroposeidon"
	incidentProof = "pr-048ac8024ee0"
)

func incidentContainers() []proofContainer {
	return []proofContainer{
		{"funny_feistel", incidentProof, incidentHost + ":114342:18612240"},
		{"orkstack-proof-" + incidentProof + "-temporal", incidentProof, incidentHost + ":3902900:18609085"},
	}
}

// incidentCleaner is the sweep of another proof starting on that host while
// the proof ran (alive) or after it had gone.
func incidentCleaner(t *testing.T, proofAlive bool, docker *fakeCleanDocker, out *strings.Builder) *cleaner {
	t.Helper()
	wt := t.TempDir()
	write(t, filepath.Join(wt, "proof", "api-hardening-feature", "proof.pid"), "3902900 3902900 18609085 "+incidentProof+"\n")
	// The sweeping proof's own record: it is running too.
	write(t, filepath.Join(wt, "proof", "upgrade-ui-site-3-feature", "proof.pid"), "4171848 4171848 18737132 pr-c9517f608fda\n")
	starts := map[int]string{4171848: "18737132"}
	if proofAlive {
		starts[3902900] = "18609085"
	}
	return &cleaner{
		worktrees: []string{wt},
		host:      incidentHost,
		alive:     func(pid int) bool { return starts[pid] != "" },
		procStart: func(pid int) string { return starts[pid] },
		docker:    docker,
		remove:    true,
		out:       out,
		removeAll: os.RemoveAll,
	}
}

func TestClean_ALiveProofsBrowserServerIsNeverSwept(t *testing.T) {
	var out strings.Builder
	docker := &fakeCleanDocker{temporals: incidentContainers()}
	incidentCleaner(t, true, docker, &out).run(context.Background())
	if len(docker.stopped) != 0 {
		t.Fatalf("the sweep stopped %q, of a proof whose proof.pid says it still runs:\n%s", docker.stopped, out.String())
	}
	if !strings.Contains(out.String(), "funny_feistel: in use: proof "+incidentProof+" is still running") {
		t.Errorf("the sweep does not say why it left the browser server:\n%s", out.String())
	}

	// Once the proof is gone, both of its containers are its leftovers.
	out.Reset()
	docker = &fakeCleanDocker{temporals: incidentContainers()}
	incidentCleaner(t, false, docker, &out).run(context.Background())
	if got, want := sorted(docker.stopped), []string{"funny_feistel", "orkstack-proof-" + incidentProof + "-temporal"}; !slices.Equal(got, want) {
		t.Errorf("stopped %q, want %q:\n%s", got, want, out.String())
	}
}

// deadPID is the pid of a process that has exited.
func deadPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("sh", "-c", ":")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	return cmd.Process.Pid
}

// captureOSStdout runs fn with os.Stdout redirected to a file, as a command
// that writes to the process's standard output (cmdClean) is observed.
func captureOSStdout(t *testing.T, fn func() int) (string, int) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stdout-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	prev := os.Stdout
	os.Stdout = f
	code := func() int {
		defer func() { os.Stdout = prev }()
		return fn()
	}()
	b, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return string(b), code
}

// goneHost is a host as `proofrun clean` finds it: the one directory it runs
// in is the repository's worktree (git, which would list the real ones, is
// told it has no repository), the object store is a server of its own that
// counts the requests it gets, and a process that has exited left a test work
// root.
type goneHost struct {
	worktree     string
	deadWorkRoot string
	storeReqs    *atomic.Int32
	deadOwner    string // the owner label of a container whose starter has exited
}

func newGoneHost(t *testing.T) *goneHost {
	t.Helper()
	h := &goneHost{worktree: t.TempDir(), storeReqs: &atomic.Int32{}}
	t.Chdir(h.worktree)
	t.Setenv("GIT_DIR", filepath.Join(h.worktree, "no-repository"))
	t.Setenv("ORK_ORKER_WORKDIR", "")
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.storeReqs.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>x</ID></Owner><Buckets></Buckets></ListAllMyBucketsResult>`)
	}))
	t.Cleanup(store.Close)
	t.Setenv("ORK_S3_ENDPOINT", store.URL)
	host, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	dead := deadPID(t)
	h.deadOwner = fmt.Sprintf("%s:%d:18612240", host, dead)
	h.deadWorkRoot = filepath.Join(h.worktree, ".data", "work", fmt.Sprintf("soak-p%d-abc123", dead))
	write(t, filepath.Join(h.deadWorkRoot, "left-behind"), "x")
	return h
}

// The incident's two containers of a proof that is gone, a container of
// nothing of the proofs', and an activity container of an Orker.
func (h *goneHost) containers() []fakeDockerContainer {
	return []fakeDockerContainer{
		{Name: "funny_feistel", Labels: map[string]string{"orkstack.proof": incidentProof, "orkstack.proof.owner": h.deadOwner}},
		{Name: "unrelated", Labels: map[string]string{"app": "something-else"}},
		{Name: "orkstack-proof-" + incidentProof + "-temporal", Labels: map[string]string{"orkstack.proof": incidentProof, "orkstack.proof.owner": h.deadOwner}},
		{Name: "c-activity", Labels: map[string]string{"ork.orker": "soak-pr-aaa-17"}},
	}
}

func hasLine(out, line string) bool { return slices.Contains(strings.Split(out, "\n"), line) }

// scripts/proof.sh's own sweep at its Temporal's start runs `proofrun clean
// -remove -proof-containers`: it stops a gone proof's containers alone,
// touching no bucket, work root or activity container, and says each one it
// stopped on a line of its own (sweep_stale reads those lines). The run is
// the command's own, flag parsing and all.
func TestCmdClean_TheProofContainersFlagLimitsTheSweepToAGoneProofsContainers(t *testing.T) {
	h := newGoneHost(t)
	tools := installFakeDocker(t, h.containers())
	out, code := captureOSStdout(t, func() int { return cmdClean([]string{"-remove", "-proof-containers"}) })
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	names := []string{"funny_feistel", "orkstack-proof-" + incidentProof + "-temporal"}
	for _, name := range names {
		if !hasLine(out, "stopped "+name) {
			t.Errorf("the sweep does not say on a line of its own that it stopped %s:\n%s", name, out)
		}
	}
	if strings.Contains(out, "cleanup:") || strings.Contains(out, "buckets kept by test processes") {
		t.Errorf("the proof-containers sweep ran the whole cleanup:\n%s", out)
	}
	if got, want := tools.verbs(), []string{"docker ps", "docker stop", "docker stop"}; !slices.Equal(got, want) {
		t.Errorf("docker was asked %q, want one listing and two stops", got)
	}
	var stopped []string
	for _, c := range tools.calls() {
		if c[1] == "stop" {
			stopped = append(stopped, c[2:]...)
		}
	}
	if !slices.Equal(sorted(stopped), sorted(names)) {
		t.Errorf("docker stopped %q, want %q", stopped, names)
	}
	if n := h.storeReqs.Load(); n != 0 {
		t.Errorf("the proof-containers sweep made %d requests of the object store", n)
	}
	if _, err := os.Stat(h.deadWorkRoot); err != nil {
		t.Errorf("the proof-containers sweep removed a dead process's work root: %v", err)
	}
}

// The control of the test above: the same host without the flag is the whole
// cleanup, which removes what the dead process left (its work root) and ends
// with the summary line scripts/proof.sh copies into setup.log; it prints no
// `stopped` line, which only the proof-containers sweep does.
func TestCmdClean_WithoutTheFlagItIsTheWholeCleanup(t *testing.T) {
	h := newGoneHost(t)
	tools := installFakeDocker(t, h.containers())
	out, code := captureOSStdout(t, func() int { return cmdClean([]string{"-remove"}) })
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if want := "cleanup: removed 0 buckets, 1 work roots, 2 containers of dead proofs"; !hasLine(out, want) {
		t.Errorf("the output lacks the line %q:\n%s", want, out)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "stopped ") {
			t.Errorf("the whole cleanup printed %q, which only the proof-containers sweep does", line)
		}
	}
	if _, err := os.Stat(h.deadWorkRoot); !os.IsNotExist(err) {
		t.Errorf("the dead process's work root is still there: %v", err)
	}
	if h.storeReqs.Load() == 0 {
		t.Error("the whole cleanup never looked at the object store")
	}
	if got := tools.verbs(); !slices.Contains(got, "docker stop") {
		t.Errorf("docker was asked %q, want the gone proof's containers stopped", got)
	}
}

// dockerCLI lists the proofs' own containers through one `docker ps` template
// of three columns (name, proof, owner), and reads them back by position. The
// incident this guards: a container's owner read from the wrong column, or a
// container of no proof read as a proof's, and a live proof's container was
// stopped. The fake renders the template as docker does.
func TestDockerCLI_ProofContainersReadsTheColumnsTheTemplateNames(t *testing.T) {
	installFakeDocker(t, []fakeDockerContainer{
		{Name: "funny_feistel", Labels: map[string]string{"orkstack.proof": incidentProof, "orkstack.proof.owner": incidentHost + ":114342:18612240"}},
		{Name: "unlabelled", Labels: map[string]string{"app": "something-else"}},
		{Name: "orkstack-proof-" + incidentProof + "-temporal", Labels: map[string]string{"orkstack.proof": incidentProof, "orkstack.proof.owner": incidentHost + ":3902900:18609085"}},
		// A container of a proof that named no owner is the last row, whose
		// empty column ends the listing.
		{Name: "no-owner", Labels: map[string]string{"orkstack.proof": "pr-1"}},
	})
	got, err := dockerCLI{}.ProofContainers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []proofContainer{
		{"funny_feistel", incidentProof, incidentHost + ":114342:18612240"},
		{"orkstack-proof-" + incidentProof + "-temporal", incidentProof, incidentHost + ":3902900:18609085"},
		{"no-owner", "pr-1", ""},
	}
	if !slices.Equal(got, want) {
		t.Errorf("ProofContainers = %q, want %q", got, want)
	}
}

// A docker that fails is an error the sweep reports, not an empty listing.
func TestDockerCLI_ProofContainersFailsWhenDockerDoes(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "docker"), "#!/bin/sh\necho 'Cannot connect to the Docker daemon' >&2\nexit 1\n")
	if err := os.Chmod(filepath.Join(dir, "docker"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if got, err := (dockerCLI{}).ProofContainers(context.Background()); err == nil {
		t.Errorf("ProofContainers = %q, nil, want the error of a docker that failed", got)
	}
}

// scripts/proof.sh gives everything it starts for the gate its own process as
// the owner. That string is read back by `proofrun clean` (a container is a
// gone proof's when no process here has that pid and start time), so the start
// time the shell records must be the one the cleaner reads for the same live
// process; otherwise every container of a live proof reads as abandoned.
func TestProofScript_TheOwnerItNamesIsTheOneTheCleanerReadsAsAlive(t *testing.T) {
	bash := needBash(t)
	b, err := os.ReadFile(filepath.Join(mustRepoRoot(), "scripts", "proof.sh"))
	if err != nil {
		t.Fatal(err)
	}
	var ownerLine string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "PROOF_OWNER=") {
			ownerLine = l
		}
	}
	if ownerLine == "" {
		t.Fatal("scripts/proof.sh sets no PROOF_OWNER")
	}
	// The shell prints its pid and its owner, then waits for stdin to close,
	// so it is alive while the cleaner's reading of it is taken.
	cmd := exec.Command(bash, "-c", proofScriptFunctions(t, "proc_start")+"\n"+ownerLine+"\necho \"$$ $PROOF_OWNER\"\nread -r _")
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { in.Close(); _ = cmd.Wait() }()
	line, err := bufio.NewReader(stdoutPipe).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	f := strings.Fields(line)
	if len(f) != 2 {
		t.Fatalf("the shell printed %q", line)
	}
	owner := strings.SplitN(f[1], ":", 3)
	if len(owner) != 3 {
		t.Fatalf("the owner %q is not host:pid:start", f[1])
	}
	if owner[1] != f[0] {
		t.Errorf("the owner names pid %s, want the proof's shell, %s", owner[1], f[0])
	}
	if want := processStart(cmd.Process.Pid); owner[2] != want || want == "" {
		t.Errorf("the owner names start time %q, the cleaner reads %q of that process", owner[2], want)
	}
}

// sweep_stale (scripts/proof.sh, at the start of the proof's own Temporal)
// stops the containers of a gone proof, says each one it stopped, and records
// them in SWEPT; it spares every container of a proof whose proof.pid says it
// still runs, whatever its owner label says (the incident: a browser server
// started by a helper that had exited). It runs the real `proofrun clean`
// against a docker that lists those containers.
func TestProofScript_SweepStaleStopsAGoneProofsContainersAndSparesARunningProofs(t *testing.T) {
	needBash(t)
	sweep := proofScriptFunctions(t, "sweep_stale") // read before the test leaves the repository
	h := newGoneHost(t)
	livePID := os.Getpid()
	write(t, filepath.Join(h.worktree, "proof", "live", "proof.pid"),
		fmt.Sprintf("%d %d %s pr-live\n", livePID, livePID, processStart(livePID)))
	tools := installFakeDocker(t, append(h.containers(), fakeDockerContainer{
		Name:   "browser-of-the-live-proof",
		Labels: map[string]string{"orkstack.proof": "pr-live", "orkstack.proof.owner": h.deadOwner},
	}))
	out, err := runProofShell(t, h.worktree, sweep+
		"\nPR="+proofrunStandIn(t)+"\nSWEPT=\"\"\nsweep_stale\necho \"SWEPT=$SWEPT\"")
	if err != nil {
		t.Fatalf("sweep_stale failed: %v\n%s", err, out)
	}
	gone := []string{"funny_feistel", "orkstack-proof-" + incidentProof + "-temporal"}
	for _, name := range gone {
		if !strings.Contains(out, "stopped stale "+name+" ") {
			t.Errorf("the sweep does not say it stopped %s:\n%s", name, out)
		}
	}
	var swept []string
	for _, l := range strings.Split(out, "\n") {
		if v, ok := strings.CutPrefix(l, "SWEPT="); ok {
			swept = strings.Fields(v)
		}
	}
	if !slices.Equal(sorted(swept), sorted(gone)) {
		t.Errorf("SWEPT = %q, want %q", swept, gone)
	}
	for _, c := range tools.calls() {
		if c[1] == "stop" && !slices.Contains(gone, c[2]) {
			t.Errorf("the sweep stopped %q, which is no gone proof's", c[2:])
		}
	}
	if strings.Contains(out, "browser-of-the-live-proof") && strings.Contains(out, "stopped stale browser-of-the-live-proof") {
		t.Errorf("the sweep stopped the live proof's container:\n%s", out)
	}
}

// A sweep that cannot run is said and never stops the proof: its own Temporal
// is still started.
func TestProofScript_SweepStaleSaysWhenItCouldNotRun(t *testing.T) {
	needBash(t)
	dir := t.TempDir()
	failing := filepath.Join(dir, "proofrun")
	write(t, failing, "#!/bin/sh\necho 'object storage: refused' >&2\necho 'the cleanup broke' >&2\nexit 3\n")
	if err := os.Chmod(failing, 0o755); err != nil {
		t.Fatal(err)
	}
	out, err := runProofShell(t, dir, proofScriptFunctions(t, "sweep_stale")+
		"\nPR="+failing+"\nSWEPT=\"\"\nsweep_stale\necho \"after: status $? swept [$SWEPT]\"")
	if err != nil {
		t.Fatalf("sweep_stale failed the proof: %v\n%s", err, out)
	}
	if !strings.Contains(out, "the stale-container sweep could not run") || !strings.Contains(out, "the cleanup broke") {
		t.Errorf("the sweep does not say it could not run, or why:\n%s", out)
	}
	if !strings.Contains(out, "after: status 0 swept []") {
		t.Errorf("a sweep that could not run changed the proof's course:\n%s", out)
	}
}

// start_temporal labels the proof's own Temporal with the proof's id and its
// owner, which is what the sweep of the next proof reads.
func TestProofScript_StartTemporalLabelsItsContainerWithTheProofAndItsOwner(t *testing.T) {
	needBash(t)
	dir := t.TempDir()
	tools := installFakeDocker(t, nil)
	pr := filepath.Join(dir, "proofrun")
	write(t, pr, "#!/bin/sh\n[ \"$1\" = temporal-args ] && echo temporalio/server:1.2.3\nexit 0\n")
	if err := os.Chmod(pr, 0o755); err != nil {
		t.Fatal(err)
	}
	const owner = "builder:4242:99"
	// The fake docker publishes no port, so start_temporal stops after the
	// container is started: all this reads is how it was started.
	out, _ := runProofShell(t, dir, proofScriptFunctions(t, "sweep_stale", "start_temporal")+
		"\nPR="+pr+"\nPROOF_TEMPORAL=own PROOF_ID=pr-me PROOF_OWNER="+owner+" TEMPORAL_NAME=orkstack-proof-pr-me-temporal SWEPT=\"\"\nstart_temporal")
	var run []string
	for _, c := range tools.calls() {
		if c[1] == "run" {
			run = c
		}
	}
	if run == nil {
		t.Fatalf("start_temporal never started the container:\n%s", out)
	}
	var labels []string
	for i, a := range run {
		if a == "--label" && i+1 < len(run) {
			labels = append(labels, run[i+1])
		}
	}
	if want := []string{"orkstack.proof=pr-me", "orkstack.proof.owner=" + owner}; !slices.Equal(sorted(labels), sorted(want)) {
		t.Errorf("the container is labelled %q, want %q (docker %q)", labels, want, run)
	}
	if i := slices.Index(run, "--name"); i < 0 || run[i+1] != "orkstack-proof-pr-me-temporal" {
		t.Errorf("the container is not named after the proof: %q", run)
	}
}

// The Playwright gate's browser server is started by a helper that exits
// (`bun e2e/browser-server.ts start`), so it is handed the proof's owner, which
// the container is then labelled with; the suite's invocations find the
// server, and the gate stops it. With one worker no server is started for the
// gate. ui_e2e_locked runs under bash with a bun, a Playwright, a docker and
// a proofrun that record how they were called.
func TestProofScript_TheGatesBrowserServerIsStartedWithTheProofsOwner(t *testing.T) {
	needBash(t)
	for _, c := range []struct {
		workers     string
		wantServer  bool
		wantInvokes int
	}{
		{"2", true, 2},
		{"1", false, 1},
	} {
		t.Run("workers="+c.workers, func(t *testing.T) {
			repo, dir, bin := t.TempDir(), t.TempDir(), t.TempDir()
			log := filepath.Join(dir, "stubs.log")
			write(t, filepath.Join(repo, "ui", "e2e", "browser-image.txt"), "# the pinned browser\nexample.com/browser@sha256:abc\n")
			write(t, filepath.Join(dir, "ui-e2e-list.json"), "{}")
			write(t, filepath.Join(dir, "ui-run-files.txt"), "")
			stub := func(path, body string) {
				write(t, path, "#!/bin/sh\n"+body)
				if err := os.Chmod(path, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			stub(filepath.Join(bin, "bun"), `echo "bun $* owner=$ORK_PROOF_OWNER" >>"$STUB_LOG"
echo '{"wsEndpoint":"ws://127.0.0.1:9/pw","container":"cafe0123456789abcdef"}'
`)
			stub(filepath.Join(repo, "ui", "node_modules", ".bin", "playwright"),
				`echo "playwright owner=$ORK_PROOF_OWNER ws=$ORK_E2E_BROWSER_WS container=$ORK_E2E_BROWSER_CONTAINER" >>"$STUB_LOG"`+"\n")
			pr := filepath.Join(bin, "proofrun")
			stub(pr, "exit 0\n")
			tools := installFakeDocker(t, nil)
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			const owner = "builder:4242:99"
			out, err := runProofShell(t, dir, proofScriptFunctions(t, "browser_image", "ui_e2e_locked", "playwright_run")+
				"\nPR="+pr+" REPO="+repo+" DIR="+dir+" TIER=full PROOF_UI_WORKERS="+c.workers+" PROOF_FAIL_FAST=1 PROOF_ID=pr-me PROOF_OWNER="+owner+
				"\nSTACK_ENV=() SPEC_ARGS=() BROWSER_WS=\"\" BROWSER_CONTAINER=\"\"\nui_e2e_locked", "STUB_LOG="+log)
			if err != nil {
				t.Fatalf("the gate failed: %v\n%s", err, out)
			}
			b, _ := os.ReadFile(log)
			var bunCalls, invokes []string
			for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
				switch {
				case strings.HasPrefix(l, "bun "):
					bunCalls = append(bunCalls, l)
				case strings.HasPrefix(l, "playwright "):
					invokes = append(invokes, l)
				}
			}
			if len(invokes) != c.wantInvokes {
				t.Fatalf("Playwright ran %d times, want %d:\n%s", len(invokes), c.wantInvokes, b)
			}
			stopped := slices.ContainsFunc(tools.calls(), func(call []string) bool {
				return slices.Equal(call, []string{"docker", "stop", "-t", "5", "cafe0123456789abcdef"})
			})
			if !c.wantServer {
				if len(bunCalls) != 0 || stopped {
					t.Errorf("a gate with one worker started or stopped a browser server: %q, stopped %v", bunCalls, stopped)
				}
				if want := "playwright owner=" + owner + " ws= container="; invokes[0] != want {
					t.Errorf("the invocation = %q, want %q", invokes[0], want)
				}
				return
			}
			if want := []string{"bun e2e/browser-server.ts start pr-me owner=" + owner}; !slices.Equal(bunCalls, want) {
				t.Errorf("the browser server was started as %q, want %q", bunCalls, want)
			}
			for _, inv := range invokes {
				if want := "playwright owner=" + owner + " ws=ws://127.0.0.1:9/pw container=cafe0123456789abcdef"; inv != want {
					t.Errorf("the invocation = %q, want %q", inv, want)
				}
			}
			if !stopped {
				t.Errorf("the gate did not stop its browser server; docker was asked %q", tools.calls())
			}
		})
	}
}

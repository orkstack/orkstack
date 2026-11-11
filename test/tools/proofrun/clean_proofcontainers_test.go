package main

import (
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

	"github.com/orkstack/orkstack/internal/testsupport/must"
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
	must.OK(t, cmd.Run())
	return cmd.Process.Pid
}

// captureOSStdout runs fn with os.Stdout redirected to a file, as a command
// that writes to the process's standard output (cmdClean) is observed.
func captureOSStdout(t *testing.T, fn func() int) (string, int) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stdout-*")
	must.OK(t, err)
	defer f.Close()
	prev := os.Stdout
	os.Stdout = f
	code := func() int {
		defer func() { os.Stdout = prev }()
		return fn()
	}()
	b, err := os.ReadFile(f.Name())
	must.OK(t, err)
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
	must.OK(t, err)
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
	must.OK(t, err)
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
	must.OK(t, os.Chmod(filepath.Join(dir, "docker"), 0o755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if got, err := (dockerCLI{}).ProofContainers(context.Background()); err == nil {
		t.Errorf("ProofContainers = %q, nil, want the error of a docker that failed", got)
	}
}

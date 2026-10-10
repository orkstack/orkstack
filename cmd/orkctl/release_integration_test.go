//go:build integration

package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// `orkctl release`'s CLI acceptance (docs/DEPLOY.md §3) on the real binaries: `ork` and `orker`
// started with ORK_BUILD_ID and a per-test ORK_DEPLOYMENT_NAME, each test in
// its own Temporal namespace (one namespace holds one deployment), and
// `orkctl` as a built binary whose exit codes are the real ones. Tasks are
// real containers; a "gated" task waits for a file the test writes into its
// own /orkstack/tmp, which is how the test holds a run in flight and lets it
// go.

var releaseBins struct {
	once               sync.Once
	dir                string
	orkctl, ork, orker string
	err                error
}

// removeReleaseBinaries deletes the directory releaseBinaries built into, once
// every test of the process has finished (TestMain).
func removeReleaseBinaries() {
	if releaseBins.dir != "" {
		_ = os.RemoveAll(releaseBins.dir)
	}
}

// releaseBinaries builds orkctl, ork and orker once per test process.
func releaseBinaries(t *testing.T) (string, string, string) {
	t.Helper()
	releaseBins.once.Do(func() {
		dir, err := os.MkdirTemp("", "ork-release-bins-")
		if err != nil {
			releaseBins.err = err
			return
		}
		releaseBins.dir = dir
		root := repoRoot(t)
		for _, name := range []string{"orkctl", "ork", "orker"} {
			cmd := exec.Command("go", "build", "-o", filepath.Join(dir, name), "./cmd/"+name)
			cmd.Dir = root
			if out, err := cmd.CombinedOutput(); err != nil {
				releaseBins.err = fmt.Errorf("go build ./cmd/%s: %v\n%s", name, err, out)
				return
			}
		}
		releaseBins.orkctl = filepath.Join(dir, "orkctl")
		releaseBins.ork = filepath.Join(dir, "ork")
		releaseBins.orker = filepath.Join(dir, "orker")
	})
	if releaseBins.err != nil {
		t.Fatal(releaseBins.err)
	}
	return releaseBins.orkctl, releaseBins.ork, releaseBins.orker
}

// relProc is one process this test started, stoppable by its own PID.
type relProc struct {
	name string
	cmd  *exec.Cmd
	done chan struct{}
	mu   sync.Mutex
	log  strings.Builder
}

func (p *relProc) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.log.Write(b)
}

func (p *relProc) stop() {
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

// relStack is one test's deployment: its namespace, its processes, and the
// orkctl transcript.
type relStack struct {
	t           *testing.T
	ctx         context.Context
	root        string
	orkctl      string
	ork, orker  string
	address     string
	namespace   string
	deployment  string
	workRoot    string
	artifactDir string
	engines     map[string]string // engine name -> address
	first       string
	procs       map[string]*relProc
	started     time.Time
	mu          sync.Mutex
	log         strings.Builder
	name        string
}

func newRelStack(t *testing.T, name string, timeout time.Duration) *relStack {
	t.Helper()
	address := temporalAddress()
	requireTemporal(t, address)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	orkctl, ork, orker := releaseBinaries(t)
	s := &relStack{
		t: t, ctx: ctx, root: repoRoot(t), orkctl: orkctl, ork: ork, orker: orker,
		address:     address,
		namespace:   isolatedNamespace() + "-rel",
		deployment:  fmt.Sprintf("ork-rel-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000),
		workRoot:    bindableOrkerWorkRoot(t),
		artifactDir: sharedLocalArtifactDir(t),
		engines:     map[string]string{},
		procs:       map[string]*relProc{},
		started:     time.Now(),
		name:        name,
	}
	// Provision the namespace before any process races to.
	dialIsolatedNamespace(ctx, t, address, s.namespace)
	s.note("namespace %s, deployment %s", s.namespace, s.deployment)
	t.Cleanup(func() {
		for _, p := range s.procs {
			p.stop()
		}
		for _, p := range s.procs {
			p.mu.Lock()
			t.Logf("%s log:\n%s", p.name, p.log.String())
			p.mu.Unlock()
		}
		proof.Write(t, "orkctl", "release-"+name+".txt", []byte(s.transcript()))
	})
	return s
}

func (s *relStack) note(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(&s.log, "[%7.1fs] %s\n", time.Since(s.started).Seconds(), fmt.Sprintf(format, args...))
}

func (s *relStack) transcript() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.String()
}

func (s *relStack) start(name, path string, env ...string) {
	s.t.Helper()
	if _, ok := s.procs[name]; ok {
		s.t.Fatalf("process %s already running", name)
	}
	p := &relProc{name: name, done: make(chan struct{})}
	if filepath.Base(path) == "ork" {
		env = append(env, localActivityCatalogEnv(s.t))
	}
	p.cmd = exec.CommandContext(s.ctx, path)
	p.cmd.Env = append(os.Environ(), env...)
	p.cmd.Stdout, p.cmd.Stderr = p, p
	if err := p.cmd.Start(); err != nil {
		s.t.Fatalf("start %s: %v", name, err)
	}
	go func() { _ = p.cmd.Wait(); close(p.done) }()
	s.procs[name] = p
}

// stopProc stops one process this test started, by its own PID.
func (s *relStack) stopProc(name string) {
	s.t.Helper()
	p, ok := s.procs[name]
	if !ok {
		s.t.Fatalf("no process %s", name)
	}
	p.stop()
	p.mu.Lock()
	s.t.Logf("%s log:\n%s", name, p.log.String())
	p.mu.Unlock()
	delete(s.procs, name)
	s.note("stopped %s", name)
}

func (s *relStack) common(build string) []string {
	return []string{
		"ORK_TEMPORAL_ADDRESS=" + s.address,
		"ORK_TEMPORAL_NAMESPACE=" + s.namespace,
		"ORK_ORKER_QUEUE=ork-tasks",
		"ORK_ARTIFACT_BACKEND=local",
		"ORK_ARTIFACT_DIR=" + s.artifactDir,
		"ORK_DEPLOYMENT_NAME=" + s.deployment,
		"ORK_BUILD_ID=" + build,
	}
}

// startEngine starts one `ork` of release build ("" = unversioned).
func (s *relStack) startEngine(name, build string) {
	s.t.Helper()
	addr := freeAddress(s.t)
	s.start(name, s.ork, append(s.common(build), "ORK_HTTP_ADDR="+addr)...)
	s.engines[name] = addr
	if s.first == "" {
		s.first = name
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		if _, _, code := s.cliQuietOn(name, "orkflow", "list"); code == 0 {
			break
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("engine %s did not become ready", name)
		}
		time.Sleep(250 * time.Millisecond)
	}
	s.note("engine %s up (release %q)", name, build)
}

// startOrker starts one `orker` of release build with id and pin capacity.
func (s *relStack) startOrker(id, build string, pins int) {
	s.t.Helper()
	work := filepath.Join(s.workRoot, id)
	if err := os.MkdirAll(work, 0o755); err != nil {
		s.t.Fatal(err)
	}
	s.start(id, s.orker, append(s.common(build),
		"ORK_ORKER_ID="+id,
		"ORK_ORKER_WORKDIR="+work,
		"ORK_ORKER_MAX_PINS="+strconv.Itoa(pins),
	)...)
	s.awaitOrker(id, func(o *orkstackv1.Orker) bool { return o != nil && o.GetUnavailableReason() == "" })
	s.note("orker %s up (release %q, max pins %d)", id, build, pins)
}

func (s *relStack) cliQuietOn(engine string, args ...string) (string, string, int) {
	full := append(append([]string(nil), args...), "--server", "http://"+s.engines[engine])
	return runBinary(s.ctx, s.t, s.orkctl, s.root, full...)
}

// cliOn runs orkctl against one engine and records the exchange.
func (s *relStack) cliOn(engine string, args ...string) (string, string, int) {
	s.t.Helper()
	started := time.Now()
	stdout, stderr, code := s.cliQuietOn(engine, args...)
	s.mu.Lock()
	fmt.Fprintf(&s.log, "[%7.1fs] $ orkctl %s   # engine %s\n%s", time.Since(s.started).Seconds(),
		strings.Join(args, " "), engine, stdout)
	if stderr != "" {
		fmt.Fprint(&s.log, stderr)
	}
	fmt.Fprintf(&s.log, "(exit %d, %s)\n", code, time.Since(started).Round(time.Millisecond))
	s.mu.Unlock()
	return stdout, stderr, code
}

func (s *relStack) cli(args ...string) (string, string, int) {
	s.t.Helper()
	return s.cliOn(s.first, args...)
}

func (s *relStack) mustCLI(args ...string) string {
	s.t.Helper()
	stdout, stderr, code := s.cli(args...)
	if code != 0 {
		s.t.Fatalf("orkctl %s: exit %d\n%s%s", strings.Join(args, " "), code, stdout, stderr)
	}
	return stdout
}

func (s *relStack) statusOn(engine string) *orkstackv1.ListReleasesResponse {
	s.t.Helper()
	stdout, stderr, code := s.cliQuietOn(engine, "release", "status", "--json")
	if code != 0 {
		s.t.Logf("release status --json: exit %d: %s", code, stderr)
		return nil
	}
	resp := &orkstackv1.ListReleasesResponse{}
	if err := api.UnmarshalJSON([]byte(stdout), resp); err != nil {
		s.t.Fatalf("release status --json is not a ListReleasesResponse: %v\n%s", err, stdout)
	}
	return resp
}

// awaitStatus polls `release status --json` until want holds.
func (s *relStack) awaitStatus(what string, want func(*orkstackv1.ListReleasesResponse) bool) *orkstackv1.ListReleasesResponse {
	s.t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	var last *orkstackv1.ListReleasesResponse
	for {
		last = s.statusOn(s.first)
		if last != nil && want(last) {
			s.note("observed: %s", what)
			return last
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("%s: never observed; last answer %v", what, last)
		}
		time.Sleep(time.Second)
	}
}

func releaseNamed(resp *orkstackv1.ListReleasesResponse, build string) *orkstackv1.Release {
	for _, r := range resp.GetReleases() {
		if r.GetBuildId() == build {
			return r
		}
	}
	return nil
}

// readyRelease is a release whose engine and an answered Orker are visible.
func readyRelease(build string, orkers int) func(*orkstackv1.ListReleasesResponse) bool {
	return func(resp *orkstackv1.ListReleasesResponse) bool {
		r := releaseNamed(resp, build)
		return r != nil && r.GetEngineReplicas() >= 1 && len(r.GetOrkers()) >= orkers
	}
}

func noBlockers(build string) func(*orkstackv1.ListReleasesResponse) bool {
	return func(resp *orkstackv1.ListReleasesResponse) bool {
		r := releaseNamed(resp, build)
		return r != nil && r.GetEngineReplicas() >= 1 && len(r.GetBlockers()) == 0
	}
}

func (s *relStack) orkerRow(id string) *orkstackv1.Orker {
	stdout, _, code := s.cliQuietOn(s.first, "orker", "get", id, "--json")
	resp := &orkstackv1.ListOrkersResponse{}
	if err := api.UnmarshalJSON([]byte(stdout), resp); err != nil || code != 0 || len(resp.GetOrkers()) == 0 {
		return nil
	}
	return resp.GetOrkers()[0]
}

func (s *relStack) awaitOrker(id string, want func(*orkstackv1.Orker) bool) *orkstackv1.Orker {
	s.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		row := s.orkerRow(id)
		if want(row) {
			return row
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("orker %s never reached the wanted state; last %v", id, row)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// gateLoop is a task's payload that waits for the test to open its gate.
const gateLoop = `i=0; while [ ! -e /orkstack/tmp/gate ] && [ $i -lt 1200 ]; do sleep 0.5; i=$((i+1)); done; echo gate open`

func (s *relStack) apply(name, yaml string) {
	s.t.Helper()
	path := filepath.Join(s.t.TempDir(), name+".ork.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		s.t.Fatal(err)
	}
	s.mustCLI("apply", path)
}

func (s *relStack) applyGated(name string) {
	s.apply(name, fmt.Sprintf(`name: %s
description: a run held in flight by a gate.
tasks:
  - id: gate
    uses: shell
    timeout_seconds: 900
    with:
      op: exec
      command: ["sh", "-c", %q]
  - id: after
    uses: shell
    needs: [gate]
    with:
      op: exec
      command: ["sh", "-c", "echo after"]
`, name, gateLoop))
}

func (s *relStack) applyPinnedGated(name string) {
	s.apply(name, fmt.Sprintf(`name: %s
description: a pinned run held in flight by a gate.
pins:
  build:
    timeout_seconds: 3600
tasks:
  - id: hold
    uses: shell
    pin: build
    run_as_orker_user: true
    timeout_seconds: 900
    with:
      op: exec
      command: ["sh", "-c", %q]
  - id: after
    uses: shell
    pin: build
    needs: [hold]
    run_as_orker_user: true
    with:
      op: exec
      command: ["sh", "-c", "echo after"]
`, name, gateLoop))
}

func (s *relStack) applyQuick(name string) {
	s.apply(name, fmt.Sprintf(`name: %s
description: one quick task.
tasks:
  - id: only
    uses: shell
    with:
      op: exec
      command: ["sh", "-c", "echo quick"]
`, name))
}

func (s *relStack) submit(name string) string {
	s.t.Helper()
	return parseRunID(s.t, s.mustCLI("run", "submit", name))
}

// openGates writes the gate file into every attempt's tmp directory under
// the Orkers' work roots: every gated task waiting now lets go.
func (s *relStack) openGates() {
	s.t.Helper()
	opened := 0
	_ = filepath.WalkDir(s.workRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && d.Name() == "tmp" {
			if _, statErr := os.Stat(filepath.Join(filepath.Dir(path), "input")); statErr == nil {
				if os.WriteFile(filepath.Join(path, "gate"), []byte("open\n"), 0o644) == nil {
					opened++
				}
			}
			return filepath.SkipDir
		}
		return nil
	})
	s.note("opened %d gate(s)", opened)
}

func (s *relStack) getRun(id string) *orkstackv1.Run {
	stdout, _, code := s.cliQuietOn(s.first, "run", "get", id, "--json")
	resp := &orkstackv1.GetRunResponse{}
	if code != 0 || api.UnmarshalJSON([]byte(stdout), resp) != nil {
		return nil
	}
	return resp.GetRun()
}

func (s *relStack) awaitRun(id string, timeout time.Duration) *orkstackv1.Run {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	var run *orkstackv1.Run
	for {
		run = s.getRun(id)
		switch run.GetStatus() {
		case orkstackv1.Status_STATUS_SUCCEEDED:
			return run
		case orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_CANCELED:
			s.t.Fatalf("run %s ended %s: %v", id, run.GetStatus(), run.GetError())
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("run %s did not succeed within %s; last %v", id, timeout, run)
		}
		time.Sleep(time.Second)
	}
}

// taskOrkers is task id -> the Orker that ran it.
func taskOrkers(run *orkstackv1.Run) map[string]string {
	out := map[string]string{}
	for _, task := range run.GetTaskRuns() {
		out[task.GetTaskId()] = task.GetOrkerId()
	}
	return out
}

func requireAllOn(t *testing.T, run *orkstackv1.Run, orker string) {
	t.Helper()
	for task, ran := range taskOrkers(run) {
		if ran != orker {
			t.Fatalf("run %s task %s ran on %q, want %q", run.GetId(), task, ran, orker)
		}
	}
}

// versionedPair starts v1 (engine + Orker) and makes it current, then starts
// v2's engine and Orker — the common start of most scenarios.
func (s *relStack) versionedPair(v1Pins, v2Pins int) {
	s.t.Helper()
	s.startEngine("ork-v1", "v1")
	s.startOrker("orker-v1", "v1", v1Pins)
	s.awaitStatus("v1 registered with its engine and Orker", readyRelease("v1", 1))
	s.mustCLI("release", "promote", "v1", "--yes")
	s.startEngine("ork-v2", "v2")
	s.startOrker("orker-v2", "v2", v2Pins)
	s.awaitStatus("v2 registered with its engine and Orker", readyRelease("v2", 1))
}

func TestIntegration_ReleaseRolloutThroughOrkctl(t *testing.T) {
	s := newRelStack(t, "rollout", 25*time.Minute)

	s.startEngine("ork-v1", "v1")
	s.startOrker("orker-v1", "v1", 0)
	status := s.awaitStatus("v1 is a candidate and nothing is current", func(r *orkstackv1.ListReleasesResponse) bool {
		v1 := releaseNamed(r, "v1")
		return readyRelease("v1", 1)(r) && v1.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_CANDIDATE
	})
	out := s.mustCLI("release", "status")
	if !strings.Contains(out, "current:     none") || !regexp.MustCompile(`(?m)^v1\s+candidate\s+1\s+1\s+main\s+0\s+ready$`).MatchString(out) {
		t.Fatalf("status before the first promotion:\n%s", out)
	}
	_ = status

	out = s.mustCLI("release", "promote", "v1", "--yes")
	if !strings.Contains(out, "release v1 is current.") || strings.Contains(out, "The change is recorded, but") {
		t.Fatalf("first promotion:\n%s", out)
	}
	// A role filter selects release rows; the deployment lines stay whole.
	out = s.mustCLI("release", "status", "--filter", "role=CURRENT")
	if !regexp.MustCompile(`(?m)^v1\s+current\s`).MatchString(out) || !strings.Contains(out, "deployment:") {
		t.Fatalf("status --filter role=CURRENT:\n%s", out)
	}
	if out = s.mustCLI("release", "status", "--filter", "role=CANDIDATE"); regexp.MustCompile(`(?m)^v1\s`).MatchString(out) {
		t.Fatalf("status --filter role=CANDIDATE lists the current release:\n%s", out)
	}
	s.note("release status --filter role=CURRENT lists v1 alone")

	s.applyGated("rel-gated")
	gated := s.submit("rel-gated")
	s.awaitOrker("orker-v1", func(o *orkstackv1.Orker) bool { return o.GetRunningTasks() >= 1 })
	s.note("run %s is in flight on orker-v1 (its gate task is running)", gated)

	s.startEngine("ork-v2", "v2")
	refusal := `pool "main" is in use (orker-v1 of release v1 serves it) and no Orker of release v2 serves it`
	s.awaitStatus("v2 not ready: pool main has no Orker of v2", func(r *orkstackv1.ListReleasesResponse) bool {
		v2 := releaseNamed(r, "v2")
		if v2 == nil || v2.GetEngineReplicas() < 1 {
			return false
		}
		for _, b := range v2.GetBlockers() {
			if strings.Contains(b.GetMessage(), refusal) {
				return true
			}
		}
		return false
	})
	out = s.mustCLI("release", "status")
	if !strings.Contains(out, "not ready (1 blocker)") || !strings.Contains(out, refusal) {
		t.Fatalf("status with v2's engine only:\n%s", out)
	}
	_, stderr, code := s.cli("release", "promote", "v2", "--yes")
	if code != 1 || !strings.Contains(stderr, "release v2 cannot take new runs yet:") || !strings.Contains(stderr, refusal) {
		t.Fatalf("promote v2 without an Orker: exit %d\n%s", code, stderr)
	}
	after := s.statusOn(s.first)
	if r := releaseNamed(after, "v1"); r.GetRole() != orkstackv1.ReleaseRole_RELEASE_ROLE_CURRENT {
		t.Fatalf("the refused promote changed the routing: %v", after)
	}
	s.note("EVIDENCE refused promote wrote nothing: v1 is still current")

	s.startOrker("orker-v2", "v2", 0)
	s.awaitStatus("v2 passes", noBlockers("v2"))
	out = s.mustCLI("release", "promote", "v2", "--yes")
	if !strings.Contains(out, "release v2 is current (was v1).") || strings.Contains(out, "The change is recorded, but") {
		t.Fatalf("promote v2:\n%s", out)
	}
	s.awaitStatus("v1 previous, finishing the gated run", func(r *orkstackv1.ListReleasesResponse) bool {
		v1 := releaseNamed(r, "v1")
		return v1.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_PREVIOUS && v1.GetRunsInFlight() == 1 &&
			len(v1.GetRuns()) == 1 && v1.GetRuns()[0].GetId() == gated
	})
	out = s.mustCLI("release", "status")
	if !strings.Contains(out, "v1 is still finishing work:") || !strings.Contains(out, gated) {
		t.Fatalf("status after the promotion:\n%s", out)
	}

	s.applyQuick("rel-quick")
	fresh := s.submit("rel-quick")
	freshRun := s.awaitRun(fresh, 3*time.Minute)
	requireAllOn(t, freshRun, "orker-v2")
	s.note("EVIDENCE run %s, submitted after the promotion, ran on %v", fresh, taskOrkers(freshRun))

	s.openGates()
	gatedRun := s.awaitRun(gated, 3*time.Minute)
	requireAllOn(t, gatedRun, "orker-v1")
	s.note("EVIDENCE run %s, started on v1, finished there: %v", gated, taskOrkers(gatedRun))

	began := time.Now()
	_, stderr, code = s.cli("release", "wait", "v1", "--timeout", "10m")
	if code != 0 {
		t.Fatalf("release wait v1: exit %d\n%s", code, stderr)
	}
	s.note("EVIDENCE release wait v1 exited 0 after %s (the cluster's drain refresh is minutes)",
		time.Since(began).Round(time.Second))
}

func TestIntegration_ReleaseRampSplitsNewRuns(t *testing.T) {
	s := newRelStack(t, "ramp", 15*time.Minute)
	s.versionedPair(0, 0)
	s.awaitStatus("v2 passes", noBlockers("v2"))
	out := s.mustCLI("release", "ramp", "v2", "--percent", "50")
	if !strings.Contains(out, "release v2 now takes 50% of new runs; v1 (current) takes the rest.") {
		t.Fatalf("ramp:\n%s", out)
	}
	s.applyQuick("rel-split")
	var ids []string
	for i := 0; i < 20; i++ {
		ids = append(ids, s.submit("rel-split"))
	}
	ran := map[string]int{}
	for _, id := range ids {
		for _, orker := range taskOrkers(s.awaitRun(id, 5*time.Minute)) {
			ran[orker]++
		}
	}
	s.note("EVIDENCE 20 runs at a 50%% ramp ran on %v", ran)
	if ran["orker-v1"] == 0 || ran["orker-v2"] == 0 {
		t.Fatalf("a 50%% ramp sent every run to one release: %v", ran)
	}

	out = s.mustCLI("release", "ramp", "v2", "--percent", "0")
	if !strings.Contains(out, "release v2 no longer takes new runs;") {
		t.Fatalf("ramp 0:\n%s", out)
	}
	ids = nil
	for i := 0; i < 10; i++ {
		ids = append(ids, s.submit("rel-split"))
	}
	after := map[string]int{}
	for _, id := range ids {
		for _, orker := range taskOrkers(s.awaitRun(id, 5*time.Minute)) {
			after[orker]++
		}
	}
	s.note("EVIDENCE 10 runs after the ramp stopped ran on %v", after)
	if after["orker-v2"] != 0 || after["orker-v1"] != 10 {
		t.Fatalf("runs after the ramp stopped: %v", after)
	}
	s.awaitStatus("v2 previous", func(r *orkstackv1.ListReleasesResponse) bool {
		return releaseNamed(r, "v2").GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_PREVIOUS
	})
	s.mustCLI("release", "status")
}

func TestIntegration_ReleaseRollbackKeepsTheRolledBackReleasesRunsWhereTheyAre(t *testing.T) {
	s := newRelStack(t, "rollback", 15*time.Minute)
	since := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	s.versionedPair(0, 0)
	s.awaitStatus("v2 passes", noBlockers("v2"))
	s.mustCLI("release", "promote", "v2", "--yes")

	s.applyGated("rel-gated")
	gated := s.submit("rel-gated")
	s.awaitOrker("orker-v2", func(o *orkstackv1.Orker) bool { return o.GetRunningTasks() >= 1 })
	s.awaitStatus("v1 can be rolled back to", noBlockers("v1"))

	out := s.mustCLI("release", "rollback", "v1", "--yes")
	if !strings.Contains(out, "release v1 is current again (was v2).") {
		t.Fatalf("rollback:\n%s", out)
	}
	s.applyQuick("rel-quick")
	quick := s.awaitRun(s.submit("rel-quick"), 3*time.Minute)
	requireAllOn(t, quick, "orker-v1")
	s.note("EVIDENCE a run submitted after the rollback ran on %v", taskOrkers(quick))

	s.openGates()
	gatedRun := s.awaitRun(gated, 3*time.Minute)
	requireAllOn(t, gatedRun, "orker-v2")
	s.note("EVIDENCE run %s, started on v2, finished there after the rollback: %v", gated, taskOrkers(gatedRun))

	s.startEngine("ork-v3", "v3")
	s.awaitStatus("v3 is a candidate", func(r *orkstackv1.ListReleasesResponse) bool {
		v3 := releaseNamed(r, "v3")
		return v3 != nil && v3.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_CANDIDATE
	})
	_, stderr, code := s.cli("release", "rollback", "v3", "--yes")
	if code != 1 || !strings.Contains(stderr, `rollback returns to a release that was current before; v3 is candidate`) {
		t.Fatalf("rollback v3: exit %d\n%s", code, stderr)
	}
	listed := s.mustCLI("admin", "audit", "--filter", "action^release.", "--filter", "at>="+since, "--json")
	resp := &orkstackv1.ListAuditRecordsResponse{}
	if err := api.UnmarshalJSON([]byte(listed), resp); err != nil {
		t.Fatalf("admin audit --json: %v", err)
	}
	for _, rec := range resp.GetRecords() {
		if rec.GetTarget() == "v3" {
			t.Fatalf("a refused-before-sending rollback reached the engine: %v", rec)
		}
	}
	s.note("EVIDENCE %d release.* audit records, none for v3", len(resp.GetRecords()))
}

func TestIntegration_ACordonedOrkerDoesNotServeARelease(t *testing.T) {
	s := newRelStack(t, "cordon", 10*time.Minute)
	s.versionedPair(0, 0)
	s.awaitStatus("v2 passes", noBlockers("v2"))
	s.mustCLI("orker", "cordon", "orker-v2", "--reason", "kernel upgrade")
	want := `no uncordoned Orker of release v2 serves it (orker-v2 of release v2 is cordoned: "kernel upgrade")`
	s.awaitStatus("v2 blocked by its cordon", func(r *orkstackv1.ListReleasesResponse) bool {
		for _, b := range releaseNamed(r, "v2").GetBlockers() {
			if strings.Contains(b.GetMessage(), want) {
				return true
			}
		}
		return false
	})
	_, stderr, code := s.cli("release", "promote", "v2", "--yes")
	if code != 1 || !strings.Contains(stderr, want) {
		t.Fatalf("promote with v2's only Orker cordoned: exit %d\n%s", code, stderr)
	}
	s.mustCLI("orker", "uncordon", "orker-v2")
	s.awaitStatus("v2 passes again", noBlockers("v2"))
	out := s.mustCLI("release", "promote", "v2", "--yes")
	if !strings.Contains(out, "release v2 is current (was v1).") {
		t.Fatalf("promote after uncordon:\n%s", out)
	}
}

func TestIntegration_APinHostIsRequiredWherePinsAreHosted(t *testing.T) {
	s := newRelStack(t, "pin-host", 10*time.Minute)
	s.versionedPair(1, 0)
	want := `pool "main" hosts pins (orker-v1 of release v1 is a pin host) and no Orker of release v2 in it hosts pins`
	s.awaitStatus("v2 blocked: no pin host", func(r *orkstackv1.ListReleasesResponse) bool {
		for _, b := range releaseNamed(r, "v2").GetBlockers() {
			if b.GetKind() == orkstackv1.ReleaseBlockerKind_RELEASE_BLOCKER_KIND_PINS_NOT_HOSTED && strings.Contains(b.GetMessage(), want) {
				return true
			}
		}
		return false
	})
	_, stderr, code := s.cli("release", "promote", "v2", "--yes")
	if code != 1 || !strings.Contains(stderr, want) {
		t.Fatalf("promote without a pin host: exit %d\n%s", code, stderr)
	}
	s.stopProc("orker-v2")
	s.startOrker("orker-v2", "v2", 1)
	s.awaitStatus("v2 passes with a pin host", noBlockers("v2"))
	out := s.mustCLI("release", "promote", "v2", "--yes")
	if !strings.Contains(out, "release v2 is current (was v1).") {
		t.Fatalf("promote with a pin host:\n%s", out)
	}
}

func TestIntegration_ReleaseStatusNamesAnOpenPinAndItsHost(t *testing.T) {
	s := newRelStack(t, "open-pin", 20*time.Minute)
	s.startEngine("ork-v1", "v1")
	s.startOrker("orker-v1", "v1", 1)
	s.awaitStatus("v1 registered", readyRelease("v1", 1))
	s.mustCLI("release", "promote", "v1", "--yes")
	s.applyPinnedGated("rel-pinned")
	pinned := s.submit("rel-pinned")
	s.awaitOrker("orker-v1", func(o *orkstackv1.Orker) bool { return o.GetOpenPins() == 1 && o.GetRunningTasks() >= 1 })
	s.note("run %s holds a pin on orker-v1", pinned)

	s.startEngine("ork-v2", "v2")
	s.startOrker("orker-v2", "v2", 1)
	s.awaitStatus("v2 passes", noBlockers("v2"))
	s.mustCLI("release", "promote", "v2", "--yes")

	pinLine := `pin "build" of run ` + pinned + ` on orker-v1`
	s.awaitStatus("v1 previous holding the pin", func(r *orkstackv1.ListReleasesResponse) bool {
		v1 := releaseNamed(r, "v1")
		return v1.GetRole() == orkstackv1.ReleaseRole_RELEASE_ROLE_PREVIOUS && len(v1.GetOrkers()) == 1 &&
			len(v1.GetOrkers()[0].GetPins()) == 1 && v1.GetOrkers()[0].GetPins()[0].GetPin() == "build"
	})
	out := s.mustCLI("release", "status")
	if !strings.Contains(out, pinLine) {
		t.Fatalf("status does not name the pin and its host:\n%s", out)
	}
	stdout, _, code := s.cli("release", "wait", "v1", "--timeout", "20s")
	if code != exitDrainTimedOut || !strings.Contains(stdout, pinLine) ||
		!strings.Contains(stdout, "stopping orker-v1 fails it with PIN_HOST_LOST.") {
		t.Fatalf("wait --timeout 20s: exit %d\n%s", code, stdout)
	}
	s.note("EVIDENCE release wait v1 --timeout 20s exited 4 naming the pin and orker-v1")

	s.openGates()
	run := s.awaitRun(pinned, 3*time.Minute)
	requireAllOn(t, run, "orker-v1")
	began := time.Now()
	if _, stderr, code := s.cli("release", "wait", "v1", "--timeout", "10m"); code != 0 {
		t.Fatalf("release wait v1 after the pin closed: exit %d\n%s", code, stderr)
	}
	s.note("EVIDENCE release wait v1 exited 0 after %s, once the pin closed and the cluster drained it",
		time.Since(began).Round(time.Second))
}

func TestIntegration_FirstPromotionWaitsForRunsStartedBeforeVersioning(t *testing.T) {
	s := newRelStack(t, "first-promotion", 12*time.Minute)
	s.startEngine("ork-unversioned", "")
	s.startOrker("orker-u", "", 0)
	s.applyGated("rel-gated")
	gated := s.submit("rel-gated")
	s.awaitOrker("orker-u", func(o *orkstackv1.Orker) bool { return o.GetRunningTasks() >= 1 })
	s.note("run %s, started before any release existed, is in flight", gated)

	s.startEngine("ork-v1", "v1")
	s.startOrker("orker-v1", "v1", 0)
	s.awaitStatus("v1 registered", readyRelease("v1", 1))
	want := "1 run(s) started before any release was current"
	s.awaitStatus("v1 blocked by the unversioned run", func(r *orkstackv1.ListReleasesResponse) bool {
		for _, b := range releaseNamed(r, "v1").GetBlockers() {
			if strings.Contains(b.GetMessage(), want) {
				return true
			}
		}
		return false
	})
	_, stderr, code := s.cli("release", "promote", "v1", "--yes")
	if code != 1 || !strings.Contains(stderr, want) {
		t.Fatalf("first promotion with an unversioned run in flight: exit %d\n%s", code, stderr)
	}
	s.openGates()
	run := s.awaitRun(gated, 3*time.Minute)
	requireAllOn(t, run, "orker-u")
	s.awaitStatus("the unversioned run is no longer counted", noBlockers("v1"))
	out := s.mustCLI("release", "promote", "v1", "--yes")
	if !strings.Contains(out, "release v1 is current.") {
		t.Fatalf("first promotion after the run finished:\n%s", out)
	}
}

func TestIntegration_ListReleasesIsAnsweredByAReplicaThatPromotedNothing(t *testing.T) {
	s := newRelStack(t, "two-replicas", 10*time.Minute)
	s.versionedPair(0, 0)
	s.awaitStatus("v2 passes", noBlockers("v2"))
	s.cliOn("ork-v1", "release", "promote", "v2", "--yes")
	var a, b *orkstackv1.ListReleasesResponse
	for attempt := 0; attempt < 15; attempt++ {
		a, b = s.statusOn("ork-v1"), s.statusOn("ork-v2")
		if a != nil && b != nil {
			a.ObservedAt, b.ObservedAt = nil, nil
			if proto.Equal(a, b) {
				break
			}
		}
		time.Sleep(time.Second)
	}
	if !proto.Equal(a, b) {
		t.Fatalf("two replicas answered differently:\n%v\n%v", a, b)
	}
	encoded, _ := marshalJSON(a)
	s.note("EVIDENCE status --json through ork-v1 (release v1) and ork-v2 (release v2) are equal except observedAt:\n%s", encoded)
}

func TestIntegration_ReleaseWritesAreAudited(t *testing.T) {
	s := newRelStack(t, "audit", 10*time.Minute)
	since := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	s.versionedPair(0, 0)
	s.awaitStatus("v2 passes", noBlockers("v2"))
	s.mustCLI("release", "ramp", "v2", "--percent", "10")
	listed := s.mustCLI("admin", "audit", "--filter", "action^release.", "--filter", "at>="+since, "--json")
	resp := &orkstackv1.ListAuditRecordsResponse{}
	if err := api.UnmarshalJSON([]byte(listed), resp); err != nil {
		t.Fatalf("admin audit --json: %v", err)
	}
	var promoted, ramped *orkstackv1.AuditRecord
	for _, rec := range resp.GetRecords() {
		switch rec.GetAction() {
		case "release.promote":
			promoted = rec
		case "release.ramp":
			ramped = rec
		}
	}
	if promoted == nil || promoted.GetTarget() != "v1" || promoted.GetDetail() != "current - -> v1" || !promoted.GetSucceeded() {
		t.Fatalf("promote record = %v", promoted)
	}
	if ramped == nil || ramped.GetTarget() != "v2" || ramped.GetDetail() != "percent=10 was -@0" || !ramped.GetSucceeded() {
		t.Fatalf("ramp record = %v", ramped)
	}
	s.mustCLI("admin", "audit", "--filter", "action^release.", "--filter", "at>="+since)
	s.note("EVIDENCE audit: %s %s %q; %s %s %q", promoted.GetAction(), promoted.GetTarget(), promoted.GetDetail(),
		ramped.GetAction(), ramped.GetTarget(), ramped.GetDetail())
}

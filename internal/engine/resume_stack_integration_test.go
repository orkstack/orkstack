//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The harness of the resume suite (E85 PR9): a chaosStack whose engine
// replicas run a resumed run's activities too, the real ResumeRun over it
// with the production claim workflow, and Orkers that are real processes of
// the built orker binary, so one killed is a process killed.
//
// The service is built here rather than served by the ork binary, which
// resumes no runs while engine.ResumeShipped is off. Only its clock is the
// test's: it is moved past windows that take minutes by design, and each test
// that moves it says so. The cluster, the Orkers and the engines keep real
// time.

// resumeStack is one such deployment.
type resumeStack struct {
	*chaosStack
	stores  *realOrkerStores
	records *artifacts.RecordStore
	endings artifacts.RunEndingStore // where each ending of a run is archived
	// created is each run's creation as it read before it was resumed.
	created map[string]time.Time
}

// pastContainers is how far the service's clock is moved where a test is not
// about the containers of the run it resumes: past the instant the last of
// them must be gone by, a heartbeat timeout, a grace and 30 s after its
// attempt ended.
const pastContainers = 5 * time.Minute

func newResumeStack(t *testing.T, ctx context.Context, scenario string) *resumeStack {
	t.Helper()
	cfg := s3Config()
	requireObjectStore(t, cfg)
	archive, err := artifacts.NewRunArchive(cfg)
	must.OKf(t, err, "run archive")
	endings, ok := archive.(artifacts.RunEndingStore)
	if !ok {
		t.Fatalf("run archive %T keeps one ending a run", archive)
	}
	return resumeStackOn(t, ctx, newChaosStackWith(t, ctx, scenario, cfg, archive), endings)
}

// resumeStackOn is the suite's stack over c, whose run archive is endings.
func resumeStackOn(t *testing.T, ctx context.Context, c *chaosStack, endings artifacts.RunEndingStore) *resumeStack {
	t.Helper()
	s := &resumeStack{chaosStack: c, endings: endings, created: map[string]time.Time{}}
	s.reader.WithEndings(s.archive)
	s.stores = s.realStores()
	records, err := artifacts.NewRecordStore(ctx, s.cfg)
	must.OKf(t, err, "record store")
	s.records = records
	return s
}

// resumeFaults are the proxies a replica reads the object store through in a
// resumed run; nil is the store itself.
type resumeFaults struct {
	results *faultproxy.Proxy // the commits it reads back
	plans   *faultproxy.Proxy // the resume's records
}

// startResumingReplica starts an engine replica with a resumed run's local
// activities beside the run's own, registered as cmd/ork registers them.
func (s *resumeStack) startResumingReplica(name string, o replicaOptions, f resumeFaults) *chaosReplica {
	s.t.Helper()
	if o.client == nil {
		o.client = s.dial(name, o.temporal)
	}
	o.extra = append(o.extra, s.registeringResume(o.client, f))
	return s.startReplica(name, o)
}

// registeringResume registers a resumed run's activities on a run worker
// whose client is c.
func (s *resumeStack) registeringResume(c client.Client, f resumeFaults) func(worker.Worker) {
	s.t.Helper()
	commits, values, records := s.stores.commits, s.stores.store, s.records
	if f.results != nil {
		commits, values = s.storesVia(f.results)
	}
	if f.plans != nil {
		cfg := s.cfg
		cfg.S3Endpoint = "http://" + f.plans.Addr()
		var err error
		records, err = artifacts.NewRecordStore(s.ctx, cfg)
		must.OKf(s.t, err, "record store through the proxy")
	}
	return func(w worker.Worker) {
		w.RegisterActivityWithOptions(engine.NewSnapshotActivities(s.store).WriteResumeEnding,
			activity.RegisterOptions{Name: engine.ActivityWriteResumeEnding})
		resume := engine.NewResumeActivities(commits, values).WithHistory(c.WorkflowService(), s.namespace, records).
			WithRecordReads(records)
		w.RegisterActivityWithOptions(resume.ReadResetFacts, activity.RegisterOptions{Name: engine.ActivityReadResetFacts})
		w.RegisterActivityWithOptions(resume.LoadResumePlan, activity.RegisterOptions{Name: engine.ActivityLoadResumePlan})
		w.RegisterActivityWithOptions(resume.AdoptRecorded, activity.RegisterOptions{Name: engine.ActivityAdoptRecorded})
	}
}

// storesVia are the commit and artifact stores dialing proxy instead of the
// object store.
func (s *resumeStack) storesVia(proxy *faultproxy.Proxy) (artifacts.CommitStore, artifacts.Store) {
	s.t.Helper()
	cfg := s.cfg
	cfg.S3Endpoint = "http://" + proxy.Addr()
	commits, err := artifacts.NewCommitStore(s.ctx, cfg)
	must.OKf(s.t, err, "commit store through the proxy")
	store, err := artifacts.New(cfg)
	must.OKf(s.t, err, "artifact store through the proxy")
	return commits, store
}

// startEngine starts one resuming replica and the projection worker.
func (s *resumeStack) startEngine() *chaosReplica {
	s.t.Helper()
	r := s.startResumingReplica("replica-a", replicaOptions{}, resumeFaults{})
	s.startProjection(nil)
	return r
}

// resumePlan is the routing plan of this suite's runs: every task on the
// stack's task channel, its commits in the stack's bucket, and one history.
func (s *resumeStack) resumePlan() engine.RunPlan {
	s.t.Helper()
	plan := s.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, nil)
	root, err := artifacts.CommitRoot(s.cfg)
	must.OK(s.t, err)
	plan.CommitRoot = root
	plan.Limits = engine.DefaultLimits()
	plan.TransferBudgetSeconds = 300
	return plan
}

// ── documents ───────────────────────────────────────────────────────────────

// resumeTask is one task of a document of this suite.
type resumeTask struct {
	id       string
	needs    []string
	pin      string
	join     string
	attempts int // maximum_attempts of its own; 0 is 1
	grace    int32
	// command is the container's shell command, for a run on real Orkers.
	command string
}

// succeedCommand is a container that succeeds; failUntilResumed is one that
// fails in the run as it was submitted and succeeds when a resume runs it
// again, told apart by the key it is given.
const (
	succeedCommand = `printf '{"ok":true}' > /orkstack/output/values.json`

	failUntilResumed = `grep -q '"idempotencyKey": *"[^"]*@r' /orkstack/input/task.json || exit 1; ` + succeedCommand
)

// resumeDocument compiles tasks. An interruption never runs one again: what
// ends an attempt is the task's own failure.
func resumeDocument(t *testing.T, name string, tasks ...resumeTask) *orkstackv1.Orkflow {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\n", name)
	if slices.ContainsFunc(tasks, func(tk resumeTask) bool { return tk.pin != "" }) {
		b.WriteString("pins:\n")
	}
	var pins []string
	for _, tk := range tasks {
		if tk.pin != "" && !slices.Contains(pins, tk.pin) {
			pins = append(pins, tk.pin)
			fmt.Fprintf(&b, "  %s:\n    timeout_seconds: 2400\n", tk.pin)
		}
	}
	b.WriteString("tasks:\n")
	for _, tk := range tasks {
		fmt.Fprintf(&b, "  - id: %s\n    uses: shell\n    retry:\n      maximum_attempts: %d\n      max_interruptions: 0\n    with:\n      op: exec\n",
			tk.id, max(tk.attempts, 1))
		if len(tk.needs) > 0 {
			b.WriteString("    needs:\n")
			for _, need := range tk.needs {
				fmt.Fprintf(&b, "      - %s\n", need)
			}
		}
		if tk.pin != "" {
			fmt.Fprintf(&b, "    pin: %s\n", tk.pin)
		}
		if tk.join != "" {
			fmt.Fprintf(&b, "    join: %s\n", tk.join)
		}
		if tk.grace > 0 {
			fmt.Fprintf(&b, "    cancellation_grace_period_seconds: %d\n", tk.grace)
		}
	}
	wf := compile(t, b.String())
	if slices.ContainsFunc(tasks, func(tk resumeTask) bool { return tk.command != "" }) {
		image := pinnedAlpineForCommitE2E(t)
		for i, tk := range wf.GetTasks() {
			if tasks[i].command == "" {
				t.Fatalf("task %s of %s has no command", tk.GetId(), name)
			}
			tk.Uses = image
			tk.Image = &orkstackv1.ImageRef{Reference: image, Pinned: image}
			tk.Command = []string{"/bin/sh", "-c", tasks[i].command}
			tk.TimeoutSeconds = 900
			tk.Network = orkstackv1.NetworkMode_NETWORK_MODE_NONE
		}
	}
	wf.Namespace = "default"
	stampDigest(t, wf)
	return wf
}

// ── in-process Orkers ───────────────────────────────────────────────────────

// committing is an Orker that runs no container: each task writes its commit
// to the stack's bucket and succeeds, unless fail answers an error for it.
// A commit already there is adopted, as a real Orker's retry adopts it.
func (s *resumeStack) committing(fail func(in *orkstackv1.ExecuteTaskInput) error) chaosBehavior {
	return func(ctx context.Context, _ *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if fail != nil {
			if err := fail(in); err != nil {
				return nil, err
			}
		}
		record, err := s.stores.commits.Create(ctx, in.GetCommitUri(), &orkstackv1.ActivityCommit{
			SchemaVersion: artifacts.CommitSchemaVersion, Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			RunId: in.GetRunId(), TaskId: in.GetTaskId(), OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
			IdempotencyKey: in.GetIdempotencyKey(), Image: in.GetImage(), Attempt: 1, OrkerId: orker,
		})
		if errors.Is(err, artifacts.ErrPreconditionFailed) {
			record, err = s.stores.commits.Get(ctx, in.GetCommitUri())
		}
		if err != nil {
			return nil, err
		}
		result := succeeded(in.GetTaskId(), map[string]any{"ok": true})
		result.OrkerId = orker
		result.Commit = &orkstackv1.CommitRef{Uri: in.GetCommitUri(), Sha256: record.SHA256}
		return result, nil
	}
}

// failsUntilResumed fails ids in the run as it was submitted, and lets them
// succeed when a resume runs them again.
func failsUntilResumed(ids ...string) func(*orkstackv1.ExecuteTaskInput) error {
	return func(in *orkstackv1.ExecuteTaskInput) error {
		if slices.Contains(ids, in.GetTaskId()) && !strings.Contains(in.GetIdempotencyKey(), "@r") {
			return temporal.NewNonRetryableApplicationError(in.GetTaskId()+" failed", "ActivityFailed", nil)
		}
		return nil
	}
}

// dispatches keeps the idempotency key of every attempt, in order.
type dispatches struct {
	mu   sync.Mutex
	keys []string
}

// noting is behavior, with each attempt's key kept.
func (d *dispatches) noting(behavior chaosBehavior) chaosBehavior {
	return func(ctx context.Context, s *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		d.mu.Lock()
		d.keys = append(d.keys, in.GetIdempotencyKey())
		d.mu.Unlock()
		return behavior(ctx, s, orker, in)
	}
}

// of counts the attempts made under runID's key for taskID: as submitted when
// scope is 0, as the resume of that scope runs it otherwise.
func (d *dispatches) of(runID, taskID string, scope int64) int {
	want := "default/" + runID + "/" + taskID
	if scope > 0 {
		want += fmt.Sprintf("@r%d", scope)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, k := range d.keys {
		if k == want {
			n++
		}
	}
	return n
}

// ── Orker processes ─────────────────────────────────────────────────────────

// orkerProcess is a running orker binary.
type orkerProcess struct {
	id       string
	workRoot string
	cmd      *exec.Cmd
	proxy    *faultproxy.Proxy // in front of the cluster
	exited   chan struct{}
	log      *lockedBuffer
}

// lockedBuffer is a process's log.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// startOrkerProcess starts an Orker named name on the default pool, reaching
// the cluster through a proxy of its own.
func (s *resumeStack) startOrkerProcess(name string, env ...string) *orkerProcess {
	s.t.Helper()
	return s.bootOrkerProcess(&orkerProcess{
		id:       proof.OrkerID("resume-" + chaosLabel(s.scenario) + "-" + name),
		workRoot: chaosWorkRoot(s.t, name), proxy: s.temporalProxy(),
	}, env...)
}

// restartOrkerProcess boots o again under its identity and on its work root,
// as an operator restarts an Orker that died.
func (s *resumeStack) restartOrkerProcess(o *orkerProcess, env ...string) *orkerProcess {
	s.t.Helper()
	return s.bootOrkerProcess(&orkerProcess{id: o.id, workRoot: o.workRoot, proxy: o.proxy}, env...)
}

func (s *resumeStack) bootOrkerProcess(o *orkerProcess, env ...string) *orkerProcess {
	t := s.t
	t.Helper()
	o.exited, o.log = make(chan struct{}), &lockedBuffer{}
	o.cmd = exec.CommandContext(s.ctx, testbin.Path(t, "./cmd/orker"))
	o.cmd.Env = testbin.OrkerEnv(append(append(testbin.StackEnv(os.Environ()),
		"ORK_TEMPORAL_ADDRESS="+o.proxy.Addr(),
		"ORK_TEMPORAL_NAMESPACE="+s.namespace,
		"ORK_ORKER_QUEUE="+s.orkerQueue,
		"ORK_ORKER_WORKDIR="+o.workRoot,
		"ORK_ORKER_ID="+o.id,
		"ORK_ORKER_HEARTBEAT_INTERVAL=1s",
		"ORK_ARTIFACT_BACKEND=s3",
	), env...))
	o.cmd.Stdout, o.cmd.Stderr = o.log, o.log
	must.OKf(t, o.cmd.Start(), "start orker %s", o.id)
	go func() { _ = o.cmd.Wait(); close(o.exited) }()
	t.Cleanup(func() {
		_ = o.cmd.Process.Signal(os.Interrupt)
		select {
		case <-o.exited:
		case <-time.After(20 * time.Second):
			_ = o.cmd.Process.Kill()
			<-o.exited
		}
		if t.Failed() {
			t.Logf("orker %s log:\n%s", o.id, o.log)
		}
		s.removeContainers("ork.orker=" + o.id)
	})
	s.eventually(2*time.Minute, "orker "+o.id+" to answer", func() (bool, string) {
		select {
		case <-o.exited:
			t.Fatalf("orker %s exited while it started:\n%s", o.id, o.log)
		default:
		}
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
		defer cancel()
		answer, err := s.starter.DescribeOrkerByID(ctx, o.id)
		return err == nil && answer.GetOrkerId() == o.id, fmt.Sprint(err)
	})
	s.transcript.step("orker %s started, pid %d", o.id, o.cmd.Process.Pid)
	return o
}

// kill ends o as a host that lost power would: nothing of it runs again, and
// its containers are the daemon's.
func (s *resumeStack) kill(o *orkerProcess) {
	s.t.Helper()
	must.OKf(s.t, o.cmd.Process.Signal(syscall.SIGKILL), "SIGKILL orker %s", o.id)
	<-o.exited
	s.transcript.step("orker %s killed", o.id)
}

// containers counts the running containers label selects.
func (s *resumeStack) containers(label string) int {
	s.t.Helper()
	out, err := exec.CommandContext(s.ctx, "docker", "ps", "--quiet", "--filter", "label="+label).Output()
	must.OKf(s.t, err, "docker ps")
	return len(strings.Fields(string(out)))
}

// removeContainers removes what this test's own label selects, so no
// container of a killed Orker outlives its test.
func (s *resumeStack) removeContainers(label string) {
	out, err := exec.Command("docker", "ps", "--all", "--quiet", "--filter", "label="+label).Output()
	if err != nil {
		s.t.Logf("cleanup: list the containers of %s: %v", label, err)
		return
	}
	if ids := strings.Fields(string(out)); len(ids) > 0 {
		if msg, err := exec.Command("docker", append([]string{"rm", "--force"}, ids...)...).CombinedOutput(); err != nil {
			s.t.Logf("cleanup: remove the containers of %s: %v: %s", label, err, msg)
		}
	}
}

func (s *resumeStack) awaitContainers(label string, want int, within time.Duration) {
	s.t.Helper()
	s.eventually(within, fmt.Sprintf("%d containers of %s", want, label), func() (bool, string) {
		n := s.containers(label)
		return n == want, fmt.Sprintf("%d running", n)
	})
}

// eventually polls done until it answers true, and fails naming what it
// waited for and the last it saw.
func (s *resumeStack) eventually(within time.Duration, what string, done func() (bool, string)) {
	s.t.Helper()
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		ok, last := done()
		if ok {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			s.t.Fatalf("waited %s for %s; last: %s", within, what, last)
		case <-s.ctx.Done():
			s.t.Fatalf("the test ended while it waited for %s; last: %s", what, last)
		}
	}
}

// ── the service ─────────────────────────────────────────────────────────────

// movedClock is the service's clock: real time, plus what the test moved it
// by.
type movedClock struct {
	mu sync.Mutex
	by time.Duration
}

func (c *movedClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.by)
}

func (c *movedClock) move(by time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.by += by
}

// moveTo moves the clock forward to at; it is never moved back.
func (c *movedClock) moveTo(at time.Time) {
	if by := at.Sub(c.now()); by > 0 {
		c.move(by)
	}
}

// resumeDoor is ResumeRun as one engine replica answers it.
type resumeDoor struct {
	s     *resumeStack
	svc   *api.Service
	clock *movedClock

	mu        sync.Mutex
	anomalies []string
}

func (r *resumeDoor) RecordResumeRequest(context.Context, telemetry.ResumeRequest)     {}
func (r *resumeDoor) RecordResumePreflight(context.Context, string, float64)           {}
func (r *resumeDoor) RecordResumeConfirmed(context.Context, telemetry.ResumeConfirmed) {}

func (r *resumeDoor) RecordResumeAnomaly(_ context.Context, _, kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.anomalies = append(r.anomalies, kind)
}

func (r *resumeDoor) anomalyCount(kind string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, k := range r.anomalies {
		if k == kind {
			n++
		}
	}
	return n
}

// serveResume is the door over the stack, with its clock moved by by.
// configure, when set, changes the configuration last.
func (s *resumeStack) serveResume(by time.Duration, configure ...func(*api.Config)) *resumeDoor {
	s.t.Helper()
	r := &resumeDoor{s: s, clock: &movedClock{by: by}}
	settings := api.DefaultResumeSettings()
	// Room for a slow host: the wait for the run to carry on.
	settings.ConfirmTimeout = 2 * time.Minute
	cfg := api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(), Reader: s.reader, Starter: s.starter, RawQueue: s.orkerQueue,
		Runs: s.runs, Bus: s.bus, Archive: s.archive,
		Artifacts: s.cfg, ArtifactStore: s.stores.store, Commits: s.stores.commits, Attempts: s.stores.attempts, Logs: s.stores.logs,
		Limits: engine.DefaultLimits(), Pools: servedPools{}, Fleet: s.starter,
		Resume: s.counting(), ResumeSettings: settings, ResumeObserver: r,
		ResumeRecords: s.records, ResumeSnapshots: s.store,
		ResumeEndings: s.endings, ResumeArchiver: engine.NewHistoryArchiver(s.reader, s.archive),
		ResumeClaims: engine.NewTemporalIdempotency(s.control, s.runQueue, api.ResumeClaimTTL),
		Now:          r.clock.now,
	}
	for _, change := range configure {
		change(&cfg)
	}
	r.svc = api.NewService(cfg)
	return r
}

// try is one ResumeRun: answered, refused, or an error that is no refusal.
func (r *resumeDoor) try(ctx context.Context, req *orkstackv1.ResumeRunRequest) (*orkstackv1.ResumeRunResponse, *orkstackv1.ResumeRefusal, error) {
	resp, err := r.svc.ResumeRun(ctx, connect.NewRequest(req))
	if err == nil {
		return resp.Msg, nil, nil
	}
	var wire *connect.Error
	if errors.As(err, &wire) {
		for _, d := range wire.Details() {
			if v, derr := d.Value(); derr == nil {
				if refused, ok := v.(*orkstackv1.ResumeRefusal); ok {
					return nil, refused, nil
				}
			}
		}
	}
	return nil, nil, err
}

// ask is try where an error that is no refusal fails the test.
func (r *resumeDoor) ask(req *orkstackv1.ResumeRunRequest) (*orkstackv1.ResumeRunResponse, *orkstackv1.ResumeRefusal) {
	r.s.t.Helper()
	resp, refused, err := r.try(r.s.ctx, req)
	if err != nil {
		r.s.t.Fatalf("ResumeRun = %v", err)
	}
	return resp, refused
}

// preview is the plan a person is shown, which must not be refused.
func (r *resumeDoor) preview(runID string) *orkstackv1.ResumeRunResponse {
	r.s.t.Helper()
	resp, refused := r.ask(&orkstackv1.ResumeRunRequest{RunId: runID, DryRun: true})
	if refused != nil {
		r.s.t.Fatalf("the preview of %s was refused: %v", runID, refused)
	}
	return resp
}

// requireRefused fails unless req is refused with reason and cause.
func (r *resumeDoor) requireRefused(req *orkstackv1.ResumeRunRequest, reason, cause string) *orkstackv1.ResumeRefusal {
	r.s.t.Helper()
	resp, refused := r.ask(req)
	if refused.GetReason() != reason || refused.GetCause() != cause {
		r.s.t.Fatalf("ResumeRun(%v) = %v, refused %v; want %s %s", req, resp, refused, reason, cause)
	}
	return refused
}

// refusedPreview is the refusal a preview of runID must meet.
func (r *resumeDoor) refusedPreview(runID, reason, cause string) *orkstackv1.ResumeRefusal {
	r.s.t.Helper()
	return r.requireRefused(&orkstackv1.ResumeRunRequest{RunId: runID, DryRun: true}, reason, cause)
}

// confirmOf asks for what preview showed, accepting acknowledged.
func confirmOf(preview *orkstackv1.ResumeRunResponse, acknowledged ...string) *orkstackv1.ResumeRunRequest {
	return &orkstackv1.ResumeRunRequest{
		RunId: preview.GetRunId(), Reason: "integration",
		ExpectedTerminalSequence: preview.GetTerminalSequence(), ExpectedPlanDigest: preview.GetPlanDigest(),
		AcknowledgedUnknownAttempts: acknowledged,
	}
}

// confirm resumes the run as preview showed it, which must not be refused.
func (r *resumeDoor) confirm(preview *orkstackv1.ResumeRunResponse, acknowledged ...string) *orkstackv1.ResumeRunResponse {
	r.s.t.Helper()
	resp, refused := r.ask(confirmOf(preview, acknowledged...))
	if refused != nil {
		r.s.t.Fatalf("the resume of %s was refused: %v", preview.GetRunId(), refused)
	}
	return resp
}

// unknownAttempts are preview's warnings of attempts nothing answers for.
func unknownAttempts(preview *orkstackv1.ResumeRunResponse) []*orkstackv1.ResumeWarning {
	var out []*orkstackv1.ResumeWarning
	for _, w := range preview.GetWarnings() {
		if w.GetKind() == orkstackv1.ResumeWarningKind_RESUME_WARNING_KIND_ATTEMPT_OUTCOME_UNKNOWN {
			out = append(out, w)
		}
	}
	return out
}

// ── runs ────────────────────────────────────────────────────────────────────

// startFailing starts a run of wf and waits until it has failed, published
// its ending and archived it.
func (s *resumeStack) startFailing(wf *orkstackv1.Orkflow) string {
	s.t.Helper()
	runID := s.recordedRunID()
	s.start(runID, wf, nil, s.resumePlan())
	s.awaitFailed(runID)
	return runID
}

func (s *resumeStack) awaitFailed(runID string) {
	s.t.Helper()
	s.awaitFailedWithin(runID, 10*time.Minute)
}

// awaitFailedWithin is awaitFailed for a run that may take up to within.
func (s *resumeStack) awaitFailedWithin(runID string, within time.Duration) {
	s.t.Helper()
	if status, err := s.wait(runID, within); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		s.t.Fatalf("run %s closed %v (%v); it was meant to fail", runID, status, err)
	}
	awaitArchivedEvents(s.t, s.ctx, s.archive, runID)
	if _, ok := s.created[runID]; !ok {
		s.created[runID] = s.read(runID).GetCreatedAt().AsTime()
	}
}

// awaitSucceeded waits for the run's current execution to succeed.
func (s *resumeStack) awaitSucceeded(runID string) *orkstackv1.Run {
	s.t.Helper()
	if status, err := s.wait(runID, 5*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		s.t.Fatalf("run %s closed %v: %v", runID, status, err)
	}
	return s.read(runID)
}

func (s *resumeStack) read(runID string) *orkstackv1.Run {
	s.t.Helper()
	run, err := s.reader.GetRun(s.ctx, runID)
	must.OKf(s.t, err, "GetRun %s", runID)
	return run
}

// requireRow fails unless the run's row for id has status and code.
func (s *resumeStack) requireRow(run *orkstackv1.Run, id string, status orkstackv1.Status, code string) *orkstackv1.TaskRun {
	s.t.Helper()
	row := taskRunOfRun(run, id)
	if row == nil || row.GetStatus() != status || row.GetError().GetCode() != code {
		s.t.Fatalf("task %s reads %v %v; want %v %q", id, row.GetStatus(), row.GetError(), status, code)
	}
	return row
}

// published is the events of type typ the run has published, across every
// execution of it.
func (s *resumeStack) published(runID string, typ orkstackv1.EventType) []*orkstackv1.Event {
	var out []*orkstackv1.Event
	for _, e := range s.bus.Events(runID, 0) {
		if e.GetType() == typ {
			out = append(out, e)
		}
	}
	return out
}

// requireResumedOnce fails unless RUN_RESUMED and RUN_RESULTS_KEPT were each
// published once, in that order and RUN_RESUMED at scope, and the run reads as
// one run: created when it first was, its events whole from the first on, what
// the resume set aside included.
func (s *resumeStack) requireResumedOnce(runID string, scope int64) {
	s.t.Helper()
	resumed := s.published(runID, orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED)
	kept := s.published(runID, orkstackv1.EventType_EVENT_TYPE_RUN_RESULTS_KEPT)
	if len(resumed) != 1 || resumed[0].GetSequence() != scope || len(kept) != 1 || kept[0].GetSequence() <= scope {
		s.t.Fatalf("run %s published RUN_RESUMED %v and RUN_RESULTS_KEPT %v; want each once, the first at %d", runID, resumed, kept, scope)
	}
	if want, got := s.created[runID], s.read(runID).GetCreatedAt().AsTime(); want.IsZero() || !got.Equal(want) {
		s.t.Fatalf("run %s reads as created at %s after its resume, want %s", runID, got, want)
	}
	s.requireWholeStream(runID)
}

// requireWholeStream fails unless the run's events read from the first to the
// last with none missing, none twice and no gap in their place.
func (s *resumeStack) requireWholeStream(runID string) []*orkstackv1.Event {
	s.t.Helper()
	events, err := s.reader.Events(s.ctx, runID, 0)
	must.OKf(s.t, err, "Events(0) of %s", runID)
	requireContiguous(s.t, events)
	for _, e := range events {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_STREAM_GAP {
			s.t.Fatalf("run %s's events have a gap at %d: %s", runID, e.GetSequence(), e.GetMessage())
		}
	}
	return events
}

// countingCluster is the resume seam, counting the resets it sends.
type countingCluster struct {
	engine.ResumeCluster
	mu   sync.Mutex
	sent int
	// before and after, when set, run around the reset. An error of before's
	// is the answer, and nothing is sent; an error of after's is the answer
	// to a reset that was sent.
	before func(context.Context, engine.ResumeReset) error
	after  func(ctx context.Context, execution string) error
}

func (c *countingCluster) ResetForResume(ctx context.Context, reset engine.ResumeReset) (string, error) {
	if c.before != nil {
		if err := c.before(ctx, reset); err != nil {
			return "", err
		}
	}
	c.mu.Lock()
	c.sent++
	c.mu.Unlock()
	execution, err := c.ResumeCluster.ResetForResume(ctx, reset)
	if err == nil && c.after != nil {
		if err := c.after(ctx, execution); err != nil {
			return "", err
		}
	}
	return execution, err
}

func (c *countingCluster) resets() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sent
}

func (s *resumeStack) counting() *countingCluster {
	return &countingCluster{ResumeCluster: s.starter.WithClusterIdentity(s.namespace, "ork-resume-it")}
}

// ── what was attempted ──────────────────────────────────────────────────────

// runsNow reports whether the run's row for taskID reads RUNNING. A run that
// cannot be read yet runs nothing yet.
func (s *resumeStack) runsNow(runID, taskID string) bool {
	run, err := s.reader.GetRun(s.ctx, runID)
	return err == nil && taskRunOfRun(run, taskID).GetStatus() == orkstackv1.Status_STATUS_RUNNING
}

// awaitNoOpenClaims waits until every attempt of the run that was claimed is
// released.
func (s *resumeStack) awaitNoOpenClaims(runID string) {
	s.t.Helper()
	s.eventually(2*time.Minute, "every claim of "+runID+" to be released", func() (bool, string) {
		n := s.openClaims(runID)
		return n == 0, fmt.Sprintf("%d are not", n)
	})
}

// openClaims counts the run's attempts claimed and never released.
func (s *resumeStack) openClaims(runID string) int {
	s.t.Helper()
	reader, ok := s.stores.attempts.(artifacts.RunClaimReader)
	if !ok {
		s.t.Fatalf("attempt store %T lists no claims", s.stores.attempts)
	}
	open, err := reader.OpenAttemptClaims(s.ctx, runID)
	must.OKf(s.t, err, "the claims of %s", runID)
	return len(open)
}

// requireAttempts fails unless want attempts of the task were claimed, in the
// run as it was submitted and in every resume of it.
func (s *resumeStack) requireAttempts(runID, taskID string, want int) {
	s.t.Helper()
	prefix, err := artifacts.RunAttemptPrefix(runID)
	must.OK(s.t, err)
	got := 0
	for _, key := range s.listObjects(prefix) {
		if ref, ok := artifacts.ParseAttemptClaimKey(key); ok && ref.TaskID == taskID {
			got++
		}
	}
	if got != want {
		s.t.Fatalf("%d attempts of %s were claimed, want %d", got, taskID, want)
	}
}

// listObjects lists the keys under prefix in the stack's bucket.
func (s *resumeStack) listObjects(prefix string) []string {
	s.t.Helper()
	var keys []string
	pager := s3.NewListObjectsV2Paginator(newRawS3ClientForCommitE2E(s.t, s.cfg), &s3.ListObjectsV2Input{
		Bucket: aws.String(s.cfg.S3Bucket), Prefix: aws.String(prefix),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(s.ctx)
		must.OKf(s.t, err, "list %s", prefix)
		for _, object := range page.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
	}
	return keys
}

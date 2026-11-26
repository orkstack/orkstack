//go:build integration

package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The chaos suite of results the engine records (AGENTS.md §11, B8;
// docs/TASK_IDENTITY.md, "Results the engine records"). Each test is one
// real execution on the proof's own Temporal whose ExecuteTask attempts run
// on the real dispatcher and the host's container runtime, with the records
// in the process's own bucket of the real store and the engine's real
// CommitResult. The task holds no identity, so it meets the engine at its
// preflight and at its commit. The unit suites hold the rest of B8's list:
// a memo planted by an Orker (internal/orker TestMemo_*), forged upstream
// references (TestCheckUpstreamRecord*), an Orker of another pool and two
// attempts at once (TestIntegration_Commit_*).
//
// The one stand-in is the engine's reachability: chaosEngine answers
// unavailable, or records and then loses its answer, on the test's word. It
// is a fault a real engine cannot be made to produce on demand at an exact
// call, and it wraps the real service, whose writes and refusals are its
// own.

// chaosEngine is the engine as the Orker reaches it: the real service's
// CommitResult behind a switch.
type chaosEngine struct {
	svc     *Service
	orkerID string
	// down answers every commit (not a preflight) unavailable while set.
	down atomic.Bool
	// loseAnswers is how many commits are recorded and then answered
	// unavailable, as an engine lost between its write and its answer is.
	loseAnswers atomic.Int32
	calls       atomic.Int32
	preflights  atomic.Int32
	// asked is the last preflight, whose coordinates a stolen Orker sends
	// with a result of its own.
	asked atomic.Pointer[orkstackv1.CommitResultRequest]
}

// steal asks the engine, inside the attempt in is of, to record a result the
// attempt's Orker made up with no container of its own.
func (e *chaosEngine) steal(ctx context.Context, in *orkstackv1.ExecuteTaskInput) error {
	req := proto.Clone(e.asked.Load()).(*orkstackv1.CommitResultRequest)
	req.Preflight = false
	req.Commit = &orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: in.GetNamespace(), RunId: in.GetRunId(),
		TaskId: in.GetTaskId(), OrkflowName: in.GetOrkflowName(), OrkflowVersion: in.GetOrkflowVersion(),
		OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(), IdempotencyKey: in.GetIdempotencyKey(), Image: in.GetImage(),
		Attempt: req.GetAttempt(), OrkerId: e.orkerID}
	_, err := e.svc.CommitResult(e.asOrker(ctx), connect.NewRequest(req))
	return err
}

func (e *chaosEngine) asOrker(ctx context.Context) context.Context {
	return authn.WithPrincipal(authn.WithMode(ctx, authn.ModeEnforce), &authn.Principal{Kind: authn.KindService,
		ID: "serviceaccount:" + e.orkerID, Issuer: "serviceaccount", Method: authn.MethodServiceAccount, OrkerID: e.orkerID})
}

func (e *chaosEngine) commit(ctx context.Context, req *orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
	if req.GetPreflight() {
		e.preflights.Add(1)
		e.asked.Store(req)
	} else {
		e.calls.Add(1)
		if e.down.Load() {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("IDENTITY_UNAVAILABLE: the engine is away (chaos)"))
		}
	}
	resp, err := e.svc.CommitResult(e.asOrker(ctx), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	if !req.GetPreflight() && e.loseAnswers.Load() > 0 {
		e.loseAnswers.Add(-1)
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("IDENTITY_UNAVAILABLE: the answer was lost (chaos)"))
	}
	return resp.Msg, nil
}

// countingRuntime counts the containers the dispatcher creates.
type countingRuntime struct {
	orker.ContainerRuntime
	creates atomic.Int32
}

func (r *countingRuntime) Create(ctx context.Context, spec orker.ContainerSpec) (orker.ContainerID, error) {
	r.creates.Add(1)
	return r.ContainerRuntime.Create(ctx, spec)
}

// chaosOutcome is one attempt's end, as its activity saw it.
type chaosOutcome struct {
	attempt int32
	result  *orkstackv1.ExecuteTaskResult
	err     error
	// stolen is the engine's answer to a result made up in an attempt of a
	// run that follows, whose preflight was refused.
	stolen error
}

type chaosRig struct {
	t        *testing.T
	ctx      context.Context
	temporal client.Client
	commits  artifacts.CommitStore
	raw      *s3.Client
	bucket   string
	engine   *chaosEngine
	runtime  *countingRuntime
	in       *orkstackv1.ExecuteTaskInput
	// dep is the task that needs in's, dispatched after it with the upstream
	// the interpreter would build from its result; nil where the run has
	// none.
	dep      *orkstackv1.ExecuteTaskInput
	runID    string
	produced atomic.Pointer[orkstackv1.UpstreamRef]
	// given is the upstream the task of a run that follows is dispatched with.
	given    atomic.Pointer[orkstackv1.UpstreamRef]
	store    artifacts.Store
	root     string
	outcomes chan chaosOutcome
	o        chaosOptions

	dispatcher *orker.OCIDispatcher
	records    *artifacts.RecordStore
	sha        string
	wfQueue    string
	actQueue   string
}

type chaosOptions struct {
	commitWait  int32 // the run's pinned commit wait, in seconds
	maxAttempts int32
	mayCommit   bool // the Orker's binding grants task.result.commit at the pool
	// failFirstCompletion fails the first attempt's activity after its
	// dispatcher returned, as an Orker lost after the record and before
	// Temporal heard of the completion is.
	failFirstCompletion bool
	// dependent adds a second task to the run, which reads the first one's
	// values from the file its staging wrote.
	dependent bool
	// follows is what the task follows; one who forges the run's history may
	// dispatch it before any of it is on record. It reads the values the
	// upstream given it names, and is dispatched again on a signal.
	follows []artifacts.FollowRef
	// steal has a refused attempt's Orker hand the engine a result of its own
	// all the same.
	steal bool
}

// chaosDependent is the dependent's task id.
const chaosDependent = "report"

// newChaosRig builds the run, its admission, the engine and the Orker; the
// test arranges its faults and then calls start.
func newChaosRig(t *testing.T, o chaosOptions) *chaosRig {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	cli := e2eContainerCLI(t)
	pinned := e2ePinnedReference(t, ctx, cli)
	_, indexDigest, _ := strings.Cut(pinned, "@")
	orkerID := proof.OrkerID("orker-recorded-chaos")

	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	raw, err := s3bucket.Client(ctx, cfg)
	if err != nil {
		mintInfraAbsent(t, "object store client: %v", err)
	}
	if _, err := raw.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(cfg.S3Bucket)}); err != nil {
		mintInfraAbsent(t, "object store bucket %q at %s: %v; run `make infra-up`", cfg.S3Bucket, cfg.S3Endpoint, err)
	}
	records, err := artifacts.NewRecordStore(ctx, cfg)
	if err != nil {
		t.Fatalf("record store: %v", err)
	}
	commits, err := artifacts.NewCommitStore(ctx, cfg)
	if err != nil {
		t.Fatalf("commit store: %v", err)
	}
	commitRoot, err := artifacts.CommitRoot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	stamp := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	r := &chaosRig{t: t, ctx: ctx, records: records, commits: commits, raw: raw, bucket: cfg.S3Bucket, o: o,
		temporal: mintTemporal(ctx, t, mintNamespace(t, stamp)), runID: engine.NewRunID(),
		wfQueue: "chaos-wf-" + stamp, actQueue: "chaos-act-" + stamp, outcomes: make(chan chaosOutcome, 8)}
	starts := identity.ClusterStarts{RecordStore: records, Temporal: r.temporal}

	// The task, holding no identity, from which the admission's digest and
	// the dispatched input are both made.
	tk := &orkstackv1.Task{Id: mintTask, Pool: "main",
		Image:   &orkstackv1.ImageRef{Reference: e2eImageTag, Pinned: pinned, IndexDigest: indexDigest},
		Command: []string{"/bin/sh", "-c", `printf '{"ok":true}' > /orkstack/output/values.json`},
		Network: orkstackv1.NetworkMode_NETWORK_MODE_NONE}
	if len(o.follows) > 0 {
		tk.Command = []string{"/bin/sh", "-c", `cp /orkstack/input/upstream/` + o.follows[0].TaskID + `/values.json /orkstack/output/values.json`}
	}
	spec, _, err := task.IdentitySpecDigest(task.IdentitySpecVersion, task.IdentitySpecOfTask(tk, nil))
	if err != nil {
		t.Fatal(err)
	}
	adm := artifacts.Admission{RunID: r.runID, Namespace: artifacts.DefaultNamespace, Orkflow: "deploy", OrkflowVersion: "1",
		DefinitionDigest: strings.Repeat("d", 64), Starter: artifacts.Starter{Kind: artifacts.StarterEngine},
		Origin: artifacts.AdmittedOrigin{Kind: "submit"}, Nonce: strings.Repeat("0f", 16), AdmittedAt: time.Now().UTC(),
		EngineBuild: "chaos-test", SubjectFormat: artifacts.SubjectFormatOrk, SpecVersion: int(task.IdentitySpecVersion),
		CommitWait: o.commitWait, Tasks: map[string]artifacts.AdmittedTask{mintTask: {Pool: "main", SpecSHA256: spec}}}
	depTask := &orkstackv1.Task{Id: chaosDependent, Pool: "main", Needs: []string{mintTask}, Image: tk.GetImage(), Network: tk.GetNetwork(),
		Command: []string{"/bin/sh", "-c", `cp /orkstack/input/upstream/` + mintTask + `/values.json /orkstack/output/values.json`}}
	if len(o.follows) > 0 {
		adm.ValueRecords, adm.Follows = 1, map[string][]artifacts.FollowRef{mintTask: o.follows}
		for _, f := range o.follows {
			if f.Kind == artifacts.FollowContainer {
				adm.Tasks[f.TaskID] = adm.Tasks[mintTask]
			}
		}
	}
	if o.dependent {
		depSpec, _, err := task.IdentitySpecDigest(task.IdentitySpecVersion, task.IdentitySpecOfTask(depTask, nil))
		if err != nil {
			t.Fatal(err)
		}
		adm.Tasks[chaosDependent] = artifacts.AdmittedTask{Pool: "main", SpecSHA256: depSpec}
	}
	body, sha, err := artifacts.EncodeAdmission(adm)
	if err != nil {
		t.Fatal(err)
	}
	r.sha = sha
	key, err := artifacts.AdmissionKey(adm.Namespace, adm.RunID, sha)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key),
		Body: bytes.NewReader(body), IfNoneMatch: aws.String("*")}); err != nil {
		t.Fatalf("write admission: %v", err)
	}

	// The engine: its real recorder over the store and the cluster, with
	// the Orker bound at the task's pool.
	describe := func(ctx context.Context, runID string) (identity.ExecutionFacts, error) {
		return starts.Describe(ctx, runID, "")
	}
	recorder := &identity.Recorder{Stores: records, Starts: starts, Describe: describe, Follows: &identity.FollowsCheck{Store: records},
		PinnedImage: func(context.Context, artifacts.Admission, string) (string, error) { return indexDigest, nil },
		EngineBuild: "chaos-test", Now: time.Now}
	svc := NewService(Config{AuditStore: artifacts.NewMemoryAuditStore(), Recorder: recorder, Artifacts: cfg})
	binding := func(perms ...string) *AccessControl {
		a, _ := accessFor(t, authn.ModeEnforce, []*orkstackv1.Role{{Name: "chaos-orker", Permissions: perms}},
			&orkstackv1.Binding{Name: "chaos-orker", Role: "chaos-orker", Scope: "pool:main", Subjects: []string{"serviceaccount:" + orkerID}})
		return a
	}
	svc.access = binding("identity.token.mint", "task.result.commit")
	r.engine = &chaosEngine{svc: svc, orkerID: orkerID}

	// The real Orker: its dispatcher on the host's runtime, its results
	// handed to the engine.
	workRoot := e2eWorkRoot(t, ctx)
	store, err := artifacts.NewS3Store(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r.store, r.root = store, strings.TrimSuffix(commitRoot, "/")
	idCfg := orker.DefaultIdentityConfig()
	idCfg.RetryMin = time.Second
	idCfg.SetupBudget = 20 * time.Second
	policy := orker.DefaultSecurityPolicy()
	r.runtime = &countingRuntime{ContainerRuntime: orker.NewDockerRuntime()}
	acts := orker.NewActivitiesWithOptions(store, orker.Options{
		WorkRoot: workRoot, Commits: commits, Records: records, Refs: artifacts.MemoryRefPredictor{},
		Runtime: r.runtime, Policy: &policy, OrkerID: orkerID, BuildID: "chaos-test", Pool: "main", Identity: &idCfg,
	})
	// The Orker's self-check, asked of the engine's real GetSignin, finds it
	// serves task identity: its binding grants the mint and the commit.
	support := orker.NewIdentitySupport(signinAs{svc: svc, ctx: r.engine.asOrker}, orkerID, "main",
		func() orkstackv1.StoreScope { return orkstackv1.StoreScope_STORE_SCOPE_SCOPED }, nil, nil)
	support.Step(ctx)
	if ok, why := support.Report(); !ok {
		t.Fatalf("the Orker's self-check did not grant support: %s", why)
	}
	acts.WatchIdentitySupport(support)
	r.dispatcher = acts.Dispatcher()
	r.dispatcher.SetResultCommit(r.engine.commit)
	if !o.mayCommit {
		// The role drifted after the self-check: the engine no longer lets
		// this Orker commit at the pool.
		svc.access = binding("identity.token.mint")
	}

	commitKey, err := artifacts.CommitKey(artifacts.DefaultNamespace, r.runID, mintTask)
	if err != nil {
		t.Fatal(err)
	}
	r.in = &orkstackv1.ExecuteTaskInput{RunId: r.runID, OrkflowName: "deploy", OrkflowVersion: "1", Namespace: artifacts.DefaultNamespace,
		Pool: "main", TaskId: mintTask, IdempotencyKey: task.IdempotencyKeyFor(artifacts.DefaultNamespace, r.runID, mintTask),
		OrkflowDefinitionDigest: "sha256:" + strings.Repeat("d", 64),
		CommitUri:               strings.TrimSuffix(commitRoot, "/") + "/" + commitKey,
		Image:                   tk.GetImage(), Command: tk.GetCommand(), Network: tk.GetNetwork(),
		TimeoutSeconds: 120, CancellationGracePeriodSeconds: 2,
		Limits: &orkstackv1.WorkspaceLimits{ValuesMaxBytes: 262144, ArtifactMaxCount: 256, ArtifactMaxBytes: 1 << 30,
			LogMaxBytes: 16 << 20, CommitMaxBytes: 4 << 20},
		Identity: &orkstackv1.IdentityContext{AdmissionSha256: sha, DigestVersion: task.IdentitySpecVersion, CommitWaitSeconds: o.commitWait},
	}
	if o.dependent {
		depKey, err := artifacts.CommitKey(artifacts.DefaultNamespace, r.runID, chaosDependent)
		if err != nil {
			t.Fatal(err)
		}
		r.dep = proto.Clone(r.in).(*orkstackv1.ExecuteTaskInput)
		r.dep.TaskId, r.dep.Command = chaosDependent, depTask.GetCommand()
		r.dep.IdempotencyKey = task.IdempotencyKeyFor(artifacts.DefaultNamespace, r.runID, chaosDependent)
		r.dep.CommitUri = strings.TrimSuffix(commitRoot, "/") + "/" + depKey
	}
	return r
}

// chaosChainWorkflow runs the run's task and then the one that needs it,
// each as the interpreter names its activity.
func chaosChainWorkflow(ctx workflow.Context, queue string) error {
	for _, id := range []string{mintTask, chaosDependent} {
		actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID: identity.ActivityID(id, artifacts.Item{}), TaskQueue: queue, StartToCloseTimeout: 5 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}})
		if err := workflow.ExecuteActivity(actx, task.ActivityExecuteTask).Get(actx, nil); err != nil {
			return err
		}
	}
	return nil
}

// chaosAgainWorkflow dispatches the run's task, and again on each signal, as
// one who writes the run's history may.
func chaosAgainWorkflow(ctx workflow.Context, queue string) error {
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID: identity.ActivityID(mintTask, artifacts.Item{}), TaskQueue: queue, StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}})
	for {
		// Its failure is the test's to read, from the attempt.
		if err := workflow.ExecuteActivity(actx, task.ActivityExecuteTask).Get(actx, nil); temporal.IsCanceledError(err) {
			return err
		}
		workflow.GetSignalChannel(ctx, chaosAgain).Receive(ctx, nil)
	}
}

const chaosAgain = "again"

// again dispatches the task of a run that follows once more, with up.
func (r *chaosRig) again(up *orkstackv1.UpstreamRef) {
	r.t.Helper()
	r.given.Store(up)
	if err := r.temporal.SignalWorkflow(r.ctx, r.runID, "", chaosAgain, nil); err != nil {
		r.t.Fatal(err)
	}
}

// start runs the execution: the engine's worker for the run, the Orker's
// worker whose activity is the dispatcher's real attempt, and the chain
// record the engine completes from the run's first start.
func (r *chaosRig) start() {
	r.t.Helper()
	ew := worker.New(r.temporal, r.wfQueue, worker.Options{Identity: "engine"})
	ew.RegisterWorkflow(mintRigWorkflow)
	ew.RegisterWorkflow(chaosChainWorkflow)
	ew.RegisterWorkflow(chaosAgainWorkflow)
	if err := ew.Start(); err != nil {
		r.t.Fatalf("engine worker: %v", err)
	}
	r.t.Cleanup(ew.Stop)
	ow := worker.New(r.temporal, r.actQueue, worker.Options{Identity: r.engine.orkerID})
	ow.RegisterActivityWithOptions(func(ctx context.Context) error {
		n, in := activity.GetInfo(ctx).Attempt, r.in
		if activity.GetInfo(ctx).ActivityID == identity.ActivityID(chaosDependent, artifacts.Item{}) {
			in = proto.Clone(r.dep).(*orkstackv1.ExecuteTaskInput)
			in.Upstream = []*orkstackv1.UpstreamRef{r.produced.Load()}
		}
		if up := r.given.Load(); up != nil {
			in = proto.Clone(in).(*orkstackv1.ExecuteTaskInput)
			in.Upstream = []*orkstackv1.UpstreamRef{up}
		}
		result, err := r.dispatcher.Execute(ctx, in)
		if err == nil && in == r.in {
			// As the interpreter builds it (runState.upstreamFor).
			r.produced.Store(&orkstackv1.UpstreamRef{TaskId: mintTask, CommitUri: result.GetCommit().GetUri(),
				Values: result.GetValuesRef(), Artifacts: result.GetArtifacts()})
		}
		got := chaosOutcome{attempt: n, result: result, err: err}
		if err != nil && r.o.steal && r.engine.asked.Load() != nil {
			got.stolen = r.engine.steal(ctx, in)
		}
		r.outcomes <- got
		if err == nil && r.o.failFirstCompletion && n == 1 {
			return errors.New("the Orker was lost before the cluster heard of its completion (chaos)")
		}
		return err
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	if err := ow.Start(); err != nil {
		r.t.Fatalf("Orker worker: %v", err)
	}
	r.t.Cleanup(ow.Stop)
	opts := client.StartWorkflowOptions{ID: r.runID, TaskQueue: r.wfQueue}
	var run client.WorkflowRun
	var err error
	switch {
	case len(r.o.follows) > 0:
		run, err = r.temporal.ExecuteWorkflow(r.ctx, opts, chaosAgainWorkflow, r.actQueue)
	case r.dep != nil:
		run, err = r.temporal.ExecuteWorkflow(r.ctx, opts, chaosChainWorkflow, r.actQueue)
	default:
		run, err = r.temporal.ExecuteWorkflow(r.ctx, opts, mintRigWorkflow, r.actQueue, max(r.o.maxAttempts, 1))
	}
	if err != nil {
		r.t.Fatalf("start run %s: %v", r.runID, err)
	}
	r.t.Cleanup(func() { _ = r.temporal.TerminateWorkflow(context.Background(), r.runID, "", "the test is done") })
	if err := r.records.CompleteChain(r.ctx, artifacts.DefaultNamespace, r.runID,
		artifacts.Chain{FirstExecutionID: run.GetRunID(), AdmissionSHA256: r.sha}); err != nil {
		r.t.Fatalf("chain record: %v", err)
	}
}

// next is the next attempt's end.
func (r *chaosRig) next() chaosOutcome {
	r.t.Helper()
	select {
	case o := <-r.outcomes:
		return o
	case <-r.ctx.Done():
		r.t.Fatalf("no attempt ended: %v", r.ctx.Err())
	}
	return chaosOutcome{}
}

// recorded is the engine's record of the task, or nil when there is none.
func (r *chaosRig) recorded() *orkstackv1.ActivityCommit {
	r.t.Helper()
	uri, err := artifacts.AttestedCommitURI(r.in.GetCommitUri())
	if err != nil {
		r.t.Fatal(err)
	}
	rec, err := r.commits.Get(r.ctx, uri)
	if errors.Is(err, artifacts.ErrNotFound) {
		return nil
	}
	if err != nil {
		r.t.Fatalf("read the record: %v", err)
	}
	return rec.Commit
}

// orkerCommitted reports whether anything is at the task's commit key,
// where no Orker of such a run writes.
func (r *chaosRig) orkerCommitted() bool {
	r.t.Helper()
	_, err := r.commits.Get(r.ctx, r.in.GetCommitUri())
	return !errors.Is(err, artifacts.ErrNotFound)
}

// waitFor waits until cond holds, while the attempt makes progress.
func (r *chaosRig) waitFor(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for !cond() {
		if time.Now().After(deadline) {
			r.t.Fatalf("never: %s", what)
		}
		select {
		case <-r.ctx.Done():
			r.t.Fatal(r.ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func reasonIn(err error, reason string) bool {
	return err != nil && strings.Contains(err.Error(), reason)
}

// TestIntegration_RecordedResults_TheEngineAwayInsideTheWait: the engine
// unreachable when the container has finished: inside the wait nothing
// fails, and the result is recorded once an engine answers; nothing is
// written under metadata/commits/, and the container ran once.
func TestIntegration_RecordedResults_TheEngineAwayInsideTheWait(t *testing.T) {
	r := newChaosRig(t, chaosOptions{commitWait: 120, maxAttempts: 1, mayCommit: true})
	r.engine.down.Store(true)
	r.start()
	r.waitFor("the Orker asked to commit twice", func() bool { return r.engine.calls.Load() >= 2 })
	r.engine.down.Store(false)
	got := r.next()
	if got.err != nil {
		t.Fatalf("the attempt failed: %v", got.err)
	}
	rec := r.recorded()
	if rec == nil || rec.GetRecorded().GetOrkerId() != r.engine.orkerID || got.result.GetCommit().GetUri() == r.in.GetCommitUri() {
		t.Fatalf("the record = %v, the result's commit %v", rec, got.result.GetCommit())
	}
	if r.orkerCommitted() || r.runtime.creates.Load() != 1 {
		t.Fatalf("an Orker commit %v, containers %d", r.orkerCommitted(), r.runtime.creates.Load())
	}
}

// TestIntegration_RecordedResults_PastTheWaitNothingIsRecordedAroundTheEngine:
// an engine away for the whole wait fails the attempt
// COMMIT_ENGINE_UNAVAILABLE, and no record exists that the engine did not
// write, under either prefix.
func TestIntegration_RecordedResults_PastTheWaitNothingIsRecordedAroundTheEngine(t *testing.T) {
	r := newChaosRig(t, chaosOptions{commitWait: 6, maxAttempts: 1, mayCommit: true})
	r.engine.down.Store(true)
	r.start()
	got := r.next()
	if !reasonIn(got.err, orker.ReasonCommitEngineUnavailable) {
		t.Fatalf("the attempt = %v, want %s", got.err, orker.ReasonCommitEngineUnavailable)
	}
	if r.recorded() != nil || r.orkerCommitted() {
		t.Fatalf("a record %v or an Orker commit %v exists with no engine to write it", r.recorded(), r.orkerCommitted())
	}
}

// TestIntegration_RecordedResults_ARetryAfterTheWaitRunsTheContainerAgain:
// a result uploaded and never recorded was not committed: the retry runs
// the container again and the engine records its result.
func TestIntegration_RecordedResults_ARetryAfterTheWaitRunsTheContainerAgain(t *testing.T) {
	r := newChaosRig(t, chaosOptions{commitWait: 6, maxAttempts: 2, mayCommit: true})
	r.engine.down.Store(true)
	r.start()
	if first := r.next(); !reasonIn(first.err, orker.ReasonCommitEngineUnavailable) {
		t.Fatalf("the first attempt = %v", first.err)
	}
	r.engine.down.Store(false)
	second := r.next()
	if second.err != nil || second.result.GetCommitAdopted() {
		t.Fatalf("the retry = %v (adopted %v)", second.err, second.result.GetCommitAdopted())
	}
	if r.runtime.creates.Load() != 2 || r.recorded() == nil {
		t.Fatalf("containers %d, record %v; want two runs and one record", r.runtime.creates.Load(), r.recorded())
	}
}

// TestIntegration_RecordedResults_ACancelDuringTheWait: a cancel during
// the wait ends the attempt cancelled, with no record.
func TestIntegration_RecordedResults_ACancelDuringTheWait(t *testing.T) {
	r := newChaosRig(t, chaosOptions{commitWait: 240, maxAttempts: 1, mayCommit: true})
	r.engine.down.Store(true)
	r.start()
	r.waitFor("the Orker asked to commit", func() bool { return r.engine.calls.Load() >= 1 })
	if err := r.temporal.CancelWorkflow(r.ctx, r.runID, ""); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	got := r.next()
	if got.err == nil || reasonIn(got.err, orker.ReasonCommitEngineUnavailable) {
		t.Fatalf("the attempt = %v, want it stopped by the cancel", got.err)
	}
	if r.recorded() != nil {
		t.Fatal("a cancelled attempt's result was recorded")
	}
}

// TestIntegration_RecordedResults_ALostAnswerIsAnsweredWithTheRecord: an
// engine lost between its write and its answer: the Orker's repeat is
// answered with that record, and the task completes once.
func TestIntegration_RecordedResults_ALostAnswerIsAnsweredWithTheRecord(t *testing.T) {
	r := newChaosRig(t, chaosOptions{commitWait: 120, maxAttempts: 1, mayCommit: true})
	r.engine.loseAnswers.Store(1)
	r.start()
	got := r.next()
	if got.err != nil || r.engine.calls.Load() != 2 {
		t.Fatalf("the attempt = %v after %d commits", got.err, r.engine.calls.Load())
	}
	if r.recorded() == nil || r.runtime.creates.Load() != 1 {
		t.Fatalf("record %v, containers %d", r.recorded(), r.runtime.creates.Load())
	}
}

// TestIntegration_RecordedResults_AnAnswerNeverHeardIsFoundBeforeTheAttemptFails:
// an attempt that never hears an answer finds the engine's record at its
// key before it gives up, and reports it.
func TestIntegration_RecordedResults_AnAnswerNeverHeardIsFoundBeforeTheAttemptFails(t *testing.T) {
	r := newChaosRig(t, chaosOptions{commitWait: 6, maxAttempts: 1, mayCommit: true})
	r.engine.loseAnswers.Store(1 << 20)
	r.start()
	got := r.next()
	if got.err != nil {
		t.Fatalf("the attempt = %v, want the record found", got.err)
	}
	if r.recorded() == nil || r.runtime.creates.Load() != 1 {
		t.Fatalf("record %v, containers %d", r.recorded(), r.runtime.creates.Load())
	}
}

// TestIntegration_RecordedResults_LostAfterTheRecordTheRetryAdopts: an
// Orker lost after the record and before the cluster heard: the retry
// adopts the record and no container runs again.
func TestIntegration_RecordedResults_LostAfterTheRecordTheRetryAdopts(t *testing.T) {
	r := newChaosRig(t, chaosOptions{commitWait: 120, maxAttempts: 2, mayCommit: true, failFirstCompletion: true})
	r.start()
	if first := r.next(); first.err != nil {
		t.Fatalf("the first attempt = %v", first.err)
	}
	second := r.next()
	if second.err != nil || !second.result.GetCommitAdopted() || r.runtime.creates.Load() != 1 {
		t.Fatalf("the retry = %v (adopted %v), containers %d", second.err, second.result.GetCommitAdopted(), r.runtime.creates.Load())
	}
}

// TestIntegration_RecordedResults_APlantedCommitIsNeverAdopted: a commit
// written under metadata/commits/ by anything but an engine, before the
// task runs: it is not adopted, the task runs, and the engine records it.
func TestIntegration_RecordedResults_APlantedCommitIsNeverAdopted(t *testing.T) {
	r := newChaosRig(t, chaosOptions{commitWait: 120, maxAttempts: 1, mayCommit: true})
	planted := &orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: r.in.GetNamespace(), RunId: r.runID,
		TaskId: mintTask, OrkflowDefinitionDigest: r.in.GetOrkflowDefinitionDigest(), IdempotencyKey: r.in.GetIdempotencyKey(),
		Image: r.in.GetImage(), Attempt: 1, OrkerId: "a-stolen-orker",
		Values: &orkstackv1.ArtifactRef{Name: "values.json", Uri: "s3://elsewhere/forged", Sha256: strings.Repeat("e", 64), SizeBytes: 2}}
	doc, err := artifacts.MarshalCommit(planted)
	if err != nil {
		t.Fatal(err)
	}
	key := strings.TrimPrefix(r.in.GetCommitUri(), "s3://"+r.bucket+"/")
	if _, err := r.raw.PutObject(r.ctx, &s3.PutObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(key), Body: bytes.NewReader(doc)}); err != nil {
		t.Fatalf("plant: %v", err)
	}
	r.start()
	got := r.next()
	if got.err != nil || got.result.GetCommitAdopted() || r.runtime.creates.Load() != 1 {
		t.Fatalf("the attempt = %v (adopted %v), containers %d; want the planted commit ignored", got.err, got.result.GetCommitAdopted(), r.runtime.creates.Load())
	}
	if rec := r.recorded(); rec == nil || rec.GetOrkerId() == "a-stolen-orker" {
		t.Fatalf("the record = %v", rec)
	}
}

// TestIntegration_RecordedResults_ADependentStagesItsProducersRecord: a
// task that needs an engine-recorded one is dispatched with the uri its
// producer's result reported, which is the engine's own answer, and its
// container reads the recorded values; its own result is the engine's too,
// and nothing is written under metadata/commits/ for either.
func TestIntegration_RecordedResults_ADependentStagesItsProducersRecord(t *testing.T) {
	r := newChaosRig(t, chaosOptions{commitWait: 120, maxAttempts: 1, mayCommit: true, dependent: true})
	r.start()
	producer := r.next()
	if producer.err != nil || r.orkerCommitted() {
		t.Fatalf("the producer = %v, an Orker commit %v", producer.err, r.orkerCommitted())
	}
	dependent := r.next()
	if dependent.err != nil {
		t.Fatalf("the dependent = %v", dependent.err)
	}
	if got, want := dependent.result.GetValuesRef().GetSha256(), producer.result.GetValuesRef().GetSha256(); got == "" || got != want {
		t.Fatalf("the dependent's values hash to %q, the producer's to %q: it copied what it was staged", got, want)
	}
	uri, err := artifacts.AttestedCommitURI(r.dep.GetCommitUri())
	if err != nil {
		t.Fatal(err)
	}
	rec, err := r.commits.Get(r.ctx, uri)
	if err != nil || rec.Commit.GetRecorded().GetOrkerId() != r.engine.orkerID || dependent.result.GetCommit().GetUri() != uri {
		t.Fatalf("the dependent's record = %v, %v; its result names %v", rec.Commit, err, dependent.result.GetCommit())
	}
	if _, err := r.commits.Get(r.ctx, r.dep.GetCommitUri()); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("the dependent's commit key = %v, want nothing there", err)
	}
}

// TestIntegration_RecordedResults_AnOrkerThatMayNotCommitRunsNothing: an
// Orker whose role may not commit at its pool is refused at its preflight,
// before any container is created.
func TestIntegration_RecordedResults_AnOrkerThatMayNotCommitRunsNothing(t *testing.T) {
	r := newChaosRig(t, chaosOptions{commitWait: 120, maxAttempts: 1, mayCommit: false})
	r.start()
	got := r.next()
	if !reasonIn(got.err, orker.ReasonCommitEngineRefused) || r.engine.preflights.Load() == 0 || r.runtime.creates.Load() != 0 {
		t.Fatalf("the attempt = %v, preflights %d, containers %d", got.err, r.engine.preflights.Load(), r.runtime.creates.Load())
	}
	if r.recorded() != nil {
		t.Fatal("a refused attempt's result was recorded")
	}
}

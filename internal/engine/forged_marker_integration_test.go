//go:build integration

package engine_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A forged marker (proof-plan §3.6p, G3): whoever answers a run's workflow
// tasks can write into its history that an approval no door recorded was
// found on record. The real interpreter then believes it and dispatches the
// task that follows the approval; that task's attempt is refused by the
// engine's check and the Orker's staging, which read records, never the
// history.
//
// Why the forger is a lying local activity, not a hand-written workflow task:
// the interpreter reads an answer through ReadResolutionRecord and, in the same
// workflow task, publishes its resolution and schedules the dependent. A
// workflow task forged to hold only the marker is replayed by an honest engine
// as non-determinism (its commands are more than the history holds), so the
// honest engine dispatches nothing and the line could not be proven. The real
// RunWorkflow on a replica whose ReadResolutionRecord lies writes that same
// forged marker, and the real interpreter dispatches on it.

// lyingResolutions is ReadResolutionRecord as a forger answers it: "found",
// with values of its choosing, for the runs it lies about, and the engine's own
// read for every other. It stands in for the one fault no real cluster produces
// on demand, a marker no engine wrote (AGENTS.md §11.1); every check that
// follows it is the real one.
type lyingResolutions struct {
	honest *engine.ResolutionReadActivities
	values *orkstackv1.ArtifactRef
	raw    []byte
	mu     sync.Mutex
	lie    map[string]bool
	// told counts the forged answers given.
	told atomic.Int32
}

func (l *lyingResolutions) ReadResolution(ctx context.Context, req engine.ResolutionReadRequest) (engine.ResolutionRead, error) {
	l.mu.Lock()
	lie := l.lie[req.RunID]
	l.mu.Unlock()
	if !lie {
		return l.honest.ReadResolution(ctx, req)
	}
	l.told.Add(1)
	ref := engine.ArtifactRefPlanOf(l.values)
	return engine.ResolutionRead{Outcome: engine.ResolutionFound, RecordSHA256: strings.Repeat("f", 64), Values: &ref, ValuesJSON: l.raw,
		Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API.String(), Principal: "forger"}, nil
}

// orkerSignin answers the Orker's self-check as the engine's GetSignin answers
// for its binding: its own id, holding the mint and the commit at its pool.
// The self-check's own suites prove that answer; here it only lets the Orker
// serve the run, and the commit check behind it is the engine's real one.
type orkerSignin struct{ orkerID, pool string }

func (o orkerSignin) Whoami(context.Context) (*orkstackv1.GetSigninResponse, error) {
	return &orkstackv1.GetSigninResponse{PrincipalId: "serviceaccount:" + o.orkerID, OrkerId: o.orkerID,
		Permissions: []*orkstackv1.ScopedPermissions{{Scope: "pool:" + o.pool, Permissions: []string{"identity.token.mint", "task.result.commit"}}}}, nil
}

// countedRuntime counts the containers the Orker creates.
type countedRuntime struct {
	orker.ContainerRuntime
	creates atomic.Int32
}

func (r *countedRuntime) Create(ctx context.Context, spec orker.ContainerSpec) (orker.ContainerID, error) {
	r.creates.Add(1)
	return r.ContainerRuntime.Create(ctx, spec)
}

// forgedMarkerEngine is the engine's CommitResult over the real recorder, with
// the Orker bound at its pool in an access document that enforces.
func forgedMarkerEngine(t *testing.T, s *resumeStack, orkerID, pool string) (func(context.Context, *orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error), identity.ClusterStarts) {
	t.Helper()
	cas := artifacts.NewMemoryCAS()
	docs := artifacts.NewAccessStoreOn(cas)
	_, err := docs.Write(s.ctx, &orkstackv1.AccessDocument{SchemaVersion: 1,
		Roles: []*orkstackv1.Role{{Name: "forged-orker", Permissions: []string{"identity.token.mint", "task.result.commit"}}},
		Bindings: []*orkstackv1.Binding{{Name: "forged-orker", Role: "forged-orker", Scope: "pool:" + pool,
			Subjects: []string{"serviceaccount:" + orkerID}}}}, nil, artifacts.AccessWrite{At: time.Now()})
	must.OK(t, err)
	// The checks and the recorder are built as the engine builds them
	// (engine.NewIdentityChecks, which cmd/ork's mint and CommitResult use);
	// only the definition's pinned image is not read, since the test stores
	// no definition, and pins nothing.
	checks := engine.NewIdentityChecks(s.records, s.control, identity.DefaultAdmissionCacheBytes)
	recorder := checks.Recorder(s.records, func(context.Context, artifacts.Admission, string) (string, error) { return "", nil },
		"forged-marker", time.Now)
	starts := checks.Starts
	svc := api.NewService(api.Config{AuditStore: artifacts.NewMemoryAuditStore(), Recorder: recorder, Artifacts: s.cfg})
	api.NewMux(svc, api.MuxOptions{Access: &api.AccessControl{Mode: authn.ModeEnforce, Policies: api.NewPolicyCache(docs, api.PolicyCacheOptions{})}})
	as := &authn.Principal{Kind: authn.KindService, ID: "serviceaccount:" + orkerID, Issuer: "serviceaccount",
		Method: authn.MethodServiceAccount, OrkerID: orkerID}
	return func(ctx context.Context, req *orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
		resp, err := svc.CommitResult(authn.WithPrincipal(authn.WithMode(ctx, authn.ModeEnforce), as), connect.NewRequest(req))
		if err != nil {
			return nil, err
		}
		return resp.Msg, nil
	}, starts
}

// TestIntegration_AForgedMarkerDispatchesNothingThatRuns: the history of a run
// whose results the engine records says its approval was found on record, and
// no door recorded one. The real interpreter takes it and dispatches the task
// that follows the approval; that attempt is refused, naming the approval, no
// container starts, no result is recorded, and the run fails. A run beside it
// whose approval the door recorded runs the same task and succeeds.
func TestIntegration_AForgedMarkerDispatchesNothingThatRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "identity/forged-marker")
	const pool = "real"
	queue := mustQueueForPool(t, pool)

	forged, err := s.stores.store.Put(ctx, "values.json", strings.NewReader(`{"approved":"forged"}`), artifacts.PutOptions{ContentType: "application/json"})
	must.OK(t, err)
	reads := &lyingResolutions{honest: engine.NewResolutionReadActivities(s.records), lie: map[string]bool{}, raw: []byte(`{"approved":"forged"}`),
		values: &orkstackv1.ArtifactRef{Name: "values.json", Uri: forged.URI, Sha256: forged.SHA256, SizeBytes: forged.SizeBytes}}
	s.startReplica("replica-a", replicaOptions{extra: []func(worker.Worker){func(w worker.Worker) {
		w.RegisterActivityWithOptions(reads.ReadResolution, activity.RegisterOptions{Name: engine.ActivityReadResolution})
	}}})
	s.startProjection(nil)

	// The real Orker: its dispatcher on the host's runtime, its preflight and
	// its results handed to the engine's real check.
	orkerID := proof.OrkerID("forged-marker-orker")
	runtime := &countedRuntime{ContainerRuntime: orker.NewDockerRuntime()}
	idCfg := orker.DefaultIdentityConfig()
	idCfg.RetryMin, idCfg.SetupBudget = time.Second, 20*time.Second
	acts := orker.NewActivitiesWithOptions(s.stores.store, orker.Options{
		Logs: s.stores.logs, Commits: s.stores.commits, Attempts: s.stores.attempts, Refs: s.stores.refs, Records: s.records,
		Runtime: runtime, Pool: pool, OrkerID: orkerID, BuildID: "forged-marker", WorkRoot: chaosWorkRoot(t, "forged-marker"), Identity: &idCfg,
	})
	if err := acts.Boot(ctx); err != nil {
		infraAbsent(t, "orker boot checks failed: %v", err)
	}
	t.Cleanup(acts.Close)
	support := orker.NewIdentitySupport(orkerSignin{orkerID: orkerID, pool: pool}, orkerID, pool,
		func() orkstackv1.StoreScope { return orkstackv1.StoreScope_STORE_SCOPE_SCOPED }, nil, nil)
	support.Step(ctx)
	if ok, why := support.Report(); !ok {
		t.Fatalf("the Orker's self-check did not grant support: %s", why)
	}
	acts.WatchIdentitySupport(support)
	commit, starts := forgedMarkerEngine(t, s, orkerID, pool)
	// The engine's check is wired as the engine's is, before any run: it
	// enforces, holds a recorder and takes the Orker's principal, so a request
	// for an attempt nobody holds is refused as one, never as an engine that
	// records nothing.
	if _, err := commit(ctx, &orkstackv1.CommitResultRequest{Preflight: true, Namespace: artifacts.DefaultNamespace, RunId: "ork-none",
		TaskId: "apply", Attempt: 1}); err == nil || strings.Contains(err.Error(), identity.ReasonEngineRefused) ||
		connect.CodeOf(err) == connect.CodeUnauthenticated {
		t.Fatalf("the engine's check as wired: %v; want a refusal of the attempt", err)
	}
	acts.Dispatcher().SetResultCommit(commit)
	// The worker's identity is the Orker's id: the engine holds an attempt
	// to the worker the cluster says took it.
	s.startOrkerWith(orkerID, queue, s.control, func(ctx context.Context, _ *chaosStack, _ string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return acts.ExecuteTask(ctx, in)
	})

	// The Orkflow is created as a person creates it: through the API's
	// create, which resolves each task's image against the proof's registry
	// (the first-party images ORK_TEST_IMAGE_TAG names) and stamps every
	// field a run is dispatched and its result held to with. The definition
	// the run starts is the one create stored.
	env, err := localcatalog.CatalogEnv()
	if err != nil {
		infraAbsent(t, "%v", err)
	}
	catalog, err := dsl.ParseCatalog([]byte(strings.TrimPrefix(env, "ORK_ACTIVITY_CATALOG=")))
	must.OKf(t, err, "parse the local catalog")
	orkflows := engine.NewMemoryOrkflowStore()
	create := s.serveAPI(api.Config{Catalog: catalog, Resolver: dsl.NewResolver(""), Orkflows: orkflows})
	create.apply(t, ctx, `apiVersion: orkstack/v1
kind: Orkflow
name: forged-marker
version: "1.0.0"
tasks:
  - id: approval
    await:
      timeout_seconds: 600
  - id: apply
    needs:
      - approval
    uses: shell
    pool: real
    network: none
    timeout_seconds: 120
    with:
      op: exec
      command:
        - echo
        - ok
`)
	wf, err := orkflows.Get(ctx, artifacts.DefaultNamespace, "forged-marker", "1.0.0")
	must.OKf(t, err, "the created Orkflow")
	for _, tk := range wf.GetTasks() {
		if tk.GetId() == "apply" && (tk.GetImage().GetIndexDigest() == "" || tk.GetImage().GetPinned() == "") {
			t.Fatalf("create stamped apply's image as %v; a result is held to its index digest", tk.GetImage())
		}
	}
	var apply *orkstackv1.Task
	for _, tk := range wf.GetTasks() {
		if tk.GetId() == "apply" {
			apply = tk
		}
	}
	// The admission is made by the code SubmitRun makes it with
	// (api.AdmissionTemplate, api.Admitted, api.IdentityPlanOf), over the
	// engine's part of it: what the commit check compares is the
	// definition's own, and is held to it before any run.
	template, err := api.AdmissionTemplate(artifacts.Admission{Version: 1, Namespace: artifacts.DefaultNamespace,
		Origin: artifacts.AdmittedOrigin{Kind: "submit"}, EngineBuild: "forged-marker", SubjectFormat: artifacts.SubjectFormatOrk,
		CommitWait: 120, SpecVersion: int(task.IdentitySpecVersion), ValueRecords: artifacts.RecordFormat},
		wf, &engine.RunPlan{TransferBudgetSeconds: 300}, identity.DefaultAttemptsMax)
	must.OK(t, err)
	spec, _, err := task.IdentitySpecDigest(task.IdentitySpecVersion, task.IdentitySpecOfTask(apply, nil))
	must.OK(t, err)
	if want := strings.TrimPrefix(wf.GetDefinitionDigest(), "sha256:"); template.DefinitionDigest != want || template.Orkflow != wf.GetName() ||
		template.OrkflowVersion != wf.GetVersion() || template.Tasks["apply"].SpecSHA256 != spec || template.Tasks["apply"].Pool != pool ||
		len(template.Follows["apply"]) != 1 || template.Follows["apply"][0].TaskID != "approval" {
		t.Fatalf("the admission of the definition: %+v", template)
	}

	// start admits a run of wf as a submit does, and starts it.
	start := func() (string, string) {
		runID := s.recordedRunID()
		a, err := api.Admitted(template, runID, "", artifacts.Starter{Kind: artifacts.StarterEngine}, time.Now().UTC())
		must.OK(t, err)
		sha, _, err := s.records.WriteAdmission(ctx, a)
		must.OK(t, err)
		plan := realTaskPlan(t, s.chaosStack, queue)
		plan.Identity = api.IdentityPlanOf(a, sha, false)
		s.start(runID, wf, nil, plan)
		s.awaitRow(runID, "approval", orkstackv1.Status_STATUS_AWAITING)
		// The start check reads what the engine's reads: the admission the
		// run's first start names.
		execution, _ := s.current(runID)
		if got, err := starts.FirstStartAdmission(ctx, runID, execution); err != nil || got != sha {
			t.Fatalf("the first start of %s names %q (%v); want its admission %q", runID, got, err, sha)
		}
		return runID, sha
	}

	// The forged run: the run is told of an answer, and its history then says
	// one was found that no door recorded.
	forgedRun, _ := start()
	reads.mu.Lock()
	reads.lie[forgedRun] = true
	reads.mu.Unlock()
	must.OKf(t, s.starter.SignalAwait(ctx, forgedRun, &orkstackv1.AwaitSignal{TaskId: "approval"}), "tell the run")
	s.awaitFailed(forgedRun)
	if reads.told.Load() == 0 {
		t.Fatal("the run never asked for the answer the forger gives")
	}
	run := s.read(forgedRun)
	if row := taskRunOfRun(run, "approval"); row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("approval = %v; the interpreter must have taken the forged answer", row)
	}
	row := taskRunOfRun(run, "apply")
	why := row.GetError().GetMessage() + " " + row.GetError().GetCode()
	if row.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(why, "approval") ||
		!(strings.Contains(why, orker.ReasonRequiredResultNotRecorded) || strings.Contains(why, "UPSTREAM_RESULT_NOT_RECORDED")) {
		t.Fatalf("apply = %v; want refused for want of the approval's record", row)
	}
	if n := runtime.creates.Load(); n != 0 {
		t.Fatalf("%d containers started for a task dispatched on a forged answer", n)
	}
	if read, err := s.records.ReadAttestedCommit(ctx, artifacts.SlotRun, nil, artifacts.AttestedWant{Namespace: artifacts.DefaultNamespace,
		RunID: forgedRun, TaskID: "apply"}); err != nil || read.Answer != artifacts.RecordNone {
		t.Fatalf("apply's result: %v, %v; want none recorded", read.Answer, err)
	}

	// The control: an approval the door recorded is taken, and the same task
	// runs and has its result recorded.
	honestRun, sha := start()
	s.approve(honestRun, "approval", 0, sha, nil)
	done := s.awaitSucceeded(honestRun)
	s.requireRow(done, "apply", orkstackv1.Status_STATUS_SUCCEEDED, "")
	if n := runtime.creates.Load(); n != 1 {
		t.Fatalf("%d containers for the approved run, want one", n)
	}
}

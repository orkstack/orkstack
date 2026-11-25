package engine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const resumeHolderDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: resume-holder
tasks:
  - id: build
    uses: shell
    with: {op: exec, command: "echo build"}
`

// resetHarness runs a run as a reset run, with the two plan loads standing
// in for the cluster's history and the store.
type resetHarness struct {
	*harness
	facts     engine.ResetFacts
	factsErr  error
	plan      *orkstackv1.ResumePlanRecord
	planCalls atomic.Int32
	commits   *artifacts.MemoryCommitStore
	hasConc   bool
}

func newResetHarness(t *testing.T) *resetHarness {
	t.Helper()
	h := &resetHarness{harness: newHarness(t, nil, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	})}
	h.env.RegisterActivityWithOptions(func(context.Context, engine.ReadResetFactsInput) (engine.ResetFacts, error) {
		return h.facts, h.factsErr
	}, activity.RegisterOptions{Name: engine.ActivityReadResetFacts})
	h.env.RegisterActivityWithOptions(func(context.Context, engine.LoadResumePlanInput) (*orkstackv1.ResumePlanRecord, error) {
		h.planCalls.Add(1)
		if h.plan != nil {
			return h.plan, nil
		}
		return nil, temporal.NewNonRetryableApplicationError("no such plan", engine.ReasonResumePlanInvalid, nil)
	}, activity.RegisterOptions{Name: engine.ActivityLoadResumePlan})
	h.commits = artifacts.NewMemoryCommitStore()
	h.env.RegisterActivityWithOptions(engine.NewResumeActivities(h.commits, h.values).AdoptRecorded,
		activity.RegisterOptions{Name: engine.ActivityAdoptRecorded})
	h.env.RegisterActivityWithOptions(engine.NewSnapshotActivities(h.snapshots).WriteResumeEnding,
		activity.RegisterOptions{Name: engine.ActivityWriteResumeEnding})
	h.env.RegisterWorkflowWithOptions(engine.ResetRunWorkflow(false), workflow.RegisterOptions{Name: "ResetRunWorkflow"})
	h.env.RegisterWorkflowWithOptions(engine.ResetRunWorkflow(true), workflow.RegisterOptions{Name: "ResetRunWorkflowWithConcurrency"})
	return h
}

func (h *resetHarness) runReset(t *testing.T) error {
	t.Helper()
	return h.runResetOf(t, resumeHolderDocument)
}

func (h *resetHarness) runResetOf(t *testing.T, document string) error {
	t.Helper()
	plan := defaultPlan()
	plan.CommitRoot = "mem://commits"
	name := "ResetRunWorkflow"
	if h.hasConc {
		name = "ResetRunWorkflowWithConcurrency"
	}
	h.env.ExecuteWorkflow(name, "ork-reset-1", compile(t, document), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the reset run did not complete")
	}
	return h.env.GetWorkflowError()
}

// A cluster reset made by hand carries no plan key: the run carries on as a
// reset run always has, and no plan is loaded.
func TestResetRun_AResetByHandIsNoResume(t *testing.T) {
	h := newResetHarness(t)
	h.facts = engine.ResetFacts{Reason: "reset by an operator"}
	must.OKf(t, h.runReset(t), "a hand reset run")
	if n := h.planCalls.Load(); n != 0 {
		t.Fatalf("a hand reset loaded a plan %d times", n)
	}
}

// A reason that claims to be a plan key and does not parse never reads as
// a reset by hand: the run ends RESUME_PLAN_INVALID.
func TestResetRun_AMalformedPlanKeyEndsTheRunInvalid(t *testing.T) {
	h := newResetHarness(t)
	h.facts = engine.ResetFacts{Reason: "ork-resume:v9:42.000:" + strings.Repeat("ab", 32)}
	err := h.runReset(t)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != engine.ReasonResumePlanInvalid {
		t.Fatalf("error = %v, want RESUME_PLAN_INVALID", err)
	}
	if n := h.planCalls.Load(); n != 0 {
		t.Fatalf("a malformed key loaded a plan %d times", n)
	}
}

func TestResetRun_AnUnreadableResetEndsTheRunUnreadable(t *testing.T) {
	h := newResetHarness(t)
	h.factsErr = temporal.NewNonRetryableApplicationError("history gone", engine.ReasonResumePlanUnreadable, nil)
	err := h.runReset(t)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != engine.ReasonResumePlanUnreadable {
		t.Fatalf("error = %v, want RESUME_PLAN_UNREADABLE", err)
	}
}

// A plan the store refuses ends the run invalid, after its key was read.
func TestResetRun_APlanThatCannotBeVerifiedEndsTheRunInvalid(t *testing.T) {
	h := newResetHarness(t)
	h.facts = engine.ResetFacts{Reason: engine.ResumePlanKey{Scope: 42, N: 0, PlanDigest: strings.Repeat("ab", 32)}.String()}
	err := h.runReset(t)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != engine.ReasonResumePlanInvalid || h.planCalls.Load() != 1 {
		t.Fatalf("error = %v after %d plan loads, want RESUME_PLAN_INVALID after one", err, h.planCalls.Load())
	}
}

// A plan that cannot be run ends the run FAILED at the key's sequence from
// its base's own last snapshot (R4-1): its rows read exactly as the base
// ended, and only the ending is new.
func TestResetRun_APlanThatCannotBeRunEndsFromTheBasesRows(t *testing.T) {
	h := newResetHarness(t)
	wf := compile(t, resumeHolderDocument)
	base := &orkstackv1.Run{Id: "ork-reset-1", Status: orkstackv1.Status_STATUS_FAILED,
		Error:    &orkstackv1.Error{Code: "RUN_FAILED", Message: "build failed"},
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "build", Status: orkstackv1.Status_STATUS_FAILED, Attempt: 3}}}
	data, err := proto.Marshal(base)
	must.OK(t, err)
	_, err = h.snapshots.WriteSnapshot(context.Background(), wf.GetNamespace(), "ork-reset-1", 41, data)
	must.OK(t, err)
	h.facts = engine.ResetFacts{Reason: engine.ResumePlanKey{Scope: 42, N: 0, PlanDigest: strings.Repeat("ab", 32)}.String()}

	err = h.runReset(t)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != engine.ReasonResumePlanInvalid {
		t.Fatalf("error = %v, want RESUME_PLAN_INVALID", err)
	}
	raw, ok := h.snapshots.get(fmt.Sprintf("memory://snapshots/%s/ork-reset-1/42.pb", wf.GetNamespace()))
	if !ok {
		t.Fatalf("no ending at the key's sequence 42; stored: %v", h.snapshots.keys())
	}
	ended := &orkstackv1.Run{}
	must.OK(t, proto.Unmarshal(raw, ended))
	if ended.GetStatus() != orkstackv1.Status_STATUS_FAILED || ended.GetError().GetCode() != engine.ReasonResumePlanInvalid {
		t.Fatalf("ending = %v %v, want FAILED RESUME_PLAN_INVALID", ended.GetStatus(), ended.GetError())
	}
	if !proto.Equal(&orkstackv1.Run{TaskRuns: ended.GetTaskRuns()}, &orkstackv1.Run{TaskRuns: base.GetTaskRuns()}) {
		t.Fatalf("rows = %v, want the base's %v", ended.GetTaskRuns(), base.GetTaskRuns())
	}
	if _, ok := h.snapshots.get(fmt.Sprintf("memory://snapshots/%s/ork-reset-1/3.pb", wf.GetNamespace())); ok {
		t.Fatal("the reset run wrote a snapshot at sequence 3, which the base had already used")
	}
}

const resumeTwoTaskDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: resume-two
tasks:
  - id: build
    uses: shell
    with: {op: exec, command: "echo build"}
  - id: deploy
    uses: shell
    needs: [build]
    with: {op: exec, command: "echo deploy"}
`

func testReadback() *orkstackv1.ResumeReadback {
	return &orkstackv1.ResumeReadback{
		Deadline: durationpb.New(10 * 60e9), Budget: 128 << 10, MaxEntries: 128, Concurrency: 16,
		GetTimeout: durationpb.New(10e9), GetTries: 3, CallTimeout: durationpb.New(90e9),
		RetryInitial: durationpb.New(1e9), RetryCoefficient: 2, CallsPerTask: 4,
	}
}

func keptUnit(task string) engine.ResumeUnitPlan {
	return engine.ResumeUnitPlan{Task: task, Item: -1, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT, EncodedSize: 512}
}

func rerunUnit(task string) engine.ResumeUnitPlan {
	return engine.ResumeUnitPlan{Task: task, Item: -1, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN}
}

// pack gives the harness's plan these units of document.
func (h *resetHarness) pack(t *testing.T, document string, units ...engine.ResumeUnitPlan) {
	t.Helper()
	must.OK(t, engine.EncodeResumeUnits(compile(t, document), h.plan.Block, units, false))
}

// keepBuildRerunDeploy is the plan of a run whose build succeeded and whose
// deploy failed: build kept, deploy run again under scope 42.
func (h *resetHarness) keepBuildRerunDeploy(t *testing.T, commitBuild bool) {
	t.Helper()
	wf := compile(t, resumeTwoTaskDocument)
	h.facts = engine.ResetFacts{Reason: engine.ResumePlanKey{Scope: 42, N: 0, PlanDigest: strings.Repeat("ab", 32)}.String()}
	h.plan = &orkstackv1.ResumePlanRecord{
		RunId: "ork-reset-1", Scope: 42, BaseTerminalSequence: 41, Principal: "ops@example.com", Reason: "flaky network",
		Block: &orkstackv1.ResumeBlock{
			SeqBase: 42, Scope: 42, Action: orkstackv1.ResumeAction_RESUME_ACTION_RESUME,
			Readback: testReadback(),
		},
	}
	h.pack(t, resumeTwoTaskDocument, keptUnit("build"), rerunUnit("deploy"))
	if !commitBuild {
		return
	}
	identity, err := engine.ExpectedCommitIdentity(wf, "ork-reset-1", wf.GetTasks()[0], -1, 0)
	must.OK(t, err)
	_, err = h.commits.Create(context.Background(), "mem://commits/"+identity.Key, &orkstackv1.ActivityCommit{
		SchemaVersion: artifacts.CommitSchemaVersion, Namespace: identity.Namespace, RunId: identity.RunID, TaskId: "build",
		OrkflowDefinitionDigest: identity.DefinitionDigest, IdempotencyKey: identity.IdempotencyKey,
		Image: wf.GetTasks()[0].GetImage(), Attempt: 2, OrkerId: "orker-a",
	})
	must.OK(t, err)
}

// A resume keeps what succeeded and runs only what must run again: build is
// read back from its commit and never dispatched, and deploy runs again
// under the resume's own key (E85 §5.3, steps 1–3 and 5).
func TestResetRun_KeepsWhatSucceededAndRunsTheRestAgain(t *testing.T) {
	h := newResetHarness(t)
	h.keepBuildRerunDeploy(t, true)
	must.OKf(t, h.runResetOf(t, resumeTwoTaskDocument), "the resumed run")
	if calls := h.taskCalls("build"); len(calls) != 0 {
		t.Fatalf("the kept task ran again %d times", len(calls))
	}
	deploys := h.taskCalls("deploy")
	if len(deploys) != 1 || deploys[0].GetIdempotencyKey() != "default/ork-reset-1/deploy@r42" ||
		!strings.Contains(deploys[0].GetCommitUri(), "/deploy/r00000000000000000042/commit.json") {
		t.Fatalf("deploy dispatched as %v, want once under scope 42", deploys)
	}
	run, ok := h.store.Get("ork-reset-1")
	if !ok || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v, want SUCCEEDED", run.GetStatus())
	}
	for _, row := range run.GetTaskRuns() {
		// What tells a resumed run's ending from its first one (ork.run.resumption).
		if row.GetResumeDisposition() == orkstackv1.ResumeDisposition_RESUME_DISPOSITION_UNSPECIFIED {
			t.Errorf("row %s of the resumed run's ending has no disposition", row.GetTaskId())
		}
		if row.GetTaskId() == "build" && (!row.GetAdopted() || row.GetAttempt() != 2 || row.GetOrkerId() != "orker-a") {
			t.Fatalf("build row = %v, want the committing attempt's, adopted", row)
		}
	}
	var resumed, kept *orkstackv1.Event
	for _, e := range h.bus.Events("ork-reset-1", 0) {
		switch e.GetType() {
		case orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED:
			resumed = e
		case orkstackv1.EventType_EVENT_TYPE_RUN_RESULTS_KEPT:
			kept = e
		}
	}
	if resumed.GetSequence() != 42 || resumed.GetResumed().GetPrincipal() != "ops@example.com" || resumed.GetResumed().GetKeptCount() != 1 ||
		resumed.GetResumed().GetRerunCount() != 1 || resumed.GetResumed().GetResumedAfterSequence() != 41 {
		t.Fatalf("RUN_RESUMED = %v, want at 42 with the plan's account", resumed)
	}
	if kept.GetSequence() != 43 || kept.GetResultsKept().GetKeptCount() != 1 {
		t.Fatalf("RUN_RESULTS_KEPT = %v, want at 43 with one result kept", kept)
	}
}

// A kept result that is gone fails its task with its reason, and nothing
// runs again: the run ends FAILED, and can be resumed.
func TestResetRun_AKeptResultThatIsGoneFailsBeforeAnythingRunsAgain(t *testing.T) {
	h := newResetHarness(t)
	h.keepBuildRerunDeploy(t, false)
	if err := h.runResetOf(t, resumeTwoTaskDocument); err == nil {
		t.Fatal("the run succeeded without its kept result")
	}
	if calls := h.taskCalls("deploy"); len(calls) != 0 {
		t.Fatalf("deploy ran %d times although build could not be read back", len(calls))
	}
	run, _ := h.store.Get("ork-reset-1")
	for _, row := range run.GetTaskRuns() {
		if row.GetTaskId() == "build" && (row.GetStatus() != orkstackv1.Status_STATUS_FAILED || row.GetError().GetCode() != "COMMIT_MISSING" ||
			row.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR) {
			t.Fatalf("build row = %v %v, want FAILED COMMIT_MISSING, an infrastructure error", row.GetStatus(), row.GetError())
		}
	}
}

const resumeAwaitDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: resume-await
tasks:
  - id: build
    uses: shell
    with: {op: exec, command: "echo build"}
  - id: approve
    needs: [build]
    await: {timeout_seconds: 3600}
  - id: deploy
    uses: shell
    needs: [approve]
    with: {op: exec, command: "echo deploy"}
`

// A plan needing a part this build has not built is never resumed half-way.
func TestResetRun_APlanNeedingAnUnbuiltPartIsNotRun(t *testing.T) {
	h := newResetHarness(t)
	h.keepBuildRerunDeploy(t, true)
	h.pack(t, resumeAwaitDocument, keptUnit("build"),
		engine.ResumeUnitPlan{Task: "approve", Item: -1, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_WAIT}, rerunUnit("deploy"))
	err := h.runResetOf(t, resumeAwaitDocument)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != engine.ReasonResumePlanInvalid || !strings.Contains(app.Error(), "resume_unsupported:wait_again") {
		t.Fatalf("error = %v, want RESUME_PLAN_INVALID resume_unsupported:wait_again", err)
	}
	if n := len(h.taskCalls("deploy")) + len(h.taskCalls("build")); n != 0 {
		t.Fatalf("%d tasks ran", n)
	}
}

// A block that says nothing of a task is not carried out: that task would run
// again under its first run's key.
func TestResetRun_APlanThatLeavesATaskOutIsNotRun(t *testing.T) {
	for name, units := range map[string][]byte{
		"a task left out": packedUnits(t, &orkstackv1.ResumeUnits{RunLengths: []int32{1}, RunEpochs: []int32{0},
			RunDispositions: []orkstackv1.ResumeDisposition{orkstackv1.ResumeDisposition_RESUME_DISPOSITION_RERUN}}),
		"no unit at all": nil,
	} {
		t.Run(name, func(t *testing.T) {
			h := newResetHarness(t)
			h.keepBuildRerunDeploy(t, true)
			h.plan.Block.Units = units
			err := h.runResetOf(t, resumeTwoTaskDocument)
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || app.Type() != engine.ReasonResumePlanInvalid {
				t.Fatalf("error = %v, want RESUME_PLAN_INVALID", err)
			}
			if n := len(h.taskCalls("deploy")) + len(h.taskCalls("build")); n != 0 {
				t.Fatalf("%d tasks ran", n)
			}
		})
	}
}

func packedUnits(t *testing.T, u *orkstackv1.ResumeUnits) []byte {
	t.Helper()
	raw, err := proto.Marshal(u)
	must.OK(t, err)
	return raw
}

// While a resumed run cannot retake its concurrency slot, a run held to a
// limit is never resumed past it; a run with none resumes (§5.3 step 4).
func TestResetRun_ARunWithConcurrencyIsNotResumedWithoutItsSlot(t *testing.T) {
	h := newResetHarness(t)
	h.hasConc = true
	h.keepBuildRerunDeploy(t, true)
	err := h.runResetOf(t, resumeTwoTaskDocument)
	if err == nil || !strings.Contains(err.Error(), "concurrency") {
		t.Fatalf("error = %v, want the concurrency refusal", err)
	}
	if n := len(h.taskCalls("deploy")); n != 0 {
		t.Fatalf("deploy ran %d times without its slot", n)
	}
}

// A key whose scope is not above this run's events is no seq_base of its
// base (a stale plan, or a reason shaped like a key): the run fails with no
// event, never at a sequence already used.
func TestResetRun_AScopeAtOrBelowTheRunsEventsWritesNoEnding(t *testing.T) {
	h := newResetHarness(t)
	h.facts = engine.ResetFacts{Reason: engine.ResumePlanKey{Scope: 2, N: 0, PlanDigest: strings.Repeat("ab", 32)}.String()}
	err := h.runReset(t)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != engine.ReasonResumePlanInvalid {
		t.Fatalf("error = %v, want RESUME_PLAN_INVALID", err)
	}
	for _, e := range h.bus.Events("ork-reset-1", 0) {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
			t.Fatalf("a RUN_FAILED at %d was published for a scope inside the run's own events", e.GetSequence())
		}
	}
}

// An ending never takes a sequence another execution already wrote: a
// snapshot already at the key's scope means a later resume used it.
func TestResetRun_AnEndingNeverOverwritesASnapshot(t *testing.T) {
	h := newResetHarness(t)
	wf := compile(t, resumeHolderDocument)
	base, _ := proto.Marshal(&orkstackv1.Run{Id: "ork-reset-1", Status: orkstackv1.Status_STATUS_FAILED})
	later, _ := proto.Marshal(&orkstackv1.Run{Id: "ork-reset-1", Status: orkstackv1.Status_STATUS_RUNNING, Error: &orkstackv1.Error{Code: "LATER"}})
	ctx := context.Background()
	_, err := h.snapshots.WriteSnapshot(ctx, wf.GetNamespace(), "ork-reset-1", 41, base)
	must.OK(t, err)
	_, err = h.snapshots.WriteSnapshot(ctx, wf.GetNamespace(), "ork-reset-1", 42, later)
	must.OK(t, err)
	h.facts = engine.ResetFacts{Reason: engine.ResumePlanKey{Scope: 42, N: 0, PlanDigest: strings.Repeat("ab", 32)}.String()}
	if err := h.runReset(t); err == nil {
		t.Fatal("the run did not fail")
	}
	raw, _ := h.snapshots.get(fmt.Sprintf("memory://snapshots/%s/ork-reset-1/42.pb", wf.GetNamespace()))
	got := &orkstackv1.Run{}
	must.OK(t, proto.Unmarshal(raw, got))
	if got.GetError().GetCode() != "LATER" {
		t.Fatalf("the snapshot at 42 was overwritten: %v", got.GetError())
	}
}

const resumeHandlerDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: resume-handler
tasks:
  - id: build
    uses: shell
    with: {op: exec, command: "echo build"}
  - id: deploy
    uses: shell
    needs: [build]
    with: {op: exec, command: "echo deploy"}
  - id: notify
    uses: shell
    needs: [deploy]
    join: any_failed
    with: {op: exec, command: "echo notify"}
`

// A resumed run that fails while it reads its kept results back has run
// nothing again and starts nothing: not even a failure handler (§4.3), and
// its unread rows end CANCELED RESUME_NOT_READ_BACK, still reading back.
func TestResetRun_AReadBackFailureStartsNoFailureHandler(t *testing.T) {
	h := newResetHarness(t)
	h.keepBuildRerunDeploy(t, false)
	h.pack(t, resumeHandlerDocument, keptUnit("build"), rerunUnit("deploy"), keptUnit("notify"))
	if err := h.runResetOf(t, resumeHandlerDocument); err == nil {
		t.Fatal("the run succeeded without its kept results")
	}
	if n := len(h.taskCalls("notify")) + len(h.taskCalls("deploy")); n != 0 {
		t.Fatalf("%d tasks ran during a failed read-back", n)
	}
	run, _ := h.store.Get("ork-reset-1")
	for _, row := range run.GetTaskRuns() {
		if row.GetTaskId() == "notify" && (row.GetStatus() != orkstackv1.Status_STATUS_CANCELED ||
			row.GetError().GetCode() != "RESUME_NOT_READ_BACK" || !row.GetReadingBack()) {
			t.Fatalf("notify row = %v %v reading_back=%v, want CANCELED RESUME_NOT_READ_BACK, still reading back",
				row.GetStatus(), row.GetError(), row.GetReadingBack())
		}
	}
}

// The last kept row failing its read-back starts no failure handler either:
// what decides is that the run has not left its plan yet, not whether a row
// is still reading back.
func TestResetRun_TheLastKeptRowFailingStartsNoFailureHandler(t *testing.T) {
	h := newResetHarness(t)
	h.keepBuildRerunDeploy(t, false)
	h.pack(t, resumeHandlerDocument, keptUnit("build"), rerunUnit("deploy"), rerunUnit("notify"))
	if err := h.runResetOf(t, resumeHandlerDocument); err == nil {
		t.Fatal("the run succeeded without its kept result")
	}
	if n := len(h.taskCalls("notify")) + len(h.taskCalls("deploy")); n != 0 {
		t.Fatalf("%d tasks ran after the only kept row failed its read-back", n)
	}
}

// A retried ending whose first write landed finds its own bytes there and
// completes: the run ends with its event, not with none.
func TestResetRun_ARetriedEndingFindsItsOwnSnapshot(t *testing.T) {
	h := newResetHarness(t)
	wf := compile(t, resumeHolderDocument)
	base, _ := proto.Marshal(&orkstackv1.Run{Id: "ork-reset-1", Status: orkstackv1.Status_STATUS_FAILED})
	ctx := context.Background()
	_, err := h.snapshots.WriteSnapshot(ctx, wf.GetNamespace(), "ork-reset-1", 41, base)
	must.OK(t, err)
	acts := engine.NewSnapshotActivities(h.snapshots)
	in := engine.ResumeEndingInput{Namespace: wf.GetNamespace(), RunID: "ork-reset-1", From: 41, To: 42,
		Code: engine.ReasonResumePlanInvalid, Message: "no such plan"}
	first, err := acts.WriteResumeEnding(ctx, in)
	must.OK(t, err)
	again, err := acts.WriteResumeEnding(ctx, in)
	if err != nil || again != first {
		t.Fatalf("the retried ending = %q, %v; want its own %q", again, err, first)
	}
	in.Message = "another ending"
	if _, err := acts.WriteResumeEnding(ctx, in); err == nil {
		t.Fatal("another ending at the same sequence was accepted")
	}
}

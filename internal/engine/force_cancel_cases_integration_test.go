//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIntegration_ForceCancelPausedRun: a paused run that is stuck is
// force-canceled as any other is: the pause neither holds it nor is lifted.
func TestIntegration_ForceCancelPausedRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	f := newFCStack(t, ctx, "fc-paused")
	held := newGates()
	f.startOrker("orker-1", gated(held, "a"))
	f.startEvents()
	healthy := f.startReplica("engine-1", f.steps())
	runID := f.recordedRunID()
	f.start(runID, compile(t, pauseChaosDoc), nil, f.pausePlan())
	stream := f.openStream(f.reader, runID, 0)
	held.waitEntered(t, "a", 2*time.Minute)
	f.sendPause(runID, true, "pause-1", 3600)
	f.awaitPublished(runID, "RUN_PAUSED", time.Minute, ofType(orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED))
	healthy.stop()
	f.startPoisoned("engine-2")
	f.poke(runID)
	f.failing(runID)

	answer, err := f.force.Request(ctx, f.ask(runID))
	must.OK(t, err)
	if got := f.over(runID, answer.RequestID, 4*time.Minute); got.State != engine.FCStateForceCanceled || got.Incomplete {
		t.Fatalf("%+v", got)
	}
	if _, status := f.current(runID); status != enums.WORKFLOW_EXECUTION_STATUS_TERMINATED {
		t.Fatalf("the run closed %s", status)
	}
	events := f.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_CANCELED, engine.RunForceCanceledCode)
	for _, e := range events {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_UNPAUSED {
			t.Errorf("its pause was lifted: %v", e)
		}
	}
	if published := f.lastEvents(runID); len(published) != 1 {
		t.Errorf("published last events %v", published)
	}
	run, err := f.reader.GetRun(ctx, runID)
	must.OK(t, err)
	if run.GetPause() != nil || run.GetLastPause().GetEnd() != orkstackv1.PauseEnd_PAUSE_END_RUN_ENDED {
		t.Errorf("an ended run is paused: %v, last %v", run.GetPause(), run.GetLastPause())
	}
}

// TestIntegration_ForceCancelContainerStopped: a force cancel ends the run
// without the run's code, and its container still stops: its Orker's next
// heartbeat is answered that the attempt is gone, and that is a cancellation.
func TestIntegration_ForceCancelContainerStopped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	f := newFCStack(t, ctx, "fc-container")
	stores := f.realStores()
	image := pinnedAlpineForCommitE2E(t)
	queue := mustQueueForPool(t, "real")
	f.startEvents()
	healthy := f.startReplica("engine-1", f.steps())
	returned := make(chan error, 1)
	var once sync.Once
	_, orkerID := f.startRealOrker("orker-a", "real", queue, stores, nil, func(next orkerCall) orkerCall {
		return func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			out, err := next(ctx, in)
			once.Do(func() { returned <- err })
			return out, err
		}
	})
	// Nothing but a kill ends its container: a test that fails leaves none.
	t.Cleanup(func() {
		ids, err := exec.Command("docker", "ps", "-aq", "--filter", "label=ork.orker="+orkerID).Output()
		if err == nil && len(strings.Fields(string(ids))) > 0 {
			_ = exec.Command("docker", append([]string{"rm", "-f"}, strings.Fields(string(ids))...)...).Run()
		}
	})
	wf := compile(t, `apiVersion: orkstack/v1
kind: Orkflow
name: long
version: "1.0.0"
tasks:
  - id: long
    uses: shell
    pool: real
    cancellation_grace_period_seconds: 1
    with: {op: exec}
`)
	tk := wf.GetTasks()[0]
	tk.Uses, tk.Image = image, &orkstackv1.ImageRef{Reference: image, Pinned: image}
	// It answers no signal but the one a container cannot refuse, and outlives
	// the test.
	tk.Command = []string{"/bin/sh", "-c", `trap "" TERM; while :; do sleep 1; done`}
	tk.Network, tk.TimeoutSeconds = orkstackv1.NetworkMode_NETWORK_MODE_NONE, 1800
	plan := f.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, nil)
	plan.Routes["real"] = queue
	root, err := artifacts.CommitRoot(f.cfg)
	must.OK(t, err)
	plan.CommitRoot, plan.Limits, plan.TransferBudgetSeconds = root, engine.DefaultLimits(), 300
	runID := f.recordedRunID()
	f.start(runID, wf, nil, plan)
	stream := f.openStream(f.reader, runID, 0)
	awaitRunningContainersForCommitE2E(t, ctx, orkerID, 1, 2*time.Minute)
	healthy.stop()
	f.startPoisoned("engine-2")
	f.poke(runID)
	f.failing(runID)

	answer, err := f.force.Request(ctx, f.ask(runID))
	must.OK(t, err)
	deadline := time.Now().Add(3 * time.Minute)
	for {
		if _, status := f.current(runID); status == enums.WORKFLOW_EXECUTION_STATUS_TERMINATED {
			break
		}
		select {
		case err := <-returned:
			t.Fatalf("the attempt ended before the run did: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the run was not terminated")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Within a heartbeat, its grace and the kill.
	awaitRunningContainersForCommitE2E(t, ctx, orkerID, 0, 2*time.Minute)
	select {
	case err := <-returned:
		if err == nil {
			t.Error("the attempt reported a result")
		}
	case <-time.After(time.Minute):
		t.Fatal("the attempt never returned")
	}
	if got := f.over(runID, answer.RequestID, 2*time.Minute); got.State != engine.FCStateForceCanceled || got.Incomplete {
		t.Fatalf("%+v", got)
	}
	f.requireOneRunRead(runID, stream, orkstackv1.Status_STATUS_CANCELED, engine.RunForceCanceledCode)
}

// stuckTree is a stuck run whose child run is stuck too, seen by the cluster's
// index as below it.
func (f *fcStack) stuckTree() (runID, childID string) {
	t, ctx := f.t, f.ctx
	t.Helper()
	held := newGates()
	f.startOrker("orker-1", gated(held, "hold"))
	f.startEvents()
	healthy := f.startReplica("engine-1", f.steps())
	served := f.serveAPI(tcAPIConfig())
	served.apply(t, ctx, stuckDoc)
	served.apply(t, ctx, `apiVersion: orkstack/v1
kind: Orkflow
name: parent
version: "1.0.0"
tasks:
  - id: below
    uses: orkflow:stuck@1.0.0
`)
	runID = served.submit(t, ctx, "parent").GetRun().GetId()
	f.transcript.run(runID)
	deadline := time.Now().Add(3 * time.Minute)
	for childID == "" {
		run, err := f.reader.GetRun(ctx, runID)
		if err == nil && len(run.GetTaskRuns()) == 1 {
			childID = run.GetTaskRuns()[0].GetChild().GetRunId()
		}
		if time.Now().After(deadline) {
			t.Fatal("no child run started")
		}
		time.Sleep(200 * time.Millisecond)
	}
	held.waitEntered(t, "hold", 2*time.Minute)
	healthy.stop()
	f.startPoisoned("engine-2")
	deadline = time.Now().Add(3 * time.Minute)
	for _, id := range []string{runID, childID} {
		f.poke(id)
		f.failing(id)
	}
	for {
		below, err := f.force.Below(ctx, "default", runID)
		if err == nil && len(below.Open) == 1 {
			if b := below.Open[0]; b.RunID != childID || b.ParentRunID != runID || !b.Reached || b.Orkflow != "stuck" {
				t.Fatalf("below: %+v", b)
			}
			return runID, childID
		}
		if time.Now().After(deadline) {
			t.Fatalf("below: %+v, %v", below, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestIntegration_ForceCancelCascadeSurvivesCoordinatorTermination: the force
// cancel that cascades is itself ended while the run below's is in its grace:
// the one below goes on, ends its run and records who asked.
func TestIntegration_ForceCancelCascadeSurvivesCoordinatorTermination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	f := newFCStack(t, ctx, "fc-survives")
	runID, childID := f.stuckTree()
	ask := f.ask(runID)
	ask.Cascade = true
	answer, err := f.force.Request(ctx, ask)
	must.OK(t, err)
	var below string
	deadline := time.Now().Add(time.Minute)
	for below == "" {
		got, err := f.force.Status(ctx, "default", runID, answer.RequestID)
		if err == nil && got.Cascade != nil && len(got.Cascade.Results) == 1 {
			below = got.Cascade.Results[0].RequestID
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cascade started nothing: %+v, %v", got, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// The one below runs: its own is found by its request id.
	deadline = time.Now().Add(time.Minute)
	for {
		got, err := f.force.Status(ctx, "default", childID, below)
		if err == nil && got.Running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the force cancel below is not running: %+v, %v", got, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	must.OK(t, f.control.TerminateWorkflow(ctx, engine.ForceCancelWorkflowIDPrefix("default", runID)+answer.RequestID, "", "by hand"))
	f.transcript.step("the cascading force cancel terminated")
	if _, status := f.current(childID); status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatalf("the run below had already closed %s: the test proves nothing", status)
	}

	if got := f.over(childID, below, 4*time.Minute); got.State != engine.FCStateForceCanceled || got.Incomplete {
		t.Fatalf("%+v", got)
	}
	run, err := f.reader.GetRun(ctx, childID)
	if err != nil || run.GetError().GetCode() != engine.RunForceCanceledCode || run.GetCanceledBy() != "user:corp:ada" || !run.GetCanceledByVerified() {
		t.Errorf("%s reads %v by %s, %v", childID, run.GetError(), run.GetCanceledBy(), err)
	}
	// Its own run was not ended by a force cancel that did not finish.
	if got := f.over(runID, answer.RequestID, time.Minute); got.State == engine.FCStateForceCanceled || (got.Cascade != nil && got.Cascade.State == engine.FCCascadeComplete) {
		t.Errorf("the one ended by hand reads %+v", got)
	}
}

// TestIntegration_ForceCancelResumedRun: a failed run is resumed by a real reset
// and is stuck after it, once it ran again or before it recorded anything: the
// force cancel ends the execution the resume made, and the run's end follows
// what its resume set aside, taking no sequence of it.
func TestIntegration_ForceCancelResumedRun(t *testing.T) {
	for _, began := range []bool{true, false} {
		t.Run(fmt.Sprintf("began=%t", began), func(t *testing.T) { forceCancelResumed(t, began) })
	}
}

func forceCancelResumed(t *testing.T, began bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	f := newFCStack(t, ctx, fmt.Sprintf("fc-resumed-%t", began))
	commits, held := artifacts.NewMemoryCommitStore(), newGates()
	records, err := artifacts.NewRecordStore(ctx, s3Config())
	must.OKf(t, err, "record store")
	f.startOrker("orker-1", func(ctx context.Context, _ *chaosStack, _ string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		switch {
		case in.GetTaskId() == "deploy" && strings.Contains(in.GetIdempotencyKey(), "@r"):
			return nil, held.hold(ctx, "deploy")
		case in.GetTaskId() == "deploy":
			return nil, temporal.NewNonRetryableApplicationError("deploy failed", "ActivityFailed", nil)
		}
		record, err := commits.Create(ctx, in.GetCommitUri(), &orkstackv1.ActivityCommit{
			SchemaVersion: artifacts.CommitSchemaVersion, Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			RunId: in.GetRunId(), TaskId: in.GetTaskId(), OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
			IdempotencyKey: in.GetIdempotencyKey(), Image: in.GetImage(), Attempt: 1, OrkerId: "orker-it",
		})
		if err != nil {
			return nil, err
		}
		result := succeeded(in.GetTaskId(), map[string]any{"ok": true})
		result.Commit = &orkstackv1.CommitRef{Uri: in.GetCommitUri(), Sha256: record.SHA256}
		return result, nil
	})
	f.startEvents()
	steps := f.steps()
	steps.extra = append(steps.extra, func(w worker.Worker) {
		acts := engine.NewResumeActivities(commits, nil).WithHistory(f.control.WorkflowService(), f.namespace, records)
		w.RegisterActivityWithOptions(acts.ReadResetFacts, activity.RegisterOptions{Name: engine.ActivityReadResetFacts})
		w.RegisterActivityWithOptions(acts.LoadResumePlan, activity.RegisterOptions{Name: engine.ActivityLoadResumePlan})
		w.RegisterActivityWithOptions(acts.AdoptRecorded, activity.RegisterOptions{Name: engine.ActivityAdoptRecorded})
	})
	healthy := f.startReplica("engine-1", steps)
	runID := f.recordedRunID()
	f.start(runID, compile(t, resumeTwoTaskDocument), nil, f.plan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, nil))
	base, status := f.current(runID)
	if err := f.control.GetWorkflow(ctx, runID, base).Get(ctx, nil); err == nil {
		t.Fatalf("the base run succeeded (%s); deploy was meant to fail", status)
	}
	before, err := f.reader.Events(ctx, runID, 0)
	must.OK(t, err)
	terminal := before[len(before)-1].GetSequence()
	if !began {
		healthy.stop()
		f.startPoisoned("engine-2")
	}
	resumed := f.resumeDeploy(runID, base, terminal, records)
	if began {
		held.waitEntered(t, "deploy", 2*time.Minute)
		healthy.stop()
		f.startPoisoned("engine-2")
		f.poke(runID)
	}
	f.failing(runID)

	answer, err := f.force.Request(ctx, f.ask(runID))
	must.OK(t, err)
	if answer.Decision.TargetExecutionRunID != resumed {
		t.Errorf("decided on %s, want the resume's execution %s (the base was %s)", answer.Decision.TargetExecutionRunID, resumed, base)
	}
	if got := f.over(runID, answer.RequestID, 4*time.Minute); got.State != engine.FCStateForceCanceled || got.Incomplete {
		t.Fatalf("%+v", got)
	}
	for exec, want := range map[string]enums.WorkflowExecutionStatus{base: enums.WORKFLOW_EXECUTION_STATUS_FAILED, resumed: enums.WORKFLOW_EXECUTION_STATUS_TERMINATED} {
		d, err := describeExecution(f.control, ctx, runID, exec)
		if err != nil || d.GetWorkflowExecutionInfo().GetStatus() != want {
			t.Errorf("execution %s is %v, %v; want %s", exec, d.GetWorkflowExecutionInfo().GetStatus(), err, want)
		}
	}
	// What the resume set aside is read from the archive of that ending, so the
	// stream is whole and in order, and the end comes last, above every event
	// before it.
	events, err := f.reader.Events(ctx, runID, 0)
	must.OK(t, err)
	var gaps, resumes int
	for i, e := range events {
		if i > 0 && e.GetSequence() <= events[i-1].GetSequence() {
			t.Errorf("event %d follows %d", e.GetSequence(), events[i-1].GetSequence())
		}
		switch e.GetType() {
		case orkstackv1.EventType_EVENT_TYPE_STREAM_GAP:
			gaps++
		case orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED:
			resumes++
			if e.GetSequence() != terminal+1 {
				t.Errorf("resumed at %d after an end at %d", e.GetSequence(), terminal)
			}
		}
	}
	last := events[len(events)-1]
	if gaps != 0 || int64(len(events)) != last.GetSequence() || (resumes == 1) != began || last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED || last.GetSequence() <= terminal || !began && last.GetSequence() != terminal+1 {
		t.Errorf("%d gaps, %d resumes, and it ends %v at %d after %d", gaps, resumes, last.GetType(), last.GetSequence(), terminal)
	}
	if published := f.lastEvents(runID); !slices.Equal(published, []int64{terminal, last.GetSequence()}) {
		t.Errorf("published ends %v, want %d and %d", published, terminal, last.GetSequence())
	}
	run, err := f.reader.GetRun(ctx, runID)
	must.OK(t, err)
	rows := map[string]orkstackv1.Status{}
	for _, r := range run.GetTaskRuns() {
		rows[r.GetTaskId()] = r.GetStatus()
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != engine.RunForceCanceledCode || len(rows) != 2 ||
		rows["build"] != orkstackv1.Status_STATUS_SUCCEEDED || run.GetCanceledBy() != "user:corp:ada" || !run.GetCanceledByVerified() {
		t.Errorf("it reads %v %v, %v, canceled by %s", run.GetStatus(), run.GetError(), rows, run.GetCanceledBy())
	}
}

// resumeDeploy resets the failed run as ResumeRun would, keeping build and
// running deploy again, and answers the execution that makes.
func (f *fcStack) resumeDeploy(runID, base string, terminal int64, records *artifacts.RecordStore) string {
	t, ctx := f.t, f.ctx
	t.Helper()
	scanned, err := engine.ScanResumeBase(f.history(runID, base).GetEvents(), converter.GetDefaultDataConverter())
	must.OKf(t, err, "scan the base")
	scope := uint64(terminal + 1)
	record := &orkstackv1.ResumePlanRecord{
		Format: artifacts.ResumePlanFormat, Namespace: "default", RunId: runID, Scope: scope, BaseExecutionRunId: base,
		RScheduledEventId: scanned.ResetPoint.ScheduledEventID, RStartedEventId: scanned.ResetPoint.StartedEventID,
		BaseTerminalSequence: terminal, Principal: "ops@example.com", Reason: "integration",
		Block: &orkstackv1.ResumeBlock{
			SeqBase: scope, Scope: scope, Action: orkstackv1.ResumeAction_RESUME_ACTION_RESUME,
			Readback: &orkstackv1.ResumeReadback{
				Deadline: durationpb.New(2 * time.Minute), Budget: 128 << 10, MaxEntries: 128, Concurrency: 4,
				GetTimeout: durationpb.New(10 * time.Second), GetTries: 3, CallTimeout: durationpb.New(30 * time.Second),
				RetryInitial: durationpb.New(time.Second), RetryCoefficient: 2, CallsPerTask: 4,
			},
		},
	}
	must.OKf(t, engine.EncodeResumeUnits(compile(t, resumeTwoTaskDocument), record.Block,
		[]engine.ResumeUnitPlan{keptUnit("build"), rerunUnit("deploy")}, false), "pack the units")
	record.PlanDigest = engine.ResumePlanDigestOf(record)
	digest, err := records.WriteResumePlan(ctx, record)
	must.OKf(t, err, "write the plan")
	must.OKf(t, records.WriteResume(ctx, artifacts.ResumeRecord{Namespace: "default", RunID: runID, Scope: scope, Tasks: []string{"deploy"}}), "write the identity record")
	must.OKf(t, f.endings.MarkResumed(ctx, runID, scope, 0), "mark the run resumed")
	reset, err := f.control.WorkflowService().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: f.namespace, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: runID, RunId: base},
		Reason:                    engine.ResumePlanKey{Scope: scope, PlanDigest: digest}.String(),
		WorkflowTaskFinishEventId: scanned.ResetPoint.CompletedEventID, RequestId: uuid.NewString(),
		// Nothing the base received after R is applied again.
		ResetReapplyExcludeTypes: []enums.ResetReapplyExcludeType{enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL, enums.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE, enums.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS},
	})
	must.OKf(t, err, "reset the base")
	f.transcript.step("run %s resumed as %s", runID, reset.GetRunId())
	return reset.GetRunId()
}

package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// restoredOrkflow is build → fan (a fan-out over three parts) → publish.
func restoredOrkflow(t *testing.T) *orkstackv1.Orkflow {
	t.Helper()
	wf, err := dsl.Compile([]byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: pipeline
version: v1
inputs:
  parts: {type: array, default: [a, b, c]}
tasks:
  - id: build
    uses: shell
    with: {op: exec}
  - id: fan
    uses: shell
    needs: [build]
    each: {over: "{{ .inputs.parts }}", max_items: 10}
    with: {op: exec}
  - id: publish
    uses: shell
    needs: [fan]
    with: {op: exec}
`))
	if err != nil {
		t.Fatal(err)
	}
	wf.Namespace = "team-a"
	return wf
}

// restoredRows is the exact state a switch could save for restoredOrkflow:
// build SUCCEEDED, the fan-out RUNNING with item 0 dispatched and SUCCEEDED
// and items 1–2 never dispatched, publish PENDING.
func restoredRows() *orkstackv1.Run {
	now := timestamppb.New(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	return &orkstackv1.Run{
		Id: refusalWorkflowID, Namespace: "team-a", OrkflowName: "pipeline", OrkflowVersion: "v1",
		Status: orkstackv1.Status_STATUS_RUNNING, CreatedAt: now, StartedAt: now,
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED, ValuesRef: &orkstackv1.ArtifactRef{Uri: "s3://x/values.json"}},
			{TaskId: "fan", Status: orkstackv1.Status_STATUS_RUNNING, Each: &orkstackv1.TaskEach{Expanded: true, Count: 3},
				Items: []*orkstackv1.TaskRun{
					{TaskId: "fan", Item: proto.Int32(0), Status: orkstackv1.Status_STATUS_SUCCEEDED, ScheduledAt: now},
					{TaskId: "fan", Item: proto.Int32(1), Status: orkstackv1.Status_STATUS_PENDING},
					{TaskId: "fan", Item: proto.Int32(2), Status: orkstackv1.Status_STATUS_PENDING},
				}},
			{TaskId: "publish", Status: orkstackv1.Status_STATUS_PENDING},
		},
	}
}

// carryOver writes run's checkpoint and switch snapshot into store, under
// (namespace, runID)'s keys, and returns the generation-1 carry naming them.
func carryOver(t *testing.T, store *memSnapshotStore, run *orkstackv1.Run, namespace, runID string) *orkstackv1.RunContinuation {
	t.Helper()
	parts := checkpointPartsForTest(run, 2)
	data, checkpoint := checkpointObjectForTest(t, parts, namespace, runID, 0)
	store.put(checkpoint.GetUri(), data)
	snapData, snapshot := gzipObjectForTest(t, run, namespace, runID, 0, artifacts.ContinuationKindSnapshot)
	store.put(snapshot.GetUri(), snapData)
	return &orkstackv1.RunContinuation{
		Version: 1, Generation: 1, EventSequence: 41,
		CreatedAt: run.GetCreatedAt(), StartedAt: run.GetStartedAt(),
		Checkpoint: checkpoint, Snapshot: snapshot,
		Segments: []*orkstackv1.EventSegmentRef{{Generation: 0, FirstSequence: 1, LastSequence: 41, EventCount: 41,
			Object: segmentObjectForTest(t, namespace, runID)}},
		VersionDecisions: map[string]int32{leanEventPathChange: 1, ContinueAsNewChange: 1},
	}
}

// restoreOutcome runs the next generation from carry, and returns its error
// and the reads it made.
func restoreOutcome(t *testing.T, store *memSnapshotStore, plan RunPlan, carry *orkstackv1.RunContinuation) (*refusalHarness, error) {
	t.Helper()
	h := newRefusalHarnessOver(t, store)
	h.env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: time.Second})
	h.env.ExecuteWorkflow(RunWorkflowType, "", restoredOrkflow(t), (*orkstackv1.Payload)(nil), plan, carry)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the generation did not end")
	}
	return h, h.env.GetWorkflowError()
}

// TestContinuation_RestoreAdoptsAValidState is the positive control of the
// refusals below: the same carry, unaltered, restores and the run completes
// from where it switched.
func TestContinuation_RestoreAdoptsAValidState(t *testing.T) {
	store := newMemSnapshotStore()
	carry := carryOver(t, store, restoredRows(), "team-a", refusalWorkflowID)
	h, err := restoreOutcome(t, store, refusalRoutedPlan(t), carry)
	if err != nil {
		t.Fatalf("a valid saved state was not adopted: %v", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if first := h.events[0].GetSequence(); first != 42 {
		t.Fatalf("the restored run's first event is %d, want 42", first)
	}
}

// restoreRefusal is one saved state the bootstrap must refuse.
type restoreRefusal struct {
	mutate    func(run *orkstackv1.Run)
	namespace string
	runID     string
}

// requireRestoreRefused: a saved state with an orphan — a RUNNING, FAILED or
// CANCELED row, an item RUNNING, a fan-out whose settled count disagrees with
// its dispatched one — or one naming another run's objects, is never
// adopted: RUN_STATE_CORRUPT with a terminal event, and an object of another
// run is never even read.
func requireRestoreRefused(t *testing.T, c restoreRefusal) {
	t.Helper()
	store := newMemSnapshotStore()
	run := restoredRows()
	if c.mutate != nil {
		c.mutate(run)
	}
	namespace, runID := "team-a", refusalWorkflowID
	if c.namespace != "" {
		namespace = c.namespace
	}
	if c.runID != "" {
		runID = c.runID
	}
	carry := carryOver(t, store, run, namespace, runID)
	foreign := c.runID != "" || c.namespace != ""
	if foreign {
		// The segment reference is this run's own, so only the saved state
		// names another's.
		carry.Segments[0].Object = segmentObjectForTest(t, "team-a", refusalWorkflowID)
	}
	h, err := restoreOutcome(t, store, refusalRoutedPlan(t), carry)
	requireRunStateError(t, err, RunStateCorruptCode)
	h.mu.Lock()
	events := append([]*orkstackv1.Event(nil), h.events...)
	h.mu.Unlock()
	if len(events) != 1 || events[0].GetSequence() != 42 || events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Fatalf("published %v, want one RUN_FAILED at 42", events)
	}
	if foreign {
		for _, uri := range store.reads {
			if uri == carry.GetCheckpoint().GetUri() {
				t.Fatal("another run's saved state was read")
			}
		}
	}
}

func TestContinuation_RestoreRefusesARunningRow(t *testing.T) {
	requireRestoreRefused(t, restoreRefusal{mutate: func(run *orkstackv1.Run) {
		run.TaskRuns[2].Status = orkstackv1.Status_STATUS_RUNNING
	}})
}

func TestContinuation_RestoreRefusesAFailedRow(t *testing.T) {
	for _, status := range []orkstackv1.Status{orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_CANCELED} {
		requireRestoreRefused(t, restoreRefusal{mutate: func(run *orkstackv1.Run) { run.TaskRuns[0].Status = status }})
	}
}

func TestContinuation_RestoreRefusesAnItemRunning(t *testing.T) {
	requireRestoreRefused(t, restoreRefusal{mutate: func(run *orkstackv1.Run) {
		run.TaskRuns[1].Items[1].Status = orkstackv1.Status_STATUS_RUNNING
		run.TaskRuns[1].Items[1].ScheduledAt = timestamppb.Now()
	}})
}

func TestContinuation_RestoreRefusesASettledCountMismatch(t *testing.T) {
	requireRestoreRefused(t, restoreRefusal{mutate: func(run *orkstackv1.Run) {
		run.TaskRuns[1].Items[1].ScheduledAt = timestamppb.Now() // dispatched, never settled
	}})
}

func TestContinuation_RestoreRefusesACheckpointOfAnotherRun(t *testing.T) {
	requireRestoreRefused(t, restoreRefusal{runID: "ork-1700000000-43"})
	requireRestoreRefused(t, restoreRefusal{namespace: "team-b"})
}

func TestContinuation_RestoreRefusesASwitchSnapshotOfAnotherRun(t *testing.T) {
	store := newMemSnapshotStore()
	carry := carryOver(t, store, restoredRows(), "team-a", refusalWorkflowID)
	_, other := gzipObjectForTest(t, restoredRows(), "team-a", "ork-1700000000-43", 0, artifacts.ContinuationKindSnapshot)
	carry.Snapshot = other
	_, err := restoreOutcome(t, store, refusalRoutedPlan(t), carry)
	requireRunStateError(t, err, RunStateCorruptCode)
	if len(store.reads) != 0 {
		t.Fatalf("read %v before refusing a carry naming another run's object", store.reads)
	}
}

// TestRestore_NeverPanicsOnAMalformedCarryOrPlan: every malformed carry or
// plan ends RUN_STATE_CORRUPT or RUN_STATE_UNSUPPORTED with a published
// terminal event — never a panic, which in workflow code would be a
// workflow-task failure loop, a stuck run.
func TestRestore_NeverPanicsOnAMalformedCarryOrPlan(t *testing.T) {
	bad := validBudget()
	bad.ChunkBytes = 0
	cases := map[string]struct {
		plan  func(RunPlan) RunPlan
		carry func(c *orkstackv1.RunContinuation)
	}{
		"a nil plan.Continuation": {plan: func(p RunPlan) RunPlan { p.Continuation = nil; return p }},
		"a nil plan.History":      {plan: func(p RunPlan) RunPlan { p.History = nil; return p }},
		"a budget failing valid()": {plan: func(p RunPlan) RunPlan {
			p.Continuation = &bad
			return p
		}},
		"a nil checkpoint": {carry: func(c *orkstackv1.RunContinuation) { c.Checkpoint = nil }},
		"empty parts":      {carry: func(c *orkstackv1.RunContinuation) { c.Checkpoint.Parts = nil }},
		"nil timestamps":   {carry: func(c *orkstackv1.RunContinuation) { c.CreatedAt, c.StartedAt = nil, nil }},
		"a nil snapshot and no segments": {carry: func(c *orkstackv1.RunContinuation) {
			c.Snapshot = nil
			c.Segments = nil
		}},
		"a negative event_sequence": {carry: func(c *orkstackv1.RunContinuation) { c.EventSequence = -5 }},
		"mismatched segment counts": {carry: func(c *orkstackv1.RunContinuation) { c.Generation = 3 }},
		"an unknown format":         {carry: func(c *orkstackv1.RunContinuation) { c.Version = 7 }},
		"an empty carry":            {carry: func(c *orkstackv1.RunContinuation) { proto.Reset(c); c.Version = 1 }},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			store := newMemSnapshotStore()
			carry := carryOver(t, store, restoredRows(), "team-a", refusalWorkflowID)
			plan := refusalRoutedPlan(t)
			if c.plan != nil {
				plan = c.plan(plan)
			}
			if c.carry != nil {
				c.carry(carry)
			}
			h, err := restoreOutcome(t, store, plan, carry)
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || (app.Type() != RunStateCorruptCode && app.Type() != RunStateUnsupportedCode) {
				t.Fatalf("workflow error = %v, want RUN_STATE_CORRUPT or RUN_STATE_UNSUPPORTED", err)
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			if len(h.events) != 1 || h.events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
				t.Fatalf("published %v, want exactly one terminal event", h.events)
			}
		})
	}
}

// TestContinuation_RestoreDeadlinePassedIssuesNoRead: a deadline already
// passed between chunks ends the read RUN_STATE_UNAVAILABLE without
// scheduling another.
func TestContinuation_RestoreDeadlinePassedIssuesNoRead(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var reads int
	var mu sync.Mutex
	env.RegisterActivityWithOptions(func(context.Context, ContinuationLocator, int64, int64, int64, ContinuationObservationInput) ([]byte, error) {
		mu.Lock()
		reads++
		mu.Unlock()
		return []byte("x"), nil
	}, activity.RegisterOptions{Name: ActivityReadContinuationChunk})
	var code string
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		s := &runState{runID: "ork-deadline", wf: &orkstackv1.Orkflow{}}
		_, code = s.readChunkBefore(ctx, workflow.Now(ctx).Add(-time.Second), validBudget(),
			HistoryLimit{Reserve: DefaultHistoryReserve}, ContinuationLocator{URI: "s3://x"}, 1, 0, 1, ContinuationObservationInput{})
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if code != RunStateUnavailableCode || reads != 0 {
		t.Fatalf("code %q after %d reads, want %s and none", code, reads, RunStateUnavailableCode)
	}
}

// memoRecorder records every UpsertMemo a workflow makes.
type memoRecorder struct {
	interceptor.WorkerInterceptorBase
	mu      sync.Mutex
	upserts int
	cans    int
}

func (r *memoRecorder) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &memoRecorderInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, r: r}
}

type memoRecorderInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	r *memoRecorder
}

func (i *memoRecorderInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&memoRecorderOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, r: i.r})
}

type memoRecorderOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	r *memoRecorder
}

func (o *memoRecorderOutbound) UpsertMemo(ctx workflow.Context, memo map[string]any) error {
	o.r.mu.Lock()
	o.r.upserts++
	o.r.mu.Unlock()
	return o.Next.UpsertMemo(ctx, memo)
}

func (o *memoRecorderOutbound) NewContinueAsNewError(ctx workflow.Context, wfn any, args ...any) error {
	o.r.mu.Lock()
	o.r.cans++
	o.r.mu.Unlock()
	return o.Next.NewContinueAsNewError(ctx, wfn, args...)
}

// TestContinuation_OversizedCarryLeavesNoMemoUpsert: a generation 0 whose
// carry would exceed 256 KiB never switches, and — because the memo upsert
// comes after the carry's check — leaves no stray memo upsert in history.
func TestContinuation_OversizedCarryLeavesNoMemoUpsert(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	recorder := &memoRecorder{}
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{recorder}})
	store := newMemSnapshotStore()
	a := NewSnapshotActivities(store)
	env.RegisterActivityWithOptions(a.WriteEventSegment, activity.RegisterOptions{Name: ActivityWriteEventSegment})
	env.RegisterActivityWithOptions(a.WriteSwitchSnapshot, activity.RegisterOptions{Name: ActivityWriteSwitchSnapshot})
	env.RegisterActivityWithOptions(a.WriteCheckpoint, activity.RegisterOptions{Name: ActivityWriteCheckpoint})
	var noSwitch string
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		plan := refusalRoutedPlan(t)
		s := newRunState(ctx, refusalWorkflowID, restoredOrkflow(t), nil, plan)
		s.switchDue = true
		// Decisions an earlier build made, carried through untouched, too
		// large for any carry.
		s.versions = map[string]workflow.Version{strings.Repeat("x", 300<<10): 1}
		canErr, _, fatal := s.continueAsNew(ctx, map[string]workflow.Future{})
		if canErr != nil || fatal != nil {
			return fmt.Errorf("switched: %v %v", canErr, fatal)
		}
		noSwitch = s.noSwitch
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if noSwitch != "its carried bookkeeping is larger than 256 KiB" || recorder.upserts != 0 || recorder.cans != 0 {
		t.Fatalf("noSwitch %q, %d memo upserts, %d switches", noSwitch, recorder.upserts, recorder.cans)
	}
}

var _ = task.DefaultPool

// TestReadyLoop_HeldTasksCostConstantPerPass: a 4,000-task flat stage held
// by a quiesce after 586 dispatches (spec 7.4's flat state: its reserve
// alone is over the hold line, so the hold starts the moment its budget is
// crossed). Every task is classified — and its condition evaluated — once,
// on its first pass, never again however many passes it waits through; no
// pin closure is rebuilt; and one held pass's workflow code takes at most
// 20 ms of its thread's CPU time (passClock; the wall clock is logged).
func TestReadyLoop_HeldTasksCostConstantPerPass(t *testing.T) {
	const tasks, dispatched = 4000, 586
	wf := &orkstackv1.Orkflow{Name: "flat", Version: "v1", Namespace: "team-a"}
	for i := 0; i < tasks; i++ {
		wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: fmt.Sprintf("t%d", i), Image: &orkstackv1.ImageRef{Reference: "alpine"}})
	}
	plan := refusalRoutedPlan(t)
	budget := DefaultContinuationBudget(dispatched, 1<<40, time.Hour)
	plan.Continuation = &budget
	// Small inputs: the test environment's raw history never moves, so every
	// dispatch's byte charge accumulates; the event dimension is the one
	// under test.
	plan.Limits = WorkspaceLimits{ValuesMaxBytes: 4096}

	// Each pass timed on the pass clock, which the bound is asserted on, and
	// on the wall clock, which is logged as evidence.
	probe := &readyLoopProbe{}
	clock := &passClock{}
	var passStart time.Duration
	var wallStart time.Time
	var passes, wallPasses []time.Duration
	probe.pass = func() { passStart, wallStart = clock.now(), time.Now() }
	probe.selector = func() {
		passes = append(passes, clock.now()-passStart)
		wallPasses = append(wallPasses, time.Since(wallStart))
	}
	readyLoopProbeForTest = probe
	defer func() { readyLoopProbeForTest = nil }()

	store := newMemSnapshotStore()
	h := newRefusalHarnessOver(t, store)
	h.env.SetTestTimeout(10 * time.Minute)
	h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, wf, (*orkstackv1.Payload)(nil), plan, (*orkstackv1.RunContinuation)(nil))
	var can *workflow.ContinueAsNewError
	if !errors.As(h.env.GetWorkflowError(), &can) {
		t.Fatalf("workflow error = %v, want the switch after the hold", h.env.GetWorkflowError())
	}
	if probe.classify != tasks || probe.conditions != tasks {
		t.Fatalf("classify ran %d times and conditions %d, want each task once (%d)", probe.classify, probe.conditions, tasks)
	}
	if probe.closureRebuilds > len(passes) {
		t.Fatalf("%d pin-closure rebuilds over %d passes, want at most one a pass", probe.closureRebuilds, len(passes))
	}
	if len(passes) < dispatched {
		t.Fatalf("%d passes, want at least one per collected completion (%d)", len(passes), dispatched)
	}
	held := passes[1:] // the first pass dispatched; every later one held 3,414
	var worst, total, wallWorst time.Duration
	for _, d := range held {
		worst = max(worst, d)
		total += d
	}
	for _, d := range wallPasses[1:] {
		wallWorst = max(wallWorst, d)
	}
	proof.WriteJSON(t, "continuation", "held-pass-cost.json", map[string]any{
		"tasks": tasks, "dispatched_before_hold": dispatched, "held_passes": len(held),
		"slowest_pass_ms": float64(worst.Microseconds()) / 1000, "mean_pass_ms": float64(total.Microseconds()) / 1000 / float64(len(held)),
		"pass_clock": clock.kind(), "slowest_pass_wall_ms": float64(wallWorst.Microseconds()) / 1000,
	})
	t.Logf("E2 held tasks: %d held passes over %d tasks, slowest %s, mean %s of %s (slowest %s wall clock)",
		len(held), tasks-dispatched, worst, total/time.Duration(len(held)), clock.kind(), wallWorst)
	if worst > 20*time.Millisecond {
		t.Fatalf("the slowest held pass took %s of %s, over 20 ms", worst, clock.kind())
	}
}

// awaitedRestoreOrkflow is build → three awaits → publish.
func awaitedRestoreOrkflow(t *testing.T) *orkstackv1.Orkflow {
	t.Helper()
	wf, err := dsl.Compile([]byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: awaited
version: v1
tasks:
  - id: build
    uses: shell
    with: {op: exec}
  - id: gate0
    needs: [build]
    await: {timeout_seconds: 600}
  - id: gate1
    needs: [build]
    await: {timeout_seconds: 600}
  - id: gate2
    needs: [build]
    await: {timeout_seconds: 600}
  - id: publish
    uses: shell
    needs: [gate0, gate1, gate2]
    with: {op: exec}
`))
	if err != nil {
		t.Fatal(err)
	}
	wf.Namespace = "team-a"
	return wf
}

// awaitedRestoreRows is a state a switch could save for
// awaitedRestoreOrkflow: build SUCCEEDED, the first open gates AWAITING with
// a deadline still ahead, the rest PENDING, publish PENDING.
func awaitedRestoreRows(open int) *orkstackv1.Run {
	now := timestamppb.New(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	ahead := timestamppb.New(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))
	rows := []*orkstackv1.TaskRun{{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED,
		ValuesRef: &orkstackv1.ArtifactRef{Uri: "s3://x/values.json"}}}
	for i := 0; i < 3; i++ {
		row := &orkstackv1.TaskRun{TaskId: fmt.Sprintf("gate%d", i), Status: orkstackv1.Status_STATUS_PENDING}
		if i < open {
			row.Status = orkstackv1.Status_STATUS_AWAITING
			row.Await = &orkstackv1.TaskAwait{Deadline: ahead}
		}
		rows = append(rows, row)
	}
	rows = append(rows, &orkstackv1.TaskRun{TaskId: "publish", Status: orkstackv1.Status_STATUS_PENDING})
	return &orkstackv1.Run{
		Id: refusalWorkflowID, Namespace: "team-a", OrkflowName: "awaited", OrkflowVersion: "v1",
		Status: orkstackv1.Status_STATUS_RUNNING, CreatedAt: now, StartedAt: now, TaskRuns: rows,
	}
}

// restoreProbe restores run's carry over a fixed raw history length, then
// marks the generation's base, exactly as RunWorkflow does, and hands the
// state and the number of chunk reads to inspect. inspect runs on the
// workflow's goroutine, so it reports with t.Errorf, never t.Fatalf.
func restoreProbe(t *testing.T, run *orkstackv1.Run, inspect func(s *runState, code string, reads int)) {
	t.Helper()
	store := newMemSnapshotStore()
	carry := carryOver(t, store, run, "team-a", refusalWorkflowID)
	wf, plan := awaitedRestoreOrkflow(t), refusalPlan()
	runGuardWorkflow(t, func(env *testsuite.TestWorkflowEnvironment) {
		env.SetCurrentHistoryLength(restoreProbeLength)
		env.RegisterActivityWithOptions(NewSnapshotActivities(store).ReadContinuationChunk,
			activity.RegisterOptions{Name: ActivityReadContinuationChunk})
	}, func(ctx workflow.Context) error {
		s := newRunState(ctx, refusalWorkflowID, wf, guardInput(t, map[string]any{}), plan)
		s.beginPass()
		code, _ := s.restore(ctx, ctx, carry)
		if code == "" {
			s.markGenerationBase(ctx)
		}
		store.mu.Lock()
		reads := len(store.reads)
		store.mu.Unlock()
		inspect(s, code, reads)
		return nil
	})
}

const restoreProbeLength = 100

// TestContinuation_RestoredAwaitTimersArePartOfTheBase (implementer note 4):
// each open await's recreated deadline timer is a command of the bootstrap's
// workflow task, so it is counted before the generation marks its base —
// part of the base, never the generation's growth, exactly like the saved
// state's chunk reads. With K awaits open the base is the raw history, one
// event per chunk read, and K.
func TestContinuation_RestoredAwaitTimersArePartOfTheBase(t *testing.T) {
	for _, open := range []int{0, 1, 3} {
		restoreProbe(t, awaitedRestoreRows(open), func(s *runState, code string, reads int) {
			if code != "" {
				t.Errorf("open=%d: restore refused a valid state: %s", open, code)
			}
			if reads == 0 {
				t.Errorf("open=%d: restore read nothing", open)
			}
			want := int64(restoreProbeLength + reads + open)
			if s.baseEvents != want {
				t.Errorf("open=%d: base = %d events, want %d (history %d + %d chunk reads + %d await timers)",
					open, s.baseEvents, want, restoreProbeLength, reads, open)
			}
			for i := 0; i < 3; i++ {
				a := s.awaits[fmt.Sprintf("gate%d", i)]
				if recreated := a.deadline != nil; recreated != (i < open) {
					t.Errorf("open=%d: gate%d timer recreated = %t", open, i, recreated)
				}
			}
		})
	}
}

// TestContinuation_RestoreRefusalRecreatesNoAwaitTimer: every row is
// validated before any timer is recreated, so a saved state refused for a
// row after an open await leaves no stray timer behind.
func TestContinuation_RestoreRefusalRecreatesNoAwaitTimer(t *testing.T) {
	run := awaitedRestoreRows(3)
	run.TaskRuns[4].Status = orkstackv1.Status_STATUS_FAILED
	restoreProbe(t, run, func(s *runState, code string, _ int) {
		if code != RunStateCorruptCode {
			t.Errorf("code = %q, want %s", code, RunStateCorruptCode)
		}
		for i := 0; i < 3; i++ {
			if s.awaits[fmt.Sprintf("gate%d", i)].deadline != nil {
				t.Errorf("gate%d's timer was recreated for a refused state", i)
			}
		}
	})
}

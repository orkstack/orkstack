package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// timeoutHarness runs RunWorkflow for a definition that declares a run
// timeout; the test sets how long each container task takes.
type timeoutHarness struct {
	env    *testsuite.TestWorkflowEnvironment
	store  *memSnapshotStore
	mu     sync.Mutex
	events []*orkstackv1.Event
	uris   []string
	tasks  []string
}

// timed is document with a top-level `timeout:` of d.
func timed(document, d string) string {
	return strings.Replace(document, "\nname: ", "\ntimeout: "+d+"\nname: ", 1)
}

func newTimeoutHarness(t *testing.T) *timeoutHarness {
	t.Helper()
	h := &timeoutHarness{store: newMemSnapshotStore()}
	var suite testsuite.WorkflowTestSuite
	h.env = generousDeadlock(suite.NewTestWorkflowEnvironment())
	h.env.SetTestTimeout(60 * time.Second)
	h.env.SetStartWorkflowOptions(startOptionsForTest())
	h.env.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	activities := NewSnapshotActivities(h.store)
	h.env.RegisterActivityWithOptions(activities.WriteSnapshot, activity.RegisterOptions{Name: ActivityWriteSnapshot})
	h.env.RegisterActivityWithOptions(func(_ context.Context, event *orkstackv1.Event, uri, _ string) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.events = append(h.events, event)
		h.uris = append(h.uris, uri)
		return nil
	}, activity.RegisterOptions{Name: ActivityPublishEvent})
	h.env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return nil, errors.New("replaced by the mock")
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	return h
}

// taskTakes makes the next n container calls (all of them when n is 0) take
// d of workflow time and succeed.
func (h *timeoutHarness) taskTakes(d time.Duration, n int) {
	call := h.env.OnActivity(task.ActivityExecuteTask, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			h.mu.Lock()
			h.tasks = append(h.tasks, in.GetTaskId())
			h.mu.Unlock()
			return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, ValuesJson: []byte(`{}`)}, nil
		}).After(d)
	if n > 0 {
		call.Times(n)
	}
}

func (h *timeoutHarness) run(t *testing.T, document string, plan RunPlan) error {
	t.Helper()
	h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, guardCompile(t, document), (*orkstackv1.Payload)(nil),
		plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the run did not complete")
	}
	return h.env.GetWorkflowError()
}

// last is the terminal event and the run its snapshot holds.
func (h *timeoutHarness) last(t *testing.T) (*orkstackv1.Event, *orkstackv1.Run) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	event := h.events[len(h.events)-1]
	data, ok := h.store.get(h.uris[len(h.uris)-1])
	if !ok {
		t.Fatalf("terminal snapshot %q was never written", h.uris[len(h.uris)-1])
	}
	run := &orkstackv1.Run{}
	must.OK(t, proto.Unmarshal(data, run))
	return event, run
}

func rowOf(run *orkstackv1.Run, id string) *orkstackv1.TaskRun {
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == id {
			return tr
		}
	}
	return nil
}

func timeoutPlan(t *testing.T, grace int64) RunPlan {
	plan := guardPlan(t, 1_000_000, 1<<40)
	plan.RunTimeout = &RunTimeoutPlan{HandlerGraceSeconds: grace, ResolutionsPerPass: DefaultResolutionsPerPass}
	return plan
}

const twoStepDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: two-step
tasks:
  - id: build
    uses: shell
    with: {op: exec}
  - id: ship
    uses: shell
    needs: [build]
    with: {op: exec}
`

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) || applicationErr.Type() != code || !applicationErr.NonRetryable() {
		t.Fatalf("workflow error = %v, want a non-retryable %s", err, code)
	}
}

// At D a run fails RUN_TIMED_OUT: running work is recorded stopped by the
// run timeout, never-started work CANCELED, and the closing error and the
// terminal snapshot carry the same message.
func TestRunTimeout_FiresMidTask(t *testing.T) {
	const limit = "60s"
	h := newTimeoutHarness(t)
	h.taskTakes(2*time.Hour, 0)
	err := h.run(t, timed(twoStepDocument, limit), timeoutPlan(t, 600))
	requireCode(t, err, RunTimedOutCode)

	event, run := h.last(t)
	if event.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("terminal event %s, run %s; want RUN_FAILED, FAILED", event.GetType(), run.GetStatus())
	}
	runErr := run.GetError()
	if runErr.GetCode() != RunTimedOutCode || runErr.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT {
		t.Fatalf("run error = %v", runErr)
	}
	for _, want := range []string{"run did not finish within its timeout of 1m0s", "stopped: build", "not started: 1 task"} {
		if !strings.Contains(runErr.GetMessage(), want) {
			t.Fatalf("run message %q lacks %q", runErr.GetMessage(), want)
		}
	}
	var applicationErr *temporal.ApplicationError
	_ = errors.As(err, &applicationErr)
	if applicationErr.Message() != runErr.GetMessage() {
		t.Fatalf("closing error %q differs from the snapshot's %q", applicationErr.Message(), runErr.GetMessage())
	}
	build, ship := rowOf(run, "build"), rowOf(run, "ship")
	if build.GetStatus() != orkstackv1.Status_STATUS_FAILED || build.GetError().GetCode() != TaskStoppedByRunTimeoutCode ||
		build.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT {
		t.Fatalf("build row = %s %v; want FAILED %s", build.GetStatus(), build.GetError(), TaskStoppedByRunTimeoutCode)
	}
	if ship.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("ship row = %s; want CANCELED, never started", ship.GetStatus())
	}
	if took := run.GetFinishedAt().AsTime().Sub(run.GetStartedAt().AsTime()); took < time.Minute || took > time.Minute+time.Second {
		t.Fatalf("the run ended %s after it started, want at its 1m deadline", took)
	}
}

// A run that finishes before D succeeds; its timer is abandoned.
func TestRunTimeout_FinishedBeforeTheDeadlineSucceeds(t *testing.T) {
	const limit = "3600s"
	h := newTimeoutHarness(t)
	h.taskTakes(10*time.Second, 0)
	must.OK(t, h.run(t, timed(twoStepDocument, limit), timeoutPlan(t, 600)))
	_, run := h.last(t)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s, want SUCCEEDED", run.GetStatus())
	}
}

// A definition with a run timeout whose plan lacks it ends explicitly,
// before it starts anything.
func TestRunTimeout_PlanWithoutItIsUnsupported(t *testing.T) {
	const limit = "60s"
	h := newTimeoutHarness(t)
	h.taskTakes(time.Second, 0)
	err := h.run(t, timed(twoStepDocument, limit), guardPlan(t, 1_000_000, 1<<40))
	requireCode(t, err, RunDefinitionUnsupportedCode)
	if len(h.tasks) != 0 {
		t.Fatalf("started %v", h.tasks)
	}
	_, run := h.last(t)
	if run.GetError().GetCode() != RunDefinitionUnsupportedCode ||
		run.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION {
		t.Fatalf("run error = %v", run.GetError())
	}
}

// Without a timeout nothing changes: no state, no timer.
func TestRunTimeout_AbsentBuildsNothing(t *testing.T) {
	if newRunTimeout(guardCompile(t, twoStepDocument), RunPlan{}) != nil {
		t.Fatal("a definition with no timeout built run-timeout state")
	}
}

const handlerDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: with-handler
tasks:
  - id: build
    uses: shell
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [build]
    join: any_failed
    with: {op: exec}
`

// Interrupted work is FAILED, so an any_failed handler of it runs; the run
// still ends RUN_TIMED_OUT.
func TestRunTimeout_AnyFailedHandlerRunsForInterruptedWork(t *testing.T) {
	const limit = "60s"
	h := newTimeoutHarness(t)
	h.taskTakes(2*time.Hour, 1)    // build
	h.taskTakes(10*time.Second, 0) // rollback
	err := h.run(t, timed(handlerDocument, limit), timeoutPlan(t, 600))
	requireCode(t, err, RunTimedOutCode)
	_, run := h.last(t)
	if got := rowOf(run, "build").GetError().GetCode(); got != TaskStoppedByRunTimeoutCode {
		t.Fatalf("build error code %q", got)
	}
	if got := rowOf(run, "rollback").GetStatus(); got != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("rollback %s, want SUCCEEDED: an any_failed handler of interrupted work runs", got)
	}
	if run.GetError().GetCode() != RunTimedOutCode {
		t.Fatalf("run error %v", run.GetError())
	}
}

// At H = D + grace a failure handler still running is stopped, CANCELED, and
// the run ends RUN_TIMED_OUT with what the handlers did.
func TestRunTimeout_HardEndStopsARunningHandler(t *testing.T) {
	const limit = "60s"
	h := newTimeoutHarness(t)
	h.taskTakes(2*time.Hour, 1) // build
	h.taskTakes(2*time.Hour, 0) // rollback outlives the grace
	err := h.run(t, timed(handlerDocument, limit), timeoutPlan(t, 30))
	requireCode(t, err, RunTimedOutCode)
	_, run := h.last(t)
	rollback := rowOf(run, "rollback")
	if rollback.GetStatus() != orkstackv1.Status_STATUS_CANCELED ||
		!strings.Contains(rollback.GetError().GetMessage(), "stopped at the run timeout's hard end") {
		t.Fatalf("rollback %s %v; want CANCELED, stopped at the hard end", rollback.GetStatus(), rollback.GetError())
	}
	if !strings.Contains(run.GetError().GetMessage(), "its timeout's hard end passed at") ||
		!strings.Contains(run.GetError().GetMessage(), "rollback was stopped") {
		t.Fatalf("run message %q", run.GetError().GetMessage())
	}
	if took := run.GetFinishedAt().AsTime().Sub(run.GetStartedAt().AsTime()); took < 90*time.Second || took > 91*time.Second {
		t.Fatalf("the run ended %s after it started, want at its hard end, 1m30s", took)
	}
}

// With a grace of 0 no failure handler runs after the deadline.
func TestRunTimeout_GraceZeroStartsNoHandler(t *testing.T) {
	const limit = "60s"
	h := newTimeoutHarness(t)
	h.taskTakes(2*time.Hour, 0)
	err := h.run(t, timed(handlerDocument, limit), timeoutPlan(t, 0))
	requireCode(t, err, RunTimedOutCode)
	_, run := h.last(t)
	rollback := rowOf(run, "rollback")
	if rollback.GetStatus() != orkstackv1.Status_STATUS_CANCELED || rollback.GetScheduledAt() != nil ||
		!strings.Contains(rollback.GetError().GetMessage()+" "+eventsFor(h, "rollback"), "passed before this handler started") {
		t.Fatalf("rollback %s scheduled=%v; want CANCELED, never dispatched, its reason the hard end",
			rollback.GetStatus(), rollback.GetScheduledAt())
	}
}

// eventsFor joins the messages of every event of one task.
func eventsFor(h *timeoutHarness, id string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var b strings.Builder
	for _, e := range h.events {
		if e.GetTaskId() == id {
			b.WriteString(e.GetMessage() + "\n")
		}
	}
	return b.String()
}

// The run timeout's codes read the same with no snapshot as with one, and a
// child's own timeout maps to its parent's composed row as TIMED_OUT.
func TestRunTimeout_CodesAreRunLevel(t *testing.T) {
	cases := map[string]orkstackv1.FailureCategory{
		RunTimedOutCode:              orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT,
		RunDefinitionUnsupportedCode: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
		RunTimeoutStateInvalidCode:   orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
	}
	for code, category := range cases {
		if !isRunLevelFailureType(code) || runLevelFailureCategory(code) != category {
			t.Errorf("%s: run-level %v, category %s; want true, %s", code, isRunLevelFailureType(code),
				runLevelFailureCategory(code), category)
		}
		_, childCode, childCategory := childFailure(temporal.NewNonRetryableApplicationError("x", code, nil))
		if childCode != ChildRunFailedCode || childCategory != category {
			t.Errorf("%s as a child's end: %s %s; want %s %s", code, childCode, childCategory, ChildRunFailedCode, category)
		}
	}
}

// A timed-out run fingerprints as run-level: its interrupted rows are not a
// primary failed task.
func TestFailures_RunTimeoutIsRunLevel(t *testing.T) {
	run := &orkstackv1.Run{TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "build", Status: orkstackv1.Status_STATUS_FAILED, Error: &orkstackv1.Error{Code: TaskStoppedByRunTimeoutCode}},
		{TaskId: "fan", Status: orkstackv1.Status_STATUS_FAILED, Error: &orkstackv1.Error{Code: TaskStoppedByRunTimeoutCode},
			Items: []*orkstackv1.TaskRun{{TaskId: "fan", Status: orkstackv1.Status_STATUS_FAILED,
				Error: &orkstackv1.Error{Code: TaskStoppedByRunTimeoutCode}}}},
	}}
	if got := PrimaryFailedTask(run); got != nil {
		t.Fatalf("PrimaryFailedTask = %s, want none for a run its timeout stopped", got.GetTaskId())
	}
}

// The reserve counts the run timeout's six events only for a run that
// declares one.
func TestRunTimeout_ReserveCountsItsTimers(t *testing.T) {
	r := HistoryReserve{Fixed: 10}
	if got := (&runState{}).fixedReserve(r); got != 10 {
		t.Fatalf("without a timeout: %d, want 10", got)
	}
	if got := (&runState{runTimeout: &runTimeout{seconds: 60}}).fixedReserve(r); got != 16 {
		t.Fatalf("with a timeout: %d, want 16", got)
	}
}

// A fan-out whose items were running at D is itself stopped: its interrupted
// items are FAILED, the node FAILED TASK_STOPPED_BY_RUN_TIMEOUT, and its
// summary counts them as running.
func TestRunTimeout_FanOutNode(t *testing.T) {
	const limit = "60s"
	h := newTimeoutHarness(t)
	h.taskTakes(2*time.Hour, 0)
	h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, guardCompile(t, `
apiVersion: orkstack/v1
kind: Orkflow
timeout: `+limit+`
name: fan
inputs:
  regions: {type: array}
tasks:
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.regions }}", max_items: 3, max_parallel: 3}
    with: {op: exec, r: "{{ .each.item }}"}
`), guardInput(t, map[string]any{"regions": []any{"a", "b", "c"}}), timeoutPlan(t, 600), (*orkstackv1.RunContinuation)(nil))
	requireCode(t, h.env.GetWorkflowError(), RunTimedOutCode)
	_, run := h.last(t)
	node := rowOf(run, "convert")
	if node.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != TaskStoppedByRunTimeoutCode {
		t.Fatalf("node %s %v; want FAILED %s", node.GetStatus(), node.GetError(), TaskStoppedByRunTimeoutCode)
	}
	for _, item := range node.GetItems() {
		if item.GetStatus() != orkstackv1.Status_STATUS_FAILED || item.GetError().GetCode() != TaskStoppedByRunTimeoutCode {
			t.Fatalf("item %s %v", item.GetStatus(), item.GetError())
		}
	}
	if !strings.Contains(eventsFor(h, "convert"), "3 were running") {
		t.Fatalf("no summary counting the interrupted items as running:\n%s", eventsFor(h, "convert"))
	}
}

const gatesDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: gates
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
`

// More resolutions queued at once than a pass applies: each pass applies at
// most its bound, none is lost, and the run finishes.
func TestRunTimeout_ResolutionsOverTheBoundAreAllApplied(t *testing.T) {
	const limit = "3600s"
	h := newTimeoutHarness(t)
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() {
		for _, id := range []string{"gate0", "gate1", "gate2"} {
			h.env.SignalWorkflow(ResolveAwaitSignal, &orkstackv1.AwaitSignal{
				TaskId: id, Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API,
				Values:     &orkstackv1.ArtifactRef{Uri: "memory://values/" + id},
				ValuesJson: []byte(`{}`),
			})
		}
	}, time.Minute)
	plan := timeoutPlan(t, 600)
	plan.RunTimeout.ResolutionsPerPass = 1
	must.OK(t, h.run(t, timed(gatesDocument, limit), plan))
	_, run := h.last(t)
	for _, id := range []string{"gate0", "gate1", "gate2", "publish"} {
		if got := rowOf(run, id).GetStatus(); got != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("%s %s, want SUCCEEDED", id, got)
		}
	}
}

// Resolutions still awaited at D are not applied: the awaits are stopped by
// the run timeout, and the run ends RUN_TIMED_OUT.
func TestRunTimeout_OpenAwaitsAreInterrupted(t *testing.T) {
	const limit = "60s"
	h := newTimeoutHarness(t)
	h.taskTakes(10*time.Second, 0)
	err := h.run(t, timed(gatesDocument, limit), timeoutPlan(t, 600))
	requireCode(t, err, RunTimedOutCode)
	_, run := h.last(t)
	for _, id := range []string{"gate0", "gate1", "gate2"} {
		row := rowOf(run, id)
		if row.GetStatus() != orkstackv1.Status_STATUS_FAILED || row.GetError().GetCode() != TaskStoppedByRunTimeoutCode {
			t.Fatalf("%s %s %v; want FAILED %s", id, row.GetStatus(), row.GetError(), TaskStoppedByRunTimeoutCode)
		}
	}
	if got := rowOf(run, "publish").GetStatus(); got != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("publish %s, want CANCELED", got)
	}
}

// With a grace of 0 no "waiting for its failure handlers" note is emitted:
// none will run.
func TestRunTimeout_GraceZeroEmitsNoWaitingNote(t *testing.T) {
	const limit = "60s"
	h := newTimeoutHarness(t)
	h.taskTakes(2*time.Hour, 0)
	requireCode(t, h.run(t, timed(handlerDocument, limit), timeoutPlan(t, 0)), RunTimedOutCode)
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range h.events {
		if strings.Contains(e.GetMessage(), "waiting for its failure handlers") {
			t.Fatalf("emitted %q though no handler can start", e.GetMessage())
		}
	}
}

// A run with handlers ended RUN_DEFINITION_UNSUPPORTED says so on its rows,
// never that it reached its history limit.
func TestRunTimeout_UnsupportedPlanNamesItsCauseWithHandlers(t *testing.T) {
	const limit = "60s"
	h := newTimeoutHarness(t)
	h.taskTakes(time.Second, 0)
	requireCode(t, h.run(t, timed(handlerDocument, limit), guardPlan(t, 1_000_000, 1<<40)), RunDefinitionUnsupportedCode)
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range h.events {
		if strings.Contains(e.GetMessage(), "history limit") {
			t.Fatalf("event says %q for a RUN_DEFINITION_UNSUPPORTED run", e.GetMessage())
		}
	}
}

// TestRunTimeout_SnapshotCarriesTheClock: every snapshot of a run whose
// definition declares `timeout:` shows the limit and, once known, the
// deadline, the hard end and how the run timeout ended it; a run without one
// shows nothing.
func TestRunTimeout_SnapshotCarriesTheClock(t *testing.T) {
	d := time.Date(2026, 11, 12, 16, 0, 0, 0, time.UTC)
	h := d.Add(10 * time.Minute)
	cases := []struct {
		name    string
		timeout *runTimeout
		code    string
		want    *orkstackv1.RunTimeout
	}{
		{name: "no timeout declared"},
		{name: "not started", timeout: &runTimeout{seconds: 21600},
			want: &orkstackv1.RunTimeout{Seconds: 21600}},
		{name: "running", timeout: &runTimeout{seconds: 21600, deadline: d},
			want: &orkstackv1.RunTimeout{Seconds: 21600, Deadline: timestamppb.New(d)}},
		{name: "failing for another cause", timeout: &runTimeout{seconds: 21600, deadline: d, hardEnd: h},
			code: "TASK_FAILED",
			want: &orkstackv1.RunTimeout{Seconds: 21600, Deadline: timestamppb.New(d), HardEnd: timestamppb.New(h)}},
		{name: "timed out", timeout: &runTimeout{seconds: 21600, deadline: d, taken: true, hardEnd: h},
			code: RunTimedOutCode,
			want: &orkstackv1.RunTimeout{Seconds: 21600, Deadline: timestamppb.New(d), HardEnd: timestamppb.New(h), TimedOut: true}},
		{name: "hard end reached", timeout: &runTimeout{seconds: 21600, deadline: d, taken: true, hardEnd: h, hardEnded: true},
			code: RunTimedOutCode,
			want: &orkstackv1.RunTimeout{Seconds: 21600, Deadline: timestamppb.New(d), HardEnd: timestamppb.New(h),
				TimedOut: true, HardEndReached: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &runState{wf: &orkstackv1.Orkflow{}, runTimeout: c.timeout}
			if c.code != "" {
				s.runError = &orkstackv1.Error{Code: c.code}
			}
			if got := s.snapshot().GetTimeout(); !proto.Equal(got, c.want) {
				t.Fatalf("Run.timeout = %v, want %v", got, c.want)
			}
		})
	}
}

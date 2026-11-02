package engine_test

import (
	"os"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The recorded runs behind the fairness-pin-creation and retry-max-interval
// gates (testdata/history/README.md), each pair from ONE plan:
//
//   - gated: by today's code, so the gate's marker is in the history, once,
//     and the command it decides carries the plan's value;
//   - legacy: with the gate off, which is the command sequence of every build
//     before it — no marker, and the value those builds used, whatever the
//     plan said.
//
// Temporal's replayer matches a scheduled activity by type and id alone
// (TestSDKPremise_TheReplayerComparesNoPriorityAndNoRetryPolicy), so these tests
// compare the decided value themselves: what the history recorded against
// what current code sends on replay.
const (
	fairnessPinCreationGated  = "testdata/history/run-fairness-pin-creation-v1.json"
	fairnessPinCreationLegacy = "testdata/history/run-fairness-pin-creation-legacy-v1.json"
	retryMaxIntervalGated     = "testdata/history/run-retry-max-interval-v1.json"
	retryMaxIntervalLegacy    = "testdata/history/run-retry-max-interval-legacy-v1.json"
	retryLongInitialGated     = "testdata/history/run-retry-long-initial-v1.json"
	retryLongInitialLegacy    = "testdata/history/run-retry-long-initial-legacy-v1.json"

	// retryMaxIntervalPlanned is the number every retry fixture's plan
	// carries.
	retryMaxIntervalPlanned = 60 * time.Second
	// retryLongInitial is the first wait of the long-initial fixtures'
	// retrying task, above the plan's number and the legacy 100 seconds.
	retryLongInitial = 120 * time.Second
)

func requireE1Fixture(t *testing.T, path, recorder string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the committed fixture %s is missing (%v); record it with "+
			"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ -run %s", path, err, recorder)
	}
}

// recordedScheduled is the attributes of every activity of activityType a
// history scheduled, in order.
func recordedScheduled(recorded *historypb.History, activityType string) []*historypb.ActivityTaskScheduledEventAttributes {
	var out []*historypb.ActivityTaskScheduledEventAttributes
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil &&
			attrs.GetActivityType().GetName() == activityType {
			out = append(out, attrs)
		}
	}
	return out
}

func recordedPriorityOf(attrs *historypb.ActivityTaskScheduledEventAttributes) temporal.Priority {
	p := attrs.GetPriority()
	return temporal.Priority{PriorityKey: int(p.GetPriorityKey()), FairnessKey: p.GetFairnessKey(), FairnessWeight: p.GetFairnessWeight()}
}

// replayCapturingDispatch replays recorded against current code, as the
// execution it was, with the run worker's own interceptors and, before
// them, the capture, which therefore sees what they set.
func replayCapturingDispatch(t *testing.T, recorded *historypb.History, runID string) (*dispatchCapture, error) {
	t.Helper()
	capture := &dispatchCapture{}
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		Interceptors: append([]interceptor.WorkerInterceptor{capture}, engine.RunWorkerInterceptors()...),
	})
	if err != nil {
		t.Fatalf("new replayer: %v", err)
	}
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	return capture, replayer.ReplayWorkflowHistoryWithOptions(nil, recorded, worker.ReplayWorkflowHistoryOptions{
		OriginalExecution: workflow.Execution{ID: runID, RunID: "replay"},
	})
}

// requireOneDecision fails unless replaying asked change exactly once and
// was answered want.
func requireOneDecision(t *testing.T, path string, capture *dispatchCapture, change string, want workflow.Version) {
	t.Helper()
	if asked := capture.versionsAsked(change); len(asked) != 1 || asked[0] != want {
		t.Fatalf("replaying %s was answered %v for %s, want exactly %d", path, asked, change, want)
	}
}

func wantCreationPriority(legacy bool) temporal.Priority {
	if legacy {
		return temporal.Priority{}
	}
	return temporal.Priority{FairnessKey: fairnessKeysPlannedKey, FairnessWeight: fairnessKeysPlannedWeight}
}

// requireRecordedCreationPriority: both pins' creation requests recorded the
// path's priority, and every task the plan's.
func requireRecordedCreationPriority(t *testing.T, path string, recorded *historypb.History, legacy bool) {
	t.Helper()
	creations := recordedScheduled(recorded, sessionCreationActivityName)
	if len(creations) != 2 {
		t.Fatalf("%s scheduled %d pin creation requests, want twoPinsDocument's two", path, len(creations))
	}
	for _, attrs := range creations {
		if got := recordedPriorityOf(attrs); got != wantCreationPriority(legacy) {
			t.Fatalf("%s recorded a pin creation request with %+v, want %+v", path, got, wantCreationPriority(legacy))
		}
	}
	requireRecordedFairness(t, path, recorded, false)
}

// TestRunWorkflow_ReplaysCommittedFairnessPinCreationHistories is the command
// half, for both: a history recorded before the gate replays its
// DefaultVersion branch and keys no creation request though its plan carries
// a selection, and one recorded with the gate replays its new path, the
// plan's key and weight on both pins' creation requests.
func TestRunWorkflow_ReplaysCommittedFairnessPinCreationHistories(t *testing.T) {
	for _, tc := range []struct {
		name   string
		path   string
		legacy bool
	}{{"gated", fairnessPinCreationGated, false}, {"legacy", fairnessPinCreationLegacy, true}} {
		t.Run(tc.name, func(t *testing.T) {
			requireE1Fixture(t, tc.path, "TestIntegration_RecordFairnessPinCreationHistories")
			recorded := loadHistoryFile(t, tc.path)
			runID, _, _, plan, _ := startArgs(t, recorded)
			if plan.Fairness == nil || plan.Fairness.Key != fairnessKeysPlannedKey || !plan.Fairness.PinRequests {
				t.Fatalf("%s was started with a plan carrying %+v: the fixture does not say what it was recorded to say",
					tc.path, plan.Fairness)
			}
			markers := versionMarkers(t, recorded)
			if tc.legacy != (markers[engine.FairnessPinCreationChange] == 0) || markers[engine.FairnessKeysChange] == 0 {
				t.Fatalf("%s records markers %v, want legacy=%v for %s and a %s marker", tc.path, markers, tc.legacy,
					engine.FairnessPinCreationChange, engine.FairnessKeysChange)
			}
			requireRecordedCreationPriority(t, tc.path, recorded, tc.legacy)
			capture, err := replayCapturingDispatch(t, recorded, runID)
			if err != nil {
				t.Fatalf("replaying %s against current code failed: %v", tc.path, err)
			}
			requireKeyed(t, "pin creation request on replay", capture.of(sessionCreationActivityName), 2, wantCreationPriority(tc.legacy))
			requireKeyed(t, "ExecuteTask on replay", capture.of(task.ActivityExecuteTask), 3, wantPriority(false))
			want := workflow.Version(1)
			if tc.legacy {
				want = workflow.DefaultVersion
			}
			requireOneDecision(t, tc.path, capture, engine.FairnessPinCreationChange, want)
		})
	}
}

// TestFairnessPinCreationHistories_CatchTheGateRemoved is the negative test:
// the gated fixture replayed with the gate skipped keys no creation request
// where it recorded one, which the comparison above refuses.
func TestFairnessPinCreationHistories_CatchTheGateRemoved(t *testing.T) {
	requireE1Fixture(t, fairnessPinCreationGated, "TestIntegration_RecordFairnessPinCreationHistories")
	recorded := loadHistoryFile(t, fairnessPinCreationGated)
	runID, _, _, _, _ := startArgs(t, recorded)
	defer engine.DisableFairnessPinCreationGateForTest()()
	capture, err := replayCapturingDispatch(t, recorded, runID)
	if err != nil {
		t.Fatalf("replay with the gate skipped: %v", err)
	}
	// Both requests are sent, and both unkeyed: the comparison above sees a
	// difference at each, rather than passing on nothing.
	requireKeyed(t, "pin creation request with the gate skipped", capture.of(sessionCreationActivityName), 2, temporal.Priority{})
	if got := len(capture.of(sessionCreationActivityName)); got != 2 {
		t.Fatalf("replay with the gate skipped sent %d pin creation requests, want the recorded 2", got)
	}
}

// TestFairnessPinCreationHistories_CatchTheInterceptorMissing: the gated
// fixture replayed by a worker without the run worker's interceptors sends
// the creation request unkeyed, which the comparison refuses — so a run
// worker built without them is caught, not silently unfair.
func TestFairnessPinCreationHistories_CatchTheInterceptorMissing(t *testing.T) {
	requireE1Fixture(t, fairnessPinCreationGated, "TestIntegration_RecordFairnessPinCreationHistories")
	recorded := loadHistoryFile(t, fairnessPinCreationGated)
	runID, _, _, _, _ := startArgs(t, recorded)
	capture := &dispatchCapture{}
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		Interceptors: []interceptor.WorkerInterceptor{capture},
	})
	if err != nil {
		t.Fatalf("new replayer: %v", err)
	}
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	if err := replayer.ReplayWorkflowHistoryWithOptions(nil, recorded, worker.ReplayWorkflowHistoryOptions{
		OriginalExecution: workflow.Execution{ID: runID, RunID: "replay"},
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	creations := capture.of(sessionCreationActivityName)
	if len(creations) != 2 {
		t.Fatalf("replay without the interceptors sent %d pin creation requests, want the recorded 2", len(creations))
	}
	requireKeyed(t, "pin creation request without the interceptors", creations, 2, temporal.Priority{})
}

func wantRetryMaxInterval(legacy bool) time.Duration {
	if legacy {
		return 100 * time.Second
	}
	return retryMaxIntervalPlanned
}

// retryingIntervals is the MaximumInterval of every ExecuteTask a history
// scheduled with more than one attempt.
func retryingIntervals(recorded *historypb.History) []time.Duration {
	var out []time.Duration
	for _, attrs := range recordedScheduled(recorded, task.ActivityExecuteTask) {
		if policy := attrs.GetRetryPolicy(); policy.GetMaximumAttempts() > 1 {
			out = append(out, policy.GetMaximumInterval().AsDuration())
		}
	}
	return out
}

// TestRunWorkflow_ReplaysCommittedRetryMaxIntervalHistories is the command
// half, for both: a history recorded before the gate replays its
// DefaultVersion branch and waits 100 seconds at most though its plan
// carries 60, and one recorded with the gate replays its new path, the
// plan's 60.
func TestRunWorkflow_ReplaysCommittedRetryMaxIntervalHistories(t *testing.T) {
	for _, tc := range []struct {
		name   string
		path   string
		legacy bool
	}{{"gated", retryMaxIntervalGated, false}, {"legacy", retryMaxIntervalLegacy, true}} {
		t.Run(tc.name, func(t *testing.T) {
			requireE1Fixture(t, tc.path, "TestIntegration_RecordRetryMaxIntervalHistories")
			recorded := loadHistoryFile(t, tc.path)
			runID, _, _, plan, _ := startArgs(t, recorded)
			if plan.RetryMaxIntervalSeconds != int32(retryMaxIntervalPlanned/time.Second) {
				t.Fatalf("%s was started with a plan carrying %d s: the fixture does not say what it was recorded to say",
					tc.path, plan.RetryMaxIntervalSeconds)
			}
			markers := versionMarkers(t, recorded)
			if tc.legacy != (markers[engine.RetryMaxIntervalChange] == 0) {
				t.Fatalf("%s records markers %v, want legacy=%v for %s", tc.path, markers, tc.legacy, engine.RetryMaxIntervalChange)
			}
			if got := retryingIntervals(recorded); len(got) != 1 || got[0] != wantRetryMaxInterval(tc.legacy) {
				t.Fatalf("%s recorded retrying tasks with maximum intervals %v, want one of %s", tc.path, got, wantRetryMaxInterval(tc.legacy))
			}
			capture, err := replayCapturingDispatch(t, recorded, runID)
			if err != nil {
				t.Fatalf("replaying %s against current code failed: %v", tc.path, err)
			}
			requireIntervals(t, maximumIntervals(capture), 1, wantRetryMaxInterval(tc.legacy))
			want := workflow.Version(1)
			if tc.legacy {
				want = workflow.DefaultVersion
			}
			requireOneDecision(t, tc.path, capture, engine.RetryMaxIntervalChange, want)
		})
	}
}

// TestRunWorkflow_ReplaysCommittedRetryLongInitialHistories is the command
// half of the raise: a retry: whose first wait is above both the plan's
// number and the legacy 100 seconds, with no maximum of its own, was recorded
// — by today's code and with the gate off — with its maximum raised to that
// first wait, and current code sends the same on replay. The cluster refuses
// anything lower, so no build before the raise recorded such a run: it never
// got past the task.
func TestRunWorkflow_ReplaysCommittedRetryLongInitialHistories(t *testing.T) {
	for _, tc := range []struct {
		name   string
		path   string
		legacy bool
	}{{"gated", retryLongInitialGated, false}, {"legacy", retryLongInitialLegacy, true}} {
		t.Run(tc.name, func(t *testing.T) {
			requireE1Fixture(t, tc.path, "TestIntegration_RecordRetryLongInitialHistories")
			recorded := loadHistoryFile(t, tc.path)
			runID, wf, _, plan, _ := startArgs(t, recorded)
			if plan.RetryMaxIntervalSeconds != int32(retryMaxIntervalPlanned/time.Second) {
				t.Fatalf("%s was started with a plan carrying %d s: the fixture does not say what it was recorded to say",
					tc.path, plan.RetryMaxIntervalSeconds)
			}
			retrying := 0
			for _, spec := range wf.GetTasks() {
				if retry := spec.GetRetry(); retry.GetMaximumAttempts() > 1 {
					retrying++
					if time.Duration(retry.GetInitialIntervalSeconds())*time.Second != retryLongInitial || retry.GetMaximumIntervalSeconds() != 0 {
						t.Fatalf("%s's task %s retries with %v, want a first wait of %s and no maximum", tc.path, spec.GetId(), retry, retryLongInitial)
					}
				}
			}
			if retrying != 1 {
				t.Fatalf("%s's definition has %d retrying tasks, want 1", tc.path, retrying)
			}
			markers := versionMarkers(t, recorded)
			if tc.legacy != (markers[engine.RetryMaxIntervalChange] == 0) {
				t.Fatalf("%s records markers %v, want legacy=%v for %s", tc.path, markers, tc.legacy, engine.RetryMaxIntervalChange)
			}
			if got := retryingIntervals(recorded); len(got) != 1 || got[0] != retryLongInitial {
				t.Fatalf("%s recorded retrying tasks with maximum intervals %v, want one of %s", tc.path, got, retryLongInitial)
			}
			capture, err := replayCapturingDispatch(t, recorded, runID)
			if err != nil {
				t.Fatalf("replaying %s against current code failed: %v", tc.path, err)
			}
			requireIntervals(t, maximumIntervals(capture), 1, retryLongInitial)
			want := workflow.Version(1)
			if tc.legacy {
				want = workflow.DefaultVersion
			}
			requireOneDecision(t, tc.path, capture, engine.RetryMaxIntervalChange, want)
		})
	}
}

// TestRetryMaxIntervalHistories_CatchTheGateRemoved is the negative test: the
// gated fixture replayed with the gate skipped is sent 100 seconds where it
// recorded 60.
func TestRetryMaxIntervalHistories_CatchTheGateRemoved(t *testing.T) {
	requireE1Fixture(t, retryMaxIntervalGated, "TestIntegration_RecordRetryMaxIntervalHistories")
	recorded := loadHistoryFile(t, retryMaxIntervalGated)
	runID, _, _, _, _ := startArgs(t, recorded)
	defer engine.DisableRetryMaxIntervalGateForTest()()
	capture, err := replayCapturingDispatch(t, recorded, runID)
	if err != nil {
		t.Fatalf("replay with the gate skipped: %v", err)
	}
	// The retrying task is sent, at 100 seconds: the comparison above sees
	// the difference, rather than passing on nothing.
	requireIntervals(t, maximumIntervals(capture), 1, 100*time.Second)
}

// TestSDKPremise_TheReplayerComparesNoPriorityAndNoRetryPolicy is a premise
// check on the Temporal SDK, not a test of OrkStack code: it holds the SDK to
// the behaviour the gates above rest on, so an SDK upgrade that starts
// comparing a command's priority or retry policy fails here, naming the
// premise, rather than in a replay test that looks unrelated.
//
// The premise: a recorded history whose pin creation requests and tasks
// carried no priority, and whose retries had their own policy, replays clean
// against code that sends every one of those commands with a fairness key and
// a different retry policy — the replayer matches a scheduled activity by its
// id and type only (go.temporal.io/sdk internal/internal_task_handlers.go,
// isCommandMatchEvent). So adding a key to a recorded command can never break
// a replay, and only a version gate keeps a recorded run on the values it was
// recorded with. The control changes one command's activity type, which the
// replayer refuses, so the replay is known to compare commands at all.
func TestSDKPremise_TheReplayerComparesNoPriorityAndNoRetryPolicy(t *testing.T) {
	recorded := loadHistoryFile(t, pinCreationTimeoutLegacy)
	runID, _, _, plan, _ := startArgs(t, recorded)
	if plan.Fairness != nil {
		t.Fatalf("%s's plan carries fairness %+v; the premise is a history recorded without", pinCreationTimeoutLegacy, plan.Fairness)
	}
	for _, attrs := range append(recordedScheduled(recorded, sessionCreationActivityName),
		recordedScheduled(recorded, task.ActivityExecuteTask)...) {
		if attrs.GetPriority() != nil {
			t.Fatalf("%s recorded an activity with priority %v; the premise is none", pinCreationTimeoutLegacy, attrs.GetPriority())
		}
	}
	replay := func(rewrite *commandRewriter) error {
		replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
			Interceptors: []interceptor.WorkerInterceptor{rewrite},
		})
		if err != nil {
			t.Fatalf("new replayer: %v", err)
		}
		replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
		return replayer.ReplayWorkflowHistoryWithOptions(nil, recorded, worker.ReplayWorkflowHistoryOptions{
			OriginalExecution: workflow.Execution{ID: runID, RunID: "replay"},
		})
	}
	keyed := &commandRewriter{priority: temporal.Priority{FairnessKey: "added", FairnessWeight: 5},
		retry: &temporal.RetryPolicy{MaximumAttempts: 9, InitialInterval: 7 * time.Second, MaximumInterval: 11 * time.Second}}
	if err := replay(keyed); err != nil {
		t.Fatalf("replay with a fairness key and a retry policy added to every recorded command failed: %v", err)
	}
	if keyed.rewritten < 3 {
		t.Fatalf("only %d commands were rewritten; want both creation requests and the tasks", keyed.rewritten)
	}
	if err := replay(&commandRewriter{renameTask: true}); err == nil {
		t.Fatal("a renamed activity type replayed clean: the control shows the replay compares nothing")
	}
}

// commandRewriter rewrites every ExecuteTask and pin creation request a
// workflow schedules: their priority and retry policy, or, for the control,
// an ExecuteTask's activity type.
type commandRewriter struct {
	interceptor.WorkerInterceptorBase
	priority   temporal.Priority
	retry      *temporal.RetryPolicy
	renameTask bool
	rewritten  int
}

func (r *commandRewriter) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &rewriterInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, root: r}
}

type rewriterInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root *commandRewriter
}

func (i *rewriterInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&rewriterOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, root: i.root})
}

type rewriterOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *commandRewriter
}

func (o *rewriterOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	if activityType != task.ActivityExecuteTask && activityType != sessionCreationActivityName {
		return o.Next.ExecuteActivity(ctx, activityType, args...)
	}
	if o.root.renameTask && activityType == task.ActivityExecuteTask {
		return o.Next.ExecuteActivity(ctx, "RenamedTask", args...)
	}
	o.root.rewritten++
	ctx = workflow.WithPriority(ctx, o.root.priority)
	ctx = workflow.WithRetryPolicy(ctx, *o.root.retry)
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

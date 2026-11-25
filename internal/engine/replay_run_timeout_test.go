package engine_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// runTimeoutFixture is one recorded run of a definition declaring a run
// timeout (TestIntegration_RecordRunTimeoutHistories): one command path of
// the run timeout each, recorded once and left to age
// (testdata/history/README.md).
type runTimeoutFixture struct {
	name        string
	timeout     int64 // the definition's timeout_seconds
	grace       int64 // the plan's handler grace; -1: the plan carries no RunTimeout
	tasks       []*orkstackv1.Task
	cancelAfter time.Duration // the operator cancels this long after the start
	wantCode    string        // the run's ending: "" succeeded, "CANCELED", or its error type
}

func (f runTimeoutFixture) path() string {
	return "testdata/history/run-timeout-" + f.name + "-v1.json"
}

func shellTask(id string, needs ...string) *orkstackv1.Task {
	return &orkstackv1.Task{Id: id, Uses: "shell", Needs: needs}
}

func anyFailedHandler(id string, needs ...string) *orkstackv1.Task {
	t := shellTask(id, needs...)
	t.Join = orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED
	return t
}

// runTimeoutFixtures: "slow…" runs until it is cancelled, "boom" fails at
// once, the rest succeed at once (runTimeoutOrker).
var runTimeoutFixtures = []runTimeoutFixture{
	// D passes mid-task: the task is stopped, its dependent never starts.
	{name: "fires-mid-task", timeout: 3, grace: 2,
		tasks: []*orkstackv1.Task{shellTask("slow"), shellTask("ship", "slow")}, wantCode: engine.RunTimedOutCode},
	// A failure handler outlives the grace: H stops it.
	{name: "hard-end", timeout: 3, grace: 2,
		tasks: []*orkstackv1.Task{shellTask("slow"), anyFailedHandler("slowhandler", "slow")}, wantCode: engine.RunTimedOutCode},
	// A run already failing at D keeps its own cause; its handler stops at H.
	{name: "already-failing", timeout: 3, grace: 2,
		tasks: []*orkstackv1.Task{shellTask("boom"), anyFailedHandler("slowhandler", "boom")}, wantCode: "TASK_FAILED"},
	// Done before D: the timer is abandoned.
	{name: "finished-before-deadline", timeout: 600, grace: 2,
		tasks: []*orkstackv1.Task{shellTask("a"), shellTask("b", "a")}},
	// The operator's cancel settles the timer and wins.
	{name: "operator-cancel", timeout: 600, grace: 2,
		tasks: []*orkstackv1.Task{shellTask("slow")}, cancelAfter: 2 * time.Second, wantCode: "CANCELED"},
	// A definition declaring a timeout whose plan carries none.
	{name: "definition-unsupported", timeout: 600, grace: -1,
		tasks: []*orkstackv1.Task{shellTask("a")}, wantCode: engine.RunDefinitionUnsupportedCode},
}

// requireRunTimeoutFixture fails when f is missing: the fixtures are
// committed, so their replay tests never skip.
func requireRunTimeoutFixture(t *testing.T, f runTimeoutFixture) {
	t.Helper()
	if _, err := os.Stat(f.path()); err != nil {
		t.Fatalf("the run-timeout fixture %s is missing (%v); record it with ORK_RECORD_GOLDEN_HISTORY=1 "+
			"go test -tags=integration ./internal/engine/ -run TestIntegration_RecordRunTimeoutHistories", f.path(), err)
	}
}

func replayRunTimeoutHistory(t *testing.T, recorded *historypb.History, runID string) error {
	t.Helper()
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	return replayer.ReplayWorkflowHistoryWithOptions(nil, recorded, worker.ReplayWorkflowHistoryOptions{
		OriginalExecution: replayExecution(runID, recorded),
	})
}

// closingEvent is how the recorded execution closed, and the error type it
// failed with ("" when it did not fail).
func closingEvent(recorded *historypb.History) (enumspb.EventType, string) {
	events := recorded.GetEvents()
	last := events[len(events)-1]
	if attrs := last.GetWorkflowExecutionFailedEventAttributes(); attrs != nil {
		return last.GetEventType(), attrs.GetFailure().GetApplicationFailureInfo().GetType()
	}
	return last.GetEventType(), ""
}

// TestRunWorkflow_ReplaysCommittedRunTimeoutHistories is the command half of
// every run-timeout fixture: each replays against current code, its start
// declares the timeout and plan it was recorded to declare, and it closed as
// its fixture says.
func TestRunWorkflow_ReplaysCommittedRunTimeoutHistories(t *testing.T) {
	for _, f := range runTimeoutFixtures {
		t.Run(f.name, func(t *testing.T) {
			requireRunTimeoutFixture(t, f)
			recorded := loadHistoryFile(t, f.path())
			runID, wf, _, plan, _ := startArgs(t, recorded)
			if wf.GetTimeoutSeconds() != f.timeout {
				t.Fatalf("%s started a definition with timeout_seconds %d, want %d", f.path(), wf.GetTimeoutSeconds(), f.timeout)
			}
			if (plan.RunTimeout != nil) != (f.grace >= 0) {
				t.Fatalf("%s started with plan RunTimeout %+v; want one: %v", f.path(), plan.RunTimeout, f.grace >= 0)
			}
			closed, failureType := closingEvent(recorded)
			switch f.wantCode {
			case "":
				if closed != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
					t.Fatalf("%s closed %s, want completed", f.path(), closed)
				}
			case "CANCELED":
				if closed != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED {
					t.Fatalf("%s closed %s, want canceled", f.path(), closed)
				}
			default:
				if failureType != f.wantCode {
					t.Fatalf("%s closed %s %q, want %s", f.path(), closed, failureType, f.wantCode)
				}
			}
			if err := replayRunTimeoutHistory(t, recorded, runID); err != nil {
				t.Fatalf("replaying %s against current code failed: %v\n\n"+
					"A run with a run timeout in flight when this build was deployed would fail its next "+
					"workflow task. Restore the run timeout's command stream, or re-record and say why.", f.path(), err)
			}
		})
	}
}

// TestRunTimeoutHistories_CatchAMovedTimer is the negative test: each fixture
// that started the deadline's timer is replayed with that timer's id
// rewritten, and the replayer must refuse it, so the fixture is known to pin
// the timer's place in the command stream.
func TestRunTimeoutHistories_CatchAMovedTimer(t *testing.T) {
	for _, f := range runTimeoutFixtures {
		if f.grace < 0 {
			continue // the run ends before it arms a deadline timer
		}
		t.Run(f.name, func(t *testing.T) {
			requireRunTimeoutFixture(t, f)
			recorded := loadHistoryFile(t, f.path())
			runID, _, _, _, _ := startArgs(t, recorded)
			moved := false
			for _, event := range recorded.GetEvents() {
				if attrs := event.GetTimerStartedEventAttributes(); attrs != nil {
					attrs.TimerId = "moved-" + attrs.GetTimerId()
					moved = true
					break
				}
			}
			if !moved {
				t.Fatalf("%s started no timer: it does not record the run timeout's deadline timer", f.path())
			}
			if err := replayRunTimeoutHistory(t, recorded, runID); err == nil {
				t.Fatalf("%s replayed with its deadline timer moved: it does not pin the timer", f.path())
			}
		})
	}
}

// The run-timeout fixtures whose commands depend on the continuation's carry
// and hold (TestIntegration_RecordRunTimeoutContinuedHistories).
const (
	runTimeoutCarriedG0 = "testdata/history/run-timeout-continued-carried-g0-v1.json"
	runTimeoutCarriedG1 = "testdata/history/run-timeout-continued-carried-g1-v1.json"
	runTimeoutPastG0    = "testdata/history/run-timeout-continued-past-deadline-g0-v1.json"
	runTimeoutPastG1    = "testdata/history/run-timeout-continued-past-deadline-g1-v1.json"
	runTimeoutHold      = "testdata/history/run-timeout-continued-hold-v1.json"
)

// runTimeoutContinuedFixtures maps each generation to the generation 0 whose
// start names the run id it replays as.
var runTimeoutContinuedFixtures = []struct{ path, first string }{
	{runTimeoutCarriedG0, runTimeoutCarriedG0},
	{runTimeoutCarriedG1, runTimeoutCarriedG0},
	{runTimeoutPastG0, runTimeoutPastG0},
	{runTimeoutPastG1, runTimeoutPastG0},
	{runTimeoutHold, runTimeoutHold},
}

func requireCommittedFixture(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the run-timeout fixture %s is missing (%v); record it with ORK_RECORD_GOLDEN_HISTORY=1 "+
			"go test -tags=integration ./internal/engine/ -run TestIntegration_RecordRunTimeoutContinuedHistories", path, err)
	}
}

// TestRunWorkflow_ReplaysCommittedContinuedRunTimeoutHistories is the command
// half: each generation replays as the execution of its run's id, and each
// declares the run timeout it was recorded under.
func TestRunWorkflow_ReplaysCommittedContinuedRunTimeoutHistories(t *testing.T) {
	for _, f := range runTimeoutContinuedFixtures {
		t.Run(filepath.Base(f.path), func(t *testing.T) {
			requireCommittedFixture(t, f.path)
			requireCommittedFixture(t, f.first)
			runID, _, _, _, _ := startArgs(t, loadHistoryFile(t, f.first))
			recorded := loadHistoryFile(t, f.path)
			_, wf, _, plan, _ := startArgs(t, recorded)
			if wf.GetTimeoutSeconds() == 0 || plan.RunTimeout == nil || plan.Continuation == nil {
				t.Fatalf("%s started without its run timeout or continuation budget", f.path)
			}
			if err := replayRunTimeoutHistory(t, recorded, runID); err != nil {
				t.Fatalf("replaying %s against current code failed: %v", f.path, err)
			}
		})
	}
}

// TestContinuedRunTimeoutHistories_CatchTheTimeoutRemoved is the negative
// test: each generation, its start's definition rewritten to declare no run
// timeout, must be refused by the replayer, so each fixture is known to pin
// the run timeout's commands — the carried deadline's timer, the first pass's
// timeout with no timer, the timeout taken while held.
func TestContinuedRunTimeoutHistories_CatchTheTimeoutRemoved(t *testing.T) {
	for _, f := range runTimeoutContinuedFixtures {
		t.Run(filepath.Base(f.path), func(t *testing.T) {
			requireCommittedFixture(t, f.path)
			requireCommittedFixture(t, f.first)
			runID, _, _, _, _ := startArgs(t, loadHistoryFile(t, f.first))
			recorded := loadHistoryFile(t, f.path)
			attrs := recorded.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
			id, wf, input, plan, carry := startArgs(t, recorded)
			// The positive control: the start re-encoded unchanged replays,
			// so a refusal below is the removed timeout's, not the encoding's.
			unchanged, err := converter.GetDefaultDataConverter().ToPayloads(id, wf, input, plan, carry)
			must.OKf(t, err, "re-encode the start")
			attrs.Input = unchanged
			must.OKf(t, replayRunTimeoutHistory(t, recorded, runID), "%s re-encoded unchanged does not replay", f.path)
			wf.TimeoutSeconds = 0
			plan.RunTimeout = nil
			payloads, err := converter.GetDefaultDataConverter().ToPayloads(id, wf, input, plan, carry)
			must.OKf(t, err, "re-encode the start")
			attrs.Input = payloads
			if err := replayRunTimeoutHistory(t, recorded, runID); err == nil {
				t.Fatalf("%s replayed with its run timeout removed: it does not pin the run timeout's commands", f.path)
			}
		})
	}
}

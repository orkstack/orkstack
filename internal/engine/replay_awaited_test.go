package engine_test

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The two awaited-run fixtures: real histories recorded by a versioned worker
// against a real cluster by TestIntegration_RecordAwaitedRunHistories,
// never hand-edited (testdata/history/README.md). They hold the await path —
// the first Timer and the first Signal RunWorkflow ever issued — still, the
// way run-succeeded-v1.json holds the container path.
const (
	awaitedResolvedHistory = "testdata/history/run-awaited-resolved-v1.json"
	awaitedTimeoutHistory  = "testdata/history/run-awaited-timeout-v1.json"
)

// requireAwaitedFixture skips while a fixture has not been recorded yet. The
// fixtures can only be produced against a real cluster; once one is
// committed this never skips again, and the replay below is a hard gate.
func requireAwaitedFixture(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		if os.Getenv("ORK_REQUIRE_AWAITED_FIXTURES") != "" {
			t.Fatalf("the awaited replay fixture %s is missing (%v); record it with "+
				"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
				"-run TestIntegration_RecordAwaitedRunHistories", path, err)
		}
		t.Skipf("%s is not recorded yet (%v): record it with ORK_RECORD_GOLDEN_HISTORY=1 "+
			"go test -tags=integration ./internal/engine/ -run TestIntegration_RecordAwaitedRunHistories", path, err)
	}
}

func TestRunWorkflow_ReplaysCommittedAwaitedHistories(t *testing.T) {
	for _, path := range []string{awaitedResolvedHistory, awaitedTimeoutHistory} {
		t.Run(path, func(t *testing.T) {
			requireAwaitedFixture(t, path)
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{
				Name: engine.RunWorkflowType,
			})
			if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
				t.Fatalf("replaying %s against current code failed: %v\n\n"+
					"A run that was awaiting when this build was deployed would fail its next "+
					"workflow task. Restore the await command stream, or re-record and say why.", path, err)
			}
		})
	}
}

// awaitClock matches the RFC 3339 instants an await writes into its messages
// (its deadline). A re-execution can never reproduce the recorded wall clock.
var awaitClock = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z`)

// withoutAwaitClocks is withoutTimestamps plus the await's own clocks: the
// TaskAwait.deadline field and every RFC 3339 instant inside a string.
func withoutAwaitClocks(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, inner := range typed {
			if key == "deadline" {
				continue
			}
			out[key] = withoutAwaitClocks(inner)
		}
		return withoutTimestamps(out)
	case []any:
		out := make([]any, 0, len(typed))
		for _, inner := range typed {
			out = append(out, withoutAwaitClocks(inner))
		}
		return out
	case string:
		return awaitClock.ReplaceAllString(typed, "<instant>")
	default:
		return value
	}
}

func normalizedMaps(in []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(in))
	for _, m := range in {
		out = append(out, withoutAwaitClocks(m).(map[string]any))
	}
	return out
}

func loadHistoryFile(t *testing.T, path string) *historypb.History {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	recorded := &historypb.History{}
	if err := (temporalproto.CustomJSONUnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, recorded); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return recorded
}

// recordedResolutions is every resolution signal in a fixture, with how long
// after the run's start it was recorded.
func recordedResolutions(t *testing.T, recorded *historypb.History) ([]*orkstackv1.AwaitSignal, []time.Duration) {
	t.Helper()
	var (
		started time.Time
		signals []*orkstackv1.AwaitSignal
		offsets []time.Duration
	)
	for _, event := range recorded.GetEvents() {
		if event.GetWorkflowExecutionStartedEventAttributes() != nil {
			started = event.GetEventTime().AsTime()
		}
		attrs := event.GetWorkflowExecutionSignaledEventAttributes()
		if attrs == nil || attrs.GetSignalName() != engine.ResolveAwaitSignal {
			continue
		}
		var sig *orkstackv1.AwaitSignal
		if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &sig); err != nil {
			t.Fatalf("decode a recorded resolution: %v", err)
		}
		signals = append(signals, sig)
		offset := event.GetEventTime().AsTime().Sub(started)
		if offset <= 0 {
			offset = time.Millisecond
		}
		offsets = append(offsets, offset)
	}
	return signals, offsets
}

// TestAwaitedHistory_ActivityPayloadsMatchWhatCurrentCodeSends is the payload
// half for the await path: re-execute the resolved fixture from its own input,
// feed back its recorded activity results and its recorded resolution, and
// compare every payload current code sends with what the recorded build sent.
func TestAwaitedHistory_ActivityPayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	requireAwaitedFixture(t, awaitedResolvedHistory)
	recorded := loadHistoryFile(t, awaitedResolvedHistory)
	runID, wf, runInput, plan := goldenWorkflowArgs(t, recorded)
	recordedTaskInputs, recordedEvents, recordedResults, recordedRun := goldenActivityPayloads(t, recorded)
	signals, offsets := recordedResolutions(t, recorded)
	if len(signals) == 0 {
		t.Fatal("the resolved fixture carries no resolution")
	}

	var (
		mu        sync.Mutex
		sentTasks []*orkstackv1.ExecuteTaskInput
		sentEvent []*orkstackv1.Event
	)
	suite := &testsuite.WorkflowTestSuite{}
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	env.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			mu.Lock()
			defer mu.Unlock()
			sentTasks = append(sentTasks, in)
			result, ok := recordedResults[in.TaskId]
			if !ok {
				return nil, fmt.Errorf("current code ran task %q, which the recorded run never executed", in.TaskId)
			}
			return result, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
			return fmt.Sprintf("memory://snapshots/%s/%d.pb", run.GetId(), sequence), nil
		},
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, event *orkstackv1.Event, _ string, _ string) error {
			mu.Lock()
			defer mu.Unlock()
			sentEvent = append(sentEvent, event)
			return nil
		},
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	for i := range signals {
		sig := signals[i]
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(engine.ResolveAwaitSignal, sig)
		}, offsets[i])
	}
	env.ExecuteWorkflow(engine.RunWorkflow, runID, wf, runInput, plan, (*orkstackv1.RunContinuation)(nil))
	if !env.IsWorkflowCompleted() {
		t.Fatal("re-executing the resolved fixture did not complete the workflow")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("re-executing the resolved fixture failed: %v", err)
	}

	sentTaskInputs := map[string]map[string]any{}
	for _, in := range sentTasks {
		sentTaskInputs[in.TaskId] = withoutAwaitClocks(canonicalPayload(t, in)).(map[string]any)
	}
	normalizedRecordedInputs := map[string]map[string]any{}
	for id, in := range recordedTaskInputs {
		normalizedRecordedInputs[id] = withoutAwaitClocks(in).(map[string]any)
	}
	compareTaskInputs(t, normalizedRecordedInputs, sentTaskInputs)

	sent := make([]map[string]any, 0, len(sentEvent))
	for _, event := range sentEvent {
		sent = append(sent, canonicalPayload(t, event))
	}
	compareEventPayloads(t, normalizedMaps(recordedEvents), normalizedMaps(sent))

	var finished *orkstackv1.Run
	if err := env.GetWorkflowResult(&finished); err != nil {
		t.Fatalf("decode the re-executed run: %v", err)
	}
	want := withoutAwaitClocks(compactRecordedRun(t, recordedRun)).(map[string]any)
	got := withoutAwaitClocks(canonicalPayload(t, finished)).(map[string]any)
	if diff := payloadDiff(want, got); diff != "" {
		t.Errorf("the Run this code returns for the resolved fixture differs from the recorded one:\n%s", diff)
	}
}

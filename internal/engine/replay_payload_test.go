package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends is the half of
// the cross-version guard that WorkflowReplayer cannot give.
//
// The replayer compares COMMANDS: an activity's type and its position in the
// stream. It never looks inside a payload. Measured against this very fixture:
// reordering the emit calls or deleting one fails ReplayWorkflowHistory
// (TMPRL1100), while adding a field to task.ExecuteTaskInput, or emitting
// EVENT_TYPE_TASK_LOG where the recorded run emitted EVENT_TYPE_TASK_STARTED,
// replays perfectly clean — the command stream is identical either way. Those
// two are precisely the changes that break an Orker of another build (it
// decodes an input it was never compiled for) and a reader of another build
// (it is handed an event type the recorded stream never carried).
//
// So this test re-executes RunWorkflow from the fixture's OWN workflow input,
// feeding back the fixture's own activity results, and compares what today's
// code puts INTO each activity call against what the recorded build put there.
//
// It compares as a multiset, not as a sequence: the fixture's orkflow fans out
// (transform and sidecar are siblings), so which of two ready activities
// completes first is a property of the machine that ran it, not of the code.
// Ordering is what the replayer already holds; content is what this holds.
func TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	recorded := loadGoldenHistory(t)
	runID, wf, runInput := goldenWorkflowArgs(t, recorded)
	recordedTaskInputs, recordedEvents, recordedResults, recordedRun := goldenActivityPayloads(t, recorded)

	var (
		mu        sync.Mutex
		sentTasks []task.ExecuteTaskInput
		sentEvent []*orkstackv1.Event
		replayErr []error
	)

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.WorkflowName})
	env.RegisterActivityWithOptions(
		func(_ context.Context, in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
			mu.Lock()
			defer mu.Unlock()
			sentTasks = append(sentTasks, in)
			result, ok := recordedResults[in.TaskID]
			if !ok {
				err := fmt.Errorf("current code ran task %q, which the recorded run never executed", in.TaskID)
				replayErr = append(replayErr, err)
				return task.ExecuteTaskResult{}, err
			}
			// Feed the recorded result back, so every downstream input is
			// built from exactly the upstream data the recorded run had.
			return result, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, event *orkstackv1.Event, _ *orkstackv1.Run, _ string) error {
			mu.Lock()
			defer mu.Unlock()
			sentEvent = append(sentEvent, event)
			return nil
		},
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	env.ExecuteWorkflow(engine.RunWorkflow, runID, wf, runInput)

	if !env.IsWorkflowCompleted() {
		t.Fatal("re-executing the fixture's own input did not complete the workflow")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("re-executing the fixture's own input failed: %v", err)
	}
	for _, err := range replayErr {
		t.Error(err)
	}

	// --- what the engine sends an Orker ------------------------------------
	sentTaskInputs := map[string]map[string]any{}
	for _, in := range sentTasks {
		sentTaskInputs[in.TaskID] = canonicalPayload(t, in)
	}
	compareTaskInputs(t, recordedTaskInputs, sentTaskInputs)

	// --- what the engine sends a reader ------------------------------------
	sent := make([]map[string]any, 0, len(sentEvent))
	for _, event := range sentEvent {
		sent = append(sent, canonicalPayload(t, event))
	}
	compareEventPayloads(t, recordedEvents, sent)

	// --- what the run itself returns ---------------------------------------
	var finished *orkstackv1.Run
	if err := env.GetWorkflowResult(&finished); err != nil {
		t.Fatalf("decode the re-executed run: %v", err)
	}
	if diff := payloadDiff(recordedRun, canonicalPayload(t, finished)); diff != "" {
		t.Errorf("the Run this code returns no longer matches the one the recorded build "+
			"returned for the same input:\n%s\n\n"+
			"A replica of another build reads that projection. Either restore the shape, or "+
			"re-record the fixture and say so in the commit.", diff)
	}
}

// compareTaskInputs holds the engine->Orker activity contract to the fixture,
// field by field, per task.
func compareTaskInputs(t *testing.T, recorded, sent map[string]map[string]any) {
	t.Helper()
	for _, taskID := range sortedPayloadKeys(recorded) {
		got, ok := sent[taskID]
		if !ok {
			t.Errorf("the recorded run executed task %q and this code does not", taskID)
			continue
		}
		if diff := payloadDiff(recorded[taskID], got); diff != "" {
			t.Errorf("the ExecuteTask input for task %q is not the one the recorded build sent:\n%s\n\n"+
				"That is a cross-version break the replayer cannot see: it compares commands, never "+
				"payloads. An Orker of the recorded build decodes this input, and a field it does not "+
				"know is a field it silently drops. Either restore task.ExecuteTaskInput, or re-record "+
				"the fixture and say so in the commit.", taskID, diff)
		}
	}
	for _, taskID := range sortedPayloadKeys(sent) {
		if _, ok := recorded[taskID]; !ok {
			t.Errorf("this code executes task %q, which the recorded run never scheduled", taskID)
		}
	}
}

// compareEventPayloads holds the lifecycle stream to the fixture as a
// multiset, and the numbering to its own contract.
//
// Sequence and timestamp are compared separately from the rest: both depend on
// the order two sibling activities happened to complete in, which is not a
// property of the code. What must hold is that the numbering is still 1..N
// with no gap and no repeat — the property a replica rebuilding a stream from
// history depends on.
func compareEventPayloads(t *testing.T, recorded, sent []map[string]any) {
	t.Helper()

	seen := map[int]bool{}
	for _, event := range sent {
		sequence, ok := event["sequence"].(string)
		if !ok {
			t.Errorf("an emitted event carries no sequence: %v", event)
			continue
		}
		var number int
		if _, err := fmt.Sscanf(sequence, "%d", &number); err != nil {
			t.Errorf("event sequence %q is not a number: %v", sequence, err)
			continue
		}
		if seen[number] {
			t.Errorf("sequence %d was emitted twice; a replica cannot order a stream with repeats", number)
		}
		seen[number] = true
	}
	for number := 1; number <= len(sent); number++ {
		if !seen[number] {
			t.Errorf("the emitted stream has no event %d of %d: a replica rebuilding it from history "+
				"would report a gap", number, len(sent))
		}
	}

	recordedCounts := countPayloads(recorded, "sequence", "timestamp")
	sentCounts := countPayloads(sent, "sequence", "timestamp")
	for _, key := range sortedCountKeys(recordedCounts) {
		if sentCounts[key] != recordedCounts[key] {
			t.Errorf("the recorded run published this event %d time(s) and this code publishes it %d:\n  %s\n\n"+
				"The replayer cannot see this: renaming an event type or rewording a message leaves the "+
				"command stream identical. Readers of another build see the difference. Either restore "+
				"the event, or re-record the fixture and say so in the commit.",
				recordedCounts[key], sentCounts[key], key)
		}
	}
	for _, key := range sortedCountKeys(sentCounts) {
		if _, ok := recordedCounts[key]; !ok {
			t.Errorf("this code publishes an event the recorded run never did (%d time(s)):\n  %s",
				sentCounts[key], key)
		}
	}
}

// loadGoldenHistory decodes the committed fixture.
func loadGoldenHistory(t *testing.T) *historypb.History {
	t.Helper()
	raw, err := os.ReadFile(goldenHistory)
	if err != nil {
		t.Fatalf("read %s: %v", goldenHistory, err)
	}
	recorded := &historypb.History{}
	if err := (temporalproto.CustomJSONUnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, recorded); err != nil {
		t.Fatalf("decode %s: %v", goldenHistory, err)
	}
	return recorded
}

// goldenWorkflowArgs reads the run's own input back out of the fixture, so the
// re-execution is the recorded run and not a test-authored imitation of it.
func goldenWorkflowArgs(t *testing.T, recorded *historypb.History) (string, *orkstackv1.Orkflow, *orkstackv1.Payload) {
	t.Helper()
	for _, event := range recorded.GetEvents() {
		attrs := event.GetWorkflowExecutionStartedEventAttributes()
		if attrs == nil {
			continue
		}
		var (
			runID string
			wf    *orkstackv1.Orkflow
			input *orkstackv1.Payload
		)
		if err := converter.GetDefaultDataConverter().
			FromPayloads(attrs.GetInput(), &runID, &wf, &input); err != nil {
			t.Fatalf("decode the recorded run's workflow input: %v", err)
		}
		return runID, wf, input
	}
	t.Fatal("the fixture has no WorkflowExecutionStarted event")
	return "", nil, nil
}

// goldenActivityPayloads reads, from the fixture: the ExecuteTask input the
// recorded build sent per task, every Event it published, the result each task
// returned, and the Run the execution completed with.
func goldenActivityPayloads(t *testing.T, recorded *historypb.History) (
	taskInputs map[string]map[string]any,
	events []map[string]any,
	results map[string]task.ExecuteTaskResult,
	run map[string]any,
) {
	t.Helper()
	taskInputs = map[string]map[string]any{}
	results = map[string]task.ExecuteTaskResult{}
	activityByScheduledID := map[int64]string{}
	taskByScheduledID := map[int64]string{}

	for _, event := range recorded.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			name := attrs.GetActivityType().GetName()
			activityByScheduledID[event.GetEventId()] = name
			switch name {
			case task.ActivityExecuteTask:
				decoded := recordedPayload(t, attrs.GetInput(), 0)
				taskID, _ := decoded["task_id"].(string)
				if taskID == "" {
					t.Fatalf("a recorded ExecuteTask input has no task_id: %v", decoded)
				}
				taskByScheduledID[event.GetEventId()] = taskID
				taskInputs[taskID] = decoded
			case engine.ActivityPublishEvent:
				events = append(events, recordedPayload(t, attrs.GetInput(), 0))
			}
			continue
		}
		if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil {
			if activityByScheduledID[attrs.GetScheduledEventId()] != task.ActivityExecuteTask {
				continue
			}
			var result task.ExecuteTaskResult
			if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetResult(), &result); err != nil {
				t.Fatalf("decode a recorded ExecuteTask result: %v", err)
			}
			results[taskByScheduledID[attrs.GetScheduledEventId()]] = result
			continue
		}
		if attrs := event.GetWorkflowExecutionCompletedEventAttributes(); attrs != nil {
			run = recordedPayload(t, attrs.GetResult(), 0)
		}
	}

	if len(taskInputs) == 0 || len(events) == 0 || run == nil {
		t.Fatalf("the fixture carries %d task inputs, %d events and run=%v; it cannot hold anything to a contract",
			len(taskInputs), len(events), run != nil)
	}
	return taskInputs, events, results, run
}

// recordedPayload reads one recorded payload as the JSON the recorded build
// actually put on the wire — deliberately NOT decoded into a current Go type,
// which is what would hide a renamed or deleted field.
func recordedPayload(t *testing.T, payloads *commonpb.Payloads, index int) map[string]any {
	t.Helper()
	if payloads == nil || len(payloads.GetPayloads()) <= index {
		t.Fatalf("a recorded activity has no payload %d", index)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payloads.GetPayloads()[index].GetData(), &decoded); err != nil {
		t.Fatalf("decode recorded payload %d as JSON: %v", index, err)
	}
	return decoded
}

// canonicalPayload renders what current code would put on the wire, through
// the same data converter Temporal uses, and reads it back as plain JSON.
func canonicalPayload(t *testing.T, value any) map[string]any {
	t.Helper()
	payload, err := converter.GetDefaultDataConverter().ToPayload(value)
	if err != nil {
		t.Fatalf("encode %T through the Temporal data converter: %v", value, err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload.GetData(), &decoded); err != nil {
		t.Fatalf("decode the encoded %T as JSON: %v", value, err)
	}
	return decoded
}

// payloadDiff renders the difference between two decoded payloads, ignoring
// the fields whose value is a wall clock.
func payloadDiff(recorded, sent map[string]any) string {
	want := payloadJSON(withoutTimestamps(recorded))
	got := payloadJSON(withoutTimestamps(sent))
	if want == got {
		return ""
	}
	return "  recorded: " + want + "\n  current:  " + got
}

// withoutTimestamps strips every wall-clock field, at any depth. A re-executed
// run is not the recorded run's clock, and never can be.
func withoutTimestamps(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, inner := range typed {
			switch key {
			case "timestamp", "createdAt", "startedAt", "finishedAt":
				continue
			}
			out[key] = withoutTimestamps(inner)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, inner := range typed {
			out = append(out, withoutTimestamps(inner))
		}
		return out
	default:
		return value
	}
}

func payloadJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("<unencodable: %v>", err)
	}
	return string(encoded)
}

// countPayloads renders each payload as canonical JSON, minus the named
// fields, and counts how often it occurs.
func countPayloads(payloads []map[string]any, ignore ...string) map[string]int {
	counts := map[string]int{}
	for _, payload := range payloads {
		stripped := map[string]any{}
		for key, value := range payload {
			if slicesContains(ignore, key) {
				continue
			}
			stripped[key] = value
		}
		counts[payloadJSON(withoutTimestamps(stripped))]++
	}
	return counts
}

func slicesContains(haystack []string, needle string) bool {
	for _, candidate := range haystack {
		if candidate == needle {
			return true
		}
	}
	return false
}

func sortedPayloadKeys(m map[string]map[string]any) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func sortedCountKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

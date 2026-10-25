package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// historyLimitHistory is the history guard's fixture: a run that stopped
// RUN_HISTORY_LIMIT on a real cluster, recorded by a versioned worker
// (TestIntegration_RecordHistoryLimitRunHistory, testdata/history/README.md).
const historyLimitHistory = "testdata/history/run-history-limit-v1.json"

func requireHistoryLimitFixture(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(historyLimitHistory); err != nil {
		if os.Getenv("ORK_REQUIRE_HISTORY_LIMIT_FIXTURE") != "" {
			t.Fatalf("the history-limit replay fixture %s is missing (%v); record it with "+
				"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
				"-run TestIntegration_RecordHistoryLimitRunHistory", historyLimitHistory, err)
		}
		t.Skipf("%s is not recorded (%v): record it with ORK_RECORD_GOLDEN_HISTORY=1 go test "+
			"-tags=integration ./internal/engine/ -run TestIntegration_RecordHistoryLimitRunHistory",
			historyLimitHistory, err)
	}
}

// TestRunWorkflow_ReplaysCommittedHistoryLimitHistory is the command half: the
// replayer sets the history length and size from every recorded
// WorkflowTaskStarted — the path live execution takes — so today's body
// reproduces the exact crossing, the lazy run-history-guard marker and the
// terminal command sequence. It is the exact check of the crossing point.
func TestRunWorkflow_ReplaysCommittedHistoryLimitHistory(t *testing.T) {
	requireHistoryLimitFixture(t)
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, historyLimitHistory); err != nil {
		t.Fatalf("replaying the committed history-limit history against current code failed: %v", err)
	}
}

// guardMarkerEventID is the event id of the recorded run-history-guard
// version marker, and the id of the WorkflowTaskStarted of the workflow task
// that recorded it.
func guardMarkerEventID(t *testing.T, recorded *historypb.History) (marker, taskStarted int64) {
	t.Helper()
	var lastStarted int64
	for _, event := range recorded.GetEvents() {
		if event.GetWorkflowTaskStartedEventAttributes() != nil {
			lastStarted = event.GetEventId()
		}
		attrs := event.GetMarkerRecordedEventAttributes()
		if attrs == nil || attrs.GetMarkerName() != "Version" {
			continue
		}
		var change string
		if payload := attrs.GetDetails()["change-id"]; payload != nil {
			_ = converter.GetDefaultDataConverter().FromPayloads(payload, &change)
		}
		if change == engine.HistoryGuardChange {
			return event.GetEventId(), lastStarted
		}
	}
	t.Fatal("the fixture records no run-history-guard marker")
	return 0, 0
}

// historyLimitSentence parses the guard's events form.
var historyLimitSentence = regexp.MustCompile(`^run (\S+) stopped before reaching the cluster's history limit: ` +
	`([\d,]+) of ([\d,]+) events recorded, ([\d,]+) kept back so it could end cleanly`)

func parseGrouped(t *testing.T, s string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(strings.ReplaceAll(s, ",", ""), 10, 64)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return n
}

// TestHistoryLimitHistory_PayloadsMatchWhatCurrentCodeSends is the payload
// half. A test environment holds one static history length, so it does not try
// to reproduce the crossing (the command half does). It checks (i) the
// prefix — re-executed with the guard switched off, every ExecuteTask input
// and every event published before the recorded marker equals the recorded
// one — and (ii) the terminal event: RUN_FAILED owned by no task, in the
// guard's sentence, its counts at or above the workflow task that recorded
// the marker and below the limit, and the workflow failure typed
// RUN_HISTORY_LIMIT. The exact text is TestHistoryLimitMessage_Format's.
func TestHistoryLimitHistory_PayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	requireHistoryLimitFixture(t)
	recorded := loadHistoryFile(t, historyLimitHistory)
	runID, wf, runInput, plan := goldenWorkflowArgs(t, recorded)
	if plan.History == nil {
		t.Fatal("the fixture's plan carries no history limit; it cannot be the guard's fixture")
	}
	marker, markerTaskStarted := guardMarkerEventID(t, recorded)
	inputs, results := recordedItemPayloads(t, recorded)

	// Every event published before the marker, by sequence; and the terminal one.
	before := map[string]map[string]any{}
	var terminal map[string]any
	var failureType string
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil &&
			attrs.GetActivityType().GetName() == engine.ActivityPublishEvent {
			published := recordedPayload(t, attrs.GetInput(), 0)
			if event.GetEventId() < marker {
				before[fmt.Sprint(published["sequence"])] = published
			}
			if published["type"] == orkstackv1.EventType_EVENT_TYPE_RUN_FAILED.String() {
				terminal = published
			}
		}
		if attrs := event.GetWorkflowExecutionFailedEventAttributes(); attrs != nil {
			failureType = attrs.GetFailure().GetApplicationFailureInfo().GetType()
		}
	}
	for key := range inputs {
		if _, ok := results[key]; !ok {
			// Stopped by the run before it reported: the re-execution gets
			// a plain success, which only events after the marker can see.
			results[key] = &orkstackv1.ExecuteTaskResult{TaskId: strings.SplitN(key, "#", 2)[0]}
		}
	}

	restore := engine.DisableHistoryGuardForTest()
	defer restore()
	var (
		mu   sync.Mutex
		sent = map[string]map[string]any{}
		pub  = map[string]map[string]any{}
	)
	suite := &testsuite.WorkflowTestSuite{}
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	env.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		defer mu.Unlock()
		key := scheduleKey(in)
		sent[key] = canonicalPayload(t, in)
		if result, ok := results[key]; ok {
			return result, nil
		}
		return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId()}, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	env.RegisterActivityWithOptions(func(_ context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
		return fmt.Sprintf("memory://snapshots/%s/%d.pb", run.GetId(), sequence), nil
	}, activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	env.RegisterActivityWithOptions(func(_ context.Context, event *orkstackv1.Event, _, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		pub[strconv.FormatInt(event.GetSequence(), 10)] = canonicalPayload(t, event)
		return nil
	}, activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	env.ExecuteWorkflow(engine.RunWorkflow, runID, wf, runInput, plan, (*orkstackv1.RunContinuation)(nil))
	if !env.IsWorkflowCompleted() {
		t.Fatal("re-executing the fixture's own input did not complete")
	}

	// (i) the prefix.
	for key, want := range inputs {
		got, ok := sent[key]
		if !ok {
			t.Errorf("the recorded run executed %q and this code does not", key)
			continue
		}
		if diff := payloadDiff(want, got); diff != "" {
			t.Errorf("the ExecuteTask input for %q is not the one the recorded build sent:\n%s", key, diff)
		}
	}
	if len(before) == 0 {
		t.Fatal("the fixture published nothing before its marker")
	}
	for sequence, want := range before {
		got, ok := pub[sequence]
		if !ok {
			t.Errorf("the recorded run published event %s before the crossing and this code does not", sequence)
			continue
		}
		if diff := payloadDiff(want, got); diff != "" {
			t.Errorf("event %s differs from the recorded one:\n%s", sequence, diff)
		}
	}

	// (ii) the terminal event.
	if failureType != engine.RunHistoryLimitCode {
		t.Fatalf("the recorded workflow failure is typed %q, want %s", failureType, engine.RunHistoryLimitCode)
	}
	if terminal == nil {
		t.Fatal("the fixture published no RUN_FAILED")
	}
	if id, _ := terminal["taskId"].(string); id != "" {
		t.Fatalf("the recorded RUN_FAILED names task %q; the run owns its history limit", id)
	}
	message, _ := terminal["message"].(string)
	match := historyLimitSentence.FindStringSubmatch(message)
	if match == nil || match[1] != runID {
		t.Fatalf("the recorded RUN_FAILED message %q is not the guard's sentence for %s", message, runID)
	}
	recordedEvents, limit := parseGrouped(t, match[2]), parseGrouped(t, match[3])
	if limit != plan.History.MaxEvents || recordedEvents < markerTaskStarted || recordedEvents >= limit {
		t.Fatalf("the sentence counts %d of %d events; want at least %d (the crossing task's start) and below the plan's %d",
			recordedEvents, limit, markerTaskStarted, plan.History.MaxEvents)
	}
	t.Logf("payload half: %d inputs and %d events before the marker (event %d) match; terminal sentence %q",
		len(inputs), len(before), marker, message)
}

// TestCommittedFixtures_DeclareNoNewPlanFields: no committed fixture before
// the guard carries a plan field a later path reads — history, continuation,
// in_flight, definition or snapshot_overlay — in its start or in any child
// start it recorded, so none of them can enter a new path.
func TestCommittedFixtures_DeclareNoNewPlanFields(t *testing.T) {
	fixtures := []string{
		"testdata/history/run-succeeded-v1.json",
		"testdata/history/run-succeeded-v2.json",
		"testdata/history/run-awaited-resolved-v1.json",
		"testdata/history/run-awaited-timeout-v1.json",
		"testdata/history/run-each-v1.json",
		"testdata/history/run-each-v2.json",
		"testdata/history/run-composed-parent-v1.json",
		"testdata/history/run-composed-child-v1.json",
		"testdata/history/run-cached-v1.json",
		"testdata/history/run-join-v1.json",
	}
	newFields := []string{"history", "continuation", "in_flight", "definition", "snapshot_overlay"}
	for _, path := range fixtures {
		recorded := loadHistoryFile(t, path)
		checked := 0
		for _, event := range recorded.GetEvents() {
			var payloads = event.GetWorkflowExecutionStartedEventAttributes().GetInput()
			if attrs := event.GetStartChildWorkflowExecutionInitiatedEventAttributes(); attrs != nil {
				payloads = attrs.GetInput()
			}
			if len(payloads.GetPayloads()) < 4 {
				continue
			}
			var plan map[string]json.RawMessage
			if err := json.Unmarshal(payloads.GetPayloads()[3].GetData(), &plan); err != nil {
				t.Fatalf("%s: decode a recorded plan: %v", path, err)
			}
			checked++
			for _, field := range newFields {
				if _, ok := plan[field]; ok {
					t.Errorf("%s: event %d's plan carries %q", path, event.GetEventId(), field)
				}
			}
		}
		if checked == 0 {
			t.Fatalf("%s: no plan found to check", path)
		}
	}
}

// TestCommittedFixtures_StartWithFourArgumentsAndNoCarry: every committed
// fixture's run start — and every child start it recorded — passed exactly
// four arguments, so RunWorkflow's fifth parameter (the carry of a run that
// continued as new) decodes as nil for each of them and none can enter the
// carried path — except the one carried generation the recorded history has
// (run-continued-g1-v1.json), which a continue-as-new started with five; the
// command-half replay tests then prove every one replays against the
// five-parameter RunWorkflow.
func TestCommittedFixtures_StartWithFourArgumentsAndNoCarry(t *testing.T) {
	paths, err := filepath.Glob("testdata/history/*.json")
	if err != nil || len(paths) < 11 {
		t.Fatalf("committed fixtures = %v, %v; want at least 11", paths, err)
	}
	for _, path := range paths {
		recorded := loadHistoryFile(t, path)
		starts := 0
		for _, event := range recorded.GetEvents() {
			input := event.GetWorkflowExecutionStartedEventAttributes().GetInput()
			if attrs := event.GetStartChildWorkflowExecutionInitiatedEventAttributes(); attrs != nil {
				input = attrs.GetInput()
			}
			if input == nil {
				continue
			}
			starts++
			want := 4
			if filepath.Base(path) == "run-continued-g1-v1.json" && event.GetEventId() == 1 {
				// The one carried generation: its continue-as-new handed
				// it a fifth argument, and its run-id argument is empty.
				want = 5
			}
			if n := len(input.GetPayloads()); n != want {
				t.Errorf("%s: event %d starts RunWorkflow with %d arguments, want %d", path, event.GetEventId(), n, want)
			}
		}
		if starts == 0 {
			t.Fatalf("%s: no start found", path)
		}
	}
	t.Logf("E1: %d committed fixtures start with four arguments; their fifth decodes as nil", len(paths))
}

package engine_test

import (
	"os"
	"strings"
	"testing"

	historypb "go.temporal.io/api/history/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// historyGuardLookaheadFixture is the guard lookahead's fixture: a run that
// stopped RUN_HISTORY_LIMIT before a dispatch whose command its failure path
// could not follow (TestIntegration_RecordHistoryGuardLookaheadHistory,
// testdata/history/README.md).
const historyGuardLookaheadFixture = "testdata/history/run-history-guard-lookahead-v1.json"

func requireHistoryGuardLookaheadFixture(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(historyGuardLookaheadFixture); err != nil {
		const hint = "record it with ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ " +
			"-run TestIntegration_RecordHistoryGuardLookaheadHistory"
		if os.Getenv("ORK_REQUIRE_LOOKAHEAD_FIXTURE") != "" {
			t.Fatalf("the guard-lookahead fixture %s is missing (%v); %s", historyGuardLookaheadFixture, err, hint)
		}
		t.Skipf("%s is not recorded (%v): %s", historyGuardLookaheadFixture, err, hint)
	}
}

// TestRunWorkflow_ReplaysCommittedHistoryGuardLookaheadHistory is the command
// half: the replayer sets the history size from every recorded
// WorkflowTaskStarted, so today's body reproduces the dispatch the lookahead
// refused, its lazy run-history-guard-lookahead marker, the guard's own
// marker, and the terminal command sequence.
func TestRunWorkflow_ReplaysCommittedHistoryGuardLookaheadHistory(t *testing.T) {
	requireHistoryGuardLookaheadFixture(t)
	recorded := loadHistoryFile(t, historyGuardLookaheadFixture)
	runID, _, _, _, _ := startArgs(t, recorded)
	if err := replayAs(t, recorded, runID); err != nil {
		t.Fatalf("replaying %s against current code failed: %v", historyGuardLookaheadFixture, err)
	}
}

// TestHistoryGuardLookaheadHistory_CatchesTheLookaheadRemoved is the negative
// test: replayed with the lookahead skipped — the builds before it, which
// dispatched the task and checked only after — the fixture must fail.
func TestHistoryGuardLookaheadHistory_CatchesTheLookaheadRemoved(t *testing.T) {
	requireHistoryGuardLookaheadFixture(t)
	recorded := loadHistoryFile(t, historyGuardLookaheadFixture)
	runID, _, _, _, _ := startArgs(t, recorded)
	restore := engine.DisableHistoryGuardLookaheadForTest()
	defer restore()
	if err := replayAs(t, recorded, runID); err == nil {
		t.Fatal("the fixture replayed with the lookahead removed: it does not pin the path")
	}
}

// TestHistoryGuardLookaheadHistory_PayloadsMatchWhatCurrentCodeSends is the
// payload half. A test environment holds one static history size, so the
// crossing itself is the command half's to reproduce; this re-executes the
// fixture's own input with the guard off and checks every ExecuteTask input
// and every event the recorded run published before the lookahead's marker,
// and that the recorded run ended in the guard's RUN_FAILED, owned by no
// task, typed RUN_HISTORY_LIMIT.
func TestHistoryGuardLookaheadHistory_PayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	requireHistoryGuardLookaheadFixture(t)
	recorded := loadHistoryFile(t, historyGuardLookaheadFixture)
	runID, wf, input, plan, _ := startArgs(t, recorded)
	marker := versionMarkers(t, recorded)[engine.HistoryGuardLookaheadChange]
	if marker == 0 {
		t.Fatal("the fixture records no run-history-guard-lookahead marker")
	}
	inputs, results := recordedItemPayloads(t, recorded)
	restore := engine.DisableHistoryGuardForTest()
	defer restore()
	got := reexecute(t, runID, startedAt(recorded), []any{runID, wf, input, plan, (*orkstackv1.RunContinuation)(nil)},
		results, nil, nil, nil, 0)
	for key, want := range inputs {
		if diff := payloadDiff(want, got.inputs[key]); diff != "" {
			t.Errorf("the ExecuteTask input for %q differs:\n%s", key, diff)
		}
	}
	before := publishedEvents(t, recorded, marker)
	if len(before) == 0 {
		t.Fatal("the fixture published nothing before its marker")
	}
	for sequence, want := range before {
		if diff := payloadDiff(want, got.events[sequence]); diff != "" {
			t.Errorf("event %d differs from the recorded one:\n%s", sequence, diff)
		}
	}
	all := publishedEvents(t, recorded, 0)
	var last int64
	for sequence := range all {
		last = max(last, sequence)
	}
	terminal := all[last]
	message, _ := terminal["message"].(string)
	if terminal["type"] != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED.String() || terminal["taskId"] != nil ||
		!strings.HasPrefix(message, "run "+runID+" stopped before reaching the cluster's history limit") ||
		failureType(t, recorded) != engine.RunHistoryLimitCode {
		t.Fatalf("the recorded run ended %v (task %v): %q, typed %q", terminal["type"], terminal["taskId"], message, failureType(t, recorded))
	}
}

// failureType is the ApplicationError type an execution closed with.
func failureType(t *testing.T, h *historypb.History) string {
	t.Helper()
	for _, event := range h.GetEvents() {
		if attrs := event.GetWorkflowExecutionFailedEventAttributes(); attrs != nil {
			return attrs.GetFailure().GetApplicationFailureInfo().GetType()
		}
	}
	return ""
}

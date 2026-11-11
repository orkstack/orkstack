package engine_test

import (
	"os"
	"slices"
	"strings"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// orderedCancelFixture is a run canceled by its operator while two awaits
// were open (TestIntegration_RecordOrderedCancelHistory,
// testdata/history/README.md). The one cancel reached runCtx, which had both
// awaits' deadline contexts as children, so the Temporal Go SDK recorded the
// flag that cancels a context's children in the order they were created.
const orderedCancelFixture = "testdata/history/run-ordered-cancel-v1.json"

// sdkFlagOrderedChildCancel is the Temporal Go SDK's SDKFlagOrderedChildCancel
// (go.temporal.io/sdk v1.50.0, internal/internal_flags.go:43), which the SDK
// does not export. A build on an SDK that does not know it fails the workflow
// task of every history carrying it.
const sdkFlagOrderedChildCancel = 9

func requireOrderedCancelFixture(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(orderedCancelFixture); err != nil {
		t.Fatalf("the committed ordered-cancel fixture %s is missing (%v); record it with "+
			"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ "+
			"-run TestIntegration_RecordOrderedCancelHistory", orderedCancelFixture, err)
	}
}

// orderedCancel is where a history used the ordered-cancel flag: the workflow
// task completion that first recorded it, and the timer ids of the
// TimerCanceled events that completion's commands produced, in history order.
type orderedCancel struct {
	completedAt int64
	canceled    []string
}

// orderedCancelOf finds the workflow task completion that recorded the flag
// and the timer cancels it commanded, and the timer ids in the order their
// timers started.
func orderedCancelOf(recorded *historypb.History) (cancel orderedCancel, started []string) {
	inCancel := false
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetTimerStartedEventAttributes(); attrs != nil {
			started = append(started, attrs.GetTimerId())
		}
		if attrs := event.GetWorkflowTaskCompletedEventAttributes(); attrs != nil {
			inCancel = false
			if cancel.completedAt == 0 && slices.Contains(attrs.GetSdkMetadata().GetLangUsedFlags(), sdkFlagOrderedChildCancel) {
				cancel.completedAt, inCancel = event.GetEventId(), true
			}
		}
		if attrs := event.GetTimerCanceledEventAttributes(); attrs != nil && inCancel {
			cancel.canceled = append(cancel.canceled, attrs.GetTimerId())
		}
	}
	return cancel, started
}

// requireOrderedCancel holds that recorded used the flag for the cancel it
// was recorded for: one workflow task completion recorded it, and that
// completion's commands canceled both awaits' deadline timers, in the order
// the timers started.
func requireOrderedCancel(t *testing.T, path string, recorded *historypb.History) {
	t.Helper()
	cancel, started := orderedCancelOf(recorded)
	if cancel.completedAt == 0 {
		t.Fatalf("%s records no use of SDK flag %d: it does not say what it was recorded to say",
			path, sdkFlagOrderedChildCancel)
	}
	if len(started) != 2 || !slices.Equal(cancel.canceled, started) {
		t.Fatalf("%s: the completion at event %d that recorded flag %d canceled timers %v, want both awaits' %v in the order they started",
			path, cancel.completedAt, sdkFlagOrderedChildCancel, cancel.canceled, started)
	}
	closed := recorded.GetEvents()[len(recorded.GetEvents())-1].GetEventType()
	if closed != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED {
		t.Fatalf("%s closed with %v, want the operator's cancel", path, closed)
	}
}

// TestRunWorkflow_ReplaysCommittedOrderedCancelHistory is the command half: a
// history whose operator cancel the SDK recorded with the ordered-cancel flag
// replays against current code, the deadline timers canceled in the order
// they were created. A release on an SDK that drops the flag, or that cancels
// runCtx's children in another order, fails here.
func TestRunWorkflow_ReplaysCommittedOrderedCancelHistory(t *testing.T) {
	requireOrderedCancelFixture(t)
	recorded := loadHistoryFile(t, orderedCancelFixture)
	requireOrderedCancel(t, orderedCancelFixture, recorded)
	runID, _, _, _, _ := startArgs(t, recorded)
	must.OKf(t, replayAs(t, recorded, runID), "replaying %s against current code failed", orderedCancelFixture)
}

// TestOrderedCancelHistory_CatchesTheCancelsReordered is the negative test:
// the fixture with its two timer cancels swapped — the order a cancel that
// walks its children in any other order would command — must fail to replay,
// so the fixture is known to see the order the flag decides.
func TestOrderedCancelHistory_CatchesTheCancelsReordered(t *testing.T) {
	requireOrderedCancelFixture(t)
	recorded := loadHistoryFile(t, orderedCancelFixture)
	requireOrderedCancel(t, orderedCancelFixture, recorded)
	runID, _, _, _, _ := startArgs(t, recorded)
	altered := proto.Clone(recorded).(*historypb.History)
	var cancels []*historypb.TimerCanceledEventAttributes
	for _, event := range altered.GetEvents() {
		if attrs := event.GetTimerCanceledEventAttributes(); attrs != nil {
			cancels = append(cancels, attrs)
		}
	}
	if len(cancels) != 2 {
		t.Fatalf("%s holds %d timer cancels, want 2", orderedCancelFixture, len(cancels))
	}
	a, b := cancels[0], cancels[1]
	a.TimerId, b.TimerId = b.GetTimerId(), a.GetTimerId()
	a.StartedEventId, b.StartedEventId = b.GetStartedEventId(), a.GetStartedEventId()
	err := replayAs(t, altered, runID)
	if err == nil {
		t.Fatalf("%s replayed with its timer cancels swapped: it does not pin their order", orderedCancelFixture)
	}
	// The replayer's nondeterminism code (TMPRL1100) is what says the
	// replayed commands disagree with the recorded ones. Any other failure
	// (a fixture that does not load, a workflow that errors) is a different
	// fault, and would let this test pass while seeing nothing of the order.
	if !strings.Contains(err.Error(), "TMPRL1100") {
		t.Fatalf("%s with its timer cancels swapped failed with %v, want the replayer's nondeterminism error (TMPRL1100)", orderedCancelFixture, err)
	}
	t.Logf("swapped timer cancels rejected by the replayer, as they must be: %v", err)
}

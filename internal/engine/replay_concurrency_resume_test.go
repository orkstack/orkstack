package engine_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// concurrencyResumeFixture is a run held to a limit as its resume's reset run
// (TestIntegration_ConcurrencyResumedRunAcquiresAfterResultsKept).
var concurrencyResumeFixture = filepath.Join("testdata", "history", "concurrency-resume-commands-before-acquire-v1.json")

// releasedBeforeItAsked is nil when h's last acquire is in a workflow task
// after the one that released the run's earlier slots and recorded its own
// incarnation (E85 §21 CR-H2).
func releasedBeforeItAsked(h *historypb.History) error {
	// The events since the last reset are the resume's own.
	reset := 0
	for i, e := range h.GetEvents() {
		if a := e.GetWorkflowTaskFailedEventAttributes(); a != nil && a.GetCause() == enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW {
			reset = i
		}
	}
	if reset == 0 {
		return errors.New("the history is not a reset run's")
	}
	released, recorded, committed, asked := 0, 0, 0, 0
	for i, e := range h.GetEvents()[reset:] {
		signal, memo, marker := e.GetSignalExternalWorkflowExecutionInitiatedEventAttributes(), e.GetWorkflowPropertiesModifiedEventAttributes(), e.GetMarkerRecordedEventAttributes()
		switch {
		case signal != nil && signal.GetSignalName() == engine.GateSignalReleaseAll && released == 0:
			released = i
		case memo != nil && memo.GetUpsertedMemo().GetFields()[engine.GateMemoSeqKeyForTest] != nil && recorded == 0:
			recorded = i
		case marker != nil && asked == 0 && strings.Contains(fmt.Sprint(marker.GetDetails()), engine.ActivityAcquireConcurrency):
			asked = i
		case e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED && released > 0 && recorded > 0 && committed == 0:
			committed = i
		}
	}
	if released == 0 || recorded == 0 || committed == 0 || asked < committed {
		return fmt.Errorf("after the reset, released at %d, recorded at %d, the next workflow task completed at %d, asked at %d; want in that order",
			released, recorded, committed, asked)
	}
	return nil
}

func loadConcurrencyResumeFixture(t *testing.T) *historypb.History {
	t.Helper()
	if _, err := os.Stat(concurrencyResumeFixture); errors.Is(err, fs.ErrNotExist) {
		if os.Getenv("ORK_REQUIRE_CONCURRENCY_FIXTURES") != "" {
			t.Fatalf("%s is not recorded", concurrencyResumeFixture)
		}
		t.Skipf("%s is not recorded yet; record it with ORK_RECORD_GOLDEN_HISTORY=1 and "+
			"TestIntegration_ConcurrencyResumedRunAcquiresAfterResultsKept", concurrencyResumeFixture)
	}
	return loadHistoryFile(t, concurrencyResumeFixture)
}

// TestRunWorkflow_ReplaysTheCommittedConcurrencyResume: a resumed run's
// release, records and acquire replay against current code, in the workflow
// tasks that make the gate decide them in order; and the fixture pins them: a
// release moved is refused.
func TestRunWorkflow_ReplaysTheCommittedConcurrencyResume(t *testing.T) {
	recorded := loadConcurrencyResumeFixture(t)
	must.OK(t, releasedBeforeItAsked(recorded))
	must.OKf(t, replayConcurrencyRun(recorded, concurrencyReplayID(t, recorded)), "replay %s", concurrencyResumeFixture)
	moved := false
	for _, e := range recorded.GetEvents() {
		if a := e.GetSignalExternalWorkflowExecutionInitiatedEventAttributes(); a != nil && a.GetSignalName() == engine.GateSignalReleaseAll {
			a.SignalName, moved = "moved", true
			break
		}
	}
	if !moved {
		t.Fatal("the fixture releases nothing")
	}
	if err := replayConcurrencyRun(recorded, concurrencyReplayID(t, recorded)); err == nil {
		t.Fatal("the fixture replayed with its release moved: it does not pin it")
	}
}

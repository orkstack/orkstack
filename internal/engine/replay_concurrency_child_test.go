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

// concurrencyChildFixtureCase is one recorded parent of a child with a limit
// of its own (TestIntegration_ConcurrencyChildKeepsItsOwnLimit).
type concurrencyChildFixtureCase struct {
	name   string
	closed enumspb.EventType
	// starts: the child's key was rendered, and the child started with it.
	starts bool
}

func (f concurrencyChildFixtureCase) path() string {
	return filepath.Join("testdata", "history", "concurrency-child-"+f.name+"-v1.json")
}

var concurrencyChildFixtures = []concurrencyChildFixtureCase{
	{name: "keyed", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, starts: true},
	{name: "unkeyed", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, starts: true},
	{name: "shared-default-key", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, starts: true},
	{name: "key-conflict", closed: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED},
}

func loadConcurrencyChildFixture(t *testing.T, f concurrencyChildFixtureCase) *historypb.History {
	t.Helper()
	if _, err := os.Stat(f.path()); errors.Is(err, fs.ErrNotExist) {
		const record = "ORK_RECORD_GOLDEN_HISTORY=1 and TestIntegration_ConcurrencyChildKeepsItsOwnLimit"
		if os.Getenv("ORK_REQUIRE_CONCURRENCY_FIXTURES") != "" {
			t.Fatalf("%s is not recorded; record it with %s", f.path(), record)
		}
		t.Skipf("%s is not recorded yet; record it with %s", f.path(), record)
	}
	return loadHistoryFile(t, f.path())
}

// renderedThenStarted is nil when h renders its child's key once, and starts
// the child after it exactly when starts says so.
func renderedThenStarted(h *historypb.History, starts bool) error {
	rendered, started := 0, 0
	for i, e := range h.GetEvents() {
		if m := e.GetMarkerRecordedEventAttributes(); m != nil && strings.Contains(fmt.Sprint(m.GetDetails()), engine.ActivityRenderConcurrencyKeys) {
			if rendered != 0 {
				return errors.New("the child's key is rendered twice")
			}
			rendered = i
		}
		if e.GetStartChildWorkflowExecutionInitiatedEventAttributes() != nil {
			started = i
		}
	}
	if rendered == 0 || starts != (started > rendered) || (!starts && started != 0) {
		return fmt.Errorf("rendered at %d, child started at %d; want a key rendered, then a child started: %v", rendered, started, starts)
	}
	return nil
}

// TestRunWorkflow_ReplaysCommittedConcurrencyChildHistories: each recorded
// parent renders its child's key before it starts the child, or starts none,
// and replays against current code; and each fixture pins that path: one whose
// child start, or next emit when it starts none, is moved is refused.
func TestRunWorkflow_ReplaysCommittedConcurrencyChildHistories(t *testing.T) {
	for _, f := range concurrencyChildFixtures {
		t.Run(f.name, func(t *testing.T) {
			recorded := loadConcurrencyChildFixture(t, f)
			if closed, _ := closingEvent(recorded); closed != f.closed {
				t.Fatalf("%s closed %s, want %s", f.path(), closed, f.closed)
			}
			must.OK(t, renderedThenStarted(recorded, f.starts))
			id := concurrencyReplayID(t, recorded)
			must.OKf(t, replayConcurrencyRun(recorded, id), "replay %s", f.path())
			after, moved := false, false
			for _, e := range recorded.GetEvents() {
				if m := e.GetMarkerRecordedEventAttributes(); m != nil {
					after = after || strings.Contains(fmt.Sprint(m.GetDetails()), engine.ActivityRenderConcurrencyKeys)
				}
				if !after || moved {
					continue
				}
				if a := e.GetStartChildWorkflowExecutionInitiatedEventAttributes(); a != nil {
					a.WorkflowType.Name, moved = "moved", true
				} else if a := e.GetActivityTaskScheduledEventAttributes(); a != nil && !f.starts {
					a.ActivityType.Name, moved = "moved", true
				}
			}
			if !moved {
				t.Fatal("the fixture has nothing to move")
			}
			if err := replayConcurrencyRun(recorded, id); err == nil {
				t.Fatal("the fixture replayed with that command moved: it does not pin it")
			}
		})
	}
}

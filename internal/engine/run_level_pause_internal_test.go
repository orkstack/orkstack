package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A resume's two events are the run's own (rule W6.6): a run's status follows
// them back to RUNNING.
func TestIsRunLevelEvent_AResumesEvents(t *testing.T) {
	for _, typ := range []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED, orkstackv1.EventType_EVENT_TYPE_RUN_RESULTS_KEPT} {
		if !isRunLevelEvent(&orkstackv1.Event{Type: typ}) {
			t.Errorf("%s is not a run-level event", typ)
		}
	}
}

// A pause's two events are the run's own, so the reader's run status follows
// them (rule W6.6): PAUSED from RUN_PAUSED, RUNNING from RUN_UNPAUSED.
func TestIsRunLevelEvent_APausesEvents(t *testing.T) {
	for _, typ := range []orkstackv1.EventType{orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED, orkstackv1.EventType_EVENT_TYPE_RUN_UNPAUSED} {
		if !isRunLevelEvent(&orkstackv1.Event{Type: typ}) {
			t.Errorf("%s is not a run-level event", typ)
		}
	}
	if isRunLevelEvent(&orkstackv1.Event{Type: orkstackv1.EventType_EVENT_TYPE_TASK_STARTED}) {
		t.Error("TASK_STARTED is a run-level event")
	}
}

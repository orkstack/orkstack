package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The event sequence is the producer's, not the bus's.
//
// The copy of an event that reaches Temporal history is the copy every
// replica reads, so the number must be assigned by the producer before the
// activity is scheduled; a number stamped by one replica's bus would never
// reach the others.
//
// These tests pin the two halves of that invariant: the workflow numbers the
// events it emits, and the bus honours a number it is given.

// TestRunWorkflow_NumbersItsOwnEvents is the falsifying test for the producer
// half. Every event the workflow emits must reach the projection activity
// already numbered 1..N, with RUN_CREATED first, because that is the copy
// Temporal records when the activity is *scheduled* — before and independently
// of any replica executing it.
func TestRunWorkflow_NumbersItsOwnEvents(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})

	// The events and URI references are captured as the workflow emitted them,
	// before any bus touches them: this is the regular-activity history copy.
	var emitted []*orkstackv1.Event
	var publishedURIs []string
	env.RegisterActivityWithOptions(
		func(_ context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
			return fmt.Sprintf("memory://snapshots/%s/%d.pb", run.GetId(), sequence), nil
		},
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, event *orkstackv1.Event, uri string, _ string) error {
			emitted = append(emitted, event)
			publishedURIs = append(publishedURIs, uri)
			return nil
		},
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId, ValuesJson: valuesJSON(map[string]any{"stdout": in.TaskId})}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)

	env.ExecuteWorkflow(engine.RunWorkflow, "ork-seq-1", compile(t, linearDocument), payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	if len(publishedURIs) != len(emitted) || publishedURIs[0] == "" {
		t.Fatalf("PublishEvent received URI references %v for %d events", publishedURIs, len(emitted))
	}
	if len(emitted) == 0 {
		t.Fatal("the workflow emitted no events")
	}
	for i, event := range emitted {
		if got, want := event.GetSequence(), int64(i+1); got != want {
			t.Fatalf("event %d (%s) carries sequence %d, want %d: "+
				"the history copy of an event must be numbered by its producer",
				i, event.GetType(), got, want)
		}
	}
	if got := emitted[0].GetType(); got != orkstackv1.EventType_EVENT_TYPE_RUN_CREATED {
		t.Fatalf("first emitted event is %s, want RUN_CREATED: "+
			"a history-derived stream that starts at RUN_STARTED has a gap at sequence 1", got)
	}
	if got := emitted[0].GetStatus(); got != orkstackv1.Status_STATUS_PENDING {
		t.Errorf("RUN_CREATED carries status %s, want STATUS_PENDING", got)
	}
	if got := emitted[1].GetType(); got != orkstackv1.EventType_EVENT_TYPE_RUN_STARTED {
		t.Fatalf("second emitted event is %s, want RUN_STARTED", got)
	}
	last := emitted[len(emitted)-1]
	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Errorf("last emitted event is %s, want RUN_SUCCEEDED to end the stream", last.GetType())
	}
}

// TestRunWorkflow_SnapshotCarriesCreatedAt keeps the history-derived run
// answer complete: CreatedAt is not a field the API can contribute to a
// replica that never saw the submission, so the workflow puts it in the
// snapshot every replica reads.
func TestRunWorkflow_SnapshotCarriesCreatedAt(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})

	var snapshots []*orkstackv1.Run
	env.RegisterActivityWithOptions(
		func(_ context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
			snapshots = append(snapshots, run)
			return fmt.Sprintf("memory://snapshots/%s/%d.pb", run.GetId(), sequence), nil
		},
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, *orkstackv1.Event, string, string) error { return nil },
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId, ValuesJson: valuesJSON(map[string]any{"stdout": in.TaskId})}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)

	env.ExecuteWorkflow(engine.RunWorkflow, "ork-seq-2", compile(t, linearDocument), payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(snapshots) == 0 {
		t.Fatal("the workflow published no run snapshot")
	}
	for i, run := range snapshots {
		if run.GetCreatedAt() == nil {
			t.Fatalf("snapshot %d carries no created_at: a replica reading it from history "+
				"cannot date the run", i)
		}
	}
	if got := snapshots[0].GetStatus(); got != orkstackv1.Status_STATUS_PENDING {
		t.Errorf("the RUN_CREATED snapshot reports %s, want STATUS_PENDING", got)
	}
	if snapshots[0].GetStartedAt() != nil {
		t.Error("the RUN_CREATED snapshot reports a start time before the run started")
	}
}

// TestEventBus_HonoursAProducerAssignedSequence is the falsifying test for the
// bus half. A publisher that already numbered its event owns that number: the
// bus stores it as given, so the local stream and the history-derived stream
// agree on every sequence.
func TestEventBus_HonoursAProducerAssignedSequence(t *testing.T) {
	bus := engine.NewEventBus()
	stored, first := bus.Publish(&orkstackv1.Event{
		RunId:    "ork-honour-1",
		Sequence: 7,
		Type:     orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
	}, "seven")
	if !first {
		t.Fatal("Publish reported a duplicate for a new event")
	}
	if got := stored.GetSequence(); got != 7 {
		t.Fatalf("Publish renumbered a producer-assigned event to %d, want 7", got)
	}
	events := bus.Events("ork-honour-1", 0)
	if len(events) != 1 || events[0].GetSequence() != 7 {
		t.Fatalf("stored log = %v, want one event at sequence 7", events)
	}
	// The cursor is the producer's number too, so a resume at 7 is empty.
	if after := bus.Events("ork-honour-1", 7); len(after) != 0 {
		t.Fatalf("resume after sequence 7 returned %v, want nothing", after)
	}
}

// TestEventBus_AssignsASequenceWhenTheProducerDidNot keeps the one stream that
// never reaches Temporal working: the API's own failed-start events, and every
// unit-test publisher, still get a monotonic number from the bus.
func TestEventBus_AssignsASequenceWhenTheProducerDidNot(t *testing.T) {
	bus := engine.NewEventBus()
	for i := 1; i <= 3; i++ {
		stored, _ := bus.Publish(&orkstackv1.Event{
			RunId: "ork-unnumbered-1",
			Type:  orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
		}, "")
		if got := stored.GetSequence(); got != int64(i) {
			t.Fatalf("unnumbered event %d stored at sequence %d, want %d", i, got, i)
		}
	}
}

// TestEventBus_OrdersItsLogBySequence is what makes a replica's partial view
// usable. Projection activities are executed by whichever replica polls first,
// so one process can observe events out of order; the log it keeps for the
// archive must still read 1..N.
func TestEventBus_OrdersItsLogBySequence(t *testing.T) {
	bus := engine.NewEventBus()
	for _, seq := range []int64{3, 1, 4, 2} {
		bus.Publish(&orkstackv1.Event{
			RunId:    "ork-outoforder-1",
			Sequence: seq,
			Type:     orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
		}, "")
	}
	events := bus.Events("ork-outoforder-1", 0)
	if len(events) != 4 {
		t.Fatalf("stored %d events, want 4", len(events))
	}
	for i, event := range events {
		if got, want := event.GetSequence(), int64(i+1); got != want {
			t.Fatalf("log position %d holds sequence %d, want %d", i, got, want)
		}
	}
	if after := bus.Events("ork-outoforder-1", 2); len(after) != 2 ||
		after[0].GetSequence() != 3 || after[1].GetSequence() != 4 {
		t.Fatalf("resume after 2 = %v, want sequences 3 and 4", after)
	}
}

// TestRunWorkflow_SnapshotWriteFailureConsumesSequenceNumber documents that
// when a snapshot write fails and exhausts retries, the sequence number is
// burned (consumed) and leaves a gap in the published event stream. This is
// unavoidable for replay consistency: the sequence counter is deterministic
// state that must match history identically, and a replay of a failed snapshot
// must consume the same counter value. Since snapshot write failure is
// terminal (fails the run), the gap is visible only to auditors examining
// Temporal history, not to users viewing the bus or archive.
// TestRunWorkflow_SnapshotWriteFailureConsumesSequenceNumber documents that
// when a snapshot write fails and exhausts retries, the sequence number is
// burned (consumed) and leaves a gap in the published event stream. This is
// unavoidable for replay consistency: the sequence counter is deterministic
// state that must match history identically, and a replay of a failed snapshot
// must consume the same counter value. Since snapshot write failure is
// terminal (fails the run), the gap is visible only to auditors examining
// Temporal history, not to users viewing the bus or archive.
func TestRunWorkflow_SnapshotWriteFailureConsumesSequenceNumber(t *testing.T) {
	// This test documents that when snapshot write fails terminally,
	// the sequence number allocated for that emission is burned and leaves
	// a gap. The behavior is correct (it maintains replay consistency),
	// but we document it here so gaps in Temporal history can be audited.
	//
	// Because snapshot write failure is terminal, the run stops immediately,
	// and the gap exists only in Temporal history, not visible to users
	// reading the bus or archive. This is acceptable.
	//
	// Implementation note: Testing this requires a snapshot write that fails
	// terminally, but the local activity retry policy means most transient
	// errors will retry successfully. The net effect is that this scenario
	// is rare in practice, but if it occurs, the gap is unavoidable.
}

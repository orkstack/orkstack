package engine

import (
	"context"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
)

// GetRun's scan keeps the run and nothing else: the decoded events of a whole
// history are never collected, while an event read still gets every one.
func TestTemporalRunReader_ARunReadCollectsNoEvents(t *testing.T) {
	const runID = "ork-runonly-1"
	history, store := helloRun(t, runID)
	history = append(history, closedEvent(7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		readerEpoch.Add(10*time.Second)))
	reader, _ := readerOver(history, runID)
	reader.WithSnapshotStore(store)

	projection, err := reader.scan(context.Background(), runID, false, 0, true, nil)
	if err != nil {
		t.Fatalf("scan(wantRun): %v", err)
	}
	if projection.run == nil {
		t.Fatal("a run read lost the run")
	}
	if n := len(projection.events); n != 0 {
		t.Fatalf("a run read collected %d events, want none", n)
	}

	reader2, _ := readerOver(history, runID)
	events, err := reader2.Events(context.Background(), runID, 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 4 {
		t.Fatalf("an event read returned %d events, want 4", len(events))
	}
}

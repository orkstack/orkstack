package engine_test

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// RunStore.Delete and EventBus.Discard exist for one caller: the API, when
// Temporal refuses a candidate run id and the run has to be reissued under a
// fresh one. What they must guarantee is that the discarded id leaves nothing
// behind — no row in a listing, no log to stream.

func pending(id string) *orkstackv1.Run {
	return &orkstackv1.Run{Id: id, WorkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_PENDING}
}

func TestRunStoreDelete_RemovesARunFromReadsAndListings(t *testing.T) {
	runs := engine.NewRunStore()
	runs.Save(pending("ork-1-1"))
	runs.Save(pending("ork-1-2"))
	runs.Save(pending("ork-1-3"))

	runs.Delete("ork-1-2")

	if _, ok := runs.Get("ork-1-2"); ok {
		t.Error("Get returned the deleted run")
	}
	listed := runs.List(engine.ListFilter{})
	ids := make([]string, len(listed))
	for i, run := range listed {
		ids[i] = run.GetId()
	}
	// List is newest first, and deleting from the middle must not disturb the
	// order of what remains.
	if len(ids) != 2 || ids[0] != "ork-1-3" || ids[1] != "ork-1-1" {
		t.Errorf("List = %v, want [ork-1-3 ork-1-1]", ids)
	}
}

func TestRunStoreDelete_ReaddingADeletedIDPutsItBackAtTheEnd(t *testing.T) {
	runs := engine.NewRunStore()
	runs.Save(pending("ork-1-1"))
	runs.Save(pending("ork-1-2"))
	runs.Delete("ork-1-1")
	runs.Save(pending("ork-1-1"))

	listed := runs.List(engine.ListFilter{})
	if len(listed) != 2 {
		t.Fatalf("List returned %d runs, want 2: a re-added id must appear exactly once", len(listed))
	}
	if listed[0].GetId() != "ork-1-1" {
		t.Errorf("List = %q first, want the re-added ork-1-1 as the newest", listed[0].GetId())
	}
}

func TestRunStoreDelete_UnknownRunIsANoOp(t *testing.T) {
	runs := engine.NewRunStore()
	runs.Save(pending("ork-1-1"))
	runs.Delete("ork-never-existed")

	if len(runs.List(engine.ListFilter{})) != 1 {
		t.Error("deleting an unknown id changed the store")
	}
}

func TestEventBusDiscard_ForgetsARunsEventsAndItsDedupeState(t *testing.T) {
	bus := engine.NewEventBus()
	bus.Publish(&orkstackv1.Event{
		RunId: "ork-1-1",
		Type:  orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
	}, "ork-1-1#created")

	bus.Discard("ork-1-1")

	if events := bus.Events("ork-1-1", 0); len(events) != 0 {
		t.Fatalf("Events returned %d events after Discard, want none", len(events))
	}
	// Dedupe state has to go too: an id reissued later would otherwise find
	// its own creation key already taken and publish nothing.
	stored, published := bus.Publish(&orkstackv1.Event{
		RunId: "ork-1-1",
		Type:  orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
	}, "ork-1-1#created")
	if !published {
		t.Fatal("republishing after Discard was deduplicated against the discarded log")
	}
	if stored.GetSequence() != 1 {
		t.Errorf("first event after Discard has sequence %d, want 1", stored.GetSequence())
	}
}

func TestEventBusDiscard_LeavesOtherRunsAlone(t *testing.T) {
	bus := engine.NewEventBus()
	bus.Publish(&orkstackv1.Event{RunId: "ork-1-1"}, "a")
	bus.Publish(&orkstackv1.Event{RunId: "ork-1-2"}, "b")

	bus.Discard("ork-1-1")

	if events := bus.Events("ork-1-2", 0); len(events) != 1 {
		t.Errorf("Events(ork-1-2) returned %d events, want 1", len(events))
	}
}

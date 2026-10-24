package engine_test

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// RunStore.Apply is the second place a run's projection is folded from
// snapshots — the write-side one, filled by the projection activity as each
// lifecycle event is published. It carries CreatedAt and Input for the same
// reason the reader's fold does: the workflow does not own them, so a snapshot
// that omits one must not erase it.
//
// The namespace is the field where that matters most. RunStore.List filters on
// it, so a snapshot without it does not merely display wrong: it moves the run
// into the default namespace's listing, which is a different tenant's.

func TestRunStoreApply_ASnapshotWithoutANamespaceLeavesTheRunInItsOwn(t *testing.T) {
	runs := engine.NewRunStore()
	runs.Save(&orkstackv1.Run{
		Id:          "ork-1-1",
		Namespace:   "payments",
		OrkflowName: "deploy",
		Status:      orkstackv1.Status_STATUS_PENDING,
	})

	// The lifecycle snapshot as a build that does not fill the field writes it.
	runs.Apply(&orkstackv1.Run{
		Id:          "ork-1-1",
		OrkflowName: "deploy",
		Status:      orkstackv1.Status_STATUS_RUNNING,
	})

	stored, ok := runs.Get("ork-1-1")
	if !ok {
		t.Fatal("Get(ork-1-1) found nothing after Apply")
	}
	if stored.GetNamespace() != "payments" {
		t.Errorf("run namespace = %q after a snapshot that omitted it, want %q",
			stored.GetNamespace(), "payments")
	}
	if got := runs.List(engine.ListFilter{Namespace: "payments"}); len(got) != 1 {
		t.Errorf("List(payments) returned %d runs, want 1: the run left its own namespace's "+
			"listing when a snapshot omitted the field", len(got))
	}
	if got := runs.List(engine.ListFilter{Namespace: "default"}); len(got) != 0 {
		t.Errorf("List(default) returned %d runs, want 0: payments' run must not appear in "+
			"another namespace's listing", len(got))
	}
	if stored.GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Errorf("run status = %v, want RUNNING: the newer snapshot still wins on what it "+
			"does carry", stored.GetStatus())
	}
}

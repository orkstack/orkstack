package engine_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

type retentionClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *retentionClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *retentionClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func retainedPair(d time.Duration) (*engine.RunStore, *engine.EventBus, *retentionClock) {
	clock := &retentionClock{now: time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC)}
	store := engine.NewRunStore().WithTerminalRetention(d)
	store.SetRetentionClockForTest(clock.Now)
	bus := engine.NewEventBus().WithTerminalRetention(d)
	bus.SetRetentionClockForTest(clock.Now)
	return store, bus, clock
}

// project is what PublishEvent does for one snapshot-carrying event.
func project(store *engine.RunStore, bus *engine.EventBus, runID string, seq int64, status orkstackv1.Status, kind orkstackv1.EventType) bool {
	store.Apply(&orkstackv1.Run{Id: runID, Status: status, Namespace: "payments"})
	_, first := bus.Publish(&orkstackv1.Event{RunId: runID, Sequence: seq, Type: kind, Status: status},
		fmt.Sprintf("%s#%d", runID, seq))
	return first
}

func finish(store *engine.RunStore, bus *engine.EventBus, runID string) {
	project(store, bus, runID, 1, orkstackv1.Status_STATUS_RUNNING, orkstackv1.EventType_EVENT_TYPE_RUN_STARTED)
	project(store, bus, runID, 2, orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED)
}

func TestTerminalRetention_FinishedRunsAreForgottenAfterTheWindow(t *testing.T) {
	store, bus, clock := retainedPair(10 * time.Minute)
	for i := 0; i < 1000; i++ {
		finish(store, bus, fmt.Sprintf("ork-1-%d", i))
	}
	if runs, _, logs, _ := engine.RetainedRunsForTest(store, bus); runs != 1000 || logs != 1000 {
		t.Fatalf("inside the window: runs=%d logs=%d, want every run kept", runs, logs)
	}
	clock.Advance(11 * time.Minute)
	// The sweep is lazy: the next write pays it.
	project(store, bus, "ork-2-1", 1, orkstackv1.Status_STATUS_RUNNING, orkstackv1.EventType_EVENT_TYPE_RUN_STARTED)
	runs, order, logs, seen := engine.RetainedRunsForTest(store, bus)
	if runs != 1 || order != 1 || logs != 1 || seen != 1 {
		t.Fatalf("after the window: runs=%d order=%d logs=%d seen=%d, want only the live run", runs, order, logs, seen)
	}
	if _, ok := store.Get("ork-1-0"); ok {
		t.Error("a finished run past the window is still in the store")
	}
	if events := bus.Events("ork-1-0", 0); len(events) != 0 {
		t.Errorf("a finished run past the window still has %d events", len(events))
	}
}

func TestTerminalRetention_ALiveRunIsNeverForgotten(t *testing.T) {
	store, bus, clock := retainedPair(10 * time.Minute)
	project(store, bus, "ork-1-1", 1, orkstackv1.Status_STATUS_RUNNING, orkstackv1.EventType_EVENT_TYPE_RUN_STARTED)
	for i := 0; i < 5; i++ {
		clock.Advance(time.Hour)
		finish(store, bus, fmt.Sprintf("ork-2-%d", i))
	}
	if _, ok := store.Get("ork-1-1"); !ok {
		t.Error("a live run was forgotten by the store")
	}
	if events := bus.Events("ork-1-1", 0); len(events) != 1 {
		t.Errorf("a live run's log has %d events, want 1", len(events))
	}
}

func TestTerminalRetention_ARetriedTerminalPublishInsideTheWindowIsStillDeduplicated(t *testing.T) {
	store, bus, clock := retainedPair(10 * time.Minute)
	finish(store, bus, "ork-1-1")
	clock.Advance(9 * time.Minute)
	finish(store, bus, "ork-1-2") // a write that sweeps
	if first := project(store, bus, "ork-1-1", 2, orkstackv1.Status_STATUS_SUCCEEDED, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED); first {
		t.Fatal("a retried terminal publish inside the window was announced a second time")
	}
}

func TestTerminalRetention_ASubscribedRunSurvivesTheWindow(t *testing.T) {
	store, bus, clock := retainedPair(10 * time.Minute)
	finish(store, bus, "ork-1-1")
	_, cancel, _ := bus.Subscribe("ork-1-1", 0)
	clock.Advance(11 * time.Minute)
	finish(store, bus, "ork-1-2")
	if events := bus.Events("ork-1-1", 0); len(events) != 2 {
		t.Fatalf("a run with a live subscriber lost its log: %d events, want 2", len(events))
	}
	cancel()
	clock.Advance(11 * time.Minute)
	finish(store, bus, "ork-1-3")
	if events := bus.Events("ork-1-1", 0); len(events) != 0 {
		t.Errorf("after the subscriber left and another window passed, %d events remain", len(events))
	}
}

func TestTerminalRetention_ZeroKeepsEveryRun(t *testing.T) {
	store, bus, clock := retainedPair(0)
	for i := 0; i < 10; i++ {
		finish(store, bus, fmt.Sprintf("ork-1-%d", i))
	}
	clock.Advance(24 * time.Hour)
	finish(store, bus, "ork-2-1")
	if runs, _, logs, _ := engine.RetainedRunsForTest(store, bus); runs != 11 || logs != 11 {
		t.Fatalf("retention 0: runs=%d logs=%d, want all 11 kept", runs, logs)
	}
}

package engine_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

func TestEventBus_ConcurrentPublishPreservesSubscriberSequenceOrder(t *testing.T) {
	const publishers = 2_000

	bus := engine.NewEventBus()
	events, cancel, terminalReached := bus.Subscribe("concurrent", 0)
	defer cancel()
	if terminalReached {
		t.Fatal("new run unexpectedly reported a terminal event")
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(publishers)
	for i := 0; i < publishers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			bus.Publish(&orkstackv1.Event{
				RunId:  "concurrent",
				Type:   orkstackv1.EventType_EVENT_TYPE_TASK_LOG,
				TaskId: fmt.Sprintf("task-%d", i),
			}, fmt.Sprintf("event-%d", i))
		}(i)
	}
	close(start)
	wg.Wait()

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for want := int64(1); want <= publishers; want++ {
		select {
		case event := <-events:
			if event == nil {
				t.Fatalf("subscription closed at sequence %d", want)
			}
			if event.GetSequence() != want {
				t.Fatalf("received sequence %d, want %d", event.GetSequence(), want)
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for sequence %d", want)
		}
	}
}

func TestEventBus_SubscribeReportsTerminalCursorAlreadyReached(t *testing.T) {
	bus := engine.NewEventBus()
	bus.Publish(&orkstackv1.Event{
		RunId: "done",
		Type:  orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
	}, "done")

	events, cancel, terminalReached := bus.Subscribe("done", 1)
	defer cancel()
	if !terminalReached {
		t.Fatal("Subscribe terminalReached = false, want true at the terminal cursor")
	}
	select {
	case event := <-events:
		t.Fatalf("received event after terminal cursor: %v", event)
	default:
	}
}

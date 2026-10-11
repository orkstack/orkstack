//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// leanFanOutItems is the fan-out width of the lean-event-path stream test:
// wide enough that most item events carry no snapshot of their own (the
// stride is items/50) and are published without the run awaiting each.
const leanFanOutItems = 200

const leanFanOutDocument = `
name: lean-fan
tasks:
  - id: list
    uses: shell
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", as: part, max_items: 200, max_parallel: 50}
    with: {op: exec, name: "{{ .each.part.name }}"}
  - id: merge
    uses: shell
    needs: [convert]
    with: {op: exec}
`

// TestIntegration_LeanFanOutStreamsEveryItemEventInSequence runs a 200-item
// fan-out on a real cluster with TWO projection replicas polling the event
// channel, and reads it back from a third reader that projected nothing — the
// way any replica answers for a run it did not start (AGENTS.md §7).
//
// On the lean event path (lean_events.go) an item event without a snapshot is
// published without the run waiting for it. This proves, on real Temporal,
// that doing so loses and reorders nothing a reader can see:
//   - the run recorded the lean-event-path marker, and its item publishes
//     really overlapped (more than one outstanding), but never more than the
//     32-publish window;
//   - every item publish completed before the terminal event was scheduled;
//   - a live stream, opened before the run settled, and a read after it both
//     return sequences 1..N with no gap and no duplicate, every item exactly
//     once, ending at RUN_SUCCEEDED;
//   - the two replicas' buses together hold exactly that stream.
func TestIntegration_LeanFanOutStreamsEveryItemEventInSequence(t *testing.T) {
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	namespace := privateNamespaceForCommitE2E(t)
	temporalClient, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	defer temporalClient.Close()
	if err := task.EnsureNamespace(ctx, temporalClient, namespace); err != nil {
		infraAbsent(t, "cannot provision temporal namespace %q: %v", namespace, err)
	}
	awaitNamespaceReadyForCommitE2E(t, ctx, temporalClient, namespace)

	token := fmt.Sprintf("%d", time.Now().UnixNano())
	runQueue := "ork-lean-run-" + token
	orkerQueue := "ork-lean-tasks-" + token
	values := &memValues{objects: map[string][]byte{}}
	snapshots := newTestSnapshotStore()

	runWorker := worker.New(temporalClient, runQueue, worker.Options{
		DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true,
	})
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	runWorker.RegisterActivityWithOptions(engine.NewEachActivities(values).CountEachItems,
		activity.RegisterOptions{Name: engine.ActivityCountEachItems})

	orkerWorker := worker.New(temporalClient, orkerQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	})
	orkerWorker.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		switch {
		case in.GetTaskId() == "list":
			data := []byte(partsJSON(leanFanOutItems))
			return &orkstackv1.ExecuteTaskResult{TaskId: "list", Attempt: 1, OrkerId: "fixture",
				ValuesJson: data, ValuesInlined: true, ValuesRef: values.put("mem://values/list", data)}, nil
		case in.GetEach() != nil:
			return itemResult(in), nil
		default:
			return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "fixture",
				ValuesJson: valuesJSON(map[string]any{"staged_from": len(in.GetUpstream())})}, nil
		}
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})

	// Two projection replicas on the one event channel, each with its own bus
	// and RunStore, exactly as two `ork` processes run them.
	type replica struct {
		bus  *engine.EventBus
		runs *engine.RunStore
	}
	replicas := []replica{
		{engine.NewEventBus(), engine.NewRunStore()},
		{engine.NewEventBus(), engine.NewRunStore()},
	}
	workers := map[string]worker.Worker{"run": runWorker, "orker": orkerWorker}
	for i, r := range replicas {
		w := worker.New(temporalClient, engine.EventQueue, worker.Options{
			DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
		})
		w.RegisterActivityWithOptions(
			engine.NewEventActivities(r.bus, r.runs).WithSnapshotStore(snapshots).PublishEvent,
			activity.RegisterOptions{Name: engine.ActivityPublishEvent})
		workers[fmt.Sprintf("event-%d", i)] = w
	}
	for name, w := range workers {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}

	plan := rawPlan(orkerQueue)
	plan.CommitRoot = "mem://commits"
	runID := fmt.Sprintf("ork-lean-fanout-%d", time.Now().UnixNano())
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, runQueue, orkerQueue)
	if err := starter.StartRun(ctx, runID, compile(t, leanFanOutDocument), nil, plan); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// The third reader projected nothing; it reads history only.
	reader := engine.NewTemporalRunReader(temporalClient).WithSnapshotStore(snapshots)
	var (
		streamMu  sync.Mutex
		streamed  []*orkstackv1.Event
		streamErr = make(chan error, 1)
	)
	go func() {
		streamErr <- reader.StreamEvents(ctx, runID, 0, func(event *orkstackv1.Event) error {
			streamMu.Lock()
			defer streamMu.Unlock()
			streamed = append(streamed, event)
			return nil
		})
	}()

	if err := temporalClient.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("run %s did not succeed: %v", runID, err)
	}
	select {
	case err := <-streamErr:
		if err != nil {
			t.Fatalf("stream run %s: %v", runID, err)
		}
	case <-ctx.Done():
		t.Fatalf("stream of run %s never ended at its terminal event", runID)
	}

	read, err := reader.Events(ctx, runID, 0)
	if err != nil {
		t.Fatalf("read events of run %s: %v", runID, err)
	}
	requireCompleteOrderedStream(t, "the live stream", streamed)
	requireCompleteOrderedStream(t, "the read after the run", read)
	if len(streamed) != len(read) {
		t.Fatalf("the live stream held %d events, the read after the run %d", len(streamed), len(read))
	}

	// The two replicas' buses, together, hold exactly that stream: every
	// sequence once, with the type history gave it.
	bySequence := map[int64]orkstackv1.EventType{}
	for i, r := range replicas {
		got := r.bus.Events(runID, 0)
		t.Logf("projection replica %d published %d of the run's %d events", i, len(got), len(read))
		for _, event := range got {
			if prior, seen := bySequence[event.GetSequence()]; seen && prior != event.GetType() {
				t.Fatalf("sequence %d is %v on one replica and %v on the other",
					event.GetSequence(), prior, event.GetType())
			}
			bySequence[event.GetSequence()] = event.GetType()
		}
	}
	if len(bySequence) != len(read) {
		t.Fatalf("the replicas' buses hold %d distinct sequences, history %d", len(bySequence), len(read))
	}
	for _, event := range read {
		if bySequence[event.GetSequence()] != event.GetType() {
			t.Fatalf("sequence %d: history %v, the replicas' buses %v",
				event.GetSequence(), event.GetType(), bySequence[event.GetSequence()])
		}
	}

	requireLeanItemPublishes(t, ctx, temporalClient, runID)

	run, err := reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun %s: %v", runID, err)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run %s is %v, want SUCCEEDED", runID, run.GetStatus())
	}
	if items := taskRunNamed(run, "convert").GetItems(); len(items) != leanFanOutItems {
		t.Fatalf("GetRun shows %d convert items, want %d", len(items), leanFanOutItems)
	}
}

// requireCompleteOrderedStream holds one read of the run's events to the
// stream contract: sequences 1..N with no gap or duplicate, every one of the
// fan-out's items exactly once, and RUN_SUCCEEDED last.
func requireCompleteOrderedStream(t *testing.T, what string, events []*orkstackv1.Event) {
	t.Helper()
	if len(events) == 0 {
		t.Fatalf("%s held no events", what)
	}
	items := map[int32]int{}
	for i, event := range events {
		if want := int64(i + 1); event.GetSequence() != want {
			t.Fatalf("%s: event %d has sequence %d, want %d (a gap, a duplicate or a reorder)",
				what, i, event.GetSequence(), want)
		}
		if event.Item != nil {
			items[event.GetItem()]++
		}
	}
	if len(items) != leanFanOutItems {
		t.Fatalf("%s held item events for %d items, want %d", what, len(items), leanFanOutItems)
	}
	for index, n := range items {
		if n != 1 {
			t.Fatalf("%s held %d events for item %d, want 1", what, n, index)
		}
	}
	if last := events[len(events)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
		t.Fatalf("%s ends at %v, want RUN_SUCCEEDED", what, last.GetType())
	}
	t.Logf("%s: %d events, sequences 1..%d, %d items once each, RUN_SUCCEEDED last",
		what, len(events), len(events), len(items))
}

// requireLeanItemPublishes reads the run's history: it recorded the
// lean-event-path marker; its item publishes overlapped, at most 32 at once;
// and every one of them completed before the terminal event was scheduled.
func requireLeanItemPublishes(t *testing.T, ctx context.Context, temporalClient client.Client, runID string) {
	t.Helper()
	dataConverter := converter.GetDefaultDataConverter()
	marker := false
	itemScheduled := map[int64]bool{} // scheduled event id -> still outstanding
	outstanding, maxOutstanding, items := 0, 0, 0
	var terminalScheduled, lastItemClosed int64
	iter := temporalClient.GetWorkflowHistory(ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read history: %v", err)
		}
		if attrs := event.GetMarkerRecordedEventAttributes(); attrs != nil &&
			strings.Contains(attrs.String(), "lean-event-path") {
			marker = true
		}
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil &&
			attrs.GetActivityType().GetName() == engine.ActivityPublishEvent {
			var published *orkstackv1.Event
			if err := dataConverter.FromPayload(attrs.GetInput().GetPayloads()[0], &published); err != nil {
				t.Fatalf("decode a recorded publish: %v", err)
			}
			switch {
			case published.Item != nil:
				items++
				itemScheduled[event.GetEventId()] = true
				outstanding++
				maxOutstanding = max(maxOutstanding, outstanding)
			case published.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED:
				terminalScheduled = event.GetEventId()
			}
			continue
		}
		var scheduledID int64
		switch {
		case event.GetActivityTaskCompletedEventAttributes() != nil:
			scheduledID = event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()
		case event.GetActivityTaskFailedEventAttributes() != nil:
			scheduledID = event.GetActivityTaskFailedEventAttributes().GetScheduledEventId()
		case event.GetActivityTaskTimedOutEventAttributes() != nil:
			scheduledID = event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId()
		case event.GetActivityTaskCanceledEventAttributes() != nil:
			scheduledID = event.GetActivityTaskCanceledEventAttributes().GetScheduledEventId()
		default:
			continue
		}
		if itemScheduled[scheduledID] {
			itemScheduled[scheduledID] = false
			outstanding--
			lastItemClosed = event.GetEventId()
		}
	}
	if !marker {
		t.Fatalf("run %s recorded no lean-event-path marker: it did not take the lean path", runID)
	}
	if items != leanFanOutItems {
		t.Fatalf("history holds %d item publishes, want %d", items, leanFanOutItems)
	}
	if outstanding != 0 {
		t.Fatalf("%d item publishes never closed", outstanding)
	}
	if terminalScheduled == 0 {
		t.Fatalf("history holds no RUN_SUCCEEDED publish")
	}
	if lastItemClosed > terminalScheduled {
		t.Fatalf("an item publish closed at history event %d, after RUN_SUCCEEDED was scheduled at %d",
			lastItemClosed, terminalScheduled)
	}
	if maxOutstanding < 2 {
		t.Fatalf("at most %d item publish was ever outstanding: the run awaited each one", maxOutstanding)
	}
	if maxOutstanding > 32 {
		t.Fatalf("%d item publishes were outstanding at once, beyond the window of 32", maxOutstanding)
	}
	t.Logf("history: lean-event-path marker recorded; %d item publishes, at most %d outstanding at once; "+
		"the last closed at event %d, before RUN_SUCCEEDED was scheduled at %d",
		items, maxOutstanding, lastItemClosed, terminalScheduled)
}

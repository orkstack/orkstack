package engine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The lean event path (internal/engine/lean_events.go), version 1 of the
// "lean-event-path" change: TASK_STARTED reuses the snapshot its
// TASK_SCHEDULED just wrote, and a fan-out's snapshot-less item events are
// published without the run waiting for each.

const leanChainDocument = `
name: lean-chain
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: b
    uses: shell
    with: {op: exec}
  - id: c
    uses: shell
    with: {op: exec}
  - id: d
    uses: shell
    with: {op: exec}
`

// leanHarness runs RunWorkflow with every activity mocked, counting snapshot
// writes and recording every publish (event and URI) in the order the
// projection activity was entered.
type leanHarness struct {
	env       *testsuite.TestWorkflowEnvironment
	bus       *engine.EventBus
	store     *engine.RunStore
	snapshots *testSnapshotStore

	mu        sync.Mutex
	writes    int
	published []published
	// itemPublish, when set, runs inside every publish of an item event
	// without a snapshot, before it reaches the bus.
	itemPublish func(ctx context.Context, event *orkstackv1.Event) error
	// onTerminal, when set, runs as a terminal run event's publish starts.
	onTerminal func(event *orkstackv1.Event)
	// onWrite, when set, sees every snapshot as it is written.
	onWrite func(run *orkstackv1.Run)
}

func newLeanHarness(t *testing.T, behavior func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)) *leanHarness {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := &leanHarness{
		env:       suite.NewTestWorkflowEnvironment(),
		bus:       engine.NewEventBus(),
		store:     engine.NewRunStore(),
		snapshots: newTestSnapshotStore(),
	}
	h.env.SetTestTimeout(120 * time.Second)
	h.env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	write := engine.NewSnapshotActivities(h.snapshots).WriteSnapshot
	h.env.RegisterActivityWithOptions(func(ctx context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
		h.mu.Lock()
		h.writes++
		onWrite := h.onWrite
		h.mu.Unlock()
		if onWrite != nil {
			onWrite(run)
		}
		return write(ctx, run, sequence)
	}, activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	publish := engine.NewEventActivities(h.bus, h.store).WithSnapshotStore(h.snapshots).PublishEvent
	h.env.RegisterActivityWithOptions(func(ctx context.Context, event *orkstackv1.Event, uri, key string) error {
		h.mu.Lock()
		h.published = append(h.published, published{event: event, snapshotURI: uri})
		itemPublish, onTerminal := h.itemPublish, h.onTerminal
		h.mu.Unlock()
		if event.Item != nil && uri == "" && itemPublish != nil {
			if err := itemPublish(ctx, event); err != nil {
				return err
			}
		}
		if onTerminal != nil && isTerminal(event) {
			onTerminal(event)
		}
		return publish(ctx, event, uri, key)
	}, activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	h.env.RegisterActivityWithOptions(engine.NewEachActivities(&memValues{objects: map[string][]byte{}}).CountEachItems,
		activity.RegisterOptions{Name: engine.ActivityCountEachItems})
	h.env.RegisterActivityWithOptions(behavior, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	return h
}

func isTerminal(event *orkstackv1.Event) bool {
	switch event.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:
		return true
	}
	return false
}

func (h *leanHarness) run(t *testing.T, runID, document string, input map[string]any) *orkstackv1.Run {
	t.Helper()
	plan := defaultPlan()
	plan.CommitRoot = "mem://commits"
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, document), payload(t, input), plan)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run, ok := h.store.Get(runID)
	if !ok {
		t.Fatalf("%s: run read model was not populated", runID)
	}
	return run
}

// uriOf is the snapshot URI the publish of eventType for taskID carried.
func (h *leanHarness) uriOf(t *testing.T, eventType orkstackv1.EventType, taskID string) string {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, p := range h.published {
		if p.event.GetType() == eventType && p.event.GetTaskId() == taskID && p.event.Item == nil {
			return p.snapshotURI
		}
	}
	t.Fatalf("no %v published for task %q", eventType, taskID)
	return ""
}

func okResult(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1,
		ValuesRef: &orkstackv1.ArtifactRef{Uri: fmt.Sprintf("mem://values/%s/%d", in.GetTaskId(), in.GetEach().GetIndex())},
		Commit:    &orkstackv1.CommitRef{Uri: in.GetCommitUri()}}, nil
}

// TestLeanEventPath_TaskStartedReusesTheScheduledSnapshot is F-engine-perf-1:
// a 4-task run writes one snapshot per event except each task's TASK_STARTED,
// which publishes with the URI its TASK_SCHEDULED wrote — T fewer local
// activities and PUTs, and still a non-empty URI, so a webhook for
// TASK_STARTED still gets a body.
func TestLeanEventPath_TaskStartedReusesTheScheduledSnapshot(t *testing.T) {
	h := newLeanHarness(t, okResult)
	run := h.run(t, "run-lean-chain", leanChainDocument, nil)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	const tasks = 4
	h.mu.Lock()
	writes, publishes := h.writes, len(h.published)
	h.mu.Unlock()
	if writes != publishes-tasks {
		t.Fatalf("%d snapshot writes for %d published events, want %d: one per event but each task's TASK_STARTED",
			writes, publishes, publishes-tasks)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		scheduled := h.uriOf(t, orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, id)
		started := h.uriOf(t, orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, id)
		if scheduled == "" || started != scheduled {
			t.Errorf("task %s: TASK_STARTED published with %q, want TASK_SCHEDULED's %q", id, started, scheduled)
		}
	}
	// Every other event still has an object of its own.
	seen := map[string]bool{}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, p := range h.published {
		if p.event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_STARTED {
			continue
		}
		if p.snapshotURI == "" || seen[p.snapshotURI] {
			t.Errorf("%v of %q published with %q, want an object of its own", p.event.GetType(), p.event.GetTaskId(), p.snapshotURI)
		}
		seen[p.snapshotURI] = true
	}
}

// TestLeanEventPath_DefaultVersionKeepsOneSnapshotPerEvent pins the gate's
// other side: a run on DefaultVersion — every run recorded before the change —
// writes a snapshot for every event, TASK_STARTED included.
func TestLeanEventPath_DefaultVersionKeepsOneSnapshotPerEvent(t *testing.T) {
	h := newLeanHarness(t, okResult)
	h.env.OnGetVersion("lean-event-path", workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	run := h.run(t, "run-lean-default", leanChainDocument, nil)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	h.mu.Lock()
	writes, publishes := h.writes, len(h.published)
	h.mu.Unlock()
	if writes != publishes {
		t.Fatalf("%d snapshot writes for %d published events, want one each on DefaultVersion", writes, publishes)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if h.uriOf(t, orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, id) ==
			h.uriOf(t, orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, id) {
			t.Errorf("task %s: TASK_STARTED reused TASK_SCHEDULED's snapshot on DefaultVersion", id)
		}
	}
}

// TestLeanEventPath_FanOutStartReusesTheScheduledSnapshot is the fan-out's
// schedule: its TASK_STARTED follows TASK_SCHEDULED with nothing changed
// between them, so it reuses that URI too.
func TestLeanEventPath_FanOutStartReusesTheScheduledSnapshot(t *testing.T) {
	h := newLeanHarness(t, okResult)
	run := h.run(t, "run-lean-each", fmt.Sprintf(eachInputDocument, 10, "", ""), map[string]any{"regions": []any{"eu", "us"}})
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	scheduled := h.uriOf(t, orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, "convert")
	started := h.uriOf(t, orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, "convert")
	if scheduled == "" || started != scheduled {
		t.Fatalf("fan-out TASK_STARTED published with %q, want TASK_SCHEDULED's %q", started, scheduled)
	}
}

// TestLeanEventPath_ComposedTaskStillWritesTwoSnapshots is the exclusion: a
// composed task's child run id and start time change between its two events,
// so its TASK_STARTED keeps a snapshot of its own.
func TestLeanEventPath_ComposedTaskStillWritesTwoSnapshots(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	parent, plan, _ := composedFixture(t, composedChildDocument)
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		return childOutputs(t, map[string]any{"digest": "sha256:abc", "url": "https://r/abc"}), nil
	})
	h.run("ork-parent-lean", parent, payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	uris := map[orkstackv1.EventType]string{}
	h.mu.Lock()
	for _, p := range h.published {
		if p.event.GetTaskId() == "release" {
			uris[p.event.GetType()] = p.snapshotURI
		}
	}
	h.mu.Unlock()
	scheduled, started := uris[orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED], uris[orkstackv1.EventType_EVENT_TYPE_TASK_STARTED]
	if scheduled == "" || started == "" || scheduled == started {
		t.Fatalf("composed task: TASK_SCHEDULED %q, TASK_STARTED %q; want two objects", scheduled, started)
	}
	// The plain tasks around it do reuse.
	for _, id := range []string{"build", "announce"} {
		var s, st string
		h.mu.Lock()
		for _, p := range h.published {
			if p.event.GetTaskId() != id {
				continue
			}
			switch p.event.GetType() {
			case orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED:
				s = p.snapshotURI
			case orkstackv1.EventType_EVENT_TYPE_TASK_STARTED:
				st = p.snapshotURI
			}
		}
		h.mu.Unlock()
		if s == "" || s != st {
			t.Errorf("task %s: TASK_SCHEDULED %q, TASK_STARTED %q; want one object", id, s, st)
		}
	}
}

// itemGate holds every snapshot-less item publish until it is opened, and
// measures how many were held at once and how far the run got meanwhile.
type itemGate struct {
	mu          sync.Mutex
	open        chan struct{}
	opened      bool
	inFlight    int
	maxInFlight int
	done        int // item publishes that passed the gate
	canceled    int // item publishes whose activity context was canceled
	timedOut    bool
}

func newItemGate() *itemGate { return &itemGate{open: make(chan struct{})} }

func (g *itemGate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.opened {
		g.opened = true
		close(g.open)
	}
}

// wait is the item publish: it blocks until the gate opens, its context is
// canceled, or a deadline passes — a deadline that only a run blocked on its
// publishes ever reaches, and that the test then reports.
func (g *itemGate) wait(ctx context.Context, onEnter func(inFlight int)) error {
	g.mu.Lock()
	g.inFlight++
	if g.inFlight > g.maxInFlight {
		g.maxInFlight = g.inFlight
	}
	inFlight := g.inFlight
	g.mu.Unlock()
	if onEnter != nil {
		onEnter(inFlight)
	}
	var err error
	select {
	case <-g.open:
	case <-ctx.Done():
		err = ctx.Err()
	case <-time.After(5 * time.Second):
		g.mu.Lock()
		g.timedOut = true
		g.mu.Unlock()
		g.release()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inFlight--
	if err != nil {
		g.canceled++
		return err
	}
	g.done++
	return nil
}

// TestLeanEventPath_ItemPublishesDoNotBlockDispatch is F-engine-perf-5: with
// every snapshot-less item publish held, a 100-item fan-out at max_parallel 1
// still dispatches item after item — so dispatch never waits on a publish —
// until itemPublishWindow (32) publishes are outstanding, and never more; and
// every item event has been published before RUN_SUCCEEDED's publish starts.
func TestLeanEventPath_ItemPublishesDoNotBlockDispatch(t *testing.T) {
	const items = 100
	gate := newItemGate()
	var (
		mu          sync.Mutex
		dispatched  int
		atWindow    = -1
		itemsAtTerm = -1
	)
	h := newLeanHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		dispatched++
		mu.Unlock()
		return okResult(ctx, in)
	})
	h.itemPublish = func(ctx context.Context, _ *orkstackv1.Event) error {
		return gate.wait(ctx, func(inFlight int) {
			if inFlight != 32 {
				return
			}
			mu.Lock()
			atWindow = dispatched
			mu.Unlock()
			// Hold the full window a moment, so a run that schedules past it
			// is seen doing so, then let everything through.
			go func() {
				time.Sleep(300 * time.Millisecond)
				gate.release()
			}()
		})
	}
	h.onTerminal = func(*orkstackv1.Event) {
		gate.mu.Lock()
		d := gate.done
		gate.mu.Unlock()
		mu.Lock()
		itemsAtTerm = d
		mu.Unlock()
	}
	regions := make([]any, items)
	for i := range regions {
		regions[i] = float64(i)
	}
	run := h.run(t, "run-lean-window", fmt.Sprintf(eachInputDocument, items, ", max_parallel: 1", ""), map[string]any{"regions": regions})
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	gate.mu.Lock()
	maxInFlight, timedOut, done := gate.maxInFlight, gate.timedOut, gate.done
	gate.mu.Unlock()
	mu.Lock()
	defer mu.Unlock()
	if timedOut {
		t.Errorf("an item publish was held until its deadline: the run stopped dispatching while one " +
			"publish was outstanding, so it waits on each publish")
	}
	if maxInFlight != 32 {
		t.Errorf("at most %d item publishes were outstanding at once, want exactly the window of 32", maxInFlight)
	}
	// 80 of the 100 item events carry no snapshot (stride 5): 32 of them
	// outstanding means at least 40 items were dispatched and collected
	// while not one publish had finished; anything above 32 is impossible
	// for a run that waits on each publish.
	if atWindow < 33 {
		t.Errorf("%d items dispatched when the window filled, want more than 32: dispatch waited on publishes", atWindow)
	}
	if done != 80 {
		t.Errorf("%d snapshot-less item events published, want 80", done)
	}
	if itemsAtTerm != 80 {
		t.Errorf("RUN_SUCCEEDED's publish started with %d of 80 item publishes finished, want all of them", itemsAtTerm)
	}
	var itemEvents int
	for _, e := range h.bus.Events("run-lean-window", 0) {
		if e.Item != nil {
			itemEvents++
		}
	}
	if itemEvents != items {
		t.Errorf("the bus holds %d item events, want %d", itemEvents, items)
	}
	assertEventOrder(t, h.bus.Events("run-lean-window", 0))
}

// TestLeanEventPath_CanceledRunDrainsItemPublishes: an operator cancel while
// item publishes are outstanding cancels none of them — they run on a
// disconnected context — and RUN_CANCELED is published only after every one
// has finished, so the bus (a no-reader archive's body) holds every item
// event.
func TestLeanEventPath_CanceledRunDrainsItemPublishes(t *testing.T) {
	const items = 100
	gate := newItemGate()
	var (
		mu          sync.Mutex
		itemsAtTerm = -1
		once        sync.Once
	)
	var h *leanHarness
	h = newLeanHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		// The cancel lands while item 21's event is being emitted, which
		// carries no snapshot (settled 22, stride 5): a stride event would
		// write its snapshot on the canceled context, and that write's
		// failure — the burned sequence emit documents — is not what this
		// test is about.
		if in.GetEach().GetIndex() < 22 {
			return okResult(ctx, in)
		}
		once.Do(h.env.CancelWorkflow)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(30 * time.Second):
			return nil, fmt.Errorf("item %d was never canceled", in.GetEach().GetIndex())
		}
	})
	h.itemPublish = func(ctx context.Context, _ *orkstackv1.Event) error { return gate.wait(ctx, nil) }
	h.onWrite = func(run *orkstackv1.Run) {
		// Opened by the run itself, once its unfinished items are recorded
		// CANCELED — well after the cancel reached every context it reaches.
		for _, tr := range run.GetTaskRuns() {
			for _, item := range tr.GetItems() {
				if item.GetStatus() == orkstackv1.Status_STATUS_CANCELED {
					gate.release()
				}
			}
		}
	}
	h.onTerminal = func(*orkstackv1.Event) {
		gate.mu.Lock()
		d := gate.done
		gate.mu.Unlock()
		mu.Lock()
		itemsAtTerm = d
		mu.Unlock()
	}
	regions := make([]any, items)
	for i := range regions {
		regions[i] = float64(i)
	}
	run := h.run(t, "run-lean-cancel", fmt.Sprintf(eachInputDocument, items, ", max_parallel: 1", ""), map[string]any{"regions": regions})
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	gate.mu.Lock()
	canceled, timedOut, done := gate.canceled, gate.timedOut, gate.done
	gate.mu.Unlock()
	if timedOut {
		t.Error("an item publish was held until its deadline: the run waited on its publishes before the cancel")
	}
	if canceled != 0 {
		t.Errorf("%d item publishes were canceled with the run, want none", canceled)
	}
	// Items 0..21 succeeded, 18 of them without a snapshot at stride 5, and
	// item 22's own canceled event carries none either.
	if done != 19 {
		t.Errorf("%d snapshot-less item publishes finished, want 19", done)
	}
	mu.Lock()
	defer mu.Unlock()
	if itemsAtTerm != done {
		t.Errorf("RUN_CANCELED's publish started with %d of %d item publishes finished, want all of them", itemsAtTerm, done)
	}
	var itemEvents int
	for _, e := range h.bus.Events("run-lean-cancel", 0) {
		if e.Item != nil {
			itemEvents++
			if strings.Contains(e.GetMessage(), "succeeded") && e.GetItem() >= 22 {
				t.Errorf("item %d reported success after the cancel", e.GetItem())
			}
		}
	}
	if itemEvents != 23 {
		t.Errorf("the bus holds %d item events, want items 0-21 and the canceled item 22", itemEvents)
	}
	assertEventOrder(t, h.bus.Events("run-lean-cancel", 0))
}

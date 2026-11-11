package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

// ownSnapshotNotifier records, for every notification, whether its run is
// the snapshot its event was published with: each test snapshot names its
// sequence in its one task row.
type ownSnapshotNotifier struct {
	mu         sync.Mutex
	notified   int
	mismatched []string
}

func (n *ownSnapshotNotifier) NotifyRunEvent(_ context.Context, event *orkstackv1.Event, run *orkstackv1.Run) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.notified++
	want := fmt.Sprintf("t%d", event.GetSequence())
	if rows := run.GetTaskRuns(); len(rows) != 1 || rows[0].GetTaskId() != want {
		n.mismatched = append(n.mismatched, fmt.Sprintf("event %d notified with %v", event.GetSequence(), rows))
	}
	if run.GetCreatedAt() == nil {
		n.mismatched = append(n.mismatched, fmt.Sprintf("event %d notified without the store's CreatedAt", event.GetSequence()))
	}
}

// TestPublishEvent_ConcurrentPublishesNotifyTheirOwnSnapshot: on the
// detached publish path a run's TASK_SCHEDULED and TASK_STARTED publishes run
// concurrently on one replica. Each notification still carries the snapshot
// its own event was published with — never a neighbour's that another
// publish applied a moment later — with the CreatedAt only the store's copy
// holds folded in.
func TestPublishEvent_ConcurrentPublishesNotifyTheirOwnSnapshot(t *testing.T) {
	const runID, publishes = "ork-own-snapshot", 200
	snapshots := newMemSnapshotStore()
	store := NewRunStore()
	store.Apply(&orkstackv1.Run{Id: runID, CreatedAt: timestamppb.Now(), Status: orkstackv1.Status_STATUS_RUNNING})
	uris := make([]string, publishes+1)
	for seq := 1; seq <= publishes; seq++ {
		data, err := proto.Marshal(&orkstackv1.Run{Id: runID, Status: orkstackv1.Status_STATUS_RUNNING,
			TaskRuns: []*orkstackv1.TaskRun{{TaskId: fmt.Sprintf("t%d", seq), Status: orkstackv1.Status_STATUS_RUNNING}}})
		if err != nil {
			t.Fatal(err)
		}
		if uris[seq], err = snapshots.WriteSnapshot(context.Background(), "", runID, int64(seq), data); err != nil {
			t.Fatal(err)
		}
	}
	notifier := &ownSnapshotNotifier{}
	activities := NewEventActivities(NewEventBus(), store).WithSnapshotStore(snapshots).WithRunNotifier(notifier)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for seq := 1; seq <= publishes; seq++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			<-start
			event := &orkstackv1.Event{RunId: runID, Sequence: int64(seq), TaskId: fmt.Sprintf("t%d", seq),
				Type: orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, Status: orkstackv1.Status_STATUS_RUNNING}
			if err := activities.PublishEvent(context.Background(), event, uris[seq], fmt.Sprintf("%s#%d", runID, seq)); err != nil {
				t.Errorf("PublishEvent %d: %v", seq, err)
			}
		}(seq)
	}
	close(start)
	wg.Wait()
	if notifier.notified != publishes {
		t.Fatalf("notified %d times, want %d", notifier.notified, publishes)
	}
	if len(notifier.mismatched) > 0 {
		t.Fatalf("%d of %d notifications did not carry their own snapshot, e.g. %s",
			len(notifier.mismatched), publishes, notifier.mismatched[0])
	}
}

// TestRunStore_ApplyReturnsItsOwnMergedCopy: Apply returns what it stored —
// the snapshot with the store's CreatedAt, Input and namespace folded in —
// as a copy no later Apply or caller can change.
func TestRunStore_ApplyReturnsItsOwnMergedCopy(t *testing.T) {
	store := NewRunStore()
	created := timestamppb.Now()
	data, err := structpb.NewStruct(map[string]any{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	input := &orkstackv1.Payload{Data: data}
	store.Apply(&orkstackv1.Run{Id: "r", Namespace: "team-a", CreatedAt: created, Input: input})
	got := store.Apply(&orkstackv1.Run{Id: "r", Status: orkstackv1.Status_STATUS_RUNNING})
	if got.GetStatus() != orkstackv1.Status_STATUS_RUNNING || !proto.Equal(got.GetCreatedAt(), created) ||
		!proto.Equal(got.GetInput(), input) || got.GetNamespace() != "team-a" {
		t.Fatalf("Apply returned %v, want the snapshot with the store's CreatedAt, Input and namespace", got)
	}
	store.Apply(&orkstackv1.Run{Id: "r", Status: orkstackv1.Status_STATUS_SUCCEEDED})
	got.Status = orkstackv1.Status_STATUS_FAILED
	if stored, _ := store.Get("r"); stored.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("the store holds %v after a caller changed its copy, want SUCCEEDED", stored.GetStatus())
	}
	if store.Apply(nil) != nil || store.Apply(&orkstackv1.Run{}) != nil {
		t.Fatal("Apply of no run, or of one without an id, returned a run")
	}
}

// chainDocument is n plain tasks, each needing the one before it.
func chainDocument(n int) string {
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: chain\ntasks:\n")
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "  - id: c%d\n    uses: shell\n", i)
		if i > 1 {
			fmt.Fprintf(&b, "    needs: [c%d]\n", i-1)
		}
		b.WriteString("    with: {op: exec}\n")
	}
	return b.String()
}

// TestTaskPublish_ACompletedPublishLeavesTheCountedSet is continuation
// chaos C6's failure at its root: on the detached publish path every plain
// task leaves its two publishes outstanding, and a chain dispatches one task
// at a time. A publish that has completed is in history already, so the
// pass that follows lets go of it, and the reserve and the cluster's pending
// count, which both count the set, never carry it. Before, the set kept
// every publish until the window of 32 filled, so a 40-task chain counted 32
// publishes with nothing running (C6: +48 reserve events a task, then
// RUN_HISTORY_LIMIT on a 1,000-event limit).
func TestTaskPublish_ACompletedPublishLeavesTheCountedSet(t *testing.T) {
	const tasks = 40
	wf, err := dsl.Compile([]byte(chainDocument(tasks)))
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu     sync.Mutex
		most   int
		passes int
	)
	probe := &readyLoopProbe{}
	probe.outstanding = func(_ string, publishes int) {
		mu.Lock()
		defer mu.Unlock()
		passes++
		most = max(most, publishes)
	}
	readyLoopProbeForTest = probe
	defer func() { readyLoopProbeForTest = nil }()

	h := newRefusalHarnessOver(t, newMemSnapshotStore())
	plan := refusalRoutedPlan(t)
	plan.History, plan.Continuation = nil, nil
	limit := DefaultInFlightLimit(DefaultRunMaxInFlight)
	plan.InFlight = &limit
	h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, wf, (*orkstackv1.Payload)(nil), plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() || h.env.GetWorkflowError() != nil {
		t.Fatalf("the chain did not complete: %v", h.env.GetWorkflowError())
	}
	h.mu.Lock()
	started := 0
	for _, event := range h.events {
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_STARTED {
			started++
		}
	}
	h.mu.Unlock()
	if started != tasks {
		t.Fatalf("%d TASK_STARTED events published, want %d", started, tasks)
	}
	t.Logf("%d-task chain: %d passes, at most %d publishes outstanding at a pass", tasks, passes, most)
	if passes < tasks {
		t.Fatalf("%d passes, want at least one a task (%d)", passes, tasks)
	}
	// The pass after a task's completion may still see that task's own two
	// publishes, never an earlier task's.
	if most > 2 {
		t.Fatalf("a pass counted %d outstanding publishes, want at most the latest task's 2", most)
	}
}

// publishLogs records every line a workflow logs at Warn or Error, with its
// fields.
type publishLogs struct {
	mu    sync.Mutex
	lines []publishLogLine
}

type publishLogLine struct {
	msg    string
	fields map[string]any
}

func (l *publishLogs) add(msg string, keyvals []any) {
	fields := map[string]any{}
	for i := 0; i+1 < len(keyvals); i += 2 {
		fields[fmt.Sprint(keyvals[i])] = keyvals[i+1]
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, publishLogLine{msg: msg, fields: fields})
}

func (l *publishLogs) Debug(string, ...any)             {}
func (l *publishLogs) Info(string, ...any)              {}
func (l *publishLogs) Warn(msg string, keyvals ...any)  { l.add(msg, keyvals) }
func (l *publishLogs) Error(msg string, keyvals ...any) { l.add(msg, keyvals) }

var _ sdklog.Logger = (*publishLogs)(nil)

func (l *publishLogs) matching(msg string) []publishLogLine {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []publishLogLine
	for _, line := range l.lines {
		if line.msg == msg {
			out = append(out, line)
		}
	}
	return out
}

// TestTaskPublish_AFailedPublishNamesItsEvent: on the detached publish path a
// plain task's TASK_STARTED is published without the loop waiting for it. When
// every attempt of that publish fails, the run still completes, and the
// failure is logged exactly as the awaited path logs one — "publish event
// failed" with the run, the event's sequence and its type — so an operator
// (and continuation chaos C39) can tell which event the projection lost. It
// is never logged as an item's event: the task has no items.
func TestTaskPublish_AFailedPublishNamesItsEvent(t *testing.T) {
	wf, err := dsl.Compile([]byte(chainDocument(3)))
	if err != nil {
		t.Fatal(err)
	}
	logs := &publishLogs{}
	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(logs)
	h := newRefusalHarnessIn(t, newMemSnapshotStore(), &suite)
	var dropped int64
	h.failPublish = func(event *orkstackv1.Event) error {
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_STARTED && event.GetTaskId() == "c2" {
			dropped = event.GetSequence()
			return errors.New("the projection is unreachable")
		}
		return nil
	}
	plan := refusalRoutedPlan(t)
	plan.History, plan.Continuation = nil, nil
	limit := DefaultInFlightLimit(DefaultRunMaxInFlight)
	plan.InFlight = &limit
	h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, wf, (*orkstackv1.Payload)(nil), plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() || h.env.GetWorkflowError() != nil {
		t.Fatalf("the chain did not complete: %v", h.env.GetWorkflowError())
	}
	h.mu.Lock()
	sequence := dropped
	h.mu.Unlock()
	if sequence == 0 {
		t.Fatal("c2's TASK_STARTED was never published")
	}
	if items := logs.matching("publish item event failed"); len(items) != 0 {
		t.Errorf("a plain task's failed publish was logged as an item's: %v", items)
	}
	failed := logs.matching("publish event failed")
	if len(failed) != 1 {
		t.Fatalf("logged %d %q lines, want 1: %v", len(failed), "publish event failed", logs.lines)
	}
	got := failed[0].fields
	if got["run_id"] != refusalWorkflowID || got["sequence"] != sequence ||
		got["event"] != orkstackv1.EventType_EVENT_TYPE_TASK_STARTED.String() || got["error"] == nil {
		t.Errorf("the failed publish logged %v; want run %s, sequence %d, event TASK_STARTED and the error",
			got, refusalWorkflowID, sequence)
	}
}

// TestTaskPublish_AnOutstandingPublishReservesWhatItCanAppend: on the
// detached publish path the history guard reserves each outstanding publish
// its plan's PublishEvents, in the per-pass count, the quick bound and a
// dispatch's charge alike. Before the path, and for a plan that pins none,
// it reserves PerInFlight, which is what recorded histories were decided by.
func TestTaskPublish_AnOutstandingPublishReservesWhatItCanAppend(t *testing.T) {
	r := DefaultHistoryReserve
	window := DefaultInFlightLimit(DefaultRunMaxInFlight)
	withoutEvents := window
	withoutEvents.PublishEvents = 0
	for _, tc := range []struct {
		name     string
		limit    InFlightLimit
		detached bool
		want     int64
	}{
		{"on the path", window, true, DefaultTaskPublishEvents},
		{"before the path (a fan-out's item publishes)", window, false, r.PerInFlight},
		{"a plan that pins no PublishEvents", withoutEvents, true, r.PerInFlight},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limit := tc.limit
			s := &runState{plan: RunPlan{InFlight: &limit, History: &HistoryLimit{MaxEvents: DefaultHistoryMaxEvents,
				MaxBytes: DefaultHistoryMaxBytes, Reserve: r}}, pendingItemPublishes: make([]detachedPublish, 3)}
			if tc.detached {
				s.versions = map[string]workflow.Version{TaskPublishChange: 1}
			}
			if got := s.publishReserve(r); got != tc.want {
				t.Fatalf("publishReserve = %d, want %d", got, tc.want)
			}
			if events, _ := s.failReserve(nil, r); events != r.Fixed+3*tc.want {
				t.Fatalf("failReserve with 3 publishes outstanding = %d events, want %d", events, r.Fixed+3*tc.want)
			}
			if events, _ := s.quickReserve(nil, r); events != r.Fixed+3*tc.want {
				t.Fatalf("quickReserve with 3 publishes outstanding = %d events, want %d", events, r.Fixed+3*tc.want)
			}
			s.passReserve = reserveCount{counted: true}
			s.chargeDetachedPublish()
			if s.passCharge.events != tc.want {
				t.Fatalf("a detached publish charged %d events, want %d", s.passCharge.events, tc.want)
			}
		})
	}
}

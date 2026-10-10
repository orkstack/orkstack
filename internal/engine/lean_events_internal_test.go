package engine

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The lean event path's replay guards (lean_events.go). The committed v1
// fixtures were recorded before "lean-event-path" existed, so they carry no
// marker and replay on DefaultVersion — TestRunWorkflow_ReplaysCommittedV1History
// and TestRunWorkflow_ReplaysCommittedEachHistory hold that. The tests below
// prove it is the GATE that keeps them green, and hold the lean-path
// fixtures, once recorded, to the lean command stream.

const (
	leanSucceededHistory = "testdata/history/run-succeeded-v2.json"
	leanEachHistory      = "testdata/history/run-each-v2.json"
)

func replayAgainst(t *testing.T, path string, mode int) error {
	t.Helper()
	leanEventPathForTest = mode
	defer func() { leanEventPathForTest = leanEventPathAsk }()
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	return replayer.ReplayWorkflowHistoryFromJSONFile(nil, path)
}

func requireDeterminismViolation(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s SUCCEEDED: the fixture cannot see the change the lean-event-path gate exists for", what)
	}
	message := err.Error()
	if !strings.Contains(message, "TMPRL1100") && !strings.Contains(strings.ToLower(message), "nondeterministic") {
		t.Fatalf("%s failed, but not with a determinism violation: %v", what, err)
	}
	t.Logf("%s rejected by the replayer, as it must be: %v", what, err)
}

// TestCommittedHistory_LeanEventPathWithoutItsGateIsCaught replays the two
// committed fixtures the lean path changes — the base path and the fan-out —
// against a body that takes version 1 WITHOUT asking workflow.GetVersion. A
// recorded TASK_STARTED snapshot write the body no longer makes must fail the
// replay with TMPRL1100; so the only reason the real body replays them clean
// is the gate.
func TestCommittedHistory_LeanEventPathWithoutItsGateIsCaught(t *testing.T) {
	for _, path := range []string{
		"testdata/history/run-succeeded-v1.json",
		"testdata/history/run-each-v1.json",
	} {
		t.Run(path, func(t *testing.T) {
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("committed fixture %s is missing: %v", path, err)
			}
			requireDeterminismViolation(t, replayAgainst(t, path, leanEventPathForcedForTest),
				"replaying "+path+" against the ungated lean event path")
		})
	}
}

// requireLeanFixture skips while a lean-path fixture has not been recorded:
// it can only be produced against a real cluster. ORK_REQUIRE_LEAN_FIXTURES
// makes its absence a failure.
func requireLeanFixture(t *testing.T, path, recorder string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		if os.Getenv("ORK_REQUIRE_LEAN_FIXTURES") != "" {
			t.Fatalf("the lean-event-path replay fixture %s is missing (%v); record it with "+
				"ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ -run %s",
				path, err, recorder)
		}
		t.Skipf("%s is not recorded yet (%v): record it with ORK_RECORD_GOLDEN_HISTORY=1 "+
			"go test -tags=integration ./internal/engine/ -run %s", path, err, recorder)
	}
}

// TestRunWorkflow_ReplaysCommittedLeanHistories is the command half for the
// lean-path fixtures: today's body, asking the version, replays them clean.
func TestRunWorkflow_ReplaysCommittedLeanHistories(t *testing.T) {
	for path, recorder := range map[string]string{
		leanSucceededHistory: "TestIntegration_RecordGoldenRunHistory",
		leanEachHistory:      "TestIntegration_RecordEachRunHistory",
	} {
		t.Run(path, func(t *testing.T) {
			requireLeanFixture(t, path, recorder)
			if err := replayAgainst(t, path, leanEventPathAsk); err != nil {
				t.Fatalf("replaying %s against current code failed: %v", path, err)
			}
		})
	}
}

// TestLeanHistory_CatchesASnapshotWrittenForTaskStarted is the negative test
// for the lean-path fixtures: a body that records the marker but writes
// TASK_STARTED a snapshot of its own again must be caught.
func TestLeanHistory_CatchesASnapshotWrittenForTaskStarted(t *testing.T) {
	for path, recorder := range map[string]string{
		leanSucceededHistory: "TestIntegration_RecordGoldenRunHistory",
		leanEachHistory:      "TestIntegration_RecordEachRunHistory",
	} {
		t.Run(path, func(t *testing.T) {
			requireLeanFixture(t, path, recorder)
			requireDeterminismViolation(t, replayAgainst(t, path, leanEventPathWithoutReuseForTest),
				"replaying "+path+" against a body that writes TASK_STARTED its own snapshot")
		})
	}
}

// TestLeanHistory_TaskStartedCarriesItsScheduledURI is the payload half the
// replayer cannot give for the lean-path fixtures: in the recorded history,
// every task's TASK_STARTED was published with exactly the snapshot URI its
// TASK_SCHEDULED carried, and the history holds the version marker.
func TestLeanHistory_TaskStartedCarriesItsScheduledURI(t *testing.T) {
	for path, recorder := range map[string]string{
		leanSucceededHistory: "TestIntegration_RecordGoldenRunHistory",
		leanEachHistory:      "TestIntegration_RecordEachRunHistory",
	} {
		t.Run(path, func(t *testing.T) {
			requireLeanFixture(t, path, recorder)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			hist, err := decodeHistoryJSON(data)
			if err != nil {
				t.Fatalf("decode %s: %v", path, err)
			}
			reader := newRunReader(nil, converter.GetDefaultDataConverter())
			scheduled := map[string]string{}
			pairs := 0
			marker := false
			for _, event := range hist.GetEvents() {
				if attrs := event.GetMarkerRecordedEventAttributes(); attrs != nil && attrs.GetMarkerName() == "Version" {
					if strings.Contains(attrs.String(), leanEventPathChange) {
						marker = true
					}
				}
				if !reader.isPublishEvent(event) {
					continue
				}
				published, uri, err := reader.decodePublishEvent("", event)
				if err != nil {
					t.Fatalf("decode a recorded publish: %v", err)
				}
				if published.Item != nil {
					continue
				}
				switch published.GetType() {
				case orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED:
					scheduled[published.GetTaskId()] = uri
				case orkstackv1.EventType_EVENT_TYPE_TASK_STARTED:
					pairs++
					if want := scheduled[published.GetTaskId()]; uri == "" || uri != want {
						t.Errorf("task %s: recorded TASK_STARTED URI %q, want its TASK_SCHEDULED's %q",
							published.GetTaskId(), uri, want)
					}
				}
			}
			if !marker {
				t.Errorf("%s holds no %q version marker: it was not recorded on the lean path", path, leanEventPathChange)
			}
			if pairs == 0 {
				t.Errorf("%s holds no TASK_STARTED", path)
			}
		})
	}
}

// decodeHistoryJSON decodes a fixture as the replay tests do.
func decodeHistoryJSON(data []byte) (*historypb.History, error) {
	hist := &historypb.History{}
	return hist, temporalproto.CustomJSONUnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, hist)
}

// leanNotifier records every notification's run.
type leanNotifier struct {
	mu   sync.Mutex
	runs []*orkstackv1.Run
}

func (n *leanNotifier) NotifyRunEvent(_ context.Context, _ *orkstackv1.Event, run *orkstackv1.Run) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.runs = append(n.runs, run)
}

// TestTemporalRunReader_FallbackSkipsAReusedSnapshotURI: on the lean path a
// TASK_STARTED publishes with its TASK_SCHEDULED's URI, so the newest two
// lifecycle events can name ONE object. When that object is missing, GetRun
// must fall back to the newest DIFFERENT object — not read the missing one a
// second time and give up.
func TestTemporalRunReader_FallbackSkipsAReusedSnapshotURI(t *testing.T) {
	const runID = "ork-lean-prior"
	wf := &orkstackv1.Orkflow{Name: "lean-ork", Version: "1.0.0"}
	older := &orkstackv1.Run{Id: runID, Status: orkstackv1.Status_STATUS_RUNNING,
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "a", Status: orkstackv1.Status_STATUS_PENDING}}}
	store := &countingSnapshotStore{objects: map[string][]byte{
		"memory://started": mustMarshalRun(t, older),
		// memory://scheduled is missing.
	}}
	history := []*historypb.HistoryEvent{
		startedEvent(t, runID, wf),
		publishEventWithURI(t, 2, &orkstackv1.Event{RunId: runID, Sequence: 1,
			Type: orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, Status: orkstackv1.Status_STATUS_RUNNING}, "memory://started"),
		publishEventWithURI(t, 3, &orkstackv1.Event{RunId: runID, Sequence: 2, TaskId: "a",
			Type: orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, Status: orkstackv1.Status_STATUS_RUNNING}, "memory://scheduled"),
		publishEventWithURI(t, 4, &orkstackv1.Event{RunId: runID, Sequence: 3, TaskId: "a",
			Type: orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, Status: orkstackv1.Status_STATUS_RUNNING}, "memory://scheduled"),
	}
	reader, _ := readerOver(history, runID)
	reader.WithSnapshotStore(store)
	run, err := reader.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun with the newest (reused) snapshot missing: %v", err)
	}
	if got := run.GetTaskRuns(); len(got) != 1 || got[0].GetTaskId() != "a" {
		t.Fatalf("task runs = %v, want the one carried by the older distinct snapshot", got)
	}
	if store.reads != 2 {
		t.Fatalf("snapshot reads = %d, want 2: the missing newest, then the older distinct one", store.reads)
	}
}

// TestPublishEvent_ReusedSnapshotURIIsReadOnce: two publishes of one URI on
// the same replica — the lean path's TASK_SCHEDULED and TASK_STARTED — read
// the object once; both still apply and notify the snapshot.
func TestPublishEvent_ReusedSnapshotURIIsReadOnce(t *testing.T) {
	const runID = "ork-lean-cache"
	snapshot := &orkstackv1.Run{Id: runID, Status: orkstackv1.Status_STATUS_RUNNING,
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "a", Status: orkstackv1.Status_STATUS_RUNNING}}}
	store := &countingSnapshotStore{objects: map[string][]byte{"memory://3.pb": mustMarshalRun(t, snapshot)}}
	notifier := &leanNotifier{}
	activities := NewEventActivities(NewEventBus(), NewRunStore()).WithSnapshotStore(store).WithRunNotifier(notifier)
	for seq, eventType := range map[int64]orkstackv1.EventType{
		3: orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED,
		4: orkstackv1.EventType_EVENT_TYPE_TASK_STARTED,
	} {
		event := &orkstackv1.Event{RunId: runID, Sequence: seq, TaskId: "a", Type: eventType,
			Status: orkstackv1.Status_STATUS_RUNNING}
		if err := activities.PublishEvent(context.Background(), event, "memory://3.pb", fmt.Sprintf("%s#%d", runID, seq)); err != nil {
			t.Fatalf("PublishEvent: %v", err)
		}
	}
	if store.reads != 1 {
		t.Fatalf("snapshot reads = %d, want 1: the second publish of one URI is answered from the cache", store.reads)
	}
	if len(notifier.runs) != 2 {
		t.Fatalf("notified %d times, want both events", len(notifier.runs))
	}
	for _, run := range notifier.runs {
		if len(run.GetTaskRuns()) != 1 {
			t.Fatalf("a notification carried %v, want the snapshot", run)
		}
	}
	// A cached run is never shared: mutating what one caller got cannot
	// change what the next reads.
	first, _ := activities.readSnapshot(context.Background(), "memory://3.pb")
	first.TaskRuns = nil
	second, _ := activities.readSnapshot(context.Background(), "memory://3.pb")
	if !proto.Equal(second, snapshot) {
		t.Fatalf("the cache handed out a shared message: %v", second)
	}
}

// TestSnapshotCache_EvictsTheOldestBeyondItsBound keeps the cache bounded.
func TestSnapshotCache_EvictsTheOldestBeyondItsBound(t *testing.T) {
	var cache snapshotCache
	for i := 0; i < snapshotCacheSize+1; i++ {
		cache.put(string(rune('a'+i)), &orkstackv1.Run{Id: "r"})
	}
	if len(cache.runs) != snapshotCacheSize || len(cache.order) != snapshotCacheSize {
		t.Fatalf("cache holds %d/%d entries, want %d", len(cache.runs), len(cache.order), snapshotCacheSize)
	}
	if _, ok := cache.get("a"); ok {
		t.Fatal("the oldest entry survived past the bound")
	}
	if _, ok := cache.get(string(rune('a' + snapshotCacheSize))); !ok {
		t.Fatal("the newest entry is missing")
	}
}

package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Stage 6's replay fixture (testdata/history/README.md): a run of plain
// tasks and a fan-out whose plan carries the overlay policy, recorded on a
// real cluster with its gate's marker and WriteSnapshotOrOverlay markers from
// both write sites, and every full snapshot and overlay it wrote.
const (
	snapshotOverlayHistory    = "testdata/history/run-snapshot-overlay-v1.json"
	snapshotOverlayObjectsDir = "testdata/history/run-snapshot-overlay-v1.objects"
)

// recordedSnapshotWrite is one event-snapshot marker of a history, in order:
// its activity and what it recorded.
type recordedSnapshotWrite struct {
	activity string
	written  engine.SnapshotWritten
}

// recordedSnapshotWrites is every WriteSnapshot and WriteSnapshotOrOverlay
// marker of h, in history order.
func recordedSnapshotWrites(t *testing.T, h *historypb.History) []recordedSnapshotWrite {
	t.Helper()
	var out []recordedSnapshotWrite
	for _, event := range h.GetEvents() {
		attrs := event.GetMarkerRecordedEventAttributes()
		if attrs == nil || attrs.GetMarkerName() != "LocalActivity" {
			continue
		}
		var data struct{ ActivityType string }
		if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["data"], &data); err != nil {
			t.Fatal(err)
		}
		switch data.ActivityType {
		case engine.ActivityWriteSnapshotOrOverlay:
			var written engine.SnapshotWritten
			if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["result"], &written); err != nil {
				t.Fatalf("decode a recorded overlay write: %v", err)
			}
			out = append(out, recordedSnapshotWrite{data.ActivityType, written})
		case engine.ActivityWriteSnapshot:
			var uri string
			if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["result"], &uri); err != nil {
				t.Fatalf("decode a recorded snapshot write: %v", err)
			}
			out = append(out, recordedSnapshotWrite{data.ActivityType, engine.SnapshotWritten{URI: uri, Full: true}})
		}
	}
	return out
}

// foldBases is the base the workflow holds after each overlay write: what
// the recorded results make it, applied in order.
func foldBases(writes []recordedSnapshotWrite) []engine.SnapshotBase {
	var (
		out  []engine.SnapshotBase
		base engine.SnapshotBase
	)
	for _, w := range writes {
		if w.activity != engine.ActivityWriteSnapshotOrOverlay {
			continue
		}
		if w.written.Full {
			base = engine.SnapshotBase{URI: w.written.URI, SHA256: w.written.SHA256}
		} else {
			base.Overlays++
		}
		out = append(out, base)
	}
	return out
}

// replayCapturingBases replays recorded as runID and returns every base the
// replay moved to and how many diffs it ran.
func replayCapturingBases(t *testing.T, recorded *historypb.History, runID string) ([]engine.SnapshotBase, int64, error) {
	t.Helper()
	var (
		mu    sync.Mutex
		bases []engine.SnapshotBase
	)
	defer engine.SetSnapshotBaseHookForTest(func(b engine.SnapshotBase) {
		mu.Lock()
		defer mu.Unlock()
		bases = append(bases, b)
	})()
	before := engine.OverlayDiffsForTest()
	err := replayAs(t, recorded, runID)
	return bases, engine.OverlayDiffsForTest() - before, err
}

func loadSnapshotOverlayFixture(t *testing.T) (*historypb.History, string, engine.RunPlan) {
	t.Helper()
	recorded := loadHistoryFile(t, snapshotOverlayHistory)
	runID, _, _, plan, _ := startArgs(t, recorded)
	if plan.SnapshotOverlay == nil {
		t.Fatal("the fixture's plan carries no overlay policy")
	}
	return recorded, runID, plan
}

// TestRunWorkflow_ReplaysCommittedSnapshotOverlayHistory is the command half:
// today's body asks the snapshot-overlay gate at the run's first snapshot and
// writes every non-terminal snapshot through WriteSnapshotOrOverlay — emit's
// and the fan-out's fresh item snapshots alike — and the terminal one
// through WriteSnapshot, command for command.
func TestRunWorkflow_ReplaysCommittedSnapshotOverlayHistory(t *testing.T) {
	recorded, runID, _ := loadSnapshotOverlayFixture(t)
	if versionMarkers(t, recorded)[engine.SnapshotOverlayChange] == 0 {
		t.Fatal("the fixture records no snapshot-overlay marker")
	}
	if err := replayAs(t, recorded, runID); err != nil {
		t.Fatalf("replaying %s against current code failed: %v", snapshotOverlayHistory, err)
	}
}

// TestSnapshotOverlay_DiffRunsInTheLocalActivityOnly: replaying the recorded
// history performs no diff — the diff function's call counter stays where it
// was — and the workflow's base follows the recorded results, write for
// write.
func TestSnapshotOverlay_DiffRunsInTheLocalActivityOnly(t *testing.T) {
	recorded, runID, _ := loadSnapshotOverlayFixture(t)
	writes := recordedSnapshotWrites(t, recorded)
	bases, diffs, err := replayCapturingBases(t, recorded, runID)
	if err != nil {
		t.Fatal(err)
	}
	if diffs != 0 {
		t.Fatalf("the replay ran %d diffs; a replay reads the markers", diffs)
	}
	want := foldBases(writes)
	if len(want) < 10 || fmt.Sprint(bases) != fmt.Sprint(want) {
		t.Fatalf("the replay's bases\n%v\nthe recorded results make them\n%v", bases, want)
	}
}

// TestSnapshotOverlay_ItemFreshEmitReplays: the fixture holds a fan-out's
// fresh item snapshot that the recording wrote as an overlay, and an emit
// overlay; its replay runs no diff and moves through the same bases as the
// recorded results, item-fresh writes included.
func TestSnapshotOverlay_ItemFreshEmitReplays(t *testing.T) {
	recorded, runID, _ := loadSnapshotOverlayFixture(t)
	overlays := map[string]bool{}
	for _, w := range recordedSnapshotWrites(t, recorded) {
		if w.activity == engine.ActivityWriteSnapshotOrOverlay && !w.written.Full {
			overlays[w.written.URI] = true
		}
	}
	var itemFresh, emit int
	for _, event := range recorded.GetEvents() {
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs == nil || attrs.GetActivityType().GetName() != engine.ActivityPublishEvent {
			continue
		}
		var (
			published *orkstackv1.Event
			uri, key  string
		)
		if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &published, &uri, &key); err != nil {
			t.Fatal(err)
		}
		if !overlays[uri] {
			continue
		}
		if published.Item != nil {
			itemFresh++
		} else {
			emit++
		}
	}
	if itemFresh == 0 || emit == 0 {
		t.Fatalf("%d item-fresh and %d emit overlays published: the fixture does not hold both", itemFresh, emit)
	}
	bases, diffs, err := replayCapturingBases(t, recorded, runID)
	if err != nil || diffs != 0 {
		t.Fatalf("replay = %v with %d diffs", err, diffs)
	}
	if fmt.Sprint(bases) != fmt.Sprint(foldBases(recordedSnapshotWrites(t, recorded))) {
		t.Fatal("the replay's bases differ from the recorded results'")
	}
}

// TestSnapshotOverlayHistory_CatchesAChangedPinnedMinRows: the fixture with
// its plan's min_rows raised over the run's declared rows must fail to
// replay — the pinned floor, not a code constant, decides which activity a
// snapshot is written by.
func TestSnapshotOverlayHistory_CatchesAChangedPinnedMinRows(t *testing.T) {
	recorded, runID, _ := loadSnapshotOverlayFixture(t)
	attrs := recorded.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	plan := map[string]any{}
	if err := json.Unmarshal(attrs.GetInput().GetPayloads()[3].GetData(), &plan); err != nil {
		t.Fatal(err)
	}
	policy := plan["snapshot_overlay"].(map[string]any)
	policy["min_rows"] = float64(10_000)
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	attrs.GetInput().GetPayloads()[3] = &commonpb.Payload{Metadata: attrs.GetInput().GetPayloads()[3].GetMetadata(), Data: data}
	err = replayAs(t, recorded, runID)
	if err == nil {
		t.Fatal("the fixture replayed with a floor over its rows: it cannot see its pinned min_rows")
	}
	t.Logf("caught: %v", err)
}

// fixtureObjects is a snapshot store over the fixture's companion objects,
// found by the leaf of the URI the recording wrote them at.
type fixtureObjects struct {
	engine.SnapshotStore
	byLeaf map[string][]byte
}

func (f *fixtureObjects) ReadSnapshot(_ context.Context, uri string) ([]byte, error) {
	data, ok := f.byLeaf[path.Base(uri)]
	if !ok {
		return nil, fmt.Errorf("fixture object %s: %w", uri, artifacts.ErrNotFound)
	}
	return data, nil
}

func loadFixtureObjects(t *testing.T) *fixtureObjects {
	t.Helper()
	entries, err := os.ReadDir(snapshotOverlayObjectsDir)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixtureObjects{byLeaf: map[string][]byte{}}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(snapshotOverlayObjectsDir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		f.byLeaf[entry.Name()] = data
	}
	return f
}

// withoutTimes clears every google.protobuf.Timestamp in m, recursively: the
// re-execution runs on the test environment's clock, not the recording's.
func withoutTimes(m proto.Message) proto.Message {
	clone := proto.Clone(m)
	var clear func(protoreflect.Message)
	clear = func(msg protoreflect.Message) {
		msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			switch {
			case fd.Message() != nil && fd.Message().FullName() == (&timestamppb.Timestamp{}).ProtoReflect().Descriptor().FullName():
				msg.Clear(fd)
			case fd.IsList() && fd.Message() != nil:
				for i := 0; i < v.List().Len(); i++ {
					clear(v.List().Get(i).Message())
				}
			case fd.Message() != nil && !fd.IsMap():
				clear(v.Message())
			}
			return true
		})
	}
	clear(clone.ProtoReflect())
	return clone
}

// TestSnapshotOverlayHistory_PayloadsMatchWhatCurrentCodeSends is the payload
// half: the run, re-executed from its recorded arguments with its recorded
// results and snapshot writes fed back, sends every recorded ExecuteTask
// input and publishes every recorded event; every overlay write is handed
// the base the recorded results make it and the plan's policy; and every
// companion object the recording wrote — read as a reader reads it, an
// overlay through its recorded base — is exactly the run current code hands
// that write, times aside.
func TestSnapshotOverlayHistory_PayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	recorded, runID, plan := loadSnapshotOverlayFixture(t)
	id, wf, input, _, _ := startArgs(t, recorded)
	inputs, results := recordedItemPayloads(t, recorded)
	writes := recordedSnapshotWrites(t, recorded)
	objects := loadFixtureObjects(t)

	type call struct {
		activity string
		seq      int64
		run      *orkstackv1.Run
		base     engine.SnapshotBase
		policy   engine.OverlayPolicy
	}
	var (
		mu      sync.Mutex
		calls   []call
		sent    = map[string]map[string]any{}
		events  = map[int64]map[string]any{}
		failure error
	)
	next := func(c call) engine.SnapshotWritten {
		mu.Lock()
		defer mu.Unlock()
		i := len(calls)
		calls = append(calls, c)
		if i >= len(writes) || writes[i].activity != c.activity {
			failure = errors.Join(failure, fmt.Errorf("write %d is %s; the recording's is not", i, c.activity))
			return engine.SnapshotWritten{URI: "memory://none", Full: true}
		}
		return writes[i].written
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(startedAt(recorded))
	env.SetTestTimeout(60 * time.Second)
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: runID})
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	env.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		defer mu.Unlock()
		key := scheduleKey(in)
		sent[key] = canonicalPayload(t, in)
		if result, ok := results[key]; ok {
			return result, nil
		}
		return nil, fmt.Errorf("current code ran %q, which the recording never did", key)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	env.RegisterActivityWithOptions(func(_ context.Context, run *orkstackv1.Run, seq int64, base engine.SnapshotBase,
		policy engine.OverlayPolicy,
	) (engine.SnapshotWritten, error) {
		return next(call{engine.ActivityWriteSnapshotOrOverlay, seq, proto.Clone(run).(*orkstackv1.Run), base, policy}), nil
	}, activity.RegisterOptions{Name: engine.ActivityWriteSnapshotOrOverlay})
	env.RegisterActivityWithOptions(func(_ context.Context, run *orkstackv1.Run, seq int64) (string, error) {
		return next(call{engine.ActivityWriteSnapshot, seq, proto.Clone(run).(*orkstackv1.Run), engine.SnapshotBase{},
			engine.OverlayPolicy{}}).URI, nil
	}, activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	env.RegisterActivityWithOptions(func(_ context.Context, event *orkstackv1.Event, _, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		events[event.GetSequence()] = canonicalPayload(t, event)
		return nil
	}, activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	env.ExecuteWorkflow(engine.RunWorkflowType, id, wf, input, plan, (*orkstackv1.RunContinuation)(nil))
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("the re-execution ended %v", env.GetWorkflowError())
	}
	if failure != nil {
		t.Fatal(failure)
	}
	compareAll(t, inputs, sent, publishedEvents(t, recorded, 0), events)
	if len(calls) != len(writes) {
		t.Fatalf("current code wrote %d snapshots, the recording %d", len(calls), len(writes))
	}
	bases := foldBases(writes)
	overlayIndex := 0
	var previous engine.SnapshotBase
	for i, c := range calls {
		if c.activity == engine.ActivityWriteSnapshotOrOverlay {
			if c.base != previous || c.policy != *plan.SnapshotOverlay {
				t.Fatalf("write %d was handed base %+v and policy %+v; want %+v and the plan's %+v",
					i, c.base, c.policy, previous, *plan.SnapshotOverlay)
			}
			previous = bases[overlayIndex]
			overlayIndex++
		}
		got, err := engine.MaterializeSnapshotForTest(context.Background(), objects, writes[i].written.URI)
		if err != nil {
			t.Fatalf("write %d: the recorded %s does not read: %v", i, path.Base(writes[i].written.URI), err)
		}
		if !proto.Equal(withoutTimes(got), withoutTimes(c.run)) {
			t.Fatalf("write %d (event %d): the recorded %s is not the run current code hands that write",
				i, c.seq, path.Base(writes[i].written.URI))
		}
	}
}

// TestSnapshotOverlay_FirstEmitOfAGenerationIsFull: a run that carries on
// across generations starts each generation's overlay chain afresh — the
// first WriteSnapshotOrOverlay of every generation is handed no base, so it
// writes a full snapshot; snapBase never crosses a switch.
func TestSnapshotOverlay_FirstEmitOfAGenerationIsFull(t *testing.T) {
	defer engine.ContinueAtEveryIdleForTest()()
	wf := compile(t, `
name: carried-overlay
inputs:
  regions: {type: array}
tasks:
  - id: prep
    uses: shell
    with: {op: exec}
  - id: fan
    uses: shell
    needs: [prep]
    each: {over: "{{ .inputs.regions }}", max_items: 260}
    with: {op: exec}
  - id: merge
    uses: shell
    needs: [fan]
    with: {op: exec}
`)
	plan := defaultPlan()
	plan.History = &engine.HistoryLimit{MaxEvents: engine.DefaultHistoryMaxEvents, MaxBytes: engine.DefaultHistoryMaxBytes,
		Reserve: engine.DefaultHistoryReserve}
	budget := engine.DefaultContinuationBudget(1, 1<<40, time.Hour)
	plan.Continuation = &budget
	policy := engine.DefaultOverlayPolicy()
	plan.SnapshotOverlay = &policy
	regions := make([]any, 260)
	for i := range regions {
		regions[i] = fmt.Sprintf("r%d", i)
	}
	g := runGenerations(t, "ork-carried-overlay", wf, payload(t, map[string]any{"regions": regions}), plan, okBehavior, nil)
	if g.err != nil || len(g.carries) < 2 {
		t.Fatalf("the run ended %v after %d switches; want success across at least two", g.err, len(g.carries))
	}
	for i, h := range g.harnesses {
		if len(h.overlayBases) == 0 {
			t.Fatalf("generation %d wrote no snapshot through the overlay activity", i)
		}
		if h.overlayBases[0] != (engine.SnapshotBase{}) || !h.overlayWrites[0].Full {
			t.Fatalf("generation %d's first write was handed %+v and wrote %+v", i, h.overlayBases[0], h.overlayWrites[0])
		}
	}
}

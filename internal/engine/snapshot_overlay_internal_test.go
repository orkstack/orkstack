package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The snapshot-overlay unit evidence (T6): snapshot overlays, written by
// WriteSnapshotOrOverlay from both snapshot write sites, read back by
// materializeSnapshot.

// overlayWrite is one snapshot write a run made: which activity, for which
// event, the run as it stood (a clone), the base the workflow passed and
// what the activity reported.
type overlayWrite struct {
	activity string
	seq      int64
	run      *orkstackv1.Run
	base     SnapshotBase
	written  SnapshotWritten
}

// overlayHarness runs RunWorkflow in the test environment with the real
// snapshot activities over an in-memory store whose URIs are the real
// stores' (s3://test/…), recording every write and every publish.
type overlayHarness struct {
	t          *testing.T
	env        *testsuite.TestWorkflowEnvironment
	store      *memSnapshotStore
	activities *SnapshotActivities

	mu     sync.Mutex
	writes []overlayWrite
	events map[int64]*orkstackv1.Event
	uris   map[int64]string
	// failItem, when >= 0, is the fan-out item whose ExecuteTask fails, for
	// good.
	failItem int32
}

func newOverlayHarness(t *testing.T) *overlayHarness {
	t.Helper()
	h := &overlayHarness{t: t, store: newMemSnapshotStore(), events: map[int64]*orkstackv1.Event{},
		uris: map[int64]string{}, failItem: -1}
	h.activities = NewSnapshotActivities(h.store)
	var suite testsuite.WorkflowTestSuite
	h.env = suite.NewTestWorkflowEnvironment()
	h.env.SetTestTimeout(120 * time.Second)
	h.env.SetStartWorkflowOptions(startOptionsForTest())
	h.env.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	h.env.RegisterActivityWithOptions(func(ctx context.Context, run *orkstackv1.Run, seq int64, base SnapshotBase,
		policy OverlayPolicy,
	) (SnapshotWritten, error) {
		clone := proto.Clone(run).(*orkstackv1.Run)
		written, err := h.activities.WriteSnapshotOrOverlay(ctx, run, seq, base, policy)
		h.mu.Lock()
		h.writes = append(h.writes, overlayWrite{ActivityWriteSnapshotOrOverlay, seq, clone, base, written})
		h.mu.Unlock()
		return written, err
	}, activity.RegisterOptions{Name: ActivityWriteSnapshotOrOverlay})
	h.env.RegisterActivityWithOptions(func(ctx context.Context, run *orkstackv1.Run, seq int64) (string, error) {
		clone := proto.Clone(run).(*orkstackv1.Run)
		uri, err := h.activities.WriteSnapshot(ctx, run, seq)
		h.mu.Lock()
		h.writes = append(h.writes, overlayWrite{ActivityWriteSnapshot, seq, clone, SnapshotBase{},
			SnapshotWritten{URI: uri, Full: true}})
		h.mu.Unlock()
		return uri, err
	}, activity.RegisterOptions{Name: ActivityWriteSnapshot})
	h.env.RegisterActivityWithOptions(func(_ context.Context, event *orkstackv1.Event, uri, _ string) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.events[event.GetSequence()] = event
		h.uris[event.GetSequence()] = uri
		return nil
	}, activity.RegisterOptions{Name: ActivityPublishEvent})
	h.env.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetEach() != nil && in.GetEach().GetIndex() == h.failItem {
			return nil, temporal.NewNonRetryableApplicationError("item failed", "EXIT_NONZERO", nil)
		}
		return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "orker-1",
			ValuesJson: []byte(fmt.Sprintf(`{"task":%q}`, in.GetTaskId()))}, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	return h
}

// overlayDocument is a fan-out of items (max_parallel parallel) between a
// task before and one after: 2 + 1 + items rows.
func overlayDocument(items, parallel int) string {
	return fmt.Sprintf(`
apiVersion: orkstack/v1
kind: Orkflow
name: overlay
inputs:
  regions: {type: array}
tasks:
  - id: prep
    uses: shell
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [prep]
    each: {over: "{{ .inputs.regions }}", max_items: %d, max_parallel: %d}
    with: {op: exec, r: "{{ .each.item }}"}
  - id: merge
    uses: shell
    needs: [convert]
    with: {op: exec}
`, items, parallel)
}

// overlayPlan is a routable plan carrying policy.
func overlayPlan(t *testing.T, policy *OverlayPolicy) RunPlan {
	plan := guardPlan(t, 1_000_000, 1<<40)
	plan.SnapshotOverlay = policy
	return plan
}

func defaultPolicy() *OverlayPolicy {
	p := DefaultOverlayPolicy()
	return &p
}

// execute runs the definition to its end and returns the workflow's error.
func (h *overlayHarness) execute(wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan RunPlan) error {
	h.t.Helper()
	h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, wf, input, plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		h.t.Fatal("the run did not complete")
	}
	return h.env.GetWorkflowError()
}

// runOverlay runs items items of overlayDocument under plan and fails the
// test unless the run ends as wantErr says.
func runOverlay(t *testing.T, items, parallel int, plan RunPlan, failItem int32) *overlayHarness {
	t.Helper()
	h := newOverlayHarness(t)
	h.failItem = failItem
	err := h.execute(guardCompile(t, overlayDocument(items, parallel)),
		guardInput(t, map[string]any{"regions": itemsArray(items)}), plan)
	if (failItem < 0) != (err == nil) {
		t.Fatalf("the run ended %v with item %d failing", err, failItem)
	}
	return h
}

// checkMaterializes: every snapshot the run wrote, read back UNCACHED (two
// GETs for an overlay), is exactly the run it was written for.
func (h *overlayHarness) checkMaterializes(t *testing.T) (overlays, fulls int) {
	t.Helper()
	for _, w := range h.writes {
		got, err := materializeSnapshot(context.Background(), h.store, nil, w.written.URI)
		if err != nil {
			t.Fatalf("event %d: read back %s: %v", w.seq, w.written.URI, err)
		}
		if !proto.Equal(got, w.run) {
			t.Fatalf("event %d: %s does not materialize to the run it was written for", w.seq, w.written.URI)
		}
		if w.written.Full {
			fulls++
		} else {
			overlays++
		}
	}
	return overlays, fulls
}

// TestSnapshotOverlay_MaterializesToTheFullRun: a property over every
// snapshot write. Every object either write site produced — emit's, and a
// fan-out's fresh item snapshots — reads back as exactly the run it was
// written for: a fan-out that succeeds; one that fails with items still
// pending (collectItem's failure, then finishFanOutOnRunEnd canceling the
// rest before the run's summary); and, at the activity, rows changed in
// place the way attributeLostPinHost and hintPinSessionHost change them,
// plus random field changes on random rows. Change detection is by diff, so
// no mutation site has to mark anything.
func TestSnapshotOverlay_MaterializesToTheFullRun(t *testing.T) {
	t.Run("a fan-out that succeeds", func(t *testing.T) {
		h := runOverlay(t, 600, 40, overlayPlan(t, defaultPolicy()), -1)
		overlays, fulls := h.checkMaterializes(t)
		if overlays == 0 || fulls < 2 {
			t.Fatalf("%d overlays and %d full snapshots: the property was not exercised", overlays, fulls)
		}
	})
	t.Run("a fan-out failed with items pending", func(t *testing.T) {
		h := runOverlay(t, 600, 20, overlayPlan(t, defaultPolicy()), 100)
		overlays, _ := h.checkMaterializes(t)
		last := h.writes[len(h.writes)-1].run
		canceled := 0
		for _, item := range last.GetTaskRuns()[1].GetItems() {
			if item.GetStatus() == orkstackv1.Status_STATUS_CANCELED {
				canceled++
			}
		}
		if overlays == 0 || canceled == 0 {
			t.Fatalf("%d overlays, %d items canceled at the run's end: not the shape under test", overlays, canceled)
		}
	})
	t.Run("rows changed in place by the pin and random mutations", func(t *testing.T) {
		checkInPlaceMutations(t)
	})
}

// checkInPlaceMutations drives WriteSnapshotOrOverlay directly over a run
// state whose rows are changed IN PLACE between writes — the pointers
// snapshot() hands the activity are the workflow's own, which a base cache
// must never alias — by attributeLostPinHost, by a note appended to an
// error's details as hintPinSessionHost appends one, and by random field
// changes.
func checkInPlaceMutations(t *testing.T) {
	wf := guardCompile(t, `
apiVersion: orkstack/v1
kind: Orkflow
name: pinned
inputs:
  regions: {type: array}
pins:
  build: {}
tasks:
  - id: a
    uses: shell
    pin: build
    with: {op: exec}
  - id: b
    uses: shell
    pin: build
    needs: [a]
    with: {op: exec}
  - id: fan
    uses: shell
    each: {over: "{{ .inputs.regions }}", max_items: 300}
    with: {op: exec}
`)
	s := buildRunState("ork-overlay-pins", wf, nil, overlayPlan(t, defaultPolicy()))
	s.createdAt = timestamppb.New(time.Unix(1_700_000_000, 0))
	expandForTest(s, "fan", 300)
	store := newMemSnapshotStore()
	a := NewSnapshotActivities(store)
	policy := DefaultOverlayPolicy()
	r := rand.New(rand.NewSource(6))
	base := SnapshotBase{}
	write := func(step string, seq int64) {
		t.Helper()
		want := proto.Clone(s.snapshot()).(*orkstackv1.Run)
		written, err := a.WriteSnapshotOrOverlay(context.Background(), s.snapshot(), seq, base, policy)
		if err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		got, err := materializeSnapshot(context.Background(), store, nil, written.URI)
		if err != nil || !proto.Equal(got, want) {
			t.Fatalf("%s: event %d (%s) does not read back as the run (%v)", step, seq, written.URI, err)
		}
		if written.Full {
			base = SnapshotBase{URI: written.URI, SHA256: written.SHA256}
		} else {
			base.Overlays++
		}
	}
	seq := int64(1)
	write("first", seq)
	// The pin's first member reported its Orker; the second was lost with
	// the host: attributeLostPinHost fills its empty orker_id in place.
	s.taskRuns["a"].OrkerId = "orker-7"
	s.taskRuns["a"].Status = orkstackv1.Status_STATUS_SUCCEEDED
	seq++
	write("a succeeded", seq)
	s.taskRuns["b"].Status = orkstackv1.Status_STATUS_FAILED
	s.taskRuns["b"].Error = &orkstackv1.Error{Code: "PIN_HOST_LOST", Message: "host lost"}
	p := s.pins["build"]
	p.lost = true
	s.attributeLostPinHost(p)
	if s.taskRuns["b"].GetOrkerId() != "orker-7" {
		t.Fatalf("attributeLostPinHost did not fill b's orker_id: %q", s.taskRuns["b"].GetOrkerId())
	}
	seq++
	write("attributeLostPinHost", seq)
	// hintPinSessionHost's change: a note appended to an existing error's
	// details, in place.
	s.taskRuns["b"].Error.Details += "; " + pinSessionHostLabel + "host-9"
	seq++
	write("hintPinSessionHost", seq)
	items := s.taskRuns["fan"].Items
	for step := 0; step < 300; step++ {
		item := items[r.Intn(len(items))]
		switch r.Intn(4) {
		case 0:
			item.Status = orkstackv1.Status_STATUS_RUNNING
			item.Attempt++
		case 1:
			item.Status = orkstackv1.Status_STATUS_SUCCEEDED
			item.OrkerId = fmt.Sprintf("orker-%d", r.Intn(5))
			item.FinishedAt = timestamppb.New(time.Unix(1_700_000_000+int64(step), 0))
		case 2:
			item.Error = &orkstackv1.Error{Code: "EXIT_NONZERO", Message: fmt.Sprintf("exit %d", r.Intn(9))}
		case 3:
			s.taskRuns["fan"].Attempt = int32(r.Intn(3))
		}
		if r.Intn(25) == 0 {
			// Many rows at once: past MaxRows, the write must be full.
			for _, other := range items {
				other.ExitCode = int32(step)
			}
		}
		seq++
		write(fmt.Sprintf("random step %d", step), seq)
	}
}

// TestSnapshotOverlay_BothWriteSitesUseOneActivity: in a run with a 600-item
// fan-out, every non-terminal snapshot — emit's and a fan-out's fresh item
// snapshots alike — is written by WriteSnapshotOrOverlay; the base each call
// is handed is exactly what the previous call's recorded result makes it;
// and an overlay written right after an item-fresh full snapshot names that
// snapshot as its base. A MaxChain of 3 makes full snapshots frequent at
// both sites.
func TestSnapshotOverlay_BothWriteSitesUseOneActivity(t *testing.T) {
	policy := DefaultOverlayPolicy()
	policy.MaxChain = 3
	h := runOverlay(t, 600, 40, overlayPlan(t, &policy), -1)
	var (
		expected     SnapshotBase
		itemFresh    int
		namedItemful bool
		lastFullItem string
	)
	for i, w := range h.writes {
		event := h.events[w.seq]
		if event == nil {
			t.Fatalf("write for event %d: no event published", w.seq)
		}
		if isTerminalRunEventType(event.GetType()) {
			if w.activity != ActivityWriteSnapshot {
				t.Fatalf("terminal event %d written by %s", w.seq, w.activity)
			}
			continue
		}
		if w.activity != ActivityWriteSnapshotOrOverlay {
			t.Fatalf("non-terminal event %d (%s, item %v) written by %s", w.seq, event.GetType(), event.Item, w.activity)
		}
		if w.base != expected {
			t.Fatalf("write %d was handed base %+v, the recorded results make it %+v", i, w.base, expected)
		}
		if !w.written.Full && lastFullItem != "" && w.base.URI == lastFullItem {
			overlay := decodeOverlayForTest(t, h.store, w.written.URI)
			if overlay.GetBaseUri() != lastFullItem {
				t.Fatalf("the overlay after an item-fresh full snapshot names %s, want %s", overlay.GetBaseUri(), lastFullItem)
			}
			namedItemful = true
		}
		if event.Item != nil {
			itemFresh++
			if w.written.Full {
				lastFullItem = w.written.URI
			}
		}
		if w.written.Full {
			expected = SnapshotBase{URI: w.written.URI, SHA256: w.written.SHA256}
		} else {
			expected.Overlays++
		}
	}
	if itemFresh == 0 || !namedItemful {
		t.Fatalf("%d item-fresh writes, an overlay named an item-fresh base: %v", itemFresh, namedItemful)
	}
}

func decodeOverlayForTest(t *testing.T, store *memSnapshotStore, uri string) *orkstackv1.RunSnapshotOverlay {
	t.Helper()
	data, ok := store.get(uri)
	if !ok {
		t.Fatalf("no object at %s", uri)
	}
	overlay := &orkstackv1.RunSnapshotOverlay{}
	if err := proto.Unmarshal(data, overlay); err != nil {
		t.Fatal(err)
	}
	return overlay
}

// TestSnapshotOverlay_BaseIsAlwaysAFullSnapshot: no overlay a run wrote names
// another overlay; every one names a {seq}.pb of the same run.
func TestSnapshotOverlay_BaseIsAlwaysAFullSnapshot(t *testing.T) {
	h := runOverlay(t, 600, 40, overlayPlan(t, defaultPolicy()), -1)
	seen := 0
	for _, w := range h.writes {
		if w.written.Full {
			continue
		}
		overlay := decodeOverlayForTest(t, h.store, w.written.URI)
		if strings.HasSuffix(overlay.GetBaseUri(), ".ovl") ||
			!artifacts.OverlayBaseURIMatches(overlay.GetBaseUri(), "default", refusalWorkflowID) {
			t.Fatalf("overlay %s names %s", w.written.URI, overlay.GetBaseUri())
		}
		seen++
	}
	if seen == 0 {
		t.Fatal("the run wrote no overlay")
	}
}

// TestSnapshotOverlay_TerminalEventAndSwitchSnapshotAreAlwaysFull: a run's
// terminal event is written by WriteSnapshot — a full {seq}.pb, never asked
// of the overlay activity — whether it succeeds, fails or is canceled; and
// the switch snapshot, the checkpoint and a refused generation's terminal
// snapshot have activities of their own, never this one.
func TestSnapshotOverlay_TerminalEventAndSwitchSnapshotAreAlwaysFull(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failItem int32
		cancel   bool
	}{{"succeeded", -1, false}, {"failed", 100, false}, {"canceled", -1, true}} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOverlayHarness(t)
			h.failItem = tc.failItem
			if tc.cancel {
				h.env.RegisterDelayedCallback(h.env.CancelWorkflow, time.Second)
			}
			_ = h.execute(guardCompile(t, overlayDocument(600, 20)),
				guardInput(t, map[string]any{"regions": itemsArray(600)}), overlayPlan(t, defaultPolicy()))
			var terminal *overlayWrite
			for i := range h.writes {
				if event := h.events[h.writes[i].seq]; event != nil && isTerminalRunEventType(event.GetType()) {
					terminal = &h.writes[i]
				}
			}
			if terminal == nil || terminal.activity != ActivityWriteSnapshot || !strings.HasSuffix(terminal.written.URI, ".pb") {
				t.Fatalf("terminal write %+v, want WriteSnapshot of a {seq}.pb", terminal)
			}
		})
	}
	for _, name := range []string{ActivityWriteSwitchSnapshot, ActivityWriteCheckpoint, ActivityWriteFailedRunSnapshot} {
		if name == ActivityWriteSnapshotOrOverlay || name == ActivityWriteSnapshot {
			t.Fatalf("%s shares an event snapshot's activity", name)
		}
	}
}

// TestSnapshotOverlay_ThresholdsComeFromThePlan: MinRows, MaxChain and
// MaxRows are the plan's, never a code constant: the same 300-row run writes
// overlays under a floor of 100 and none under 400; a MaxChain of 2 forces a
// full snapshot after every two overlays; a MaxRows of 0 forces a full one
// whenever any row changed, so an overlay then holds the header alone.
func TestSnapshotOverlay_ThresholdsComeFromThePlan(t *testing.T) {
	count := func(h *overlayHarness) (overlays, fulls, longest int) {
		run := 0
		for _, w := range h.writes {
			if w.activity != ActivityWriteSnapshotOrOverlay {
				continue
			}
			if w.written.Full {
				fulls++
				run = 0
				continue
			}
			overlays++
			run++
			longest = max(longest, run)
		}
		return
	}
	floor := func(min int64) *OverlayPolicy {
		p := DefaultOverlayPolicy()
		p.MinRows = min
		return &p
	}
	if o, _, _ := count(runOverlay(t, 297, 40, overlayPlan(t, floor(100)), -1)); o == 0 {
		t.Fatal("a 300-row run under a floor of 100 wrote no overlay")
	}
	if h := runOverlay(t, 297, 40, overlayPlan(t, floor(400)), -1); len(h.writes) == 0 || func() bool {
		for _, w := range h.writes {
			if w.activity != ActivityWriteSnapshot {
				return true
			}
		}
		return false
	}() {
		t.Fatal("a 300-row run under a floor of 400 used the overlay activity")
	}
	chain := DefaultOverlayPolicy()
	chain.MaxChain = 2
	if _, _, longest := count(runOverlay(t, 297, 40, overlayPlan(t, &chain), -1)); longest != 2 {
		t.Fatalf("the longest run of overlays is %d under a MaxChain of 2", longest)
	}
	rows := DefaultOverlayPolicy()
	rows.MaxRows = 0
	h := runOverlay(t, 297, 40, overlayPlan(t, &rows), -1)
	if _, f, _ := count(h); f < 3 {
		t.Fatalf("%d full snapshots under a MaxRows of 0", f)
	}
	for _, w := range h.writes {
		if overlay := (*orkstackv1.RunSnapshotOverlay)(nil); !w.written.Full {
			overlay = decodeOverlayForTest(t, h.store, w.written.URI)
			if len(overlay.GetTaskRuns())+len(overlay.GetItems()) != 0 {
				t.Fatalf("an overlay under a MaxRows of 0 holds %d rows", len(overlay.GetTaskRuns())+len(overlay.GetItems()))
			}
		}
	}
}

// TestSnapshotOverlay_SmallRunUnderMinRowsWritesFull: a run whose own rows
// are at or under the floor writes every snapshot in full and never asks the
// gate, even with the policy on its plan — which is what a small child of a
// large parent has, since startChild copies the parent's policy.
func TestSnapshotOverlay_SmallRunUnderMinRowsWritesFull(t *testing.T) {
	// 3 + 253 = 256 rows: exactly the floor.
	h := newOverlayHarness(t)
	asks := 0
	h.env.OnGetVersion(SnapshotOverlayChange, workflow.DefaultVersion, 1).Return(
		func(string, workflow.Version, workflow.Version) workflow.Version {
			asks++
			return 1
		})
	if err := h.execute(guardCompile(t, overlayDocument(253, 40)),
		guardInput(t, map[string]any{"regions": itemsArray(253)}), overlayPlan(t, defaultPolicy())); err != nil {
		t.Fatal(err)
	}
	for _, w := range h.writes {
		if w.activity != ActivityWriteSnapshot {
			t.Fatalf("event %d of a 256-row run written by %s", w.seq, w.activity)
		}
	}
	if asks != 0 {
		t.Fatalf("a run under the floor asked the gate %d times", asks)
	}
}

// TestSnapshotOverlay_GateIsLazy: the snapshot-overlay version is asked only
// by a run that uses overlays, at its first non-terminal snapshot, once;
// never by a small run or a run with no policy. A history whose gate
// answered DefaultVersion writes every snapshot in full.
func TestSnapshotOverlay_GateIsLazy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		items  int
		policy *OverlayPolicy
		answer workflow.Version
		asks   int
	}{
		{"a large run with the policy", 600, defaultPolicy(), 1, 1},
		{"a small run with the policy", 100, defaultPolicy(), 1, 0},
		{"a large run with no policy", 600, nil, 1, 0},
		{"a recorded history that answered DefaultVersion", 600, defaultPolicy(), workflow.DefaultVersion, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOverlayHarness(t)
			asks := 0
			h.env.OnGetVersion(SnapshotOverlayChange, workflow.DefaultVersion, 1).Return(
				func(string, workflow.Version, workflow.Version) workflow.Version {
					asks++
					return tc.answer
				})
			if err := h.execute(guardCompile(t, overlayDocument(tc.items, 40)),
				guardInput(t, map[string]any{"regions": itemsArray(tc.items)}), overlayPlan(t, tc.policy)); err != nil {
				t.Fatal(err)
			}
			if asks != tc.asks {
				t.Fatalf("the gate was asked %d times, want %d", asks, tc.asks)
			}
			first := h.writes[0]
			for _, w := range h.writes {
				usedOverlay := w.activity == ActivityWriteSnapshotOrOverlay
				if usedOverlay != (tc.asks > 0 && tc.answer >= 1 && !isTerminalRunEventType(h.events[w.seq].GetType())) {
					t.Fatalf("event %d written by %s", w.seq, w.activity)
				}
			}
			if tc.asks > 0 && tc.answer >= 1 && first.activity != ActivityWriteSnapshotOrOverlay {
				t.Fatalf("the first snapshot was written by %s; the gate is asked there", first.activity)
			}
		})
	}
}

// TestWriteSnapshotOrOverlay_FirstWriteOfARunIsFull: a run's first snapshot
// has no base and is full; snapBase is ephemeral, so a carried generation
// starts from nothing too (TestSnapshotOverlay_FirstEmitOfAGenerationIsFull
// runs one); and the activity never writes an overlay without a base.
func TestWriteSnapshotOrOverlay_FirstWriteOfARunIsFull(t *testing.T) {
	h := runOverlay(t, 600, 40, overlayPlan(t, defaultPolicy()), -1)
	if first := h.writes[0]; first.base != (SnapshotBase{}) || !first.written.Full ||
		first.activity != ActivityWriteSnapshotOrOverlay {
		t.Fatalf("the first write was %+v", first)
	}
	if runStateFieldClass["runState.snapBase"] != fieldEphemeral {
		t.Fatal("snapBase is not ephemeral: a carried generation would name its predecessor's base")
	}
	a := NewSnapshotActivities(newMemSnapshotStore())
	run := &orkstackv1.Run{Id: "ork-x", Namespace: "default"}
	written, err := a.WriteSnapshotOrOverlay(context.Background(), run, 7, SnapshotBase{}, DefaultOverlayPolicy())
	if err != nil || !written.Full || written.SHA256 == "" {
		t.Fatalf("a write with no base = %+v, %v; want a full snapshot with its digest", written, err)
	}
}

// TestSnapshotOverlay_ChildRunsUseTheCopiedPolicy: startChild copies the
// run-tree policy into the child's start plan, like the other run-tree
// limits, and a recorded parent with none starts its child with none.
func TestSnapshotOverlay_ChildRunsUseTheCopiedPolicy(t *testing.T) {
	for _, policy := range []*OverlayPolicy{defaultPolicy(), nil} {
		plan := childStartPlanForTest(t, policy)
		if (plan.SnapshotOverlay == nil) != (policy == nil) || (policy != nil && *plan.SnapshotOverlay != *policy) {
			t.Fatalf("parent policy %+v: child start plan carries %+v", policy, plan.SnapshotOverlay)
		}
	}
}

// ---------------------------------------------------------------- readers

// overlayFixture writes a full base for a run of rows rows and an overlay on
// it, and returns the store, the overlay's URI and the run it stands for.
func overlayFixture(t *testing.T, rows int) (*memSnapshotStore, *SnapshotActivities, string, *orkstackv1.Run, SnapshotWritten) {
	t.Helper()
	store := newMemSnapshotStore()
	a := NewSnapshotActivities(store)
	run := &orkstackv1.Run{Id: "ork-reader", Namespace: "default", Status: orkstackv1.Status_STATUS_RUNNING}
	for i := 0; i < rows; i++ {
		run.TaskRuns = append(run.TaskRuns, &orkstackv1.TaskRun{TaskId: fmt.Sprintf("t%d", i), Status: orkstackv1.Status_STATUS_PENDING})
	}
	full, err := a.WriteSnapshotOrOverlay(context.Background(), run, 1, SnapshotBase{}, DefaultOverlayPolicy())
	if err != nil || !full.Full {
		t.Fatalf("base: %+v, %v", full, err)
	}
	run.TaskRuns[3].Status = orkstackv1.Status_STATUS_SUCCEEDED
	written, err := a.WriteSnapshotOrOverlay(context.Background(), run, 2,
		SnapshotBase{URI: full.URI, SHA256: full.SHA256}, DefaultOverlayPolicy())
	if err != nil || written.Full {
		t.Fatalf("overlay: %+v, %v", written, err)
	}
	return store, a, written.URI, run, full
}

// TestMaterializeSnapshot_ReadsAtMostTwoObjects: an uncached overlay read is
// the overlay and its base, nothing else; a cached one is the overlay alone.
func TestMaterializeSnapshot_ReadsAtMostTwoObjects(t *testing.T) {
	store, _, uri, want, _ := overlayFixture(t, 400)
	before := store.readCount()
	got, err := materializeSnapshot(context.Background(), store, nil, uri)
	if err != nil || !proto.Equal(got, want) {
		t.Fatalf("read = %v", err)
	}
	if reads := store.readCount() - before; reads != 2 {
		t.Fatalf("an uncached overlay read took %d reads, want 2", reads)
	}
	cache := NewSnapshotBaseCache()
	for i, want := range []int{2, 1} {
		before = store.readCount()
		if _, err := materializeSnapshot(context.Background(), store, cache, uri); err != nil {
			t.Fatal(err)
		}
		if reads := store.readCount() - before; reads != want {
			t.Fatalf("read %d through a cache took %d reads, want %d", i, reads, want)
		}
	}
}

// TestMaterializeSnapshot_RefusesABaseOfAnotherRun: an overlay naming
// another run's snapshot — or an overlay, or a continuation object — is
// refused before the base is read, with an error that is not NotFound.
func TestMaterializeSnapshot_RefusesABaseOfAnotherRun(t *testing.T) {
	store, _, uri, _, _ := overlayFixture(t, 300)
	other := &orkstackv1.Run{Id: "ork-other", Namespace: "default"}
	otherURI, err := store.WriteSnapshot(context.Background(), "default", "ork-other", 1, mustMarshal(t, other))
	if err != nil {
		t.Fatal(err)
	}
	for name, base := range map[string]string{
		"another run's snapshot": otherURI,
		"an overlay":             strings.TrimSuffix(otherURI, ".pb") + ".ovl",
		"another bucket":         strings.Replace(otherURI, "s3://test/", "s3://elsewhere/", 1),
	} {
		t.Run(name, func(t *testing.T) {
			overlay := decodeOverlayForTest(t, store, uri)
			overlay.BaseUri = strings.ReplaceAll(base, "ork-other", "ork-reader")
			if name == "another run's snapshot" {
				overlay.BaseUri = otherURI
			}
			store.put(uri, mustMarshal(t, overlay))
			before := store.readCount()
			_, err := materializeSnapshot(context.Background(), store, nil, uri)
			if !errors.Is(err, ErrSnapshotOverlayInvalid) || errors.Is(err, artifacts.ErrNotFound) {
				t.Fatalf("err = %v, want ErrSnapshotOverlayInvalid and not NotFound", err)
			}
			if reads := store.readCount() - before; reads != 1 {
				t.Fatalf("%d reads: the base was read before it was checked", reads)
			}
		})
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestMaterializeSnapshot_LRUBounds: the cache keeps at most 8 bases and at
// most 128 MiB of their bytes, least recently used out first, keyed by
// (URI, SHA-256): the same URI with other bytes is another entry.
func TestMaterializeSnapshot_LRUBounds(t *testing.T) {
	cache := NewSnapshotBaseCache()
	if cache.maxEntries != 8 || cache.maxBytes != 128<<20 {
		t.Fatalf("bounds %d entries, %d bytes; want 8 and 128 MiB", cache.maxEntries, cache.maxBytes)
	}
	small := func(i int) (string, string, []byte) {
		data := mustMarshal(t, &orkstackv1.Run{Id: fmt.Sprintf("ork-%d", i)})
		return fmt.Sprintf("s3://test/b/%d.pb", i), artifacts.ContinuationSHA256(data), data
	}
	for i := 0; i < 10; i++ {
		uri, sum, data := small(i)
		cache.put(uri, sum, data)
		if i == 2 {
			// Touch entry 0, so 1 is the least recently used when the ninth
			// arrives.
			u, s, _ := small(0)
			if cache.get(u, s) == nil {
				t.Fatal("entry 0 is gone early")
			}
		}
	}
	if cache.len() != 8 {
		t.Fatalf("%d entries, want 8", cache.len())
	}
	for i, want := range map[int]bool{0: true, 1: false, 2: false, 3: true, 9: true} {
		u, s, _ := small(i)
		if (cache.get(u, s) != nil) != want {
			t.Fatalf("entry %d cached = %v, want %v", i, !want, want)
		}
	}
	u, _, _ := small(9)
	if cache.get(u, strings.Repeat("0", 64)) != nil {
		t.Fatal("a URI with another digest hit")
	}
	bytesCache := newSnapshotBaseCache(8, 1000)
	big := mustMarshal(t, &orkstackv1.Run{Id: "ork-big", Input: &orkstackv1.Payload{}, Error: &orkstackv1.Error{Message: strings.Repeat("x", 400)}})
	for i := 0; i < 3; i++ {
		bytesCache.put(fmt.Sprintf("s3://test/big/%d.pb", i), artifacts.ContinuationSHA256(big), big)
	}
	if bytesCache.size() > 1000 || bytesCache.len() != 1000/len(big) {
		t.Fatalf("%d bytes in %d entries of %d, want at most 1000", bytesCache.size(), bytesCache.len(), len(big))
	}
	huge := mustMarshal(t, &orkstackv1.Run{Id: "ork-huge", Error: &orkstackv1.Error{Message: strings.Repeat("x", 2000)}})
	if got := bytesCache.put("s3://test/huge/1.pb", artifacts.ContinuationSHA256(huge), huge); got == nil || bytesCache.get("s3://test/huge/1.pb", artifacts.ContinuationSHA256(huge)) != nil {
		t.Fatal("a base over the byte bound was kept, or not returned")
	}
}

// TestMaterializeSnapshot_BaseRewrittenAfterTheOverlayIsRefusedNotDegraded: a
// retried emit rewrites {seq}.pb with the same state at a later time (spec
// 1.2). An uncached read of an overlay diffed against the old bytes then
// fails — not NotFound, never rows mixed from two bases — while a reader
// whose cache holds the verified old bytes still answers exactly.
func TestMaterializeSnapshot_BaseRewrittenAfterTheOverlayIsRefusedNotDegraded(t *testing.T) {
	store, _, uri, want, full := overlayFixture(t, 300)
	warm := NewSnapshotBaseCache()
	if _, err := materializeSnapshot(context.Background(), store, warm, uri); err != nil {
		t.Fatal(err)
	}
	old, _ := store.get(full.URI)
	rewritten := &orkstackv1.Run{}
	if err := proto.Unmarshal(old, rewritten); err != nil {
		t.Fatal(err)
	}
	rewritten.StartedAt = timestamppb.New(time.Unix(1_800_000_000, 0))
	store.put(full.URI, mustMarshal(t, rewritten))
	_, err := materializeSnapshot(context.Background(), store, nil, uri)
	if !errors.Is(err, ErrSnapshotOverlayInvalid) || errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("uncached read after the rewrite = %v, want ErrSnapshotOverlayInvalid, not NotFound", err)
	}
	got, err := materializeSnapshot(context.Background(), store, warm, uri)
	if err != nil || !proto.Equal(got, want) {
		t.Fatalf("a warm reader = %v; it holds the verified bytes the overlay was diffed against", err)
	}
	store.mu.Lock()
	delete(store.objects, full.URI)
	store.mu.Unlock()
	if _, err := materializeSnapshot(context.Background(), store, nil, uri); !errors.Is(err, ErrSnapshotOverlayInvalid) ||
		errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("a missing base = %v, want ErrSnapshotOverlayInvalid, not NotFound", err)
	}
	store.mu.Lock()
	delete(store.objects, uri)
	store.mu.Unlock()
	if _, err := materializeSnapshot(context.Background(), store, nil, uri); !errors.Is(err, ErrSnapshotOverlayInvalid) ||
		errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("a missing overlay = %v, want ErrSnapshotOverlayInvalid, not NotFound", err)
	}
}

// TestWriteSnapshotOrOverlay_WritesFullWhenTheBaseChanged: on a cache miss
// the writer reads the base, finds its bytes no longer hash to the digest
// the run recorded, and writes a full snapshot, reporting its own digest —
// a new chain from bytes it knows. A base gone, or one this cache does not
// hold that still verifies, behave as their names say.
func TestWriteSnapshotOrOverlay_WritesFullWhenTheBaseChanged(t *testing.T) {
	store, _, _, run, full := overlayFixture(t, 300)
	cold := NewSnapshotActivities(store)
	ref := SnapshotBase{URI: full.URI, SHA256: full.SHA256, Overlays: 1}
	run.TaskRuns[4].Status = orkstackv1.Status_STATUS_RUNNING
	verified, err := cold.WriteSnapshotOrOverlay(context.Background(), run, 3, ref, DefaultOverlayPolicy())
	if err != nil || verified.Full {
		t.Fatalf("an unchanged base read on a miss = %+v, %v; want an overlay", verified, err)
	}
	old, _ := store.get(full.URI)
	changed := &orkstackv1.Run{}
	if err := proto.Unmarshal(old, changed); err != nil {
		t.Fatal(err)
	}
	changed.Status = orkstackv1.Status_STATUS_FAILED
	store.put(full.URI, mustMarshal(t, changed))
	written, err := NewSnapshotActivities(store).WriteSnapshotOrOverlay(context.Background(), run, 4, ref, DefaultOverlayPolicy())
	if err != nil || !written.Full || !strings.HasSuffix(written.URI, "/4.pb") {
		t.Fatalf("after the base changed: %+v, %v; want a full {4}.pb", written, err)
	}
	data, _ := store.get(written.URI)
	if written.SHA256 != artifacts.ContinuationSHA256(data) {
		t.Fatal("the reported digest is not the written bytes'")
	}
	store.mu.Lock()
	delete(store.objects, full.URI)
	store.mu.Unlock()
	gone, err := NewSnapshotActivities(store).WriteSnapshotOrOverlay(context.Background(), run, 5, ref, DefaultOverlayPolicy())
	if err != nil || !gone.Full {
		t.Fatalf("a base gone = %+v, %v; want a full snapshot", gone, err)
	}
}

// TestSnapshotOverlay_DiffCountsOnlyLiveWrites is the in-package half of
// TestSnapshotOverlay_DiffRunsInTheLocalActivityOnly: a live run diffs once
// per overlay attempt, through the activity.
func TestSnapshotOverlay_DiffCountsOnlyLiveWrites(t *testing.T) {
	before := overlayDiffs.Load()
	h := runOverlay(t, 300, 40, overlayPlan(t, defaultPolicy()), -1)
	attempts := 0
	for _, w := range h.writes {
		if w.activity == ActivityWriteSnapshotOrOverlay && w.base.URI != "" {
			attempts++
		}
	}
	if got := overlayDiffs.Load() - before; got == 0 || got > int64(attempts) {
		t.Fatalf("%d diffs for %d writes that had a base", got, attempts)
	}
}

// childStartPlanForTest is the start plan a run with policy on its plan gives
// a composed child.
func childStartPlanForTest(t *testing.T, policy *OverlayPolicy) RunPlan {
	t.Helper()
	s := buildRunState("ork-parent", flatDefinition(t, 1), nil, overlayPlan(t, policy))
	return s.childStartPlan(&ChildRunPlan{RunID: "ork-child", Plan: RunPlan{Namespace: "default"}})
}

// TestSnapshotBaseCache_HoldsEightBasesOrItsByteBudget pins the cache's two
// bounds and their encoded-byte accounting: the default is 8 bases or 128 MiB,
// the count evicts first once the writer has more runs than it holds, bytes
// are counted, and a base larger than the whole byte bound is returned but
// never kept.
func TestSnapshotBaseCache_HoldsEightBasesOrItsByteBudget(t *testing.T) {
	runBytes := func(id string) []byte {
		t.Helper()
		data, err := proto.Marshal(&orkstackv1.Run{Id: id})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	def := NewSnapshotBaseCache()
	if def.maxEntries != snapshotBaseCacheEntries || def.maxBytes != snapshotBaseCacheBytes {
		t.Fatalf("the default cache is %d entries / %d bytes, want %d / %d",
			def.maxEntries, def.maxBytes, snapshotBaseCacheEntries, snapshotBaseCacheBytes)
	}
	if snapshotBaseCacheEntries != 8 || snapshotBaseCacheBytes != 128<<20 {
		t.Fatalf("the documented bounds are %d entries / %d bytes, want 8 / 128 MiB",
			snapshotBaseCacheEntries, snapshotBaseCacheBytes)
	}

	byCount := newSnapshotBaseCache(2, 1<<20)
	for _, id := range []string{"a", "b"} {
		byCount.put("s3://test/"+id, id, runBytes(id))
	}
	byCount.get("s3://test/a", "a")
	byCount.put("s3://test/c", "c", runBytes("c"))
	if byCount.get("s3://test/a", "a") == nil || byCount.get("s3://test/c", "c") == nil {
		t.Fatal("the least recently used base was not the one evicted")
	}
	if byCount.get("s3://test/b", "b") != nil {
		t.Fatal("the cache kept three bases over its two-entry bound")
	}
	if byCount.len() != 2 {
		t.Fatalf("the cache holds %d bases, want 2", byCount.len())
	}

	a := runBytes("a")
	b := runBytes("b")
	byBytes := newSnapshotBaseCache(10, int64(len(a)+len(b)-1))
	byBytes.put("s3://test/a", "a", a)
	byBytes.put("s3://test/b", "b", b)
	if byBytes.len() != 1 || byBytes.size() != int64(len(b)) {
		t.Fatalf("the byte budget kept %d bases / %d bytes, want 1 / %d", byBytes.len(), byBytes.size(), len(b))
	}
	tooBig, err := proto.Marshal(&orkstackv1.Run{Id: strings.Repeat("x", int(byBytes.maxBytes))})
	if err != nil {
		t.Fatal(err)
	}
	if got := byBytes.put("s3://test/big", "big", tooBig); got == nil {
		t.Fatal("a base over the whole byte bound was not decoded for its caller")
	}
	if byBytes.get("s3://test/big", "big") != nil {
		t.Fatal("a base over the byte bound was kept")
	}
	if byBytes.len() != 1 {
		t.Fatalf("a base over the byte bound evicted the ones it cannot share the cache with: %d entries", byBytes.len())
	}
	if got := byCount.put("s3://test/a", "a", runBytes("a")); got == nil || got.GetId() != "a" {
		t.Fatal("a hit did not return the cached base")
	}
}

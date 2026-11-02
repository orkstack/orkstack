package engine_test

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// errStoreDown is the snapshot write failure a test injects.
var errStoreDown = errors.New("object storage unreachable")

// snapshotWrites fails event snapshot writes by their number — the n-th
// write of one event's sequence, counted from 1 — and counts the distinct
// writes of each sequence. A write is one local activity however many
// attempts the engine's retry policy gives it, and a refused write refuses
// every attempt, the way a store that is down does; so what a row says is
// which writes the store refuses, not how often the engine asks again. It is
// an interceptor on the run's worker, which sees each write as the workflow
// starts it, and the hook of the harness's snapshot store (onSnapshot), which
// sees each attempt.
type snapshotWrites struct {
	interceptor.WorkerInterceptorBase
	// refuse says whether the store refuses write n (from 1) of sequence.
	refuse func(sequence int64, n int) bool

	mu     sync.Mutex
	writes map[int64]int
}

func (w *snapshotWrites) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &snapshotWritesInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, root: w}
}

type snapshotWritesInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root *snapshotWrites
}

func (i *snapshotWritesInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&snapshotWritesOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, root: i.root})
}

type snapshotWritesOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *snapshotWrites
}

func (o *snapshotWritesOutbound) ExecuteLocalActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	if sequence, ok := args[1].(int64); ok && (activityType == engine.ActivityWriteSnapshot || activityType == engine.ActivityWriteSnapshotOrOverlay) {
		o.root.mu.Lock()
		if o.root.writes == nil {
			o.root.writes = map[int64]int{}
		}
		o.root.writes[sequence]++
		o.root.mu.Unlock()
	}
	return o.Next.ExecuteLocalActivity(ctx, activityType, args...)
}

// store is the harness's onSnapshot hook: it fails an attempt of a write the
// store refuses.
func (w *snapshotWrites) store(_ *orkstackv1.Run, sequence int64) error {
	w.mu.Lock()
	n := w.writes[sequence]
	w.mu.Unlock()
	if w.refuse != nil && w.refuse(sequence, n) {
		return errStoreDown
	}
	return nil
}

// of is how many distinct writes of sequence the workflow started.
func (w *snapshotWrites) of(sequence int64) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes[sequence]
}

// refusing builds a refuse function from the writes it refuses: by sequence,
// the write numbers (every write of the sequence when the list is empty).
func refusing(bySequence map[int64][]int) func(int64, int) bool {
	return func(sequence int64, n int) bool {
		numbers, ok := bySequence[sequence]
		if !ok {
			return false
		}
		return len(numbers) == 0 || slices.Contains(numbers, n)
	}
}

// TestRunWorkflow_AnEndingBeforeStartWritesRunCreatedAgainBeforeItsEnd:
// generation 0 of a run whose definition could not be used ends through
// endBeforeStart, which writes RUN_CREATED and then the terminal event. When
// RUN_CREATED's write fails, it is written again under the same number
// before the terminal event, so a store back for that second write leaves
// both, from 1. When that fails too, the terminal event still goes at 2,
// under a key of its own, and the archive records 1 as a gap
// (unpublishedStart, TestTemporalRunReader_AnEndingBeforeStartIsServedAndArchivedWithItsGap).
// When the terminal write fails as well, nothing is published and the reader
// numbers the execution's close from 1.
func TestRunWorkflow_AnEndingBeforeStartWritesRunCreatedAgainBeforeItsEnd(t *testing.T) {
	endings := []struct {
		name     string
		cancel   bool
		terminal string
	}{
		{"canceled during the load", true, "RUN_CANCELED"},
		{"definition unavailable", false, "RUN_FAILED"},
	}
	writes := []struct {
		name string
		// refuse is the writes the store refuses, by event sequence: the
		// write numbers of that sequence (every write when the list is empty).
		refuse map[int64][]int
		// want is the stream published; a trailing ":" takes the ending's
		// terminal event type.
		want string
		// runCreated is how many distinct writes RUN_CREATED's number
		// (sequence 1) is given: one, and one more where the first is refused.
		runCreated int
	}{
		{"its write takes", nil, "1:RUN_CREATED 2:", 1},
		{"its first write fails and the second takes", map[int64][]int{1: {1}}, "1:RUN_CREATED 2:", 2},
		{"neither write takes", map[int64][]int{1: {}}, "2:", 2},
		// The terminal event's own failure is the one any run may meet: the
		// reader numbers the close after the last event published.
		{"its write takes and the terminal's does not", map[int64][]int{2: {}}, "1:RUN_CREATED", 1},
		{"its second write takes and the terminal's does not", map[int64][]int{1: {1}, 2: {}}, "1:RUN_CREATED", 2},
		{"nothing takes", map[int64][]int{1: {}, 2: {}}, "", 2},
	}
	for _, ending := range endings {
		for _, w := range writes {
			t.Run(ending.name+"/"+w.name, func(t *testing.T) {
				var suite testsuite.WorkflowTestSuite
				h := newHarnessCtx(t, &suite, okBehavior)
				store := &snapshotWrites{refuse: refusing(w.refuse)}
				h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{store}}))
				h.onSnapshot = store.store
				wf := definedChain(t)
				stub, plan := storeDefinition(t, h.snapshots, definitionRunID, wf, continuedPlan(1<<20))
				h.snapshots.remove(definitionURI(t, h.snapshots, wf, definitionRunID, plan))
				if ending.cancel {
					h.env.RegisterDelayedCallback(h.env.CancelWorkflow, 20*time.Second)
				}
				h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: definitionRunID})
				h.env.ExecuteWorkflow(engine.RunWorkflowType, definitionRunID, stub, payload(t, nil), plan,
					(*orkstackv1.RunContinuation)(nil))
				err := h.env.GetWorkflowError()
				if ending.cancel && !temporal.IsCanceledError(err) {
					t.Fatalf("workflow error = %v, want a cancel", err)
				}
				if !ending.cancel && applicationType(err) != engine.RunDefinitionUnavailableCode {
					t.Fatalf("workflow error = %v, want %s", err, engine.RunDefinitionUnavailableCode)
				}
				want := w.want
				if strings.HasSuffix(want, ":") {
					want += ending.terminal
				}
				published := h.bus.Events(definitionRunID, 0)
				if got := strings.Join(definitionEventTypes(published), " "); got != want {
					t.Fatalf("published %q, want %q", got, want)
				}
				if got := store.of(1); got != w.runCreated {
					t.Errorf("RUN_CREATED's number was written %d times, want %d", got, w.runCreated)
				}
				// The terminal event's snapshot is written once, at 2,
				// whatever became of RUN_CREATED; nothing is ever written
				// past it.
				if got, after := store.of(2), store.of(3); got != 1 || after != 0 {
					t.Errorf("the terminal event's number was written %d times and the one after it %d, want once and never", got, after)
				}
			})
		}
	}
}

// endBeforeStartWith runs generation 0 of a run whose definition object is
// gone, canceled during its load, with store in front of its snapshot writes
// and capture on its worker, and returns what it published.
func endBeforeStartWith(t *testing.T, store *snapshotWrites, capture *pinCreationCapture) []*orkstackv1.Event {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, okBehavior)
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture, store}}))
	h.onSnapshot = store.store
	wf := definedChain(t)
	stub, plan := storeDefinition(t, h.snapshots, definitionRunID, wf, continuedPlan(1<<20))
	h.snapshots.remove(definitionURI(t, h.snapshots, wf, definitionRunID, plan))
	h.env.RegisterDelayedCallback(h.env.CancelWorkflow, 20*time.Second)
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: definitionRunID})
	h.env.ExecuteWorkflow(engine.RunWorkflowType, definitionRunID, stub, payload(t, nil), plan,
		(*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); !temporal.IsCanceledError(err) {
		t.Fatalf("workflow error = %v, want a cancel", err)
	}
	return h.bus.Events(definitionRunID, 0)
}

// TestRunWorkflow_TheRunCreatedFirstGateIsAskedOnlyWhereItsWriteFails: an
// ending before start whose RUN_CREATED takes asks run-created-first
// nothing, so its history carries no marker; one whose RUN_CREATED write
// fails asks it once, where it fails.
func TestRunWorkflow_TheRunCreatedFirstGateIsAskedOnlyWhereItsWriteFails(t *testing.T) {
	for _, tc := range []struct {
		name   string
		refuse map[int64][]int
		asked  int
	}{
		{"its write takes", nil, 0},
		{"its first write fails", map[int64][]int{1: {1}}, 1},
		{"neither write takes", map[int64][]int{1: {}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture := &pinCreationCapture{}
			endBeforeStartWith(t, &snapshotWrites{refuse: refusing(tc.refuse)}, capture)
			asked := capture.versionsAsked(engine.RunCreatedFirstChange)
			if len(asked) != tc.asked {
				t.Fatalf("%s asked %v, want %d question(s)", engine.RunCreatedFirstChange, asked, tc.asked)
			}
			for _, v := range asked {
				if v != 1 {
					t.Fatalf("%s answered %v, want 1", engine.RunCreatedFirstChange, asked)
				}
			}
		})
	}
}

// TestRunWorkflow_AGateOffEndingBeforeStartKeepsTheLegacyWrites: with the
// gate switched off — the command sequence of every build before it — a
// failed RUN_CREATED write is logged and the terminal event still written,
// at sequence 2, past the number RUN_CREATED burned. This is the path a
// history recorded before the gate replays (run-created-first-legacy-v1.json),
// and the gap the gate exists to close.
func TestRunWorkflow_AGateOffEndingBeforeStartKeepsTheLegacyWrites(t *testing.T) {
	defer engine.DisableRunCreatedFirstGateForTest()()
	capture := &pinCreationCapture{}
	store := &snapshotWrites{refuse: refusing(map[int64][]int{1: {1}})}
	published := endBeforeStartWith(t, store, capture)
	if got := strings.Join(definitionEventTypes(published), " "); got != "2:RUN_CANCELED" {
		t.Fatalf("published %q, want the legacy %q", got, "2:RUN_CANCELED")
	}
	if got := store.of(1); got != 1 {
		t.Fatalf("RUN_CREATED's number was written %d times, want the one write the legacy path makes", got)
	}
	if asked := capture.versionsAsked(engine.RunCreatedFirstChange); len(asked) != 0 {
		t.Fatalf("%s asked %v with the gate off, want no question", engine.RunCreatedFirstChange, asked)
	}
}

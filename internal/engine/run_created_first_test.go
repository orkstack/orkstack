package engine_test

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// errStoreDown is the snapshot write failure a test injects.
var errStoreDown = errors.New("object storage unreachable")

// snapshotAttempts fails the first failing write attempts of sequence 1 —
// every one when failing is negative — and every attempt of sequence 2 when
// terminal is set, and counts every attempt by sequence.
type snapshotAttempts struct {
	mu       sync.Mutex
	failing  int
	terminal bool
	tries    map[int64]int
}

func (a *snapshotAttempts) write(_ *orkstackv1.Run, sequence int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tries[sequence]++
	if sequence == 1 && (a.failing < 0 || a.tries[1] <= a.failing) {
		return errStoreDown
	}
	if sequence == 2 && a.terminal {
		return errStoreDown
	}
	return nil
}

func (a *snapshotAttempts) of(sequence int64) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.tries[sequence]
}

// TestRunWorkflow_AnEndingBeforeStartPublishesNothingPastAnUnwrittenRunCreated:
// generation 0 of a run whose definition could not be used ends through
// endBeforeStart, which writes RUN_CREATED and then the terminal event. Its
// stream starts at sequence 1 or holds nothing the run wrote: when
// RUN_CREATED's write fails, it is written again under the same number,
// and when that fails too, nothing is written after it, so the reader
// numbers the execution's close from 1 (closeRun). A terminal event at 2
// with no event at 1 is the gap this rules out: once published, the stream
// and the archive keep it for good.
func TestRunWorkflow_AnEndingBeforeStartPublishesNothingPastAnUnwrittenRunCreated(t *testing.T) {
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
		// failing is how many of sequence 1's write attempts fail: the
		// snapshot's local activity makes three, so 3 fails its first
		// write, and -1 every one.
		failing int
		// terminal fails every write of the terminal event's snapshot.
		terminal bool
		// want is the stream published; a trailing ":" takes the ending's
		// terminal event type.
		want string
		// tries is how many attempts sequence 1's writes make in all.
		tries int
	}{
		{"its write takes", 0, false, "1:RUN_CREATED 2:", 1},
		{"its first write fails and the second takes", 3, false, "1:RUN_CREATED 2:", 4},
		{"neither write takes", -1, false, "", 6},
		// The terminal event's own failure is the one any run may meet: the
		// reader numbers the close after RUN_CREATED.
		{"its write takes and the terminal's does not", 0, true, "1:RUN_CREATED", 1},
		{"its second write takes and the terminal's does not", 3, true, "1:RUN_CREATED", 4},
	}
	for _, ending := range endings {
		for _, w := range writes {
			t.Run(ending.name+"/"+w.name, func(t *testing.T) {
				var suite testsuite.WorkflowTestSuite
				h := newHarnessCtx(t, &suite, okBehavior)
				attempts := &snapshotAttempts{failing: w.failing, terminal: w.terminal, tries: map[int64]int{}}
				h.onSnapshot = attempts.write
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
				requireContiguous(t, published)
				if got := attempts.of(1); got != w.tries {
					t.Errorf("RUN_CREATED's writes made %d attempts, want %d", got, w.tries)
				}
				if want == "" && attempts.of(2) != 0 {
					t.Errorf("the terminal snapshot was written %d times past an unwritten RUN_CREATED, want never",
						attempts.of(2))
				}
			})
		}
	}
}

// endBeforeStartWith runs generation 0 of a run whose definition object is
// gone, canceled during its load, with attempts in front of its snapshot
// writes and capture on its worker, and returns what it published.
func endBeforeStartWith(t *testing.T, attempts *snapshotAttempts, capture *pinCreationCapture) []*orkstackv1.Event {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, okBehavior)
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}}))
	h.onSnapshot = attempts.write
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
		name    string
		failing int
		asked   int
	}{
		{"its write takes", 0, 0},
		{"its first write fails", 3, 1},
		{"neither write takes", -1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture := &pinCreationCapture{}
			endBeforeStartWith(t, &snapshotAttempts{failing: tc.failing, tries: map[int64]int{}}, capture)
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
	attempts := &snapshotAttempts{failing: 3, tries: map[int64]int{}}
	published := endBeforeStartWith(t, attempts, capture)
	if got := strings.Join(definitionEventTypes(published), " "); got != "2:RUN_CANCELED" {
		t.Fatalf("published %q, want the legacy %q", got, "2:RUN_CANCELED")
	}
	if got := attempts.of(1); got != 3 {
		t.Fatalf("RUN_CREATED's write made %d attempts, want one write's 3", got)
	}
	if asked := capture.versionsAsked(engine.RunCreatedFirstChange); len(asked) != 0 {
		t.Fatalf("%s asked %v with the gate off, want no question", engine.RunCreatedFirstChange, asked)
	}
}

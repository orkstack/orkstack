package engine_test

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// switched is a run's generation 0, ended by its first switch: the shared
// store it wrote to and the arguments its next generation starts with.
type switched struct {
	store *testSnapshotStore
	runID string
	wf    *orkstackv1.Orkflow
	input *orkstackv1.Payload
	plan  engine.RunPlan
	carry *orkstackv1.RunContinuation
}

// firstSwitch runs generation 0 until it continues as new.
func firstSwitch(t *testing.T, runID, doc string, input *orkstackv1.Payload, plan engine.RunPlan) *switched {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, itemBehavior)
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: runID})
	h.env.ExecuteWorkflow(engine.RunWorkflowType, runID, compile(t, doc), input, plan, (*orkstackv1.RunContinuation)(nil))
	var can *workflow.ContinueAsNewError
	if !errors.As(h.env.GetWorkflowError(), &can) {
		t.Fatalf("generation 0 ended %v, want a switch", h.env.GetWorkflowError())
	}
	out := &switched{store: h.snapshots}
	if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &out.runID, &out.wf, &out.input, &out.plan, &out.carry); err != nil {
		t.Fatal(err)
	}
	out.runID = runID
	return out
}

// next starts the next generation in a fresh harness over the run's store,
// prepared by setup.
func (s *switched) next(t *testing.T, setup func(h *harness)) *harness {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := newHarnessOver(t, &suite, s.store, itemBehavior)
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: s.runID})
	if setup != nil {
		setup(h)
	}
	h.env.ExecuteWorkflow(engine.RunWorkflowType, "", s.wf, s.input, s.plan, s.carry)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the next generation did not end")
	}
	return h
}

// bigChainDocument is a chain long enough that its saved state spans many
// small chunks.
const bigChainDocument = `
name: big-chain
tasks:
  - id: a
    uses: shell
    with: {op: exec, note: "a note long enough to give the saved state some bytes to read"}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec, note: "a note long enough to give the saved state some bytes to read"}
  - id: c
    uses: shell
    needs: [b]
    with: {op: exec, note: "a note long enough to give the saved state some bytes to read"}
  - id: d
    uses: shell
    needs: [c]
    with: {op: exec}
  - id: e
    uses: shell
    needs: [d]
    with: {op: exec}
  - id: f
    uses: shell
    needs: [e]
    with: {op: exec}
`

// smallChunkPlan reads the saved state in 16-byte chunks, perTask per
// workflow task.
func smallChunkPlan(events, perTask int64) engine.RunPlan {
	plan := continuedPlan(events)
	plan.Continuation.ChunkBytes = 16
	plan.Continuation.ChunksPerTask = perTask
	return plan
}

// TestContinuation_BootstrapBatchesChunkReads: the next generation reads its
// saved state in chunks of at most the plan's ChunkBytes, ChunksPerTask per
// workflow task, with a 1 ms timer ending each batch's workflow task — and
// it is the pinned ChunksPerTask that decides the batches.
func TestContinuation_BootstrapBatchesChunkReads(t *testing.T) {
	for _, perTask := range []int64{8, 4} {
		t.Run(strings.Repeat("·", int(perTask)), func(t *testing.T) {
			s := firstSwitch(t, "ork-batches", bigChainDocument, payload(t, nil), smallChunkPlan(2, perTask))
			var r *commandRecorder
			var mu sync.Mutex
			var lengths []int64
			h := s.next(t, func(h *harness) {
				r = recordCommands(h)
				prev := int64(-1)
				h.onContinuation = func(kind string, offset int64) error {
					if kind == "read" {
						mu.Lock()
						if prev >= 0 {
							lengths = append(lengths, offset-prev)
						}
						prev = offset
						mu.Unlock()
					}
					return nil
				}
			})
			if err := h.env.GetWorkflowError(); err != nil && !isContinueAsNew(err) {
				t.Fatalf("generation 1 failed: %v", err)
			}
			chunks := int((s.carry.GetCheckpoint().GetSizeBytes() + 15) / 16)
			if chunks <= int(perTask) {
				t.Fatalf("the saved state is %d chunks, too few to batch", chunks)
			}
			var batches []int
			reads := 0
			for _, c := range r.list() {
				switch {
				case c == "local:"+engine.ActivityReadContinuationChunk:
					reads++
				case c == "timer:1ms" && reads > 0:
					batches = append(batches, reads)
					reads = 0
				case strings.HasPrefix(c, "activity:"):
					if reads > 0 {
						batches = append(batches, reads)
						reads = 0
					}
				}
			}
			total := 0
			for i, n := range batches {
				total += n
				if n > int(perTask) || (i < len(batches)-1 && n != int(perTask)) {
					t.Fatalf("batches %v, want %d chunk reads per workflow task", batches, perTask)
				}
			}
			if total != chunks {
				t.Fatalf("%d chunk reads in batches %v, want %d", total, batches, chunks)
			}
			for _, n := range lengths {
				if n > 16 {
					t.Fatalf("a chunk of %d bytes, over the plan's 16", n)
				}
			}
		})
	}
}

func isContinueAsNew(err error) bool {
	var can *workflow.ContinueAsNewError
	return errors.As(err, &can)
}

// TestContinuation_BootstrapIsPartOfTheBase: the saved state's chunk
// markers are counted into the generation's base, never its growth: a
// generation that read many chunks does not start out due.
func TestContinuation_BootstrapIsPartOfTheBase(t *testing.T) {
	s := firstSwitch(t, "ork-base", bigChainDocument, payload(t, nil), smallChunkPlan(2, 8))
	h := s.next(t, nil)
	if h.continuationCount("read") <= 2 {
		t.Fatalf("%d chunk reads, want more than the budget of 2", h.continuationCount("read"))
	}
	if got := len(h.taskOrder()); got < 2 {
		t.Fatalf("generation 1 dispatched %d tasks before switching, want its whole budget of 2", got)
	}
}

// TestContinuation_ChunkReadsUseTemporalsRetryPolicy: workflow code issues
// exactly one local activity per chunk, whose options carry the plan's
// restore policy — Temporal's own retries, with RUN_STATE_CORRUPT never
// retried — and whose ScheduleToClose is the time left to the bootstrap's
// deadline.
func TestContinuation_ChunkReadsUseTemporalsRetryPolicy(t *testing.T) {
	plan := smallChunkPlan(2, 8)
	plan.Continuation.Restore = engine.RestorePolicy{TimeoutSeconds: 600, AttemptSeconds: 20, BackoffInitialMillis: 1500, BackoffMaxSeconds: 45}
	s := firstSwitch(t, "ork-policy", bigChainDocument, payload(t, nil), plan)
	var r *commandRecorder
	h := s.next(t, func(h *harness) { r = recordCommands(h) })
	if h.continuationCount("read") != len(r.chunks) {
		t.Fatalf("%d reads ran for %d local activities issued: one each, no loop in workflow code", h.continuationCount("read"), len(r.chunks))
	}
	for i, o := range r.chunks {
		p := o.RetryPolicy
		if p == nil || p.InitialInterval != 1500*time.Millisecond || p.MaximumInterval != 45*time.Second ||
			p.BackoffCoefficient != 2 || len(p.NonRetryableErrorTypes) != 1 || p.NonRetryableErrorTypes[0] != engine.RunStateCorruptCode {
			t.Fatalf("read %d retry policy = %+v", i, p)
		}
		if o.StartToCloseTimeout != 20*time.Second || o.ScheduleToCloseTimeout <= 0 || o.ScheduleToCloseTimeout > 600*time.Second {
			t.Fatalf("read %d options = %+v, want 20s per attempt and the deadline's remainder", i, o)
		}
	}
}

// failedStart is the terminal event and snapshot of a generation that could
// not start: exactly one event, published at the carried sequence + 1.
func failedStart(t *testing.T, h *harness, s *switched) *orkstackv1.Event {
	t.Helper()
	events := h.bus.Events(s.runID, 0)
	if len(events) != 1 || events[0].GetSequence() != s.carry.GetEventSequence()+1 {
		t.Fatalf("published %v, want one terminal event at %d", eventKeys(events), s.carry.GetEventSequence()+1)
	}
	return events[0]
}

// TestContinuation_CancelDuringBootstrapEndsCanceledNotStateUnavailable: an
// operator's cancel that arrives with a chunk read pending ends the run
// RUN_CANCELED — never RUN_STATE_UNAVAILABLE — with a terminal event.
func TestContinuation_CancelDuringBootstrapEndsCanceledNotStateUnavailable(t *testing.T) {
	s := firstSwitch(t, "ork-boot-cancel", bigChainDocument, payload(t, nil), smallChunkPlan(2, 8))
	var once sync.Once
	h := s.next(t, func(h *harness) {
		h.onContinuation = func(kind string, _ int64) error {
			if kind == "read" {
				once.Do(h.env.CancelWorkflow)
			}
			return nil
		}
	})
	requireCanceled(t, h.env.GetWorkflowError())
	if e := failedStart(t, h, s); e.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Fatalf("terminal event %v, want RUN_CANCELED", e.GetType())
	}
}

func requireCanceled(t *testing.T, err error) {
	t.Helper()
	if !temporal.IsCanceledError(err) && !temporal.IsCanceledError(errors.Unwrap(err)) {
		t.Fatalf("workflow error = %v, want a cancellation", err)
	}
}

// TestContinuation_CancelDuringARetriedReadEndsCanceled: a read that fails
// with a retryable error while the cancel is recorded maps, by ctx.Err(), to
// RUN_CANCELED — never to RUN_STATE_UNAVAILABLE.
func TestContinuation_CancelDuringARetriedReadEndsCanceled(t *testing.T) {
	s := firstSwitch(t, "ork-boot-cancel-retry", bigChainDocument, payload(t, nil), smallChunkPlan(2, 8))
	var once sync.Once
	h := s.next(t, func(h *harness) {
		h.onContinuation = func(kind string, _ int64) error {
			if kind == "read" {
				once.Do(h.env.CancelWorkflow)
				return errors.New("object storage is unavailable")
			}
			return nil
		}
	})
	requireCanceled(t, h.env.GetWorkflowError())
	if e := failedStart(t, h, s); e.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Fatalf("terminal event %v, want RUN_CANCELED", e.GetType())
	}
}

// TestContinuation_CancelDuringBootstrapBackoffIsObservedAtOnce: reads fail;
// the cancel lands while Temporal waits out a retry's backoff; the run ends
// RUN_CANCELED in that workflow task and no further read executes.
func TestContinuation_CancelDuringBootstrapBackoffIsObservedAtOnce(t *testing.T) {
	plan := smallChunkPlan(2, 8)
	plan.Continuation.Restore.BackoffInitialMillis = 30_000 // every retry waits on a timer
	s := firstSwitch(t, "ork-boot-backoff", bigChainDocument, payload(t, nil), plan)
	var reads atomic.Int32
	h := s.next(t, func(h *harness) {
		h.onContinuation = func(kind string, _ int64) error {
			if kind == "read" {
				reads.Add(1)
				return errors.New("object storage is unavailable")
			}
			return nil
		}
		var once sync.Once
		h.env.SetOnTimerScheduledListener(func(string, time.Duration) { once.Do(h.env.CancelWorkflow) })
	})
	requireCanceled(t, h.env.GetWorkflowError())
	if got := reads.Load(); got != 1 {
		t.Fatalf("%d read attempts, want the one before the backoff", got)
	}
	if e := failedStart(t, h, s); e.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Fatalf("terminal event %v, want RUN_CANCELED", e.GetType())
	}
}

// TestContinuation_RestoreDeadlineReachedIsUnavailable: reads that keep
// failing are retried by Temporal until the restore timeout, then the run
// ends RUN_STATE_UNAVAILABLE naming the last error, with a terminal event
// built from the switch snapshot (UnreadableCheckpointFailsUnavailableAfterTheRestoreTimeout).
func TestContinuation_RestoreDeadlineReachedIsUnavailable(t *testing.T) {
	plan := smallChunkPlan(2, 8)
	plan.Continuation.Restore = engine.RestorePolicy{TimeoutSeconds: 60, AttemptSeconds: 5, BackoffInitialMillis: 1000, BackoffMaxSeconds: 10}
	s := firstSwitch(t, "ork-boot-deadline", bigChainDocument, payload(t, nil), plan)
	var reads atomic.Int32
	h := s.next(t, func(h *harness) {
		h.onContinuation = func(kind string, _ int64) error {
			if kind == "read" {
				reads.Add(1)
				return errors.New("bucket unreachable")
			}
			return nil
		}
	})
	requireRunStateError(t, h.env.GetWorkflowError(), engine.RunStateUnavailableCode)
	if reads.Load() < 2 {
		t.Fatalf("%d attempts: Temporal must retry the read until the deadline", reads.Load())
	}
	e := failedStart(t, h, s)
	if e.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || !strings.Contains(e.GetMessage(), "bucket unreachable") ||
		!strings.Contains(e.GetMessage(), "for 1m0s") {
		t.Fatalf("terminal event %v %q", e.GetType(), e.GetMessage())
	}
}

func requireRunStateError(t *testing.T, err error, code string) {
	t.Helper()
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != code || !app.NonRetryable() {
		t.Fatalf("workflow error = %v, want a non-retryable %s", err, code)
	}
}

// TestContinuation_CheckpointHashMismatchFailsCorruptWithATerminalEvent: a
// checkpoint altered after it was written is never adopted: RUN_STATE_CORRUPT,
// a terminal event, and the run's rows from the switch snapshot with every
// unfinished one CANCELED.
func TestContinuation_CheckpointHashMismatchFailsCorruptWithATerminalEvent(t *testing.T) {
	s := firstSwitch(t, "ork-boot-corrupt", bigChainDocument, payload(t, nil), smallChunkPlan(2, 8))
	uri := s.carry.GetCheckpoint().GetUri()
	data, _ := s.store.get(uri)
	data[len(data)/2] ^= 0xff
	s.store.put(uri, data)
	h := s.next(t, nil)
	requireRunStateError(t, h.env.GetWorkflowError(), engine.RunStateCorruptCode)
	e := failedStart(t, h, s)
	if e.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || !strings.Contains(e.GetMessage(), "failed verification") {
		t.Fatalf("terminal event %v %q", e.GetType(), e.GetMessage())
	}
	run := mustFinalRun(t, h.store, s.runID)
	if statusOf(run, "a") != orkstackv1.Status_STATUS_SUCCEEDED || statusOf(run, "f") != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("rows a=%v f=%v, want a kept and f canceled", statusOf(run, "a"), statusOf(run, "f"))
	}
}

// TestContinuation_UnknownCarryVersionFailsUnsupported: a carry format this
// release does not know is refused explicitly, never restored.
func TestContinuation_UnknownCarryVersionFailsUnsupported(t *testing.T) {
	s := firstSwitch(t, "ork-boot-version", bigChainDocument, payload(t, nil), smallChunkPlan(2, 8))
	s.carry.Version = engine.ContinuationCarryVersion + 1
	h := s.next(t, nil)
	requireRunStateError(t, h.env.GetWorkflowError(), engine.RunStateUnsupportedCode)
	if h.continuationCount("read") != 0 {
		t.Fatal("a carry of an unknown format was read")
	}
	failedStart(t, h, s)
}

// TestContinuation_OversizedCarryStopsSwitching: a carry that would exceed
// AGENTS.md §2.2's 256 KiB bound is never handed on — here a generation
// whose carried decisions, kept untouched from an earlier one, are too
// large — and the generation carries on in its own history.
func TestContinuation_OversizedCarryStopsSwitching(t *testing.T) {
	s := firstSwitch(t, "ork-boot-carry", bigChainDocument, payload(t, nil), smallChunkPlan(2, 8))
	s.carry.VersionDecisions[strings.Repeat("x", 300<<10)] = 1
	var r *commandRecorder
	h := s.next(t, func(h *harness) { r = recordCommands(h) })
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("generation 1 = %v, want it to finish the run without switching", err)
	}
	if r.count("continue-as-new") != 0 || r.count("memo") != 0 {
		t.Fatalf("commands %v: an oversized carry left a switch or a memo upsert", r.list())
	}
	if run := mustFinalRun(t, h.store, s.runID); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("status = %v", run.GetStatus())
	}
}

var _ = worker.Options{}

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The workflow half of a definition passed by reference (spec 5.2): every
// generation copies the definition into its own history before it decides
// anything, verifies it, and ends explicitly when it cannot.

const definitionRunID = "ork-1700000000-77"

// storeDefinition writes wf's definition object under runID, as the
// submitting engine does before the start, and returns the stub the start
// carries and the plan naming the object.
func storeDefinition(t *testing.T, store *testSnapshotStore, runID string, wf *orkstackv1.Orkflow, plan engine.RunPlan) (*orkstackv1.Orkflow, engine.RunPlan) {
	t.Helper()
	data, ref, err := engine.EncodeDefinitionObject(wf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteContinuation(context.Background(), wf.GetNamespace(), runID, 0,
		artifacts.ContinuationKindDefinition, data); err != nil {
		t.Fatal(err)
	}
	plan.Definition = &ref
	return engine.DefinitionStub(wf), plan
}

// definitionURI is the URI the test store gives runID's definition object.
func definitionURI(t *testing.T, store *testSnapshotStore, wf *orkstackv1.Orkflow, runID string, plan engine.RunPlan) string {
	t.Helper()
	uri, err := store.ContinuationURI(wf.GetNamespace(), runID, 0, artifacts.ContinuationKindDefinition, plan.Definition.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	return uri
}

// remove deletes the object at uri.
func (s *testSnapshotStore) remove(uri string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, uri)
}

// definedChain is continuationChainDocument with an identity a stub can be
// checked against.
func definedChain(t *testing.T) *orkstackv1.Orkflow {
	wf := compile(t, continuationChainDocument)
	wf.Version, wf.Namespace, wf.DefinitionDigest = "1.0.0", "team-a", strings.Repeat("cd", 32)
	return wf
}

// eventTypes is the run's published event types, by sequence.
func definitionEventTypes(events []*orkstackv1.Event) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, fmt.Sprintf("%d:%s", e.GetSequence(), strings.TrimPrefix(e.GetType().String(), "EVENT_TYPE_")))
	}
	return out
}

func applicationType(err error) string {
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		return applicationErr.Type()
	}
	return ""
}

// TestRunWorkflow_LoadsAndVerifiesTheDefinitionReference: a run started with
// a stub runs the whole definition the object holds. Its first command is
// the gate's version ask, then the definition's chunk reads — in batches of
// the plan's ChunksPerTask with a yield between, each named by its key
// (namespace, run id, generation 0, kind, SHA-256) and never by a URI, so
// workflow code sees no store root — and only then RUN_CREATED, which names
// the definition.
func TestRunWorkflow_LoadsAndVerifiesTheDefinitionReference(t *testing.T) {
	wf := definedChain(t)
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, okBehavior)
	plan := smallChunkPlan(1<<20, 4)
	plan.Continuation.Bytes = 1 << 40 // never due: one generation
	stub, plan := storeDefinition(t, h.snapshots, definitionRunID, wf, plan)
	r := recordCommands(h)
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: definitionRunID})
	h.env.ExecuteWorkflow(engine.RunWorkflowType, definitionRunID, stub, payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run failed: %v", err)
	}
	run := mustFinalRun(t, h.store, definitionRunID)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(run.GetTaskRuns()) != 5 {
		t.Fatalf("run = %v with %d tasks, want SUCCEEDED with 5", run.GetStatus(), len(run.GetTaskRuns()))
	}
	if got := strings.Join(h.taskOrder(), ","); got != "a,b,c,d,e" {
		t.Fatalf("ran %q, want the definition's five tasks", got)
	}
	chunks := int((plan.Definition.SizeBytes + 15) / 16)
	if len(h.locators) != chunks || chunks <= 4 {
		t.Fatalf("%d chunk reads for a %d-byte definition, want %d (more than one batch)", len(h.locators), plan.Definition.SizeBytes, chunks)
	}
	for _, loc := range h.locators {
		want := engine.ContinuationLocator{Namespace: "team-a", RunID: definitionRunID, Generation: 0,
			Kind: artifacts.ContinuationKindDefinition, SHA256: plan.Definition.SHA256}
		if loc != want {
			t.Fatalf("a read was named %+v, want the key %+v and no URI", loc, want)
		}
	}
	commands := r.list()
	if commands[0] != "version:"+engine.DefinitionByReferenceChange {
		t.Fatalf("the first command is %q, want the definition-by-reference version ask", commands[0])
	}
	created := -1
	reads, yields := 0, 0
	for i, c := range commands {
		switch {
		case c == "local:"+engine.ActivityReadContinuationChunk && created < 0:
			reads++
		case c == "timer:1ms" && created < 0:
			yields++
			if reads%4 != 0 {
				t.Fatalf("a yield after %d reads, want one per 4", reads)
			}
		case c == "local:"+engine.ActivityWriteSnapshot && created < 0:
			created = i
		}
	}
	if reads != chunks || yields != (chunks-1)/4 {
		t.Fatalf("%d reads and %d yields before RUN_CREATED, want %d and %d", reads, yields, chunks, (chunks-1)/4)
	}
	events := h.bus.Events(definitionRunID, 0)
	if events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CREATED || !strings.Contains(events[0].GetMessage(), "chain@1.0.0") {
		t.Fatalf("event 1 = %v %q, want RUN_CREATED naming chain@1.0.0", events[0].GetType(), events[0].GetMessage())
	}
}

// TestRunWorkflow_DefinitionIdentityMismatchFailsCorrupt: an object that
// verifies by its SHA-256 but is not the definition the stub names — another
// version, another namespace, another digest — is never used: the run ends
// RUN_DEFINITION_CORRUPT, ContractViolation, before any task, with
// RUN_CREATED and RUN_FAILED published at sequences 1 and 2 (implementer
// note 8). So does an object whose bytes were altered.
func TestRunWorkflow_DefinitionIdentityMismatchFailsCorrupt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		object func(wf *orkstackv1.Orkflow)
		alter  bool
	}{
		{name: "another version", object: func(wf *orkstackv1.Orkflow) { wf.Version = "9.9.9" }},
		{name: "another namespace", object: func(wf *orkstackv1.Orkflow) { wf.Namespace = "team-b" }},
		{name: "another digest", object: func(wf *orkstackv1.Orkflow) { wf.DefinitionDigest = strings.Repeat("ef", 32) }},
		{name: "altered bytes", alter: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			h := newHarnessCtx(t, &suite, okBehavior)
			wf := definedChain(t)
			stored := definedChain(t)
			if tc.object != nil {
				tc.object(stored)
			}
			// The object is written where the stub's run reads it.
			data, ref, err := engine.EncodeDefinitionObject(stored)
			if err != nil {
				t.Fatal(err)
			}
			if tc.alter {
				data[len(data)/2] ^= 0xff
			}
			key, err := artifacts.RunContinuationKey(wf.GetNamespace(), definitionRunID, 0, artifacts.ContinuationKindDefinition, ref.SHA256)
			if err != nil {
				t.Fatal(err)
			}
			h.snapshots.put("s3://test/"+key, data)
			plan := continuedPlan(1 << 20)
			plan.Definition = &ref
			h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: definitionRunID})
			h.env.ExecuteWorkflow(engine.RunWorkflowType, definitionRunID, engine.DefinitionStub(wf), payload(t, nil), plan,
				(*orkstackv1.RunContinuation)(nil))
			err = h.env.GetWorkflowError()
			if applicationType(err) != engine.RunDefinitionCorruptCode || !strings.Contains(err.Error(), "its definition failed verification") {
				t.Fatalf("workflow error = %v, want %s", err, engine.RunDefinitionCorruptCode)
			}
			if n := len(h.invocations()); n != 0 {
				t.Fatalf("%d tasks ran on a definition that failed verification", n)
			}
			events := h.bus.Events(definitionRunID, 0)
			if got := strings.Join(definitionEventTypes(events), " "); got != "1:RUN_CREATED 2:RUN_FAILED" {
				t.Fatalf("published %q, want RUN_CREATED then RUN_FAILED", got)
			}
			run := mustFinalRun(t, h.store, definitionRunID)
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != engine.RunDefinitionCorruptCode ||
				run.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION ||
				run.GetOrkflowName() != "chain" || run.GetNamespace() != "team-a" {
				t.Fatalf("final run = %v %v, want FAILED %s ContractViolation of chain in team-a", run.GetStatus(), run.GetError(),
					engine.RunDefinitionCorruptCode)
			}
		})
	}
}

// TestRunWorkflow_UnreadableDefinitionFailsUnavailableWithATerminalEvent: a
// definition object that stays missing is read until the plan's restore
// timeout, then the run ends RUN_DEFINITION_UNAVAILABLE, InfrastructureError,
// explicitly: in generation 0 through endBeforeStart, with RUN_CREATED and
// RUN_FAILED at sequences 1 and 2; in a later generation through
// endUnrestorable, its terminal event right after the carried sequence and
// its rows those of the switch snapshot.
func TestRunWorkflow_UnreadableDefinitionFailsUnavailableWithATerminalEvent(t *testing.T) {
	t.Run("generation 0", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		h := newHarnessCtx(t, &suite, okBehavior)
		wf := definedChain(t)
		stub, plan := storeDefinition(t, h.snapshots, definitionRunID, wf, continuedPlan(1<<20))
		h.snapshots.remove(definitionURI(t, h.snapshots, wf, definitionRunID, plan))
		h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: definitionRunID})
		h.env.ExecuteWorkflow(engine.RunWorkflowType, definitionRunID, stub, payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
		err := h.env.GetWorkflowError()
		if applicationType(err) != engine.RunDefinitionUnavailableCode ||
			!strings.Contains(err.Error(), "could not be read from object storage for 5m0s") {
			t.Fatalf("workflow error = %v, want %s after 5m0s", err, engine.RunDefinitionUnavailableCode)
		}
		if h.continuationCount("read") < 2 {
			t.Fatalf("%d reads, want Temporal to have retried the missing object", h.continuationCount("read"))
		}
		if got := strings.Join(definitionEventTypes(h.bus.Events(definitionRunID, 0)), " "); got != "1:RUN_CREATED 2:RUN_FAILED" {
			t.Fatalf("published %q, want RUN_CREATED then RUN_FAILED", got)
		}
		run := mustFinalRun(t, h.store, definitionRunID)
		if run.GetError().GetCode() != engine.RunDefinitionUnavailableCode ||
			run.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
			t.Fatalf("final error = %v, want %s InfrastructureError", run.GetError(), engine.RunDefinitionUnavailableCode)
		}
	})
	t.Run("a later generation", func(t *testing.T) {
		wf := definedChain(t)
		var uri string
		g := runGenerationsWithArgs(t, definitionRunID, func(h *harness) []any {
			stub, plan := storeDefinition(t, h.snapshots, definitionRunID, wf, continuedPlan(2))
			uri = definitionURI(t, h.snapshots, wf, definitionRunID, plan)
			return []any{definitionRunID, stub, payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil)}
		}, func(h *harness, generation int) {
			if generation == 1 {
				h.snapshots.remove(uri)
			}
		}, nil)
		if len(g.harnesses) != 2 {
			t.Fatalf("the run had %d generations, want it to end in its second", len(g.harnesses))
		}
		if applicationType(g.err) != engine.RunDefinitionUnavailableCode {
			t.Fatalf("generation 1 ended %v, want %s", g.err, engine.RunDefinitionUnavailableCode)
		}
		events := g.events()
		requireContiguous(t, events)
		last := events[len(events)-1]
		if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || last.GetSequence() != g.carries[0].GetEventSequence()+1 {
			t.Fatalf("the last event is %v at %d, want RUN_FAILED at %d", last.GetType(), last.GetSequence(),
				g.carries[0].GetEventSequence()+1)
		}
		run := g.final(t)
		succeeded := 0
		for _, row := range run.GetTaskRuns() {
			if row.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
				succeeded++
			}
		}
		if run.GetError().GetCode() != engine.RunDefinitionUnavailableCode || len(run.GetTaskRuns()) != 5 || succeeded == 0 {
			t.Fatalf("final run = %v with %d rows (%d succeeded), want %s over the switch snapshot's rows",
				run.GetError(), len(run.GetTaskRuns()), succeeded, engine.RunDefinitionUnavailableCode)
		}
		// The category is generation 0's: a carried generation's end never
		// collapses it into ContractViolation.
		if got := run.GetError().GetCategory(); got != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
			t.Fatalf("final error category = %v, want %s InfrastructureError", got, engine.RunDefinitionUnavailableCode)
		}
	})
}

// runGenerationsWithArgs is runGenerations whose first arguments are built
// over the run's own store, by first; rewrite, when set, may change a
// carried generation's plan before it starts.
func runGenerationsWithArgs(t *testing.T, runID string, first func(h *harness) []any, setup func(h *harness, generation int),
	rewrite func(generation int, plan *engine.RunPlan, carry *orkstackv1.RunContinuation),
) *generations {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	store := newTestSnapshotStore()
	values := &memValues{objects: map[string][]byte{}}
	out := &generations{runID: runID}
	var args []any
	for generation := 0; generation < 50; generation++ {
		h := newHarnessShared(t, &suite, store, values, okBehavior)
		h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: runID})
		if generation == 0 {
			args = first(h)
		}
		if setup != nil {
			setup(h, generation)
		}
		out.harnesses = append(out.harnesses, h)
		h.env.ExecuteWorkflow(engine.RunWorkflowType, args...)
		if !h.env.IsWorkflowCompleted() {
			t.Fatalf("generation %d did not end", generation)
		}
		var can *workflow.ContinueAsNewError
		if !errors.As(h.env.GetWorkflowError(), &can) {
			out.err = h.env.GetWorkflowError()
			return out
		}
		var (
			nextID    string
			nextWf    *orkstackv1.Orkflow
			nextInput *orkstackv1.Payload
			nextPlan  engine.RunPlan
			carry     *orkstackv1.RunContinuation
		)
		if err := converter.GetDefaultDataConverter().FromPayloads(can.Input,
			&nextID, &nextWf, &nextInput, &nextPlan, &carry); err != nil {
			t.Fatalf("decode generation %d's continue-as-new arguments: %v", generation, err)
		}
		if rewrite != nil {
			rewrite(generation+1, &nextPlan, carry)
		}
		out.carries = append(out.carries, carry)
		out.plans = append(out.plans, nextPlan)
		args = []any{nextID, nextWf, nextInput, nextPlan, carry}
	}
	t.Fatal("the run did not end within 50 generations")
	return nil
}

// TestRunWorkflow_CancelDuringDefinitionLoadEndsCanceled: an operator's
// cancel that reaches generation 0 while its definition reads keep failing
// cancels Temporal's retrying of them and ends the run RUN_CANCELED, never
// RUN_DEFINITION_*: RUN_CREATED and RUN_CANCELED at sequences 1 and 2.
func TestRunWorkflow_CancelDuringDefinitionLoadEndsCanceled(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, okBehavior)
	wf := definedChain(t)
	stub, plan := storeDefinition(t, h.snapshots, definitionRunID, wf, continuedPlan(1<<20))
	h.snapshots.remove(definitionURI(t, h.snapshots, wf, definitionRunID, plan))
	h.env.RegisterDelayedCallback(h.env.CancelWorkflow, 20*time.Second)
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: definitionRunID})
	h.env.ExecuteWorkflow(engine.RunWorkflowType, definitionRunID, stub, payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); !temporal.IsCanceledError(err) {
		t.Fatalf("workflow error = %v, want a cancel", err)
	}
	if got := strings.Join(definitionEventTypes(h.bus.Events(definitionRunID, 0)), " "); got != "1:RUN_CREATED 2:RUN_CANCELED" {
		t.Fatalf("published %q, want RUN_CREATED then RUN_CANCELED", got)
	}
	if run := mustFinalRun(t, h.store, definitionRunID); run.GetError().GetCode() != "RUN_CANCELED" {
		t.Fatalf("final error = %v, want RUN_CANCELED", run.GetError())
	}
}

// TestContinuation_CapCountsTheDefinition: each generation's copy is the
// definition plus the checkpoint, under one cap. A switch whose
// checkpoint alone fits but whose sum with the definition does not is never
// taken — the run carries on in its current history — and a carried
// generation whose plan's cap the sum exceeds is refused RUN_STATE_CORRUPT
// before it reads anything.
func TestContinuation_CapCountsTheDefinition(t *testing.T) {
	wf := definedChain(t)
	// The control: with room for both, the run switches.
	control := runGenerationsWithArgs(t, definitionRunID, func(h *harness) []any {
		stub, plan := storeDefinition(t, h.snapshots, definitionRunID, wf, continuedPlan(2))
		return []any{definitionRunID, stub, payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil)}
	}, nil, nil)
	if control.err != nil || len(control.carries) == 0 {
		t.Fatalf("the control ended %v after %d switches, want SUCCEEDED across generations", control.err, len(control.carries))
	}
	checkpoint := control.carries[0].GetCheckpoint().GetSizeBytes()

	t.Run("a switch", func(t *testing.T) {
		g := runGenerationsWithArgs(t, definitionRunID, func(h *harness) []any {
			stub, plan := storeDefinition(t, h.snapshots, definitionRunID, wf, continuedPlan(2))
			// The checkpoint alone fits; with the definition it does not.
			plan.Continuation.CheckpointMaxBytes = plan.Definition.SizeBytes + checkpoint/2
			if checkpoint > plan.Continuation.CheckpointMaxBytes {
				t.Fatalf("the cap %d leaves no room for a %d-byte checkpoint alone", plan.Continuation.CheckpointMaxBytes, checkpoint)
			}
			return []any{definitionRunID, stub, payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil)}
		}, nil, nil)
		if g.err != nil || len(g.harnesses) != 1 {
			t.Fatalf("the run ended %v after %d generations, want SUCCEEDED in one history", g.err, len(g.harnesses))
		}
	})
	t.Run("a restore", func(t *testing.T) {
		g := runGenerationsWithArgs(t, definitionRunID, func(h *harness) []any {
			stub, plan := storeDefinition(t, h.snapshots, definitionRunID, wf, continuedPlan(2))
			return []any{definitionRunID, stub, payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil)}
		}, nil, func(generation int, plan *engine.RunPlan, carry *orkstackv1.RunContinuation) {
			if generation == 1 {
				budget := *plan.Continuation
				budget.CheckpointMaxBytes = plan.Definition.SizeBytes + carry.GetCheckpoint().GetSizeBytes() - 1
				plan.Continuation = &budget
			}
		})
		if applicationType(g.err) != engine.RunStateCorruptCode || !strings.Contains(g.err.Error(), "over the") {
			t.Fatalf("generation 1 ended %v, want %s over its cap", g.err, engine.RunStateCorruptCode)
		}
		if reads := g.harnesses[1].continuationCount("read"); reads != 0 {
			t.Fatalf("generation 1 read %d chunks before refusing its cap, want none", reads)
		}
	})
}

// TestDefinitionByReference_GateIsCarried: generation 0 asks the gate once,
// before anything else, and every later generation reads the decision from
// its carry before its state is built — it asks no version for it, so it
// records no marker for it.
func TestDefinitionByReference_GateIsCarried(t *testing.T) {
	wf := definedChain(t)
	var recorders []*commandRecorder
	g := runGenerationsWithArgs(t, definitionRunID, func(h *harness) []any {
		stub, plan := storeDefinition(t, h.snapshots, definitionRunID, wf, continuedPlan(2))
		return []any{definitionRunID, stub, payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil)}
	}, func(h *harness, _ int) {
		recorders = append(recorders, recordCommands(h))
	}, nil)
	if g.err != nil || len(g.carries) < 2 {
		t.Fatalf("the run ended %v after %d switches, want SUCCEEDED across at least 3 generations", g.err, len(g.carries))
	}
	ask := "version:" + engine.DefinitionByReferenceChange
	for i, r := range recorders {
		want := 0
		if i == 0 {
			want = 1
			if first := r.list()[0]; first != ask {
				t.Fatalf("generation 0's first command is %q, want %q", first, ask)
			}
		}
		if got := r.count(ask); got != want {
			t.Fatalf("generation %d asked the gate %d times, want %d", i, got, want)
		}
		if i > 0 && r.count("local:"+engine.ActivityReadContinuationChunk) == 0 {
			t.Fatalf("generation %d read nothing, want its definition and its saved state", i)
		}
	}
	for i, carry := range g.carries {
		if v, ok := carry.GetVersionDecisions()[engine.DefinitionByReferenceChange]; !ok || v != 1 {
			t.Fatalf("carry %d records the gate as %d (%t), want 1", i, v, ok)
		}
	}
}

// TestRestoreHistoryBound_DefinitionAndCheckpointSplitAtTheCap pins the
// worst case RestoreHistoryBound's comment argues: a carried generation
// whose definition and checkpoint together are exactly at the cap, split so
// each object ends in a partial chunk that one object of their summed size
// would not. The two objects take one chunk more than the bound's chunk
// term (ceil(cap/chunk) + 1 markers), and never a yield more than its yield
// term — the reads share one batch counter, so N+1 reads take floor(N/P)
// yields, never over the ceil(N/P) the bound charges — so the bootstrap's
// markers and yields exceed the bound's chunk and yield terms by at most
// that one marker, which the bound's fixed 24 holds.
func TestRestoreHistoryBound_DefinitionAndCheckpointSplitAtTheCap(t *testing.T) {
	wf := definedChain(t)
	for _, tc := range []struct {
		name    string
		perTask func(chunks int64) int64
	}{
		{"one chunk per task", func(int64) int64 { return 1 }},
		{"two chunks per task", func(int64) int64 { return 2 }},
		{"a batch one short of the chunks", func(n int64) int64 { return n - 1 }},
		{"a batch of exactly the chunks", func(n int64) int64 { return n }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				budget   engine.ContinuationBudget
				chunks   int64 // ceil(cap / chunk): the bound's chunk term
				recorder *commandRecorder
			)
			g := runGenerationsWithArgs(t, definitionRunID, func(h *harness) []any {
				stub, plan := storeDefinition(t, h.snapshots, definitionRunID, wf, continuedPlan(2))
				return []any{definitionRunID, stub, payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil)}
			}, func(h *harness, generation int) {
				if generation == 1 {
					recorder = recordCommands(h)
				}
			}, func(generation int, plan *engine.RunPlan, carry *orkstackv1.RunContinuation) {
				if generation != 1 {
					return
				}
				d, k := plan.Definition.SizeBytes, carry.GetCheckpoint().GetSizeBytes()
				// The smallest chunk with at most 40 chunks at which both
				// objects end in a partial chunk whose remainders fit one.
				var chunk int64
				for c := max(1, (d+k+39)/40); c < d+k; c++ {
					if r1, r2 := d%c, k%c; r1 > 0 && r2 > 0 && r1+r2 <= c && (d+k+c-1)/c >= 3 {
						chunk = c
						break
					}
				}
				if chunk == 0 {
					t.Fatalf("no chunk size splits a %d-byte definition and a %d-byte checkpoint", d, k)
				}
				budget = *plan.Continuation
				budget.CheckpointMaxBytes = d + k // exactly at the cap
				budget.ChunkBytes = chunk
				chunks = (d + k + chunk - 1) / chunk
				budget.ChunksPerTask = tc.perTask(chunks)
				plan.Continuation = &budget
			})
			if g.err != nil {
				t.Fatalf("the run ended %v, want SUCCEEDED", g.err)
			}
			if len(g.harnesses) < 2 || recorder == nil {
				t.Fatalf("the run had %d generations, want a carried one", len(g.harnesses))
			}
			commands := recorder.list()
			last := -1
			for i, c := range commands {
				if c == "local:"+engine.ActivityReadContinuationChunk {
					last = i
				}
			}
			reads, yields := int64(0), int64(0)
			for _, c := range commands[:last+1] {
				switch c {
				case "local:" + engine.ActivityReadContinuationChunk:
					reads++
				case "timer:1ms":
					yields++
				}
			}
			perTask := budget.ChunksPerTask
			boundYields := (chunks + perTask - 1) / perTask
			if reads != chunks+1 {
				t.Fatalf("generation 1 read %d chunks, want %d: the split is not the worst case", reads, chunks+1)
			}
			if want := chunks / perTask; yields != want || yields > boundYields {
				t.Fatalf("generation 1 yielded %d times over %d reads of %d per task, want %d, never over the bound's %d",
					yields, reads, perTask, want, boundYields)
			}
			// The bound's chunk and yield terms: RestoreHistoryBound with no
			// time to fail in, less its fixed 24.
			noFailures := budget
			noFailures.Restore.TimeoutSeconds = 0
			terms := engine.RestoreHistoryBound(noFailures) - 24
			if terms != chunks+5*boundYields {
				t.Fatalf("the bound's chunk and yield terms are %d, want %d", terms, chunks+5*boundYields)
			}
			if got := reads + 5*yields; got > terms+1 {
				t.Fatalf("the bootstrap appended %d markers and yield events, over the bound's terms %d plus one", got, terms)
			}
			t.Logf("cap %d = definition %d + checkpoint %d, chunk %d, %d per task: %d reads and %d yields "+
				"(%d events) against the bound's %d chunks and %d yields (%d events)",
				budget.CheckpointMaxBytes, budget.CheckpointMaxBytes-g.carries[0].GetCheckpoint().GetSizeBytes(),
				g.carries[0].GetCheckpoint().GetSizeBytes(), budget.ChunkBytes, perTask, reads, yields, reads+5*yields,
				chunks, boundYields, terms)
		})
	}
}

// TestRunWorkflow_CarriedGenerationEndsOnAMalformedPlanOrCarry (implementer
// note 2, a carried generation): a later generation whose plan names a
// definition with no valid budget or a reference that is not well formed,
// or whose carry does not record the definition-by-reference decision, ends
// RUN_DEFINITION_CORRUPT through endUnrestorable — RUN_FAILED one sequence
// after the carry's last event, over the switch snapshot's rows, never a
// panic that retries the workflow task forever — and reads nothing.
func TestRunWorkflow_CarriedGenerationEndsOnAMalformedPlanOrCarry(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		edit       func(plan *engine.RunPlan, carry *orkstackv1.RunContinuation)
	}{
		{"no budget", "no valid budget", func(p *engine.RunPlan, _ *orkstackv1.RunContinuation) { p.Continuation = nil }},
		{"no parts", "names no parts", func(p *engine.RunPlan, _ *orkstackv1.RunContinuation) {
			ref := *p.Definition
			ref.Parts = nil
			p.Definition = &ref
		}},
		{"bad digest", "no valid SHA-256", func(p *engine.RunPlan, _ *orkstackv1.RunContinuation) {
			ref := *p.Definition
			ref.SHA256 = "XYZ"
			p.Definition = &ref
		}},
		{"no recorded decision", "does not record how its definition is read",
			func(_ *engine.RunPlan, c *orkstackv1.RunContinuation) {
				delete(c.VersionDecisions, engine.DefinitionByReferenceChange)
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wf := definedChain(t)
			g := runGenerationsWithArgs(t, definitionRunID, func(h *harness) []any {
				stub, plan := storeDefinition(t, h.snapshots, definitionRunID, wf, continuedPlan(2))
				return []any{definitionRunID, stub, payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil)}
			}, nil, func(generation int, plan *engine.RunPlan, carry *orkstackv1.RunContinuation) {
				if generation == 1 {
					tc.edit(plan, carry)
				}
			})
			if len(g.harnesses) != 2 {
				t.Fatalf("the run had %d generations, want it to end in its second", len(g.harnesses))
			}
			if applicationType(g.err) != engine.RunDefinitionCorruptCode || !strings.Contains(g.err.Error(), tc.want) {
				t.Fatalf("generation 1 ended %v, want %s (%q)", g.err, engine.RunDefinitionCorruptCode, tc.want)
			}
			if reads := g.harnesses[1].continuationCount("read"); reads != 0 {
				t.Fatalf("generation 1 read %d chunks, want none", reads)
			}
			events := g.events()
			requireContiguous(t, events)
			last := events[len(events)-1]
			if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || last.GetSequence() != g.carries[0].GetEventSequence()+1 {
				t.Fatalf("the last event is %v at %d, want RUN_FAILED at %d", last.GetType(), last.GetSequence(),
					g.carries[0].GetEventSequence()+1)
			}
			run := g.final(t)
			if run.GetError().GetCode() != engine.RunDefinitionCorruptCode ||
				run.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION ||
				len(run.GetTaskRuns()) != 5 {
				t.Fatalf("final run = %v with %d rows, want %s ContractViolation over the switch snapshot's rows",
					run.GetError(), len(run.GetTaskRuns()), engine.RunDefinitionCorruptCode)
			}
		})
	}
}

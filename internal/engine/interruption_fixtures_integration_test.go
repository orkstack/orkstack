//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// The interruption-retry fixtures (replay_interruption_test.go), each from a
// REAL run on a real cluster by a versioned, paced worker (startPacedCANStack:
// one completion per workflow task, so every recording of a shape is the same
// sequence). Their ExecuteTask is a
// scripted stand-in for an Orker, a hand-written fake for fault cases: every
// shape is a fault a real Orker produces only when it is stopped, killed or
// loses object storage at an exact point of an attempt (a stopping Orker's
// hand-back, a loss with the beat a stopping Orker sends, a store fault, a
// profile drift), which no test can bring about on demand. Each answers as
// the Orker does: the typed OrkInterruption:<reason> error carrying its
// ActivityFailureDetail, or a last beat (task.HeartbeatProgress) followed by
// silence until the cluster's heartbeat timeout.

// The committed fixtures.
const (
	interruptionBeatStarted     = "testdata/history/run-interruption-retry-running-beat-started-v1.json"
	interruptionLostStopping    = "testdata/history/run-interruption-retry-lost-while-stopping-v1.json"
	interruptionStoreFault      = "testdata/history/run-interruption-retry-store-fault-budget-v1.json"
	interruptionProfileDrifted  = "testdata/history/run-interruption-retry-profile-drifted-v1.json"
	interruptionItemContinuedG0 = "testdata/history/run-interruption-retry-item-continuation-g0-v1.json"
	interruptionItemContinuedG1 = "testdata/history/run-interruption-retry-item-continuation-g1-v1.json"
	interruptionLegacy          = "testdata/history/run-interruption-retry-legacy-v1.json"
)

// interruptionFixtureDocument is one task with a one-second cancellation
// grace, so a loss's fence is 31 s.
const interruptionFixtureDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: interruption-fixture
tasks:
  - id: work
    uses: shell
    cancellation_grace_period_seconds: 1
    with: {op: exec}
`

// interruptionItemsDocument is a fan-out of two items and nothing else, so
// the run is idle while its item waits, and carries on then.
const interruptionItemsDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: interruption-items
inputs:
  parts: {type: array}
tasks:
  - id: spread
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 10}
    with: {op: exec, part: "{{ .each.item }}"}
`

// scriptedOrker answers each unit's executions from its script in order,
// then succeeds. A loss step heartbeats once with its beat and then says
// nothing, as a killed Orker does.
type scriptedOrker struct {
	mu    sync.Mutex
	steps map[string][]orkerStep
	calls map[string]int
}

type orkerStep struct {
	err  error
	beat *task.HeartbeatProgress
}

func (o *scriptedOrker) behavior(ctx context.Context, _ *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	key := in.GetTaskId()
	if each := in.GetEach(); each != nil {
		key = fmt.Sprintf("%s#%d", key, each.GetIndex())
	}
	o.mu.Lock()
	n := o.calls[key]
	o.calls[key]++
	o.mu.Unlock()
	if steps := o.steps[key]; n < len(steps) {
		step := steps[n]
		if step.beat != nil {
			// One beat, then silence, as a killed Orker; the paced stack
			// keeps this attempt (losing), so it holds no turn meanwhile.
			activity.RecordHeartbeat(ctx, *step.beat)
			select {
			case <-ctx.Done():
			case <-time.After(3 * time.Minute):
			}
			return nil, errors.New("the Orker was lost")
		}
		if step.err != nil {
			return nil, step.err
		}
	}
	return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), ValuesJson: []byte(`{}`)}, nil
}

// losing says the unit's next execution is a loss step: the attempt a paced
// stack keeps (startPacedCANStackKeeping).
func (o *scriptedOrker) losing(in *orkstackv1.ExecuteTaskInput) bool {
	key := in.GetTaskId()
	if each := in.GetEach(); each != nil {
		key = fmt.Sprintf("%s#%d", key, each.GetIndex())
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	steps, n := o.steps[key], o.calls[key]
	return n < len(steps) && steps[n].beat != nil
}

func (o *scriptedOrker) count(key string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls[key]
}

// interruptionFixturePlan is s.plan with the deployment's budgets, changed
// by mutate.
func interruptionFixturePlan(s *canStack, budget *engine.ContinuationBudget, mutate func(*engine.InterruptionBudgets)) engine.RunPlan {
	plan := s.plan(engine.DefaultHistoryMaxEvents, budget)
	budgets := engine.DefaultInterruptionBudgets
	if mutate != nil {
		mutate(&budgets)
	}
	plan.Interruptions = &budgets
	return plan
}

// TestIntegration_RecordInterruptionRetryHistories records every
// interruption-retry fixture. It never skips: by default the recordings land
// under ORK_PROOF_DIR and the committed fixtures are left alone; only
// ORK_RECORD_GOLDEN_HISTORY=1 also writes them. It fails unless every shape
// ends as it should, holds the gate's marker (none for the legacy shape) and
// replays against current code.
func TestIntegration_RecordInterruptionRetryHistories(t *testing.T) {
	write := os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != ""
	// The parallel shapes run after this function returns, so the context
	// ends with the test's cleanups, never with this function.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	var mu sync.Mutex
	files := map[string]*historypb.History{}
	keep := func(path string, h *historypb.History) {
		mu.Lock()
		files[path] = h
		mu.Unlock()
	}
	t.Cleanup(func() {
		for path, h := range files {
			data := encodeHistory(t, h)
			proof.Write(t, "interruption-retry", filepath.Base(path), data)
			if write {
				must.OKf(t, os.WriteFile(path, data, 0o644), "write %s", path)
			}
			t.Logf("recorded %d events for %s (fixture written: %v)", len(h.GetEvents()), filepath.Base(path), write)
		}
	})
	stoppingSince := time.Now().UTC().Format(time.RFC3339Nano)

	single := func(t *testing.T, label string, steps []orkerStep, mutate func(*engine.InterruptionBudgets), wantOK bool, path string, legacy bool) {
		orker := &scriptedOrker{steps: map[string][]orkerStep{"work": steps}, calls: map[string]int{}}
		s := startPacedCANStackKeeping(t, ctx, label, orker.behavior, orker.losing)
		runID := fmt.Sprintf("ork-%s-%d", label, time.Now().UnixNano())
		func() {
			if legacy {
				defer engine.DisableInterruptionRetryGateForTest()()
			}
			must.OKf(t, s.starter.StartRun(s.ctx, runID, compile(t, interruptionFixtureDocument), nil,
				interruptionFixturePlan(s, nil, mutate)), "StartRun")
			runErr := s.client.GetWorkflow(s.ctx, runID, "").Get(s.ctx, nil)
			if wantOK != (runErr == nil) {
				t.Fatalf("%s ended with %v, want success %v", label, runErr, wantOK)
			}
		}()
		recorded := s.history(runID, "")
		if markers := versionMarkers(t, recorded); legacy != (markers[engine.InterruptionRetryChange] == 0) {
			t.Fatalf("%s: legacy=%v but markers %v", label, legacy, markers)
		}
		must.OKf(t, replayRecorded(recorded, runID), "%s just recorded does not replay", label)
		t.Logf("%s: work ran %d times", label, orker.count("work"))
		keep(path, recorded)
	}

	// The legacy shape turns the gate off for the whole process, so it runs
	// first, alone, before the parallel shapes start.
	t.Run("legacy", func(t *testing.T) {
		single(t, "ir-legacy", []orkerStep{{err: stopBounceError()}}, nil, false, interruptionLegacy, true)
	})
	t.Run("running-beat-started", func(t *testing.T) {
		t.Parallel()
		single(t, "ir-beat", []orkerStep{{beat: &task.HeartbeatProgress{Phase: "run", OrkerID: "orker-fixture", Started: true}}},
			nil, true, interruptionBeatStarted, false)
	})
	t.Run("lost-while-stopping", func(t *testing.T) {
		t.Parallel()
		single(t, "ir-stopping", []orkerStep{{beat: &task.HeartbeatProgress{Phase: "run", OrkerID: "orker-fixture",
			Started: true, StoppingSince: stoppingSince}}}, nil, true, interruptionLostStopping, false)
	})
	t.Run("store-fault-budget", func(t *testing.T) {
		t.Parallel()
		fault := orkerStep{err: storeFaultError()}
		single(t, "ir-store", []orkerStep{fault, fault, fault}, func(b *engine.InterruptionBudgets) {
			b.NeverStartedFaults, b.NeverStartedBackoffMillis, b.NeverStartedFaultFloorMillis = 2, 1000, 1000
		}, false, interruptionStoreFault, false)
	})
	t.Run("profile-drifted", func(t *testing.T) {
		t.Parallel()
		single(t, "ir-drift", []orkerStep{{err: interruptionError("PROFILE_DRIFTED",
			&orkstackv1.ActivityFailureDetail{NeverStarted: true, OrkerId: "orker-fixture"})}}, nil, true, interruptionProfileDrifted, false)
	})
	t.Run("item-continuation", func(t *testing.T) {
		t.Parallel()
		fault := orkerStep{err: storeFaultError()}
		orker := &scriptedOrker{steps: map[string][]orkerStep{"spread#0": {fault, fault}}, calls: map[string]int{}}
		s := startPacedCANStack(t, ctx, "ir-items", orker.behavior, nil)
		runID := fmt.Sprintf("ork-ir-items-%d", time.Now().UnixNano())
		budget := engine.DefaultContinuationBudget(30, 1<<20, 3*time.Minute)
		plan := interruptionFixturePlan(s, &budget, func(b *engine.InterruptionBudgets) { b.NeverStartedFaultFloorMillis = 20000 })
		must.OKf(t, s.starter.StartRun(s.ctx, runID, compile(t, interruptionItemsDocument),
			payload(t, map[string]any{"parts": []any{"x", "y"}}), plan), "StartRun")
		must.OKf(t, s.client.GetWorkflow(s.ctx, runID, "").Get(s.ctx, nil), "the item run failed")
		described, err := s.client.DescribeWorkflowExecution(s.ctx, runID, "")
		must.OK(t, err)
		chain := s.chainOf(runID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
		if len(chain) != 2 {
			t.Fatalf("the item run had %d generations, want 2", len(chain))
		}
		if got := orker.count("spread#0"); got != 3 {
			t.Fatalf("item 0 ran %d times, want 3", got)
		}
		g0, g1 := s.history(runID, chain[0]), s.history(runID, chain[1])
		if !continuedAsNew(g0) {
			t.Fatal("generation 0 did not carry on")
		}
		for name, h := range map[string]*historypb.History{"g0": g0, "g1": g1} {
			must.OKf(t, replayRecorded(h, runID), "item %s just recorded does not replay", name)
		}
		keep(interruptionItemContinuedG0, g0)
		keep(interruptionItemContinuedG1, g1)
	})
}

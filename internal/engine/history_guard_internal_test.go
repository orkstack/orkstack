package engine

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
)

// fakeFuture stands in for an in-flight activity's or a pin's future where
// only its presence, or whether it is ready, matters.
type fakeFuture struct{ ready bool }

func (f *fakeFuture) Get(workflow.Context, interface{}) error { return nil }
func (f *fakeFuture) IsReady() bool                           { return f.ready }

func guardCompile(t *testing.T, document string) *orkstackv1.Orkflow {
	t.Helper()
	wf, err := dsl.Compile([]byte(document))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return wf
}

// guardPlan is a routable plan with a history limit at the default reserve.
func guardPlan(t *testing.T, maxEvents, maxBytes int64) RunPlan {
	t.Helper()
	route, err := task.RouteForPool(task.DefaultPool, task.DefaultQueue)
	if err != nil {
		t.Fatal(err)
	}
	return RunPlan{
		Namespace: "default",
		Routes:    map[string]string{route.Pool: route.Queue},
		History:   &HistoryLimit{MaxEvents: maxEvents, MaxBytes: maxBytes, Reserve: DefaultHistoryReserve},
	}
}

func guardInput(t *testing.T, values map[string]any) *orkstackv1.Payload {
	t.Helper()
	data, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatal(err)
	}
	return &orkstackv1.Payload{Data: data}
}

func itemsArray(n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = fmt.Sprintf("r%d", i)
	}
	return out
}

const guardFanOutDocument = `
name: guard-fan
inputs:
  regions: {type: array}
tasks:
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.regions }}", max_items: 1000}
    with: {op: exec, r: "{{ .each.item }}"}
`

// expandForTest puts a fan-out into the state expandEach leaves it in, with
// count PENDING items, without its events.
func expandForTest(s *runState, id string, count int32) {
	e := s.eaches[id]
	e.expanded = true
	e.count = count
	e.limit = count
	e.stride = 1
	node := s.taskRuns[id]
	node.Status = orkstackv1.Status_STATUS_RUNNING
	node.Items = make([]*orkstackv1.TaskRun, count)
	for i := range node.Items {
		node.Items[i] = &orkstackv1.TaskRun{TaskId: id, Item: proto.Int32(int32(i)), Status: orkstackv1.Status_STATUS_PENDING}
	}
}

// runGuardWorkflow runs body as a workflow of its own, with ExecuteTask
// answering at once, and fails the test when it does not complete.
func runGuardWorkflow(t *testing.T, setup func(env *testsuite.TestWorkflowEnvironment), body func(ctx workflow.Context) error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{}, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	if setup != nil {
		setup(env)
	}
	env.ExecuteWorkflow(body)
	if !env.IsWorkflowCompleted() {
		t.Fatal("the test workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("the test workflow failed: %v", err)
	}
}

// TestHistoryNow_CountsCommandsOfTheCurrentWorkflowTask: one dispatch batch of
// 125 items, issued in one workflow task with an unchanged raw length, raises
// historyNow by 125 events and by the batch's bytes — each item's input bound
// plus the plan's CommandBytes — and the count resets once the raw length
// moves.
func TestHistoryNow_CountsCommandsOfTheCurrentWorkflowTask(t *testing.T) {
	wf := guardCompile(t, guardFanOutDocument)
	input := guardInput(t, map[string]any{"regions": itemsArray(125)})
	type observed struct{ events, bytes int64 }
	var before, afterBatch, afterMove observed
	var dispatched int
	var perItem int64
	runGuardWorkflow(t, func(env *testsuite.TestWorkflowEnvironment) {
		env.SetCurrentHistoryLength(500)
		env.SetCurrentHistorySize(100_000)
		env.RegisterDelayedCallback(func() {
			env.SetCurrentHistoryLength(900)
			env.SetCurrentHistorySize(400_000)
		}, 500*time.Millisecond)
	}, func(ctx workflow.Context) error {
		s := newRunState(ctx, "ork-guard", wf, input, guardPlan(t, 1_000_000, 1<<40))
		expandForTest(s, "convert", 125)
		perItem = s.itemInputLimit + s.plan.History.Reserve.CommandBytes
		before.events, before.bytes = s.historyNow(ctx)
		inFlight := map[string]workflow.Future{}
		if err := s.dispatchItems(ctx, ctx, "convert", inFlight); err != nil {
			return err
		}
		dispatched = len(inFlight)
		afterBatch.events, afterBatch.bytes = s.historyNow(ctx)
		if err := workflow.Sleep(ctx, time.Second); err != nil {
			return err
		}
		afterMove.events, afterMove.bytes = s.historyNow(ctx)
		return nil
	})
	if dispatched != 125 {
		t.Fatalf("dispatched %d items, want one batch of 125", dispatched)
	}
	if before != (observed{500, 100_000}) {
		t.Fatalf("before the batch historyNow = %+v, want the raw counters", before)
	}
	if got := afterBatch.events - before.events; got != 125 {
		t.Errorf("the batch raised historyNow by %d events, want 125", got)
	}
	if got := afterBatch.bytes - before.bytes; got != 125*perItem {
		t.Errorf("the batch raised historyNow by %d bytes, want 125 × %d", got, perItem)
	}
	if afterMove != (observed{900, 400_000}) {
		t.Errorf("after the raw length moved historyNow = %+v, want exactly the new raw counters", afterMove)
	}
}

// TestNoteCommand_ResetsWhenTheRawLengthMoved: a count noted in a new
// workflow task never inherits the previous one's.
func TestNoteCommand_ResetsWhenTheRawLengthMoved(t *testing.T) {
	wf := guardCompile(t, guardFanOutDocument)
	var first, second int64
	runGuardWorkflow(t, func(env *testsuite.TestWorkflowEnvironment) {
		env.SetCurrentHistoryLength(100)
		env.RegisterDelayedCallback(func() { env.SetCurrentHistoryLength(140) }, 500*time.Millisecond)
	}, func(ctx workflow.Context) error {
		s := newRunState(ctx, "ork-guard", wf, nil, guardPlan(t, 1_000_000, 1<<40))
		for i := 0; i < 3; i++ {
			s.noteCommand(ctx, 1, 10)
		}
		first, _ = s.historyNow(ctx)
		if err := workflow.Sleep(ctx, time.Second); err != nil {
			return err
		}
		s.noteCommand(ctx, 1, 10)
		second, _ = s.historyNow(ctx)
		return nil
	})
	if first != 103 {
		t.Errorf("three commands over raw 100 = %d, want 103", first)
	}
	if second != 141 {
		t.Errorf("one command after the raw length moved to 140 = %d, want 141 (none inherited)", second)
	}
}

// TestNoteCommand_ChargesThePlansBytes: the bytes a dispatch is charged come
// from the plan's reserve, so a plan whose CommandBytes differs from the
// default crosses the bytes line at a different point.
func TestNoteCommand_ChargesThePlansBytes(t *testing.T) {
	wf := guardCompile(t, guardFanOutDocument)
	input := guardInput(t, map[string]any{"regions": itemsArray(10)})
	// measure dispatches ten items under commandBytes and answers where the
	// run stands against maxBytes, and whether it crossed.
	measure := func(commandBytes, maxBytes int64) (bytes, reserveBytes int64, crossed bool) {
		runGuardWorkflow(t, nil, func(ctx workflow.Context) error {
			plan := guardPlan(t, 1_000_000, 1<<40)
			plan.History.Reserve.CommandBytes = commandBytes
			s := newRunState(ctx, "ork-guard", wf, input, plan)
			expandForTest(s, "convert", 10)
			inFlight := map[string]workflow.Future{}
			if err := s.dispatchItems(ctx, ctx, "convert", inFlight); err != nil {
				return err
			}
			// The line is drawn after the batch, so the guard inside the
			// dispatch never stops it; only the measurement below uses it.
			plan.History.MaxBytes = maxBytes
			s.beginPass()
			_, bytes = s.historyNow(ctx)
			_, reserveBytes = s.reserveNow(inFlight)
			crossed = s.nearHistoryLimit(ctx, inFlight, 100)
			return nil
		})
		return bytes, reserveBytes, crossed
	}
	const large = 64 << 10
	smallBytes, smallReserve, _ := measure(DefaultHistoryReserve.CommandBytes, 1<<40)
	largeBytes, largeReserve, _ := measure(large, 1<<40)
	if got, want := largeBytes-smallBytes, int64(10*(large-DefaultHistoryReserve.CommandBytes)); got != want {
		t.Fatalf("ten dispatches under a 64 KiB CommandBytes cost %d more bytes, want %d", got, want)
	}
	if smallReserve != largeReserve {
		t.Fatalf("the reserve moved with CommandBytes: %d vs %d", smallReserve, largeReserve)
	}
	// A bytes limit between the two: the default plan stays under it, the
	// other crosses it, with nothing else different.
	line := smallBytes + smallReserve + (largeBytes-smallBytes)/2
	if _, _, crossed := measure(DefaultHistoryReserve.CommandBytes, line); crossed {
		t.Fatalf("the default CommandBytes crossed a %d-byte line it is under", line)
	}
	if _, _, crossed := measure(large, line); !crossed {
		t.Fatalf("a 64 KiB CommandBytes did not cross a %d-byte line it is over", line)
	}
}

// guardStateDocument has one of everything failReserve counts.
const guardStateDocument = `
name: guard-state
inputs:
  regions: {type: array}
pins:
  build: {}
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: b
    uses: shell
    with: {op: exec}
  - id: approve
    await: {timeout_seconds: 600}
  - id: fan
    uses: shell
    each: {over: "{{ .inputs.regions }}", max_items: 10}
    with: {op: exec, r: "{{ .each.item }}"}
  - id: m1
    uses: shell
    pin: build
    run_as_orker_user: true
    with: {op: exec}
  - id: m2
    uses: shell
    pin: build
    run_as_orker_user: true
    needs: [m1]
    with: {op: exec}
  - id: rollback
    uses: shell
    needs: [a]
    join: all_done
    with: {op: exec}
`

func bareGuardState(t *testing.T) *runState {
	t.Helper()
	wf := guardCompile(t, guardStateDocument)
	s := buildRunState("ork-guard", wf, guardInput(t, map[string]any{"regions": itemsArray(3)}),
		guardPlan(t, DefaultHistoryMaxEvents, DefaultHistoryMaxBytes))
	return s
}

// TestFailReserve: every term, and each dispatch's charge equals the change
// in failReserve, in events — a running task counts PerInFlight only, never
// PerPending as well. The quick bound is never below the exact one.
func TestFailReserve(t *testing.T) {
	r := DefaultHistoryReserve
	s := bareGuardState(t)
	inFlight := map[string]workflow.Future{}
	count := func() (int64, int64) { return s.failReserve(inFlight, r) }
	quick := func() (int64, int64) { return s.quickReserve(inFlight, r) }

	tasks := int64(len(s.order))
	events, bytes := count()
	if want := r.Fixed + r.PerPending*tasks + r.PerHandler; events != want {
		t.Fatalf("all pending: %d events, want Fixed + %d pending + one handler = %d", events, tasks, want)
	}
	if want := int64(task.MaxActivityInputBytes); bytes != want {
		t.Fatalf("all pending: %d bytes, want the pending handler's start (%d)", bytes, want)
	}

	steps := []struct {
		name         string
		apply        func()
		chargeEvents int64
		chargeBytes  int64 // the charge column's own bytes, an upper bound on the change
	}{
		{"schedule a", func() {
			s.taskRuns["a"].Status = orkstackv1.Status_STATUS_RUNNING
			inFlight["a"] = &fakeFuture{}
		}, r.PerInFlight - r.PerPending, r.PerInFlightBytes},
		{"open await approve", func() {
			s.taskRuns["approve"].Status = orkstackv1.Status_STATUS_AWAITING
			s.awaits["approve"].open = true
		}, r.PerAwait - r.PerPending, 0},
		{"open pin build", func() {
			p := s.pins["build"]
			p.opened = true
			p.closed = &fakeFuture{}
		}, r.PerOpenPin, 0},
		{"expand fan", func() { expandForTest(s, "fan", 3) }, r.PerFanOut - r.PerPending, 0},
		{"dispatch item fan[0]", func() {
			s.taskRuns["fan"].Items[0].Status = orkstackv1.Status_STATUS_RUNNING
			inFlight[itemFlightKey("fan", 0)] = &fakeFuture{}
		}, r.PerInFlight, r.PerInFlightBytes},
		{"publish an item event detached", func() {
			s.pendingItemPublishes = append(s.pendingItemPublishes, &fakeFuture{})
		}, r.PerInFlight, 0},
		{"schedule pin member m1", func() {
			s.taskRuns["m1"].Status = orkstackv1.Status_STATUS_RUNNING
			inFlight["m1"] = &fakeFuture{}
		}, r.PerInFlight - r.PerPending, r.PerInFlightBytes},
		{"schedule handler rollback", func() {
			s.taskRuns["rollback"].Status = orkstackv1.Status_STATUS_RUNNING
			inFlight["rollback"] = &fakeFuture{}
		}, r.PerInFlight - r.PerPending, r.PerInFlightBytes},
	}
	for _, step := range steps {
		beforeEvents, beforeBytes := count()
		step.apply()
		afterEvents, afterBytes := count()
		if got := afterEvents - beforeEvents; got != step.chargeEvents {
			t.Errorf("%s: failReserve moved by %d events, its charge is %d", step.name, got, step.chargeEvents)
		}
		if got := (afterEvents-beforeEvents)*r.BytesPerEvent + afterBytes - beforeBytes; got > step.chargeEvents*r.BytesPerEvent+step.chargeBytes {
			t.Errorf("%s: failReserve moved by %d bytes, over its charge", step.name, got)
		}
		qe, qb := quick()
		ee, eb := count()
		if qe < ee || qb < ee*r.BytesPerEvent+eb {
			t.Errorf("%s: the quick bound (%d, %d) is below the exact one (%d, %d)", step.name, qe, qb, ee, eb)
		}
	}
	// A pin that closed, a resolved await and a finished task each lower it.
	beforeEvents, _ := count()
	s.pins["build"].closed = &fakeFuture{ready: true}
	s.awaits["approve"].open = false
	s.taskRuns["approve"].Status = orkstackv1.Status_STATUS_SUCCEEDED
	delete(inFlight, "a")
	s.taskRuns["a"].Status = orkstackv1.Status_STATUS_SUCCEEDED
	if afterEvents, _ := count(); afterEvents != beforeEvents-r.PerOpenPin-r.PerAwait-r.PerInFlight {
		t.Errorf("closing a pin, resolving an await and collecting a task moved failReserve %d → %d",
			beforeEvents, afterEvents)
	}
}

// TestReserveNow_IsAnUpperBoundAcrossAPass is the per-pass reserve's proof: a
// random walk over every transition — schedule, item dispatch, child start,
// detached publish, pin open and close, await open and resolve, fan-out
// expand, skip, completion, a refill in the Selector — asserting after each
// that reserveNow ≥ failReserve, equality right after beginPass, and that a
// pass counts failReserve once.
func TestReserveNow_IsAnUpperBoundAcrossAPass(t *testing.T) {
	r := DefaultHistoryReserve
	for seed := int64(1); seed <= 200; seed++ {
		rng := rand.New(rand.NewSource(seed))
		s := bareGuardState(t)
		inFlight := map[string]workflow.Future{}
		exact := func() (int64, int64) {
			e, b := s.failReserve(inFlight, r)
			return e, e*r.BytesPerEvent + b
		}
		pending := func() []string {
			var out []string
			for _, id := range s.order {
				if s.taskRuns[id].GetStatus() == orkstackv1.Status_STATUS_PENDING &&
					s.awaits[id] == nil && s.eaches[id] == nil {
					out = append(out, id)
				}
			}
			return out
		}
		transitions := []func() string{
			func() string { // schedule (a task or, through the same charge, a child start)
				ids := pending()
				if len(ids) == 0 {
					return ""
				}
				id := ids[rng.Intn(len(ids))]
				s.taskRuns[id].Status = orkstackv1.Status_STATUS_RUNNING
				inFlight[id] = &fakeFuture{}
				s.chargeTaskDispatch()
				return "schedule " + id
			},
			func() string { // skip
				ids := pending()
				if len(ids) == 0 {
					return ""
				}
				id := ids[rng.Intn(len(ids))]
				s.taskRuns[id].Status = orkstackv1.Status_STATUS_SKIPPED
				return "skip " + id
			},
			func() string { // completion of a task
				for _, id := range s.order {
					if _, ok := inFlight[id]; ok {
						delete(inFlight, id)
						s.taskRuns[id].Status = orkstackv1.Status_STATUS_SUCCEEDED
						return "complete " + id
					}
				}
				return ""
			},
			func() string { // await open
				if s.taskRuns["approve"].GetStatus() != orkstackv1.Status_STATUS_PENDING {
					return ""
				}
				s.taskRuns["approve"].Status = orkstackv1.Status_STATUS_AWAITING
				s.awaits["approve"].open = true
				s.chargeAwaitOpen()
				return "open await"
			},
			func() string { // await resolve
				if !s.awaits["approve"].open {
					return ""
				}
				s.awaits["approve"].open = false
				s.taskRuns["approve"].Status = orkstackv1.Status_STATUS_SUCCEEDED
				return "resolve await"
			},
			func() string { // pin open
				p := s.pins["build"]
				if p.opened {
					return ""
				}
				p.opened = true
				p.closed = &fakeFuture{}
				s.chargePinOpen()
				return "open pin"
			},
			func() string { // pin close
				p := s.pins["build"]
				if !p.opened {
					return ""
				}
				p.closed = &fakeFuture{ready: true}
				return "close pin"
			},
			func() string { // fan-out expand
				if s.eaches["fan"].expanded {
					return ""
				}
				expandForTest(s, "fan", 3)
				s.chargeFanOutExpand()
				return "expand"
			},
			func() string { // item dispatch, or a refill after an item's completion
				e := s.eaches["fan"]
				if !e.expanded || e.next >= e.count {
					return ""
				}
				if rng.Intn(2) == 0 && e.inFlight > 0 {
					for index := int32(0); index < e.count; index++ {
						key := itemFlightKey("fan", index)
						if _, ok := inFlight[key]; ok {
							delete(inFlight, key)
							e.inFlight--
							s.taskRuns["fan"].Items[index].Status = orkstackv1.Status_STATUS_SUCCEEDED
							break
						}
					}
				}
				index := e.next
				e.next++
				e.inFlight++
				s.taskRuns["fan"].Items[index].Status = orkstackv1.Status_STATUS_RUNNING
				inFlight[itemFlightKey("fan", index)] = &fakeFuture{}
				s.chargeItemDispatch()
				return "dispatch item"
			},
			func() string { // detached publish, then one drained
				if rng.Intn(2) == 0 && len(s.pendingItemPublishes) > 0 {
					s.pendingItemPublishes = s.pendingItemPublishes[1:]
					return "drain publish"
				}
				s.pendingItemPublishes = append(s.pendingItemPublishes, &fakeFuture{})
				s.chargeDetachedPublish()
				return "publish"
			},
		}
		s.beginPass()
		for step := 0; step < 60; step++ {
			if rng.Intn(6) == 0 {
				s.beginPass()
				re, rb := s.reserveNow(inFlight)
				ee, eb := exact()
				if re != ee || rb != eb {
					t.Fatalf("seed %d step %d: right after beginPass reserveNow = (%d, %d), failReserve = (%d, %d)",
						seed, step, re, rb, ee, eb)
				}
				// A pass counts once: growth that was never charged is not
				// seen until the next pass (which is why every growth path
				// charges).
				s.pendingItemPublishes = append(s.pendingItemPublishes, &fakeFuture{})
				if again, _ := s.reserveNow(inFlight); again != re {
					t.Fatalf("seed %d step %d: reserveNow counted failReserve twice in one pass", seed, step)
				}
				s.pendingItemPublishes = s.pendingItemPublishes[:len(s.pendingItemPublishes)-1]
				continue
			}
			name := transitions[rng.Intn(len(transitions))]()
			if name == "" {
				continue
			}
			re, rb := s.reserveNow(inFlight)
			ee, eb := exact()
			if re < ee || rb < eb {
				t.Fatalf("seed %d step %d (%s): reserveNow = (%d, %d) < failReserve = (%d, %d)",
					seed, step, name, re, rb, ee, eb)
			}
		}
	}
}

// flatDocument is n tasks with no needs: one flat stage.
func flatDocument(n int) string {
	var b strings.Builder
	b.WriteString("name: flat\ntasks:\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "  - id: t%04d\n    uses: shell\n    needs: []\n    with: {op: exec}\n", i)
	}
	return b.String()
}

// TestHistoryGuard_FitsTheDeadlockBudget: a 4,000-task flat stage (the largest
// soak size) in the worst states a generation reaches from its base of 1,502
// events, at the default limit of 51,200 — both inside the band where the
// quick O(1) bound crosses and the exact count does not, so every guard check
// of the pass takes the per-pass path. One pass's guard checks — one per
// ready task, as the ready loop makes them — stay within 20 ms, under the
// production 1 s deadlock detector.
func TestHistoryGuard_FitsTheDeadlockBudget(t *testing.T) {
	wf := guardCompile(t, flatDocument(4000))
	for _, state := range []struct {
		name                 string
		length               int
		inFlight, done       int
		wantQuick, wantExact int64
	}{
		{"586 landed and uncollected", 12_636, 586, 0, 54_108, 49_420},
		{"286 in flight after 300 collections", 14_736, 286, 300, 51_408, 46_720},
	} {
		t.Run(state.name, func(t *testing.T) {
			var elapsed time.Duration
			var quickTotal, exactTotal int64
			var checks int
			var guardErr error
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: time.Second})
			env.SetCurrentHistoryLength(state.length)
			env.ExecuteWorkflow(func(ctx workflow.Context) error {
				s := newRunState(ctx, "ork-flat", wf, nil, guardPlan(t, DefaultHistoryMaxEvents, DefaultHistoryMaxBytes))
				inFlight := map[string]workflow.Future{}
				for i, id := range s.order {
					switch {
					case i < state.done:
						s.taskRuns[id].Status = orkstackv1.Status_STATUS_SUCCEEDED
					case i < state.done+state.inFlight:
						s.taskRuns[id].Status = orkstackv1.Status_STATUS_RUNNING
						inFlight[id] = &fakeFuture{ready: true}
					}
				}
				events, _ := s.historyNow(ctx)
				qe, _ := s.quickReserve(inFlight, s.plan.History.Reserve)
				quickTotal = events + qe
				started := time.Now()
				s.beginPass()
				pendingTasks := len(s.order) - state.done - state.inFlight
				for i := 0; i <= pendingTasks; i++ { // check point 1, then one per ready task
					checks++
					if err := s.historyExhausted(ctx, inFlight); err != nil {
						guardErr = err
						break
					}
				}
				elapsed = time.Since(started)
				re, _ := s.reserveNow(inFlight)
				exactTotal = events + re
				return nil
			})
			if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
				t.Fatalf("the pass did not complete under the 1 s deadlock detector: %v", env.GetWorkflowError())
			}
			if guardErr != nil {
				t.Fatalf("the guard tripped below its line: %v", guardErr)
			}
			if quickTotal != state.wantQuick || exactTotal != state.wantExact {
				t.Fatalf("history + quick = %d, + exact = %d; want %d and %d", quickTotal, exactTotal,
					state.wantQuick, state.wantExact)
			}
			if quickTotal < DefaultHistoryMaxEvents || exactTotal >= DefaultHistoryMaxEvents {
				t.Fatalf("the state is not inside the per-pass band: quick %d, exact %d, limit %d",
					quickTotal, exactTotal, DefaultHistoryMaxEvents)
			}
			t.Logf("E0 deadlock budget (%s): %d guard checks in one pass took %.2f ms (budget 20 ms)",
				state.name, checks, float64(elapsed.Microseconds())/1000)
			if elapsed > 20*time.Millisecond {
				t.Fatalf("one pass's guard checks took %s, over 20 ms", elapsed)
			}
		})
	}
}

// TestHistoryLimitMessage_Format pins the sentence, events and bytes forms,
// with and without what was still running, and with each reason a later
// stage appends.
func TestHistoryLimitMessage_Format(t *testing.T) {
	const blockers = "still running: build, test[17]; pins open: workspace; awaiting: approve"
	for _, c := range []struct {
		name                                              string
		events, maxEvents, bytes, maxBytes, reserveEvents int64
		blockers, carry                                   string
		want                                              string
	}{
		{"events", 48_912, 51_200, 10 << 20, 50 << 20, 2_400, blockers, "",
			"run ork-1 stopped before reaching the cluster's history limit: 48,912 of 51,200 events recorded, " +
				"2,288 kept back so it could end cleanly. Still running: build, test[17]; pins open: workspace; " +
				"awaiting: approve"},
		{"bytes", 1_200, 51_200, 49<<20 + 1<<19, 50 << 20, 300, "awaiting: approve", "",
			"run ork-1 stopped before reaching the cluster's history limit: 49.5 of 50.0 MiB of history recorded, " +
				"0.5 MiB kept back so it could end cleanly. Awaiting: approve"},
		{"nothing running", 600, 600, 0, 50 << 20, 96, "", "",
			"run ork-1 stopped before reaching the cluster's history limit: 600 of 600 events recorded, " +
				"0 kept back so it could end cleanly"},
		{"after a structural give-up", 48_912, 51_200, 10 << 20, 50 << 20, 2_400, "pins open: workspace; awaiting: approve",
			"it could not reach a moment with nothing running, which a run needs to carry on into a fresh history, " +
				"because pin workspace waits on await approve",
			"run ork-1 stopped before reaching the cluster's history limit: 48,912 of 51,200 events recorded, " +
				"2,288 kept back so it could end cleanly. Pins open: workspace; awaiting: approve; it could not reach " +
				"a moment with nothing running, which a run needs to carry on into a fresh history, because pin " +
				"workspace waits on await approve"},
		{"after noSwitch", 48_912, 51_200, 10 << 20, 50 << 20, 2_400, "",
			"it cannot carry on into a fresh history: its saved state is 9.3 MiB, more than the 8 MiB a run carries forward",
			"run ork-1 stopped before reaching the cluster's history limit: 48,912 of 51,200 events recorded, " +
				"2,288 kept back so it could end cleanly; it cannot carry on into a fresh history: its saved state " +
				"is 9.3 MiB, more than the 8 MiB a run carries forward"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := historyLimitMessage("ork-1", c.events, c.maxEvents, c.bytes, c.maxBytes, c.reserveEvents, c.blockers, c.carry)
			if got != c.want {
				t.Fatalf("message:\n got %q\nwant %q", got, c.want)
			}
		})
	}

	// At most five ids per list.
	wf := guardCompile(t, flatDocument(7))
	s := buildRunState("ork-1", wf, nil, guardPlan(t, 100, 1<<20))
	inFlight := map[string]workflow.Future{}
	for _, id := range s.order {
		inFlight[id] = &fakeFuture{}
	}
	if got, want := s.historyBlockers(inFlight), "still running: t0000, t0001, t0002, t0003, t0004, …"; got != want {
		t.Fatalf("blockers = %q, want %q", got, want)
	}
	// A fan-out item is named task[index].
	fan := buildRunState("ork-1", guardCompile(t, guardFanOutDocument),
		guardInput(t, map[string]any{"regions": itemsArray(20)}), guardPlan(t, 100, 1<<20))
	expandForTest(fan, "convert", 20)
	if got, want := fan.historyBlockers(map[string]workflow.Future{itemFlightKey("convert", 17): &fakeFuture{}}),
		"still running: convert[17]"; got != want {
		t.Fatalf("item blockers = %q, want %q", got, want)
	}
}

// TestHistoryGuard_MessageUsesRunVocabulary: no user-facing sentence of the
// guard names Temporal's concepts (AGENTS.md §7 vocabulary).
func TestHistoryGuard_MessageUsesRunVocabulary(t *testing.T) {
	texts := []string{
		historyLimitMessage("ork-1", 48_912, 51_200, 1, 50<<20, 2_400, "still running: a; pins open: p; awaiting: w", ""),
		historyLimitMessage("ork-1", 10, 51_200, 49<<20, 50<<20, 10, "", ""),
		handlerCanceledAtHistoryLimit,
		cancelReasonNotAHandler(""),
		failingMessage("", []string{"rollback"}),
	}
	for _, text := range texts {
		lower := strings.ToLower(text)
		for _, word := range []string{"execution", "temporal", "generation", "continue"} {
			if strings.Contains(lower, word) {
				t.Errorf("%q names %q", text, word)
			}
		}
	}
	// The task-level sentences are unchanged.
	if got := cancelReasonNotAHandler("deploy"); got != `the run is failing (task "deploy" failed) and this task is not a failure handler` {
		t.Errorf("task-level cancel reason changed: %q", got)
	}
	if got := failingMessage("deploy", []string{"rollback", "notify"}); got != `run failing after task "deploy" failed; waiting for its failure handlers: rollback, notify` {
		t.Errorf("task-level failing note changed: %q", got)
	}
}

// TestClosedExecution_MapsRunLevelFailureTypes: a workflow failure whose type
// is the run's own code reads as that code, for a run whose terminal emit did
// not land; any other type stays RUN_FAILED.
func TestClosedExecution_MapsRunLevelFailureTypes(t *testing.T) {
	for _, c := range []struct {
		failureType, wantCode string
		wantCategory          orkstackv1.FailureCategory
	}{
		{RunHistoryLimitCode, RunHistoryLimitCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR},
		{task.CategoryActivityFailed, "RUN_FAILED", orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED},
		{"", "RUN_FAILED", orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED},
	} {
		event := &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
				WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
					Failure: &failurepb.Failure{
						Message: "the run's sentence",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: c.failureType},
						},
					},
				},
			},
		}
		status, failure, ok := closedExecution(event)
		if !ok || status != orkstackv1.Status_STATUS_FAILED || failure.GetCode() != c.wantCode ||
			failure.GetCategory() != c.wantCategory || failure.GetMessage() != "the run's sentence" {
			t.Errorf("type %q: closedExecution = %v %v %v", c.failureType, status, failure, ok)
		}
	}
}

// TestChildFailure_MapsRunHistoryLimitToInfrastructureError: a child that
// stopped at its history limit reaches its parent as CHILD_RUN_FAILED with a
// category, never UNSPECIFIED.
func TestChildFailure_MapsRunHistoryLimitToInfrastructureError(t *testing.T) {
	status, code, category := childFailure(temporal.NewNonRetryableApplicationError(
		"run ork-c stopped before reaching the cluster's history limit", RunHistoryLimitCode, nil))
	if status != orkstackv1.Status_STATUS_FAILED || code != ChildRunFailedCode ||
		category != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
		t.Fatalf("childFailure(RUN_HISTORY_LIMIT) = %v %s %v, want FAILED CHILD_RUN_FAILED InfrastructureError",
			status, code, category)
	}
}

// TestIsHistoryLimit_OnlyTheGuardsOwnError: a task's or a child's error that
// merely carries the type is not the run's own history limit.
func TestIsHistoryLimit_OnlyTheGuardsOwnError(t *testing.T) {
	own := temporal.NewNonRetryableApplicationError("stopped", RunHistoryLimitCode, nil)
	if !isHistoryLimit(own) {
		t.Fatal("the guard's own error is not recognised")
	}
	if isHistoryLimit(temporal.NewNonRetryableApplicationError("x", task.CategoryActivityFailed, nil)) {
		t.Fatal("another type reads as the history limit")
	}
	if isHistoryLimit(nil) {
		t.Fatal("nil reads as the history limit")
	}
}

const historyLimitFixturePath = "testdata/history/run-history-limit-v1.json"

func replayWithGuard(t *testing.T, path string, set func()) error {
	t.Helper()
	set()
	defer func() {
		historyGuardDisabledForTest = false
		historyGuardPlanForTest = nil
		historyGuardForcedForTest = false
	}()
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	return replayer.ReplayWorkflowHistoryFromJSONFile(nil, path)
}

// TestHistoryLimitHistory_CatchesAGuardRemoved: the history-limit fixture
// replayed against a body without the guard must fail — it sees the guard.
func TestHistoryLimitHistory_CatchesAGuardRemoved(t *testing.T) {
	if _, err := os.Stat(historyLimitFixturePath); err != nil {
		t.Skipf("%s is not recorded: %v", historyLimitFixturePath, err)
	}
	requireDeterminismViolation(t, replayWithGuard(t, historyLimitFixturePath, func() {
		historyGuardDisabledForTest = true
	}), "replaying the history-limit fixture against a body without the guard")
}

// TestCommittedHistory_HistoryGuardWithoutItsGateIsCaught: a committed
// history recorded before the guard, handed a tiny limit, replays clean only
// because the guard asks its version — DefaultVersion there — and a body that
// takes version 1 without asking must fail.
func TestCommittedHistory_HistoryGuardWithoutItsGateIsCaught(t *testing.T) {
	const path = "testdata/history/run-succeeded-v2.json"
	tiny := &HistoryLimit{MaxEvents: 100, MaxBytes: 64 << 10, Reserve: DefaultHistoryReserve}
	if err := replayWithGuard(t, path, func() { historyGuardPlanForTest = tiny }); err != nil {
		t.Fatalf("replaying %s with a tiny limit behind the gate failed: %v", path, err)
	}
	requireDeterminismViolation(t, replayWithGuard(t, path, func() {
		historyGuardPlanForTest = tiny
		historyGuardForcedForTest = true
	}), "replaying "+path+" against the ungated guard")
}

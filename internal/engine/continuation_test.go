package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// continuedPlan is guardedPlan with a continuation budget whose event growth
// is events: the plan pinRunTreeLimits builds with continuation on, at a test
// size. In a TestWorkflowEnvironment the raw history length stays where a
// test set it, so a generation's growth is the commands it noted — one per
// dispatch — and events is "switch due after this many dispatches".
func continuedPlan(events int64) engine.RunPlan {
	plan := guardedPlan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes)
	budget := engine.DefaultContinuationBudget(events, 1<<20, 5*time.Minute)
	plan.Continuation = &budget
	return plan
}

// generations is one run driven across its generations: every
// generation's harness (sharing one snapshot store), the carry each
// carried one started from, and the last generation's workflow error.
type generations struct {
	runID     string
	harnesses []*harness
	carries   []*orkstackv1.RunContinuation
	plans     []engine.RunPlan
	err       error
}

// runGenerations runs RunWorkflow, and every generation its continue-as-new
// starts, from the returned ContinueAsNewError's own arguments, until a
// generation ends without one. setup, when set, prepares each generation's
// harness before it starts.
func runGenerations(
	t *testing.T,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	plan engine.RunPlan,
	behavior func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error),
	setup func(h *harness, generation int),
) *generations {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	return runGenerationsIn(t, &suite, runID, wf, input, plan, behavior, setup)
}

// runGenerationsIn is runGenerations under a suite the caller configured (a
// logger, say).
func runGenerationsIn(
	t *testing.T,
	suite *testsuite.WorkflowTestSuite,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	plan engine.RunPlan,
	behavior func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error),
	setup func(h *harness, generation int),
) *generations {
	t.Helper()
	store := newTestSnapshotStore()
	values := &memValues{objects: map[string][]byte{}}
	out := &generations{runID: runID}
	args := []any{runID, wf, input, plan, (*orkstackv1.RunContinuation)(nil)}
	for generation := 0; generation < 300; generation++ {
		h := newHarnessShared(t, suite, store, values, behavior)
		h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: runID})
		if setup != nil {
			setup(h, generation)
		}
		out.harnesses = append(out.harnesses, h)
		h.env.ExecuteWorkflow(engine.RunWorkflowType, args...)
		if !h.env.IsWorkflowCompleted() {
			t.Fatalf("generation %d did not end", generation)
		}
		err := h.env.GetWorkflowError()
		var can *workflow.ContinueAsNewError
		if !errors.As(err, &can) {
			out.err = err
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
		if can.WorkflowType.Name != engine.RunWorkflowType {
			t.Fatalf("generation %d continued as %q", generation, can.WorkflowType.Name)
		}
		out.carries = append(out.carries, carry)
		out.plans = append(out.plans, nextPlan)
		args = []any{nextID, nextWf, nextInput, nextPlan, carry}
	}
	t.Fatal("the run did not end within 300 generations")
	return nil
}

// events is every generation's published events, concatenated.
func (g *generations) events() []*orkstackv1.Event {
	var out []*orkstackv1.Event
	for _, h := range g.harnesses {
		out = append(out, h.bus.Events(g.runID, 0)...)
	}
	return out
}

// final is the run as the last generation's store holds it.
func (g *generations) final(t *testing.T) *orkstackv1.Run {
	t.Helper()
	return mustFinalRun(t, g.harnesses[len(g.harnesses)-1].store, g.runID)
}

// invocations is every ExecuteTask input of every generation, in order.
func (g *generations) invocations() []*orkstackv1.ExecuteTaskInput {
	var out []*orkstackv1.ExecuteTaskInput
	for _, h := range g.harnesses {
		out = append(out, h.invocations()...)
	}
	return out
}

// eventKey is an event without its timestamp: what a switch must leave
// unchanged.
func eventKey(e *orkstackv1.Event) string {
	item := ""
	if e.Item != nil {
		item = fmt.Sprintf("[%d]", e.GetItem())
	}
	return fmt.Sprintf("%d %s %s%s %s %q", e.GetSequence(), e.GetType(), e.GetTaskId(), item, e.GetStatus(), e.GetMessage())
}

func eventKeys(events []*orkstackv1.Event) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, eventKey(e))
	}
	return out
}

// requireContiguous asserts sequences 1..N with no gap or duplicate.
func requireContiguous(t *testing.T, events []*orkstackv1.Event) {
	t.Helper()
	for i, e := range events {
		if e.GetSequence() != int64(i+1) {
			t.Fatalf("event %d has sequence %d, want %d: %v", i, e.GetSequence(), i+1, eventKeys(events))
		}
	}
}

func okBehavior(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	return succeeded(in.GetTaskId(), map[string]any{"task": in.GetTaskId()}), nil
}

const continuationChainDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: chain
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec}
  - id: c
    uses: shell
    needs: [b]
    with: {op: exec}
  - id: d
    uses: shell
    needs: [c]
    with: {op: exec}
  - id: e
    uses: shell
    needs: [d]
    with: {op: exec}
`

// TestContinuation_SwitchesAtANaturalIdlePointWithoutHolding: a chain past
// its budget switches at the next moment its in-flight set empties, never
// holding, and the run reaches SUCCEEDED across generations with one
// contiguous stream.
func TestContinuation_SwitchesAtANaturalIdlePointWithoutHolding(t *testing.T) {
	g := runGenerations(t, "ork-chain", compile(t, continuationChainDocument), payload(t, nil),
		continuedPlan(2), okBehavior, nil)
	if g.err != nil {
		t.Fatalf("run failed: %v", g.err)
	}
	if len(g.harnesses) < 2 {
		t.Fatalf("the run had %d generations, want at least 2", len(g.harnesses))
	}
	run := g.final(t)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("status = %v, want SUCCEEDED", run.GetStatus())
	}
	events := g.events()
	requireContiguous(t, events)
	for i, carry := range g.carries {
		if carry.GetGeneration() != int64(i+1) || int64(len(carry.GetSegments())) != carry.GetGeneration() {
			t.Fatalf("carry %d = generation %d with %d segments", i, carry.GetGeneration(), len(carry.GetSegments()))
		}
	}
	if got := strings.Join(g.harnesses[0].taskOrder(), ","); !strings.HasPrefix(got, "a,b") {
		t.Fatalf("generation 0 ran %q, want a then b", got)
	}
	_ = proto.Equal
}

// commandRecorder records, in order, the commands and decisions a run's
// workflow code makes: activities, local activities (with their options),
// timers, memo upserts, version asks and the continue-as-new.
type commandRecorder struct {
	interceptor.WorkerInterceptorBase
	mu       sync.Mutex
	commands []string
	chunks   []workflow.LocalActivityOptions
}

func (r *commandRecorder) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &commandRecorderInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, r: r}
}

type commandRecorderInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	r *commandRecorder
}

func (i *commandRecorderInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&commandRecorderOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, r: i.r})
}

type commandRecorderOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	r *commandRecorder
}

func (o *commandRecorderOutbound) add(command string) {
	o.r.mu.Lock()
	defer o.r.mu.Unlock()
	o.r.commands = append(o.r.commands, command)
}

func (o *commandRecorderOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	o.add("activity:" + activityType)
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

func (o *commandRecorderOutbound) ExecuteLocalActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	o.add("local:" + activityType)
	if activityType == engine.ActivityReadContinuationChunk {
		o.r.mu.Lock()
		o.r.chunks = append(o.r.chunks, workflow.GetLocalActivityOptions(ctx))
		o.r.mu.Unlock()
	}
	return o.Next.ExecuteLocalActivity(ctx, activityType, args...)
}

func (o *commandRecorderOutbound) NewTimer(ctx workflow.Context, d time.Duration) workflow.Future {
	o.add(fmt.Sprintf("timer:%s", d))
	return o.Next.NewTimer(ctx, d)
}

func (o *commandRecorderOutbound) Sleep(ctx workflow.Context, d time.Duration) error {
	o.add(fmt.Sprintf("timer:%s", d))
	return o.Next.Sleep(ctx, d)
}

func (o *commandRecorderOutbound) UpsertMemo(ctx workflow.Context, memo map[string]any) error {
	o.add("memo")
	return o.Next.UpsertMemo(ctx, memo)
}

func (o *commandRecorderOutbound) GetVersion(ctx workflow.Context, changeID string, minSupported, maxSupported workflow.Version) workflow.Version {
	o.add("version:" + changeID)
	return o.Next.GetVersion(ctx, changeID, minSupported, maxSupported)
}

func (o *commandRecorderOutbound) NewContinueAsNewError(ctx workflow.Context, wfn any, args ...any) error {
	o.add("continue-as-new")
	return o.Next.NewContinueAsNewError(ctx, wfn, args...)
}

func (r *commandRecorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.commands...)
}

func (r *commandRecorder) count(command string) int {
	n := 0
	for _, c := range r.list() {
		if c == command {
			n++
		}
	}
	return n
}

// recordCommands installs a fresh recorder on h.
func recordCommands(h *harness) *commandRecorder {
	r := &commandRecorder{}
	h.env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{r}})
	return r
}

const spreadDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: spread
inputs:
  parts: {type: array}
tasks:
  - id: spread
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 100, max_parallel: 2}
    with: {op: exec, part: "{{ .each.item }}"}
  - id: after
    uses: shell
    needs: [spread]
    with: {op: exec}
`

func spreadInput(t *testing.T, n int) *orkstackv1.Payload {
	return payload(t, map[string]any{"parts": guardItems(n)})
}

func itemBehavior(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	if in.GetEach() != nil {
		return itemResult(in), nil
	}
	return okBehavior(context.Background(), in)
}

// itemsSucceeded counts a fan-out's SUCCEEDED items in a snapshot.
func itemsSucceeded(run *orkstackv1.Run, id string) int {
	n := 0
	for _, row := range run.GetTaskRuns() {
		if row.GetTaskId() != id {
			continue
		}
		for _, item := range row.GetItems() {
			if item.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
				n++
			}
		}
	}
	return n
}

// raiseHistoryAt moves generation 0's raw history length to length once the
// fan-out spread has n items SUCCEEDED, so its next check sees the jump.
func raiseHistoryAt(h *harness, n, length int) {
	var once sync.Once
	h.onSnapshot = func(run *orkstackv1.Run, _ int64) error {
		if itemsSucceeded(run, "spread") >= n {
			once.Do(func() { h.env.SetCurrentHistoryLength(length) })
		}
		return nil
	}
}

// itemsDispatched is the fan-out item indices a harness's Orker received,
// ascending (items dispatched together start in any order).
func itemsDispatched(h *harness) []int32 {
	var out []int32
	for _, in := range h.invocations() {
		if in.GetEach() != nil {
			out = append(out, in.GetEach().GetIndex())
		}
	}
	slices.Sort(out)
	return out
}

// holdPlan is continuedPlan(2) under a 1,000-event limit: the hold line is
// 600 events of history plus reserve.
func holdPlan() engine.RunPlan {
	plan := continuedPlan(2)
	plan.History.MaxEvents = 1000
	return plan
}

// TestContinuation_PastTheBudgetDoesNotHoldBelowTheHeadroom: a refilling
// fan-out past its budget never reaches an idle point, and keeps dispatching
// while its history plus reserve is under the hold line: no hold, every item
// in the first generation.
func TestContinuation_PastTheBudgetDoesNotHoldBelowTheHeadroom(t *testing.T) {
	g := runGenerations(t, "ork-headroom", compile(t, spreadDocument), spreadInput(t, 10), continuedPlan(2), itemBehavior, nil)
	if g.err != nil {
		t.Fatalf("run failed: %v", g.err)
	}
	if got := len(itemsDispatched(g.harnesses[0])); got != 10 {
		t.Fatalf("generation 0 dispatched %d items, want all 10: below the hold line nothing is held", got)
	}
}

// TestContinuation_QuiesceHoldsNewDispatchUntilIdle: once history plus
// reserve reaches the hold line, no new item starts; the items running
// finish, the run switches with nothing in flight, and the next generation
// resumes the fan-out at its next index (FanOutResumesAtNextIndex).
func TestContinuation_QuiesceHoldsNewDispatchUntilIdle(t *testing.T) {
	g := runGenerations(t, "ork-quiesce", compile(t, spreadDocument), spreadInput(t, 8), holdPlan(), itemBehavior,
		func(h *harness, generation int) {
			if generation == 0 {
				raiseHistoryAt(h, 2, 650)
			}
		})
	if g.err != nil {
		t.Fatalf("run failed: %v", g.err)
	}
	if len(g.harnesses) < 2 {
		t.Fatalf("%d generations, want the hold to end in a switch", len(g.harnesses))
	}
	first, second := itemsDispatched(g.harnesses[0]), itemsDispatched(g.harnesses[1])
	if len(first) >= 8 || len(first) < 3 {
		t.Fatalf("generation 0 dispatched items %v, want the hold to stop it part-way", first)
	}
	if len(second) == 0 || second[0] != int32(len(first)) {
		t.Fatalf("generation 1 dispatched items %v after %v, want it to resume at index %d", second, first, len(first))
	}
	seen := map[int32]int{}
	for _, h := range g.harnesses {
		for _, index := range itemsDispatched(h) {
			seen[index]++
		}
	}
	for index := int32(0); index < 8; index++ {
		if seen[index] != 1 {
			t.Fatalf("item %d was dispatched %d times across generations, want once", index, seen[index])
		}
	}
	if run := g.final(t); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("status = %v", run.GetStatus())
	}
	requireContiguous(t, g.events())
}

// TestContinuation_HoldHasNoClock: items in flight for three hours of the
// workflow's clock keep the hold waiting — no wall-clock bound gives it up —
// and the run then switches.
func TestContinuation_HoldHasNoClock(t *testing.T) {
	var calls atomic.Int32
	slow := func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return itemBehavior(ctx, in)
	}
	g := runGenerations(t, "ork-noclock", compile(t, spreadDocument), spreadInput(t, 6), holdPlan(), slow,
		func(h *harness, generation int) {
			if generation != 0 {
				return
			}
			raiseHistoryAt(h, 2, 650)
			h.env.OnActivity(task.ActivityExecuteTask, mock.Anything, mock.Anything).After(3 * time.Hour).
				Return(func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
					calls.Add(1)
					return itemBehavior(ctx, in)
				})
		})
	if g.err != nil {
		t.Fatalf("run failed: %v", g.err)
	}
	if len(g.harnesses) < 2 {
		t.Fatalf("%d generations: the hold must wait out the three-hour items, then switch", len(g.harnesses))
	}
	if calls.Load() == 0 {
		t.Fatal("no three-hour item ran")
	}
}

// decodeCarried reads and decodes the switch snapshot and the checkpoint a
// carry names, from the run's shared store.
func decodeCarried(t *testing.T, store *testSnapshotStore, carry *orkstackv1.RunContinuation) (snapshot, checkpoint *orkstackv1.Run) {
	t.Helper()
	data, ok := store.get(carry.GetSnapshot().GetUri())
	if !ok {
		t.Fatalf("the switch snapshot %q was never written", carry.GetSnapshot().GetUri())
	}
	snapshot, err := engine.DecodeSwitchSnapshot(data, carry.GetSnapshot())
	if err != nil {
		t.Fatalf("decode the switch snapshot: %v", err)
	}
	data, ok = store.get(carry.GetCheckpoint().GetUri())
	if !ok {
		t.Fatalf("the checkpoint %q was never written", carry.GetCheckpoint().GetUri())
	}
	checkpoint, err = engine.DecodeCheckpoint(data, carry.GetCheckpoint())
	if err != nil {
		t.Fatalf("decode the checkpoint: %v", err)
	}
	return snapshot, checkpoint
}

// TestContinuation_SwitchCarriesSequenceCreatedAtStartedAtDecisionsAndSwitchSnapshot:
// the carry holds the last sequence assigned, the run's created_at and
// started_at, every version decision so far, the segment of the generation
// that closed and a switch snapshot of the exact state.
func TestContinuation_SwitchCarriesSequenceCreatedAtStartedAtDecisionsAndSwitchSnapshot(t *testing.T) {
	g := runGenerations(t, "ork-carry", compile(t, continuationChainDocument), payload(t, nil), continuedPlan(2), okBehavior, nil)
	if g.err != nil || len(g.carries) == 0 {
		t.Fatalf("run = %v with %d switches", g.err, len(g.carries))
	}
	gen0 := g.harnesses[0]
	events := gen0.bus.Events(g.runID, 0)
	carry := g.carries[0]
	if carry.GetVersion() != engine.ContinuationCarryVersion || carry.GetGeneration() != 1 {
		t.Fatalf("carry version %d generation %d", carry.GetVersion(), carry.GetGeneration())
	}
	if last := events[len(events)-1].GetSequence(); carry.GetEventSequence() != last {
		t.Fatalf("carried sequence %d, generation 0's last event is %d", carry.GetEventSequence(), last)
	}
	created := mustFinalRun(t, gen0.store, g.runID)
	if !proto.Equal(carry.GetCreatedAt(), created.GetCreatedAt()) || !proto.Equal(carry.GetStartedAt(), created.GetStartedAt()) ||
		carry.GetCreatedAt() == nil || carry.GetStartedAt() == nil {
		t.Fatalf("carried created %v started %v, the run's %v %v", carry.GetCreatedAt(), carry.GetStartedAt(),
			created.GetCreatedAt(), created.GetStartedAt())
	}
	decisions := carry.GetVersionDecisions()
	if decisions[engine.ContinueAsNewChange] != 1 || decisions["lean-event-path"] != 1 {
		t.Fatalf("carried decisions %v, want continue-as-new and lean-event-path at 1", decisions)
	}
	segments := carry.GetSegments()
	if len(segments) != 1 || segments[0].GetFirstSequence() != 1 || segments[0].GetLastSequence() != carry.GetEventSequence() ||
		segments[0].GetEventCount() != int64(len(events)) || segments[0].GetExecutionId() == "" {
		t.Fatalf("segments = %v, want generation 0's events 1–%d", segments, carry.GetEventSequence())
	}
	data, ok := g.harnesses[0].snapshots.get(segments[0].GetObject().GetUri())
	if !ok {
		t.Fatal("the segment was never written")
	}
	segment, err := engine.DecodeEventSegment(data, segments[0].GetObject())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(eventKeys(segment.GetEvents()), "\n") != strings.Join(eventKeys(events), "\n") {
		t.Fatalf("segment events\n%v\nwant what generation 0 published\n%v", eventKeys(segment.GetEvents()), eventKeys(events))
	}
	if !strings.Contains(carry.GetSnapshot().GetUri(), "/continuations/000000-snapshot-") {
		t.Fatalf("switch snapshot %q is not a generation-0 content-addressed key", carry.GetSnapshot().GetUri())
	}
	snapshot, checkpoint := decodeCarried(t, g.harnesses[0].snapshots, carry)
	for _, run := range []*orkstackv1.Run{snapshot, checkpoint} {
		if statusOf(run, "a") != orkstackv1.Status_STATUS_SUCCEEDED || statusOf(run, "b") != orkstackv1.Status_STATUS_SUCCEEDED ||
			statusOf(run, "c") != orkstackv1.Status_STATUS_PENDING {
			t.Fatalf("saved state rows = %v", run.GetTaskRuns())
		}
	}
}

const defaultedInputDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: defaulted
inputs:
  region: {type: string, default: eu}
tasks:
  - id: a
    uses: shell
    with: {op: exec, r: "{{ .inputs.region }}"}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec}
  - id: c
    uses: shell
    needs: [b]
    with: {op: exec}
`

// TestContinuation_SwitchPassesAnEmptyRunIDAndTheOriginalArguments: the
// next generation's run-id argument is empty — a release older than
// generations ends it INVALID_RUN rather than run the run again — and its
// definition, input (as submitted, before defaults) and plan are the ones
// generation 0 started with.
func TestContinuation_SwitchPassesAnEmptyRunIDAndTheOriginalArguments(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, okBehavior)
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-args"})
	wf, input, plan := compile(t, defaultedInputDocument), payload(t, nil), continuedPlan(2)
	h.env.ExecuteWorkflow(engine.RunWorkflowType, "ork-args", wf, input, plan, (*orkstackv1.RunContinuation)(nil))
	var can *workflow.ContinueAsNewError
	if !errors.As(h.env.GetWorkflowError(), &can) {
		t.Fatalf("workflow error = %v, want a continue-as-new", h.env.GetWorkflowError())
	}
	var (
		runID    string
		gotWf    *orkstackv1.Orkflow
		gotInput *orkstackv1.Payload
		gotPlan  engine.RunPlan
		carry    *orkstackv1.RunContinuation
	)
	if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &runID, &gotWf, &gotInput, &gotPlan, &carry); err != nil {
		t.Fatal(err)
	}
	if runID != "" {
		t.Fatalf("run-id argument %q, want empty", runID)
	}
	if !proto.Equal(gotWf, wf) || !proto.Equal(gotInput, input) || gotInput.GetData() != nil {
		t.Fatalf("definition or input changed across the switch: input %v", gotInput)
	}
	want, _ := json.Marshal(plan)
	got, _ := json.Marshal(gotPlan)
	if string(got) != string(want) {
		t.Fatalf("plan changed across the switch:\n%s\nwant\n%s", got, want)
	}
	if carry == nil || can.TaskQueueName == "" {
		t.Fatalf("carry %v queue %q", carry, can.TaskQueueName)
	}
}

const strideDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: stride
inputs:
  parts: {type: array}
tasks:
  - id: spread
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 100, max_parallel: 1}
    with: {op: exec, part: "{{ .each.item }}"}
  - id: after
    uses: shell
    needs: [spread]
    with: {op: exec}
`

// TestContinuation_SwitchWritesAFullSnapshotOfTheExactState: a fan-out
// switching between two of its stride snapshots. Its switch snapshot holds
// every item that succeeded — after its last stride snapshot too, which no
// event snapshot does — exactly as the live rows, under the content-addressed
// continuations/{gen}-snapshot-{sha}.pb.gz key, and the checkpoint holds the
// same rows.
func TestContinuation_SwitchWritesAFullSnapshotOfTheExactState(t *testing.T) {
	// 60 items: a stride of 3. The hold starts after 4 succeed, so the item
	// running then settles as the fifth — between stride snapshots.
	g := runGenerations(t, "ork-stride", compile(t, strideDocument), spreadInput(t, 60), holdPlan(), itemBehavior,
		func(h *harness, generation int) {
			if generation == 0 {
				raiseHistoryAt(h, 3, 650)
			}
		})
	if g.err != nil || len(g.carries) == 0 {
		t.Fatalf("run = %v, %d switches", g.err, len(g.carries))
	}
	carry := g.carries[0]
	snapshot, checkpoint := decodeCarried(t, g.harnesses[0].snapshots, carry)
	settled := 0
	for _, e := range g.harnesses[0].bus.Events(g.runID, 0) {
		if e.Item != nil && e.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
			settled++
		}
	}
	if settled%3 == 0 {
		t.Fatalf("generation 0 settled %d items, a stride boundary: the test must switch between strides", settled)
	}
	if got := itemsSucceeded(snapshot, "spread"); got != settled {
		t.Fatalf("switch snapshot holds %d succeeded items, generation 0 settled %d", got, settled)
	}
	if !proto.Equal(&orkstackv1.Run{TaskRuns: snapshot.GetTaskRuns()}, &orkstackv1.Run{TaskRuns: checkpoint.GetTaskRuns()}) {
		t.Fatal("the checkpoint's rows differ from the switch snapshot's")
	}
	if !strings.Contains(carry.GetSnapshot().GetUri(), "/continuations/000000-snapshot-"+carry.GetSnapshot().GetSha256()+".pb.gz") {
		t.Fatalf("switch snapshot key %q", carry.GetSnapshot().GetUri())
	}
}

// TestSwitchSnapshot_NeverWritesASequenceKey: nothing a switch writes lands
// on a {seq}.pb key — those belong to the emits that own each sequence — and
// a late second write of a different state lands on its own content key,
// leaving the next generation's own {eventSeq+1}.pb byte-identical.
func TestSwitchSnapshot_NeverWritesASequenceKey(t *testing.T) {
	g := runGenerations(t, "ork-seqkey", compile(t, continuationChainDocument), payload(t, nil), continuedPlan(2), okBehavior, nil)
	if g.err != nil || len(g.carries) == 0 {
		t.Fatalf("run = %v, %d switches", g.err, len(g.carries))
	}
	store := g.harnesses[0].snapshots
	carry := g.carries[0]
	sequenceKeys := 0
	for _, uri := range store.keys() {
		switch {
		case strings.Contains(uri, "/continuations/"):
		case strings.HasSuffix(uri, ".pb"):
			sequenceKeys++
		default:
			t.Fatalf("unexpected key %q", uri)
		}
	}
	if total := len(g.events()); sequenceKeys > total {
		t.Fatalf("%d sequence keys for %d events", sequenceKeys, total)
	}
	next := fmt.Sprintf("memory://snapshots/%s/%s/%d.pb", g.final(t).GetNamespace(), g.runID, carry.GetEventSequence()+1)
	before, ok := store.get(next)
	if !ok {
		t.Fatalf("generation 1 wrote no %s", next)
	}
	// A late write from a replaced replica of a DIFFERENT state.
	late := proto.Clone(g.final(t)).(*orkstackv1.Run)
	late.Status = orkstackv1.Status_STATUS_RUNNING
	obj, err := engine.NewSnapshotActivities(store).WriteSwitchSnapshot(context.Background(), late, late.GetNamespace(), 0,
		engine.ContinuationObservationInput{})
	if err != nil {
		t.Fatal(err)
	}
	if obj.GetUri() == carry.GetSnapshot().GetUri() || !strings.Contains(obj.GetUri(), "/continuations/") {
		t.Fatalf("the late write landed on %q", obj.GetUri())
	}
	after, _ := store.get(next)
	if string(after) != string(before) {
		t.Fatalf("the late write changed %s", next)
	}
}

// TestContinuation_NoSwitchWhenNothingRemains: a run past its budget whose
// every task has finished succeeds; it never switches into an empty
// generation.
func TestContinuation_NoSwitchWhenNothingRemains(t *testing.T) {
	const doc = `
apiVersion: orkstack/v1
kind: Orkflow
name: two
tasks:
  - id: a
    uses: shell
    with: {op: exec}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec}
`
	g := runGenerations(t, "ork-nothing", compile(t, doc), payload(t, nil), continuedPlan(2), okBehavior, nil)
	if g.err != nil || len(g.harnesses) != 1 {
		t.Fatalf("run = %v over %d generations, want one that succeeds", g.err, len(g.harnesses))
	}
	if g.harnesses[0].continuationCount("events") != 0 {
		t.Fatal("a switch was attempted with nothing left to run")
	}
}

// TestContinuation_WriteFailureNeverSwitchesAndRetriesAfterGrowth: for each
// of the three writes, a failure leaves the run in its current history, and
// no switch is tried again until it has grown by the plan's retry spacing.
func TestContinuation_WriteFailureNeverSwitchesAndRetriesAfterGrowth(t *testing.T) {
	const doc = `
apiVersion: orkstack/v1
kind: Orkflow
name: long
tasks:
  - id: t1
    uses: shell
    with: {op: exec}
  - id: t2
    uses: shell
    needs: [t1]
    with: {op: exec}
  - id: t3
    uses: shell
    needs: [t2]
    with: {op: exec}
  - id: t4
    uses: shell
    needs: [t3]
    with: {op: exec}
  - id: t5
    uses: shell
    needs: [t4]
    with: {op: exec}
  - id: t6
    uses: shell
    needs: [t5]
    with: {op: exec}
  - id: t7
    uses: shell
    needs: [t6]
    with: {op: exec}
  - id: t8
    uses: shell
    needs: [t7]
    with: {op: exec}
`
	for _, kind := range []string{"events", "snapshot", "checkpoint"} {
		t.Run(kind, func(t *testing.T) {
			// Budget 4 events, so the retry spacing is 1 event: the first
			// attempt follows t4, the retry follows t5.
			plan := continuedPlan(4)
			failures := 0
			g := runGenerations(t, "ork-writefail-"+kind, compile(t, doc), payload(t, nil), plan, okBehavior,
				func(h *harness, generation int) {
					if generation != 0 {
						return
					}
					h.onContinuation = func(k string, _ int64) error {
						if k == kind && failures < 3 { // every attempt of the first try
							failures++
							return errors.New("object storage is unavailable")
						}
						return nil
					}
				})
			if g.err != nil {
				t.Fatalf("run failed: %v", g.err)
			}
			if len(g.harnesses) < 2 {
				t.Fatal("the run never switched after the store came back")
			}
			gen0 := g.harnesses[0].taskOrder()
			if strings.Join(gen0, ",") != "t1,t2,t3,t4,t5" {
				t.Fatalf("generation 0 ran %v, want t1–t4, the failed attempt, then t5 before the retry", gen0)
			}
		})
	}
}

// TestContinuation_CheckpointOverTheCapNeverSwitches: a saved state larger
// than the plan's checkpoint bound never switches; the run carries on in its
// one history, and the RUN_HISTORY_LIMIT it then reaches names the size.
func TestContinuation_CheckpointOverTheCapNeverSwitches(t *testing.T) {
	plan := continuedPlan(2)
	plan.Continuation.CheckpointMaxBytes = 64 // smaller than any saved state
	plan.History.MaxEvents = 1000
	var suite testsuite.WorkflowTestSuite
	h := newHarnessCtx(t, &suite, okBehavior)
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-overcap"})
	h.onSnapshot = onceAt(h.env, 900, taskStatusIs("c", orkstackv1.Status_STATUS_SUCCEEDED))
	h.env.ExecuteWorkflow(engine.RunWorkflowType, "ork-overcap", compile(t, continuationChainDocument), payload(t, nil),
		plan, (*orkstackv1.RunContinuation)(nil))
	requireHistoryLimitError(t, h.env)
	if h.continuationCount("checkpoint") != 1 {
		t.Fatalf("%d checkpoint writes, want exactly one: an over-cap state is never tried again", h.continuationCount("checkpoint"))
	}
	run := mustFinalRun(t, h.store, "ork-overcap")
	if !strings.Contains(run.GetError().GetMessage(), "it cannot carry on into a fresh history: its saved state is") ||
		!strings.Contains(run.GetError().GetMessage(), "more than the 0.1 KiB a run carries forward") {
		t.Fatalf("message = %q, want the size reason", run.GetError().GetMessage())
	}
	for _, uri := range h.snapshots.keys() {
		if strings.Contains(uri, "-checkpoint-") {
			t.Fatalf("an over-cap checkpoint was stored at %s", uri)
		}
	}
}

// TestContinuation_GenerationCeilingStopsSwitching: at the plan's
// MaxGenerations no switch happens; the run finishes in its last history.
func TestContinuation_GenerationCeilingStopsSwitching(t *testing.T) {
	plan := continuedPlan(1)
	plan.Continuation.MaxGenerations = 3
	g := runGenerations(t, "ork-ceiling", compile(t, continuationChainDocument), payload(t, nil), plan, okBehavior, nil)
	if g.err != nil {
		t.Fatalf("run failed: %v", g.err)
	}
	if len(g.harnesses) != 3 {
		t.Fatalf("%d generations, want exactly MaxGenerations = 3", len(g.harnesses))
	}
	if run := g.final(t); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("status = %v", run.GetStatus())
	}
}

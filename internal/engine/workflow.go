package engine

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumspb "go.temporal.io/api/enums/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// orkerHeartbeatTimeout is what the engine sets in ActivityOptions, and the
// CEILING on the Orker's heartbeat tick rate (task.MaxHeartbeatInterval is a
// third of it). The tick itself is the Orker's own setting,
// ORK_ORKER_HEARTBEAT_INTERVAL, and nothing here depends on its value.
//
// The two halves must agree or cancellation silently stops working: Temporal
// delivers cancellation to an activity ONLY through a heartbeat response, so
// an activity that never calls RecordHeartbeat never observes ctx.Done().
// Before this, no HeartbeatTimeout was set anywhere in the tree and the Orker
// heartbeated only between phases, which meant cancellation could not reach a
// running container at all — the largest correctness gap in the system (§6.3).
//
// This number is the DEAD-ORKER detector and nothing else. How fast a live
// Orker learns it was cancelled is set entirely Orker-side, by that tick and
// the matching MaxHeartbeatThrottleInterval cap on its worker; raising or
// lowering the timeout here would change replay-visible ActivityOptions
// without making cancellation any quicker.
const orkerHeartbeatTimeout = task.HeartbeatTimeout

// pinCreationTimeout is how long a pin waits for SOME pin host in its pool to
// have a free session slot. It becomes SessionOptions.CreationTimeout, which
// the SDK uses as the creation activity's ScheduleToStartTimeout (design
// §6.3).
//
// Submit already refused a pool whose Orkers host no pins at all
// (PoolNoPinHostError), so this covers SATURATION only: every host is at its
// ORK_ORKER_MAX_PINS. The SDK retries creation on the "too many outstanding
// sessions" error with a 1s x 1.1 backoff until this expires, and only then
// does the pin's first task fail with PIN_UNAVAILABLE.
//
// A constant, not a setting: workflow code may not read process configuration,
// and putting it on Pin would be one more knob for a number an author has no
// better information about than the platform does. Pin.creation_timeout_seconds
// is a one-field addition later if the trade proves wrong (design §12.4).
const pinCreationTimeout = 5 * time.Minute

// pinReleaseBudget is the slice of a DERIVED pin lifetime reserved for the
// release activity that removes /orkstack/shared, added on top of the members'
// own budgets (design §6.3).
//
// A pin's ExecutionTimeout is the creation activity's StartToCloseTimeout —
// the pin's WHOLE life, cleanup included. Without this the sum would be
// exhausted by the last member's own worst case and the session could die
// during its own release, which is the one moment the release has to survive:
// it runs on the pin host, and nothing else removes that directory promptly.
// It is not added to an author's explicit Pin.timeout_seconds, because that
// number is the author's statement about the whole pin.
const pinReleaseBudget = 60 * time.Second

// pinReleaseTimeout bounds one attempt of the release activity, and
// pinReleaseAttempts caps how many it gets (design §8.5).
//
// Small on purpose. Removing a directory on the host that owns it is fast or
// broken, and its failure is LOGGED and never returned: cleanup is not
// orchestration, and the Orker's lease sweep is the backstop. These two
// numbers are also the whole of what succeed()/fail() can be made to wait for
// by a pin, which is why they are bounded rather than generous.
const (
	pinReleaseTimeout  = 30 * time.Second
	pinReleaseAttempts = 3
)

const (
	// RunWorkflowType is the registered Temporal workflow type for a run. It
	// names a Temporal concept, not an OrkStack one: the OrkStack entity a run
	// executes is an Orkflow, and this is the workflow TYPE that runs it.
	RunWorkflowType = "RunWorkflow"

	// EventQueue is the Temporal task queue for engine-internal projection
	// activities. It is deliberately separate from task.DefaultQueue so Orkers
	// polling for task execution never receive engine projection work, and
	// carries the ork- prefix every OrkStack Temporal name uses. The literal
	// lives in internal/task because the two places that must refuse to run
	// tasks on it — task.QueueForPool and `ork` at boot — cannot import this
	// package.
	EventQueue = task.EngineEventQueue

	// ActivityPublishEvent is the registered name of the projection activity.
	ActivityPublishEvent = "PublishEvent"

	// snapshotAttemptTimeout bounds ONE attempt of the local snapshot write —
	// the same per-attempt budget the projection activity gets below.
	snapshotAttemptTimeout = 10 * time.Second

	// snapshotActivityTimeout bounds the complete local snapshot write end to
	// end, retries and backoff included: for a local activity
	// ScheduleToCloseTimeout covers every attempt. The write is run-critical —
	// emit fails the run when it exhausts this budget — so it gets MORE total
	// time than the best-effort projection below (three 10s attempts, 30s),
	// never less: three 10s attempts plus the SDK's default 1s/2.0 backoff is
	// ~33s, and the 60s ceiling leaves room for a remote object store to
	// throttle without one latency spike converting hours of completed task
	// work into a terminal failure.
	snapshotActivityTimeout = 60 * time.Second
)

// eventActivityTimeout bounds one projection activity attempt.
const eventActivityTimeout = 10 * time.Second

// RunWorkflow is the deterministic orchestration workflow. It interprets the
// compiled task DAG, schedules ready tasks as ExecuteTask activities on the
// queue their pool maps to, threads upstream outputs/artifacts into downstream
// tasks, and emits lifecycle events through the projection activity.
//
// Every side effect (event publication, read-model updates) happens in an
// activity; the workflow body uses only workflow.Now for time, ordered
// iteration, and activity futures.
//
// Arguments are passed separately rather than in one struct because the
// Temporal data converter encodes top-level protobuf messages with protojson;
// nesting them in a plain Go struct would fall back to encoding/json, which
// cannot round-trip google.protobuf.Struct.
//
// plan is this run's routing, resolved at submit by TemporalStarter.StartRun
// and bound into the input here. It is an argument, not a lookup, for two
// reasons that both matter: workflow code is deterministic and may not read
// process configuration or a namespace document, and the orkflow message must
// not carry a channel name — that is Temporal's word, the definition is the
// user's document, and the two never touch. Binding the plan
// records the routing in history, so every replay dispatches where the
// original run did and a second engine finishes a run this one started without
// redirecting its tasks onto its own defaults.
func RunWorkflow(
	ctx workflow.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	plan RunPlan,
) (*orkstackv1.Run, error) {
	if runID == "" {
		return nil, temporal.NewNonRetryableApplicationError("run id is required", "INVALID_RUN", nil)
	}
	if wf == nil || len(wf.GetTasks()) == 0 {
		return nil, temporal.NewNonRetryableApplicationError("orkflow has no tasks", "INVALID_ORKFLOW", nil)
	}

	state := newRunState(ctx, runID, wf, input, plan)
	// Every task activity is scheduled on runCtx, never on ctx directly, so the
	// run has one handle that ends all of them at once. Temporal does NOT
	// cancel activities a workflow function abandons by returning: only an
	// explicit cancellation on the context they were scheduled with reaches a
	// running container (§3.3). fail() is what pulls it.
	runCtx, cancelRun := workflow.WithCancel(ctx)
	// RUN_CREATED is emitted here, by the run itself, and not by whichever API
	// process accepted the submission. It is event 1 of every run that reaches
	// Temporal, so a replica rebuilding the stream from history starts at 1 with
	// no gap — which the API-published version could never give it, because that
	// copy only ever existed in one process's memory.
	if err := state.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, "",
		orkstackv1.Status_STATUS_PENDING,
		fmt.Sprintf("run created for orkflow %s@%s", wf.GetName(), wf.GetVersion())); err != nil {
		return nil, err
	}
	state.start(ctx)
	if err := state.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, "",
		orkstackv1.Status_STATUS_RUNNING, "run started"); err != nil {
		return nil, err
	}

	inFlight := map[string]workflow.Future{}
	for {
		// Schedule every task whose own dependencies are terminal. Re-scan after
		// each completion so a fast branch can advance without waiting for an
		// unrelated slow branch.
		for _, id := range state.readyTasks() {
			run, err := state.classify(ctx, id)
			if err != nil {
				return state.fail(ctx, cancelRun, inFlight, id, err)
			}
			if !run {
				continue
			}
			// A pin is opened by the FIRST of its members the run actually
			// decides to run, and never before: a pin whose every member is
			// skipped costs an Orker nothing, and the pin's lifetime clock
			// starts when work is about to start (design §6.2). Opening does
			// not schedule this task — nothing is emitted for it, it stays
			// PENDING, and readyTasks holds it until the session is open.
			hold, err := state.openPin(runCtx, id)
			if err != nil {
				return state.fail(ctx, cancelRun, inFlight, id, err)
			}
			if hold {
				continue
			}
			future, err := state.schedule(runCtx, id)
			if err != nil {
				return state.fail(ctx, cancelRun, inFlight, id, err)
			}
			inFlight[id] = future
		}

		// A pin that is opening is work in progress exactly as an in-flight
		// activity is: its members are held, so readyTasks is empty for them,
		// and a run with nothing else to do must wait for its readiness rather
		// than conclude there is nothing left.
		if len(inFlight) == 0 && state.openingPins() == 0 {
			if len(state.readyTasks()) == 0 {
				break
			}
			// The prior scan only propagated skips; scan again for their newly
			// ready dependants without waiting for an activity.
			continue
		}

		// Register futures in declaration order. Future readiness comes from
		// recorded Temporal history, while fixed registration order stabilizes
		// the case where several futures are ready in one workflow task.
		selector := workflow.NewSelector(ctx)
		selectedID := ""
		selectedPin := ""
		for _, id := range state.order {
			future, ok := inFlight[id]
			if !ok {
				continue
			}
			taskID := id
			selector.AddFuture(future, func(workflow.Future) {
				selectedID = taskID
			})
		}
		// Then every opening pin's readiness, in sorted pin-name order and
		// AFTER the task futures — a fixed position, so which of several
		// simultaneously ready futures the loop takes first is a property of
		// this code and not of iteration order (design §6.2).
		for _, name := range state.pinOrder {
			p := state.pins[name]
			if !p.opened || p.readyConsumed {
				continue
			}
			pinName := name
			selector.AddFuture(p.ready, func(workflow.Future) {
				selectedPin = pinName
			})
		}
		selector.Select(ctx)
		// A pin's readiness is NOT a task outcome: nothing is collected, and
		// nothing is terminal. It only unblocks the pin's members, so the loop
		// goes straight back to scanning them.
		if selectedPin != "" {
			if err := state.pinOpened(ctx, selectedPin); err != nil {
				return state.fail(ctx, cancelRun, inFlight,
					state.pins[selectedPin].triggeredBy, err)
			}
			continue
		}
		future := inFlight[selectedID]
		delete(inFlight, selectedID)
		if err := state.collect(ctx, selectedID, future); err != nil {
			return state.fail(ctx, cancelRun, inFlight, selectedID, err)
		}
	}

	return state.succeed(ctx)
}

// runState holds the deterministic in-workflow state of one run.
type runState struct {
	runID string
	wf    *orkstackv1.Orkflow
	// plan is this run's routing, bound into the workflow input at submit. It
	// is the only source of a channel name here: never the environment, never
	// a namespace document, never a lookup.
	plan     RunPlan
	input    *orkstackv1.Payload
	inputs   map[string]any
	order    []string
	tasks    map[string]*orkstackv1.Task
	taskRuns map[string]*orkstackv1.TaskRun
	// results is each completed task's ExecuteTaskResult, which is what an
	// UpstreamRef is projected from. It holds REFERENCES, not bytes.
	results map[string]*orkstackv1.ExecuteTaskResult
	// outputs is each completed task's decoded values.json, for condition
	// evaluation and for the run's aggregated output. It exists separately
	// from results because a condition needs a map and an upstream ref needs
	// a reference, and conflating them is how bytes end up on the wire.
	outputs map[string]map[string]any
	closure map[string][]string
	leaves  map[string]struct{}

	// pins is one entry per DECLARED pin, keyed by name, and pinOrder is their
	// names sorted. Both are built once in newRunState from wf.Pins, so the
	// set never changes during a run and iteration is over pinOrder — never
	// over the map, which is cancelInFlight's rule and the same reason it has
	// one (Go randomizes map order on purpose, and workflow code may not).
	pins     map[string]*pinState
	pinOrder []string

	createdAt *timestamppb.Timestamp
	startedAt *timestamppb.Timestamp
	status    orkstackv1.Status
	runError  *orkstackv1.Error
	finished  *timestamppb.Timestamp
	output    *orkstackv1.Payload
	eventSeq  int
}

// pinState is one pin's whole life inside this run: who belongs to it, and
// where the Temporal Worker Session that implements it has got to.
//
// Every field is a pure function of the workflow input and recorded history —
// the futures settle from history, workflow.Now is the replayed clock — so the
// struct replays identically. Only the pin's own coroutine writes ctx, openErr
// and lost; only the main loop writes the rest. The two never race because
// workflow coroutines are cooperatively scheduled: a write is visible to the
// other side exactly when the writer next blocks, and the main loop only reads
// ctx/openErr after `ready` has settled.
type pinState struct {
	name string
	pin  *orkstackv1.Pin
	// tasks is every member, in declaration order. It is what "all members are
	// terminal" is scanned over, so its order is the run's own.
	tasks []string

	// opened records that CreateSession has been ASKED for. It is set before
	// the coroutine starts, so the same pin is never opened twice, and it is
	// what readyTasks reads to hold the pin's other members back.
	opened bool
	// triggeredBy is the member whose readiness opened the pin. It is the task
	// a creation failure is reported on, because it is the only member the run
	// had actually decided to run at that point.
	triggeredBy string
	// openedAt is workflow.Now when opening began — the deterministic start
	// PIN_TIMEOUT is measured from (design §6.5).
	openedAt time.Time
	// timeout is this pin's ExecutionTimeout: Pin.timeout_seconds, or the
	// derived sum. Held here so the value on PinContext, the value the session
	// was created with, and the value PIN_TIMEOUT compares against are one
	// number computed once.
	timeout time.Duration

	// ready settles when the session is open or its creation failed. The main
	// loop registers it in its Selector; the coroutine settles it.
	ready         workflow.Future
	readySettable workflow.Settable
	// readyConsumed stops the Selector re-registering a future the loop has
	// already taken the outcome of.
	readyConsumed bool
	// ctx is the session context, and nil until the session is open. A pinned
	// task's activity is scheduled on THIS, which is what routes it to the one
	// Orker the session opened on.
	ctx workflow.Context
	// openErr is why the session could never be created (PIN_UNAVAILABLE).
	openErr error

	// done is the main loop's one message to the coroutine: every member is
	// terminal, release and close. Buffered with room for it, so the loop can
	// never block sending — and so the signal is not lost when it arrives
	// before the coroutine reaches its Receive.
	done     workflow.Channel
	doneSent bool

	// closed settles when the coroutine has released the shared directory and
	// completed the session. succeed()/fail() wait on it, so a run never
	// returns while a pin it opened is still open.
	closed         workflow.Future
	closedSettable workflow.Settable

	// lost records that the session was observed FAILED — the host died, or
	// the pin outlived its ExecutionTimeout. It is what turns a member's
	// cancellation into PIN_HOST_LOST/PIN_TIMEOUT rather than CANCELED.
	lost bool
}

func newRunState(
	ctx workflow.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	plan RunPlan,
) *runState {
	mergedInput := ApplyInputDefaults(wf, input)
	state := &runState{
		runID:    runID,
		wf:       wf,
		plan:     plan,
		input:    mergedInput,
		inputs:   payloadMap(mergedInput),
		tasks:    map[string]*orkstackv1.Task{},
		taskRuns: map[string]*orkstackv1.TaskRun{},
		results:  map[string]*orkstackv1.ExecuteTaskResult{},
		outputs:  map[string]map[string]any{},
		closure:  map[string][]string{},
		leaves:   map[string]struct{}{},
		// CreatedAt is the execution's own start time, taken from the
		// WorkflowExecutionStarted history event rather than from a clock, so
		// every replica that reads this run dates it identically and a replay
		// reproduces it exactly. StartedAt stays unset until the run actually
		// starts, one event later.
		createdAt: timestamppb.New(workflow.GetInfo(ctx).WorkflowStartTime),
		status:    orkstackv1.Status_STATUS_PENDING,
		pins:      map[string]*pinState{},
	}
	for _, t := range wf.GetTasks() {
		state.order = append(state.order, t.GetId())
		state.tasks[t.GetId()] = t
		state.taskRuns[t.GetId()] = &orkstackv1.TaskRun{
			TaskId: t.GetId(),
			Status: orkstackv1.Status_STATUS_PENDING,
		}
		state.leaves[t.GetId()] = struct{}{}
	}
	for _, t := range wf.GetTasks() {
		for _, need := range t.GetNeeds() {
			delete(state.leaves, need)
		}
	}
	for _, id := range state.order {
		state.closure[id] = state.transitiveNeeds(id)
	}
	// One entry per DECLARED pin, with its members in declaration order. The
	// compiler already sorts Orkflow.pins by name; sorted again here so the
	// order the Selector registers readiness in is this workflow's own
	// property and not an assumption about its input.
	for _, pin := range wf.GetPins() {
		name := pin.GetName()
		if name == "" || state.pins[name] != nil {
			continue
		}
		p := &pinState{name: name, pin: pin}
		for _, id := range state.order {
			if state.tasks[id].GetPin() == name {
				p.tasks = append(p.tasks, id)
			}
		}
		state.pins[name] = p
		state.pinOrder = append(state.pinOrder, name)
	}
	sort.Strings(state.pinOrder)
	return state
}

// start moves the run from created to running. It is separate from
// newRunState so the RUN_CREATED event and the snapshot published with it
// describe a run that has not started yet.
func (s *runState) start(ctx workflow.Context) {
	s.status = orkstackv1.Status_STATUS_RUNNING
	s.startedAt = timestamppb.New(workflow.Now(ctx))
}

// transitiveNeeds returns the sorted transitive `needs` closure of id. The DSL
// permits referencing any transitively upstream task, so the whole closure is
// threaded into the downstream activity input.
func (s *runState) transitiveNeeds(id string) []string {
	collected := map[string]struct{}{}
	var walk func(string)
	walk = func(current string) {
		for _, need := range s.tasks[current].GetNeeds() {
			if _, done := collected[need]; done {
				continue
			}
			collected[need] = struct{}{}
			if _, ok := s.tasks[need]; ok {
				walk(need)
			}
		}
	}
	walk(id)
	out := make([]string, 0, len(collected))
	for need := range collected {
		out = append(out, need)
	}
	sort.Strings(out)
	return out
}

// readyTasks returns pending tasks whose dependencies are all terminal, in
// declaration order.
//
// A task whose PIN is opening is not ready, however met its `needs:` are: the
// session it must be scheduled on does not exist yet (design §6.2). Such a
// task stays PENDING with nothing emitted for it and is re-scanned the moment
// the pin's readiness settles, which is why holding it here rather than
// half-scheduling it costs a client nothing.
//
// This stays a pure function of runState. The pin state it reads — `opened`
// and whether `ctx` has arrived — is itself only ever written in the single
// deterministic execution order of the workflow's coroutines, so a replay
// evaluates this identically.
func (s *runState) readyTasks() []string {
	var ready []string
	for _, id := range s.order {
		if s.taskRuns[id].GetStatus() != orkstackv1.Status_STATUS_PENDING {
			continue
		}
		if p := s.pins[s.tasks[id].GetPin()]; p != nil && p.opened && p.ctx == nil {
			continue
		}
		blocked := false
		for _, need := range s.tasks[id].GetNeeds() {
			if !isTerminal(s.taskRuns[need].GetStatus()) {
				blocked = true
				break
			}
		}
		if !blocked {
			ready = append(ready, id)
		}
	}
	return ready
}

func isTerminal(status orkstackv1.Status) bool {
	switch status {
	case orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED,
		orkstackv1.Status_STATUS_SKIPPED,
		orkstackv1.Status_STATUS_CANCELED:
		return true
	default:
		return false
	}
}

// pinFor is the declared pin a task belongs to, or nil when it belongs to
// none.
//
// A task naming a pin the orkflow does not declare fails the run rather than
// running unpinned. Rule 9 refuses it at compile, so this is only reachable
// through a hand-built SubmitRun — and running such a task on any Orker at all
// would quietly ignore the one thing its author asked for, which is the
// silently-wrong outcome AGENTS.md §14 ranks below explicit failure.
func (s *runState) pinFor(id string) (*pinState, error) {
	name := s.tasks[id].GetPin()
	if name == "" {
		return nil, nil
	}
	p := s.pins[name]
	if p == nil {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("task %q names pin %q, which this orkflow does not declare", id, name),
			"INVALID_PIN", nil)
	}
	return p, nil
}

// openingPins is how many pins have been asked for but have not settled.
//
// It is the loop's second kind of outstanding work, beside the in-flight
// activities: while it is non-zero there is a future to wait on, even with
// nothing dispatched.
func (s *runState) openingPins() int {
	opening := 0
	for _, name := range s.pinOrder {
		if p := s.pins[name]; p.opened && !p.readyConsumed {
			opening++
		}
	}
	return opening
}

// pinLifetime is a pin's ExecutionTimeout: the author's number, or the
// sequential worst case derived from its members (design §6.3).
//
// The derived sum is every member's own wall clock plus its cancellation grace
// plus this run's transfer budget — the same three the member's own
// StartToCloseTimeout is built from in schedule() — plus pinReleaseBudget for
// the cleanup that has to fit inside the session too. It over-estimates a pin
// whose members run concurrently, and that costs nothing: the pin closes when
// its last member is terminal (§6.4), never at this bound. An under-estimate
// would kill a healthy pin, which is why the sequential case is the one taken.
//
// A pure function of the definition and the plan, so it replays identically.
func (s *runState) pinLifetime(p *pinState) time.Duration {
	if seconds := p.pin.GetTimeoutSeconds(); seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	total := time.Duration(0)
	for _, id := range p.tasks {
		t := s.tasks[id]
		total += time.Duration(timeoutSecondsFor(t))*time.Second +
			time.Duration(graceSecondsFor(t))*time.Second +
			time.Duration(s.plan.TransferBudgetSeconds)*time.Second
	}
	return total + pinReleaseBudget
}

// openPin opens the pin of a task that is ready and classified to run, and
// reports whether the caller must HOLD that task back this pass.
//
// It holds in two cases, and the second is easy to miss: the pin it just
// opened, and a pin already opened that has not settled. The main loop walks a
// ready list computed BEFORE this pass, so two members of one pin that are
// ready together both appear in it — the first opens the pin, and the second
// would otherwise be scheduled on a session that does not exist yet.
//
// It returns (false, nil) for an unpinned task and for a member whose session
// is open; both are scheduled immediately.
//
// runCtx, never ctx: everything the pin does has to end when the run does. The
// session context descends from runCtx, so cancelRun() in fail() cancels the
// session, which cancels every activity scheduled on it — the §3.3
// cancellation path, reaching pinned tasks unchanged.
func (s *runState) openPin(runCtx workflow.Context, id string) (bool, error) {
	p, err := s.pinFor(id)
	if err != nil || p == nil {
		return false, err
	}
	if p.opened {
		return p.ctx == nil, nil
	}

	// The pool's queue, from the ONE resolution every task uses. CreateSession
	// reads it off the ActivityOptions to derive
	// `<queue>__internal_session_creation`, so no queue name is computed
	// anywhere new; rule 10 (PinPoolMismatchError) is what makes "the pin's
	// queue" well defined from any one member.
	queue, err := s.queueForTask(s.tasks[id])
	if err != nil {
		return false, err
	}

	p.opened = true
	p.triggeredBy = id
	p.openedAt = workflow.Now(runCtx)
	p.timeout = s.pinLifetime(p)
	p.ready, p.readySettable = workflow.NewFuture(runCtx)
	p.closed, p.closedSettable = workflow.NewFuture(runCtx)
	p.done = workflow.NewBufferedChannel(runCtx, 1)

	// A coroutine, and it has to be one, for two reasons that each force it
	// (design §6.2): CompleteSession must run in the coroutine that ran
	// CreateSession, so something other than the main loop has to own the
	// session's life; and CreateSession blocks until a host answers, bounded
	// only by pinCreationTimeout, which in the main loop would stall every
	// unpinned branch of the run — the exact property the loop's re-scan
	// exists to provide.
	workflow.Go(runCtx, func(gctx workflow.Context) {
		s.hostPin(gctx, p, queue)
	})
	return true, nil
}

// hostPin is one pin's coroutine: create the session, hand it to the main
// loop, and close it once the loop says every member is terminal.
//
// It emits NOTHING. Every emit stays in the main loop so the event sequence
// counter is assigned in one place (design §12.3); this coroutine's only
// channel back to the loop is the two futures and the one signal.
func (s *runState) hostPin(gctx workflow.Context, p *pinState, queue string) {
	base := workflow.WithActivityOptions(gctx, workflow.ActivityOptions{TaskQueue: queue})
	sessionCtx, err := workflow.CreateSession(base, &workflow.SessionOptions{
		ExecutionTimeout: p.timeout,
		CreationTimeout:  pinCreationTimeout,
		// The dead-host detector for the pin, and the same number a task's own
		// heartbeat uses. The SDK ticks the creation activity's heartbeat at
		// min(this/3, 10s), so a lost pin host is noticed within it.
		HeartbeatTimeout: orkerHeartbeatTimeout,
	})
	if err != nil {
		p.openErr = err
		p.readySettable.Set(nil, err)
		// Nothing was created, so there is nothing to release or complete —
		// but the loop still waits on `closed` for every pin it opened, so it
		// has to settle here too.
		p.closedSettable.Set(nil, nil)
		return
	}
	// Written before the future settles: the main loop reads ctx only after
	// `ready` has, and a coroutine's writes are visible to the loop exactly
	// when this one next blocks.
	p.ctx = sessionCtx
	p.readySettable.Set(nil, nil)

	// Blocks until the loop says every member is terminal. Channel.Receive
	// does not observe context cancellation, so this returns on the signal and
	// on nothing else — which is why succeed()/fail() signal any pin that has
	// not been signalled before they wait (awaitPinsClosed).
	p.done.Receive(gctx, nil)

	if info := workflow.GetSessionInfo(sessionCtx); info != nil &&
		info.SessionState == workflow.SessionStateOpen {
		// A DISCONNECTED context, because this runs on the cancellation path
		// too: fail() has already cancelled runCtx and therefore sessionCtx,
		// and an activity scheduled on a cancelled context is refused before
		// it is sent. The disconnected child still carries the session — the
		// SDK keeps SessionInfo as a context VALUE and a disconnected context
		// delegates Value() to its parent — so the release still lands on the
		// host that owns the directory (design §12.2).
		release, cancelRelease := workflow.NewDisconnectedContext(sessionCtx)
		releaseCtx := workflow.WithActivityOptions(release, workflow.ActivityOptions{
			StartToCloseTimeout: pinReleaseTimeout,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: pinReleaseAttempts},
		})
		in := &orkstackv1.ReleasePinInput{
			RunId:     s.runID,
			Namespace: s.wf.GetNamespace(),
			Pin: &orkstackv1.PinContext{
				Name:           p.name,
				InstanceId:     info.SessionID,
				TimeoutSeconds: int32(p.timeout / time.Second),
			},
		}
		if err := workflow.ExecuteActivity(releaseCtx, task.ActivityReleasePin, in).
			Get(releaseCtx, nil); err != nil {
			// Best effort, and deliberately so: cleanup is not orchestration,
			// the Orker's lease sweep and boot reap are the backstops, and a
			// failed rmdir must never be the reason a run reports failure.
			workflow.GetLogger(gctx).Warn("release pin failed",
				"run_id", s.runID, "pin", p.name,
				"instance_id", info.SessionID, "error", err)
		}
		cancelRelease()
	}
	// Frees the host's session slot through the SDK's own 3s completion
	// activity. Must be this coroutine, per CreateSession's contract.
	workflow.CompleteSession(sessionCtx)
	p.closedSettable.Set(nil, nil)
}

// pinOpened records the outcome of one pin's opening, taken off the Selector.
//
// A session that opened needs nothing done: the loop re-scans and the pin's
// members are no longer held. A creation that failed is terminal for the pin —
// the engine never re-creates a session for a pin (design §6.5) — so the
// member whose readiness asked for it fails with PIN_UNAVAILABLE and the
// returned error engages fail()'s fail-fast machinery exactly as any other
// task failure does.
func (s *runState) pinOpened(ctx workflow.Context, name string) error {
	p := s.pins[name]
	p.readyConsumed = true
	err := p.ready.Get(ctx, nil)
	if err == nil {
		return nil
	}

	id := p.triggeredBy
	taskRun := s.taskRuns[id]
	message := fmt.Sprintf(
		"pin %q could not be opened on pool %q within %s: no Orker of the pool had a free "+
			"pin slot, so this task never reached one: %v",
		name, s.poolForTask(s.tasks[id]), pinCreationTimeout, err)
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Pin = name
	// started_at stays UNSET, as it does for every task that never reached an
	// Orker: this one was never dispatched, and that absence is the only thing
	// telling a client so.
	taskRun.Error = &orkstackv1.Error{
		Code:      "PIN_UNAVAILABLE",
		Message:   message,
		Details:   fmt.Sprintf("task %q", id),
		Category:  orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
		Retryable: false,
	}
	taskRun.RetryState = retryStateOf(err)
	if emitErr := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
		orkstackv1.Status_STATUS_FAILED, message); emitErr != nil {
		workflow.GetLogger(ctx).Error("emit pin failure failed",
			"run_id", s.runID, "task_id", id, "pin", name, "error", emitErr)
	}
	return err
}

// closePinFor signals a pin's coroutine when the task that just became
// terminal was its last non-terminal member.
//
// Called from every place a task can become terminal — collect, markSkipped,
// markCanceled — so a pin closes the moment its own work is done rather than
// when the whole run is. A run with a fast pin and slow unpinned work releases
// the host's slot and its shared directory promptly instead of holding both
// for the run's duration.
func (s *runState) closePinFor(ctx workflow.Context, id string) {
	p := s.pins[s.tasks[id].GetPin()]
	if p == nil || !p.opened || p.doneSent {
		return
	}
	if !s.pinSettled(p) {
		return
	}
	// Every member is terminal, so every orker_id this pin will ever learn has
	// been recorded — including one a member reported AFTER a sibling was
	// already written down as lost, which is the ordering the immediate
	// attribution in collect cannot see. Last chance, and the only point at
	// which "no member ever reported" is finally true (attributeLostPinHost).
	s.attributeLostPinHost(p)
	p.doneSent = true
	// Buffered with room for exactly this, so the loop cannot block here and
	// the signal survives arriving before the coroutine's Receive.
	p.done.Send(ctx, true)
}

// pinSettled reports whether every member of p has reached a terminal status.
//
// It is "the pin's own work is done": the condition closePinFor releases the
// host on, and the condition that makes "no member ever reported an orker_id"
// a final answer rather than a not-yet.
func (s *runState) pinSettled(p *pinState) bool {
	for _, member := range p.tasks {
		if !isTerminal(s.taskRuns[member].GetStatus()) {
			return false
		}
	}
	return true
}

// awaitPinsClosed blocks until every pin this run opened has been released and
// completed. succeed() and fail() both call it before returning.
//
// A workflow that returned first would ABANDON the coroutine: the host's
// session slot would be freed by the SDK on its next heartbeat (≤ 10s), but
// the release activity would never run and /orkstack/shared would sit until
// its lease expired. Prompt cleanup is worth a bounded wait, and the bound is
// the coroutine's own work: pinReleaseTimeout x pinReleaseAttempts plus the
// SDK's 3s completion activity (design §6.4).
//
// It SIGNALS before it waits, rather than trusting closePinFor to have done
// it. By the time a run ends every member of every pin is terminal — succeed()
// runs only when nothing is pending and fail() has just marked everything —
// so the signal is always correct here; and Channel.Receive ignores context
// cancellation, so a coroutine still waiting on an unsent signal would never
// settle `closed` and this wait would not end. The order in fail() is what
// makes that a belt and not the braces: it is called AFTER cancelInFlight, by
// which point the terminal-task hook has already signalled every pin.
func (s *runState) awaitPinsClosed(ctx workflow.Context) {
	for _, name := range s.pinOrder {
		p := s.pins[name]
		if !p.opened {
			continue
		}
		if !p.doneSent {
			p.doneSent = true
			p.done.Send(ctx, true)
		}
		if err := p.closed.Get(ctx, nil); err != nil {
			workflow.GetLogger(ctx).Warn("waiting for a pin to close failed",
				"run_id", s.runID, "pin", name, "error", err)
		}
	}
}

// pinLoss reports the PIN-level reason a member's activity ended, or nil when
// the task's failure is its own.
//
// A pin never moves (design §6.5). When the host dies its creation activity
// heartbeat-times-out, the SDK marks the session FAILED and cancels the
// session context, so every in-flight member's future returns a cancellation —
// a cancellation that says nothing about the workload and must not be recorded
// as one. This is the check that tells the two apart, and it is made BEFORE
// recordFailure precisely because recordFailure's IsCanceledError branch would
// otherwise record CANCELED, which is the wrong status for a task the platform
// lost the host of.
//
// A member that failed on its OWN — a container exited non-zero — leaves the
// session Open ("Session will not be marked as failed if an activity within it
// returns an error"), so this returns nil and the ordinary path runs unchanged.
func (s *runState) pinLoss(ctx workflow.Context, id string) *orkstackv1.Error {
	p := s.pins[s.tasks[id].GetPin()]
	if p == nil || p.ctx == nil {
		return nil
	}
	info := workflow.GetSessionInfo(p.ctx)
	if info == nil || info.SessionState != workflow.SessionStateFailed {
		return nil
	}
	p.lost = true
	return pinFailureError(p.name, workflow.Now(ctx).Sub(p.openedAt), p.timeout)
}

// pinLossOf is the pin-level error already recorded against a task, or nil.
//
// It reads the outcome collect settled on rather than re-asking the session,
// so fail() and collect can never disagree about whether a task was lost with
// its pin: exactly one place decides, and this is the answer being read back.
func (s *runState) pinLossOf(id string) *orkstackv1.Error {
	t := s.tasks[id]
	if t == nil {
		return nil
	}
	p := s.pins[t.GetPin()]
	if p == nil || !p.lost {
		return nil
	}
	switch s.taskRuns[id].GetError().GetCode() {
	case "PIN_HOST_LOST", "PIN_TIMEOUT":
		return s.taskRuns[id].GetError()
	default:
		return nil
	}
}

// pinFailureError names WHY a pin's session failed, from the only two things
// that can have caused it.
//
// Both arrive at the workflow as the same event — the session's creation
// activity stopped, so the SDK marked the session failed — and the elapsed
// time is what separates them: a pin that has been open for at least its own
// ExecutionTimeout hit its lifetime bound, and one that has not lost its host.
// Deterministic on both counts: workflow.Now is the replayed clock and
// openedAt was recorded from it (design §6.5).
//
// Neither is retryable and neither is the workload's fault, which is why both
// are non-retryable and neither is ACTIVITY_FAILED.
func pinFailureError(name string, elapsed, limit time.Duration) *orkstackv1.Error {
	if limit > 0 && elapsed >= limit {
		return &orkstackv1.Error{
			Code: "PIN_TIMEOUT",
			Message: fmt.Sprintf(
				"pin %q reached its %s lifetime after %s, so its session ended and every task "+
					"still running on that Orker was stopped with it",
				name, limit, elapsed),
			Category:  orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT,
			Retryable: false,
		}
	}
	return &orkstackv1.Error{
		Code: "PIN_HOST_LOST",
		Message: fmt.Sprintf(
			"pin %q lost the Orker hosting it after %s. A pin never moves: its tasks share one "+
				"host's /orkstack/shared, so they cannot be retried on another Orker",
			name, elapsed),
		Category:  orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
		Retryable: false,
	}
}

// pinSessionHostLabel prefixes the best-effort host clue attributeLostPinHost
// leaves on Error.details, and is what makes that clue idempotent: the same
// member can be attributed on every terminal transition of its pin without the
// note being appended twice.
//
// The label is doing real work, not decoration. The value after it is
// SessionInfo.HostName, which the Temporal SDK fills from os.Hostname() on the
// worker that answered the creation request — NOT from ORK_ORKER_ID and not
// from the derived hostname-<hash> fallback. The two coincide in the common
// deployment and diverge the moment an operator gives an Orker an explicit
// identity, so the string has to say which of the two it is.
const pinSessionHostLabel = "Temporal session host (best effort, may differ from orker_id): "

// pinOrkerID is the id of the Orker hosting p, as one of its members has
// already recorded it, or empty when none has yet.
//
// A PIN NEVER MOVES (design §6.5): every member of one pin executes on the
// exact same Orker PROCESS for the pin's whole life — that is the invariant
// /orkstack/shared depends on, and the one the feature exists to provide. So an
// orker_id recorded by ANY member is, by construction rather than by inference,
// the orker_id of every member.
//
// It reads the recorded TaskRuns rather than s.results, because those are the
// two sources that ever carry one and both land here: a success writes
// ExecuteTaskResult.orker_id in collect, a member that failed on its OWN writes
// ActivityFailureDetail.orker_id in recordFailure, and a member with no
// ExecuteTaskResult at all still knows which host it failed on. Iteration is
// over p.tasks — declaration order, never a map — so the answer is the same on
// every replay.
func (s *runState) pinOrkerID(p *pinState) string {
	for _, member := range p.tasks {
		if id := s.taskRuns[member].GetOrkerId(); id != "" {
			return id
		}
	}
	return ""
}

// attributeLostPinHost answers "WHICH Orker died?" on every member the pin took
// down with it.
//
// A member lost with its pin reports nothing — that is what being lost means:
// the host died, so there is no ExecuteTaskResult and no ActivityFailureDetail,
// and orker_id came back EMPTY on precisely the task an operator is staring at.
// pinOrkerID closes that gap with no new field on the wire and no guessing: a
// sibling's recorded orker_id IS this task's, because the pin could not have
// moved between them.
//
// When no sibling ever reported — the pin's first dispatched member is the one
// whose host died — there is no OrkStack-verified identity to record, and the
// engine does not invent one. It leaves orker_id empty, which is what the field
// has always meant, and appends the Temporal session's own host name to
// Error.details as an explicitly labelled lead (pinSessionHostLabel), in the
// same "notes ON the error, never a replacement for it" shape recordFailure
// already appends scan notes in. That note waits until the pin is SETTLED: a
// member still running may yet report the real thing, and a snapshot carrying
// both would invite a reader to weigh a verified identity against a raw
// hostname that never claimed to be one.
//
// Idempotent by construction — it only ever fills an EMPTY orker_id, only on a
// member the engine itself recorded as lost with the pin, and only adds a note
// that is not already there — so it is safe to call on every terminal
// transition, which is what lets a late-reporting sibling still teach its
// already-recorded siblings who the host was.
//
// NO workflow.GetVersion GATE, for recordFailure's retry-state reason: this
// issues no command, makes no decision and reaches history through nothing. It
// rewrites fields of a snapshot message that was never in history, so a replay
// of an older execution takes the identical path and merely fills them in.
func (s *runState) attributeLostPinHost(p *pinState) {
	if p == nil || !p.lost {
		return
	}
	host := s.pinOrkerID(p)
	settled := s.pinSettled(p)
	for _, member := range p.tasks {
		taskRun := s.taskRuns[member]
		if taskRun.GetOrkerId() != "" {
			continue
		}
		// The recorded CODE, not p.lost alone: a pin that lost its host can
		// still hold members that were skipped by a condition or cancelled
		// before dispatch, and those never reached an Orker at all. Saying they
		// ran on the dead one would be a lie of exactly the kind an empty
		// orker_id exists to avoid.
		switch taskRun.GetError().GetCode() {
		case "PIN_HOST_LOST", "PIN_TIMEOUT":
		default:
			continue
		}
		if host != "" {
			taskRun.OrkerId = host
			continue
		}
		if settled {
			hintPinSessionHost(p, taskRun.GetError())
		}
	}
}

// hintPinSessionHost appends the lost session's raw OS host name to one lost
// member's Error.details. See attributeLostPinHost for why it is a note and not
// an orker_id.
//
// Deterministic: SessionInfo.HostName is set once, from the creation response
// the host signalled back, and that signal is in history.
func hintPinSessionHost(p *pinState, failure *orkstackv1.Error) {
	if failure == nil || p.ctx == nil {
		return
	}
	info := workflow.GetSessionInfo(p.ctx)
	if info == nil || info.HostName == "" ||
		strings.Contains(failure.GetDetails(), pinSessionHostLabel) {
		return
	}
	hint := pinSessionHostLabel + info.HostName
	if failure.Details == "" {
		failure.Details = hint
		return
	}
	failure.Details += "; " + hint
}

// classify decides whether a ready task runs or is skipped.
func (s *runState) classify(ctx workflow.Context, id string) (bool, error) {
	for _, need := range s.tasks[id].GetNeeds() {
		if s.taskRuns[need].GetStatus() == orkstackv1.Status_STATUS_SKIPPED {
			return false, s.markSkipped(ctx, id, fmt.Sprintf("upstream task %q was skipped", need))
		}
	}

	shouldRun, err := evaluateCondition(s.tasks[id].GetCondition(), s.inputs, s.upstreamOutputs(id))
	if err != nil {
		return false, err
	}
	if !shouldRun {
		return false, s.markSkipped(ctx, id,
			fmt.Sprintf("condition %q evaluated false", s.tasks[id].GetCondition()))
	}
	return true, nil
}

// markSkipped records a skip. The Event contract has no task-skipped type, so
// a skip is reported as a task log event carrying STATUS_SKIPPED; the
// authoritative record is TaskRun.Status.
//
// started_at stays UNSET, because the task never reached an Orker. Only
// schedule() stamps it, and that absence is how a client tells "never started"
// from "started, then ended": a skipped task has a finished_at (when the run
// resolved it) and no started_at, so nothing downstream has to invent a
// zero-length execution that did not happen.
func (s *runState) markSkipped(ctx workflow.Context, id, reason string) error {
	now := timestamppb.New(workflow.Now(ctx))
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_SKIPPED
	taskRun.FinishedAt = now
	// Terminal, so this task's pin may now have none left (§6.4). A pin whose
	// every member is skipped was never opened, so this is a no-op for it.
	defer s.closePinFor(ctx, id)
	return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_LOG, id,
		orkstackv1.Status_STATUS_SKIPPED, "task skipped: "+reason)
}

// upstreamFor returns the results of every completed task in id's transitive
// needs closure.
// upstreamFor returns one UpstreamRef per completed task in id's transitive
// needs closure.
//
// It carries REFERENCES ONLY. Fan-in is O(ancestors) small records, never a
// transitive copy of every ancestor's payload: the Orker fetches each
// ancestor's values.json and artifacts from the store, which is where bytes
// live (OQ-2, AGENTS.md §2.2).
func (s *runState) upstreamFor(id string) []*orkstackv1.UpstreamRef {
	refs := make([]*orkstackv1.UpstreamRef, 0, len(s.closure[id]))
	for _, need := range s.closure[id] {
		result, ok := s.results[need]
		if !ok {
			continue
		}
		refs = append(refs, &orkstackv1.UpstreamRef{
			TaskId:    need,
			CommitUri: result.GetCommit().GetUri(),
			Values:    result.GetValuesRef(),
			Artifacts: result.GetArtifacts(),
		})
	}
	return refs
}

// upstreamOutputs is the condition-evaluation view of the same closure: each
// completed ancestor's decoded values.json.
func (s *runState) upstreamOutputs(id string) map[string]map[string]any {
	out := make(map[string]map[string]any, len(s.closure[id]))
	for _, need := range s.closure[id] {
		if values, ok := s.outputs[need]; ok {
			out[need] = values
		}
	}
	return out
}

// schedule emits the scheduled event and starts the ExecuteTask activity.
func (s *runState) schedule(ctx workflow.Context, id string) (workflow.Future, error) {
	t := s.tasks[id]
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_RUNNING
	taskRun.StartedAt = timestamppb.New(workflow.Now(ctx))

	// Two words for one decision: the queue is how the engine dispatches, the
	// pool is what the author wrote. Both come out of routeForTask, so
	// everything below this line that a client can read says pool, and that
	// pool names the Orkers the activity really goes to.
	queue, err := s.queueForTask(t)
	if err != nil {
		return nil, err
	}
	pool := s.poolForTask(t)
	taskRun.Pool = pool
	// Empty for an unpinned task, which is the proto default — so an unpinned
	// TaskRun is byte-identical to what it was before pins existed.
	taskRun.Pin = t.GetPin()

	// A pinned task's activity is scheduled on its pin's SESSION context
	// rather than on the run context, and that substitution is the whole
	// mechanism: the SDK overrides ActivityOptions.TaskQueue with the
	// session's resource-specific queue for a call made on a session context,
	// which is how the task reaches the ONE Orker the session opened on (§3).
	// Nothing else about the call changes — the options below, the queue in
	// them, the pool a client is told — and for an unpinned task not even the
	// context does.
	activityBase := ctx
	var pinContext *orkstackv1.PinContext
	p, err := s.pinFor(id)
	if err != nil {
		return nil, err
	}
	if p != nil {
		if p.ctx == nil {
			// Unreachable through the loop, which holds a member until its pin
			// is open. Stated rather than dereferenced: scheduling a pinned
			// task on the run context would silently run it on any Orker of
			// the pool, without the shared directory it was written for.
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("task %q was scheduled before pin %q opened", id, p.name),
				"INVALID_PIN", nil)
		}
		activityBase = p.ctx
		pinContext = &orkstackv1.PinContext{
			Name: p.name,
			// ONE opening of this pin, taken from the session the engine
			// opened and from nothing else. The Orker keys the shared
			// directory by it.
			InstanceId:     workflow.GetSessionInfo(p.ctx).SessionID,
			TimeoutSeconds: int32(p.timeout / time.Second),
		}
	}

	scheduled := "task scheduled on pool " + pool
	if p != nil {
		scheduled += ", pin " + p.name
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, id,
		orkstackv1.Status_STATUS_RUNNING, scheduled); err != nil {
		return nil, err
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, id,
		orkstackv1.Status_STATUS_RUNNING, "task dispatched to an orker"); err != nil {
		return nil, err
	}

	// The three timeouts must not be confused, and this is where the
	// confusion would happen (§6.3):
	//
	//   timeout_seconds       the TASK's wall clock. The ORKER enforces it,
	//                         kills the container, and returns
	//                         TimedOut/CONTAINER_TIMEOUT. It fires first.
	//   HeartbeatTimeout      Temporal's dead-worker detector. Without it the
	//                         SDK throttles heartbeats and a dead Orker is
	//                         only noticed at StartToClose.
	//   StartToCloseTimeout   the backstop for an Orker wedged OUTSIDE the
	//                         container run. It has to be generous, because
	//                         inside it sit input staging, the image pull,
	//                         hashing, and uploading up to
	//                         artifact_max_count x artifact_max_bytes.
	//
	// A fixed transfer budget was the earlier draft and is not survivable:
	// retryPolicyFor returns MaximumAttempts: 1 whenever `retry:` is absent,
	// so a legal large upload would be terminal on its first timeout with no
	// retry at all. The budget is therefore resolved at submit, where process
	// configuration is readable, and carried on the plan — the workflow only
	// adds it up, which keeps this a pure function of its arguments.
	taskTimeout := time.Duration(timeoutSecondsFor(t)) * time.Second
	grace := time.Duration(graceSecondsFor(t)) * time.Second
	startToClose := taskTimeout + grace + time.Duration(s.plan.TransferBudgetSeconds)*time.Second

	activityCtx := workflow.WithActivityOptions(activityBase, workflow.ActivityOptions{
		// Still the POOL's queue, for a pinned task as for any other. The SDK
		// swaps it for the session's own at ExecuteActivity, so setting it
		// here is harmless and ignored — and it stays because the pool is what
		// the author wrote and what every client is told (§6.3).
		TaskQueue:           queue,
		StartToCloseTimeout: startToClose,
		// Temporal delivers cancellation to an activity ONLY through a
		// heartbeat response, so an Orker that never heartbeats can never be
		// cancelled. This timeout is the DEAD-ORKER detector; how quickly a
		// live one hears about a cancellation is set Orker-side, by its tick
		// and the matching MaxHeartbeatThrottleInterval cap on its worker.
		HeartbeatTimeout: orkerHeartbeatTimeout,
		// Without this the workflow may mark the task terminal before the
		// Orker has finished killing the container and cleaning up.
		WaitForCancellation: true,
		RetryPolicy:         retryPolicyFor(t),
	})

	commitURI, err := s.commitURIFor(id)
	if err != nil {
		return nil, err
	}
	namespace := s.wf.GetNamespace()
	input := &orkstackv1.ExecuteTaskInput{
		RunId:          s.runID,
		OrkflowName:    s.wf.GetName(),
		OrkflowVersion: s.wf.GetVersion(),
		// READ from the version document, never computed here (§4.4). The
		// digest is stamped at APPLY, after image resolution, so it describes
		// the definition this run actually pinned. Computing it in workflow
		// code would be deterministic but would describe whatever the reader
		// reconstructed, and would record a digest for a definition that was
		// never resolved at all.
		OrkflowDefinitionDigest: s.wf.GetDefinitionDigest(),
		Namespace:               namespace,
		Pool:                    pool,
		TaskId:                  id,

		IdempotencyKey: task.IdempotencyKeyFor(namespace, s.runID, id),
		CommitUri:      commitURI,

		// The image is the APPLY-TIME resolution stamped into the immutable
		// version document. The engine never resolves a tag here: workflow
		// code may not reach a registry, and a run must execute what its
		// definition pinned even if the tag has since moved (§4.1).
		Image:   t.GetImage(),
		Command: t.GetCommand(),
		// RAW, un-rendered. Rendering stays in the Orker because
		// `artifact "<task>/<name>"` resolves to a container path only the
		// Orker knows after staging (OQ-1).
		With:    t.GetWith(),
		Env:     t.GetEnv(),
		Secrets: t.GetSecrets(),

		TimeoutSeconds:                 int32(timeoutSecondsFor(t)),
		CancellationGracePeriodSeconds: graceSecondsFor(t),
		Resources:                      t.GetResources(),
		Network:                        t.GetNetwork(),
		Limits:                         s.plan.Limits.Proto(),

		RunInput: s.input,
		Upstream: s.upstreamFor(id),

		RunAsOrkerUser: t.GetRunAsOrkerUser(),

		// The selection was decided at SUBMIT (pools.go telemetryFor) from the
		// namespace's policy and the orkflow's labels — both registry/document
		// state a deterministic workflow may not read itself (§2.1) — and
		// travels on the plan like CommitRoot and Limits. Nil-safe: a plan with
		// no selection produces a nil field here, unchanged from before this
		// field existed.
		Telemetry: s.plan.Telemetry.Proto(),

		// Set ONLY for a pinned task. An unpinned task's payload is therefore
		// byte-identical to what it was before this field existed — protojson
		// omits an unset message field — which is what lets the golden
		// history fixture keep matching without being re-recorded (§10.2).
		Pin: pinContext,
	}
	// The wire bound is checked HERE, deterministically, before scheduling.
	// The deployment's limit is replay-pinned on the plan; the checker also
	// preserves Temporal's absolute 256 KiB ceiling.
	if err := task.CheckInputSize(input, s.plan.Limits.Proto().GetValuesMaxBytes()); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(
			err.Error(), task.CategoryContractViolation, err)
	}
	return workflow.ExecuteActivity(activityCtx, task.ActivityExecuteTask, input), nil
}

// commitURIFor composes one activity's commit URI.
//
// It is plan.CommitRoot + "/" + artifacts.CommitKey(...), which is a pure
// function of workflow arguments and replays identically. artifacts.CommitKey
// is the single shared renderer — the Orker MUST use the value it was sent
// rather than re-deriving one, so there is exactly one convention with exactly
// one owner (§5.2).
//
// An empty CommitRoot yields an empty URI rather than an error: a plan built
// before the commit protocol existed, or by a test harness, still runs, and
// the Orker simply has no commit to check or write.
func (s *runState) commitURIFor(taskID string) (string, error) {
	if s.plan.CommitRoot == "" {
		return "", nil
	}
	key, err := artifacts.CommitKey(s.wf.GetNamespace(), s.runID, taskID)
	if err != nil {
		return "", temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("task %q: compose the commit uri: %v", taskID, err), "INVALID_COMMIT_KEY", err)
	}
	return strings.TrimSuffix(s.plan.CommitRoot, "/") + "/" + key, nil
}

// collect awaits one task future and records its outcome.
func (s *runState) collect(ctx workflow.Context, id string, future workflow.Future) error {
	// This task is terminal on every path out of here, so this is where its
	// pin learns whether it was the last one (§6.4).
	defer s.closePinFor(ctx, id)
	taskRun := s.taskRuns[id]

	result := &orkstackv1.ExecuteTaskResult{}
	if err := future.Get(ctx, result); err != nil {
		message := err.Error()
		// BEFORE recordFailure, because the two answers differ: a member of a
		// pin whose session has failed did not fail on its own and did not get
		// cancelled in the sense a client means — the platform lost the host
		// it was pinned to (§6.5).
		if loss := s.pinLoss(ctx, id); loss != nil {
			taskRun.Status = orkstackv1.Status_STATUS_FAILED
			taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
			taskRun.Error = loss
			// Truthful about what Temporal saw on the way to this outcome; the
			// code on the Error is the primary reason.
			taskRun.RetryState = retryStateOf(err)
			// "Which Orker died?" is the first question a lost pin raises, and
			// this task cannot answer it: its host died without reporting, so
			// neither an ExecuteTaskResult nor an ActivityFailureDetail exists
			// to read orker_id off. A sibling's answer IS this task's, because
			// a pin never moves — attributeLostPinHost. Here rather than only
			// at pin close so the snapshot published with THIS failure already
			// names the host.
			s.attributeLostPinHost(s.pins[s.tasks[id].GetPin()])
			message = loss.GetMessage()
		} else {
			s.recordFailure(ctx, id, err)
		}
		// taskRun.Status, not a hardcoded FAILED: recordFailure just decided
		// between FAILED and CANCELED, and the event must say the same thing
		// the snapshot does.
		if emitErr := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
			taskRun.Status, message); emitErr != nil {
			// The task's own cause is this run's machine-readable reason
			// (AGENTS.md §15). A snapshot-write failure on the way out is
			// logged, never allowed to replace what the Orker actually said.
			workflow.GetLogger(ctx).Error("emit task failure failed",
				"run_id", s.runID, "task_id", id, "error", emitErr)
		}
		return err
	}

	taskRun.Status = orkstackv1.Status_STATUS_SUCCEEDED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Artifacts = result.GetArtifacts()
	taskRun.SystemArtifacts = result.GetSystemArtifacts()
	taskRun.ValuesRef = result.GetValuesRef()
	taskRun.Commit = result.GetCommit()
	taskRun.ExitCode = result.GetExitCode()
	// WHICH Orker produced this, for every task, pinned or not (§9). The field
	// already travelled on the result; nothing copied it onto the snapshot.
	// For a pin it is the client-visible proof the pin held: same pin, same
	// orker_id across every member.
	taskRun.OrkerId = result.GetOrkerId()
	if result.GetAttempt() > 0 {
		taskRun.Attempt = result.GetAttempt()
	}

	// TaskRun.output is the inline copy, decoded FOR DISPLAY ONLY. values_ref
	// is the authoritative reference, and a downstream task reads the
	// committed object rather than this.
	values := decodeValues(result.GetValuesJson())
	output, convErr := payloadOf(values)
	if convErr != nil {
		taskRun.Status = orkstackv1.Status_STATUS_FAILED
		taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
		taskRun.Error = &orkstackv1.Error{
			Code:     "TASK_OUTPUT_INVALID",
			Message:  convErr.Error(),
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
		}
		if emitErr := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
			orkstackv1.Status_STATUS_FAILED, convErr.Error()); emitErr != nil {
			// As above: the contract violation is the reason, not the storage
			// error that followed it.
			workflow.GetLogger(ctx).Error("emit task failure failed",
				"run_id", s.runID, "task_id", id, "error", emitErr)
		}
		return convErr
	}
	taskRun.Output = output
	s.results[id] = result
	s.outputs[id] = values

	return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, id,
		orkstackv1.Status_STATUS_SUCCEEDED, "task succeeded")
}

// cancelInFlight ends every task this run never got to finish because the
// run itself is ending: every task still actually executing is canceled so
// the Orker stops its container (AGENTS.md §3.3) instead of leaving it
// running against a run that already closed; every task that never even
// started is marked CANCELED instead of staying PENDING forever.
//
// ctx must already be safe to make activity calls on — the caller passes its
// disconnected substitution when the workflow's own context is done,
// unchanged otherwise (see fail).
func (s *runState) cancelInFlight(ctx workflow.Context, inFlight map[string]workflow.Future) {
	// s.order, never the map's own range: workflow code must stay
	// deterministic, and Go's map iteration order is randomized on purpose.
	for _, id := range s.order {
		future, ok := inFlight[id]
		if !ok {
			continue
		}
		_ = s.collect(ctx, id, future)
	}
	for _, id := range s.order {
		if s.taskRuns[id].GetStatus() == orkstackv1.Status_STATUS_PENDING {
			if err := s.markCanceled(ctx, id, "run ended before this task started"); err != nil {
				workflow.GetLogger(ctx).Error("mark canceled failed",
					"run_id", s.runID, "task_id", id, "error", err)
			}
		}
	}
}

// markCanceled records a task that never started because the run ended
// first. Mirrors markSkipped: the Event contract has no task-canceled type,
// so this is a task log event carrying STATUS_CANCELED; TaskRun.Status is the
// authoritative record.
//
// started_at stays UNSET for the same reason as markSkipped: this task never
// reached an Orker. That absence is load-bearing here rather than merely
// tidy — CANCELED is also the status of a task that WAS running and was then
// killed, and started_at is the only thing on the snapshot that tells the two
// apart. Stamping it made a task that never ran read as one that started and
// was cancelled, which is what the run page drew.
func (s *runState) markCanceled(ctx workflow.Context, id, reason string) error {
	now := timestamppb.New(workflow.Now(ctx))
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_CANCELED
	taskRun.FinishedAt = now
	// As in markSkipped: terminal, so the pin may be done with (§6.4). This is
	// the hook cancelInFlight relies on — it marks every never-started task
	// CANCELED, which is what makes every pin's members terminal on the
	// failure path and lets fail() wait on `closed` without hanging.
	defer s.closePinFor(ctx, id)
	return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_LOG, id,
		orkstackv1.Status_STATUS_CANCELED, "task canceled: "+reason)
}

// recordFailure reads what the Orker actually said and records it as DATA.
//
// The engine stops flattening everything into TASK_FAILED with a stringified
// error. It reads the ApplicationError type — which IS the §8.1 category — and
// the ActivityFailureDetail, and populates Error{code, message, details,
// retryable, category}. The event type stays TASK_FAILED; the category is data
// on the error, so the UI can say "contract violation — this image did not
// write values.json" instead of rendering a Go error string (§8.3).
//
// The failure detail is what keeps a failed attempt as inspectable as a
// successful one: without it, an exit-1 task's uploaded test report and
// captured stderr would be unreachable.
func (s *runState) recordFailure(ctx workflow.Context, id string, cause error) {
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))

	failure := &orkstackv1.Error{
		Code:    "TASK_FAILED",
		Message: cause.Error(),
		Details: fmt.Sprintf("task %q", id),
	}

	// WHY RETRYING STOPPED, as data on the snapshot.
	//
	// Temporal owns retries, so the workflow never sees the attempts it did not
	// get: what it does get, on the *ActivityError wrapping every activity
	// failure, is the reason the scheduler gave up — retries exhausted, a
	// non-retryable category, a timeout, a cancellation. Reading it is the
	// difference between "this task failed" and "this task failed three times
	// and then Temporal stopped".
	//
	// NO workflow.GetVersion GATE. This is a read of the error the workflow
	// already holds, written onto the local snapshot — the same kind of
	// derived, data-only change the ApplicationError and TimeoutError branches
	// immediately below already are. It issues no command, changes no decision
	// and reaches history through nothing: a replay of an old execution takes
	// exactly the same path and merely fills one more field of a message that
	// was never in history to begin with. Gating it would put a version marker
	// in history for a field that cannot affect it.
	taskRun.RetryState = retryStateOf(cause)

	var applicationErr *temporal.ApplicationError
	if errors.As(cause, &applicationErr) {
		category := applicationErr.Type()
		failure.Category = categoryEnumFor(category)
		failure.Retryable = !applicationErr.NonRetryable()

		// The target is a **ActivityFailureDetail: Details assigns the value
		// it holds, which is a *ActivityFailureDetail both in-process and
		// after the proto converter decodes it from the wire.
		var detail *orkstackv1.ActivityFailureDetail
		if err := applicationErr.Details(&detail); err == nil {
			// WHICH Orker produced this outcome, for every task, pinned or not
			// (§9) — read as soon as a detail decodes, before the reason-code
			// gate below, because a failed attempt is exactly when an operator
			// wants to know which host it happened on. Nil-safe: a detail
			// without one leaves the field empty, which is what it already
			// said.
			taskRun.OrkerId = detail.GetOrkerId()
		}
		if detail.GetReasonCode() != "" {
			failure.Code = detail.GetReasonCode()
			if detail.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
				failure.Category = detail.GetCategory()
			}
			taskRun.Artifacts = detail.GetArtifacts()
			taskRun.SystemArtifacts = detail.GetSystemArtifacts()
			taskRun.ExitCode = detail.GetExitCode()
			if detail.GetAttempt() > 0 {
				taskRun.Attempt = detail.GetAttempt()
			}
			if notes := detail.GetScanNotes(); len(notes) > 0 {
				// Scan notes are notes ON the workload's error, never a
				// replacement for it (§7.1 step 14).
				failure.Details = failure.Details + "; " + strings.Join(notes, "; ")
			}
		}
	}

	// A Temporal-produced timeout arrives as *TimeoutError rather than an
	// ApplicationError, so the category is read from the timeout TYPE. A
	// HEARTBEAT timeout is the one timeout that stays RETRYABLE regardless:
	// it means the Orker died, which is an infrastructure fact about the
	// host and not a statement about the workload (§8.1).
	var timeoutErr *temporal.TimeoutError
	if errors.As(cause, &timeoutErr) {
		failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT
		failure.Code = "ACTIVITY_TIMEOUT"
		if timeoutErr.TimeoutType() == enumspb.TIMEOUT_TYPE_HEARTBEAT {
			failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
			failure.Code = "ORKER_HEARTBEAT_TIMEOUT"
			failure.Retryable = true
		}
	}
	if temporal.IsCanceledError(cause) {
		failure.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED
		failure.Code = "TASK_CANCELED"
		failure.Retryable = false
		// A task that was CANCELED did not FAIL, and the outer status is what
		// every client reads. Categorizing the nested Error while leaving the
		// status at FAILED said two different things about one task.
		taskRun.Status = orkstackv1.Status_STATUS_CANCELED
	}
	taskRun.Error = failure
}

// retryStateOf reads why Temporal stopped retrying, off the *ActivityError that
// wraps every activity failure the workflow sees.
//
// errors.As, not a type assertion: the SDK nests the cause inside the
// ActivityError, and a caller may have wrapped the whole thing again on the way
// here. An error that is not an activity failure at all — a workflow-side
// contract check, say — has no retry state to report and yields UNSPECIFIED.
func retryStateOf(cause error) orkstackv1.RetryState {
	var activityErr *temporal.ActivityError
	if !errors.As(cause, &activityErr) {
		return orkstackv1.RetryState_RETRY_STATE_UNSPECIFIED
	}
	return retryStateEnumFor(activityErr.RetryState())
}

// retryStateEnumFor maps Temporal's RetryState onto OrkStack's own.
//
// The two enums are NOT numerically compatible and must never be bridged by a
// cast: temporal's RETRY_STATE_IN_PROGRESS is 1, where OrkStack's 1 is
// NON_RETRYABLE_FAILURE, so every value from 1 up would be reported as the
// wrong one. The switch is exhaustive over the SDK's enum, and an unknown value
// from a future SDK degrades to UNSPECIFIED — "OrkStack does not know" is an
// honest answer, and it is what a snapshot written before this field existed
// already says.
//
// IN_PROGRESS maps to UNSPECIFIED rather than getting a member of its own: it
// names a retry that has not finished, and this is only ever read from the
// error of a task that already reached a terminal outcome.
func retryStateEnumFor(state enumspb.RetryState) orkstackv1.RetryState {
	switch state {
	case enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE:
		return orkstackv1.RetryState_RETRY_STATE_NON_RETRYABLE_FAILURE
	case enumspb.RETRY_STATE_TIMEOUT:
		return orkstackv1.RetryState_RETRY_STATE_TIMEOUT
	case enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED:
		return orkstackv1.RetryState_RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED
	case enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET:
		return orkstackv1.RetryState_RETRY_STATE_RETRY_POLICY_NOT_SET
	case enumspb.RETRY_STATE_CANCEL_REQUESTED:
		return orkstackv1.RetryState_RETRY_STATE_CANCEL_REQUESTED
	case enumspb.RETRY_STATE_INTERNAL_SERVER_ERROR:
		return orkstackv1.RetryState_RETRY_STATE_INTERNAL_SERVER_ERROR
	default:
		return orkstackv1.RetryState_RETRY_STATE_UNSPECIFIED
	}
}

// categoryEnumFor maps an ApplicationError type string to the proto enum.
// The category names ARE the type strings, which is what makes
// NonRetryableErrorTypes work at all (§8.1).
func categoryEnumFor(category string) orkstackv1.FailureCategory {
	switch category {
	case task.CategoryActivityFailed:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED
	case task.CategoryInfrastructureError:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	case task.CategoryContractViolation:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION
	case task.CategoryTimedOut:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT
	case task.CategoryCancelled:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED
	default:
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED
	}
}

// fail terminates the run after a task failure or cancellation.
//
// It ends the OTHER tasks too. Fail-fast is this engine's only failure mode —
// there is no continue-on-error — so the moment one task's future errors the
// run is over, and every task still executing has to stop with it rather than
// keep a container alive against a closed run (§3.3), while every task that
// never started has to say so rather than sit at PENDING forever.
//
// cancelRun is the cancel half of the context every activity was scheduled on
// (see RunWorkflow); inFlight is the loop's live future set, which fail drains.
func (s *runState) fail(
	ctx workflow.Context,
	cancelRun workflow.CancelFunc,
	inFlight map[string]workflow.Future,
	taskID string,
	cause error,
) (*orkstackv1.Run, error) {
	s.finished = timestamppb.New(workflow.Now(ctx))
	s.runError = &orkstackv1.Error{
		Code:    "RUN_FAILED",
		Message: cause.Error(),
		Details: fmt.Sprintf("task %q", taskID),
	}

	// A pin that lost its host is the one failure that ARRIVES as a
	// cancellation without anything having cancelled the run: the SDK cancels
	// the session context when the session fails, so every member's future
	// returns a cancellation (§6.5). The run failed — nobody ended it — and
	// the run's reason is the pin's, not the word "canceled".
	if lost := s.pinLossOf(taskID); lost != nil {
		s.runError.Code = "RUN_FAILED"
		s.runError.Message = lost.GetMessage()
	}

	eventCtx := ctx
	eventType := orkstackv1.EventType_EVENT_TYPE_RUN_FAILED
	s.status = orkstackv1.Status_STATUS_FAILED
	// ctx.Err() first, and the cancellation of the CAUSE only when this task's
	// pin was not the thing that was lost. For an unpinned run the condition is
	// identical to what it has always been — pinLossOf is nil for every task of
	// a run with no pins — so no existing execution's behaviour moves.
	if ctx.Err() != nil || (temporal.IsCanceledError(cause) && s.pinLossOf(taskID) == nil) {
		// The workflow context is done; final projection must run on a
		// disconnected context or it would be canceled too.
		disconnected, cancelDisconnected := workflow.NewDisconnectedContext(ctx)
		defer cancelDisconnected()
		eventCtx = disconnected
		eventType = orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED
		s.status = orkstackv1.Status_STATUS_CANCELED
		s.runError.Code = "RUN_CANCELED"
		if taskRun, ok := s.taskRuns[taskID]; ok {
			taskRun.Status = orkstackv1.Status_STATUS_CANCELED
		}
	}

	// Gated because it changes the COMMAND stream: a run that is genuinely
	// open and mid-failure when this build is deployed would otherwise be
	// replayed against brand-new RequestCancelActivity commands and awaits at
	// a point where the old code simply returned — a determinism break for
	// that in-flight run (see replay_test.go, testdata/history/README.md).
	if v := workflow.GetVersion(ctx, "cancel-siblings-on-run-end", workflow.DefaultVersion, 1); v >= 1 {
		cancelRun()
		// eventCtx, not ctx: when the operator canceled the whole run, ctx is
		// already done and eventCtx is the disconnected substitution chosen
		// above precisely so the remaining activity calls are not rejected as
		// canceled. cancelInFlight makes such calls too (collect emits), so it
		// needs the same context. In the ordinary task-failure case ctx was
		// never canceled and eventCtx == ctx.
		s.cancelInFlight(eventCtx, inFlight)
	}

	// AFTER cancelInFlight and never before: it is what makes every pin
	// member terminal, which is what the terminal-task hook needs to signal
	// each pin's coroutine. Waiting first would block on a coroutine itself
	// waiting for a signal nothing had sent yet. eventCtx for the same reason
	// cancelInFlight gets it — a cancelled run's remaining activity calls run
	// on the disconnected substitution.
	s.awaitPinsClosed(eventCtx)

	if err := s.emit(eventCtx, eventType, taskID, s.status, s.runError.GetMessage()); err != nil {
		// The run is already failing for a recorded reason. That cause — a
		// task's failure or the operator's cancellation — is the run's
		// machine-readable reason (AGENTS.md §15); a snapshot-write error on
		// the terminal emit is logged rather than allowed to replace it.
		workflow.GetLogger(ctx).Error("emit terminal run event failed",
			"run_id", s.runID, "event", eventType.String(), "error", err)
	}
	return s.snapshot(), cause
}

// succeed terminates the run successfully, aggregating leaf task outputs.
func (s *runState) succeed(ctx workflow.Context) (*orkstackv1.Run, error) {
	// Every pin closed before the run reports success, so the terminal
	// snapshot describes a run that is genuinely finished — nothing of it is
	// still holding an Orker's session slot or a directory on its disk (§6.4).
	// Ordinarily a no-op: each pin was already closed by its own last member.
	s.awaitPinsClosed(ctx)

	aggregated := map[string]any{}
	for _, id := range s.order {
		if _, leaf := s.leaves[id]; !leaf {
			continue
		}
		if values, ok := s.outputs[id]; ok {
			aggregated[id] = values
		}
	}
	output, err := payloadOf(aggregated)
	if err != nil {
		return nil, err
	}

	s.output = output
	s.status = orkstackv1.Status_STATUS_SUCCEEDED
	s.finished = timestamppb.New(workflow.Now(ctx))
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, "",
		orkstackv1.Status_STATUS_SUCCEEDED, "run succeeded"); err != nil {
		return nil, err
	}
	return s.snapshot(), nil
}

// snapshot materializes the current Run read model.
//
// It carries CreatedAt because this snapshot is the run as every replica sees
// it: Temporal records it alongside each event, and a replica that never saw
// the submission has no other source for the field.
func (s *runState) snapshot() *orkstackv1.Run {
	run := &orkstackv1.Run{
		Id:             s.runID,
		Namespace:      s.wf.GetNamespace(),
		OrkflowName:    s.wf.GetName(),
		OrkflowVersion: s.wf.GetVersion(),
		// A run that started before the current pointer moved is still
		// explainable by the exact bytes it ran (§4.4). Read from the version
		// document for the same reason ExecuteTaskInput reads it.
		OrkflowDefinitionDigest: s.wf.GetDefinitionDigest(),
		Status:                  s.status,
		Input:                   s.input,
		Output:                  s.output,
		Error:                   s.runError,
		CreatedAt:               s.createdAt,
		StartedAt:               s.startedAt,
		FinishedAt:              s.finished,
		// Same selection every ExecuteTaskInput of this run carried (§2.1: the
		// plan is replay-pinned), so the run record shows what actually routed
		// each task's signals. Nil-safe, same as above.
		Telemetry: s.plan.Telemetry.Proto(),
	}
	for _, id := range s.order {
		run.TaskRuns = append(run.TaskRuns, s.taskRuns[id])
	}
	return run
}

// emit publishes one lifecycle event through the projection activity.
//
// The workflow numbers the event itself, from a counter that is part of its
// deterministic state. That number is what Temporal records when the activity
// is *scheduled*, so it is on the copy every replica can read, and it is the
// same number the bus stores and the archive keeps. The dedupe key is derived
// from the same counter, so activity retries cannot duplicate an event.
//
// If the snapshot write fails, the sequence number is consumed (burned) and
// will not appear in the published stream. This is unavoidable for replay
// consistency: the sequence counter is deterministic state recorded in Temporal
// history, and a replay must produce identical history. A snapshot write failure
// is terminal (fails the run), so the gap is visible only to auditors examining
// the history: the bus and archive contain sequences 1..N-1 and N+1..M, never
// with a gap that crosses a run boundary.
func (s *runState) emit(
	ctx workflow.Context,
	eventType orkstackv1.EventType,
	taskID string,
	status orkstackv1.Status,
	message string,
) error {
	s.eventSeq++
	event := &orkstackv1.Event{
		RunId:     s.runID,
		Sequence:  int64(s.eventSeq),
		Type:      eventType,
		TaskId:    taskID,
		Status:    status,
		Message:   message,
		Timestamp: timestamppb.New(workflow.Now(ctx)),
	}
	dedupeKey := fmt.Sprintf("%s#%d", s.runID, s.eventSeq)

	// A local activity is essential here: Temporal does not persist its input,
	// while a regular activity would put this complete snapshot straight back
	// into ActivityTaskScheduled.input and retain quadratic history growth. Its
	// only recorded result is the small URI reference.
	localCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout:    snapshotAttemptTimeout,
		ScheduleToCloseTimeout: snapshotActivityTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var snapshotURI string
	if err := workflow.ExecuteLocalActivity(
		localCtx, ActivityWriteSnapshot, s.snapshot(), int64(s.eventSeq),
	).Get(localCtx, &snapshotURI); err != nil {
		// Object storage is part of the durable record. Continuing would let a
		// run succeed without a readable terminal snapshot or complete archive,
		// so unlike the best-effort projection below this failure ends the run.
		return fmt.Errorf("write snapshot for event %d: %w", s.eventSeq, err)
	}

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           EventQueue,
		StartToCloseTimeout: eventActivityTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	future := workflow.ExecuteActivity(activityCtx, ActivityPublishEvent, event, snapshotURI, dedupeKey)
	if err := future.Get(ctx, nil); err != nil {
		// Projection is observability, not orchestration: a failed publish is
		// logged and the run continues.
		workflow.GetLogger(ctx).Error("publish event failed",
			"run_id", s.runID, "event", eventType.String(), "error", err)
	}
	return nil
}

// routeForTask resolves one task to its route: the queue its activity is
// scheduled on, and the pool a client is told it ran on.
//
// It is a table lookup and nothing else. The plan bound into this run's input
// already answered where every pool goes: the resolution — the task's own
// pool, then its namespace's default, then the built-in "main", then that
// pool's channel through task.RouteForPool — happened at submit, in
// PoolResolver.Plan, which is the last place allowed to read a store, the
// environment or the cluster. Here there is only the recorded answer, so a
// replay dispatches exactly where the original run did and a second engine
// finishing this run cannot redirect it onto its own defaults.
//
// Both halves come out of that ONE lookup, which is what stops them naming
// different sets of Orkers: the pool is the key and the queue is what the key
// maps to, so there is no second question to answer differently. The same
// property is held at submit by task.RouteForPool, which collapses a pool
// whose queue IS the deployment's bound channel onto that channel's own name —
// a client is never told `main` while the work runs on gpu's Orkers.
//
// A pool with no route fails the run rather than falling back to a channel
// nobody asked for: silently running a gpu task on the shared pool is worse
// than not running it. So does a plan that routes a pool onto one of the
// engine's own queues, which `ork` refuses at boot and the resolver refuses at
// submit, and which reaches here only from a harness that wired a plan by
// hand. Both are unreachable through SubmitRun, because the plan is built from
// this very definition; reached by hand, they say so.
//
// Pure function of the workflow input, so it holds the determinism boundary
// and a replay dispatches and reports exactly what the original run did.
func (s *runState) routeForTask(t *orkstackv1.Task) (task.Route, error) {
	pool := t.GetPool()
	if pool == "" {
		// A definition that reached this workflow without passing through the
		// resolver asks for the built-in default, which is the pool a document
		// that named none asked for. The plan either has a route for it or
		// refuses below; it is never invented here.
		pool = task.DefaultPool
	}
	queue, err := s.plan.Route(pool)
	if err != nil {
		reason := "INVALID_POOL"
		if errors.Is(err, task.ErrReservedQueue) {
			reason = "RESERVED_QUEUE"
		}
		return task.Route{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("task %q: %v", t.GetId(), err), reason, err)
	}
	return task.Route{Pool: pool, Queue: queue}, nil
}

// queueForTask is the queue a task's activity is scheduled on.
func (s *runState) queueForTask(t *orkstackv1.Task) (string, error) {
	r, err := s.routeForTask(t)
	return r.Queue, err
}

// poolForTask is where a task ran, in the word its author typed. It is what
// every client-facing surface reports: the TASK_SCHEDULED message `orkctl
// watch` prints and the run page renders, TaskRun.pool on the projection, and
// the coordinate the Orker keys its output by. It is the same lookup as
// queueForTask, so the two can never name different sets of Orkers.
//
// An unroutable pool reports the word the author typed; the run fails on it
// through queueForTask before any client reads this.
func (s *runState) poolForTask(t *orkstackv1.Task) string {
	r, err := s.routeForTask(t)
	if err != nil {
		return t.GetPool()
	}
	return r.Pool
}

// graceSecondsFor is the SIGTERM-to-SIGKILL window for one task. The default
// is AGENTS.md §3.3's 30 seconds; ordinary Unix signal handling is meant to be
// enough for cleanup inside it.
func graceSecondsFor(t *orkstackv1.Task) int32 {
	if seconds := t.GetCancellationGracePeriodSeconds(); seconds > 0 {
		return seconds
	}
	return 30
}

func timeoutSecondsFor(t *orkstackv1.Task) int {
	if t.GetTimeoutSeconds() > 0 {
		return int(t.GetTimeoutSeconds())
	}
	return 300
}

// retryPolicyFor converts the DSL retry policy to Temporal's. A missing policy
// means a single attempt: leaving it nil would inherit Temporal's default of
// unlimited retries, which contradicts the proto contract ("0 or 1 = no
// retries").
func retryPolicyFor(t *orkstackv1.Task) *temporal.RetryPolicy {
	retry := t.GetRetry()
	if retry == nil || retry.GetMaximumAttempts() <= 1 {
		return &temporal.RetryPolicy{MaximumAttempts: 1}
	}
	policy := &temporal.RetryPolicy{
		MaximumAttempts:    retry.GetMaximumAttempts(),
		InitialInterval:    time.Duration(retry.GetInitialIntervalSeconds()) * time.Second,
		BackoffCoefficient: retry.GetBackoffCoefficient(),
		MaximumInterval:    time.Duration(retry.GetMaximumIntervalSeconds()) * time.Second,
	}
	if policy.InitialInterval <= 0 {
		policy.InitialInterval = time.Second
	}
	if policy.BackoffCoefficient <= 0 {
		policy.BackoffCoefficient = 2.0
	}
	if policy.MaximumInterval <= 0 {
		policy.MaximumInterval = 100 * time.Second
	}
	return policy
}

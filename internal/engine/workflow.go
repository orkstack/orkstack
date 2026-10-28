package engine

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
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

// legacyPinCreationTimeout is how long every build before the
// pin-creation-timeout gate waited for SOME pin host in a pin's pool to have a
// free session slot, frozen: a hardcoded constant, never a setting. A run
// takes it when its history predates the gate (the DefaultVersion branch of
// pinCreationTimeoutFor) and when its plan carries no number. It is NOT the
// deployment's default, which is DefaultPinCreationTimeout (pinned_defaults.go,
// read at submit and nowhere else); the two are equal today, and a release
// that moves the default must leave this alone.
//
// What it bounds, whatever number a run takes, is the SessionOptions
// CreationTimeout, which the SDK uses as the creation activity's
// ScheduleToStartTimeout (docs/DSL.md "Pins"). Submit already refused a pool
// whose Orkers host no pins at all (PoolNoPinHostError), so it covers
// SATURATION only: every host is at its ORK_ORKER_MAX_PINS. The SDK retries
// creation on the "too many outstanding sessions" error with a 1s x 1.1
// backoff until it expires, and only then does the pin's first task fail with
// PIN_UNAVAILABLE.
const legacyPinCreationTimeout = 5 * time.Minute

// PinCreationTimeoutChange is the workflow.GetVersion change id of the pin
// creation timeout a run's plan carries (RunPlan.PinCreationTimeoutSeconds).
// Asked lazily, where a run first opens a pin (pinCreationTimeoutFor): a run
// that opens none records no marker, and a history recorded before the gate
// replays its DefaultVersion branch, the legacyPinCreationTimeout every build
// before it used. A carried generation answers from its carry (version).
const PinCreationTimeoutChange = "pin-creation-timeout"

// pinCreationTimeoutGateDisabledForTest is a TEST-ONLY switch, the precedent
// of refusedRouteGateDisabledForTest: when set, pinCreationTimeoutFor takes
// the DefaultVersion branch WITHOUT asking its version, so a recorder can
// write a history in the shape of the builds before the gate, and a negative
// replay test proves the committed fixtures can only replay because of it.
// Never set in production code.
var pinCreationTimeoutGateDisabledForTest atomic.Bool

// pinReleaseBudget is the slice of a DERIVED pin lifetime reserved for the
// release activity that removes /orkstack/shared, added on top of the members'
// own budgets.
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
// pinReleaseAttempts caps how many it gets (docs/OCI_ACTIVITY_CONTRACT.md
// §6.4).
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
//
// carry is what an earlier generation of this run handed it when it
// continued as new (continuation.go): nil for generation 0 — StartRun,
// startChild and the schedule fire all pass four arguments, and the missing
// fifth decodes as nil — and set only by this function's own continue-as-new.
// A carried generation's run-id argument is empty: its run id IS its
// workflow id (AGENTS.md §7), read from there, so a release older than
// generations, which drops the fifth payload, ends it INVALID_RUN before any
// side effect instead of running the run again from event 1. A carried
// generation restores its state from the saved state the carry names; one it
// cannot adopt ends explicitly, RUN_STATE_*, with a terminal event published
// after the carried sequence.
func RunWorkflow(
	ctx workflow.Context,
	runID string,
	wf *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	plan RunPlan,
	carry *orkstackv1.RunContinuation,
) (*orkstackv1.Run, error) {
	if carry != nil {
		// The run id IS the workflow id (AGENTS.md §7). A carried
		// generation's argument is empty, so a release older than
		// generations ends it INVALID_RUN before any side effect.
		runID = workflow.GetInfo(ctx).WorkflowExecution.ID
	} else if runID == "" {
		return nil, temporal.NewNonRetryableApplicationError("run id is required", "INVALID_RUN", nil)
	}
	if wf == nil {
		return nil, temporal.NewNonRetryableApplicationError("orkflow has no tasks", "INVALID_ORKFLOW", nil)
	}
	// A definition too large for a start payload travels by reference: the
	// start carries a stub, and every generation copies the definition into
	// its own history before it decides anything (definition_ref.go).
	started := wf
	loader, loaded, ended, end, endErr := loadDefinitionArgument(ctx, runID, wf, input, plan, carry)
	if end {
		return ended, endErr
	}
	if loader != nil {
		wf = loaded
	}
	if len(wf.GetTasks()) == 0 {
		return nil, temporal.NewNonRetryableApplicationError("orkflow has no tasks", "INVALID_ORKFLOW", nil)
	}

	state := newRunState(ctx, runID, wf, input, plan)
	// The arguments as passed, handed on unchanged at a switch: for a
	// definition passed by reference, the stub.
	state.wfArg, state.inputArg = started, input
	if loader != nil {
		state.adoptDefinitionLoad(loader, carry)
	}
	// Every task activity is scheduled on runCtx, never on ctx directly, so the
	// run has one handle that ends all of them at once. Temporal does NOT
	// cancel activities a workflow function abandons by returning: only an
	// explicit cancellation on the context they were scheduled with reaches a
	// running container (§3.3). fail() is what pulls it.
	runCtx, cancelRun := workflow.WithCancel(ctx)
	state.runCtx = runCtx
	if state.hasHandlers {
		// A SIBLING of runCtx, never a child of it: fail-fast's cancelRun()
		// does not reach it, and an operator's cancel — which cancels ctx —
		// does. A disconnected context would survive fail-fast too, and no
		// cancel could ever stop it (docs/DSL.md "Joins and failure
		// handlers"). Creating a context issues no command.
		state.handlerCtx, state.cancelHandlers = workflow.WithCancel(ctx)
	}
	inFlight := map[string]workflow.Future{}
	if carry == nil {
		// RUN_CREATED is emitted here, by the run itself, and not by whichever
		// API process accepted the submission. It is event 1 of every run that
		// reaches Temporal, so a replica rebuilding the stream from history
		// starts at 1 with no gap — which the API-published version could never
		// give it, because that copy only ever existed in one process's memory.
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
	} else {
		// A carried generation rebuilds the run's state from the saved
		// state its predecessor wrote, emitting nothing for the crossing
		// (continuation_restore.go). Anything it cannot adopt ends the run
		// explicitly, with a terminal event.
		code, message := state.restore(ctx, runCtx, carry)
		switch {
		case code == restoreCanceled:
			// The operator's cancel reached this generation during the read.
			return state.endUnrestorable(ctx, carry, "RUN_CANCELED", "run canceled")
		case code != "":
			return state.endUnrestorable(ctx, carry, code, message)
		case ctx.Err() != nil:
			// Restored, and the cancel arrived meanwhile: the rows are exact.
			return state.fail(ctx, cancelRun, inFlight, "", temporal.NewCanceledError())
		}
		state.logInfo(ctx, inFlight, "run restored its saved state",
			"state_bytes", carry.GetCheckpoint().GetSizeBytes(), "generation", state.generation)
		state.observeDecisionState("restore")
	}
	state.markGenerationBase(ctx)

	for {
		if readyLoopProbeForTest != nil {
			readyLoopProbeForTest.noteRetained(int64(state.retainedClosureEntries()))
			if readyLoopProbeForTest.pass != nil {
				readyLoopProbeForTest.pass()
			}
		}
		// The reserve is counted at most once per pass.
		state.beginPass()
		// (A) The history guard, check point 1: a run whose history plus what
		// its own failure path still needs reaches the cluster's limit stops
		// here, RUN_HISTORY_LIMIT, before the cluster terminates it
		// (history_guard.go). A no-op for a plan without a limit.
		if err := state.historyExhausted(ctx, inFlight); err != nil {
			return state.fail(ctx, cancelRun, inFlight, "", err)
		}
		// (B) The ONE switch site (continuation.go). A natural idle point is
		// here, right after a completion emptied the in-flight set and before
		// newly ready tasks dispatch; a quiesce reaches the same site through
		// the idle block below.
		state.updateContinuation(ctx, inFlight)
		if state.canSwitchNow(ctx, inFlight) {
			canErr, fatalTask, fatal := state.continueAsNew(ctx, inFlight)
			if fatal != nil {
				return state.fail(ctx, cancelRun, inFlight, fatalTask, fatal)
			}
			if canErr != nil {
				return nil, canErr
			}
			continue // state changed, or the attempt was given up
		}
		// (C) Fan-out items a hold kept back (hold.go).
		if state.heldFanOuts {
			if state.canceledBeforeDispatch(ctx) {
				return state.fail(ctx, cancelRun, inFlight, "", temporal.NewCanceledError())
			}
			if id, err := state.resumeHeldFanOuts(ctx, runCtx, inFlight); err != nil {
				return state.fail(ctx, cancelRun, inFlight, id, err)
			}
		}
		// (D) Schedule every task whose own dependencies are terminal.
		// Re-scan after each completion so a fast branch can advance without
		// waiting for an unrelated slow branch.
		for _, id := range state.readyTasks() {
			// An operator's cancel that reached the run in the same workflow
			// task as the completion that made id ready starts nothing more:
			// the run ends canceled here, before an emit on the canceled
			// context could burn a sequence number (cancel.go).
			if state.canceledBeforeDispatch(ctx) {
				return state.fail(ctx, cancelRun, inFlight, "", temporal.NewCanceledError())
			}
			// Check point 2: before every dispatch decision of the pass, with
			// the dispatch's own command counted (history_guard_lookahead.go).
			// O(1) after the pass's first count.
			if err := state.historyExhaustedBefore(ctx, inFlight, state.nextDispatchBytes(id)); err != nil {
				return state.fail(ctx, cancelRun, inFlight, "", err)
			}
			state.updateContinuation(ctx, inFlight)
			// classify is pure for a task that runs — it reads only terminal
			// rows, the run's inputs and their outputs, and emits nothing — so
			// its answer is remembered, and a task a hold keeps back costs a
			// map lookup per pass, not a condition evaluation.
			if !state.admitted[id] {
				run, err := state.classify(ctx, id)
				if err != nil {
					return state.fail(ctx, cancelRun, inFlight, id, err)
				}
				if !run {
					continue
				}
				state.admit(id)
			}
			// An awaited task starts no activity: it opens its deadline and
			// waits for a resolution. Before openPin, which it can never reach
			// (docs/DSL.md rule 15 refuses `pin:` on an await). Never held: it
			// starts nothing, and its deadline crosses a switch.
			if state.awaits[id] != nil {
				if err := state.openAwait(ctx, state.dispatchContext(runCtx, id), id); err != nil {
					return state.fail(ctx, cancelRun, inFlight, id, err)
				}
				continue
			}
			// A quiesce or the in-flight cap holds new dispatch: the task
			// stays PENDING and nothing is emitted (hold.go).
			if state.holdDispatch(ctx, id, inFlight) {
				continue
			}
			// A fan-out counts its array once and dispatches its items; it
			// is never itself an activity. Before openPin, which it can
			// never reach (docs/DSL.md rule 23 refuses `pin:` on a fan-out).
			if state.eaches[id] != nil {
				if err := state.expandEach(ctx, runCtx, id, inFlight); err != nil {
					return state.fail(ctx, cancelRun, inFlight, id, err)
				}
				continue
			}
			// A composed task starts a child run and waits on it like an
			// activity. Before openPin, which it can never reach
			// (docs/DSL.md rule 27 refuses `pin:` on a composed task).
			if state.children[id] != nil {
				future, err := state.startChild(ctx, state.dispatchContext(runCtx, id), id)
				if err != nil {
					return state.fail(ctx, cancelRun, inFlight, id, err)
				}
				inFlight[id] = future
				continue
			}
			// A pin is opened by the FIRST of its members the run actually
			// decides to run, and never before: a pin whose every member is
			// skipped costs an Orker nothing, and the pin's lifetime clock
			// starts when work is about to start. Opening does
			// not schedule this task — nothing is emitted for it, it stays
			// PENDING, and readyTasks holds it until the session is open.
			hold, err := state.openPin(runCtx, id)
			if err != nil {
				return state.fail(ctx, cancelRun, inFlight, id, err)
			}
			if hold {
				continue
			}
			future, err := state.schedule(state.dispatchContext(runCtx, id), id)
			if err != nil {
				return state.fail(ctx, cancelRun, inFlight, id, err)
			}
			inFlight[id] = future
		}

		// (E) The idle block. A pin that is opening is work in progress
		// exactly as an in-flight activity is: its members are held, so
		// readyTasks is empty for them, and a run with nothing else to do must
		// wait for its readiness rather than conclude there is nothing left.
		// With no budget and no hold every new branch below is skipped, and
		// this is the loop's original idle check.
		if len(inFlight) == 0 && state.openingPins() == 0 {
			if (state.quiescing || state.heldFanOuts) && ctx.Err() != nil {
				return state.fail(ctx, cancelRun, inFlight, "", temporal.NewCanceledError())
			}
			if state.canSwitchNow(ctx, inFlight) {
				continue // idle: (B) takes it, open awaits included
			}
			if state.quiescing && state.closingPins() == 0 && state.openedUnclosedPins() > 0 {
				if state.closureHasReadyWork() {
					// The pass only propagated skips: re-scan before judging.
					continue
				}
				// The one structural give-up: the open pin's remaining work
				// waits on an await.
				state.abandonQuiesce(ctx, inFlight, state.pinBlockedReason())
				continue
			}
			if state.openAwaits() == 0 && !(state.quiescing && state.closingPins() > 0) {
				if len(state.readyTasks()) == 0 && !state.heldFanOuts {
					break
				}
				// The prior scan only propagated skips (or a hold just
				// lifted); scan again without waiting for an activity.
				continue
			}
		}

		// (F) Register futures in declaration order. Future readiness comes
		// from recorded Temporal history, while fixed registration order
		// stabilizes the case where several futures are ready in one workflow
		// task.
		selector := workflow.NewSelector(ctx)
		selectedID := ""
		selectedItem := int32(-1)
		selectedPin := ""
		selectedClosed := false
		for _, id := range state.order {
			if future, ok := inFlight[id]; ok {
				taskID := id
				selector.AddFuture(future, func(workflow.Future) {
					selectedID = taskID
				})
			}
			// Then, for a fan-out, each in-flight item in ascending index: a
			// fixed order, so which of several ready items is taken first is
			// this code's property, and the selected key says it was an item.
			if e := state.eaches[id]; e != nil {
				for index := int32(0); index < e.count; index++ {
					future, ok := inFlight[itemFlightKey(id, index)]
					if !ok {
						continue
					}
					taskID, itemIndex := id, index
					selector.AddFuture(future, func(workflow.Future) {
						selectedID = taskID
						selectedItem = itemIndex
					})
				}
			}
		}
		// Then every opening pin's readiness, in sorted pin-name order and
		// AFTER the task futures — a fixed position, so which of several
		// simultaneously ready futures the loop takes first is a property of
		// this code and not of iteration order.
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
		// While quiescing ONLY, every closing pin's close, in pin-name order:
		// a quiescing run with nothing else in flight waits for its pins to
		// close, so it can switch.
		if state.quiescing {
			for _, name := range state.pinOrder {
				if p := state.pins[name]; p.doneSent && p.closed != nil && !p.closed.IsReady() {
					selector.AddFuture(p.closed, func(workflow.Future) { selectedClosed = true })
				}
			}
		}
		// Then the resolution channel, and then every open await's deadline in
		// declaration order — fixed positions, AFTER task futures and pin
		// readiness, so which of several ready cases the loop takes first is a
		// property of this code and not of iteration order. A resolution
		// recorded in the same workflow task as a deadline is taken first.
		var resolution *orkstackv1.AwaitSignal
		resolved := false
		expired := ""
		if state.resolutions != nil {
			selector.AddReceive(state.resolutions, func(c workflow.ReceiveChannel, _ bool) {
				c.Receive(ctx, &resolution)
				resolved = true
			})
			for _, id := range state.order {
				if a := state.awaits[id]; a != nil && a.open {
					awaitID := id
					selector.AddFuture(a.deadline, func(workflow.Future) { expired = awaitID })
				}
			}
		}
		if readyLoopProbeForTest != nil && readyLoopProbeForTest.selector != nil {
			readyLoopProbeForTest.selector()
		}
		if readyLoopProbeForTest != nil && readyLoopProbeForTest.selectorRun != nil {
			readyLoopProbeForTest.selectorRun(state.runID)
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
		if selectedClosed {
			continue // a pin closed: the switch may now be possible
		}
		if resolved {
			if err := state.resolve(ctx, resolution); err != nil {
				return state.fail(ctx, cancelRun, inFlight, resolution.GetTaskId(), err)
			}
			continue
		}
		if expired != "" {
			// expire always returns an error, the one the run ends with: the
			// await's deadline, or the cancellation that settled its timer.
			return state.fail(ctx, cancelRun, inFlight, expired, state.expire(ctx, expired))
		}
		if selectedItem >= 0 {
			key := itemFlightKey(selectedID, selectedItem)
			future := inFlight[key]
			delete(inFlight, key)
			if err := state.collectItem(ctx, runCtx, selectedID, selectedItem, future, inFlight, false); err != nil {
				return state.fail(ctx, cancelRun, inFlight, selectedID, err)
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
	// pinMemberClosures is the one retained needs-closure memo: an opened
	// pin's unfinished members' closures (pinMemberClosure), reused by
	// pinClosureAllows' per-pass set and closureHasReadyWork until the pin
	// closes. Every other closure is computed on demand (closureOf) and not
	// kept, so newRunState and a run's retained memory stay O(T).
	pinMemberClosures map[string][]string
	leaves            map[string]struct{}

	// pins is one entry per DECLARED pin, keyed by name, and pinOrder is their
	// names sorted. Both are built once in newRunState from wf.Pins, so the
	// set never changes during a run and iteration is over pinOrder — never
	// over the map, which is cancelInFlight's rule and the same reason it has
	// one (Go randomizes map order on purpose, and workflow code may not).
	pins     map[string]*pinState
	pinOrder []string

	// awaits is one entry per awaited task, keyed by task id, built once in
	// newRunState; iteration is over order, never the map. resolutions is the
	// one signal channel every resolution arrives on, and is nil for a run
	// with no await — such a run creates no channel and registers none.
	awaits      map[string]*awaitState
	resolutions workflow.ReceiveChannel

	// eaches is one entry per fan-out task, keyed by task id, built once in
	// newRunState; iteration is over order, never the map. eachShare is the
	// Orkflow's declared item total, and itemInputLimit / itemArtifactMax
	// the two per-item bounds derived from it (each.go). A
	// run with no fan-out allocates nothing and leaves all three zero.
	eaches          map[string]*eachState
	eachShare       int32
	itemInputLimit  int64
	itemArtifactMax int32

	// children is one entry per composed task, keyed by task id, built once
	// in newRunState from plan.Children; iteration is over order, never the
	// map. Nil for a run with no composed task.
	children map[string]*childState

	createdAt *timestamppb.Timestamp
	startedAt *timestamppb.Timestamp
	status    orkstackv1.Status
	runError  *orkstackv1.Error
	finished  *timestamppb.Timestamp
	output    *orkstackv1.Payload
	eventSeq  int

	// Join policies (join.go). hasHandlers is whether any task is a failure
	// handler (join any_failed or all_done); every new branch of the failure
	// path is entered only when it is, so a definition without one takes
	// today's code byte for byte. handlerCtx is a SIBLING of runCtx — created
	// only when hasHandlers — that fail-fast's cancelRun() does not reach and
	// an operator's cancel does. runCtx is kept so the failing phase can
	// dispatch on the context the main loop would. detached is the lazily
	// created disconnected context eventContext hands out once ctx is done.
	hasHandlers    bool
	runCtx         workflow.Context
	handlerCtx     workflow.Context
	cancelHandlers workflow.CancelFunc
	detached       workflow.Context
	cancelDetached workflow.CancelFunc

	// The lean event path (lean_events.go). leanDecided and lean are the
	// run's one answer to the "lean-event-path" version, asked at the first
	// event that needs it. pendingItemPublishes is every fan-out item publish
	// scheduled and not yet awaited, oldest first, and publishCtx the
	// disconnected context they are scheduled on. All three stay empty on
	// the old path.
	leanDecided          bool
	lean                 bool
	pendingItemPublishes []workflow.Future
	publishCtx           workflow.Context

	// The history guard (history_guard.go). versions holds every version
	// decision this run asked, asked once each. hist counts the commands the
	// current workflow task issued beyond the raw counters; passReserve and
	// passCharge are the pass's reserve count and its dispatch charges;
	// pendingHandlers is the failing phase's count of handlers still PENDING;
	// handlerCount and handlerStartBytes feed the quick bound, set once in
	// newRunState; guardOff is set when a history predates the guard.
	versions                        map[string]workflow.Version
	hist                            historyCount
	passReserve, passCharge         reserveCount
	pendingHandlers                 int64
	handlerCount, handlerStartBytes int64
	guardOff                        bool

	// The writer, the holds and the bootstrap (continuation.go, hold.go,
	// continuation_restore.go). generation is this execution's place in
	// the run's chain (0, or the carry's). wfArg and inputArg are
	// RunWorkflow's own arguments, passed again unchanged at a switch.
	// switchDue says the generation has grown past its budget and switches
	// at its next idle point; quiescing that new dispatch is held so one
	// comes. switchRetryAt and quiesceRetryAt are the history (events,
	// bytes) below which no switch, or no new quiesce, is tried after a
	// failed write or a given-up hold; noSwitch, when set, says why this
	// generation never switches. baseEvents and baseBytes are the history it
	// started its growth from; genFirstSeq, genEvents and segments are the
	// events it published and the earlier generations' segments.
	// heldFanOuts says some expanded fan-out has items held back; capLogged
	// that the in-flight cap's log line was written; admitted remembers
	// every task classify said runs; pinClosure is pinClosureAllows' set,
	// rebuilt (lazily) once per pass; gaveUp is the last structural
	// give-up's reason; restoreErr is the last error a bootstrap read
	// returned. bootstrapStarted and bootstrapDeadline are the bootstrap's
	// start and its one read deadline, and bootstrapBatch the reads of its
	// current batch: shared by a definition passed by reference and the
	// checkpoint read after it (readBootstrapObject).
	generation            int64
	wfArg                 *orkstackv1.Orkflow
	inputArg              *orkstackv1.Payload
	switchDue             bool
	quiescing             bool
	holdStartedAt         time.Time
	switchRetryAt         [2]int64
	quiesceRetryAt        [2]int64
	noSwitch              string
	baseEvents, baseBytes int64
	genFirstSeq           int64
	genEvents             []*orkstackv1.Event
	segments              []*orkstackv1.EventSegmentRef
	heldFanOuts           bool
	capLogged             bool
	admitted              map[string]bool
	pinClosure            map[string]bool
	pinClosureFresh       bool
	gaveUp                string
	restoreErr            error
	bootstrapStarted      time.Time
	bootstrapDeadline     time.Time
	bootstrapBatch        int64

	// declaredRows is this run's declared rows — its tasks plus every
	// fan-out's max_items (DeclaredRows) — which decides whether it writes
	// snapshot overlays (snapshot_overlay.go). Derived from the definition.
	declaredRows int64
	// snapBase is the last full snapshot this generation wrote for one of
	// its events and how many overlays have followed it, moved only by what
	// writeEventSnapshot's markers record. Ephemeral: a generation's first
	// write is always full.
	snapBase SnapshotBase
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
	// PIN_TIMEOUT is measured from.
	openedAt time.Time
	// timeout is this pin's ExecutionTimeout: Pin.timeout_seconds, or the
	// derived sum. Held here so the value on PinContext, the value the session
	// was created with, and the value PIN_TIMEOUT compares against are one
	// number computed once.
	timeout time.Duration
	// creationTimeout is how long the pin's creation request waits for a free
	// slot (pinCreationTimeoutFor), decided once where the pin opens. Held
	// here so the value the session was created with and the value
	// PIN_UNAVAILABLE reports are one number computed once.
	creationTimeout time.Duration

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
	state := buildRunState(runID, wf, input, plan)
	// CreatedAt is the execution's own start time, taken from the
	// WorkflowExecutionStarted history event rather than from a clock, so
	// every replica that reads this run dates it identically and a replay
	// reproduces it exactly. StartedAt stays unset until the run actually
	// starts, one event later.
	state.createdAt = timestamppb.New(workflow.GetInfo(ctx).WorkflowStartTime)
	if len(state.awaits) > 0 {
		state.resolutions = workflow.GetSignalChannel(ctx, ResolveAwaitSignal)
	}
	return state
}

// buildRunState is newRunState without the two things only a workflow
// context has — the start time and the resolution channel — and so a pure
// function of its arguments.
func buildRunState(
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
		leaves:   map[string]struct{}{},
		status:   orkstackv1.Status_STATUS_PENDING,
		pins:     map[string]*pinState{},
		awaits:   map[string]*awaitState{},
		// Generation 0 publishes from the run's first event; a carried
		// generation from the one after its carry's (restore).
		genFirstSeq: 1,
	}
	state.declaredRows = DeclaredRows(wf)
	for _, t := range wf.GetTasks() {
		state.order = append(state.order, t.GetId())
		state.tasks[t.GetId()] = t
		state.taskRuns[t.GetId()] = &orkstackv1.TaskRun{
			TaskId: t.GetId(),
			Status: orkstackv1.Status_STATUS_PENDING,
		}
		state.leaves[t.GetId()] = struct{}{}
		// A task with a join other than all_succeeded carries it from the
		// run's first snapshot, as await, each and child do; unset for every
		// other task, so a run with no join snapshots exactly as before.
		if join := t.GetJoin(); join != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED {
			state.taskRuns[t.GetId()].Join = join
		}
		if isFailureHandler(t) {
			state.hasHandlers = true
		}
		// An awaited task carries its await state on the snapshot from the
		// run's first event, while still PENDING, so a client tells it from a
		// container task without reading the definition. Unset for every
		// other task: a run with no await snapshots exactly as before.
		if spec := t.GetAwait(); spec != nil {
			state.awaits[t.GetId()] = &awaitState{spec: spec}
			state.taskRuns[t.GetId()].Await = &orkstackv1.TaskAwait{Callback: spec.GetCallback()}
		}
		// A fan-out carries its TaskEach from the run's first snapshot, while
		// still PENDING; unset for every other task, so a run with no fan-out
		// snapshots exactly as before.
		if spec := t.GetEach(); spec != nil {
			if state.eaches == nil {
				state.eaches = map[string]*eachState{}
			}
			state.eaches[t.GetId()] = &eachState{spec: spec}
			state.taskRuns[t.GetId()].Each = &orkstackv1.TaskEach{}
		}
		// A composed task carries its TaskChild from the run's first
		// snapshot, while still PENDING; unset for every other task, so a
		// run with no composed task snapshots exactly as before.
		if ref := t.GetOrkflow(); ref != nil {
			if state.children == nil {
				state.children = map[string]*childState{}
			}
			state.children[t.GetId()] = newChildState(plan.Children[t.GetId()], t)
			state.taskRuns[t.GetId()].Child = &orkstackv1.TaskChild{
				OrkflowName:             ref.GetName(),
				OrkflowVersion:          ref.GetVersion(),
				OrkflowDefinitionDigest: ref.GetDefinitionDigest(),
			}
		}
	}
	if len(state.eaches) > 0 {
		state.eachShare, state.itemInputLimit, state.itemArtifactMax = eachBounds(wf, plan.Limits, plan.itemInputFloor())
	}
	for _, t := range wf.GetTasks() {
		// A failure handler's needs edges do not remove its needs from the
		// leaves: adding a rollback to an Orkflow must not drop deploy's
		// values from a succeeded run's output. A definition with no failure
		// handler computes the same set as before.
		if isFailureHandler(t) {
			continue
		}
		for _, need := range t.GetNeeds() {
			delete(state.leaves, need)
		}
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
	state.countHandlers()
	return state
}

// start moves the run from created to running. It is separate from
// newRunState so the RUN_CREATED event and the snapshot published with it
// describe a run that has not started yet.
func (s *runState) start(ctx workflow.Context) {
	s.status = orkstackv1.Status_STATUS_RUNNING
	s.startedAt = timestamppb.New(workflow.Now(ctx))
}

// closureOf returns the sorted transitive `needs` closure of id, computed on
// demand from the definition's needs and not retained. The DSL permits
// referencing any transitively upstream task, so the whole closure is
// threaded into the downstream activity input. It is exactly
// transitiveNeedsOf, so every command input is what an eager closure built
// at newRunState gave; one call costs O(|closure(id)|) time and memory,
// released when the caller is done with it. Its callers ask it at most once
// per task each: classify's conditions (upstreamOutputs; a task that runs is
// remembered by admitted, a skipped one is terminal) and its dispatch
// (upstreamFor, a composed task's input). An opened pin's members keep
// theirs in pinMemberClosures.
func (s *runState) closureOf(id string) []string {
	closure := transitiveNeedsOf(s.tasks, id)
	if readyLoopProbeForTest != nil {
		readyLoopProbeForTest.closureEntries.Add(int64(len(closure)))
	}
	return closure
}

// transitiveNeedsOf is transitiveNeeds over a plain task map, shared with
// Simulate so a plan hands a condition exactly the closure classify does. It
// is pure: it reads only its arguments.
func transitiveNeedsOf(tasks map[string]*orkstackv1.Task, id string) []string {
	collected := map[string]struct{}{}
	var walk func(string)
	walk = func(current string) {
		for _, need := range tasks[current].GetNeeds() {
			if _, done := collected[need]; done {
				continue
			}
			collected[need] = struct{}{}
			if _, ok := tasks[need]; ok {
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
// session it must be scheduled on does not exist yet. Such a
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

// pinCreationTimeoutFor is how long a pin of this run waits for a pin host to
// have a free session slot: the number the plan carries, which the submitting
// engine took from engine.pin_creation_timeout.
//
// It is a versioned change (PinCreationTimeoutChange), asked lazily, here,
// where the first pin of the run opens — so a run that opens no pin records no
// marker, and a history recorded before the gate replays its DefaultVersion
// branch: legacyPinCreationTimeout, whatever the plan says. From version 1 it
// is the plan's number, or legacyPinCreationTimeout when the plan carries none
// (a plan an older engine built, a start that predates the field), so a build
// that adopts the gate mid-run, as an unversioned fleet's later generation
// may, keeps the wait the run's earlier pins had. A carried generation
// answers from its carry (version), never asking again.
//
// A pure function of the plan and the history, so it replays identically.
func (s *runState) pinCreationTimeoutFor(ctx workflow.Context) time.Duration {
	if pinCreationTimeoutGateDisabledForTest.Load() ||
		s.version(ctx, PinCreationTimeoutChange, 1) < 1 {
		return legacyPinCreationTimeout
	}
	if seconds := s.plan.PinCreationTimeoutSeconds; seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return legacyPinCreationTimeout
}

// pinLifetime is a pin's ExecutionTimeout: the author's number, or the
// sequential worst case derived from its members (docs/DSL.md "Pins").
//
// The derived sum is every member's own wall clock plus its cancellation grace
// plus this run's transfer budget — the same three the member's own
// StartToCloseTimeout is built from in schedule() — plus pinReleaseBudget for
// the cleanup that has to fit inside the session too. It over-estimates a pin
// whose members run concurrently, and that costs nothing: the pin closes when
// its last member is terminal, never at this bound. An under-estimate
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
		return false, s.refuseRoute(runCtx, id, err)
	}

	p.opened = true
	p.triggeredBy = id
	p.openedAt = workflow.Now(runCtx)
	p.timeout = s.pinLifetime(p)
	p.creationTimeout = s.pinCreationTimeoutFor(runCtx)
	p.ready, p.readySettable = workflow.NewFuture(runCtx)
	p.closed, p.closedSettable = workflow.NewFuture(runCtx)
	p.done = workflow.NewBufferedChannel(runCtx, 1)
	s.chargePinOpen()

	// A coroutine, and it has to be one, for two reasons that each force it:
	// CompleteSession must run in the coroutine that ran CreateSession, so
	// something other than the main loop has to own the session's life; and
	// CreateSession blocks until a host answers, bounded only by
	// the pin's creation timeout, which in the main loop would stall every
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
// counter is assigned in one place; this coroutine's only
// channel back to the loop is the two futures and the one signal.
func (s *runState) hostPin(gctx workflow.Context, p *pinState, queue string) {
	base := workflow.WithActivityOptions(gctx, workflow.ActivityOptions{TaskQueue: queue})
	if limit := s.historyLimit(); limit != nil {
		s.noteCommand(gctx, 1, limit.Reserve.SessionBytes)
	}
	sessionCtx, err := workflow.CreateSession(base, &workflow.SessionOptions{
		ExecutionTimeout: p.timeout,
		CreationTimeout:  p.creationTimeout,
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
		// host that owns the directory.
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
// the engine never re-creates a session for a pin — so the
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
	message := s.pinUnavailableMessage(p, s.poolForTask(s.tasks[id]), err)
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Pin = name
	// scheduled_at and started_at both stay UNSET, as they do for every task
	// that never reached an Orker: this one was never dispatched, and the
	// absence of scheduled_at is the only thing telling a client so.
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

// pinGated reports whether this run took version 1 of the pin-creation-timeout
// change: the version is decided where its first pin opens (openPin), so by
// the time a pin's failure is reported it is in the run's own decisions, and a
// history from before the gate — or the test switch that writes one — has none
// above DefaultVersion.
func (s *runState) pinGated() bool {
	if pinCreationTimeoutGateDisabledForTest.Load() {
		return false
	}
	v, ok := s.versions[PinCreationTimeoutChange]
	return ok && v >= 1
}

// pinUnavailableMessage is the explanation a pin's PIN_UNAVAILABLE failure
// carries: what ended the wait for a free pin slot.
//
// Before the gate every failure said "within <the wait>", which is true only
// when nothing polled the creation request for the whole wait. A pool whose
// every pin host is cordoned ends after the Orker's own bound on refused
// attempts (about five minutes whatever the wait is), so from version 1 the
// message states the cause: the cordon, with the refusal's own text (its
// reason and how many attempts were refused); "within <the wait>" only for the
// creation request timing out unstarted; and no claimed wait for any other
// cause. A DefaultVersion run keeps the legacy wording byte for byte.
func (s *runState) pinUnavailableMessage(p *pinState, pool string, err error) string {
	if !s.pinGated() {
		return fmt.Sprintf(
			"pin %q could not be opened on pool %q within %s: no Orker of the pool had a free "+
				"pin slot, so this task never reached one: %v",
			p.name, pool, p.creationTimeout, err)
	}
	var refusal *temporal.ApplicationError
	if errors.As(err, &refusal) && refusal.Type() == task.ReasonOrkerCordoned {
		return fmt.Sprintf(
			"pin %q could not be opened on pool %q: every pin host of the pool refused it because it "+
				"is cordoned, so this task never reached one: %v",
			p.name, pool, err)
	}
	var timeout *temporal.TimeoutError
	if errors.As(err, &timeout) && timeout.TimeoutType() == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		return fmt.Sprintf(
			"pin %q could not be opened on pool %q within %s: no Orker of the pool had a free "+
				"pin slot, so this task never reached one: %v",
			p.name, pool, p.creationTimeout, err)
	}
	return fmt.Sprintf(
		"pin %q could not be opened on pool %q: no pin host of the pool opened it, so this task "+
			"never reached one: %v",
		p.name, pool, err)
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
	// Every member is terminal: the pin's closures are no longer read.
	for _, member := range p.tasks {
		delete(s.pinMemberClosures, member)
	}
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
// SDK's 3s completion activity.
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
// A pin never moves (AGENTS.md §5). When the host dies its creation activity
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
// openedAt was recorded from it.
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
// A PIN NEVER MOVES (AGENTS.md §5): every member of one pin executes on the
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
//
// The join is tested first, then the condition. all_succeeded (UNSPECIFIED)
// is today's loop verbatim; every other policy is joinAdmits, the function
// the plan shares.
func (s *runState) classify(ctx workflow.Context, id string) (bool, error) {
	if readyLoopProbeForTest != nil {
		readyLoopProbeForTest.classify.Add(1)
	}
	t := s.tasks[id]
	if t.GetJoin() == orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED {
		for _, need := range t.GetNeeds() {
			if s.taskRuns[need].GetStatus() == orkstackv1.Status_STATUS_SKIPPED {
				return false, s.markSkipped(ctx, id, skipReasonUpstream(need))
			}
		}
	} else if ok, reason := joinAdmits(t.GetJoin(), t.GetNeeds(), s.needOutcomeOf); !ok {
		return false, s.markSkipped(ctx, id, reason)
	}

	if readyLoopProbeForTest != nil {
		readyLoopProbeForTest.conditions.Add(1)
	}
	shouldRun, err := evaluateCondition(s.tasks[id].GetCondition(), s.inputs, s.upstreamOutputs(id))
	if err != nil {
		return false, s.failCondition(ctx, id, err)
	}
	if !shouldRun {
		return false, s.markSkipped(ctx, id, skipReasonCondition(s.tasks[id].GetCondition()))
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
	// Terminal, so this task's pin may now have none left. A pin whose
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
	closure := s.closureOf(id)
	refs := make([]*orkstackv1.UpstreamRef, 0, len(closure))
	for _, need := range closure {
		// A fan-out contributes ONE ref, whatever its count, and only to a
		// DIRECT dependent: its items are files nothing else may read (rule
		// 24), and the Orker stages each from its own commit.
		if e := s.eaches[need]; e != nil {
			if s.taskRuns[need].GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED && s.directlyNeeds(id, need) {
				refs = append(refs, &orkstackv1.UpstreamRef{
					TaskId: need,
					Each:   &orkstackv1.EachUpstream{Count: e.count, CommitRoot: s.plan.CommitRoot},
				})
			}
			continue
		}
		result, ok := s.results[need]
		if !ok {
			// A failure handler is handed a DIRECT need that FAILED with
			// output artifacts as a ref with task_id and artifacts only, at
			// the need's position in the closure. Nothing changes for any
			// other task.
			if isFailureHandler(s.tasks[id]) && s.directlyNeeds(id, need) {
				if ref := failedNeedRef(s.taskRuns[need]); ref != nil {
					refs = append(refs, ref)
				}
			}
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

// directlyNeeds reports whether id names need in its own `needs:`.
func (s *runState) directlyNeeds(id, need string) bool {
	for _, candidate := range s.tasks[id].GetNeeds() {
		if candidate == need {
			return true
		}
	}
	return false
}

// upstreamOutputs is the condition-evaluation view of the same closure: each
// completed ancestor's decoded values.json.
func (s *runState) upstreamOutputs(id string) map[string]map[string]any {
	closure := s.closureOf(id)
	out := make(map[string]map[string]any, len(closure))
	for _, need := range closure {
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
	// The dispatch instant, on the workflow's own clock. The TASK_SCHEDULED
	// event emitted below is stamped by emit() from workflow.Now in this same
	// workflow task, so the two are equal by construction: scheduled_at is a
	// PROJECTION of that event onto the snapshot. started_at is NOT set here —
	// it is the Orker's own start of the deciding attempt, and only an
	// Orker's report (collect, recordFailure) fills it.
	//
	// No GetVersion gate, for the reason retry_state needed none: the
	// snapshot is a local activity's input, which history does not persist,
	// so a replay takes this same path and fills a field of a message that
	// was never in history.
	taskRun.ScheduledAt = timestamppb.New(workflow.Now(ctx))

	// Two words for one decision: the queue is how the engine dispatches, the
	// pool is what the author wrote. Both come out of routeForTask, so
	// everything below this line that a client can read says pool, and that
	// pool names the Orkers the activity really goes to.
	queue, err := s.queueForTask(t)
	if err != nil {
		return nil, s.refuseRoute(ctx, id, err)
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
	// Nothing in the run's state changes between these two events, so on the
	// lean event path TASK_STARTED publishes with the snapshot TASK_SCHEDULED
	// just wrote rather than writing an identical one (lean_events.go).
	// The version is asked before TASK_SCHEDULED, so its marker precedes the
	// pair it decides.
	s.leanEvents(ctx)
	scheduledURI, err := s.emitWithSnapshot(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, id,
		orkstackv1.Status_STATUS_RUNNING, scheduled, "")
	if err != nil {
		return nil, err
	}
	reuse := s.startedSnapshotURI(ctx, scheduledURI)
	if _, err := s.emitWithSnapshot(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, id,
		orkstackv1.Status_STATUS_RUNNING, "task dispatched to an orker", reuse); err != nil {
		return nil, err
	}

	activityCtx := s.taskActivityContext(activityBase, t, queue)

	commitURI, err := s.commitURIFor(id)
	if err != nil {
		return nil, err
	}
	input := s.executeTaskInput(t, id, pool, pinContext)
	input.IdempotencyKey = task.IdempotencyKeyFor(s.wf.GetNamespace(), s.runID, id)
	input.CommitUri = commitURI
	// A failure handler is told how its needs ended. Set before the size
	// check, so the bounded errors count toward the wire bound; unset for
	// every other task, whose payload stays byte-identical.
	if isFailureHandler(t) {
		input.Join = joinContextFor(t, func(need string) *orkstackv1.TaskRun { return s.taskRuns[need] })
	}
	// The wire bound is checked HERE, deterministically, before scheduling.
	// The deployment's limit is replay-pinned on the plan; the checker also
	// preserves Temporal's absolute 256 KiB ceiling.
	if err := task.CheckInputSize(input, s.plan.Limits.Proto().GetValuesMaxBytes()); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(
			err.Error(), task.CategoryContractViolation, err)
	}
	// The history guard counts the command this workflow task issues, and
	// charges what the dispatch adds to the run's failure path.
	if limit := s.historyLimit(); limit != nil {
		s.noteCommand(ctx, 1, s.valuesBound()+limit.Reserve.CommandBytes)
		s.chargeTaskDispatch()
	}
	return workflow.ExecuteActivity(activityCtx, task.ActivityExecuteTask, input), nil
}

// taskActivityContext is the ActivityOptions one task's ExecuteTask runs
// with — every item of a fan-out uses its node's, so an item has the task's own
// timeout, grace, heartbeat and retry budget.
func (s *runState) taskActivityContext(activityBase workflow.Context, t *orkstackv1.Task, queue string) workflow.Context {
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

	return workflow.WithActivityOptions(activityBase, workflow.ActivityOptions{
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
}

// executeTaskInput is the ExecuteTaskInput every task and every item shares:
// everything but the idempotency key and commit URI, which identify ONE
// logical activity and are set by the caller (schedule, scheduleItem).
func (s *runState) executeTaskInput(
	t *orkstackv1.Task, id, pool string, pinContext *orkstackv1.PinContext,
) *orkstackv1.ExecuteTaskInput {
	namespace := s.wf.GetNamespace()
	return &orkstackv1.ExecuteTaskInput{
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
		// Decided at SUBMIT from the ImagePolicy document and the namespace,
		// carried on the plan like Limits and Telemetry. Nil-safe: a plan
		// with no policy produces a nil field here, unchanged from before
		// this field existed.
		ImagePolicy: s.plan.ImagePolicy.Proto(),

		// Set ONLY for a pinned task. An unpinned task's payload is therefore
		// byte-identical to what it was before this field existed — protojson
		// omits an unset message field — which is what lets the golden
		// history fixture keep matching without being re-recorded.
		Pin: pinContext,

		// Set ONLY for a cached task of a run whose plan carries a cache plan
		// — every item of a cached fan-out included — so every other task's
		// payload is byte-identical to what it was before this field existed.
		Cache: s.cacheContextFor(t),
	}
}

// cacheContextFor is t's CacheContext, or nil — for a task with no cache,
// for a pinned task (docs/DSL.md rule 36; hand-built only), and for a plan
// with no Cache (a harness plan, or one a pre-caching engine built): caching
// needs the submit-time cap, and a run without one caches nothing. Pure.
func (s *runState) cacheContextFor(t *orkstackv1.Task) *orkstackv1.CacheContext {
	spec := t.GetCache()
	if spec == nil || t.GetPin() != "" || s.plan.Cache == nil || dropCacheContextForTest {
		return nil
	}
	scope := spec.GetScope()
	if scope == orkstackv1.CacheScope_CACHE_SCOPE_UNSPECIFIED {
		scope = orkstackv1.CacheScope_CACHE_SCOPE_ORKFLOW
	}
	return &orkstackv1.CacheContext{
		Scope:      scope,
		TtlSeconds: EffectiveCacheTTL(spec.GetTtlSeconds(), s.plan.Cache.MaxTTLSeconds),
		Refresh:    s.plan.Cache.Refresh,
	}
}

// dropCacheContextForTest is a TEST-ONLY switch, the precedent of
// composeStartWithoutRender: with it set, cacheContextFor returns nil, which
// is how TestCachedHistory_PayloadCheckCatchesADroppedCacheContext proves the
// cached fixture's payload check can see a dropped CacheContext. Nothing but
// that test sets it.
var dropCacheContextForTest = false

// cachedSuffix is what a success event appends for a reused result:
// " (cached from run <id>)", or " (cached from run <id>, item <n>)" when the
// producer was a fan-out item. Empty for every other result, so the message
// is unchanged byte for byte.
func cachedSuffix(c *orkstackv1.CacheResult) string {
	if c.GetOutcome() != orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT {
		return ""
	}
	source := c.GetSource()
	if source.Item != nil {
		return fmt.Sprintf(" (cached from run %s, item %d)", source.GetRunId(), source.GetItem())
	}
	return fmt.Sprintf(" (cached from run %s)", source.GetRunId())
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
	// A composed task's future is its child run's, not an activity's.
	if s.children[id] != nil {
		return s.collectChild(ctx, id, future)
	}
	// This task is terminal on every path out of here, so this is where its
	// pin learns whether it was the last one.
	defer s.closePinFor(ctx, id)
	taskRun := s.taskRuns[id]

	result := &orkstackv1.ExecuteTaskResult{}
	if err := future.Get(ctx, result); err != nil {
		message := err.Error()
		// BEFORE recordFailure, because the two answers differ: a member of a
		// pin whose session has failed did not fail on its own and did not get
		// cancelled in the sense a client means — the platform lost the host
		// it was pinned to (PIN_HOST_LOST, docs/DSL.md "Pins").
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
	// WHICH Orker produced this, for every task, pinned or not
	// (docs/UI_CLI_PARITY.md "Task pins"). The field
	// already travelled on the result; nothing copied it onto the snapshot.
	// For a pin it is the client-visible proof the pin held: same pin, same
	// orker_id across every member.
	taskRun.OrkerId = result.GetOrkerId()
	// When the Orker began the attempt that produced this result — for an
	// adopted commit the COMMITTING attempt's start, exactly as Attempt below
	// is the committing attempt. Nil-safe: a result without one leaves the
	// field unset, which is "not known", never zero.
	taskRun.StartedAt = result.GetStartedAt()
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
	// The memo decision of the attempt that produced this result; nil for
	// every uncached task, so its snapshot is byte-identical.
	taskRun.Cache = result.GetCache()
	s.results[id] = result
	s.outputs[id] = values

	return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, id,
		orkstackv1.Status_STATUS_SUCCEEDED, "task succeeded"+cachedSuffix(result.GetCache()))
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
//
// keep, when non-nil, names the tasks left alone — in flight, PENDING or
// AWAITING — which is how the failing path spares failure handlers; reason is
// the sentence a never-started task is CANCELED with. Every task key it
// collects is deleted from inFlight, so what remains is what keep spared.
func (s *runState) cancelInFlight(
	ctx workflow.Context,
	inFlight map[string]workflow.Future,
	keep func(id string) bool,
	reason func(id string) string,
) {
	kept := func(id string) bool { return keep != nil && keep(id) }
	// s.order, never the map's own range: workflow code must stay
	// deterministic, and Go's map iteration order is randomized on purpose.
	for _, id := range s.order {
		if future, ok := inFlight[id]; ok && !kept(id) {
			delete(inFlight, id)
			_ = s.collect(ctx, id, future)
		}
		// A fan-out's in-flight items, in index order: each outcome is
		// recorded and its item event emitted, but nothing is dispatched and
		// the node is not decided here.
		if e := s.eaches[id]; e != nil {
			for index := int32(0); index < e.count; index++ {
				key := itemFlightKey(id, index)
				future, ok := inFlight[key]
				if !ok {
					continue
				}
				delete(inFlight, key)
				_ = s.collectItem(ctx, ctx, id, index, future, inFlight, true)
			}
		}
	}
	for _, id := range s.order {
		if s.eaches[id] != nil {
			s.finishFanOutOnRunEnd(ctx, id)
		}
		if kept(id) {
			continue
		}
		switch s.taskRuns[id].GetStatus() {
		case orkstackv1.Status_STATUS_PENDING:
			if err := s.markCanceled(ctx, id, reason(id)); err != nil {
				workflow.GetLogger(ctx).Error("mark canceled failed",
					"run_id", s.runID, "task_id", id, "error", err)
			}
		case orkstackv1.Status_STATUS_AWAITING:
			// Reached only by an awaited task, which no history recorded
			// before awaits existed contains.
			if err := s.cancelAwaiting(ctx, id); err != nil {
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
	// As in markSkipped: terminal, so the pin may be done with. This is
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
	s.recordFailureOn(ctx, s.taskRuns[id], fmt.Sprintf("task %q", id), cause)
}

// recordFailureOn is recordFailure for any row: a task's own, or one item of
// a fan-out (collectItem), whose details label names the item.
func (s *runState) recordFailureOn(ctx workflow.Context, taskRun *orkstackv1.TaskRun, label string, cause error) {
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))

	failure := &orkstackv1.Error{
		Code:    "TASK_FAILED",
		Message: cause.Error(),
		Details: label,
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
			// WHICH Orker produced this outcome, for every task, pinned or not —
			// read as soon as a detail decodes, before the reason-code
			// gate below, because a failed attempt is exactly when an operator
			// wants to know which host it happened on. Nil-safe: a detail
			// without one leaves the field empty, which is what it already
			// said.
			taskRun.OrkerId = detail.GetOrkerId()
			// When the Orker began the failed attempt, read on the same rule
			// and at the same place as orker_id. A detail without one (an
			// older Orker) leaves it unset.
			taskRun.StartedAt = detail.GetStartedAt()
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
// The category names ARE the type strings (docs/OCI_ACTIVITY_CONTRACT.md §8.1).
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
	// The history guard's failure belongs to the run, never to the task or
	// fan-out where it tripped: every caller reaches here with the id it was
	// working on.
	if isHistoryLimit(cause) {
		taskID = ""
	}
	// A definition with a failure handler takes its own failure path
	// (join.go, F1–F13); every other run takes this body, unchanged.
	if s.hasHandlers {
		return s.failWithHandlers(ctx, cancelRun, inFlight, taskID, cause)
	}
	s.finished = timestamppb.New(workflow.Now(ctx))
	s.runError = s.runFailureFor(taskID, cause)

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
		// An AWAITING cause is left for cancelInFlight, which records it
		// properly — with finished_at and its sentence — instead of leaving
		// it CANCELED with neither.
		if taskRun, ok := s.taskRuns[taskID]; ok &&
			taskRun.GetStatus() != orkstackv1.Status_STATUS_AWAITING {
			taskRun.Status = orkstackv1.Status_STATUS_CANCELED
		}
	}

	// Gated because it changes the COMMAND stream: a run that is genuinely
	// open and mid-failure when this build is deployed would otherwise be
	// replayed against brand-new RequestCancelActivity commands and awaits at
	// a point where the old code simply returned — a determinism break for
	// that in-flight run (see replay_test.go, testdata/history/README.md).
	if v := s.version(ctx, cancelSiblingsChange, 1); v >= 1 {
		cancelRun()
		// eventCtx, not ctx: when the operator canceled the whole run, ctx is
		// already done and eventCtx is the disconnected substitution chosen
		// above precisely so the remaining activity calls are not rejected as
		// canceled. cancelInFlight makes such calls too (collect emits), so it
		// needs the same context. In the ordinary task-failure case ctx was
		// never canceled and eventCtx == ctx.
		s.cancelInFlight(eventCtx, inFlight, nil, func(string) string { return runEndedBeforeStart })
	}

	// AFTER cancelInFlight and never before: it is what makes every pin
	// member terminal, which is what the terminal-task hook needs to signal
	// each pin's coroutine. Waiting first would block on a coroutine itself
	// waiting for a signal nothing had sent yet. eventCtx for the same reason
	// cancelInFlight gets it — a cancelled run's remaining activity calls run
	// on the disconnected substitution.
	s.awaitPinsClosed(eventCtx)
	// A resolution that reached history after its await closed is recorded,
	// never dropped. No-op for a run with no await.
	s.drainResolutions(eventCtx)

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

// runEndedBeforeStart is what fail-fast records on a never-started task of a
// run without failure handlers, and on a never-started ordinary task of a
// canceled run.
const runEndedBeforeStart = "run ended before this task started"

// runFailureFor is the run's error for a failure at taskID: RUN_FAILED, the
// cause's text and `task "<id>"`, with the fan-out, composed and lost-pin
// substitutions. Shared by fail and failWithHandlers, so the two cannot
// drift.
func (s *runState) runFailureFor(taskID string, cause error) *orkstackv1.Error {
	// The history guard's own reason: the run's, with what it still had
	// going as the details.
	if isHistoryLimit(cause) {
		return historyLimitFailure(cause)
	}
	failure := &orkstackv1.Error{
		Code:    "RUN_FAILED",
		Message: cause.Error(),
	}
	if taskID != "" {
		failure.Details = fmt.Sprintf("task %q", taskID)
	}
	// A fan-out's own failure carries the node's sentence as the
	// ApplicationError's message, whichever way the array was counted or the
	// item failed; Run.error.message is that sentence, without the error
	// type's decoration. Nothing else changes: the
	// branch is reached only for a fan-out, which no earlier history holds.
	var nodeErr *temporal.ApplicationError
	// A composed task's failure carries the sentence naming its child the
	// same way; reached only for a composed task, which no earlier history
	// holds.
	if (s.eaches[taskID] != nil || s.children[taskID] != nil) && errors.As(cause, &nodeErr) {
		failure.Message = nodeErr.Message()
	}

	// A pin that lost its host is the one failure that ARRIVES as a
	// cancellation without anything having cancelled the run: the SDK cancels
	// the session context when the session fails, so every member's future
	// returns a cancellation. The run failed — nobody ended it — and
	// the run's reason is the pin's, not the word "canceled".
	if lost := s.pinLossOf(taskID); lost != nil {
		failure.Code = "RUN_FAILED"
		failure.Message = lost.GetMessage()
	}
	return failure
}

// succeed terminates the run successfully, aggregating leaf task outputs.
func (s *runState) succeed(ctx workflow.Context) (*orkstackv1.Run, error) {
	// Every pin closed before the run reports success, so the terminal
	// snapshot describes a run that is genuinely finished — nothing of it is
	// still holding an Orker's session slot or a directory on its disk.
	// Ordinarily a no-op: each pin was already closed by its own last member.
	s.awaitPinsClosed(ctx)
	// Reached only with no await open (the loop's exit condition); a
	// resolution still queued is recorded as ignored. No-op with no await.
	s.drainResolutions(ctx)

	// Declared outputs, rendered once, replace the leaf aggregation; a run
	// whose outputs cannot be produced FAILS (docs/DSL.md "Composition").
	// An Orkflow with no `outputs:` takes the path it always took.
	var output *orkstackv1.Payload
	if len(s.wf.GetOutputs()) > 0 {
		rendered, failure := s.renderOutputs(ctx)
		if failure != nil {
			return s.failOutputs(ctx, failure)
		}
		output = rendered
	} else {
		aggregated := map[string]any{}
		for _, id := range s.order {
			if _, leaf := s.leaves[id]; !leaf {
				continue
			}
			if values, ok := s.outputs[id]; ok {
				aggregated[id] = values
			}
		}
		var err error
		output, err = payloadOf(aggregated)
		if err != nil {
			return nil, err
		}
	}

	s.output = output
	s.status = orkstackv1.Status_STATUS_SUCCEEDED
	s.finished = timestamppb.New(workflow.Now(ctx))
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, "",
		orkstackv1.Status_STATUS_SUCCEEDED, "run succeeded"); err != nil {
		return nil, err
	}
	// The workflow's own result is O(1) by construction: coordinates, status
	// and timestamps, never a task row (compactRunResult), or for a child run
	// only what its parent reads (compactChildResult). Nothing reads a
	// top-level result — the reader rebuilds a run from its events and
	// snapshots — so its size can never be what refuses the completion after
	// RUN_SUCCEEDED was already published.
	return s.result(), nil
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
		// Same origin every snapshot of this run carries (the plan is
		// replay-pinned), so the run record — and every webhook payload built
		// from it — says what started it. Nil-safe, same as Telemetry above.
		Origin: s.plan.Origin.Proto(),
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
	_, err := s.emitWithSnapshot(ctx, eventType, taskID, status, message, "")
	return err
}

// emitWithSnapshot is emit, returning the snapshot URI the event was published
// with. With reuseURI empty it writes the event's own snapshot, as emit always
// has. With reuseURI set it writes none and publishes with that URI: the lean
// event path's TASK_STARTED, whose snapshot is byte-identical to the
// TASK_SCHEDULED one written just before it (startedSnapshotURI).
//
// A terminal run event first waits for every item publish still outstanding
// (drainItemPublishes), so nothing the run published is still in flight when
// it ends. On the old path there are never any, so the command stream is
// unchanged.
func (s *runState) emitWithSnapshot(
	ctx workflow.Context,
	eventType orkstackv1.EventType,
	taskID string,
	status orkstackv1.Status,
	message string,
	reuseURI string,
) (string, error) {
	if readyLoopProbeForTest != nil && readyLoopProbeForTest.emit != nil {
		readyLoopProbeForTest.emit(true)
		defer readyLoopProbeForTest.emit(false)
	}
	if isTerminalRunEventType(eventType) && len(s.pendingItemPublishes) > 0 {
		s.drainItemPublishes(ctx)
	}
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

	snapshotURI := reuseURI
	if snapshotURI == "" {
		// One local activity writes it (writeEventSnapshot): a full
		// snapshot, or an overlay for a large run's non-terminal event.
		uri, err := s.writeEventSnapshot(ctx, eventType)
		snapshotURI = uri
		if err != nil {
			// Object storage is part of the durable record. Continuing would
			// let a run succeed without a readable terminal snapshot or
			// complete archive, so unlike the best-effort projection below
			// this failure ends the run.
			return "", fmt.Errorf("write snapshot for event %d: %w", s.eventSeq, err)
		}
	}

	activityCtx := workflow.WithActivityOptions(ctx, eventActivityOptions())
	s.recordEvent(event)
	future := workflow.ExecuteActivity(activityCtx, ActivityPublishEvent, event, snapshotURI, dedupeKey)
	if err := future.Get(ctx, nil); err != nil {
		// Projection is observability, not orchestration: a failed publish is
		// logged and the run continues.
		workflow.GetLogger(ctx).Error("publish event failed",
			"run_id", s.runID, "event", eventType.String(), "error", err)
	}
	return snapshotURI, nil
}

// eventActivityOptions are the projection activity's options, shared by every
// publish: the engine's own events channel, one attempt's budget, three
// attempts.
func eventActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		TaskQueue:           EventQueue,
		StartToCloseTimeout: eventActivityTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
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
		reason := ReasonInvalidPool
		if errors.Is(err, task.ErrReservedQueue) {
			reason = ReasonReservedQueue
		}
		return task.Route{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("task %q: %v", t.GetId(), err), reason, err)
	}
	return task.Route{Pool: pool, Queue: queue}, nil
}

// RefusedRouteChange is the workflow.GetVersion change id of refuseRoute's
// record of a task whose pool its run's plan cannot route (INVALID_POOL,
// RESERVED_QUEUE).
//
// Before it, the refusal returned the error and touched nothing: the row was
// left PENDING (a pin's opening) or RUNNING (a dispatch), and the run's
// failure path recorded it — fail() cancelled a PENDING row with a TASK_LOG,
// a failure handler's run recorded a RUNNING one through
// recordRefusedDispatch. Recording the row FAILED here, with TASK_FAILED, is a
// different command sequence (an event is a command), so it is asked LAZILY,
// only where the refusal first happens: a run that never refuses a route
// records no marker, and a history recorded before the gate replays its
// DefaultVersion branch, which is the old one. A carried generation answers
// from its carry (version).
const RefusedRouteChange = "refused-route-task-failed"

// refusedRouteGateDisabledForTest is a TEST-ONLY switch, the precedent of
// historyGuardLookaheadDisabledForTest: when set, refuseRoute takes the
// DefaultVersion branch WITHOUT asking its version, so a recorder can write a
// history in the shape of the builds before the gate, and a negative replay
// test proves the committed fixtures can only replay because of it. Never set
// in production code.
var refusedRouteGateDisabledForTest atomic.Bool

// refuseRoute handles a task its run's plan cannot route, and returns err for
// the run to fail with.
//
// From RefusedRouteChange 1 it records the task FAILED, with routeForTask's
// code (INVALID_POOL or RESERVED_QUEUE) as the task's own machine-readable
// reason, a contract violation, finished_at from the workflow's clock and no
// scheduled_at (nothing was dispatched), and emits TASK_FAILED, as
// failExpansion does for a fan-out, so the stream and the snapshot agree. The
// row is terminal, so neither fail() (cancelInFlight) nor failWithHandlers
// (recordRefusedDispatch) nor settleHandlerDispatch records it a second time.
// Before RefusedRouteChange it returned err alone and left the row as it was.
func (s *runState) refuseRoute(ctx workflow.Context, id string, err error) error {
	if refusedRouteGateDisabledForTest.Load() || s.version(ctx, RefusedRouteChange, 1) < 1 {
		return err
	}
	code, message := ReasonInvalidPool, err.Error()
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		code, message = applicationErr.Type(), applicationErr.Message()
	}
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	taskRun.ScheduledAt = nil
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Error = &orkstackv1.Error{
		Code:     code,
		Message:  message,
		Details:  fmt.Sprintf("task %q", id),
		Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
	}
	ectx := s.eventContext(ctx)
	if emitErr := s.emit(ectx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
		orkstackv1.Status_STATUS_FAILED, message); emitErr != nil {
		workflow.GetLogger(ctx).Error("emit refused route failed",
			"run_id", s.runID, "task_id", id, "error", emitErr)
	}
	s.closePinFor(ectx, id)
	return err
}

// The failure codes routeForTask reports. Their values are part of the
// client-visible contract: a run that cannot route a pool says which rule it
// broke, and docs/DSL.md documents both.
const (
	ReasonInvalidPool   = "INVALID_POOL"
	ReasonReservedQueue = "RESERVED_QUEUE"
)

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

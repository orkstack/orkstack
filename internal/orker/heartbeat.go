package orker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// HeartbeatIntervalVar is the operator setting for the tick below. It is
// spelled here, beside the bounds it must satisfy, for the same reason
// OrkerIDVar and BuildIDVar are spelled in this package: the validator that
// refuses a bad value has to be able to name the variable that carried it.
const HeartbeatIntervalVar = "ORK_ORKER_HEARTBEAT_INTERVAL"

// HeartbeatTimeout is the value the ENGINE must set in ActivityOptions, and
// the number this package's tick ceiling is derived from.
//
// It is exported and lives here rather than in internal/engine because the two
// halves must agree or cancellation silently stops working: Temporal delivers
// cancellation to an activity ONLY through a heartbeat response, so an
// activity that never calls RecordHeartbeat never observes ctx.Done(). The
// Orker therefore runs one heartbeat loop for the ENTIRE attempt — every
// phase, the container run included — rather than beating between phases,
// which could not reach a container that is running, which is exactly when
// somebody presses cancel (§6.3).
const HeartbeatTimeout = task.HeartbeatTimeout

// The Orker's tick rate: its default, and the bounds an operator may move it
// between with ORK_ORKER_HEARTBEAT_INTERVAL (docs/DEPLOY.md §1).
//
// THE ORKER SENDS ITS OWN HEARTBEATS (Heartbeater.report), through the client
// activity.GetClient returns, and not through activity.RecordHeartbeat. The SDK's
// route batches beats into windows of min(0.8 × HeartbeatTimeout,
// MaxHeartbeatThrottleInterval), and it cancels the activity locally when one
// RPC outlasts its window with a status it counts as retryable, assuming the
// server is about to time the activity out (internal_task_handlers.go in
// go.temporal.io/sdk). The server has decided nothing at that point, a later
// real cancel can no longer be told from it, and it returns no error to look at.
//
// So the tick is how often a beat leaves and how soon a running container hears
// of a cancel, and nothing else: a frontend that stays silent costs a running
// container nothing until the heartbeat timeout (Heartbeater.report).
//
// The worker's MaxHeartbeatThrottleInterval is deliberately left to the SDK.
// Only a pin's session heartbeats through the SDK's route now, and that window
// is its RPC's deadline: 24 s of a 30 s timeout, not one tick (cmd/orker).
const (
	// DefaultHeartbeatInterval is the tick an unconfigured Orker uses: fast
	// enough that a failed run stops its siblings in about five seconds.
	DefaultHeartbeatInterval = 5 * time.Second
	// MinHeartbeatInterval is as often as a heartbeat is worth sending: a cancel
	// reaching a container sooner than a second later buys nothing.
	MinHeartbeatInterval = time.Second
	// MaxHeartbeatInterval keeps two missed ticks inside the dead-worker
	// window: tick more slowly than a third of HeartbeatTimeout and Temporal
	// declares live activities dead.
	MaxHeartbeatInterval = task.MaxHeartbeatInterval
)

// ValidateHeartbeatInterval answers whether an operator's tick is one this
// Orker can honor. Zero is fine and means DefaultHeartbeatInterval; anything
// else must sit inside the two bounds above, and the error says WHY rather
// than only that a bound exists — a clamped-in-silence value would make the
// boot log disagree with the running process.
func ValidateHeartbeatInterval(d time.Duration) error {
	switch {
	case d == 0:
		return nil
	case d < MinHeartbeatInterval:
		return fmt.Errorf(
			"%s=%s is below %s: a cancel gains nothing from a faster tick, and every Orker's every task sends one",
			HeartbeatIntervalVar, d, MinHeartbeatInterval)
	case d > MaxHeartbeatInterval:
		return fmt.Errorf(
			"%s=%s is above %s (a third of the %s heartbeat timeout): a slower tick leaves no margin for two missed heartbeats and Temporal would declare live activities dead",
			HeartbeatIntervalVar, d, MaxHeartbeatInterval, HeartbeatTimeout)
	default:
		return nil
	}
}

// DefaultCancellationGrace is the SIGTERM→SIGKILL window when a task does not
// set its own (AGENTS.md §3.3).
const DefaultCancellationGrace = 30 * time.Second

// HeartbeatProgress is the small detail the loop carries. Its shape is owned
// by internal/task, because the engine's InspectRun reads it back from the
// cluster: two processes, one declaration (task.HeartbeatProgress).
type HeartbeatProgress = task.HeartbeatProgress

// Heartbeater runs the heartbeat loop for the entire lifetime of one activity
// attempt, so cancellation can reach a running container at any moment rather
// than only between phases.
type Heartbeater struct {
	phase       atomic.Pointer[string]
	containerID atomic.Pointer[string]
	bytesLogged atomic.Int64
	ordinal     atomic.Int32

	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once

	// send makes one heartbeat RPC and returns what came back: nil, or what the
	// server decided, or why it could not be asked. It is a field so tests can
	// play the cluster without a Temporal environment.
	send func(context.Context, HeartbeatProgress) error
	// deliver hands a decision of the server's to the SDK. The SDK alone can
	// cancel the activity's context with the cause that makes a cancel a cancel
	// on the wire, and it does so from the answer to one more heartbeat.
	deliver func(context.Context, HeartbeatProgress)
	// onServerDecision, when set, hears every decision of the server's before
	// deliver hands it to the SDK (oci_dispatcher.go: once the worker has
	// stopped, the SDK's cancel no longer reaches the attempt).
	onServerDecision func(error)
	// onLost stops the attempt: nothing acknowledged a heartbeat for ackTimeout.
	onLost func(error)
	// ackTimeout is the attempt's heartbeat timeout, past which the cluster has
	// timed it out, or will as soon as it can. Zero means HeartbeatTimeout.
	ackTimeout time.Duration
	// now is the clock the ack timeout is measured on. Nil means time.Now.
	now func() time.Time
	// logger says when heartbeats stop reaching the cluster and when they
	// resume. Nil means no log.
	logger *slog.Logger
	// lastAck and failing belong to the loop's goroutine alone.
	lastAck time.Time
	failing bool
	// serverCanceled is set before deliver hands the server's cancel on. Execute
	// reads it: if a new stall ate that one heartbeat, the SDK cancels with the
	// stall as its cause, and the task was still cancelled.
	serverCanceled atomic.Bool
	// completeCanceled reports the attempt cancelled without the SDK, which will
	// not report a cancel once its own context carries any other cause.
	completeCanceled func(context.Context) error
	// cancelRPC ends an RPC in flight when the loop is stopped.
	cancelRPC context.CancelFunc
	// answered is closed by the first heartbeat the cluster acknowledges, and
	// never again: AwaitFirstAnswer waits on it. Start makes it.
	answered     chan struct{}
	answeredOnce sync.Once
	// interval is the tick rate. Zero means DefaultHeartbeatInterval. It is
	// filled from OCIDispatcher.HeartbeatInterval.
	interval time.Duration
	// onPhase optionally receives every phase transition, in the same order
	// Phase is called. Execute sets it to an AttemptObserver's Phase method
	// when one is configured, so a phase span or timing hangs off the exact
	// sequence the dispatcher walks — never a second, drifting copy of it.
	// It is set once, before Start, so it needs no synchronization of its
	// own: Phase is only ever called from the dispatcher's own goroutine.
	onPhase func(string)
	// orkerID is stamped on every beat so an engine reading the pending
	// detail can name the sender without a host name. Set from
	// OCIDispatcher.OrkerID before Start; never changes for the attempt.
	orkerID string

	// mu guards marks. Phase is called from the dispatcher's own sequence and,
	// on the wait path, from the step that observes the timeout or the
	// cancellation; progress() and PhaseSpans() read it from the record path.
	// An atomic pointer was enough for one name; a list needs a lock.
	mu    sync.Mutex
	marks []phaseMark
}

// phaseMark is one phase transition: the phase entered and when, on this
// Orker's clock.
type phaseMark struct {
	name string
	at   time.Time
}

// NewHeartbeater builds the loop for one activity attempt. Outside an activity
// (orkctl task run) there is nobody to tell and it sends nothing.
func NewHeartbeater(ctx context.Context) *Heartbeater {
	h := &Heartbeater{}
	if !activity.IsActivity(ctx) {
		return h
	}
	info := activity.GetInfo(ctx)
	h.ackTimeout = info.HeartbeatTimeout
	beats := activity.GetClient(ctx)
	h.send = func(call context.Context, progress HeartbeatProgress) error {
		return beats.RecordActivityHeartbeatWithOptions(call, client.RecordActivityHeartbeatOptions{
			TaskToken:    info.TaskToken,
			Details:      []any{progress},
			Namespace:    info.Namespace,
			WorkflowID:   info.WorkflowExecution.ID,
			ActivityType: info.ActivityType.Name,
			WorkflowType: info.WorkflowType.Name,
			TaskQueue:    info.TaskQueue,
		})
	}
	h.deliver = func(call context.Context, progress HeartbeatProgress) {
		activity.RecordHeartbeat(call, progress)
	}
	h.completeCanceled = func(call context.Context) error {
		return beats.CompleteActivity(call, info.TaskToken, nil, temporal.NewCanceledError())
	}
	return h
}

// Start runs the loop until Stop. It ticks on a timer AND reports immediately,
// so a cancellation delivered before the first tick is still observed
// promptly.
func (h *Heartbeater) Start(ctx context.Context) {
	h.stop = make(chan struct{})
	h.stopped = make(chan struct{})
	h.answered = make(chan struct{})
	interval := h.interval
	if interval <= 0 {
		interval = DefaultHeartbeatInterval
	}
	// An RPC survives nothing but the loop: it ends when the attempt does, and
	// never carries the attempt's own deadline or cause into the SDK.
	calls, cancelRPC := context.WithCancel(context.WithoutCancel(ctx))
	h.cancelRPC = cancelRPC
	stopWatching := context.AfterFunc(ctx, cancelRPC)
	h.lastAck = h.clock()
	go func() {
		defer close(h.stopped)
		defer cancelRPC()
		defer stopWatching()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		h.report(calls)
		for {
			select {
			case <-h.stop:
				return
			case <-ctx.Done():
				// There is nothing left to report to a stopped attempt, and
				// continuing to beat would be reporting progress on work that
				// is being torn down.
				return
			case <-ticker.C:
				h.report(calls)
			}
		}
	}()
}

// report sends one heartbeat and acts on what came back, and it is the only place
// that does:
//
//   - an answer resets the ack timeout;
//   - a decision of the server's (a cancel, a pause, a reset, an activity or
//     namespace it no longer knows) goes to the SDK, which ends the attempt
//     with that cause;
//   - anything else says only that this heartbeat was not answered. The attempt
//     and its container are left alone until nothing has been acknowledged for
//     the heartbeat timeout. Then the cluster has timed the attempt out, or will
//     the moment it can, and a retry may start elsewhere, so the attempt stops:
//     that is what bounds how long the two run side by side.
func (h *Heartbeater) report(calls context.Context) {
	if h.send == nil {
		return
	}
	progress := h.progress()
	call, cancel := context.WithTimeout(calls, h.rpcDeadline())
	defer cancel()
	err := h.send(call, progress)
	switch {
	case err == nil:
		if h.failing && h.logger != nil {
			h.logger.Info("heartbeats reach the cluster again", "unanswered_for", h.clock().Sub(h.lastAck))
		}
		h.failing = false
		h.lastAck = h.clock()
		if h.answered != nil {
			h.answeredOnce.Do(func() { close(h.answered) })
		}
		return
	case serverDecided(err):
		h.serverCanceled.Store(temporal.IsCanceledError(err))
		if h.onServerDecision != nil {
			h.onServerDecision(err)
		}
		if h.deliver != nil {
			h.deliver(calls, progress)
		}
		return
	case calls.Err() != nil:
		// The attempt ended while this heartbeat was on its way, which says
		// nothing about the cluster.
		return
	}
	if !h.failing && h.logger != nil {
		h.logger.Warn("a heartbeat did not reach the cluster; the attempt carries on until the heartbeat timeout",
			"error", err, "heartbeat_timeout", h.ackWindow())
	}
	h.failing = true
	if h.clock().Sub(h.lastAck) >= h.ackWindow() && h.onLost != nil {
		h.onLost(&heartbeatUnanswered{after: h.ackWindow(), err: err})
	}
}

// AwaitFirstAnswer is the latch an attempt passes before it claims its ordinal,
// joins a pin or fetches anything (docs/OCI_ACTIVITY_CONTRACT.md §7.1): it
// returns nil once the cluster has acknowledged one heartbeat of this attempt.
//
// It is not one RPC. The loop that Start began keeps ticking, each report with
// its own deadline, and the latch ends the way that loop ends an attempt:
//
//   - a decision of the server's (NotFound for a run that was terminated, a
//     cancel) reaches the SDK through deliver, which cancels the attempt's
//     context, so the latch returns at once with that context's cause;
//   - no answer for the heartbeat timeout, counted from Start, stops the
//     attempt (onLost), so the latch returns heartbeatUnanswered.
//
// Either way the attempt has claimed nothing, joined no pin, fetched nothing
// and created no container. Outside an activity (orkctl task run, orkctl
// activity test) nothing is sent, and the latch is open.
func (h *Heartbeater) AwaitFirstAnswer(ctx context.Context) error {
	if h.send == nil || h.answered == nil {
		return nil
	}
	select {
	case <-h.answered:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// heartbeatUnanswered is why the loop stops an attempt.
type heartbeatUnanswered struct {
	after time.Duration
	err   error
}

func (e *heartbeatUnanswered) Error() string {
	return fmt.Sprintf("nothing answered for %s, last: %v", e.after, e.err)
}

func (e *heartbeatUnanswered) Unwrap() error { return e.err }

// rpcDeadline gives one heartbeat a third of the timeout, or what is left of it,
// so the stop is not late by a whole RPC. An RPC that has less than this left
// could not be answered anyway.
func (h *Heartbeater) rpcDeadline() time.Duration {
	deadline := h.ackWindow() / 3
	if left := h.ackWindow() - h.clock().Sub(h.lastAck); left < deadline {
		deadline = max(left, min(deadline, MinHeartbeatInterval))
	}
	return deadline
}

// reportCanceled tells the cluster this attempt was cancelled, on a context of
// its own and bounded by the timeout. If it cannot, the cluster's own timeout
// ends the attempt, and the run is being cancelled either way.
func (h *Heartbeater) reportCanceled(ctx context.Context) {
	if h.completeCanceled == nil {
		return
	}
	call, cancel := context.WithTimeout(context.WithoutCancel(ctx), h.ackWindow())
	defer cancel()
	if err := h.completeCanceled(call); err != nil && h.logger != nil {
		h.logger.Warn("could not report a cancelled attempt to the cluster", "error", err)
	}
}

func (h *Heartbeater) ackWindow() time.Duration {
	if h.ackTimeout > 0 {
		return h.ackTimeout
	}
	return HeartbeatTimeout
}

func (h *Heartbeater) clock() time.Time {
	if h.now != nil {
		return h.now()
	}
	return time.Now()
}

// Stop ends the loop and waits for it, however long the cluster takes to answer:
// a heartbeat in flight is abandoned. It is idempotent, so a deferred Stop beside
// an explicit one is safe.
func (h *Heartbeater) Stop() {
	h.stopOnce.Do(func() {
		if h.stop != nil {
			close(h.stop)
		}
		if h.cancelRPC != nil {
			h.cancelRPC()
		}
	})
	if h.stopped != nil {
		<-h.stopped
	}
}

// Phase records what the attempt is doing now. It is called from the
// dispatcher's sequence, so a heartbeat always names the current step, and it
// appends the transition to the phase log the attempt record's spans are
// rendered from (PhaseSpans).
func (h *Heartbeater) Phase(phase string) {
	h.mu.Lock()
	h.marks = append(h.marks, phaseMark{name: phase, at: time.Now().UTC()})
	h.mu.Unlock()
	h.phase.Store(&phase)
	if h.onPhase != nil {
		h.onPhase(phase)
	}
}

// Container records the container id once it exists.
func (h *Heartbeater) Container(id ContainerID) {
	value := string(id)
	h.containerID.Store(&value)
}

// setOrdinal records the attempt's claimed ordinal, which every beat after it
// carries.
func (h *Heartbeater) setOrdinal(n int32) { h.ordinal.Store(n) }

// Logged adds to the byte counter the progress detail carries.
func (h *Heartbeater) Logged(n int64) { h.bytesLogged.Add(n) }

func (h *Heartbeater) progress() HeartbeatProgress {
	progress := HeartbeatProgress{BytesLogged: h.bytesLogged.Load(), OrkerID: h.orkerID, Ordinal: h.ordinal.Load()}
	if phase := h.phase.Load(); phase != nil {
		progress.Phase = *phase
	}
	if id := h.containerID.Load(); id != nil {
		progress.ContainerID = *id
	}
	return progress
}

// TerminateContainer performs the §6.3 shutdown sequence on a running
// container:
//
//	Runtime.Signal(SIGTERM)
//	  -> wait grace
//	  -> Runtime.Signal(SIGKILL) if still running
//
// It is used by BOTH the cancellation path and the task-timeout path, because
// they are the same sequence with different reasons — and writing it once is
// what keeps them from drifting.
//
// The caller owns Remove: it must run on a DETACHED context, because cleanup
// has to survive the cancellation that caused it, and the detachment belongs
// where the context is owned rather than buried in a helper.
//
// Returns true when the container exited within the grace period.
func TerminateContainer(
	ctx context.Context,
	runtime ContainerRuntime,
	id ContainerID,
	grace time.Duration,
	exited <-chan struct{},
) bool {
	if grace <= 0 {
		grace = DefaultCancellationGrace
	}
	// The signal itself must not travel on the cancelled context: the whole
	// point is to act after cancellation.
	signalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), controlTimeout)
	_ = runtime.Signal(signalCtx, id, SIGTERM)
	cancel()

	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-exited:
		return true
	case <-timer.C:
	}

	// Ordinary Unix signal handling is meant to be enough for cleanup, but a
	// workload that ignores SIGTERM must not be able to outlive its grace
	// period — verified in §14.5.
	killCtx, killCancel := context.WithTimeout(context.WithoutCancel(ctx), controlTimeout)
	_ = runtime.Signal(killCtx, id, SIGKILL)
	killCancel()
	return false
}

// PhaseSpans closes the phase log at `end` and renders it as PhaseSpan
// records: each mark starts a span that the next mark ends, and the last
// mark's span ends at `end`. It never returns a span with finished_at before
// started_at: a mark recorded after `end` (a Phase call racing the record on
// the cancel path) is dropped rather than rendered backwards, and a wall clock
// that stepped back between two marks is held at the earlier mark. Nil when no
// Phase was ever called.
//
// It is the attempt record's own copy of the §7.1 sequence — not a by-product
// of an AttemptObserver — so a record carries its phases whether or not
// telemetry is wired.
func (h *Heartbeater) PhaseSpans(end time.Time) []*orkstackv1.PhaseSpan {
	h.mu.Lock()
	marks := append([]phaseMark(nil), h.marks...)
	h.mu.Unlock()

	kept := marks[:0]
	for _, mark := range marks {
		if mark.at.After(end) {
			continue
		}
		if n := len(kept); n > 0 && mark.at.Before(kept[n-1].at) {
			mark.at = kept[n-1].at
		}
		kept = append(kept, mark)
	}
	if len(kept) == 0 {
		return nil
	}
	spans := make([]*orkstackv1.PhaseSpan, len(kept))
	for i, mark := range kept {
		finished := end
		if i+1 < len(kept) {
			finished = kept[i+1].at
		}
		spans[i] = &orkstackv1.PhaseSpan{
			Phase:      mark.name,
			StartedAt:  timestamppb.New(mark.at),
			FinishedAt: timestamppb.New(finished),
		}
	}
	return spans
}

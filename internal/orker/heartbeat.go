package orker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/activity"

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
// THE PAIRING RULE. The task worker's worker.Options.MaxHeartbeatThrottleInterval
// MUST be set to the same value as the tick this package runs at. The Go SDK
// batches RecordHeartbeat calls into windows of
// min(0.8 × HeartbeatTimeout, MaxHeartbeatThrottleInterval) — inside a window a
// heartbeat only overwrites the pending detail and no RPC leaves the process —
// and cancellation arrives only in a heartbeat RPC's RESPONSE. So a fast tick
// with the SDK's default throttle is not a fast cancellation: it is a 24 s one
// (0.8 × 30 s), which is what a measured run showed. Lowering only the throttle
// is equally useless, since there would be no beat to carry. cmd/orker sets
// both from one value; a test harness that runs this dispatcher against a real
// Temporal must do the same or it measures the SDK's default.
//
// The tick is also THE LONGEST TEMPORAL FRONTEND OUTAGE A RUNNING CONTAINER
// SURVIVES: the SDK uses the same window as the heartbeat RPC's deadline and
// cancels the activity locally when the RPC still fails a retryable status
// after it, on the assumption the server is about to time the activity out.
// That is the tradeoff which makes this an operator setting rather than a
// constant — the right value depends on how a deployment runs Temporal.
const (
	// DefaultHeartbeatInterval is the tick an unconfigured Orker uses: fast
	// enough that a failed run stops its siblings in about five seconds,
	// slow enough to ride out a brief frontend blip.
	DefaultHeartbeatInterval = 5 * time.Second
	// MinHeartbeatInterval is the SDK's own floor on an RPC deadline
	// (minRPCTimeout). Below it the SDK clamps the deadline anyway, so a
	// smaller number would only be a lie in the boot log.
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
			"%s=%s is below %s: Temporal's SDK clamps a heartbeat RPC deadline at that floor, so a smaller tick cannot be honored",
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

// HeartbeatProgress is the small detail the loop carries.
//
// It is deliberately small: heartbeat details travel through Temporal's data
// converter into history, and bytes never travel through Temporal payloads
// (AGENTS.md §2.2). Phase and a container id are diagnosis; the logs are where
// the content lives.
type HeartbeatProgress struct {
	Phase       string `json:"phase"`
	ContainerID string `json:"container_id,omitempty"`
	BytesLogged int64  `json:"bytes_logged,omitempty"`
}

// Heartbeater runs the heartbeat loop for the entire lifetime of one activity
// attempt, so cancellation can reach a running container at any moment rather
// than only between phases.
type Heartbeater struct {
	phase       atomic.Pointer[string]
	containerID atomic.Pointer[string]
	bytesLogged atomic.Int64

	stop     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once

	// beat is the heartbeat sink. It is a field so tests can observe the
	// sequence without a Temporal environment.
	beat func(HeartbeatProgress)
	// interval is the tick rate. Zero means DefaultHeartbeatInterval. It is
	// filled from OCIDispatcher.HeartbeatInterval, which cmd/orker sets from
	// the same value it caps the worker's MaxHeartbeatThrottleInterval with —
	// see the pairing rule above.
	interval time.Duration
	// onPhase optionally receives every phase transition, in the same order
	// Phase is called. Execute sets it to an AttemptObserver's Phase method
	// when one is configured, so a phase span or timing hangs off the exact
	// sequence the dispatcher walks — never a second, drifting copy of it.
	// It is set once, before Start, so it needs no synchronization of its
	// own: Phase is only ever called from the dispatcher's own goroutine.
	onPhase func(string)
}

// NewHeartbeater builds a loop that reports into the activity context.
func NewHeartbeater(ctx context.Context) *Heartbeater {
	return &Heartbeater{
		beat: func(progress HeartbeatProgress) {
			if activity.IsActivity(ctx) {
				activity.RecordHeartbeat(ctx, progress)
			}
		},
	}
}

// Start runs the loop until Stop. It ticks on a timer AND reports immediately,
// so a cancellation delivered before the first tick is still observed
// promptly.
func (h *Heartbeater) Start(ctx context.Context) {
	h.stop = make(chan struct{})
	h.stopped = make(chan struct{})
	interval := h.interval
	if interval <= 0 {
		interval = DefaultHeartbeatInterval
	}
	go func() {
		defer close(h.stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		h.beat(h.progress())
		for {
			select {
			case <-h.stop:
				return
			case <-ctx.Done():
				// The context being done is exactly what a heartbeat
				// response delivered. There is nothing left to report to a
				// cancelled activity, and continuing to beat would be
				// reporting progress on work that is being torn down.
				return
			case <-ticker.C:
				h.beat(h.progress())
			}
		}
	}()
}

// Stop ends the loop and waits for it. It is idempotent, so a deferred Stop
// beside an explicit one is safe.
func (h *Heartbeater) Stop() {
	h.stopOnce.Do(func() {
		if h.stop != nil {
			close(h.stop)
		}
	})
	if h.stopped != nil {
		<-h.stopped
	}
}

// Phase records what the attempt is doing now. It is called from the
// dispatcher's sequence, so a heartbeat always names the current step.
func (h *Heartbeater) Phase(phase string) {
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

// Logged adds to the byte counter the progress detail carries.
func (h *Heartbeater) Logged(n int64) { h.bytesLogged.Add(n) }

func (h *Heartbeater) progress() HeartbeatProgress {
	progress := HeartbeatProgress{BytesLogged: h.bytesLogged.Load()}
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

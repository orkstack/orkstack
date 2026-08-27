package orker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/activity"
)

// HeartbeatTimeout is the value the ENGINE must set in ActivityOptions, and
// the number this package's tick rate is derived from.
//
// It is exported and lives here rather than in internal/engine because the two
// halves must agree or cancellation silently stops working: Temporal delivers
// cancellation to an activity ONLY through a heartbeat response, so an
// activity that never calls RecordHeartbeat never observes ctx.Done(). Today's
// Orker heartbeats four fixed phase strings and none during the container run,
// and no HeartbeatTimeout is set anywhere — which means cancellation cannot
// reach a running container at all. That is the largest correctness gap in the
// current system (§6.3).
const HeartbeatTimeout = 30 * time.Second

// HeartbeatInterval is how often the loop ticks: HeartbeatTimeout/3.
//
// A third is the standard margin — it tolerates two consecutive lost
// heartbeats before Temporal declares the worker dead, without a rate that
// would make the heartbeat itself the load.
const HeartbeatInterval = HeartbeatTimeout / 3

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
	// interval is the tick rate. Zero means HeartbeatInterval.
	interval time.Duration
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
		interval = HeartbeatInterval
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

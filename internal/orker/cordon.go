package orker

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// A cordon stops one Orker taking new work while leaving everything in flight
// alone (docs/DEPLOY.md "Removing an Orker safely: cordon and drain"). It has
// three effects on three different SDK seams, because the three kinds of
// intake are three different pollers:
//
//   - ordinary tasks on the pool channel: the task worker's activity slot
//     supplier (cordonSlotSupplier) refuses to reserve, so its pollers stop
//     polling;
//   - new pins: an activity interceptor refuses the SDK's session creation
//     activity with a RETRYABLE error, so the SDK re-queues the creation for
//     another host;
//   - members of an already-open pin: nothing — the session activity worker
//     has its own, plain slot supplier and the interceptor passes ExecuteTask
//     through.
//
// The flag lives in this process and nowhere else. A restarted Orker is ready.

// Cordon is one Orker's process-local cordon. It is the only place the flag
// exists: not on disk, not in the bucket, not in the engine.
type Cordon struct {
	mu       sync.Mutex
	cordoned bool
	reason   string
	since    time.Time
	// changed is closed and replaced on every Clear, so a blocked slot
	// reservation wakes without polling.
	changed chan struct{}
}

// NewCordon returns a clear cordon: the state every Orker boots into.
func NewCordon() *Cordon {
	return &Cordon{changed: make(chan struct{})}
}

// Set cordons the Orker. Setting an already-set cordon replaces the reason and
// since.
func (c *Cordon) Set(reason string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cordoned = true
	c.reason = reason
	c.since = now
}

// Clear uncordons the Orker and wakes every blocked reservation. Clearing a
// clear cordon is a no-op.
func (c *Cordon) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.cordoned {
		return
	}
	c.cordoned = false
	c.reason = ""
	c.since = time.Time{}
	close(c.changed)
	c.changed = make(chan struct{})
}

// State is (cordoned, reason, since) as one read under the lock.
func (c *Cordon) State() (bool, string, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cordoned, c.reason, c.since
}

// Wait blocks until the cordon is cleared or ctx is done, returning ctx.Err()
// in the second case. The SDK passes the worker's own context to ReserveSlot
// and treats context.Canceled as a quiet stop, so worker.Stop never hangs on a
// cordoned Orker.
func (c *Cordon) Wait(ctx context.Context) error {
	for {
		c.mu.Lock()
		cordoned, changed := c.cordoned, c.changed
		c.mu.Unlock()
		if !cordoned {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ------------------------------------------------------------- slot supplier

// DefaultMaxConcurrent is orker.max_concurrent's default: how many tasks one
// Orker runs at once (docs/DEPLOY.md §1, "Sizing an Orker's task ceiling").
//
// It is a sizing decision, not the Temporal SDK's 1000: a task that requests
// nothing gets orker.resources.default_cpu (1) and default_memory (1Gi), so 16
// is what a modest 16-CPU, 64 GiB host can actually run at once. The SDK's
// 1000 capped nothing a real host could run — an unbounded fan-out took every
// item onto whichever Orker polled first and failed them OOMKilled rather
// than leaving them queued for the pool. Larger hosts raise it explicitly.
const DefaultMaxConcurrent = 16

// MaxConcurrentVar is orker.max_concurrent's environment variable.
const MaxConcurrentVar = "ORK_ORKER_MAX_CONCURRENT"

// sdkDefaultSlots is the SDK's own default for every other supplier the tuner
// must name (go.temporal.io/sdk/internal/internal_worker.go, v1.47.0).
const sdkDefaultSlots = 1000

// cordonSlotSupplier wraps the ordinary fixed-size supplier and refuses to
// reserve while cordoned. Only the task worker's ACTIVITY supplier is wrapped;
// the session activity supplier is a plain fixed-size supplier, so an open
// pin's members are never blocked.
type cordonSlotSupplier struct {
	inner  worker.SlotSupplier
	cordon *Cordon
}

var _ worker.SlotSupplier = (*cordonSlotSupplier)(nil)

// ReserveSlot is called before every poll. A poller that cannot reserve does
// not poll, and a poller that does not poll is handed no task.
func (s *cordonSlotSupplier) ReserveSlot(ctx context.Context, info worker.SlotReservationInfo) (*worker.SlotPermit, error) {
	if err := s.cordon.Wait(ctx); err != nil {
		return nil, err
	}
	return s.inner.ReserveSlot(ctx, info)
}

// TryReserveSlot is the eager-dispatch path; a cordoned Orker takes nothing
// eagerly either.
func (s *cordonSlotSupplier) TryReserveSlot(info worker.SlotReservationInfo) *worker.SlotPermit {
	if cordoned, _, _ := s.cordon.State(); cordoned {
		return nil
	}
	return s.inner.TryReserveSlot(info)
}

func (s *cordonSlotSupplier) MarkSlotUsed(info worker.SlotMarkUsedInfo) { s.inner.MarkSlotUsed(info) }
func (s *cordonSlotSupplier) ReleaseSlot(info worker.SlotReleaseInfo)   { s.inner.ReleaseSlot(info) }
func (s *cordonSlotSupplier) MaxSlots() int                             { return s.inner.MaxSlots() }

// WorkerTuner is the task worker's Tuner: the SDK's default fixed sizes
// everywhere except the activity supplier, which is this Orker's configured
// ceiling (Options.MaxConcurrent — the same number DescribeOrker reports) and
// is wrapped by the cordon.
func (a *Activities) WorkerTuner() (worker.WorkerTuner, error) {
	fixed := func(n int) (worker.SlotSupplier, error) { return worker.NewFixedSizeSlotSupplier(n) }
	workflowSlots, err := fixed(sdkDefaultSlots)
	if err != nil {
		return nil, err
	}
	activitySlots, err := fixed(int(a.maxConc))
	if err != nil {
		return nil, err
	}
	localSlots, err := fixed(sdkDefaultSlots)
	if err != nil {
		return nil, err
	}
	nexusSlots, err := fixed(sdkDefaultSlots)
	if err != nil {
		return nil, err
	}
	sessionSlots, err := fixed(sdkDefaultSlots)
	if err != nil {
		return nil, err
	}
	return worker.NewCompositeTuner(worker.CompositeTunerOptions{
		WorkflowSlotSupplier:        workflowSlots,
		ActivitySlotSupplier:        &cordonSlotSupplier{inner: activitySlots, cordon: a.cordon},
		LocalActivitySlotSupplier:   localSlots,
		NexusSlotSupplier:           nexusSlots,
		SessionActivitySlotSupplier: sessionSlots,
	})
}

// ---------------------------------------------------------------- interceptor

// sessionCreationActivityName is the SDK's internal name for the activity that
// opens a session (go.temporal.io/sdk/internal/session.go:113, v1.47.0). It is
// unexported there, so it is spelled here and pinned by
// TestIntegration_ACordonedPinHostOpensNoNewPinAndKeepsItsOpenOne, which fails
// the day the SDK renames it — the same footing docs/DEPLOY.md gives the
// `__internal_session_creation` queue suffix.
const sessionCreationActivityName = "internalSessionCreationActivity"

// ReasonOrkerCordoned is the reason a cordoned host refuses to open a pin
// with. Retryable: the SDK re-queues the creation for another host.
const ReasonOrkerCordoned = task.ReasonOrkerCordoned

// ReasonOrkerCordonInvalid refuses a CordonOrker input that could not have
// passed the API boundary: a cordon without a reason, or a reason that is too
// long or carries a control character. ContractViolation: never retried.
const ReasonOrkerCordonInvalid = "ORKER_CORDON_INVALID"

// cordonPinRefusalAttempts bounds the ping-pong when EVERY host that polls the
// creation channel refuses: past this many attempts the refusal is
// non-retryable and the pin fails PIN_UNAVAILABLE. 45 attempts at the SDK's
// backoff (1s × 1.1ⁿ⁻¹, capped at 10s: ≈98 s over the first 25, then 20 × 10 s)
// is ≈ 5 minutes — the default of engine.pin_creation_timeout — so, at that
// default, an all-cordoned pool fails a pin in the same time a saturated one
// does. The bound is the Orker's own: a cordoned host takes each creation
// attempt at once and refuses it, so the engine's creation wait never runs
// out, and the pin fails after about five minutes whatever the deployment
// sets engine.pin_creation_timeout to (docs/DEPLOY.md, "Removing an Orker
// safely: cordon and drain").
// TestCordonRefusalBound_MatchesThePinCreationTimeout sums the backoff and
// pins the number.
const cordonPinRefusalAttempts = 45

// refusePinCreation decides one creation attempt: whether a host refuses it,
// and if so whether the SDK may retry it elsewhere.
func refusePinCreation(cordoned bool, attempt int32) (refuse, retryable bool) {
	if !cordoned {
		return false, false
	}
	return true, attempt <= cordonPinRefusalAttempts
}

// WorkerInterceptor is installed on the task worker. It sees every activity of
// the process by name — the pool channel's and the session's — so it both
// counts (running_tasks, open_pins) and refuses new pins while cordoned.
func (a *Activities) WorkerInterceptor() interceptor.WorkerInterceptor {
	return &cordonWorkerInterceptor{a: a}
}

type cordonWorkerInterceptor struct {
	interceptor.WorkerInterceptorBase
	a *Activities
}

func (w *cordonWorkerInterceptor) InterceptActivity(
	_ context.Context,
	next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	i := &cordonActivityInterceptor{a: w.a}
	i.Next = next
	return i
}

type cordonActivityInterceptor struct {
	interceptor.ActivityInboundInterceptorBase
	a *Activities
}

func (i *cordonActivityInterceptor) ExecuteActivity(
	ctx context.Context,
	in *interceptor.ExecuteActivityInput,
) (interface{}, error) {
	info := activity.GetInfo(ctx)
	switch info.ActivityType.Name {
	case task.ActivityExecuteTask:
		// The running_tasks measurement, taken at the SDK seam so a test
		// Orker's own ExecuteTask is counted exactly as the shipped one is.
		i.a.running.Add(1)
		defer i.a.running.Add(-1)
		return i.Next.ExecuteActivity(ctx, in)
	case sessionCreationActivityName:
	default:
		return i.Next.ExecuteActivity(ctx, in)
	}

	cordoned, reason, _ := i.a.cordon.State()
	if refuse, retryable := refusePinCreation(cordoned, info.Attempt); refuse {
		id := i.a.dispatcher.OrkerID
		i.a.dispatcher.logger().Info("pin creation refused: orker is cordoned",
			"run_id", info.WorkflowExecution.ID, "attempt", info.Attempt, "reason", reason)
		if retryable {
			return nil, temporal.NewApplicationError(fmt.Sprintf(
				"orker %s is cordoned (%s) and opens no new pin; the pin's creation is retried on another host",
				id, reason), ReasonOrkerCordoned)
		}
		return nil, temporal.NewNonRetryableApplicationError(fmt.Sprintf(
			"orker %s is cordoned (%s) and opens no new pin; %d creation attempts were refused, "+
				"so no Orker of this pool could open the pin",
			id, reason, info.Attempt-1), ReasonOrkerCordoned, nil)
	}

	// Accepted. The creation activity blocks for the whole life of the
	// session (it heartbeats until the session completes), so the entry is
	// held exactly as long as the pin is open.
	key := info.ActivityID + "\x00" + info.WorkflowExecution.RunID
	i.a.pins.add(key, info.WorkflowExecution.ID, time.Now())
	defer i.a.pins.remove(key)
	return i.Next.ExecuteActivity(ctx, in)
}

// ---------------------------------------------------------------- open pins

// openPins is the set of sessions this Orker has accepted and not completed,
// keyed by the creation activity (one entry per session, even when one run
// opens two pins here) and carrying the run that asked.
type openPins struct {
	mu      sync.Mutex
	entries map[string]openPin
}

type openPin struct {
	runID string
	since time.Time
}

func newOpenPins() *openPins { return &openPins{entries: map[string]openPin{}} }

func (p *openPins) add(key, runID string, since time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.entries[key] = openPin{runID: runID, since: since}
}

func (p *openPins) remove(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.entries, key)
}

// snapshot is every open pin, ordered by run id then since.
func (p *openPins) snapshot() []openPin {
	p.mu.Lock()
	out := make([]openPin, 0, len(p.entries))
	for _, entry := range p.entries {
		out = append(out, entry)
	}
	p.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].runID != out[j].runID {
			return out[i].runID < out[j].runID
		}
		return out[i].since.Before(out[j].since)
	})
	return out
}

// describePins renders the open pins, decorating each with its directory's
// lease when one exists. The count is the interceptor's; the directory only
// decorates it, so a session no member has reached yet is still counted.
//
// A run that opened two pins on this Orker has two sessions and two leases
// under one run id; they are paired in order (sessions by since, leases by
// expiry), which is exact for the count and best-effort for which name goes
// with which since.
func (a *Activities) describePins() []*orkstackv1.OpenPin {
	open := a.pins.snapshot()
	if len(open) == 0 {
		return nil
	}
	leases, err := ListOpenPinLeases(a.dispatcher.WorkRoot)
	if err != nil {
		a.dispatcher.logger().Warn("describe orker: list pin leases", "error", err)
	}
	byRun := map[string][]PinLeaseInfo{}
	for _, lease := range leases {
		byRun[lease.RunID] = append(byRun[lease.RunID], lease)
	}
	for runID := range byRun {
		sort.Slice(byRun[runID], func(i, j int) bool {
			return byRun[runID][i].ExpiresAt.Before(byRun[runID][j].ExpiresAt)
		})
	}
	pins := make([]*orkstackv1.OpenPin, 0, len(open))
	for _, entry := range open {
		pin := &orkstackv1.OpenPin{RunId: entry.runID, Since: timestamppb.New(entry.since)}
		if queue := byRun[entry.runID]; len(queue) > 0 {
			pin.Pin = queue[0].Pin
			pin.ExpiresAt = timestamppb.New(queue[0].ExpiresAt)
			byRun[entry.runID] = queue[1:]
		}
		pins = append(pins, pin)
	}
	return pins
}

// ---------------------------------------------------------------- activity

// validateCordonInput is the Orker's defence in depth: the API boundary
// refuses the same input first.
func validateCordonInput(in *orkstackv1.CordonOrkerInput) error {
	if !in.GetCordoned() {
		return nil
	}
	return task.ValidCordonReason(in.GetReason())
}

// CordonOrker sets or clears this Orker's cordon and answers with its state
// afterwards. Registered on the control channel only, so it can never land on
// a different process. Idempotent: setting an already-set cordon replaces the
// reason, clearing a clear one still answers.
func (a *Activities) CordonOrker(ctx context.Context, in *orkstackv1.CordonOrkerInput) (*orkstackv1.DescribeOrkerResult, error) {
	if err := validateCordonInput(in); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s: %v", ReasonOrkerCordonInvalid, err), ReasonOrkerCordonInvalid, nil)
	}
	logger := a.dispatcher.logger()
	if in.GetCordoned() {
		a.cordon.Set(in.GetReason(), time.Now())
		logger.Info("orker cordoned",
			"reason", in.GetReason(),
			"running_tasks", a.running.Load(),
			"open_pins", len(a.pins.snapshot()))
	} else {
		was, _, since := a.cordon.State()
		a.cordon.Clear()
		if was {
			logger.Info("orker uncordoned", "was_cordoned_for", time.Since(since).Round(time.Second))
		}
	}
	return a.DescribeOrker(ctx)
}

// Cordon is this Orker's process-local cordon, for cmd/orker and in-process
// test Orkers.
func (a *Activities) Cordon() *Cordon { return a.cordon }

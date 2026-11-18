package engine

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A paused run holds its pins too (design §20, §21, U5, P0-P3): nothing new
// starts, a pin's members and closure included, and an open pin's working
// timeout stops while the run that owns it is paused. The working timeout
// is a workflow timer (pinDeadline); the session, the Orker and its lease
// use the pin's outer bound (sessionBound), reached only by a pin whose
// clock was stopped longer than the pause maximum allows.

// pausablePinTimeoutVersion gates the path: asked lazily where a pin first
// opens under a plan that pins PinTimeoutPausable, because the path changes
// the session's ExecutionTimeout. A history recorded before it replays its
// DefaultVersion branch: one number, enforced by the session alone.
const pausablePinTimeoutVersion = "pausable-pin-timeout"

// DefaultPinOuterMarginSeconds is the outer bound's margin over the pin's
// timeout plus the pause maximum (PausePlan.PinOuterMarginSeconds).
const DefaultPinOuterMarginSeconds = 600

// DefaultPinTimerEvents is what one stop and re-arm of a pin's timer costs
// in history: a cancel and a start (PausePlan.PinTimerEvents).
const DefaultPinTimerEvents = 3

// pinDeadline is a pin's working timeout as a clock that stops and runs:
// remaining is what is left as of armedAt, and armedAt is zero while it is
// stopped. Its timer exists only while the pin is open and the clock runs.
type pinDeadline struct {
	remaining time.Duration
	armedAt   time.Time
	stopped   time.Duration // in all, for the message
	stoppedAt time.Time
	timer     workflow.Future
	cancel    workflow.CancelFunc
}

// newPinDeadline is a clock of timeout, running from openedAt.
func newPinDeadline(timeout time.Duration, openedAt time.Time) *pinDeadline {
	return &pinDeadline{remaining: timeout, armedAt: openedAt}
}

func (d *pinDeadline) running() bool { return !d.armedAt.IsZero() }

// dueAt is when a running clock reaches zero.
func (d *pinDeadline) dueAt() time.Time { return d.armedAt.Add(d.remaining) }

// stop stops a running clock at now; a stopped one is unchanged.
func (d *pinDeadline) stop(now time.Time) {
	if !d.running() {
		return
	}
	d.remaining -= now.Sub(d.armedAt)
	d.armedAt, d.stoppedAt = time.Time{}, now
	if d.cancel != nil {
		d.cancel()
	}
	d.timer, d.cancel = nil, nil
}

// run starts a stopped clock at now; a running one is unchanged.
func (d *pinDeadline) run(now time.Time) {
	if d.running() {
		return
	}
	d.stopped += now.Sub(d.stoppedAt)
	d.armedAt, d.stoppedAt = now, time.Time{}
}

// stoppedFor is how long the clock has been stopped in all, as of now.
func (d *pinDeadline) stoppedFor(now time.Time) time.Duration {
	if d.running() {
		return d.stopped
	}
	return d.stopped + now.Sub(d.stoppedAt)
}

// sessionBound is what the session, the Orker's lease and the session
// failure's classification use; p.timeout stays the working timeout.
func (s *runState) sessionBound(p *pinState) time.Duration {
	if !p.pausable {
		return p.timeout
	}
	pp := s.plan.Pause
	return p.timeout + time.Duration(pp.MaxTimeoutSeconds+pp.PinOuterMarginSeconds)*time.Second
}

// pinPausable asks the gate for a pin about to open.
func (s *runState) pinPausable(ctx workflow.Context) bool {
	if s.plan.Pause == nil || !s.plan.Pause.PinTimeoutPausable {
		return false
	}
	return s.version(ctx, pausablePinTimeoutVersion, 1) >= 1
}

// pausePinsHeld reports a pin of the new path is open or opening: a paused
// run then holds its pins' closures too (§20.2).
func (s *runState) pausePinsHeld() bool {
	for _, name := range s.pinOrder {
		if p := s.pins[name]; p != nil && p.opened && !p.doneSent && p.pausable {
			return true
		}
	}
	return false
}

// pauseHolds reports that dispatch point id waits for the pause: a paused
// run starts nothing new, except, before the gate, what an open pin needs to
// finish; a run carrying a pause request to its switch lets an open pin
// finish, so it can reach that switch.
func (s *runState) pauseHolds(id string) bool {
	if s.paused() && !(s.pinClosureAllows(id) && !s.pausePinsHeld()) {
		return true
	}
	return s.deferHold && !s.pinClosureAllows(id)
}

// stopPinClocks stops every pausable pin's clock: the run became paused. A
// deadline already reached before the stop is taken instead (§21.3).
func (s *runState) stopPinClocks(ctx workflow.Context) {
	now := workflow.Now(ctx)
	for _, name := range s.pinOrder {
		p := s.pins[name]
		if p == nil || p.deadline == nil || p.deadlineReached {
			continue
		}
		if p.deadline.running() && !now.Before(p.deadline.dueAt()) {
			continue // the pass top's deadline check takes it
		}
		if p.deadline.timer != nil {
			if limit := s.historyLimit(); limit != nil {
				s.noteCommand(ctx, 1, limit.Reserve.CommandBytes)
			}
		}
		p.deadline.stop(now)
	}
}

// runPinClocks runs every pausable pin's clock again and arms the timer of
// each open one: the run became unpaused.
func (s *runState) runPinClocks(ctx workflow.Context) {
	now := workflow.Now(ctx)
	for _, name := range s.pinOrder {
		p := s.pins[name]
		if p == nil || p.deadline == nil || p.deadlineReached {
			continue
		}
		p.deadline.run(now)
		if p.ctx != nil {
			s.armPinDeadline(ctx, p)
		}
	}
}

// armPinDeadline starts the timer of an open pin whose clock runs. With
// nothing left, no timer is created: the pass top takes the deadline.
func (s *runState) armPinDeadline(ctx workflow.Context, p *pinState) {
	d := p.deadline
	if d == nil || !d.running() || d.timer != nil {
		return
	}
	left := d.dueAt().Sub(workflow.Now(ctx))
	if left <= 0 {
		return
	}
	tctx, cancel := workflow.WithCancel(ctx)
	if limit := s.historyLimit(); limit != nil {
		s.noteCommand(ctx, 1, limit.Reserve.CommandBytes)
	}
	d.timer, d.cancel = workflow.NewTimer(tctx, left), cancel
}

// pinDeadlineDue is the first open pin whose running clock has reached
// zero, or "".
func (s *runState) pinDeadlineDue(ctx workflow.Context) string {
	now := workflow.Now(ctx)
	for _, name := range s.pinOrder {
		p := s.pins[name]
		if p == nil || p.deadline == nil || p.deadlineReached || p.ctx == nil || !p.deadline.running() {
			continue
		}
		if !now.Before(p.deadline.dueAt()) {
			return name
		}
	}
	return ""
}

// pinDeadlineReached ends a pin whose working timeout ran out: its running
// members are canceled and recorded PIN_TIMEOUT (pinLoss); with none
// running, the first unfinished member is, and the run fails (§21.2).
func (s *runState) pinDeadlineReached(ctx workflow.Context, name string, inFlight map[string]workflow.Future) (failed string, err error) {
	p := s.pins[name]
	p.lost, p.deadlineReached = true, true
	if p.cancelMembers != nil {
		p.cancelMembers()
	}
	for _, id := range p.tasks {
		if _, running := inFlight[id]; running {
			return "", nil // collect records PIN_TIMEOUT through pinLoss
		}
	}
	for _, id := range p.tasks {
		tr := s.taskRuns[id]
		if isTerminal(tr.GetStatus()) {
			continue
		}
		e := s.pinTimeoutError(ctx, p)
		tr.Status = orkstackv1.Status_STATUS_FAILED
		tr.FinishedAt = timestamppb.New(workflow.Now(ctx))
		tr.Pin = name
		tr.Error = e
		return id, fmt.Errorf("%s: %s", e.GetCode(), e.GetMessage())
	}
	return "", nil
}

// pinTimeoutError is a working-timeout PIN_TIMEOUT, naming how long its
// clock was stopped while paused.
func (s *runState) pinTimeoutError(ctx workflow.Context, p *pinState) *orkstackv1.Error {
	message := fmt.Sprintf("pin %q reached its %s lifetime, so the pin closed and every task "+
		"still running on that Orker was stopped with it", p.name, p.timeout)
	if p.deadline == nil {
		return &orkstackv1.Error{Code: "PIN_TIMEOUT", Message: message, Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT}
	}
	if stopped := p.deadline.stoppedFor(workflow.Now(ctx)); stopped > 0 {
		message += fmt.Sprintf(" (its clock was stopped for %s while paused)", stopped)
	}
	return &orkstackv1.Error{
		Code:     "PIN_TIMEOUT",
		Message:  message,
		Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT,
	}
}

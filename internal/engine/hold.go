package engine

import (
	"fmt"

	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Holds: the two reasons a run does not start a ready task, fan-out item,
// child run or pin — never running work, never a wall clock, never a spin.
//
//   - A quiesce holds everything except an open pin's needs-closure, so a
//     generation that has grown past its budget and found no natural idle
//     point reaches one near its hold line (continuation.go).
//   - The in-flight cap holds while something is in flight that will wake
//     the loop (len(inFlight) > 0), so a run never keeps more than its plan's
//     InFlight.Max pending, and never more than the hard stop at all. An open
//     pin's needs-closure is exempt below the hard stop.
//
// A held task stays PENDING with nothing emitted; a held fan-out item stays
// below its fan-out's next. Holding issues no command, so it costs no
// history: it lasts until what is running finishes, bounded by that work's
// own limits. Both are interpreter control flow inside RunWorkflow, never a
// scheduler (AGENTS.md §13).

// InFlightCapChange is the workflow.GetVersion change id of the in-flight
// cap. It is asked LAZILY, the first time the cap would hold: a run that
// never reaches its cap records no marker, and a history recorded before the
// cap replays with DefaultVersion and never holds.
const InFlightCapChange = "run-in-flight-cap"

// holdDispatch reports whether dispatch point id must wait. O(1):
// pinClosureAllows is a set lookup, built at most once per pass.
func (s *runState) holdDispatch(ctx workflow.Context, id string, inFlight map[string]workflow.Future) bool {
	if s.quiescing && !s.pinClosureAllows(id) {
		return true
	}
	lim := s.plan.InFlight
	if lim == nil || lim.Max <= 0 || len(inFlight) == 0 {
		// N1: a cap hold with nothing in flight could never be released.
		return false
	}
	atCap := s.inFlightWork(inFlight) >= int(lim.Max)
	hardStop := lim.HardStop > 0 && s.pendingActivities(inFlight) >= int(lim.HardStop)
	if !hardStop && (!atCap || s.pinClosureAllows(id)) {
		return false
	}
	if s.version(ctx, InFlightCapChange, 1) < 1 {
		return false
	}
	if !s.capLogged {
		s.capLogged = true
		s.logInfo(ctx, inFlight, "run reached its in-flight cap", "cap", lim.Max)
	}
	return true
}

// inFlightWork is what the cap counts: every activity, item and child run
// the loop waits on, and every pin from its open request to its close (its
// session-creation activity stays pending for the pin's whole life). A run
// with nothing in flight counts 0.
func (s *runState) inFlightWork(inFlight map[string]workflow.Future) int {
	return len(inFlight) + s.livePins()
}

// pendingActivities is what the cluster counts: inFlightWork, the detached
// item publishes, and the one blocking publish an emit may have
// outstanding. Used only for the hard stop.
func (s *runState) pendingActivities(inFlight map[string]workflow.Future) int {
	return s.inFlightWork(inFlight) + len(s.pendingItemPublishes) + 1
}

// livePins counts the pins opening, open, or closing: opened and not yet
// closed.
func (s *runState) livePins() int {
	live := 0
	for _, name := range s.pinOrder {
		if p := s.pins[name]; p.opened && (p.closed == nil || !p.closed.IsReady()) {
			live++
		}
	}
	return live
}

// closingPins counts the pins signalled done whose coroutine has not yet
// released and completed the session.
func (s *runState) closingPins() int {
	closing := 0
	for _, name := range s.pinOrder {
		if p := s.pins[name]; p.doneSent && p.closed != nil && !p.closed.IsReady() {
			closing++
		}
	}
	return closing
}

// openedUnclosedPins counts the pins opened and not yet signalled done.
func (s *runState) openedUnclosedPins() int {
	open := 0
	for _, name := range s.pinOrder {
		if p := s.pins[name]; p.opened && !p.doneSent {
			open++
		}
	}
	return open
}

// pinClosureAllows reports whether id is an unfinished member of an opened
// pin not yet signalled done, or lies in the transitive needs closure of
// such a member. A pin's members may need unpinned tasks, a fan-out (its
// items included) or a composed task between them, so a hold that did not
// let these through would deadlock the pin. The set is rebuilt at most once
// per pass, while any pin is opened and not done; empty otherwise.
func (s *runState) pinClosureAllows(id string) bool {
	if !s.pinClosureFresh {
		s.rebuildPinClosure()
	}
	return s.pinClosure[id]
}

// rebuildPinClosure builds pinClosureAllows' set: O(the size of the open
// pins' closures).
func (s *runState) rebuildPinClosure() {
	s.pinClosureFresh = true
	s.pinClosure = nil
	for _, name := range s.pinOrder {
		p := s.pins[name]
		if !p.opened || p.doneSent {
			continue
		}
		for _, member := range p.tasks {
			if isTerminal(s.taskRuns[member].GetStatus()) {
				continue
			}
			if s.pinClosure == nil {
				s.pinClosure = map[string]bool{}
			}
			s.pinClosure[member] = true
			for _, need := range s.closure[member] {
				s.pinClosure[need] = true
			}
		}
	}
}

// fanOutCanDispatch reports whether the fan-out id is expanded, RUNNING, and
// has an item due and a slot free: the state a hold leaves it in.
func (s *runState) fanOutCanDispatch(id string) bool {
	e := s.eaches[id]
	return e != nil && e.expanded && s.taskRuns[id].GetStatus() == orkstackv1.Status_STATUS_RUNNING &&
		e.next < e.count && e.inFlight < e.limit
}

// anyFanOutHeld is heldFanOuts' definition: some expanded RUNNING fan-out
// has an item due and a slot free.
func (s *runState) anyFanOutHeld() bool {
	for _, id := range s.order {
		if s.fanOutCanDispatch(id) {
			return true
		}
	}
	return false
}

// resumeHeldFanOuts dispatches every held fan-out whose next item no hold
// keeps back, then recomputes heldFanOuts. An error returns the fan-out id
// to fail.
func (s *runState) resumeHeldFanOuts(ctx, runCtx workflow.Context, inFlight map[string]workflow.Future) (string, error) {
	for _, id := range s.order {
		if !s.fanOutCanDispatch(id) || s.holdDispatch(ctx, id, inFlight) {
			continue
		}
		if err := s.dispatchItems(ctx, runCtx, id, inFlight); err != nil {
			return id, err
		}
	}
	s.heldFanOuts = s.anyFanOutHeld()
	return "", nil
}

// closureHasReadyWork reports whether an open pin still has work this run
// can start: a ready task, or a fan-out with an item due and a slot free,
// inside an open pin's needs-closure. A ready task outside every closure
// cannot make a member ready — a member's needs are, by definition, in its
// closure — so when this is false every ready task the pin needs was
// dispatched or skipped, and its remaining work is blocked on an open await
// in its closure. The set is rebuilt first: the pass may have changed it.
func (s *runState) closureHasReadyWork() bool {
	s.rebuildPinClosure()
	for _, id := range s.readyTasks() {
		if s.pinClosure[id] {
			return true
		}
	}
	for _, id := range s.order {
		if s.pinClosure[id] && s.fanOutCanDispatch(id) {
			return true
		}
	}
	return false
}

// pinBlockedReason names the open pin that keeps a quiescing run from going
// idle and the await its remaining work waits on, in run vocabulary.
func (s *runState) pinBlockedReason() string {
	for _, name := range s.pinOrder {
		p := s.pins[name]
		if !p.opened || p.doneSent {
			continue
		}
		for _, member := range p.tasks {
			if isTerminal(s.taskRuns[member].GetStatus()) {
				continue
			}
			candidates := append([]string{member}, s.closure[member]...)
			for _, id := range candidates {
				if a := s.awaits[id]; a != nil && a.open {
					return fmt.Sprintf("pin %s waits on await %s", name, id)
				}
			}
		}
		return fmt.Sprintf("pin %s is still open", name)
	}
	return "a pin is still open"
}

// abandonQuiesce is the one structural give-up: an open pin whose remaining
// work waits on an await cannot reach idle until a person resolves it. The
// hold is given up, and not tried again until the run has grown by its
// retry spacing; switchDue stays, so a natural idle point still switches at
// once (right after the pin closes, say). Held work dispatches on the next
// pass.
func (s *runState) abandonQuiesce(ctx workflow.Context, inFlight map[string]workflow.Future, reason string) {
	budget := s.plan.Continuation
	e, b := s.historyNow(ctx)
	s.quiescing = false
	s.quiesceRetryAt = [2]int64{e + budget.RetryEvents, b + budget.RetryBytes}
	s.gaveUp = reason
	workflow.GetLogger(ctx).Warn("run resumed new tasks: it cannot reach a moment with nothing running",
		s.logFields(ctx, inFlight, "reason", reason)...)
}

// admit remembers that classify said id runs (see the ready loop).
func (s *runState) admit(id string) {
	if s.admitted == nil {
		s.admitted = map[string]bool{}
	}
	s.admitted[id] = true
}

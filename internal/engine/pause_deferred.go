package engine

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A pause request the main loop cannot record within the run's history
// limit is carried into the run's next generation and applied there first
// (design §4.4 step 3, D-S2-1). Every request after a carried one is carried
// too, so the next generation applies them in the order they arrived. While
// any is carried the run starts nothing new, so it reaches its idle point
// and switches. A run that cannot switch ends RUN_HISTORY_LIMIT naming the
// request, exactly as a run whose plan pins no deferral always does.

// pauseDeferredVersion gates the carry of pause requests. It is asked
// lazily, where a request first crosses the reserve under a plan that pins
// MaxDeferredRequests, so a history recorded before it replays its
// DefaultVersion branch, RUN_HISTORY_LIMIT, and a run that never crosses
// records no marker. The next generation reads the decision from its carry.
const pauseDeferredVersion = "pause-deferred"

// DefaultMaxDeferredPauseRequests is how many pause requests one switch may
// carry: pinned per run (PausePlan.MaxDeferredRequests).
const DefaultMaxDeferredPauseRequests = 64

// maxDeferredPauseRequestsBound is the most a plan may pin: a request is a
// few hundred bytes, so this many stays well inside the carry's 256 KiB.
const maxDeferredPauseRequestsBound = 256

// proto is the request as the carry holds it.
func (r *pauseRequest) proto() *orkstackv1.RunPauseSignal {
	sig := &orkstackv1.RunPauseSignal{
		Format: r.Format, Pause: r.Pause, Principal: r.Principal, Reason: r.Reason,
		TimeoutSeconds: r.TimeoutSeconds, RequestId: r.RequestID, Inherited: r.Inherited,
		ParentRunId: r.ParentRunID, CascadeSeq: r.CascadeSeq, Depth: r.Depth,
	}
	if t, err := time.Parse(time.RFC3339Nano, r.OriginExpiresAt); err == nil {
		sig.OriginExpiresAt = timestamppb.New(t)
	}
	return sig
}

// pauseRequestOf is the request a carried signal holds.
func pauseRequestOf(sig *orkstackv1.RunPauseSignal) *pauseRequest {
	req := &pauseRequest{
		Format: sig.GetFormat(), Pause: sig.GetPause(), Principal: sig.GetPrincipal(), Reason: sig.GetReason(),
		TimeoutSeconds: sig.GetTimeoutSeconds(), RequestID: sig.GetRequestId(), Inherited: sig.GetInherited(),
		ParentRunID: sig.GetParentRunId(), CascadeSeq: sig.GetCascadeSeq(), Depth: sig.GetDepth(),
	}
	if t := sig.GetOriginExpiresAt(); t != nil {
		req.OriginExpiresAt = t.AsTime().UTC().Format(time.RFC3339Nano)
	}
	return req
}

// canDeferPause reports that a request over the reserve can be carried: the
// plan pins deferral, the run can still switch, and the gate is taken.
func (s *runState) canDeferPause(ctx workflow.Context) bool {
	budget := s.continuationBudget()
	switch {
	case s.plan.Pause.MaxDeferredRequests <= 0, budget == nil, s.historyLimit() == nil,
		s.noSwitch != "", s.generation+1 >= budget.MaxGenerations:
		return false
	}
	// The carry gate too: a run that switches without it carries no pause
	// state (fillPauseCarry), so its carried requests would be lost.
	return s.version(ctx, pauseDeferredVersion, 1) >= 1 && s.writerAdopted(ctx) &&
		s.version(ctx, pauseCarryVersion, 1) >= 1
}

// deferPause carries req into the next generation, or stops the run
// RUN_HISTORY_LIMIT when the plan's bound is reached.
func (s *runState) deferPause(ctx workflow.Context, inFlight map[string]workflow.Future, req *pauseRequest) error {
	if int64(len(s.deferredPause)) >= s.plan.Pause.MaxDeferredRequests {
		c, _ := s.pauseApplyCrossing(ctx, inFlight)
		return s.historyLimitErrorNoting(ctx, inFlight, c, 0, fmt.Sprintf(
			"pause request %s from %s (asserted) was not applied: the run already carries %d pause requests into its next history, as many as it may",
			req.RequestID, principalOr(req.Principal), len(s.deferredPause)))
	}
	s.deferredPause = append(s.deferredPause, req.proto())
	s.deferHold = true
	s.switchDue = true // switches at its next idle point (canSwitchNow)
	workflow.GetLogger(ctx).Info("pause request carried into the run's next history: recording it here would pass its limit",
		"run_id", s.runID, "request_id", req.RequestID, "pause", req.Pause, "carried", len(s.deferredPause))
	return nil
}

// deferredPauseStranded is the run's RUN_HISTORY_LIMIT once a carried
// request can never reach a next generation: the switch was given up.
func (s *runState) deferredPauseStranded(ctx workflow.Context, inFlight map[string]workflow.Future) error {
	if len(s.deferredPause) == 0 {
		return nil
	}
	first := pauseRequestOf(s.deferredPause[0])
	c, _ := s.pauseApplyCrossing(ctx, inFlight)
	return s.historyLimitErrorNoting(ctx, inFlight, c, 0, fmt.Sprintf(
		"pause request %s from %s (asserted) was not applied, because recording it would pass that limit and the run could not carry it on",
		first.RequestID, principalOr(first.Principal)))
}

// deferredPauseStrandedBy reports a carried request whose switch was given
// up for good.
func (s *runState) deferredPauseStrandedBy() bool {
	return len(s.deferredPause) > 0 && s.noSwitch != ""
}

// rearmDeferHold holds new work again once a failed switch's retry spacing
// has passed, so the run makes its way to the next attempt.
func (s *runState) rearmDeferHold(e, b int64) {
	if len(s.deferredPause) > 0 && !s.deferHold && reached(e, b, s.switchRetryAt) {
		s.deferHold = true
	}
}

// dropDeferredPause drops the carried requests of a run that will never
// reach a next generation, naming each in the log: a failing run, whose
// failure-handler loop never carries on into a new history, keeps its own
// cause (M7); a run with nothing left to do ends as it would have. No event
// is emitted for them: the history they did not fit is the one an event
// would grow.
func (s *runState) dropDeferredPause(ctx workflow.Context, why string) {
	for _, sig := range s.restoredPause {
		workflow.GetLogger(ctx).Error("carried pause request dropped before it was applied: "+why,
			"run_id", s.runID, "request_id", sig.GetRequestId(), "pause", sig.GetPause(), "inherited", sig.GetInherited())
	}
	for _, sig := range s.deferredPause {
		workflow.GetLogger(ctx).Error("pause request dropped: recording it would pass the run's history limit, and "+why,
			"run_id", s.runID, "request_id", sig.GetRequestId(), "pause", sig.GetPause(), "inherited", sig.GetInherited())
	}
	s.restoredPause, s.deferredPause, s.deferHold = nil, nil, false
}

// restoredPauseFault names why a carry's pause requests cannot be adopted,
// or "".
func (s *runState) restoredPauseFault(requests []*orkstackv1.RunPauseSignal) string {
	if len(requests) == 0 {
		return ""
	}
	if v, ok := s.versions[pauseDeferredVersion]; !ok || v < 1 || s.plan.Pause == nil {
		return "it carries pause requests this run cannot apply"
	}
	if limit := s.plan.Pause.MaxDeferredRequests; int64(len(requests)) > limit {
		return fmt.Sprintf("it carries %d pause requests, more than its plan's %d", len(requests), limit)
	}
	// Each passed these checks when it was carried, in order, so a carry
	// whose requests no longer do, against the adopted cascade counter as it
	// advances, was not written by this run.
	adopted := s.lastCascadeSeq
	defer func() { s.lastCascadeSeq = adopted }()
	for i, sig := range requests {
		if sig == nil || sig.GetFormat() != 1 {
			return fmt.Sprintf("its carried pause request %d is not a pause request of format 1", i)
		}
		req := pauseRequestOf(sig)
		if fault := s.pauseRequestFault(req); fault != "" {
			return fmt.Sprintf("its carried pause request %d cannot be applied: %s", i, fault)
		}
		if req.Inherited {
			s.lastCascadeSeq = req.CascadeSeq
		}
	}
	return ""
}

// idleAction is what an idle, unpaused main loop does with work still held.
type idleAction int

const (
	idleRescan   idleAction = iota // nothing holds: scan again
	idleWait                       // a cascade send holds the switch: wait for its result
	idleStranded                   // a carried request can never reach a switch
)

// idleUnpaused decides it. Only a run held for a switch can be idle with
// work ready: any other dispatches what a scan finds. A cascade send still
// live holds that switch (canSwitchNow), and every send's end wakes the
// selector, so such a run waits for it rather than scanning again in a
// loop. A run carrying a pause request
// waits for a closing pin, whose close the selector then holds; scans again
// while an open pin's closure has work; and is otherwise stranded, since
// nothing it waits for can end on its own.
func (s *runState) idleUnpaused() idleAction {
	switch {
	case !s.cascadeIdle() && (s.deferHold || s.quiescing):
		return idleWait
	case !s.deferHold:
		return idleRescan
	case s.closingPins() > 0:
		return idleWait
	case s.openedUnclosedPins() > 0 && s.closureHasReadyWork():
		return idleRescan
	default:
		return idleStranded
	}
}

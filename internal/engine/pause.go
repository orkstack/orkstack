package engine

import (
	"encoding/json/v2"
	"fmt"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/task"
)

// Pausing a run (docs/API.md, PauseRun): a paused run starts nothing new,
// lets what it started finish, and ends at its pause's timeout unless it is
// unpaused first. Temporal primitives only: an internal Signal channel, one
// Timer per pause, a reason in the dispatch hold and an idle wait.
//
// Every branch is entered only for a run whose plan carries Pause, and the
// first command of the path is gated by the "run-pause" version, asked when
// the first request is applied, so every recorded history replays unchanged.

// PauseSignal is the internal Signal every pause and unpause request
// arrives on. Engine-internal: no client names it.
const PauseSignal = "ork-pause"

// pauseVersion is the gate of the pause path's first command.
const pauseVersion = "run-pause"

// RunPauseTimeoutCode ends a run whose pause outlived its timeout.
const RunPauseTimeoutCode = "RUN_PAUSE_TIMEOUT"

const (
	statusPaused     = orkstackv1.Status_STATUS_PAUSED
	eventRunPaused   = orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED
	eventRunUnpaused = orkstackv1.EventType_EVENT_TYPE_RUN_UNPAUSED
)

// pauseCarryVersion gates the carry of a run's pause across a continuation:
// asked at the first switch a run with pause state could make. Its default
// branch holds a paused run's switch instead.
const pauseCarryVersion = "pause-carry"

// PausePlan says a run accepts pause requests. Set at submit on every run
// and every composed child's plan; every number decides commands, so all are
// pinned here (AGENTS.md §7.2).
type PausePlan struct {
	DefaultTimeoutSeconds   int64 `json:"default_timeout_seconds"`
	MaxTimeoutSeconds       int64 `json:"max_timeout_seconds"`
	ApplyReserveEvents      int64 `json:"apply_reserve_events"`
	ApplyReserveBytes       int64 `json:"apply_reserve_bytes"`
	CascadeEventsPerChild   int64 `json:"cascade_events_per_child"`
	CascadeBytesPerChild    int64 `json:"cascade_bytes_per_child"`
	InheritedGraceSeconds   int64 `json:"inherited_grace_seconds"`
	InheritedGraceMaxLevels int64 `json:"inherited_grace_max_levels"`
	// Indexed says the cluster accepted OrkflowRunPaused at submit.
	Indexed bool `json:"indexed"`
	// MaxDeferredRequests is how many pause requests over the reserve one
	// switch may carry into the next generation (pause_deferred.go); 0, a
	// plan from before the carry, stops RUN_HISTORY_LIMIT instead.
	MaxDeferredRequests int64 `json:"max_deferred_requests,omitzero"`
	// Cascades says the run tells its in-flight child runs of its pause and
	// unpause (pause_cascade.go); false, a plan from before the cascade,
	// pauses the run alone. CascadeRetries and CascadeRetryIntervalSeconds
	// bound a round's retries of one failed send.
	Cascades                    bool  `json:"cascades,omitzero"`
	CascadeRetries              int64 `json:"cascade_retries,omitzero"`
	CascadeRetryIntervalSeconds int64 `json:"cascade_retry_interval_seconds,omitzero"`
	// PinTimeoutPausable says a pin's working timeout stops while the run is
	// paused (pause_pin.go); PinOuterMarginSeconds is the outer bound's
	// margin, and PinTimerEvents what one stop and re-arm costs.
	PinTimeoutPausable    bool  `json:"pin_timeout_pausable,omitzero"`
	PinOuterMarginSeconds int64 `json:"pin_outer_margin_seconds,omitzero"`
	PinTimerEvents        int64 `json:"pin_timer_events,omitzero"`
}

// pauseRequest is one decoded RunPauseSignal. Interim: decoded from the
// signal's JSON with the proto's protojson names until the message is
// generated; int64 fields accept protojson's string form.
type pauseRequest struct {
	Format    int32  `json:"format"`
	Pause     bool   `json:"pause"`
	Principal string `json:"principal"`
	// PrincipalVerified: OrkStack authenticated Principal (auth.mode observe
	// or enforce); otherwise a gateway asserted it.
	PrincipalVerified bool   `json:"principalVerified"`
	Reason            string `json:"reason"`
	TimeoutSeconds    int64  `json:"timeoutSeconds,string"`
	RequestID         string `json:"requestId"`
	Inherited         bool   `json:"inherited"`
	ParentRunID       string `json:"parentRunId"`
	CascadeSeq        int64  `json:"cascadeSeq,string"`
	OriginExpiresAt   string `json:"originExpiresAt"`
	Depth             int64  `json:"depth,string"`
}

// ownPause is the run's own pause: who, when, why, until when, and its timer.
type ownPause struct {
	by, reason, requestID string
	verified              bool // by is a principal OrkStack authenticated
	at, expiresAt         time.Time
	timer                 workflow.Future
	cancel                workflow.CancelFunc
}

// inheritedPause is the pause the run's parent cascaded to it, with its
// backstop.
type inheritedPause struct {
	parentRunID string
	at          time.Time
	backstopAt  time.Time
	backstop    workflow.Future
	cancel      workflow.CancelFunc
}

// pauseEnd is how the run's last own pause ended.
type pauseEnd int

const (
	pauseEndUnpaused pauseEnd = iota + 1
	pauseEndTimedOut
	pauseEndRunEnded
)

// lastPause is the run's last own pause and how it ended.
type lastPause struct {
	pause ownPause
	end   pauseEnd
	endAt time.Time
	// endedBy is who unpaused it, and endedVerified whether OrkStack
	// authenticated them; empty for a pause that timed out or that its run's
	// end closed.
	endedBy       string
	endedVerified bool
}

// paused reports that either pause cause is present.
func (s *runState) paused() bool { return s.pause != nil || s.pausedByParent != nil }

// decodePauseRequest decodes a raw signal defensively: a payload that does
// not decode, has another format, is an API request with no request id, or
// is an inherited request out of order or from a run that is not this run's
// parent is dropped with a warning, no event and no command.
func (s *runState) decodePauseRequest(ctx workflow.Context, raw converter.RawValue) (*pauseRequest, bool) {
	drop := func(why string) (*pauseRequest, bool) {
		workflow.GetLogger(ctx).Warn("pause request dropped", "run_id", s.runID, "why", why)
		return nil, false
	}
	payload := raw.Payload()
	if payload == nil {
		return drop("no payload")
	}
	// Sealed for this run under its key, or it opens for nobody
	// (payload-encryption.md §4.1 rule 2).
	payload, err := payloadcrypt.OpenIfSealed(s.scope, payload)
	if err != nil {
		return drop("it does not open")
	}
	req, err := parsePauseRequest(payload.GetData())
	if err != nil {
		return drop("it does not decode: " + err.Error())
	}
	if fault := s.pauseRequestFault(req); fault != "" {
		// Decoded: name the request, so its caller's record can be found.
		workflow.GetLogger(ctx).Warn("pause request dropped", "run_id", s.runID, "why", fault,
			"request_id", req.RequestID, "principal", req.Principal, "pause", req.Pause, "inherited", req.Inherited)
		return nil, false
	}
	return req, true
}

// parsePauseRequest decodes a signal's JSON. Interim, until the message is
// generated and decoded as protojson.
func parsePauseRequest(data []byte) (*pauseRequest, error) {
	var req pauseRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

// pauseRequestFault names why a decoded request cannot be applied, or "".
// A plan whose numbers cannot bound a pause makes every request a fault, so
// the run is never paused on a bound it cannot keep.
func (s *runState) pauseRequestFault(req *pauseRequest) string {
	if fault := s.plan.Pause.fault(); fault != "" {
		return "the run's pause plan is invalid: " + fault
	}
	switch {
	case req.Format != 1:
		return fmt.Sprintf("format %d is not 1", req.Format)
	case !req.Inherited && req.RequestID == "":
		return "an API request carries no request id"
	case req.Inherited && req.ParentRunID != s.parentRunID():
		return "it names " + req.ParentRunID + ", which is not this run's parent"
	case req.Inherited && req.CascadeSeq <= s.lastCascadeSeq:
		return "its cascade sequence is not after the last one applied"
	case req.Inherited && req.Pause:
		if _, err := time.Parse(time.RFC3339Nano, req.OriginExpiresAt); err != nil {
			return fmt.Sprintf("its originating pause's expiry %q is not a timestamp", req.OriginExpiresAt)
		}
	}
	return ""
}

// fault names why the plan cannot bound a pause, or "".
func (p *PausePlan) fault() string {
	switch {
	case p.MaxTimeoutSeconds <= 0:
		return fmt.Sprintf("max_timeout_seconds %d is not positive", p.MaxTimeoutSeconds)
	case p.DefaultTimeoutSeconds <= 0 || p.DefaultTimeoutSeconds > p.MaxTimeoutSeconds:
		return fmt.Sprintf("default_timeout_seconds %d is not in 1..%d", p.DefaultTimeoutSeconds, p.MaxTimeoutSeconds)
	case p.InheritedGraceSeconds < 0:
		return fmt.Sprintf("inherited_grace_seconds %d is negative", p.InheritedGraceSeconds)
	case p.InheritedGraceMaxLevels <= 0:
		return fmt.Sprintf("inherited_grace_max_levels %d is not positive", p.InheritedGraceMaxLevels)
	case p.Cascades && (p.CascadeRetries < 0 || p.CascadeRetries > 10):
		return fmt.Sprintf("cascade_retries %d is not in 0..10", p.CascadeRetries)
	case p.Cascades && p.CascadeRetries > 0 && (p.CascadeRetryIntervalSeconds <= 0 || p.CascadeRetryIntervalSeconds > 3600):
		return fmt.Sprintf("cascade_retry_interval_seconds %d is not in 1..3600", p.CascadeRetryIntervalSeconds)
	case p.PinTimeoutPausable && (p.PinOuterMarginSeconds < 0 || p.PinTimerEvents < 0):
		return fmt.Sprintf("pin_outer_margin_seconds %d and pin_timer_events %d must not be negative",
			p.PinOuterMarginSeconds, p.PinTimerEvents)
	case p.MaxDeferredRequests < 0 || p.MaxDeferredRequests > maxDeferredPauseRequestsBound:
		return fmt.Sprintf("max_deferred_requests %d is not in 0..%d", p.MaxDeferredRequests, maxDeferredPauseRequestsBound)
	}
	return ""
}

// parentRunID is the run that started this one, or "".
func (s *runState) parentRunID() string {
	if o := s.plan.Origin; o != nil && o.Proto().GetKind() == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN {
		return o.ID
	}
	return ""
}

// pauseTimeout is an own pause's length: its requested timeout or the plan's
// default, never past the plan's maximum.
func (s *runState) pauseTimeout(req *pauseRequest) time.Duration {
	p := s.plan.Pause
	seconds := req.TimeoutSeconds
	if seconds <= 0 {
		seconds = p.DefaultTimeoutSeconds
	}
	return time.Duration(min(seconds, p.MaxTimeoutSeconds)) * time.Second
}

// applyPauseRequest applies one request (the table of docs/API.md,
// PauseRun): state and commands first, then the event. A failed event write
// never fails the run; it is logged and the next snapshot carries the state.
func (s *runState) applyPauseRequest(ctx workflow.Context, inFlight map[string]workflow.Future,
	raw converter.RawValue, loop pauseLoop,
) (applied bool, err error) {
	req, ok := s.decodePauseRequest(ctx, raw)
	if !ok {
		return false, nil
	}
	return s.applyDecodedPause(ctx, inFlight, req, loop)
}

// applyDecodedPause applies one request that passed its checks, carries it
// into the next generation (pause_deferred.go), or stops the run; applied
// reports a request that changed the run's state.
func (s *runState) applyDecodedPause(ctx workflow.Context, inFlight map[string]workflow.Future,
	req *pauseRequest, loop pauseLoop,
) (applied bool, err error) {
	if ctx.Err() != nil {
		workflow.GetLogger(ctx).Info("pause request after the run was canceled: no change",
			"run_id", s.runID, "request_id", req.RequestID, "pause", req.Pause)
		return false, nil
	}
	if loop == pauseInMainLoop && len(s.deferredPause) > 0 {
		// After a carried request, every request is carried, so the next
		// generation applies them in the order they arrived.
		return false, s.deferPause(ctx, inFlight, req)
	}
	if c, crossed := s.pauseApplyCrossing(ctx, inFlight); crossed && s.guardAdopted(ctx) {
		if loop == pauseInHandlerLoop {
			workflow.GetLogger(ctx).Warn("pause request dropped: applying it would take the run's history past its limit",
				"run_id", s.runID, "request_id", req.RequestID, "pause", req.Pause,
				"events", c.events, "reserve_events", c.reserveEvents, "max_events", c.maxEvents)
			return false, nil
		}
		if s.canDeferPause(ctx) {
			return false, s.deferPause(ctx, inFlight, req)
		}
		return false, s.historyLimitErrorNoting(ctx, inFlight, c, 0,
			fmt.Sprintf("pause request %s from %s was not applied, because recording it would pass that limit",
				req.RequestID, markedPrincipal(req.Principal, req.PrincipalVerified)))
	}
	s.applyPause(ctx, inFlight, req)
	s.syncPauseIndex(ctx)
	return true, nil
}

// pauseLoop is the loop a pause request is read in. Over the reserve, the
// main loop stops RUN_HISTORY_LIMIT; the handler loop, which never carries
// on into a new history, drops the request and keeps the run's own cause
// (design §4.4, M7).
type pauseLoop int

const (
	pauseInMainLoop pauseLoop = iota
	pauseInHandlerLoop
)

// applyPause applies one decoded request that fits under the reserve.
func (s *runState) applyPause(ctx workflow.Context, inFlight map[string]workflow.Future, req *pauseRequest) {
	if req.Inherited {
		s.lastCascadeSeq = req.CascadeSeq
	}
	s.version(ctx, pauseVersion, 1)
	now := workflow.Now(ctx)
	wasPaused := s.paused()
	switch {
	case req.Pause && !req.Inherited:
		if s.pause != nil {
			workflow.GetLogger(ctx).Info("pause request for a run already paused: no change",
				"run_id", s.runID, "request_id", req.RequestID)
			return
		}
		limit := s.pauseTimeout(req)
		p := &ownPause{by: req.Principal, verified: req.PrincipalVerified, reason: req.Reason, requestID: req.RequestID,
			at: now, expiresAt: now.Add(limit)}
		p.timer, p.cancel = s.pauseTimer(ctx, limit)
		s.pause = p
		message := fmt.Sprintf("run paused by %s", markedPrincipal(req.Principal, req.PrincipalVerified))
		if req.Reason != "" {
			message += ": " + req.Reason
		}
		message += fmt.Sprintf("; running tasks finish, nothing new starts; ends as failed at %s unless unpaused",
			formatInstant(p.expiresAt))
		if wasPaused {
			message = fmt.Sprintf("run also paused by %s", markedPrincipal(req.Principal, req.PrincipalVerified))
		}
		s.becomePaused(ctx, inFlight, req, message)
	case !req.Pause && !req.Inherited:
		if s.pause == nil {
			if !wasPaused {
				s.emitPause(ctx, eventRunUnpaused, s.status, req, orkstackv1.PauseTransition_PAUSE_TRANSITION_NOT_PAUSED, "run is not paused")
			}
			return
		}
		s.endOwnPause(now, pauseEndUnpaused)
		s.lastPause.endedBy, s.lastPause.endedVerified = req.Principal, req.PrincipalVerified
		by := markedPrincipal(req.Principal, req.PrincipalVerified)
		if s.paused() {
			s.becomePaused(ctx, inFlight, req, fmt.Sprintf("own pause lifted by %s; still paused because parent run %s is paused",
				by, s.pausedByParent.parentRunID))
			return
		}
		s.becomeUnpaused(ctx, inFlight, req, fmt.Sprintf("run unpaused by %s", by))
	case req.Pause && req.Inherited:
		if s.pausedByParent != nil {
			return
		}
		ip := &inheritedPause{parentRunID: req.ParentRunID, at: now, backstopAt: s.backstopAt(req, now)}
		ip.backstop, ip.cancel = s.pauseTimer(ctx, ip.backstopAt.Sub(now))
		s.pausedByParent = ip
		s.becomePaused(ctx, inFlight, req, "run paused because parent run "+req.ParentRunID+" is paused")
	default: // inherited unpause
		if s.pausedByParent == nil {
			return
		}
		parent := s.pausedByParent.parentRunID
		s.pausedByParent.cancel()
		s.pausedByParent = nil
		if s.paused() {
			s.becomePaused(ctx, inFlight, req, "parent run "+parent+" was unpaused; still paused by its own pause")
			return
		}
		s.becomeUnpaused(ctx, inFlight, req, "run unpaused: parent run "+parent+" was unpaused")
	}
}

// backstopAt is an inherited pause's backstop: the originating pause's
// expiry plus the grace per level, at most InheritedGraceMaxLevels levels.
func (s *runState) backstopAt(req *pauseRequest, now time.Time) time.Time {
	p := s.plan.Pause
	// pauseRequestFault refused an inherited pause whose expiry does not parse.
	origin, err := time.Parse(time.RFC3339Nano, req.OriginExpiresAt)
	if err != nil {
		origin = now
	}
	levels := min(max(req.Depth, 1), p.InheritedGraceMaxLevels)
	return origin.Add(time.Duration(p.InheritedGraceSeconds*levels) * time.Second)
}

// pauseTimer starts a pause's timer on its own cancelable child of ctx,
// never of runCtx, so fail-fast cannot settle it. d <= 0 is a ready timer.
func (s *runState) pauseTimer(ctx workflow.Context, d time.Duration) (workflow.Future, workflow.CancelFunc) {
	tctx, cancel := workflow.WithCancel(ctx)
	if d > 0 {
		if limit := s.historyLimit(); limit != nil {
			s.noteCommand(ctx, 1, limit.Reserve.CommandBytes)
		}
	} else {
		d = 0
	}
	return workflow.NewTimer(tctx, d), cancel
}

// endOwnPause moves the own pause to last_pause and cancels its timer.
func (s *runState) endOwnPause(at time.Time, end pauseEnd) {
	p := s.pause
	if p.cancel != nil {
		p.cancel()
	}
	s.lastPause = &lastPause{pause: *p, end: end, endAt: at}
	s.lastPause.pause.timer, s.lastPause.pause.cancel = nil, nil
	s.pause = nil
}

// becomePaused sets PAUSED and emits RUN_PAUSED.
func (s *runState) becomePaused(ctx workflow.Context, inFlight map[string]workflow.Future, req *pauseRequest, message string) {
	transition := orkstackv1.PauseTransition_PAUSE_TRANSITION_PAUSED
	if s.status == statusPaused {
		transition = orkstackv1.PauseTransition_PAUSE_TRANSITION_CAUSES_CHANGED
	} else {
		// Commands first, then the event, whose snapshot carries the round's
		// warnings; a change between causes sends nothing (§4.11).
		s.cascadeRound(ctx, inFlight, true, req)
		s.stopPinClocks(ctx)
	}
	s.status = statusPaused
	s.emitPause(ctx, eventRunPaused, statusPaused, req, transition, message)
}

// becomeUnpaused returns the run to RUNNING and emits RUN_UNPAUSED.
func (s *runState) becomeUnpaused(ctx workflow.Context, inFlight map[string]workflow.Future, req *pauseRequest, message string) {
	s.cascadeRound(ctx, inFlight, false, req)
	s.status = orkstackv1.Status_STATUS_RUNNING
	s.runPinClocks(ctx)
	s.emitPause(ctx, eventRunUnpaused, s.status, req, orkstackv1.PauseTransition_PAUSE_TRANSITION_UNPAUSED, message)
}

// pauseEventFields is what a pause event says of the request it answers.
type pauseEventFields struct {
	requestID  string
	transition orkstackv1.PauseTransition
}

// emitPause emits a pause event answering req, with what it did to the
// pause; a failed write is logged, never fatal. Only an API request's id is
// carried: a parent's cascade has none.
func (s *runState) emitPause(ctx workflow.Context, eventType orkstackv1.EventType, status orkstackv1.Status,
	req *pauseRequest, transition orkstackv1.PauseTransition, message string,
) {
	fields := &pauseEventFields{transition: transition}
	if req != nil && !req.Inherited {
		fields.requestID = req.RequestID
	}
	s.pauseEvent = fields
	defer func() { s.pauseEvent = nil }()
	if err := s.emit(ctx, eventType, "", status, message); err != nil {
		workflow.GetLogger(ctx).Error("emit pause event failed", "run_id", s.runID, "error", err)
	}
}

// setPauseFields fills event's request_id and pause_transition: a pause
// event's from emitPause, and a terminal run event's from how the run's end
// closed a pause it still held. Every other event, and every event of a run
// whose plan lacks Pause, carries neither.
func (s *runState) setPauseFields(ctx workflow.Context, event *orkstackv1.Event) {
	switch t := event.GetType(); {
	case t == eventRunPaused || t == eventRunUnpaused:
		if p := s.pauseEvent; p != nil {
			event.RequestId, event.PauseTransition = p.requestID, p.transition
		}
	case isTerminalRunEventType(t) && s.plan.Pause != nil:
		event.PauseTransition = s.terminalPauseTransition(ctx)
	}
}

// terminalPauseTransition is how the run's end closes its pause: a pause
// whose expiry is ending the run, or had already lapsed when it ended,
// ENDED_TIMED_OUT; a pause the run still held, ENDED_RUN_ENDED; none,
// UNSPECIFIED.
func (s *runState) terminalPauseTransition(ctx workflow.Context) orkstackv1.PauseTransition {
	if s.pauseExpired || s.pauseStop != nil {
		return orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_TIMED_OUT
	}
	if s.pause != nil {
		end := workflow.Now(ctx)
		if s.finished != nil {
			end = s.finished.AsTime()
		}
		if e, _ := s.pause.endAtRunEnd(end); e == pauseEndTimedOut {
			return orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_TIMED_OUT
		}
		return orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_RUN_ENDED
	}
	if s.pausedByParent != nil {
		return orkstackv1.PauseTransition_PAUSE_TRANSITION_ENDED_RUN_ENDED
	}
	return orkstackv1.PauseTransition_PAUSE_TRANSITION_UNSPECIFIED
}

// markedPrincipal is who asked, marked as AGENTS.md §7 (Principal) says:
// verified when OrkStack authenticated them, asserted otherwise.
func markedPrincipal(principal string, verified bool) string {
	if verified {
		return principalOr(principal) + " (verified)"
	}
	return principalOr(principal) + " (asserted)"
}

func principalOr(principal string) string {
	if principal == "" {
		return "an unnamed caller"
	}
	return principal
}

// drainPauseRequests applies every request already delivered, never
// blocking, after those the previous generation carried; changed says one
// was applied. A request dropped, or carried into the next generation,
// changes nothing this generation records. A run whose plan lacks Pause has
// no channel.
//
// It stops at the first error, leaving later requests unread: an error is
// the main loop's RUN_HISTORY_LIMIT, and the run ends on it.
func (s *runState) drainPauseRequests(ctx workflow.Context, inFlight map[string]workflow.Future,
	loop pauseLoop,
) (changed bool, err error) {
	if s.pauses == nil {
		return false, nil
	}
	// The requests the previous generation carried come first, in order.
	for len(s.restoredPause) > 0 {
		req := pauseRequestOf(s.restoredPause[0])
		s.restoredPause = s.restoredPause[1:]
		if fault := s.pauseRequestFault(req); fault != "" {
			workflow.GetLogger(ctx).Warn("carried pause request dropped", "run_id", s.runID, "why", fault,
				"request_id", req.RequestID, "pause", req.Pause, "inherited", req.Inherited)
			continue
		}
		applied, err := s.applyDecodedPause(ctx, inFlight, req, loop)
		changed = changed || applied
		if err != nil {
			return true, err
		}
	}
	for {
		var raw converter.RawValue
		if !s.pauses.ReceiveAsync(&raw) {
			return changed, nil
		}
		applied, err := s.applyPauseRequest(ctx, inFlight, raw, loop)
		changed = changed || applied
		if err != nil {
			return true, err
		}
	}
}

// pauseExpiryDue reports that a present cause's timer fired cleanly; canceled
// reports a timer the run's cancel settled.
func (s *runState) pauseExpiryDue(ctx workflow.Context) (due, canceled bool) {
	ready := func(f workflow.Future) (bool, bool) {
		if f == nil || !f.IsReady() {
			return false, false
		}
		if err := f.Get(ctx, nil); err != nil || ctx.Err() != nil {
			return false, true
		}
		return true, false
	}
	if s.pause != nil {
		if d, c := ready(s.pause.timer); d || c {
			return d, c
		}
	}
	if s.pausedByParent != nil {
		return ready(s.pausedByParent.backstop)
	}
	return false, false
}

// pauseExpiredCause is the cause a run that is not failing ends with when its
// own pause, or the backstop of its inherited one, expires.
func (s *runState) pauseExpiredCause(ctx workflow.Context, inFlight map[string]workflow.Future) error {
	s.pauseExpired = true // the terminal event says the pause timed out
	// The run is no longer paused while it stops its work: its index says
	// so, within the history the reserve keeps for one request's writes.
	defer s.syncPauseIndexWithin(ctx, inFlight)
	now := workflow.Now(ctx)
	if p := s.pause; p != nil && p.timer.IsReady() {
		message := fmt.Sprintf("run was paused at %s by %s and not unpaused before %s; "+
			"resume it with orkctl run resume, and next time unpause sooner or pause with a longer --timeout (at most %s)",
			formatInstant(p.at), markedPrincipal(p.by, p.verified), formatInstant(p.expiresAt),
			time.Duration(s.plan.Pause.MaxTimeoutSeconds)*time.Second)
		s.endOwnPause(minTime(now, p.expiresAt), pauseEndTimedOut)
		if ip := s.pausedByParent; ip != nil {
			ip.cancel() // the run ends: its backstop is settled too, as on every other branch
		}
		s.pausedByParent = nil
		return runLevelError(RunPauseTimeoutCode, message)
	}
	ip := s.pausedByParent
	message := fmt.Sprintf("paused because parent run %s was paused, and %s never unpaused it nor ended it before %s",
		ip.parentRunID, ip.parentRunID, formatInstant(ip.backstopAt))
	ip.cancel()
	s.pausedByParent = nil
	if s.pause != nil {
		s.endOwnPauseAtRunEnd(now) // the run's end closes its own pause too
	}
	return runLevelError(RunPauseTimeoutCode, message)
}

// pauseStop is how a failing run's pause, expiring, ended its failing phase:
// held handlers never start, running ones are stopped, and the run keeps its
// own cause with this as detail (docs/DSL.md, "Joins and failure handlers").
type pauseStop struct {
	// detail names the pause and when it ended, with RUN_PAUSE_TIMEOUT:
	// "pause ended at …".
	detail string
}

// stopHandlersAtPauseExpiry takes a ready expiry of a failing run's pause:
// the cause that expired ends, and the failure handlers are stopped.
func (s *runState) stopHandlersAtPauseExpiry(ctx workflow.Context, inFlight map[string]workflow.Future) {
	now := workflow.Now(ctx)
	if p := s.pause; p != nil && p.timer.IsReady() {
		s.pauseStop = &pauseStop{detail: fmt.Sprintf("pause ended at %s (%s)",
			formatInstant(p.expiresAt), RunPauseTimeoutCode)}
		s.endOwnPause(minTime(now, p.expiresAt), pauseEndTimedOut)
	} else {
		ip := s.pausedByParent
		s.pauseStop = &pauseStop{detail: fmt.Sprintf("pause, inherited from parent run %s, ended at %s (%s)",
			ip.parentRunID, formatInstant(ip.backstopAt), RunPauseTimeoutCode)}
		if s.pause != nil {
			s.endOwnPauseAtRunEnd(now) // the run's end closes its own pause too
		}
	}
	if ip := s.pausedByParent; ip != nil {
		ip.cancel()
		s.pausedByParent = nil
	}
	s.syncPauseIndexWithin(ctx, inFlight) // the failing run is no longer paused while it ends
	if s.cancelHandlers != nil {
		s.cancelHandlers()
	}
}

// pauseStopStoppedReason is a handler's row when the pause's expiry stopped
// it running.
func (s *runState) pauseStopStoppedReason() string {
	return "stopped when the run's " + s.pauseStop.detail
}

// pauseStopNotStartedReason is a held handler's row when the pause expired
// before it started.
func (s *runState) pauseStopNotStartedReason() string {
	return "did not start: the run's " + s.pauseStop.detail + " before this handler started"
}

// pauseStopNote is what a failing run's message gains when its pause's
// expiry stopped its handlers: each handler's final state.
func (s *runState) pauseStopNote() string {
	return "; its " + s.pauseStop.detail + ": failure handlers " + s.handlerStates()
}

// pauseActsOnHandlers reports that the pause can still decide a failing
// run's handlers: the run accepts pauses, its pause has not stopped them, and
// the hard end has not either (that one names them; a later expiry is not
// taken). The handler loop's pass top and its selector both ask it, so a
// pause case is registered only when the pass top will take it.
func (s *runState) pauseActsOnHandlers() bool {
	return s.pauses != nil && s.pauseStop == nil && !s.hardEnded()
}

// pauseHandlerPass applies a failing run's pause requests and a ready
// expiry, at the top of each failure-handler pass.
func (s *runState) pauseHandlerPass(ctx workflow.Context, inFlight map[string]workflow.Future) {
	s.dropDeferredPause(ctx, "the run is failing")
	if !s.pauseActsOnHandlers() {
		return
	}
	s.takeCascadeResults(ctx)
	// The handler loop never stops over a request: an over-reserve one is
	// dropped (pauseInHandlerLoop), so there is no error to take.
	if _, err := s.drainPauseRequests(ctx, inFlight, pauseInHandlerLoop); err != nil {
		workflow.GetLogger(ctx).Error("pause request in the failure-handler loop returned an error it never returns", "run_id", s.runID, "error", err)
	}
	s.syncPauseIndexWithin(ctx, inFlight) // retries a write the cluster refused
	// A timer the run's cancel settled is not an expiry: ctx.Err() stops
	// the handlers instead.
	if due, _ := s.pauseExpiryDue(ctx); due {
		s.stopHandlersAtPauseExpiry(ctx, inFlight)
	}
}

// hasPauseState reports state the carry must hold: a cause, the last pause,
// a cascade counter, or requests carried into the next generation.
func (s *runState) hasPauseState() bool {
	return s.paused() || s.lastPause != nil || s.lastCascadeSeq > 0 || len(s.deferredPause) > 0
}

// pauseCarries asks the carry gate, lazily, once a switch could carry pause
// state: true when this run's saved state holds its pause.
func (s *runState) pauseCarries(ctx workflow.Context) bool {
	if s.pauses == nil || !s.hasPauseState() {
		return false
	}
	return s.version(ctx, pauseCarryVersion, 1) >= 1
}

// fillPauseCarry puts the run's pause state into the carry, when the gate
// was taken; a run without it carries none, so its carry is unchanged.
func (s *runState) fillPauseCarry(ctx workflow.Context, carry *orkstackv1.RunContinuation) {
	if v, ok := s.versions[pauseCarryVersion]; !ok || v < 1 {
		if s.hasPauseState() {
			// Only a history recorded before the gate takes this branch; it
			// never switches while paused (canSwitchNow).
			workflow.GetLogger(ctx).Warn("run carries on without its last pause and cascade counter: "+
				"its switch predates the pause carry", "run_id", s.runID)
		}
		return
	}
	carry.Pause = s.pause.proto()
	carry.LastPause = s.lastPause.proto()
	carry.PausedByParent = s.pausedByParent.proto()
	carry.LastCascadeSeq = s.lastCascadeSeq
	if v, ok := s.versions[pauseDeferredVersion]; ok && v >= 1 {
		carry.DeferredPauseRequests = s.deferredPause
	}
}

// adoptPauseCarry takes the pause state a carry holds; its timers are armed
// at the main loop's entry (armPauseTimers). The verified saved state the
// switch wrote must say the same: a carry that adds, drops or moves a pause
// is corrupt, never adopted. It returns what is wrong, or "".
func (s *runState) adoptPauseCarry(carry *orkstackv1.RunContinuation, run *orkstackv1.Run) string {
	// A run that took the carry gate carries every pause its saved state
	// holds; one that never took it carries none, whatever that state says.
	if v, ok := s.versions[pauseCarryVersion]; ok && v >= 1 {
		if what := pauseCarryMatches(carry, run); what != "" {
			return what
		}
	}
	has := carry.GetPause() != nil || carry.GetLastPause() != nil || carry.GetPausedByParent() != nil ||
		carry.GetLastCascadeSeq() != 0 || len(carry.GetDeferredPauseRequests()) > 0
	if !has {
		return ""
	}
	if v, ok := s.versions[pauseCarryVersion]; !ok || v < 1 || s.pauses == nil {
		return "it holds a pause this run cannot carry"
	}
	if carry.GetLastCascadeSeq() < 0 {
		return fmt.Sprintf("its cascade counter %d is negative", carry.GetLastCascadeSeq())
	}
	pause, err := s.ownPauseFrom(carry.GetPause())
	if err != "" {
		return "its pause " + err
	}
	last, err := s.lastPauseFrom(carry.GetLastPause())
	if err != "" {
		return "its last pause " + err
	}
	inherited, err := s.inheritedPauseFrom(carry.GetPausedByParent())
	if err != "" {
		return "its parent's pause " + err
	}
	s.pause, s.lastPause, s.pausedByParent, s.lastCascadeSeq = pause, last, inherited, carry.GetLastCascadeSeq()
	if err := s.restoredPauseFault(carry.GetDeferredPauseRequests()); err != "" {
		return err
	}
	s.restoredPause = carry.GetDeferredPauseRequests()
	if s.paused() {
		s.status = statusPaused
	}
	return ""
}

// pauseCarryMatches says how the carry's pause differs from the saved
// state's, or "": the same pause, last pause and parent's pause, and a
// PAUSED saved state exactly when a cause is carried.
func pauseCarryMatches(carry *orkstackv1.RunContinuation, run *orkstackv1.Run) string {
	switch {
	case !proto.Equal(carry.GetPause(), run.GetPause()):
		return "its carried pause differs from its saved state's"
	case !proto.Equal(carry.GetLastPause(), run.GetLastPause()):
		return "its carried last pause differs from its saved state's"
	case !proto.Equal(carry.GetPausedByParent(), run.GetPausedByParent()):
		return "its carried parent's pause differs from its saved state's"
	case (run.GetStatus() == statusPaused) != (carry.GetPause() != nil || carry.GetPausedByParent() != nil):
		return fmt.Sprintf("its saved state is %s while its carry %s a pause", run.GetStatus(),
			map[bool]string{true: "holds", false: "holds no"}[carry.GetPause() != nil || carry.GetPausedByParent() != nil])
	}
	return ""
}

// ownPauseFrom is the pause p carries, with no timer; nil for none. Its
// expiry is after its start and within the plan's longest pause.
func (s *runState) ownPauseFrom(p *orkstackv1.RunPause) (*ownPause, string) {
	if p == nil {
		return nil, ""
	}
	if p.GetPausedAt().CheckValid() != nil || p.GetExpiresAt().CheckValid() != nil {
		return nil, "has no valid start or expiry"
	}
	at, expires := p.GetPausedAt().AsTime(), p.GetExpiresAt().AsTime()
	if longest := time.Duration(s.plan.Pause.MaxTimeoutSeconds) * time.Second; expires.Before(at) || expires.Sub(at) > longest {
		return nil, fmt.Sprintf("expires at %s, not within %s of its start %s", formatInstant(expires), longest, formatInstant(at))
	}
	return &ownPause{by: p.GetPausedBy(), verified: p.GetPausedByVerified(), reason: p.GetReason(), requestID: p.GetRequestId(),
		at: at, expiresAt: expires}, ""
}

// lastPauseFrom is the ended pause p carries; nil for none. It ended one of
// the known ways, at a valid instant not before it started.
func (s *runState) lastPauseFrom(p *orkstackv1.RunPause) (*lastPause, string) {
	pause, err := s.ownPauseFrom(p)
	if pause == nil {
		return nil, err
	}
	end := p.GetEnd()
	if end < orkstackv1.PauseEnd_PAUSE_END_UNPAUSED || end > orkstackv1.PauseEnd_PAUSE_END_RUN_ENDED {
		return nil, fmt.Sprintf("ended %s, which is not a way a pause ends", end)
	}
	if p.GetEndedAt().CheckValid() != nil || p.GetEndedAt().AsTime().Before(pause.at) {
		return nil, "has no valid end, or ended before it started"
	}
	return &lastPause{pause: *pause, end: pauseEnd(end), endAt: p.GetEndedAt().AsTime(),
		endedBy: p.GetEndedBy(), endedVerified: p.GetEndedByVerified()}, ""
}

// inheritedPauseFrom is the parent's pause p carries, with no timer; nil for
// none. It must name this run's parent, which a top-level run has not, and
// its backstop is after its start and within the longest an inherited pause
// can last.
func (s *runState) inheritedPauseFrom(p *orkstackv1.InheritedPause) (*inheritedPause, string) {
	if p == nil {
		return nil, ""
	}
	if parent := s.parentRunID(); parent == "" || p.GetParentRunId() != parent {
		return nil, "names " + p.GetParentRunId() + ", which is not this run's parent"
	}
	if p.GetSince().CheckValid() != nil || p.GetBackstopAt().CheckValid() != nil {
		return nil, "has no valid start or backstop"
	}
	since, backstop := p.GetSince().AsTime(), p.GetBackstopAt().AsTime()
	plan := s.plan.Pause
	// since is this run's clock and the backstop derives from the parent's,
	// so the bound allows for the skew between the two.
	longest := time.Duration(plan.MaxTimeoutSeconds+plan.InheritedGraceSeconds*plan.InheritedGraceMaxLevels)*time.Second +
		inheritedClockSkew
	if backstop.Before(since) || backstop.Sub(since) > longest {
		return nil, fmt.Sprintf("has its backstop at %s, not within %s of its start %s", formatInstant(backstop), longest, formatInstant(since))
	}
	return &inheritedPause{parentRunID: p.GetParentRunId(), at: since, backstopAt: backstop}, ""
}

// armPauseTimers arms, at the main loop's entry, the timer of each restored
// cause for what remains of it; one already past is a ready timer, which the
// first pass takes.
func (s *runState) armPauseTimers(ctx workflow.Context) {
	now := workflow.Now(ctx)
	if p := s.pause; p != nil && p.timer == nil {
		p.timer, p.cancel = s.pauseTimer(ctx, p.expiresAt.Sub(now))
	}
	if ip := s.pausedByParent; ip != nil && ip.backstop == nil {
		ip.backstop, ip.cancel = s.pauseTimer(ctx, ip.backstopAt.Sub(now))
	}
	s.syncPauseIndex(ctx)
}

// proto is the pause as Run.pause carries it; nil for none.
func (p *ownPause) proto() *orkstackv1.RunPause {
	if p == nil {
		return nil
	}
	return &orkstackv1.RunPause{PausedAt: timestamppb.New(p.at), ExpiresAt: timestamppb.New(p.expiresAt),
		PausedBy: p.by, PausedByVerified: p.verified, Reason: p.reason, RequestId: p.requestID}
}

// proto is the last pause as Run.last_pause carries it; nil for none.
func (l *lastPause) proto() *orkstackv1.RunPause {
	if l == nil {
		return nil
	}
	out := l.pause.proto()
	out.End = orkstackv1.PauseEnd(l.end)
	out.EndedAt = timestamppb.New(l.endAt)
	out.EndedBy, out.EndedByVerified = l.endedBy, l.endedVerified
	return out
}

// proto is the parent's pause as Run.paused_by_parent carries it.
func (ip *inheritedPause) proto() *orkstackv1.InheritedPause {
	if ip == nil {
		return nil
	}
	return &orkstackv1.InheritedPause{ParentRunId: ip.parentRunID, Since: timestamppb.New(ip.at),
		BackstopAt: timestamppb.New(ip.backstopAt)}
}

// ended reports that the run's end is recorded: a terminal status and its
// finished_at. Between the two (a timed-out run draining its work) a pause
// is still shown as current, so no snapshot built then loses it.
func (s *runState) ended() bool { return isTerminal(s.status) && s.finished != nil }

// livePauseProto, liveInheritedPauseProto: a finished run carries neither.
func (s *runState) livePauseProto() *orkstackv1.RunPause {
	if s.ended() {
		return nil
	}
	return s.pause.proto()
}

func (s *runState) liveInheritedPauseProto() *orkstackv1.InheritedPause {
	if s.ended() {
		return nil
	}
	return s.pausedByParent.proto()
}

// lastPauseProto is Run.last_pause: a finished run that was still paused
// records that pause as the run's end closed it (endAtRunEnd);
// otherwise the last pause that ended.
func (s *runState) lastPauseProto() *orkstackv1.RunPause {
	if s.ended() && s.pause != nil {
		end, at := s.pause.endAtRunEnd(s.finished.AsTime())
		return (&lastPause{pause: *s.pause, end: end, endAt: at}).proto()
	}
	return s.lastPause.proto()
}

// endAtRunEnd is how the run's end at end closes this pause: RUN_ENDED then,
// unless the pause had already lapsed — its expiry due in the same workflow
// task as the run's end, taken second — which is TIMED_OUT at its expiry.
func (p *ownPause) endAtRunEnd(end time.Time) (pauseEnd, time.Time) {
	if !end.Before(p.expiresAt) {
		return pauseEndTimedOut, p.expiresAt
	}
	return pauseEndRunEnded, end
}

// endOwnPauseAtRunEnd ends the own pause as the run's end at now closes it.
func (s *runState) endOwnPauseAtRunEnd(now time.Time) {
	end, at := s.pause.endAtRunEnd(now)
	s.endOwnPause(at, end)
}

// inheritedClockSkew is how far apart a parent's clock and its child's may
// be when the child bounds a carried backstop. It refuses a corrupt saved
// state and decides no command.
const inheritedClockSkew = time.Minute

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// pauseIndexState is what this generation has written, and the cluster
// accepted, to the run's paused attribute and pause memo; nil for a half not
// yet written.
type pauseIndexState struct {
	paused  *bool
	expires *string // the own pause's expiry, RFC 3339 nano UTC; "" for none
	// failing and held say a write's error, or the reserve holding writes
	// back, was logged already: each is logged when it starts, not at every
	// pass, and cleared once a write lands.
	failing, held bool
}

// syncPauseIndex writes the run's paused attribute and pause memo where
// they do not yet say what its pause is. Only an Indexed run writes them,
// and submit pins Indexed only where boot registered the attribute
// (task.EnsureRunPausedSearchAttribute), so the cluster accepts the write:
// it refuses one only for an attribute it does not index, by failing the
// workflow task, which no code here sees (docs/LIMITATIONS.md, "Paused-run
// index"). An error returned here is the SDK's own, before any command; it
// is logged once, never recorded as written, and the write is tried again
// at the next pass. It never fails the run: GetRun and the stream stay
// exact, and only a list reads the index. A generation writes the index at
// its first pass when it starts paused, whatever the switch copied.
func (s *runState) syncPauseIndex(ctx workflow.Context) {
	if s.plan.Pause == nil || !s.plan.Pause.Indexed {
		return
	}
	paused, expires := s.pauseIndexWant()
	idx := &s.pauseIndex
	if idx.attributeStale(paused) {
		update := task.SearchKeyOrkflowRunPaused.ValueUnset()
		if paused {
			update = task.SearchKeyOrkflowRunPaused.ValueSet("true")
		}
		if err := s.pauseIndexWrite(ctx, "attribute", func() error {
			return workflow.UpsertTypedSearchAttributes(ctx, update)
		}); err == nil {
			idx.paused = &paused
		}
	}
	if idx.memoStale(expires) {
		var value any // nil removes the key
		if expires != "" {
			value = expires
		}
		if err := s.pauseIndexWrite(ctx, "memo", func() error {
			return workflow.UpsertMemo(ctx, map[string]any{MemoPauseExpiresAt: value})
		}); err == nil {
			idx.expires = &expires
		}
	}
}

// pauseIndexWrite issues one index write. Its error is logged when writes
// start failing, not at every pass; a write that lands clears that.
func (s *runState) pauseIndexWrite(ctx workflow.Context, what string, write func() error) error {
	var err error
	if fault := pauseIndexWriteFaultForTest.Load(); fault != nil {
		err = (*fault)(what)
	}
	if err == nil {
		err = write()
	}
	idx := &s.pauseIndex
	switch {
	case err == nil:
		idx.failing, idx.held = false, false
	case !idx.failing:
		idx.failing = true
		workflow.GetLogger(ctx).Error("pause index not written; tried again at every pass, logged once",
			"run_id", s.runID, "what", what, "error", err)
	}
	return err
}

// pauseIndexWriteFaultForTest is a TEST-ONLY fault, the precedent of
// historyGuardLookaheadDisabledForTest: the SDK's test environment never
// returns an index write's error, so a test that proves a failed write is
// tried again stores a func that fails one ("attribute" or "memo") before
// it is issued. Atomic, so a test's store never races a worker's read.
// Never set in production code.
var pauseIndexWriteFaultForTest atomic.Pointer[func(what string) error]

// syncPauseIndexWithin is syncPauseIndex on a path no request's reserve
// was checked for (a pause's expiry): it writes only while one request's
// writes still fit under the run's history limit, and otherwise leaves the
// index as it stands, logged, for a run that is ending anyway.
func (s *runState) syncPauseIndexWithin(ctx workflow.Context, inFlight map[string]workflow.Future) {
	if s.plan.Pause == nil || !s.plan.Pause.Indexed {
		return
	}
	if paused, expires := s.pauseIndexWant(); !s.pauseIndex.attributeStale(paused) && !s.pauseIndex.memoStale(expires) {
		return // nothing to write: the reserve is not measured
	}
	if _, crossed := s.pauseApplyCrossing(ctx, inFlight); crossed {
		if !s.pauseIndex.held {
			s.pauseIndex.held = true
			workflow.GetLogger(ctx).Warn("pause index left as it stands: writing it would take the run's history past its limit",
				"run_id", s.runID)
		}
		return
	}
	s.syncPauseIndex(ctx)
}

// pauseIndexWant is what the index should say: paused, and the own pause's
// expiry ("" for none).
func (s *runState) pauseIndexWant() (paused bool, expires string) {
	if s.pause != nil {
		expires = s.pause.expiresAt.UTC().Format(time.RFC3339Nano)
	}
	return s.paused(), expires
}

// attributeStale reports that the attribute does not yet say paused; never
// written counts as absent.
func (idx pauseIndexState) attributeStale(paused bool) bool {
	if idx.paused == nil {
		return paused
	}
	return *idx.paused != paused
}

// memoStale reports that the memo does not yet say expires.
func (idx pauseIndexState) memoStale(expires string) bool {
	if idx.expires == nil {
		return expires != ""
	}
	return *idx.expires != expires
}

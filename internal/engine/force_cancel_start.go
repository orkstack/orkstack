package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ForceCancelAsk is a force cancel as the API decided it: what was asked, and
// who was allowed to, by what.
type ForceCancelAsk struct {
	Namespace, RunID, Reason string
	// Grace is what was asked, 0 for the least; LongestTaskGrace, the longest
	// cancellation grace of the run's tasks, 0 to read it
	// (LongestTaskGraceUnknown).
	Grace, LongestTaskGrace time.Duration
	// ClientRequestID names the request to whoever retries it, "" for none.
	ClientRequestID string
	// Cascade also force-cancels the open runs below.
	Cascade bool

	Principal         string
	PrincipalVerified bool
	AuthMethod        string
	PrincipalIssuer   string
	Permission        string
	Scope             string
	Rule              string
	PolicyGeneration  int64
}

// Check holds ask's own text before anything is looked up by it.
func (ask ForceCancelAsk) Check() error {
	if ask.Reason == "" {
		return fmt.Errorf("%w: a reason is required", ErrForceCancelText)
	}
	if err := checkForceCancelText(forceCancelText{"namespace", ask.Namespace, 200}, forceCancelText{"run_id", ask.RunID, 256},
		forceCancelText{"reason", ask.Reason, 512}, forceCancelText{"client_request_id", ask.ClientRequestID, 128}); err != nil {
		return err
	}
	for _, c := range []byte(ask.ClientRequestID) {
		if c < 0x20 || c > 0x7e {
			return fmt.Errorf("%w: client_request_id is not printable ASCII", ErrForceCancelText)
		}
	}
	return nil
}

// forceCancelDefinitionTimeout bounds the read of a run's definition: a force
// cancel does not wait for the store.
const forceCancelDefinitionTimeout = 5 * time.Second

// LongestTaskGraceUnknown is that of a run whose definition cannot be read: the
// most that may be asked for is then the least it is given, so a run is not
// ended sooner because the store is away.
const LongestTaskGraceUnknown time.Duration = -1

// LongestTaskGrace is the longest cancellation grace of wf's own tasks. What
// runs no container of its own counts as the default.
func LongestTaskGrace(wf *orkstackv1.Orkflow) time.Duration {
	var longest int32
	for _, t := range wf.GetTasks() {
		longest = max(longest, graceSecondsFor(t))
	}
	return time.Duration(longest) * time.Second
}

// The reasons a request for a force cancel is refused with (docs/REASONS.md,
// "Force cancel"). Each refusal's message begins with its reason.
const (
	ForceCancelGraceTooShortCode = "FORCE_CANCEL_GRACE_TOO_SHORT"
	ForceCancelGraceTooLongCode  = "FORCE_CANCEL_GRACE_TOO_LONG"
	ForceCancelReasonTooLongCode = "FORCE_CANCEL_REASON_TOO_LONG"
	// ForceCancelRetriesExhaustedCode: ErrForceCancelRetriesExhausted.
	ForceCancelRetriesExhaustedCode = "FORCE_CANCEL_RETRIES_EXHAUSTED"
)

// ErrForceCancelGraceTooShort is a grace in which the run could not have ended
// on its own cancel; ErrForceCancelGraceTooLong, one past what may be asked.
var (
	ErrForceCancelGraceTooShort = errors.New("the grace is too short")
	ErrForceCancelGraceTooLong  = errors.New("the grace is too long")
)

// forceCancelGraceMargin is how much longer than its tasks' own cancellation
// grace a run is given.
const forceCancelGraceMargin = 30 * time.Second

// ForceCancelStarter starts force cancels.
type ForceCancelStarter struct {
	client   client.Client
	keys     ForceCancelKeys
	mode     func() string
	settings ForceCancelSettings
	now      func() time.Time
	// namespace is the cluster's; audit records what is ended of others'.
	namespace string
	audit     artifacts.AuditStore
	reader    *TemporalRunReader
	starter   *TemporalStarter
	counter   ForceCancelCounter
	// longestGrace is the longest cancellation grace of a run's own tasks.
	longestGrace func(ctx context.Context, runID string) time.Duration
}

// NewForceCancelStarter returns a starter over what the force cancel's own
// steps are wired with.
func NewForceCancelStarter(deps ForceCancelDeps, settings ForceCancelSettings) *ForceCancelStarter {
	s := &ForceCancelStarter{client: deps.Client, namespace: deps.ClusterNamespace, reader: deps.Reader, audit: deps.Audit, starter: deps.Starter, counter: deps.Counter,
		keys: deps.Keys, mode: deps.Mode, settings: settings, now: time.Now}
	s.longestGrace = func(ctx context.Context, runID string) time.Duration {
		ctx, cancel := context.WithTimeout(ctx, forceCancelDefinitionTimeout)
		defer cancel()
		return s.reader.longestTaskGrace(ctx, runID)
	}
	return s
}

// The states a request for a force cancel is answered with; a finished one's
// are ForceCancelResult's.
const (
	FCStateRequested = "requested" // started
	FCStateWaiting   = "waiting"   // one in flight was joined
)

// ForceCancelAnswer is what became of a request for a force cancel.
type ForceCancelAnswer struct {
	State, RequestID string
	// Attached: the request joined one in flight, whose decision this is; the
	// request's own reason and grace are not used.
	Attached bool
	Decision ForceCancelDecision
	// Squatters is how many executions named as this run's force cancels were
	// nobody's, and were ended.
	Squatters int
	// Direct: the run was ended here, after two force cancels of it had not.
	Direct bool
	// JoinSkipped: whether one is in flight could not be seen, so this one
	// may be beside it.
	JoinSkipped bool
	// UnsignedLeft is how many executions claiming to be force cancels of the
	// run nobody signed, where nobody signs: each was left alone.
	UnsignedLeft int
}

// forceCancelSupersedes is how many force cancels of a run may be superseded
// before the next request ends the run itself; forceCancelAttempts, how many
// tries one named request makes.
const (
	forceCancelSupersedes = 2
	forceCancelAttempts   = 5
)

// Request answers a request to force-cancel a run: it joins one in flight, or
// starts one, superseding any past its time; a run two of them left open is
// ended here, and the one started finishes it.
func (s *ForceCancelStarter) Request(ctx context.Context, ask ForceCancelAsk) (ForceCancelAnswer, error) {
	if err := ask.Check(); err != nil {
		return ForceCancelAnswer{}, err
	}
	execution, err := s.execution(ctx, ask.RunID)
	if err != nil {
		return ForceCancelAnswer{}, err
	}
	if ask.LongestTaskGrace == 0 {
		ask.LongestTaskGrace = s.longestGrace(ctx, ask.RunID)
	}
	d, err := s.decision(ask, execution)
	if err != nil {
		return ForceCancelAnswer{}, err
	}
	// Whatever could not be started is refused before anything is ended.
	probe := d
	probe.RequestID = uuid.Nil.String()
	if _, err := s.input(probe); err != nil {
		return ForceCancelAnswer{}, err
	}
	var out ForceCancelAnswer
	found, err := s.find(ctx, ask.Namespace, ask.RunID)
	if err != nil {
		// Joining is best effort: two may run, and every step may be repeated.
		slog.Warn("force cancel: those in flight could not be listed; this one starts beside any", "run_id", ask.RunID, "error", err)
	}
	out.JoinSkipped, out.UnsignedLeft = err != nil || found.partial || found.unjudged || found.unsigned > 0, found.unsigned
	s.count(ctx, FCCountUnsigned, found.unsigned)
	s.count(ctx, FCCountUnknown, found.unknown)
	for _, f := range found.squatters {
		if s.end(ctx, ask.RunID, ask.Namespace, f, forceCancelSquatter) {
			out.Squatters++
		}
	}
	for _, f := range found.join {
		if ask.Cascade && !f.decision.Cascade {
			continue // the runs below are asked for too: this one runs beside it
		}
		current, err := s.current(ctx, f.decision)
		if err != nil {
			slog.Warn("force cancel: one in flight could not be checked against its run", "run_id", ask.RunID, "error", err)
			out.JoinSkipped = true
		}
		if current {
			out.State, out.Attached, out.RequestID, out.Decision = FCStateWaiting, true, f.decision.RequestID, f.decision
			return out, nil
		}
	}
	for _, f := range found.supersede {
		if f.decision.Cascade && !ask.Cascade {
			// Who may end one run may not stop what reaches further.
			out.State, out.Attached, out.RequestID, out.Decision = FCStateWaiting, true, f.decision.RequestID, f.decision
			return out, nil
		}
	}
	for _, f := range found.supersede {
		s.end(ctx, ask.RunID, ask.Namespace, f, forceCancelSuperseded)
	}
	direct := len(found.supersede) > 0 && !found.partial && s.superseded(ctx, ask, execution) >= forceCancelSupersedes
	attempts, whole := uint64(1), 0
	if ask.ClientRequestID != "" {
		attempts = forceCancelAttempts
	}
	if direct {
		attempts = 0
	}
	for n := uint64(0); n < attempts; n++ {
		if d.RequestID, err = forceCancelRequestID(s.keys, ask, execution, n); err != nil {
			return ForceCancelAnswer{}, err
		}
		out.RequestID, out.Decision = d.RequestID, d
		err := s.start(ctx, d)
		var taken *serviceerror.WorkflowExecutionAlreadyStarted
		switch {
		case err == nil:
			out.State = FCStateRequested
			return out, nil
		case !errors.As(err, &taken):
			return ForceCancelAnswer{}, err
		}
		attempt, err := s.attempt(ctx, d)
		if err != nil {
			return ForceCancelAnswer{}, err
		}
		if attempt.whole {
			whole++
		}
		if attempt.again {
			continue
		}
		out.State, out.Attached, out.Decision = attempt.state, true, attempt.decision
		return out, nil
	}
	if !direct && (ask.ClientRequestID == "" || whole < forceCancelSupersedes) {
		return ForceCancelAnswer{}, fmt.Errorf("%w: run %s", ErrForceCancelRetriesExhausted, ask.RunID)
	}
	// Two force cancels, or every attempt of this one, were given their whole
	// time and left the run open. Under an id of its own: the one started
	// under it finishes what this ends.
	ask.ClientRequestID = ""
	if d.RequestID, err = forceCancelRequestID(s.keys, ask, execution, 0); err != nil {
		return ForceCancelAnswer{}, err
	}
	out.State, out.RequestID, out.Decision = FCStateRequested, d.RequestID, d
	// It is started first, so a run ended here always has what finishes it;
	// a run that could not be is left to it.
	if err := s.start(ctx, d); err != nil {
		return ForceCancelAnswer{}, err
	}
	err = s.direct(ctx, d, execution)
	if errors.Is(err, ErrForceCancelStale) {
		return ForceCancelAnswer{}, err // and the one started refuses it too
	}
	if err != nil {
		slog.Warn("force cancel: a run two force cancels left open could not be ended; the one started goes on", "run_id", d.RunID, "request_id", d.RequestID, "error", err)
		return out, nil
	}
	out.Direct = true
	s.count(ctx, FCCountDirect, 1)
	return out, nil
}

func (s *ForceCancelStarter) count(ctx context.Context, outcome string, n int) {
	for ; s.counter != nil && n > 0; n-- {
		s.counter.CountForceCancel(ctx, outcome)
	}
}

// ErrForceCancelRetriesExhausted is a named request every attempt of which
// ended, not enough of them given their whole time, with the run still open or
// its end not finished: only a request under another name can go on.
var ErrForceCancelRetriesExhausted = errors.New("every attempt of this request ended, and the run is still open or its end is not finished; make it again as a new request")

// Start starts d's force cancel, under an id nothing else ever has.
func (s *ForceCancelStarter) start(ctx context.Context, d ForceCancelDecision) error {
	in, err := s.input(d)
	if err != nil {
		return err
	}
	_, err = s.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       d.WorkflowID(),
		TaskQueue:                                task.DefaultQueue,
		WorkflowExecutionTimeout:                 s.timeout(d),
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, ForceCancelWorkflowType, in)
	if err != nil {
		return fmt.Errorf("start the force cancel of run %s: %w", d.RunID, err)
	}
	return nil
}

// fcAttempt is an attempt of a named request that was made before: running,
// with the decision in force; over with the run; or over with the run still
// open or its end not finished, when the request makes another. whole: it was given all its time.
type fcAttempt struct {
	state        string
	decision     ForceCancelDecision
	again, whole bool
}

func (s *ForceCancelStarter) attempt(ctx context.Context, d ForceCancelDecision) (fcAttempt, error) {
	out := fcAttempt{state: FCStateWaiting, decision: d}
	described, err := s.steps().described(ctx, d.WorkflowID(), "")
	if err != nil {
		return fcAttempt{}, fmt.Errorf("force cancel: describe the attempt of run %s: %w", d.RunID, err)
	}
	info := described.GetWorkflowExecutionInfo()
	running := info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	flight := fcFlight{workflowID: d.WorkflowID(), execution: info.GetExecution().GetRunId(), startedAt: info.GetStartTime().AsTime()}
	// What it was started with, not what was asked now, is in force.
	in, err := s.startedWith(ctx, flight)
	var inForce *ForceCancelDecision
	if err == nil {
		inForce, err = s.judged(ctx, flight, in)
	}
	if inForce == nil {
		slog.Warn("force cancel: the decision of an earlier attempt could not be judged", "workflow_id", flight.workflowID, "error", err)
	} else {
		out.decision = *inForce
	}
	if running {
		return out, nil
	}
	run, err := s.steps().describe(ctx, d.RunID)
	if err != nil {
		return fcAttempt{}, err
	}
	out.state, out.again, out.whole = FCStateAlreadyEnded, run.Open, run.Open && inForce != nil && wholeTime(info, *inForce)
	if run.Open || inForce == nil {
		return out, nil
	}
	// The run ended; what its end lacks, another attempt finishes.
	did, err := s.status(ctx, flight, *inForce, info.GetStatus())
	if err != nil {
		return fcAttempt{}, err
	}
	out.again = did.Incomplete || did.State == FCStateFailed && did.Sent
	return out, nil
}

// wholeTime reports whether a closed force cancel under d was given all the
// time d gives: one that was refused, or ended by someone else, was not.
func wholeTime(info *workflowpb.WorkflowExecutionInfo, d ForceCancelDecision) bool {
	given := min(d.Grace()+d.Ceiling(), d.ExpiresAt.Sub(d.IssuedAt))
	return info.GetCloseTime().IsValid() && info.GetCloseTime().AsTime().Sub(info.GetStartTime().AsTime()) >= given
}

// current reports whether d still names the run as it is: one made before a
// resume ends nothing, and joining it would end with it.
func (s *ForceCancelStarter) current(ctx context.Context, d ForceCancelDecision) (bool, error) {
	cause, err := s.steps().targets(ctx, d)
	return err == nil && cause == "", err
}

// steps are the force cancel's own reads, for the API's side of it.
func (s *ForceCancelStarter) steps() *ForceCancelActivities {
	return &ForceCancelActivities{client: s.client, namespace: s.namespace, reader: s.reader, starter: s.starter, keys: s.keys, mode: s.mode, now: s.now}
}

// direct ends execution here, under d: two force cancels were given their whole
// time and left the run open. What the ended run needs is done by the force
// cancel started next, which finds its own termination.
func (s *ForceCancelStarter) direct(ctx context.Context, d ForceCancelDecision, execution string) error {
	if cause, err := s.steps().targets(ctx, d); err != nil || cause != "" {
		if err == nil {
			err = fmt.Errorf("%w: %s", ErrForceCancelStale, cause)
		}
		return err
	}
	if err := s.asked(ctx, d, execution); err != nil {
		return err
	}
	rec := d.record(execution, true, s.now().UTC())
	details, err := SignForceCancelRecord(s.keys, rec)
	if errors.Is(err, identity.ErrCannotSign) && s.keys.UnsignedDecision(s.mode()) == identity.UnsignedAsserted {
		claims := d
		claims.claimNothing()
		details, err = UnsignedForceCancelRecord(claims.record(execution, true, rec.At))
	}
	if err != nil {
		return err
	}
	_, err = s.client.WorkflowService().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: d.RunID, RunId: execution},
		Reason: ForceCancelTerminateReason, Details: details, Identity: "orkstack force cancel " + d.RequestID,
	})
	if err != nil {
		return fmt.Errorf("force cancel: end run %s: %w", d.RunID, err)
	}
	slog.Warn("force cancel: a run two force cancels left open was ended", "run_id", d.RunID, "request_id", d.RequestID)
	return nil
}

// asked reports, as an error when not, whether the cluster shows execution was
// asked to cancel at least d's grace ago. Force cancels no engine ran took
// their time too, and asked nothing: the run is then asked here, so the next
// request can tell.
func (s *ForceCancelStarter) asked(ctx context.Context, d ForceCancelDecision, execution string) error {
	events, err := s.steps().newestEvents(ctx, d.RunID, execution, forceCancelScanMax, ForceCancelInput{PageSize: s.settings.PageSize}.page(),
		func(e *historypb.HistoryEvent) bool {
			return e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
		})
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return fmt.Errorf("force cancel: run %s: execution %s has no events", d.RunID, execution)
	}
	oldest := events[len(events)-1]
	since := s.now().Sub(oldest.GetEventTime().AsTime())
	if oldest.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED {
		err = s.starter.CancelRun(ctx, d.RunID, CancelRequest{Principal: d.Principal, Verified: d.PrincipalVerified})
		var already *serviceerror.CancellationAlreadyRequested
		if !errors.As(err, &already) {
			return errors.Join(fmt.Errorf("force cancel: run %s was never asked to cancel; it is asked now", d.RunID), err)
		}
		// Asked before everything that was read.
	}
	if since < d.Grace()-forceCancelSkew {
		return fmt.Errorf("force cancel: run %s is not known to have been asked to cancel its grace of %s ago", d.RunID, d.Grace())
	}
	return nil
}

// ErrForceCancelNoKey is an engine that must sign and holds no key; another of
// the deployment's may hold one.
var ErrForceCancelNoKey = errors.New("no_signing_key")

// ErrForceCancelStale is a decision the run has been resumed since.
var ErrForceCancelStale = errors.New("the run was resumed since")

// superseded is how many force cancels of the run's execution were given their
// whole time and superseded, each by its own verified decision and counted
// once: nothing anyone else starts, or ends early, counts.
func (s *ForceCancelStarter) superseded(ctx context.Context, ask ForceCancelAsk, execution string) int {
	resp, err := s.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace, PageSize: forceCancelFlights,
		Query: fmt.Sprintf("WorkflowType = %q AND WorkflowId STARTS_WITH %q AND ExecutionStatus = \"Terminated\"",
			ForceCancelWorkflowType, ForceCancelWorkflowIDPrefix(ask.Namespace, ask.RunID)),
	})
	if err != nil {
		slog.Warn("force cancel: those superseded could not be listed", "run_id", ask.RunID, "error", err)
		return 0
	}
	if len(resp.GetNextPageToken()) > 0 {
		slog.Warn("force cancel: not every superseded one was counted", "run_id", ask.RunID)
	}
	ids := map[string]bool{}
	for _, e := range resp.GetExecutions() {
		if e.GetTaskQueue() != task.DefaultQueue {
			continue
		}
		f := fcFlight{workflowID: e.GetExecution().GetWorkflowId(), execution: e.GetExecution().GetRunId(), startedAt: e.GetStartTime().AsTime()}
		in, err := s.startedWith(ctx, f)
		if err != nil {
			slog.Warn("force cancel: a superseded one could not be read", "workflow_id", f.workflowID, "error", err)
			continue
		}
		if in.Unsigned != nil {
			continue // nobody verified it
		}
		d, err := s.judged(ctx, f, in)
		if err == nil && d != nil && d.RunID == ask.RunID && d.Namespace == ask.Namespace && d.TargetExecutionRunID == execution && wholeTime(e, *d) {
			ids[f.workflowID] = true
		}
	}
	return len(ids)
}

// execution is the run's current execution.
func (s *ForceCancelStarter) execution(ctx context.Context, runID string) (string, error) {
	described, err := s.client.DescribeWorkflow(ctx, runID, "")
	var gone *serviceerror.NotFound
	if errors.As(err, &gone) {
		return "", fmt.Errorf("force cancel: run %s: %w", runID, ErrRunNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("force cancel: describe run %s: %w", runID, err)
	}
	if described == nil || described.RawResponse == nil {
		return "", fmt.Errorf("force cancel: the cluster described run %s with nothing", runID)
	}
	info := described.RawResponse.GetWorkflowExecutionInfo()
	if info.GetType().GetName() != RunWorkflowType {
		return "", fmt.Errorf("force cancel: run %s: %w", runID, ErrRunNotFound)
	}
	return info.GetExecution().GetRunId(), nil
}

// decision is ask's, about execution, with no request id yet.
func (s *ForceCancelStarter) decision(ask ForceCancelAsk, execution string) (ForceCancelDecision, error) {
	// A run whose tasks ask for more than a decision can hold is given the
	// most one can: it must still be possible to end it.
	floor := max(s.settings.Grace, min(ask.LongestTaskGrace, forceCancelMaxCeiling-forceCancelGraceMargin)+forceCancelGraceMargin)
	if ask.LongestTaskGrace < 0 {
		floor = s.settings.MaxGrace
	}
	grace := ask.Grace
	switch {
	case grace == 0:
		grace = floor
	case grace < floor:
		return ForceCancelDecision{}, fmt.Errorf("%w: %s; this run needs at least %s to end on its own cancel", ErrForceCancelGraceTooShort, grace, floor)
	case grace > max(s.settings.MaxGrace, floor):
		return ForceCancelDecision{}, fmt.Errorf("%w: %s; at most %s may be asked", ErrForceCancelGraceTooLong, grace, max(s.settings.MaxGrace, floor))
	}
	issued := s.now().UTC()
	d := ForceCancelDecision{
		RequestID: "", Cascade: ask.Cascade, Namespace: ask.Namespace, RunID: ask.RunID, TargetExecutionRunID: execution,
		Principal: ask.Principal, PrincipalVerified: ask.PrincipalVerified, AuthMethod: ask.AuthMethod, PrincipalIssuer: ask.PrincipalIssuer,
		Permission: ask.Permission, Scope: ask.Scope, Rule: ask.Rule, PolicyGeneration: ask.PolicyGeneration, Reason: ask.Reason,
		GraceSeconds: seconds(grace), CeilingSeconds: seconds(max(s.settings.Ceiling, grace)), IssuedAt: issued,
	}
	d.ExpiresAt = issued.Add(min(s.timeout(d), forceCancelMaxLife))
	return d, nil
}

// derived is d's decision for a run below it: d's caller and reason, and d's
// grace or what that run itself needs, whichever is longer.
func (s *ForceCancelStarter) derived(d ForceCancelDecision, runID, execution string, longest time.Duration) (ForceCancelDecision, error) {
	own, err := s.decision(ForceCancelAsk{
		Namespace: d.Namespace, RunID: runID, Reason: d.Reason, LongestTaskGrace: longest,
		Principal: d.Principal, PrincipalVerified: d.PrincipalVerified, AuthMethod: d.AuthMethod, PrincipalIssuer: d.PrincipalIssuer,
		Permission: d.Permission, Scope: d.Scope, Rule: d.Rule, PolicyGeneration: d.PolicyGeneration,
	}, execution)
	own.GraceSeconds = max(own.GraceSeconds, d.GraceSeconds)
	own.CeilingSeconds = max(own.CeilingSeconds, own.GraceSeconds)
	// Never past d's own life.
	if own.ExpiresAt = own.IssuedAt.Add(min(s.timeout(own), forceCancelMaxLife)); own.ExpiresAt.After(d.ExpiresAt) {
		own.ExpiresAt = d.ExpiresAt
	}
	return own, err
}

// seconds is d in whole seconds, never less than d.
func seconds(d time.Duration) int64 { return int64((d + time.Second - 1) / time.Second) }

// timeout is how long d's force cancel may take in all.
func (s *ForceCancelStarter) timeout(d ForceCancelDecision) time.Duration {
	own := s.settings
	own.MaxGrace, own.Ceiling, own.ExecutionTimeout = max(own.MaxGrace, d.Grace()), max(own.Ceiling, d.Ceiling()), 0
	took := max(s.settings.Timeout(), own.Floor())
	if d.Cascade {
		took += own.CascadeFloor()
	}
	return took
}

// input is what d's force cancel is started with: the decision signed, or,
// where this deployment signs nothing, unsigned and claiming nothing anyone
// would have verified.
func (s *ForceCancelStarter) input(d ForceCancelDecision) (ForceCancelInput, error) {
	in := ForceCancelInput{ScanEvents: s.settings.ScanEvents, PageSize: s.settings.PageSize, RecordTimeout: s.settings.RecordTimeout,
		Timings: defaultForceCancelTimings()}
	if d.Cascade {
		in.Settings = &s.settings
	}
	signed, err := SignForceCancelDecision(s.keys, d)
	if err == nil && d.Of != nil {
		in.Derived = signed
		return in, nil
	}
	if err == nil {
		in.Decision = signed
		return in, nil
	}
	if !errors.Is(err, identity.ErrCannotSign) {
		return in, err
	}
	switch s.keys.UnsignedDecision(s.mode()) {
	case identity.UnsignedAsserted:
	case identity.UnsignedBad:
		return in, fmt.Errorf("%w: this engine cannot sign while the deployment's mode is %s (%w); restore its key file (identity.key_files) or lower the mode record", ErrForceCancelNoKey, s.mode(), err)
	default:
		return in, err
	}
	d.claimNothing()
	if err := d.check(); err != nil {
		return in, err
	}
	in.Unsigned = &d
	return in, nil
}

// claimNothing leaves a decision nobody signs with nothing anyone would have
// verified.
func (d *ForceCancelDecision) claimNothing() {
	d.PrincipalVerified, d.AuthMethod, d.PrincipalIssuer, d.Rule, d.PolicyGeneration = false, "", "", "", 0
}

// requestIDDeriver derives the ids of retried requests: an engine's issuer.
type requestIDDeriver interface {
	Derive(label jwt.DeriveLabel, data []byte) ([]byte, error)
}

// forceCancelRequestID is the id of ask's attempt: a fresh one, or, for a
// request that names itself, the one every engine with this key makes of the
// same caller's same request about the same execution.
func forceCancelRequestID(keys ForceCancelKeys, ask ForceCancelAsk, execution string, attempt uint64) (string, error) {
	if err := ask.Check(); err != nil {
		return "", err
	}
	if deriver, ok := keys.(requestIDDeriver); ok && ask.ClientRequestID != "" {
		var data []byte
		for _, part := range []string{ask.Namespace, ask.RunID, ask.Principal, ask.ClientRequestID, execution} {
			data = append(binary.AppendUvarint(data, uint64(len(part))), part...)
		}
		attempted := binary.AppendUvarint(nil, attempt)
		data = append(binary.AppendUvarint(data, uint64(len(attempted))), attempted...)
		sum, err := deriver.Derive(jwt.DeriveForceCancelRequestID, data)
		if err == nil {
			var id uuid.UUID
			copy(id[:], sum)
			id[6], id[8] = id[6]&0x0f|0x80, id[8]&0x3f|0x80
			return id.String(), nil
		}
		if !errors.Is(err, identity.ErrCannotSign) && !errors.Is(err, jwt.ErrCannotDerive) {
			return "", fmt.Errorf("force cancel: its request id: %w", err)
		}
	}
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("force cancel: its request id: %w", err)
	}
	return id.String(), nil
}

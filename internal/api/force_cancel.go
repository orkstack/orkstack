package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/runstatus"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ForceCanceler answers requests to force-cancel a run
// (engine.ForceCancelStarter).
type ForceCanceler interface {
	Request(ctx context.Context, ask engine.ForceCancelAsk) (engine.ForceCancelAnswer, error)
	Status(ctx context.Context, namespace, runID, requestID string) (engine.ForceCancelStatus, error)
	Stop(ctx context.Context, namespace, runID string) (string, error)
	Below(ctx context.Context, namespace, runID string) (engine.FCSubtree, error)
}

// forceCancelNobody is who a force cancel is from when nobody said: its record
// must name somebody.
const forceCancelNobody = "anonymous"

// ForceCancelRun ends an open run an ordinary cancel does not end
// (docs/API.md, ForceCancelRun). The run is resolved as CancelRun resolves it.
// What the caller was admitted by is signed into the force cancel's decision,
// and the run's end is recorded from that.
func (s *Service) ForceCancelRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.ForceCancelRunRequest],
) (*connect.Response[orkstackv1.ForceCancelRunResponse], error) {
	msg := req.Msg
	if msg.GetRunId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("run_id is required"))
	}
	if msg.GetPreview() || msg.GetStop() {
		return s.forceCancelBelow(ctx, msg)
	}
	// Before anything is answered: an ended run's too.
	if err := (engine.ForceCancelAsk{RunID: msg.GetRunId(), Reason: msg.GetReason(), ClientRequestID: msg.GetClientRequestId()}).Check(); err != nil {
		return nil, forceCancelError(ctx, err)
	}
	var grace time.Duration
	if msg.GetGrace() != nil {
		if err := msg.GetGrace().CheckValid(); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("grace: %w", err))
		}
		// Zero is unset to the engine, and is not what was asked.
		if grace = msg.GetGrace().AsDuration(); grace <= 0 {
			return nil, forceCancelRefused(engine.ForceCancelGraceTooShortCode, connect.CodeInvalidArgument, fmt.Errorf("grace %s is not above 0s", grace))
		}
	}
	run, err := s.LookupRunIn(ctx, msg.GetRunId(), msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	ended := runstatus.IsTerminal(run.GetStatus())
	if ended && !msg.GetCascade() && !forceCancelUnfinished(run) {
		return connect.NewResponse(&orkstackv1.ForceCancelRunResponse{State: orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_ALREADY_ENDED}), nil
	}
	if s.forceCancels == nil {
		return nil, errNoForceCancels
	}
	answer, err := s.forceCancels.Request(ctx, forceCancelAsk(ctx, req, run, grace))
	if ended && msg.GetCascade() && errors.Is(err, engine.ErrRunNotFound) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("run %q is archived and gone from the cluster; force-cancel the runs below it by themselves", run.GetId()))
	}
	if ended && errors.Is(err, engine.ErrRunNotFound) {
		// Archived, and gone from the cluster: nothing is left to finish.
		return connect.NewResponse(&orkstackv1.ForceCancelRunResponse{State: orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_ALREADY_ENDED}), nil
	}
	if err != nil {
		return nil, forceCancelError(ctx, err)
	}
	return connect.NewResponse(forceCancelResponse(answer)), nil
}

// forceCancelUnfinished reports whether an ended run may be one a force cancel
// ended, whose announcement a request made again completes.
func forceCancelUnfinished(run *orkstackv1.Run) bool {
	switch run.GetError().GetCode() {
	case engine.RunForceCanceledCode, engine.RunForceCancelUnverifiedCode:
		return true
	}
	return false
}

// forceCancelAsk is the request as the engine is asked it: who asked and what
// admitted them, as the audit trail would record both.
func forceCancelAsk(ctx context.Context, req *connect.Request[orkstackv1.ForceCancelRunRequest], run *orkstackv1.Run, grace time.Duration) engine.ForceCancelAsk {
	asked := authn.Recorded(ctx, req.Header())
	ask := engine.ForceCancelAsk{
		Namespace: artifacts.ResolveNamespace(run.GetNamespace()), RunID: run.GetId(), Reason: req.Msg.GetReason(),
		Grace: grace, ClientRequestID: req.Msg.GetClientRequestId(), Cascade: req.Msg.GetCascade(),
		Principal: boundOriginAssertion(asked.ID), PrincipalVerified: asked.Verified, AuthMethod: asked.Method, PrincipalIssuer: asked.Issuer,
	}
	if ask.Principal == "" {
		ask.Principal = forceCancelNobody
	}
	// A request served although nothing admitted it claims nothing did.
	if d := decisionFrom(ctx); d != nil && d.Decision == DecisionAllowed {
		ask.Permission, ask.Scope, ask.Rule, ask.PolicyGeneration = string(d.Permission), d.Scope, d.Rule, d.Generation
	}
	return ask
}

func forceCancelResponse(a engine.ForceCancelAnswer) *orkstackv1.ForceCancelRunResponse {
	out := &orkstackv1.ForceCancelRunResponse{
		RequestId: a.RequestID, Attached: a.Attached, AttachUnverified: int32(a.Squatters), AttachUnknown: a.JoinSkipped, Direct: a.Direct,
		Principal: a.Decision.Principal, PrincipalVerified: a.Decision.PrincipalVerified, Reason: a.Decision.Reason, Cascades: a.Decision.Cascade,
	}
	if a.Decision.GraceSeconds > 0 {
		out.Grace = durationpb.New(a.Decision.Grace())
	}
	out.State = forceCancelStates[a.State]
	return out
}

var forceCancelStates = map[string]orkstackv1.ForceCancelState{
	engine.FCStateRequested:     orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_REQUESTED,
	engine.FCStateWaiting:       orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_WAITING,
	engine.FCStateAlreadyEnded:  orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_ALREADY_ENDED,
	engine.FCStateForceCanceled: orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_FORCE_CANCELED,
	engine.FCStateCanceled:      orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_CANCELED,
	engine.FCStateEnded:         orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_ENDED,
	engine.FCStateFailed:        orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_FAILED,
	// Those of a run below.
	engine.FCBelowPending: orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_PENDING,
}

var forceCancelCascadeStates = map[string]orkstackv1.ForceCancelCascadeState{
	engine.FCCascadeRunning:  orkstackv1.ForceCancelCascadeState_FORCE_CANCEL_CASCADE_STATE_RUNNING,
	engine.FCCascadeComplete: orkstackv1.ForceCancelCascadeState_FORCE_CANCEL_CASCADE_STATE_COMPLETE,
	engine.FCCascadePartial:  orkstackv1.ForceCancelCascadeState_FORCE_CANCEL_CASCADE_STATE_PARTIAL,
	engine.FCCascadeStopped:  orkstackv1.ForceCancelCascadeState_FORCE_CANCEL_CASCADE_STATE_STOPPED,
}

// GetForceCancel reads one force cancel of a run (docs/API.md,
// GetForceCancel).
func (s *Service) GetForceCancel(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetForceCancelRequest],
) (*connect.Response[orkstackv1.GetForceCancelResponse], error) {
	if req.Msg.GetRunId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("run_id is required"))
	}
	run, err := s.LookupRunIn(ctx, req.Msg.GetRunId(), req.Msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	if s.forceCancels == nil {
		return nil, errNoForceCancels
	}
	got, err := s.forceCancels.Status(ctx, artifacts.ResolveNamespace(run.GetNamespace()), run.GetId(), req.Msg.GetRequestId())
	if errors.Is(err, engine.ErrForceCancelNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}
	if err != nil {
		return nil, forceCancelError(ctx, err)
	}
	out := &orkstackv1.GetForceCancelResponse{
		RequestId: got.Decision.RequestID, State: forceCancelStates[got.State], Cause: got.Cause, StartedAt: timestamppb.New(got.StartedAt),
		Principal: got.Decision.Principal, PrincipalVerified: got.Decision.PrincipalVerified, Reason: got.Decision.Reason,
		Grace: durationpb.New(got.Decision.Grace()), Incomplete: got.Incomplete, NewerUnknown: got.Unjudged, Unaudited: got.Unaudited,
	}
	namespace := artifacts.ResolveNamespace(run.GetNamespace())
	readable := s.readableBelow(ctx, namespace)
	if got.Cascade != nil {
		if out.Cascade, err = forceCancelCascadeProto(got.Cascade, readable); err != nil {
			return nil, err
		}
	}
	if got.Cascade == nil && got.State == engine.FCStateForceCanceled {
		// Only asked to cancel, they may be open still. Not being able to say
		// is no reason to say nothing of the force cancel.
		if below, err := s.forceCancels.Below(ctx, namespace, run.GetId()); err != nil {
			slog.WarnContext(ctx, "force cancel: the open runs below could not be listed", "run_id", run.GetId(), "error", err)
			out.OpenDescendants = &orkstackv1.ForceCancelSubtree{TruncatedBy: engine.FCCauseCouldNotRead}
		} else if out.OpenDescendants, err = forceCancelSubtreeProto(below, readable); err != nil {
			return nil, err
		}
	}
	return connect.NewResponse(out), nil
}

// forceCancelBelow answers a preview of a cascade, or stops one.
func (s *Service) forceCancelBelow(ctx context.Context, msg *orkstackv1.ForceCancelRunRequest) (*connect.Response[orkstackv1.ForceCancelRunResponse], error) {
	if msg.GetPreview() && msg.GetStop() {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("preview and stop are two requests"))
	}
	run, err := s.LookupRunIn(ctx, msg.GetRunId(), msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	if s.forceCancels == nil {
		return nil, errNoForceCancels
	}
	namespace := artifacts.ResolveNamespace(run.GetNamespace())
	out := &orkstackv1.ForceCancelRunResponse{}
	if msg.GetStop() {
		out.RequestId, err = s.forceCancels.Stop(ctx, namespace, run.GetId())
		if errors.Is(err, engine.ErrForceCancelNotFound) {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no cascade of run %q is in flight", run.GetId()))
		}
	} else {
		var below engine.FCSubtree
		if below, err = s.forceCancels.Below(ctx, namespace, run.GetId()); err == nil {
			out.OpenDescendants, err = forceCancelSubtreeProto(below, s.readableBelow(ctx, namespace))
		}
	}
	if err != nil {
		return nil, forceCancelError(ctx, err)
	}
	return connect.NewResponse(out), nil
}

// readableBelow is which Orkflows' runs of namespace the caller may read, "" the
// whole namespace's.
func (s *Service) readableBelow(ctx context.Context, namespace string) func(orkflow string) (bool, error) {
	known := map[string]bool{}
	return func(orkflow string) (bool, error) {
		may, ok := known[orkflow]
		if !ok {
			var err error
			if may, err = s.mayReadRun(ctx, &orkstackv1.Run{Namespace: namespace, OrkflowName: orkflow}); err != nil {
				return false, err
			}
			known[orkflow] = may
		}
		return may, nil
	}
}

func forceCancelSubtreeProto(below engine.FCSubtree, readable func(string) (bool, error)) (*orkstackv1.ForceCancelSubtree, error) {
	out := &orkstackv1.ForceCancelSubtree{TruncatedBy: below.Truncated}
	for _, b := range below.Open {
		row, err := forceCancelRowProto(engine.FCBelowResult{FCBelow: b}, readable)
		if err != nil {
			return nil, err
		}
		out.Runs = append(out.Runs, row)
	}
	var err error
	out.Exclusions, err = forceCancelExclusionsProto(below.Excluded, readable)
	return out, err
}

// forceCancelRowProto is a run below as the caller may see it.
func forceCancelRowProto(r engine.FCBelowResult, readable func(string) (bool, error)) (*orkstackv1.ForceCancelRunBelow, error) {
	state := forceCancelStates[r.State]
	switch r.State {
	case engine.FCBelowWaiting:
		state = orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_REQUESTED
	case engine.FCBelowAttached:
		state = orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_WAITING
	}
	may, err := readable(r.Orkflow)
	if err != nil {
		return nil, err
	}
	if !may {
		return &orkstackv1.ForceCancelRunBelow{Redacted: true, State: state}, nil
	}
	return &orkstackv1.ForceCancelRunBelow{RunId: r.RunID, ParentRunId: r.ParentRunID, Orkflow: r.Orkflow, Depth: int32(r.Depth),
		State: state, RequestId: r.RequestID, Attached: r.Attached, Cause: r.Cause}, nil
}

// forceCancelExclusionsProto names the runs left out to whoever may read the
// namespace, but for another namespace's: what Orkflow they are of is what
// could not be told.
func forceCancelExclusionsProto(excluded map[string]string, readable func(string) (bool, error)) ([]*orkstackv1.ForceCancelExclusion, error) {
	all, err := readable("")
	if err != nil {
		return nil, err
	}
	var out []*orkstackv1.ForceCancelExclusion
	for _, id := range slices.Sorted(maps.Keys(excluded)) {
		e := &orkstackv1.ForceCancelExclusion{Cause: excluded[id]}
		if all && e.GetCause() != engine.FCExcludedNamespace { // which is not theirs to read
			e.RunId = id
		}
		out = append(out, e)
	}
	return out, nil
}

func forceCancelCascadeProto(c *engine.ForceCancelCascade, readable func(string) (bool, error)) (*orkstackv1.ForceCancelCascade, error) {
	out := &orkstackv1.ForceCancelCascade{State: forceCancelCascadeStates[c.State], LeftToParents: int32(c.Covered), NotStarted: int32(c.NotStarted),
		FoundByRootIndex: int32(c.ByRoot), TruncatedBy: c.Truncated, RerunSuggested: c.Rerun, Cause: c.Cause, Unread: c.Unread}
	if !c.AsOf.IsZero() {
		out.AsOf = timestamppb.New(c.AsOf)
	}
	shown := map[string]bool{}
	for _, r := range c.Results {
		row, err := forceCancelRowProto(r, readable)
		if err != nil {
			return nil, err
		}
		out.Runs = append(out.Runs, row)
		shown[row.GetRunId()] = true
	}
	var err error
	if out.Exclusions, err = forceCancelExclusionsProto(c.Excluded, readable); err != nil {
		return nil, err
	}
	if a := c.After; a != nil {
		all, err := readable("")
		if err != nil {
			return nil, err
		}
		out.OpenAfter = &orkstackv1.ForceCancelOpenAfter{Count: int32(a.Count), UnconfirmedCause: a.Unconfirmed}
		for _, id := range a.RunIDs {
			if all || shown[id] {
				out.OpenAfter.RunIds = append(out.OpenAfter.RunIds, id)
			}
		}
	}
	return out, nil
}

var errNoForceCancels = connect.NewError(connect.CodeFailedPrecondition, errors.New("no force cancel is configured"))

// forceCancelRefused is a refusal whose message begins with its reason.
func forceCancelRefused(reason string, code connect.Code, err error) error {
	return connect.NewError(code, fmt.Errorf("%s: %w", reason, err))
}

// forceCancelError is the answer to a request the engine did not take.
func forceCancelError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, engine.ErrForceCancelGraceTooShort):
		return forceCancelRefused(engine.ForceCancelGraceTooShortCode, connect.CodeInvalidArgument, err)
	case errors.Is(err, engine.ErrForceCancelGraceTooLong):
		return forceCancelRefused(engine.ForceCancelGraceTooLongCode, connect.CodeInvalidArgument, err)
	case errors.Is(err, engine.ErrForceCancelRecordTooLarge):
		return forceCancelRefused(engine.ForceCancelReasonTooLongCode, connect.CodeInvalidArgument, err)
	case errors.Is(err, engine.ErrForceCancelNoIndex):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, engine.ErrForceCancelRetriesExhausted):
		return forceCancelRefused(engine.ForceCancelRetriesExhaustedCode, connect.CodeFailedPrecondition, err)
	case errors.Is(err, engine.ErrForceCancelText):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, engine.ErrRunNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, engine.ErrForceCancelStale):
		return connect.NewError(connect.CodeAborted, fmt.Errorf("%w; make the request again", err))
	case errors.Is(err, engine.ErrForceCancelUnverifiable), errors.Is(err, identity.ErrKeysNotRead), errors.Is(err, identity.ErrKeysStale):
		return connect.NewError(connect.CodeUnavailable, err)
	case errors.Is(err, engine.ErrForceCancelNoKey), errors.Is(err, identity.ErrCannotSign):
		// A replica that holds no key, or does not know its mode yet, where
		// the deployment signs: another replica may.
		return connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewError(clusterErrorCode(ctx, err, connect.CodeInternal), err)
}

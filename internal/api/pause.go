package api

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/typeid"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// maxRunPauseReasonBytes bounds a pause or unpause request's reason.
const maxRunPauseReasonBytes = 1024

// runAndSequenceReader is a run reader that also returns the run's last
// event sequence from the same read (TemporalRunReader.GetRunAndSequence).
type runAndSequenceReader interface {
	GetRunAndSequence(ctx context.Context, runID string) (*orkstackv1.Run, int64, error)
}

// PauseRun stops a running run starting anything new and pauses its running
// child runs (docs/API.md, PauseRun). It checks the run as the read model
// has it, sends one request under a fresh request id, and answers once the
// run has recorded it; the RUN_PAUSED event carrying that id says the run
// applied it.
func (s *Service) PauseRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.PauseRunRequest],
) (*connect.Response[orkstackv1.PauseRunResponse], error) {
	msg := req.Msg
	if err := checkPauseRequestText(msg.GetRunId(), msg.GetReason()); err != nil {
		return nil, err
	}
	var asked time.Duration
	if msg.GetTimeout() != nil {
		if err := msg.GetTimeout().CheckValid(); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("timeout: %w", err))
		}
		asked = msg.GetTimeout().AsDuration()
		if asked == 0 {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("timeout 0s is below the minimum of 1s"))
		}
	}
	length, err := pauseTimeoutOf(asked, s.pausePlan)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	run, after, binding, err := s.pauseTarget(ctx, msg.GetRunId(), msg.GetNamespace(), true)
	if err != nil {
		return nil, err
	}
	expires := s.now().Add(length)
	warnings := pauseWarnings(run, length, expires, s.cancelsAtNextFire(ctx, run))
	if msg.GetDryRun() {
		// Every check passed and the warnings are what a pause would say;
		// nothing is sent and nothing is recorded.
		return connect.NewResponse(&orkstackv1.PauseRunResponse{AfterSequence: after, Warnings: warnings}), nil
	}
	asker := authn.Recorded(ctx, req.Header())
	signal := &orkstackv1.RunPauseSignal{
		Format: 1, Pause: true, Reason: msg.GetReason(), TimeoutSeconds: int64(length / time.Second),
		Principal: boundOriginAssertion(asker.ID), PrincipalVerified: asker.Verified,
	}
	requestID, err := s.sendPause(ctx, binding, run.GetId(), signal)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.PauseRunResponse{
		RequestId: requestID, AfterSequence: after, Warnings: warnings,
	}), nil
}

// UnpauseRun lifts a run's own pause and the pauses of its child runs that
// it caused (docs/API.md, UnpauseRun).
func (s *Service) UnpauseRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.UnpauseRunRequest],
) (*connect.Response[orkstackv1.UnpauseRunResponse], error) {
	msg := req.Msg
	if err := checkPauseRequestText(msg.GetRunId(), msg.GetReason()); err != nil {
		return nil, err
	}
	run, after, binding, err := s.pauseTarget(ctx, msg.GetRunId(), msg.GetNamespace(), false)
	if err != nil {
		return nil, err
	}
	asker := authn.Recorded(ctx, req.Header())
	signal := &orkstackv1.RunPauseSignal{
		Format: 1, Pause: false, Reason: msg.GetReason(),
		Principal: boundOriginAssertion(asker.ID), PrincipalVerified: asker.Verified,
	}
	requestID, err := s.sendPause(ctx, binding, run.GetId(), signal)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&orkstackv1.UnpauseRunResponse{RequestId: requestID, AfterSequence: after}), nil
}

// checkPauseRequestText refuses a request with no run id, or a reason over
// its bound or not UTF-8.
func checkPauseRequestText(runID, reason string) error {
	switch {
	case runID == "":
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("run_id is required"))
	case len(reason) > maxRunPauseReasonBytes:
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("reason is %d bytes, above the maximum of %d", len(reason), maxRunPauseReasonBytes))
	case !utf8.ValidString(reason):
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("reason is not valid UTF-8"))
	}
	return nil
}

// pauseTarget reads the run inside the request's namespace, with its last
// event sequence, checks the request against it and checks that every engine
// process that could decide the run can pause it. Nothing is sent before it
// returns.
func (s *Service) pauseTarget(
	ctx context.Context, runID, namespace string, pause bool,
) (*orkstackv1.Run, int64, engine.Binding, error) {
	ns, err := s.namespace(ctx, namespace)
	if err != nil {
		return nil, 0, engine.Binding{}, err
	}
	binding, err := s.locate(ctx, runID)
	if err != nil {
		return nil, 0, engine.Binding{}, err
	}
	run, after, err := s.runAndSequence(ctx, binding, runID, ns.GetName())
	if err != nil {
		return nil, 0, engine.Binding{}, err
	}
	if err := pauseRequestRefusal(run, pause, s.now()); err != nil {
		return nil, 0, engine.Binding{}, err
	}
	if binding.Starter == nil {
		return nil, 0, engine.Binding{}, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no run starter configured"))
	}
	if err := s.requirePauseCapability(ctx, run); err != nil {
		return nil, 0, engine.Binding{}, err
	}
	return run, after, binding, nil
}

// runAndSequence is lookupRunInResolved plus the run's last event sequence,
// from one read where the reader offers it. A run only the archive holds has
// ended, which the checks refuse; one only this process knows (no reader
// wired) reads its sequence from the bus.
func (s *Service) runAndSequence(ctx context.Context, binding engine.Binding, runID, namespace string) (*orkstackv1.Run, int64, error) {
	if reader, ok := binding.Reader.(runAndSequenceReader); ok {
		run, after, err := reader.GetRunAndSequence(ctx, runID)
		switch {
		case err == nil:
			if artifacts.ResolveNamespace(run.GetNamespace()) != namespace {
				return nil, 0, errRunNotInNamespace(runID, namespace)
			}
			return run, after, nil
		case !errors.Is(err, engine.ErrRunNotFound):
			return nil, 0, connect.NewError(connect.CodeUnavailable, fmt.Errorf("read run %q: %w", runID, err))
		}
	}
	run, err := s.lookupRunInResolved(ctx, runID, namespace)
	if err != nil {
		return nil, 0, err
	}
	var after int64
	if s.bus != nil {
		for _, event := range s.bus.Events(runID, 0) {
			after = max(after, event.GetSequence())
		}
	}
	return run, after, nil
}

// requirePauseCapability refuses unless every engine process that could
// decide the run advertises CapabilityRunPause: an older one would record
// the request and never apply it. It fails closed, as a submit's does.
func (s *Service) requirePauseCapability(ctx context.Context, run *orkstackv1.Run) error {
	what := fmt.Sprintf("run %q", run.GetId())
	if s.capabilities == nil {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
			"%s; this engine is not wired to a cluster, so it cannot check that every engine process can pause a run", what))
	}
	lacking, err := s.capabilities.MissingCapability(ctx, engine.CapabilityRunPause, engine.ScopeWorkflow)
	if err != nil {
		s.logger.Warn("pause refused: the cluster's list of engine processes could not be read",
			"missing_capability", engine.CapabilityRunPause, "run_id", run.GetId(), "error", err)
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
			"%s; the cluster's list of engine processes could not be read, so whether every engine process "+
				"can pause a run is unknown; try again", what))
	}
	if len(lacking) == 0 {
		return nil
	}
	s.logger.Warn("pause refused: engine processes run a release that cannot pause a run",
		"missing_capability", engine.CapabilityRunPause, "identities", lacking, "run_id", run.GetId())
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"%s; %s a release that cannot pause a run; finish the rollout first", what, processCount(len(lacking))))
}

// sendPause gives signal a fresh request id, notes it on the request's audit
// record and delivers it. A run that ended meanwhile is FAILED_PRECONDITION;
// a cluster that did not answer is UNAVAILABLE.
func (s *Service) sendPause(ctx context.Context, binding engine.Binding, runID string, signal *orkstackv1.RunPauseSignal) (string, error) {
	requestID := "pause-" + typeid.New()
	signal.RequestId = requestID
	noteAudit(ctx, "request_id="+requestID)
	if err := binding.Starter.SignalPause(ctx, runID, signal); err != nil {
		if errors.Is(err, engine.ErrRunEnded) {
			return "", connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("run %q has ended: %w", runID, err))
		}
		// The cluster may have recorded the request before its answer was
		// lost: the caller is told its id, so the run's events, or the
		// audit record, can say whether it applied.
		return "", connect.NewError(clusterErrorCode(ctx, err, connect.CodeUnavailable), fmt.Errorf(
			"request %s may or may not have reached run %q; its RUN_PAUSED or RUN_UNPAUSED event carries this id if it did: %w",
			requestID, runID, err))
	}
	return requestID, nil
}

// cancelsAtNextFire reports whether the schedule that started run cancels
// an open run at its next fire (overlap CANCEL_OTHER). Best effort: a
// schedule that cannot be read is logged and warns nothing.
func (s *Service) cancelsAtNextFire(ctx context.Context, run *orkstackv1.Run) bool {
	origin := run.GetOrigin()
	if origin.GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE || s.schedules == nil || origin.GetDescription() == "" {
		return false
	}
	view, err := s.schedules.Describe(ctx, run.GetNamespace(), origin.GetDescription())
	if err != nil {
		s.logger.Debug("pause warnings: the run's schedule could not be read", "run_id", run.GetId(), "error", err)
		return false
	}
	return view.ID == origin.GetId() &&
		view.Record.GetSpec().GetOverlap() == orkstackv1.OverlapPolicy_OVERLAP_POLICY_CANCEL_OTHER
}

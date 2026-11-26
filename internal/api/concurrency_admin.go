package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
)

// ResetConcurrencyLimit resets one key's limit (docs/RUNBOOKS.md). What it
// asks of the limit, and what the limit decides, is the resetter's; this maps
// it. The audit record of a call that started a reset, or was refused, is the
// audit interceptor's; a poll writes none.
func (s *Service) ResetConcurrencyLimit(ctx context.Context, req *connect.Request[orkstackv1.ResetConcurrencyLimitRequest]) (*connect.Response[orkstackv1.ResetConcurrencyLimitResponse], error) {
	msg := req.Msg
	if err := dsl.CheckConcurrencyKey(msg.GetKey(), dsl.KeyPlanned); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("key: %w", err))
	}
	if !msg.GetDryRun() {
		switch {
		case msg.GetRequestId() == "":
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("request_id names the reset and is required"))
		case len(msg.GetRequestId()) > engine.ResetRequestIDMax || !printableASCII(msg.GetRequestId()):
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("request_id must be at most %d bytes of printable ASCII", engine.ResetRequestIDMax))
		case !msg.GetIssuedAt().IsValid():
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("issued_at names the reset, with request_id, and is required"))
		}
	}
	ns, err := s.namespace(ctx, msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	incarnation, err := concurrencyIncarnation(ns)
	if err != nil {
		return nil, err
	}
	if s.concurrencyResetter == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("this engine is not wired to a cluster, so it cannot reset a concurrency limit"))
	}
	in := engine.ConcurrencyReset{Namespace: ns.GetName(), Incarnation: incarnation, Key: msg.GetKey(), RequestID: msg.GetRequestId(),
		ExpectedEpoch: msg.ExpectedEpoch, DryRun: msg.GetDryRun(), ForceUnanswered: msg.GetForceUnanswered()}
	if msg.GetIssuedAt().IsValid() {
		in.IssuedAt = msg.GetIssuedAt().AsTime()
	}
	res, err := s.concurrencyResetter.ResetConcurrencyLimit(ctx, in)
	if err != nil {
		return nil, resetRefused(err)
	}
	return connect.NewResponse(&orkstackv1.ResetConcurrencyLimitResponse{
		RequestId: msg.GetRequestId(), State: resetStates[res.State], Epoch: res.Epoch, NewEpoch: res.NewEpoch,
		Holders: int32(res.Holders), Queued: int32(res.Queued), Dropped: int32(res.Dropped), Adopted: int32(res.Adopted), Seeded: int32(res.Seeded),
		ConsequenceCode: resetConsequences[res.ConsequenceCode], Consequence: res.Consequence,
		ReasonCode: res.Code, Reason: res.Reason, Note: res.Note,
	}), nil
}

// resetRefused is a refused reset on the wire: UNAVAILABLE where the same
// request may be asked again as it is, else FAILED_PRECONDITION, each opening
// with its code.
func resetRefused(err error) error {
	var refused *engine.ConcurrencyResetError
	if !errors.As(err, &refused) {
		return connect.NewError(connect.CodeUnavailable, errors.New("the limit could not be reset; ask again with the same request id"))
	}
	code := connect.CodeFailedPrecondition
	if refused.Retryable {
		code = connect.CodeUnavailable
	}
	return connect.NewError(code, fmt.Errorf("%s: %s", refused.Code, refused.Reason))
}

var resetStates = map[string]orkstackv1.ConcurrencyResetState{
	engine.ResetStateDryRun:      orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_DRY_RUN,
	engine.ResetStateRunning:     orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_RUNNING,
	engine.ResetStateCompleted:   orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_COMPLETED,
	engine.ResetStateFailed:      orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_FAILED,
	engine.ResetStateInterrupted: orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_INTERRUPTED,
	engine.ResetStateNotApplied:  orkstackv1.ConcurrencyResetState_CONCURRENCY_RESET_STATE_NOT_APPLIED,
}

var resetConsequences = map[string]orkstackv1.ConcurrencyResetConsequence{
	engine.ResetAnswering:        orkstackv1.ConcurrencyResetConsequence_CONCURRENCY_RESET_CONSEQUENCE_ANSWERING,
	engine.ResetAbsent:           orkstackv1.ConcurrencyResetConsequence_CONCURRENCY_RESET_CONSEQUENCE_ABSENT,
	engine.ResetUnansweredForced: orkstackv1.ConcurrencyResetConsequence_CONCURRENCY_RESET_CONSEQUENCE_UNANSWERED_FORCED,
}

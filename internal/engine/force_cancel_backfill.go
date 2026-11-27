package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

// Backfill writes the audit records a force cancel that ended a run could not,
// from the signed facts the cluster holds: the decision it was started with and
// the record of the run's close. One still running writes its own; one whose
// execution is gone from the cluster, or whose decision does not verify, leaves
// nothing to write them from. A record already there stays as it is.
func (a *ForceCancelActivities) Backfill(ctx context.Context, namespace, runID string) error {
	if a.audit == nil {
		return nil
	}
	_, closed, err := a.closeOf(ctx, ForceCancelDecision{Namespace: namespace, RunID: runID}, "")
	if err != nil {
		return err
	}
	if closed.Verification != ForceCancelVerified && closed.Verification != ForceCancelAsserted {
		return nil
	}
	described, err := a.described(ctx, closed.Record.WorkflowID, "")
	var gone *serviceerror.NotFound
	if errors.As(err, &gone) {
		slog.WarnContext(ctx, "force cancel: an ended run's force cancel is gone from the cluster; audit records it lacks cannot be written", "run_id", runID, "request_id", closed.Record.RequestID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("force cancel: describe %s: %w", closed.Record.WorkflowID, err)
	}
	info := described.GetWorkflowExecutionInfo()
	if info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}
	s := a.starting()
	f := fcFlight{workflowID: closed.Record.WorkflowID, execution: info.GetExecution().GetRunId(), startedAt: info.GetStartTime().AsTime()}
	in, err := s.startedWith(ctx, f)
	if errors.Is(err, errForceCancelInput) {
		return nil
	}
	if err != nil {
		return err
	}
	d, err := s.judged(ctx, f, in)
	if err != nil {
		return err
	}
	if d == nil || d.RunID != runID || d.Namespace != namespace || d.RequestID != closed.Record.RequestID {
		return nil
	}
	// What it did beyond ending the run only its own result says.
	result := ForceCancelResult{State: FCStateForceCanceled, Terminated: true, ProgressingAtCeiling: closed.Record.ProgressingAtCeiling}
	if info.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		result.Cause = closedCause(info.GetStatus())
	} else {
		result.Cause = FCCauseUnclassified
		var own ForceCancelResult
		err := a.client.GetWorkflow(ctx, f.workflowID, f.execution).Get(ctx, &own)
		var asked serviceerror.ServiceError
		switch {
		case errors.As(err, &asked) && !errors.As(err, &gone), ctx.Err() != nil && err != nil:
			return fmt.Errorf("force cancel: read the result of %s: %w", f.workflowID, err)
		case err != nil: // no later read will read it
			slog.WarnContext(ctx, "force cancel: an ended run's force cancel has no readable result; its record says so", "run_id", runID, "request_id", d.RequestID, "error", err)
		}
		if err == nil && own.RequestID == d.RequestID && own.State == FCStateForceCanceled && checkForceCancelText(forceCancelText{"cause", own.Cause, 64}, forceCancelText{"ended_by", own.EndedBy, 128}) == nil {
			result = own
		}
	}
	mark := "; backfilled " + a.now().UTC().Format(time.RFC3339) + " from the signed record"
	for _, kind := range []string{forceCancelRequested, forceCancelCompleted} {
		rec, err := a.auditRecord(kind, *d, result)
		if err != nil {
			return err
		}
		rec.Detail += mark
		written, err := a.putNew(ctx, rec)
		if err != nil {
			return fmt.Errorf("force cancel: backfill %s of run %s: %w", kind, runID, err)
		}
		if written && kind == forceCancelCompleted {
			a.countEnding(ctx, *d, result.State, closed.Record.TerminatedExecutionRunID)
		}
		if written {
			slog.InfoContext(ctx, "force cancel: a missing audit record was written from the signed record", "action", kind, "run_id", runID, "request_id", d.RequestID)
		}
	}
	return nil
}
